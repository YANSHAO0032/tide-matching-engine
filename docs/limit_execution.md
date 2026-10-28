# 限价订单的执行约束

ReferenceOrderBook 与 ProductionOrderBook 提供相同的 IOC、FOK 和 PostOnly 窄入口。输入使用 `LimitOrder` 的身份、方向、限价和数量字段；该载荷不决定执行策略，调用的入口决定策略。

| 入口 | 成交 | 正余量 | incoming QueuePriority |
|---|---|---|---|
| `place_limit_ioc(order, math, trade_ids)` | 按价格优先、同价 FIFO 尽可能成交 | 取消，不入簿 | 不分配 |
| `place_limit_fok(order, math, trade_ids)` | 全部成交或拒绝 | 不允许部分成交后取消 | 不分配 |
| `place_limit_post_only(order)` | 入场时不能成为 taker，等价价格也算交叉 | 成功后整个订单挂簿 | 仅成功 rest 时分配 |

IOC/FOK 的 incoming 不分配 priority，但成交触发的冰山 maker 补量会分配新 priority；完整预检覆盖这些分配。

PostOnly 成功挂簿后可以被后续订单作为 maker 成交。拒绝入场交叉与禁止订单以后参与任何成交是不同的约束。

## 校验与提交

重复 OrderId 在任何其他检查前拒绝。IOC/FOK 随后使用传入的已验证 `MarketMath` 检查输入限价和数量的 tick/lot 对齐、精确本金及可表示性，并完整扫描当前可执行 maker。每笔成交本金和总成交本金都以 checked 算术预检；遇到冰山 maker，按可见片成交并模拟补量、队尾移动和新 priority，FOK 包含这部分可执行流动性；Sell 的 maker 价格可能高于限价，因此不能仅检查输入订单的本金。

扫描时只访问各自订单簿的容器，不改变 maker 数量、索引、FIFO 或 priority。Reference 使用独立的 Vec 模型，Production 使用自身价格索引和 intrusive FIFO，双方不共享扫描或提交算法。Limit 的执行边界是输入限价：Buy 只成交价格不高于限价的 maker，Sell 只成交价格不低于限价的 maker。

FOK 的计划仍有余量时返回 `FokInsufficientLiquidity = 2016`。在可执行计划确定后，调用方提供的 TradeId 必须足量；不足按现有内部调用契约在首笔 mutation 前 fail-closed panic，不伪装为客户业务拒绝。全部检查完成后才按计划顺序提交，不使用“先成交再回滚”。TradeId 的全局唯一性仍由调用方承担。

PostOnly 先判断最优对手价格是否交叉，再检查 priority 和同侧同价档位的剩余量聚合；Production 还预检档位计数与 Arena 容量。Reference 独立累加 live remaining，不以原始数量代替。交叉返回 `PostOnlyWouldCross = 2015`；成功时只挂单，不调用成交路径。此入口沿用既有 rest 的数值载荷契约，调用方须先完成 tick/lot 验证。

## 结果与取消原因

IOC/FOK 返回 `LimitImmediateExecution`。`events()` 按真实成交顺序保存 `Trade`，并保留冰山 maker 的总余量生命周期与补量事件；IOC 还有正余量时，尾部追加唯一的 `OrderCanceled { order_id, remaining, reason: IocRemainder }`。`canceled_remaining()` 返回同一正余量，完全成交时为 `None`。取消的是尚未入簿的 incoming 余量，不是删除一张 resting order。

结果对象的公开构造函数只包装载荷，不验证成交身份、数量守恒或 TradeId 唯一性；这些性质由订单簿执行入口及调用方契约保证，不能由任意手工构造的对象推断。

`CancelReason::IocRemainder = 1114` 是新增且固定的协议编号；不能借用 `UserRequested` 或 `MarketProtection`。该窄执行结果没有完整接收/终结 envelope、资金释放、费用或结算事件；这些职责仍由后续命令与资金层承担。

## 合法组合边界

`validate_basic_order_combination` 可供输入适配层检查该基础 schema 支持的类型、TIF 和标志：Limit GTC/IOC/FOK、Iceberg GTC、Market IOC；PostOnly 允许 Limit/Iceberg GTC。当前不支持 Market FOK；因此按 base 和按 quote 的 Market FOK 都会被拒绝。该基础 helper 不承担 Stop、DAY/GTD 与 ReduceOnly 的完整组合校验，非法基础组合使用稳定码 `InvalidOrderCombination = 2001`。普通 DAY/GTD 由显式 timed 入口处理，Stop 由独立入口处理；活动订单修改见[挂单修改与本地预留授权](amend.md)。

订单簿的显式限价入口不接受 Market 输入或额外 flags，因而不能构造 PostOnly + IOC/FOK 的调用组合。上述 schema 校验不代替簿状态、数值和资金校验，也不构成完整 PlaceOrder 协议。

数值换算及取整契约见 [数值计算公式](numerical_formulas.md)。

冰山输入、可见深度与事件顺序见 [冰山订单](iceberg.md)。

实现文件与测试入口见 [订单簿模块与代码入口](orderbook_modules.md)。
