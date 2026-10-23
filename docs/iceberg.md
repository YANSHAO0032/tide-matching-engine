# 冰山订单

冰山订单有总剩余量、可见剩余量、隐藏剩余量和固定显示上限。原始输入要求显示上限严格正且小于总下单量，违反关系使用稳定拒绝码 `InvalidIceberg = 2011`。总量和显示上限都需要 lot 对齐；数值校验不能以较小的显示量代替总量。

## 输入和查询

`IcebergOrder::try_new(LimitGtcOrder, peak_size)` 验证总量与显示上限关系。字段保持私有，调用方通过 `order()` 和 `peak_size()` 读取输入。两本簿提供相同的入口：

| 入口 | 职责 |
|---|---|
| `place_iceberg(order, math, trade_ids)` | 按总量和限价尝试成交，余量按显示上限分片挂簿，返回有序事件 |
| `place_iceberg_post_only(order, math)` | 在改簿前拒绝含等价的交叉，通过全部校验才挂单 |
| `place_limit_gtc_with_events(order, trade_ids)` | 普通限价成交及余量挂单，并返回冰山 maker 的生命周期事件 |

当前支持 Iceberg GTC 及其 PostOnly 形式；IOC/FOK 与 Iceberg 输入组合非法。普通 IOC/FOK 可以与冰山 maker 成交。DAY/GTD、Stop、Amend 和资金预留仍属于后续功能。

运行态的 `remaining()` 沿用可见量口径，与 `visible_remaining()` 相同。`hidden_remaining()` 是隐藏量，`total_remaining()` 是两者之和；`peak_size()` 对冰山返回 `Some(Qty)`，普通订单为 `None`。查询方须按用途选择字段：公开价档深度累计可见量，订单余量与取消数量使用总量。Cancel 返回被移除的运行态，包含尚未成交的可见和隐藏量。

## 成交与补量

incoming 冰山先以完整总量按限价作为 taker 尝试成交；若仍有余量，再显示不超过 peak 的部分，余下隐藏。这一输入行为与已挂单 maker 的显示限制分开：maker 每笔只能成交当前可见片，不能直接扣隐藏量。

maker 的可见片耗尽且仍有隐藏量时，立即显示 `min(peak, hidden)`，从隐藏量中扣除相同数量，并移到同价队尾。每次补量取得新的 market-local QueuePriority，同价其他订单因此可以先成交。即使触发补量的那一笔已经满足 incoming，maker 仍要完成补量，不能留下只有隐藏量的零可见节点。

补量可能增加档位可见聚合：例如旧片只剩 1，而下一片大小为 5。预检必须检查成交扣减后再加补量的结果，不能把所有成交路径都视为只减不增。

## 事件顺序与余量口径

有冰山 maker 参与时，富事件入口按以下顺序记录每笔成交：

1. `Trade`。
2. 该冰山 maker 的 `OrderPartiallyFilled` 或 `OrderFilled`。部分成交的 `remaining` 是总余量，包含隐藏量。
3. 如果发生补量，再记录 `IcebergRefreshed { order_id, visible }`。

补量事件的 `visible` 仅是新片显示量，不是总余量，也不是另一笔成交。IOC 正余量取消与市价保护触发保留各自的终结顺序。既有仅返回 `TradeEvent` 的接口仍提供成交投影；需要 maker 生命周期的调用方使用富事件入口。普通订单已有事件序列保持原契约；这些入口不构成完整 Accepted/WAL/资金事件协议。

incoming 冰山实际发生过成交时，在上述成交序列后报告自身最终的 `OrderPartiallyFilled(total)` 或 `OrderFilled`。只挂单、没有成交时不生成这一成交状态事件。

## 失败原子性与独立模型

Reference 通过独立 Vec 模拟价格与同价顺序，Production 使用其价格档位、Arena 和 FIFO。完整预检模拟每一片成交、补量和优先级推进；不会在真实簿上先部分执行再回滚。FOK 的可执行量包括按该顺序实际可补量的隐藏流动性。优先级不足、可见深度溢出、当前入口适用的本金或预算算术失败，以及 TradeId 不足，均须在真实状态提交前发现。

普通 GTC、IOC/FOK 和市价入口在遇到冰山 maker 时同样遵守补量顺序。IOC/FOK/Market 的 incoming 不挂单、不取得优先级，但其撮合所触发的 maker 补量仍消耗优先级。Cancel 和只读查询不需要新优先级。

数量、补量和档位聚合公式见 [数值计算公式](numerical_formulas.md)，执行顺序见 [关键逻辑流程](logic_flows.md)。

实现文件与测试入口见 [订单簿模块与代码入口](orderbook_modules.md)。
