# 订单簿模块与代码入口

订单簿提供两套独立实现：生产簿使用价格索引、订单索引、Arena 和档位链表；参考簿使用 Vec 与排序。两者遵守相同业务契约，通过差分测试比较每条命令的原序事件、订单、深度和适用不变量。

## 文件与模块的关系

[库入口](../crates/matching-orderbook/src/lib.rs) 声明两个公开模块，并重新导出 `ProductionOrderBook` 和 `ReferenceOrderBook`。各自的 `.rs` 文件是父模块入口，同名目录保存由 `mod` 声明加载的子模块。

例如 `production_orderbook.rs` 中的 `mod iceberg;` 加载 `production_orderbook/iceberg.rs`。子模块中的 `impl ProductionOrderBook` 给同一个类型增加方法，不创建新的订单簿对象或状态副本。调用者继续通过订单簿实例调用原有公开方法。

模块根文件保存状态字段、构造、基础查询和跨功能使用的内部类型。业务方法按职责移动到子模块；功能独用的计划类型与使用它的实现放在一起。

## 实现入口

| 职责 | 生产簿 | 参考簿 |
|---|---|---|
| 类型、构造与基础查询 | [production_orderbook.rs](../crates/matching-orderbook/src/production_orderbook.rs) | [reference_orderbook.rs](../crates/matching-orderbook/src/reference_orderbook.rs) |
| 挂单、取消 | [resting.rs](../crates/matching-orderbook/src/production_orderbook/resting.rs) | [resting.rs](../crates/matching-orderbook/src/reference_orderbook/resting.rs) |
| 单笔与连续撮合 | [matching.rs](../crates/matching-orderbook/src/production_orderbook/matching.rs) | [matching.rs](../crates/matching-orderbook/src/reference_orderbook/matching.rs) |
| 限价执行策略 | [limit_execution.rs](../crates/matching-orderbook/src/production_orderbook/limit_execution.rs) | [limit_execution.rs](../crates/matching-orderbook/src/reference_orderbook/limit_execution.rs) |
| 按基础数量执行市价单 | [market_by_base.rs](../crates/matching-orderbook/src/production_orderbook/market_by_base.rs) | [market_by_base.rs](../crates/matching-orderbook/src/reference_orderbook/market_by_base.rs) |
| 按报价预算执行市价单 | [market_by_quote.rs](../crates/matching-orderbook/src/production_orderbook/market_by_quote.rs) | [market_by_quote.rs](../crates/matching-orderbook/src/reference_orderbook/market_by_quote.rs) |
| 冰山可见片与补量 | [iceberg.rs](../crates/matching-orderbook/src/production_orderbook/iceberg.rs) | [iceberg.rs](../crates/matching-orderbook/src/reference_orderbook/iceberg.rs) |
| 普通订单定时挂单与到期 | [expiry.rs](../crates/matching-orderbook/src/production_orderbook/expiry.rs) | [expiry.rs](../crates/matching-orderbook/src/reference_orderbook/expiry.rs) |
| 未触发止损、触发前到期 | [stop_orders.rs](../crates/matching-orderbook/src/production_orderbook/stop_orders.rs) 与 [stop_book.rs](../crates/matching-orderbook/src/stop_book.rs) | [stop_orders.rs](../crates/matching-orderbook/src/reference_orderbook/stop_orders.rs) |
| 止损触发与级联 | [stop_trigger.rs](../crates/matching-orderbook/src/production_orderbook/stop_trigger.rs) | [stop_trigger.rs](../crates/matching-orderbook/src/reference_orderbook/stop_trigger.rs) |
| 改单候选、普通改单与完整上下文入口 | [amend_candidate.rs](../crates/matching-orderbook/src/production_orderbook/amend_candidate.rs) | [amend_candidate.rs](../crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs) |
| 冰山改单 | [amend_iceberg.rs](../crates/matching-orderbook/src/production_orderbook/amend_iceberg.rs) | [amend_iceberg.rs](../crates/matching-orderbook/src/reference_orderbook/amend_iceberg.rs) |
| 未触发止损改单 | [amend_stop.rs](../crates/matching-orderbook/src/production_orderbook/amend_stop.rs) | [amend_stop.rs](../crates/matching-orderbook/src/reference_orderbook/amend_stop.rs) |

文件按业务职责组织。预检与提交尽量保留在同一功能模块，便于核对拒绝是否发生在真实改簿之前；共享的结构预检仍由负责该结构的模块维护。限价和市价入口遇到冰山 maker 时，通过本模型的冰山计划完成相应执行。

## 依赖与可见性

- 两套模型可以共享领域类型、数值契约及不含算法的数据载荷；各自维护 maker 选择、成交规划、提交和冰山补量逻辑。
- 生产簿的 HashMap 负责按 ID 定位，BTreeMap 负责价格顺序，PriceLevel 链表负责同价 FIFO。参考簿通过 Vec 和稳定排序独立得出相同业务顺序。
- 子模块读取父模块的私有字段；字段保持私有。需要被兄弟模块调用的辅助方法使用必要范围的 `pub(super)`，不作为 crate 对外 API。
- 多个功能之间共享的内部计划类型保留在父模块；只属于市价或冰山等单一职责的计划类型保留在所属子模块。不会为了复用类型而合并两套撮合算法。
- 公开类型路径、方法签名和调用方式保持一致；模块拆分本身不改变失败原子性、数值边界、事件顺序或优先级分配。

## 测试组织与阅读顺序

两个模型的单元测试统一放在各自的 `tests/` 目录，由 `tests.rs` 声明案例分组，仍可以检查所属订单簿的内部状态，无需为测试开放生产字段。复用夹具与结构检查放在各自的测试支持模块。

订单簿基础结构也采用相同方式：`PriceLevel` 的测试入口是 [price_level/tests.rs](../crates/matching-orderbook/src/price_level/tests.rs)，其下按共享校验、尾插、部分成交、头删、非头解除和性质模型分组；`OrderArena` 的测试入口是 [order_arena/tests.rs](../crates/matching-orderbook/src/order_arena/tests.rs)，其下按生命周期、模型、访问计数和预检分组。生产文件只保留受 `cfg(test)` 保护的测试入口，结构字段和公开接口没有因测试迁移而改变。

订单簿使用的市价领域值对象及保护计算位于 matching-domain；其测试入口为 [market_order/tests.rs](../crates/matching-domain/src/market_order/tests.rs)，按订单值语义、报价执行结果、保护价格、命令开始上下文和保护来源稳定编码分组。该布局只移动测试，不改变领域类型或稳定数值编码。

市价测试按普通路径和受保护路径组织。生产簿的 [基础数量市价测试](../crates/matching-orderbook/src/production_orderbook/tests/market_by_base.rs) 与 [报价预算市价测试](../crates/matching-orderbook/src/production_orderbook/tests/market_by_quote.rs) 各自以内部测试模块分隔两条路径；参考簿将受保护基础数量、报价预检和报价执行案例分别放在 [protected_market_by_base.rs](../crates/matching-orderbook/src/reference_orderbook/tests/protected_market_by_base.rs)、[protected_market_by_quote_preflight.rs](../crates/matching-orderbook/src/reference_orderbook/tests/protected_market_by_quote_preflight.rs) 和 [protected_market_by_quote_execution.rs](../crates/matching-orderbook/src/reference_orderbook/tests/protected_market_by_quote_execution.rs)。两套测试仍各自构造模型和夹具，不通过共享撮合逻辑取得答案。

参考簿的冰山案例集中在 [tests/iceberg.rs](../crates/matching-orderbook/src/reference_orderbook/tests/iceberg.rs)，限价策略案例集中在 [tests/limit_execution.rs](../crates/matching-orderbook/src/reference_orderbook/tests/limit_execution.rs)。功能实现文件不再声明自己的测试子模块，也不为单个测试文件额外建立同名目录。

[差分测试入口](../crates/matching-orderbook/src/production_orderbook/tests/differential.rs) 继续独立适配两个模型，并保留限价约束与冰山命令 tape。结构迁移可能改变测试模块路径，但不会减少案例、改变断言、降低性质测试次数或替换固定 seed。

差分测试仍在生产订单簿的测试模块下：除了比较公开业务结果，它还直接检查生产簿的 Arena、双侧价档、订单索引及失败前后的完整物理状态。这种位置使测试能够读取私有结构，无需增加生产查询接口或扩大字段可见性。

差分入口保留共享测试类型与模块声明，案例和公共工具按职责平铺在 `tests/differential/`：

| 文件 | 职责 |
|---|---|
| [support.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/support.rs) | 两个模型的状态观察、结构不变量与失败状态核对 |
| [tape.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/tape.rs) | 命令域校验、固定种子生成、独立适配与逐命令回放 |
| [limit_gtc.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/limit_gtc.rs) | 基础限价、取消、观察校验和确定性命令序列案例 |
| [market_by_base.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/market_by_base.rs) | 按基础数量成交的市价单及保护边界案例 |
| [market_by_quote.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/market_by_quote.rs) | 按报价预算成交的固定案例和预算守恒性质 |
| [protected_market_by_quote.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/protected_market_by_quote.rs) | 受保护报价市价单的差分、预算和原序事件案例 |
| [protected_quote_scenarios.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/protected_quote_scenarios.rs) | 受保护报价市价单的专项固定场景与失败原子性 |
| [execution_constraints.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/execution_constraints.rs) | IOC、FOK、PostOnly 命令序列与语义性质 |
| [iceberg.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/iceberg.rs) | 冰山混合命令序列、补量和数量守恒性质 |
| [expiry.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/expiry.rs) | 逻辑时间、会话与普通/止损到期比较 |
| [amend_regular.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_regular.rs) | 常规改单、绝对数量、事件原序与完整 TIF 迁移 |
| [amend_full.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full.rs) 及其 [scenarios.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full/scenarios.rs)、[tapes.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full/tapes.rs) | 全容器改单、真实签名、触发/到期、100k 与两组双次 10k 完整 transcript |

公共工具只统一测试观察和执行驱动；两本订单簿分别执行自己的撮合算法，比较时保留原始事件顺序。测试数据类型的字段保持私有，跨分组辅助函数只在差分测试范围内开放。

阅读时先查看两种订单簿的字段和查询方法，再沿所需公开入口进入对应功能文件，最后查看该功能的单元测试与差分案例。基础参考撮合仍不自动挂入 incoming 余量，完整生命周期入口负责相应挂单；不能仅因两侧方法同名就忽略接口契约差异。

业务说明见 [限价订单执行约束](limit_execution.md)、[冰山订单](iceberg.md)、[挂单修改与本地预留授权](amend.md)、[数值计算公式](numerical_formulas.md) 与 [关键逻辑流程](logic_flows.md)。

## 未触发止损订单

未触发止损订单不进入常规价格档位、Arena 或常规订单索引。生产簿将它们放在独立 StopBook，档位键为触发价与来源：买方按触发价由低到高、卖方由高到低读取；同来源同价位按独立 StopPriority 的递增顺序读取。StopPriority 与常规 QueuePriority 使用相同的高水位保护规则，但维护完全独立的市场本地计数器。

StopBook 同时保存订单位置索引与触发前到期索引。GTD 按 LogicalTime 到期，DAY 按交易时段到期；索引引用包含订单标识和止损价位位置，删除、取消或到期后会同步移除引用。参考簿故意使用 Vec 与稳定排序或扫描实现相同外部语义，避免复用生产 StopBook 算法。

每个可触发参考来源保存最近价格和严格递增的来源序号。首次更新只建立基线；价格上升按低到高收集跨越区间内的买方止损，价格下降按高到低收集卖方止损。收集时先从 StopBook 和到期索引删除，再按内部先进先出顺序执行 child。每个 child 先经过本地 Trigger-R1；拒绝时产生 `StopTriggered`、`TriggeredOrderRejected` 与 `ReservationReleased`，不会产生 Trade 或第二个 `CommandRejected`。通过预检的 child 复用常规限价或市价撮合入口；child 成交会更新最近成交价并继续收集后续止损。预留、费率和保护的具体状态由调用方提供的本地、可重放验证器检查，不访问网络或本机时钟。

生产簿将止损容器、取消和到期入口放在 `production_orderbook/stop_orders.rs`，将参考价跨越、Trigger-R1、child 执行和级联队列放在 `production_orderbook/stop_trigger.rs`。参考簿以同名子模块实现对应职责，但保留其 Vec 扫描与稳定排序，绝不调用生产 StopBook 或生产撮合实现。两个根模块只保留订单簿状态、构造和共享查询，使订单类型边界与限价、市价、冰山模块保持一致。

## 改单与预留模块边界

[amend_execution_context.rs](../crates/matching-orderbook/src/amend_execution_context.rs) 保存调用者固定的 MarketMath、命令身份、费率、公钥及候选资产需求，只处理共有的数值/资金值契约。各模型的 `amend_candidate.rs` 分别构建只读候选、数值预检与普通改单计划，并路由自身的冰山/止损动作；Production 的 PriceLevel 原位修改和 StopBook replacement 计划负责真实链接、聚合与到期引用，Reference 继续使用自身 Vec。

[领域改单协议](../crates/matching-domain/src/amend_order.rs) 定义绝对目标、patch 和 typed quantity；[预留令牌](../crates/matching-domain/src/reservation_adjustment.rs) 固定签名字节；[本地风控验证器](../crates/matching-risk/src/reservation_adjustment.rs) 负责已加载 Ed25519 公钥验签和 checked 版本应用。两本簿分别按 OrderId 保存 shadow reservation，身份 map 不决定业务顺序。完整核验使用 `amend_with_context`，低层改单入口要求调用者已完成数值与资金校验。

`tests/amend_atomicity.rs` 对完整物理状态进行拒绝前后比较，`tests/amend_reservation.rs` 覆盖真实签名与 StopLimit 接续，`tests/amend_iceberg.rs` 和 `tests/amend_stop.rs` 固定优先级与触发执行矩阵。无分支 getter、重导出和透明转发由这些消费者覆盖，不以逐方法镜像测试替代真实状态验证。
