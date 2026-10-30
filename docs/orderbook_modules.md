# 订单簿模块与代码入口

订单簿提供两套独立实现：生产簿使用价格索引、订单索引、Arena 和档位链表；参考簿使用 Vec 与排序。两者遵守相同业务契约，通过差分测试比较每条命令的原序事件、订单、深度和适用不变量。

## 文件与模块的关系

[库入口](../crates/matching-orderbook/src/lib.rs) 声明两个公开模块，并重新导出 `ProductionOrderBook` 和 `ReferenceOrderBook`。各自的 `.rs` 文件是父模块入口，同名目录保存由 `mod` 声明加载的子模块。

例如 `production_orderbook.rs` 中的 `mod iceberg;` 加载 `production_orderbook/iceberg.rs`。子模块中的 `impl ProductionOrderBook` 给同一个类型增加方法，不创建新的订单簿对象或状态副本。调用者继续通过订单簿实例调用原有公开方法。

模块根文件保存状态字段、构造、基础查询和跨功能使用的内部类型。业务方法按职责移动到子模块；功能独用的计划类型与使用它的实现放在一起。

同一实现块优先保留已有公开入口的阅读顺序；新增加的内部辅助方法集中放在已有方法之后，并按所属功能归组。基础结构优先展示构造和查询，相近的读写及链表操作相邻排列。调整顺序不改变方法签名、注释或执行行为。

## 实现入口

| 职责 | 生产簿 | 参考簿 |
|---|---|---|
| 类型、构造与基础查询 | [production_orderbook.rs](../crates/matching-orderbook/src/production_orderbook.rs) | [reference_orderbook.rs](../crates/matching-orderbook/src/reference_orderbook.rs) |
| 价格带配置、边界与静态校验 | [price_band.rs](../crates/matching-orderbook/src/production_orderbook/price_band.rs) | [price_band.rs](../crates/matching-orderbook/src/reference_orderbook/price_band.rs) |
| 挂单、取消 | [resting.rs](../crates/matching-orderbook/src/production_orderbook/resting.rs) | [resting.rs](../crates/matching-orderbook/src/reference_orderbook/resting.rs) |
| 单笔与连续撮合 | [matching.rs](../crates/matching-orderbook/src/production_orderbook/matching.rs) | [matching.rs](../crates/matching-orderbook/src/reference_orderbook/matching.rs) |
| 限价执行策略 | [limit_execution.rs](../crates/matching-orderbook/src/production_orderbook/limit_execution.rs) | [limit_execution.rs](../crates/matching-orderbook/src/reference_orderbook/limit_execution.rs) |
| 按基础数量执行市价单 | [market_by_base.rs](../crates/matching-orderbook/src/production_orderbook/market_by_base.rs) | [market_by_base.rs](../crates/matching-orderbook/src/reference_orderbook/market_by_base.rs) |
| 按报价预算执行市价单 | [market_by_quote.rs](../crates/matching-orderbook/src/production_orderbook/market_by_quote.rs) | [market_by_quote.rs](../crates/matching-orderbook/src/reference_orderbook/market_by_quote.rs) |
| 冰山可见片与补量 | [iceberg.rs](../crates/matching-orderbook/src/production_orderbook/iceberg.rs) | [iceberg.rs](../crates/matching-orderbook/src/reference_orderbook/iceberg.rs) |
| 普通订单 DAY/GTD 定时挂单、到期与 EndSession | [expiry.rs](../crates/matching-orderbook/src/production_orderbook/expiry.rs) | [expiry.rs](../crates/matching-orderbook/src/reference_orderbook/expiry.rs) |
| 未触发止损、触发前到期 | [stop_orders.rs](../crates/matching-orderbook/src/production_orderbook/stop_orders.rs) 与 [stop_book.rs](../crates/matching-orderbook/src/stop_book.rs) | [stop_orders.rs](../crates/matching-orderbook/src/reference_orderbook/stop_orders.rs) |
| 止损触发与级联 | [stop_trigger.rs](../crates/matching-orderbook/src/production_orderbook/stop_trigger.rs) | [stop_trigger.rs](../crates/matching-orderbook/src/reference_orderbook/stop_trigger.rs) |
| 改单候选、普通改单与完整上下文入口 | [amend_candidate.rs](../crates/matching-orderbook/src/production_orderbook/amend_candidate.rs) | [amend_candidate.rs](../crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs) |
| 冰山改单 | [amend_iceberg.rs](../crates/matching-orderbook/src/production_orderbook/amend_iceberg.rs) | [amend_iceberg.rs](../crates/matching-orderbook/src/reference_orderbook/amend_iceberg.rs) |
| 未触发止损改单 | [amend_stop.rs](../crates/matching-orderbook/src/production_orderbook/amend_stop.rs) | [amend_stop.rs](../crates/matching-orderbook/src/reference_orderbook/amend_stop.rs) |
| 撤单替换只读过滤视图 | [cancel_replace_view.rs](../crates/matching-orderbook/src/production_orderbook/cancel_replace_view.rs) | [cancel_replace_view.rs](../crates/matching-orderbook/src/reference_orderbook/cancel_replace_view.rs) |
| 撤单替换独立计划与原子提交 | [cancel_replace.rs](../crates/matching-orderbook/src/production_orderbook/cancel_replace.rs) | [cancel_replace.rs](../crates/matching-orderbook/src/reference_orderbook/cancel_replace.rs) |
| 分片本地批量撤单计划与原子提交 | [mass_cancel.rs](../crates/matching-orderbook/src/production_orderbook/mass_cancel.rs) | [mass_cancel.rs](../crates/matching-orderbook/src/reference_orderbook/mass_cancel.rs) |
| 接收登记表的正常下单预演与归属提交 | 由撤单替换计划内部完成，未新增通用生产 Place API | [reservation_place.rs](../crates/matching-orderbook/src/reference_orderbook/reservation_place.rs) |

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
| [expiry.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/expiry.rs) | LogicalClock、GTD、DAY、EndSession 与普通/止损到期的差分 tape、生成式场景 |
| [amend_regular.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_regular.rs) | 常规改单、绝对数量、事件原序与完整 TIF 迁移 |
| [amend_full.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full.rs) 及其 [scenarios.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full/scenarios.rs)、[tapes.rs](../crates/matching-orderbook/src/production_orderbook/tests/differential/amend_full/tapes.rs) | 全容器改单、真实签名、触发/到期、100k 与两组双次 10k 完整 transcript |

公共工具只统一测试观察和执行驱动；两本订单簿分别执行自己的撮合算法，比较时保留原始事件顺序。测试数据类型的字段保持私有，跨分组辅助函数只在差分测试范围内开放。

`proptest-regressions/` 保存性质测试曾发现并缩减的失败输入。测试运行时会先重放这些种子，再生成新样本；例如 `market_by_quote.txt` 固定了“较优价档不可支付、较差价档仍可支付”的报价预算边界。该目录是测试回归证据，应随源码保留，不参与生产运行。

阅读时先查看两种订单簿的字段和查询方法，再沿所需公开入口进入对应功能文件，最后查看该功能的单元测试与差分案例。基础参考撮合仍不自动挂入 incoming 余量，完整生命周期入口负责相应挂单；不能仅因两侧方法同名就忽略接口契约差异。

业务说明见 [限价订单执行约束](limit_execution.md)、[冰山订单](iceberg.md)、[挂单修改与本地预留授权](amend.md)、[原子撤单替换与预留归属](cancel_replace.md)、[数值计算公式](numerical_formulas.md) 与 [关键逻辑流程](logic_flows.md)。

批量撤单使用领域层的 `ShardCancelScope`，只按活动订单关联预留快照中的 `user_id`、`market_id` 与持久化的 `origin_session_id` 过滤；后者不是 DAY 的交易时段。两个订单簿各自扫描其真实活动容器，统一按 `OrderId` 升序冻结目标，在不可变计划中预演活动预留归属到 `PendingR2` 的迁移，并仅在订单簿和登记表前态未漂移时一次安装后态。Regular、冰山和未触发 Stop 均走该路径；取消会复用各自已有的容器及到期引用清理，既不触发 Stop child，也不补量冰山或分配/回退任何优先级。每个目标只产生 `OrderCanceled { reason: MassCancel }`；基础数量订单携带 `OrderRemaining::Base`，未触发的报价预算 StopMarket 携带当前 `OrderRemaining::Quote`。这只是当前执行单元的本地语义，不包含跨分片协调、围栏或真实 R2 资金释放。

撤单替换领域输入位于 `matching-domain/src/cancel_replace.rs`，新预留的规范签名字节位于 `reservation_token.rs`。`matching-risk/src/reservation_transition.rs` 持有真实登记表及不可伪造的金融/归属计划，区分 ActiveOrder 与 PendingR2；订单终结不伪装成金融释放。两套簿各自规划、核对完整前态后提交，不共享过滤、撮合或提交算法。`tests/cancel_replace/` 固定约束和原子性；`tests/differential/cancel_replace/` 保存持续历史的完整混合磁带和独立性质断言。

## 市场价格带

`PriceBandConfig` 固定价格来源和最大偏离基点，两种订单簿分别保存配置及对应的市场数值规则。双侧边界复用用户滑点保护的整数计算；非法基点统一返回 `InvalidSlippageBps = 3007`。限价候选必须位于包含端点的上下界之间；缺少配置来源参考价时返回 `ReferencePriceUnavailable`。市价撮合在命令开始冻结价格带，按方向与用户保护取更严格边界，并在第一个越界挂单之前停止；相等时允许成交。用户和市场边界同时绑定时来源为 `Both`，否则记录实际生效的一方。

普通成交更新 LastTrade，当前命令仍使用开始时冻结的价格带。参考价更新触发 Stop 时，相同配置来源使用该更新后的价格，不同来源使用原有配置参考；同一个根命令中的子单沿用冻结的上下界。未实现完整市场状态机，PreOpen 重校验与 Open/Resume 引导约束尚待后续实现。

## 自成交防护

领域模块 `matching-domain/src/stp.rs` 定义身份键、模式和纯决策函数。活动订单保存身份键与模式，止损触发的子订单和撤单替换的新订单携带各自输入的身份；改单沿用原订单身份。相同用户始终构成冲突，不同用户仅在非零身份键相同时构成冲突；实际动作只由 taker 的模式决定。`DecrementAndCancel` 当前在订单校验时拒绝；`StpMode` 的稳定 wire 数字尚未分配。

参考簿独立扫描自身向量，生产簿沿价格档位和队列扫描；两者均先判断价格及保护边界，再处理冲突。报价预算不足以在当前价格成交一手时，该档不可执行，不触发防护撤单。`CancelMaker` 移除挂单全部余量并继续，`CancelTaker` 终结主动单，`CancelBoth` 先输出 maker 撤单再输出 taker 撤单。冰山撤单包含隐藏余量。报价预算主动单的撤单事件以 `OrderRemaining::Quote` 表示实际剩余预算，不把预算转换为基础数量。FOK 先在只读候选中模拟全部动作，不足时不提交；撤单替换的扫描排除旧订单。

完整订单执行入口返回包含撤单与成交的原序事件；按基础数量执行的结果和限价 GTC 结果同时提供成交投影。仅能返回成交的底层逐笔接口在遇到启用防护的主动单时拒绝，以免丢失撤单事件。带登记表的下单、市价、改单、止损触发和撤单替换计划在候选副本中核验金融快照及订单归属，实际终结的身份迁入 PendingR2；这不是资金释放，也不会生成虚构的 `ReservationReleased`。

## 未触发止损订单

未触发止损订单不进入常规价格档位、Arena 或常规订单索引。生产簿将它们放在独立 StopBook，档位键为触发价与来源：买方按触发价由低到高、卖方由高到低读取；同来源同价位按独立 StopPriority 的递增顺序读取。StopPriority 与常规 QueuePriority 使用相同的高水位保护规则，但维护完全独立的市场本地计数器。

StopBook 同时保存订单位置索引与触发前到期索引。GTD 按 LogicalTime 到期，DAY 按交易时段到期；索引引用包含订单标识和止损价位位置，删除、取消或到期后会同步移除引用。参考簿故意使用 Vec 与稳定排序或扫描实现相同外部语义，避免复用生产 StopBook 算法。

每个可触发参考来源保存最近价格和严格递增的来源序号。首次更新只建立基线；价格上升按低到高收集跨越区间内的买方止损，价格下降按高到低收集卖方止损。收集时先从 StopBook 和到期索引删除，再按内部先进先出顺序执行 child。每个 child 先经过本地 Trigger-R1；拒绝时产生 `StopTriggered`、`TriggeredOrderRejected`，不会产生 Trade 或第二个 `CommandRejected`。旧的不含登记表入口仍暴露历史 `ReservationReleased` 事件；登记表入口遵循 PendingR2 契约，不产生该事件。通过预检的 child 复用常规限价或市价撮合入口；child 成交会更新最近成交价并继续收集后续止损。预留、费率和保护的具体状态由调用方提供的本地、可重放验证器检查，不访问网络或本机时钟。

生产簿将止损容器、取消和到期入口放在 `production_orderbook/stop_orders.rs`，将参考价跨越、Trigger-R1、child 执行和级联队列放在 `production_orderbook/stop_trigger.rs`。参考簿以同名子模块实现对应职责，但保留其 Vec 扫描与稳定排序，绝不调用生产 StopBook 或生产撮合实现。两个根模块只保留订单簿状态、构造和共享查询，使订单类型边界与限价、市价、冰山模块保持一致。

## 改单与预留模块边界

[amend_execution_context.rs](../crates/matching-orderbook/src/amend_execution_context.rs) 保存调用者固定的 MarketMath、命令身份、费率、公钥及候选资产需求，只处理共有的数值/资金值契约。各模型的 `amend_candidate.rs` 分别构建只读候选、数值预检与普通改单计划，并路由自身的冰山/止损动作；Production 的 PriceLevel 原位修改和 StopBook replacement 计划负责真实链接、聚合与到期引用，Reference 继续使用自身 Vec。

[领域改单协议](../crates/matching-domain/src/amend_order.rs) 定义绝对目标、patch 和 typed quantity；[预留令牌](../crates/matching-domain/src/reservation_adjustment.rs) 固定签名字节；[本地风控验证器](../crates/matching-risk/src/reservation_adjustment.rs) 负责已加载 Ed25519 公钥验签和 checked 版本应用。两本簿分别按 OrderId 保存 shadow reservation，身份 map 不决定业务顺序。完整核验使用 `amend_with_context`，低层改单入口要求调用者已完成数值与资金校验。

`tests/amend_atomicity.rs` 对完整物理状态进行拒绝前后比较，`tests/amend_reservation.rs` 覆盖真实签名与 StopLimit 接续，`tests/amend_iceberg.rs` 和 `tests/amend_stop.rs` 固定优先级与触发执行矩阵。两个 `amend_candidate.rs` 的终结路径回归固定空簿、取消、全成与到期后的 `OrderNotFound (2010)`，并与活动订单缺失 shadow 的 `InvalidReservation (4002)` 对照。无分支 getter、重导出和透明转发由这些消费者覆盖，不以逐方法镜像测试替代真实状态验证。

## 常规订单的逻辑时间与会话到期

常规订单的 TIF 存在订单运行态中。生产簿以 `BTreeMap<LogicalTime, BTreeSet<OrderId>>` 和 session 索引维护二级引用；参考簿独立扫描 Vec，并显式排序。两侧的 GTD 输出固定为 `(expire_at, order_id)` 顺序，DAY/EndSession 输出固定为 `order_id` 顺序。取消、完全成交和到期均同步删除二级引用，避免遗留到期记录；到期不会重排或消耗 QueuePriority。
