# Matching Engine Engineering Rules v6

> 2026-10-28 当前交付：用户已授权并完成 T13 Atomic CancelReplace 的最终验收、文档/交接更新及本地提交；实现、分步审查、独立总复审和最终本地 gate 已通过。PendingR2 用户裁定取代初始任务书的提前释放：金融 Active 与 ActiveOrder/PendingR2 归属分离，终结仅迁移归属，不发 release；真实 R2 及 T14+ 不在范围内。当前授权不包含合并或推送；实际验证与交付事实以唯一交接为准。

> 2026-10-26 当前事实：T12 Amend 已由 `d694974` 与 P2 收尾提交 `b763984` 合入并推送至 `origin/main`；本地完整验证已经通过。远端 CI/GitHub Actions 与 GitHub 页面实际公式渲染尚未在本轮核验。`codex/t12-amend` 已删除。

> 2026-10-23 交付事实：用户已授权将已验证的订单簿模块整理提交、合并到 `main`、删除开发分支并推送。实现提交为 `91d13bc71488db346fe1f9d6afbd61404fa367ac`，已快进合入并推送至 `origin/main`；`codex/orderbook-module-layout` 已在本地删除，且从未存在远端同名分支。随后 T09 已由 `77761e010cc10179dfc566181f0a3b1397929f78` 合入并推送至 `origin/main`，开发分支已删除。

> 2026-10-22 历史交付授权：用户要求更新交接等文档、标记 T08 完成，然后提交代码并推送远端。T08_P3_iceberg 已完成本地验收，该次交付包含冰山实现、测试、文档和 T00–T08 中文注释修订，交付分支为 `codex/t08-iceberg`；该授权不扩展至本轮模块整理的提交或后续业务任务。

> 2026-10-21 历史授权：用户明确“解除导师模式，帮我完成T07”。本次 T07 改为直接实现与审查模式，允许 Codex 修改 Rust 行为/API/测试并完成 gate；下文旧导师限制及逐步等待用户实现的流程对本次授权已 superseded。其余工程约束与保留用户改动要求仍有效；历史 T07 的范围限制已由当时的 T08 授权取代。

## 角色与授权边界

当前采用直接实现和 Code Review 模式。T08、订单簿模块整理及 T09–T12 均已交付；T13 已在当前独立执行任务书和 PendingR2 裁定范围内完成实施、验证与审查并创建本地交付提交，未获合并或推送授权。远端 CI/GitHub Actions 与 GitHub 页面实际公式渲染仍未在本轮核验；不扩展 T14+。

- 可按当前任务规格新增、修改 Rust 行为、接口、注释和测试；保留用户已有改动。
- 仍须完成失败原子性、数值约束、独立参考模型与 gate，不以弱化测试或 lint 换取通过。
- 可维护任务相关文档及唯一 `assignment/SESSION_HANDOFF.md`；修改数值/事件协议须同步契约与边界证据。
- 不把其他任务或未来范围顺手实现，不在没有明确授权时提交、推送或发布。历史提交、合并和推送授权不扩展到后续业务任务或新的变更。

## 当前范围与文档入口

**订单簿模块整理已交付**；实现提交 `91d13bc` 已在 main 上，实施计划见 [模块整理计划](assignment/ORDERBOOK_MODULE_LAYOUT_PLAN.md)。T09、T10、T11 与 T12 已分别由 `77761e0`、`9e23e45`、`36d4a4d`、`d694974`/`b763984` 交付并推送至 `origin/main`。T12 保留 T11 的 LogicalClock/DAY/GTD/EndSession、此前数值、保护、失败原子性和 differential gate；远端 CI/GitHub Actions 与 GitHub 页面实际公式渲染仍未在本轮核验。

**T08 已完成本地验收（2026-10-22）**。2026-10-22 本地验收通过：workspace 538 项单元测试（domain 143、orderbook 395）及 2 项 doctest；完整 differential 52 项、fmt、严格 workspace Clippy 均通过。包含冰山固定 seed 100k 混合命令与两组双次 10k 重复 tape。 注释、12 张流程图与 116 项公式已审查；公式通过 GitHub 实际渲染组件验证；提交后的页面与远端 CI 状态须另行核验。中文注释专项修订覆盖 43 个 Rust 文件、3,200 处注释，非注释代码保持一致。交付分支为 `codex/t08-iceberg`，提交与推送事实见唯一交接。

T08 覆盖 visible/hidden/total 守恒、仅 visible depth、maker 补量时新 QueuePriority 与同价队尾、部分成交事件使用 total、Cancel 清除全部余量，以及已有 Limit/IOC/FOK/Market 对冰山 maker 的一致行为。FOK 只读预检须包含可补量流动性与 priority/算术失败，禁止成交后回滚。完成前运行定向、完整 differential/orderbook、workspace gate，维护公式与流程说明。

**T07 历史授权与验收**（2026-10-16 用户明确从已验收的 T06 切换）。T00–T07 已验收；T02 提交为 `161abf7`，T03 为 `f96e81b`，T04 为 `c860543`，T05 为 `e752fdb`，T06 提交及 T07 启动基线为 `a57b8d89be86f13368f2db406088bbc7a604363d`。T07 已于 2026-10-21 完成本地验收：Limit IOC、Limit FOK、Limit GTC + PostOnly 及独立 reference/differential/property 验证；workspace 495 unit + 2 doctest、fmt、严格 Clippy 通过。交付分支为 `codex/t07-ioc-fok-postonly`，远端 CI 未核验。MarketByBase + FOK 按 §8.2 的 P3 可暂禁选项处理；MarketByQuote + FOK 始终禁止。2026-10-21 用户解除导师模式并授权 Codex 直接实现 T07；该历史限制已由本轮 T08 授权取代；T09 及其他后续任务仍未授权。

每轮开始先读取：

1. 本文件及 [CODEX_GUIDANCE_RULES.md](assignment/CODEX_GUIDANCE_RULES.md)。
2. [SESSION_HANDOFF.md](assignment/SESSION_HANDOFF.md)，它是唯一交接摘要。
3. [v6 规格](assignment/rust_cex_matching_engine_development_spec_v6.md) 的 §8、§14–§17、§54（重点不变量 12、13、18）、§64；按当前小步核对 §9、§13、§45–§46.1、§58，保留 §6.5.1–§6.5.2、§26.3–§26.5、§55.2–§55.4、§63 的数值/保护和回归要求。保留当前适用不变量 1–13/16–18/26/28–30/49–52/54–56；§54.53 及 §18/§27 reservation 仅作后续资金接口契约。始终遵守 §77 和 Appendix E；读取未来契约不扩大实现范围。
4. 当前 [T13](assignment/tasks/T13_P4_cancel_replace.md) 原始任务书、用户独立执行任务书及 `.superpowers/sdd/2026-10-26-t13-cancel-replace/` 的 progress/PendingR2 裁定；用户新授权优先于原始导师模式和提前释放文字。保留已验收的 [T12](assignment/tasks/T12_P4_amend.md)、[T11](assignment/tasks/T11_P4_logical_time_day_gtd.md)、[T10](assignment/tasks/T10_P4_stop_trigger_cascade.md)、[T09](assignment/tasks/T09_P4_stopbook_expiry.md)、[T08](assignment/tasks/T08_P3_iceberg.md)、[T07](assignment/tasks/T07_P3_ioc_fok_postonly.md)、[T06](assignment/tasks/T06_P3_market_orders.md) 与 [T05](assignment/tasks/T05_P2_differential_invariants.md) 的契约和验证门。
5. 当前相关源码、配置、测试、git status 和 git diff；新增未跟踪文件也必须读取。

任务索引在 [assignment/README.md](assignment/README.md)，不是 `assignment/tasks/README.md`。当前 assignment 被 Git 忽略；不能假定它已提交或在新 clone 中存在。资料缺失时明确记录，不凭记忆重建仓库完成状态，不另建第二份权威 handoff。

T00 已建立六个 crate 的最小骨架、固定工具链、CI 和工程规则。T01 已建立领域类型，T02 已在 matching-orderbook 中建立 `Vec + sort` ReferenceOrderBook：rest、双向 Limit GTC 成交、Cancel、TradeEvent、价格优先/FIFO、重复 command tape 及数量守恒/取消性质测试。Incoming 余量不会自动 rest，完整生命周期事件/命令协议尚未实现。保留参考模型作为后续 differential 基线，不重写已验收类型。

T03 已建立 OrderIndex、OrderArena slot/free-list、PriceLevel head/tail/count/total_visible_qty、intrusive 双向 FIFO 和 market-local QueuePriority allocator/high-water guard，并通过结构 unit/edge/property 与 O(1) 尾插访问计数验证。T04 复用这些结构，组合 BTreeMap 双侧价格索引、HashMap<OrderId, OrderIndex> 与 Arena，实现 best price、按 ID 定位/Cancel、双向 Limit GTC 多档撮合、maker 移除和余量挂单。链表顺序是 FIFO 权威；槽位释放/复用必须与全部 ID/链表引用同步。每次 book mutation 完成后检查 active index、聚合和 uncrossed 等适用不变量，不将 T04 自身的行为/失败原子性/不变量测试延期。

T05 已以独立流程适配对比 T02 Reference API（其 incoming 余量不会自动 rest）和 T04 完整 GTC API，验收可复现 100k command tape 与当前适用不变量。T06 已验收的 Market incoming 不 rest、不分配 QueuePriority；当前冰山 maker 补量会取得新 priority；quote budget 严格正且不含手续费，spent<=budget，unused_quote 不是成交。所有 notional/quote→qty 换算走统一 checked MarketMath；向下取整到合法 lot，本档不能成交一个 lot 时立即终止，不构造 Qty(0) 或零量 Trade。保护参考在命令开始捕获并固定；bps 只允许 0..=9_999，Buy 对 reference×(10_000+bps)/10_000 双重向下取整，Sell 对 reference×(10_000-bps)/10_000 双重向上取整，最终边界按方向与显式 worst price/market band 取更严格者。边界比较包含等号；首个越界 maker 不成交并终结命令。reservation/fee release 只保留后续接口契约，不伪造已实现的资金闭环。

T07 的 IOC 尽可能成交、余量取消且永不入簿；FOK 在任何 mutation 前完成可执行流动性/价格约束及当前适用算术预检，不足时零成交、簿与 priority 不变，禁止先成交再回滚；PostOnly 在改簿前检查是否会作为 taker，交叉包含等价价格，交叉时拒绝，成功实际 rest 时才分配 priority。PostOnly 禁止与 Market/IOC/FOK 组合。Iceberg、StopBook/确定性触发级联、DAY/GTD/EndSession 及 T12 Amend 已交付；CancelReplace 属于当前 T13，支持矩阵见 docs/cancel_replace.md。HA、完整后续命令协议及 runtime/persistence/protocol 业务仍在后续任务；不创建 gateway/HA/marketdata 组件，不引入 raw pointer/unsafe。

## 固定工作循环

1. 读取规格、交接和真实仓库状态，保留用户已有改动，明确任务范围与验收证据。
2. 按依赖拆分实施步骤，先建立有意义的失败测试，再完成独立 Reference/Production 实现。
3. 审查真实 diff、调用链、数值与状态不变量，运行影响范围内的测试和 gate。
4. 发现问题立即修订；当前任务完整 gate 通过前不标记完成或进入下一任务。
5. 每轮结束更新唯一 handoff：Overall Goal、当前 Task、仓库事实、完成证据、测试状态、决策、不变量、TODO、Blocker、Deferred 和续接点。

## 全局工程约束

v6 §77 的全部工程规则是审查依据。以下为常用约束摘要，不替代当前任务相关章节中的完整协议和边界条件，也不授权提前实现那些功能。

### 文档与流程图

- 所有手写代码注释必须使用中文说明，包括模块文档、公开/私有接口文档、普通注释、行尾注释与测试注释。不得保留英文说明句或用英文业务词替代中文叙述；类型、函数、字段、协议枚举、路径、公式变量及可执行示例保留准确标识符。自动生成文件不手工翻译。
- 注释验收须覆盖全部当前源码与测试，单独检查语言和实际语义；Rustdoc 编译成功不等于中文规范通过。纯注释修改须验证非注释代码与可执行文档示例未变，再运行格式、Rustdoc 与文档测试检查。
- 流程图的标题、说明和图内节点只描述实际职责、输入、状态转换和验证边界；不得使用 `T00`、`T05` 等任务/阶段编号作为流程图标识。任务编号仍可用于任务书、交接、提交记录和范围管理文档。
- 涉及计算公式时，必须维护 [docs/numerical_formulas.md](docs/numerical_formulas.md)：公式单独记录，使用 TeX，按通用业务职责组织；该文档不得出现任务或阶段编号。Codex 负责维护此文档，并在公式契约改变时同步规格、实现说明和测试边界。
- 公式文档面向业务读者，不写编辑规范、维护流程或“不得使用任务编号”等元说明。以 GitHub 页面为显示目标：块公式使用 `math` fenced code block，行内公式采用 GitHub 支持的美元符号包裹反引号代码片段，保护 TeX 不被 Markdown 转义。验收须检查闭合、实际 TeX 解析及渲染，不能只数定界符；本地预览通过不等于已推送后的 GitHub 页面通过。 比较符使用 `\lt`、`\gt`，避免裸尖括号被 GitHub 的 HTML 转换截断；不使用已证实被禁的 `\operatorname`。验证须覆盖 GitHub 实际渲染组件的宏检查与 HTML 转换，并逐项比较公式符号和结构，防止无报错的内容丢失。

### 数值、时间与确定性

- Correctness / determinism 优先于性能。
- price、qty、fee、balance、notional、settlement 禁止 f32/f64；价格、参考价、tick、qty、lot、quote budget 等 canonical 值按规格验证严格正值。
- 所有算术使用 checked math；失败必须显式处理，不 silent wrap、截断或隐式降级。notional 使用统一 MarketMath checked helper。
- 数值协议遵守 v6 §6.5.1–§6.5.2/§46.1：price/qty/quote 各有独立 scale；先消公共幂，配置须保证最小 tick×lot 本金精确且可表示。本金不取整，fee policy 单独处理；数值溢出仍显式失败。T06 逐步覆盖不变量 50–52，53 的资产映射/Settlement 守恒只作后续资金契约，不能宣称当前已验证。
- 撮合业务不依赖 wall clock；只有 durable AdvanceTime 驱动 LogicalClock。客户端时间和接收时间仅用于审计/指标。
- 状态变化必须可确定性重放；HashMap 迭代顺序、随机性不得影响业务结果、事件顺序或 digest。

### 所有权、消息与依赖

- OrderBook 恰有一个 writer；不以 Mutex/RwLock 掩盖 ownership 错误。
- OrderBook、Matcher、Amend、Stop trigger 热路径不访问网络、数据库或远程 fee/balance RPC。
- 队列必须有界，并按阶段定义背压与拒绝语义。
- 仅 ShardMessage::Mutation 分配 shard_seq 并进入业务 WAL。Query、TakeSnapshot、ComputeDigest、DrainBarrier 不占 durable seq、不写业务 WAL、不 replay。
- market-routed mutation 在分配 seq 前验证 route_epoch；要求 primary fencing 的操作同样 pre-seq 校验。
- 核心撮合不依赖 Tokio；Tokio 只用于规格允许的外围组件。依赖按需引入，Cargo.lock 固定解析结果，Codex 不自行升级依赖。
- P14 显式性能阶段及 benchmark 证据 gate 前禁止 unsafe。不得提前引入 SIMD、ART、自定义 allocator 或 Appendix E 列出的优化/扩展。

### 订单和状态不变量

- PriceLevel 链表顺序是 FIFO 权威；迁移后不能按 shard_seq 重排。QueuePriority 与 StopPriority 是各自独立的 Market-local 单调状态，随市场迁移，必须在 wrap 前停止分配。
- 同价减量 Amend 保优先级；增量/改价按规格失去优先级；Iceberg replenish 移到档位尾部。StopPriority 与 regular QueuePriority 不混用。
- Amend 在任何改簿前完成校验和 reservation 授权；失败保持原状态。成功保留 OrderId 并发出 OrderAmended。
- CancelReplace 在删除旧单前验证资金及替换单；簿相关预检使用 BookWithout(old_order_id)。ReservationAdjustmentToken 必须绑定预先分配的 CommandId 并验证版本/密钥约束。
- FOK 改簿前预检可执行量和保护条件；PostOnly 不得作为 taker 成交。
- Stop 先移出 StopBook/expiry 再触发 child；child 通过 Trigger-R1 才可成交，只有实际 rest 才分配 QueuePriority。Stop child 拒绝使用规定事件，不生成第二个 CommandRejected。
- DAY/GTD、PreOpen 重校验、STP、价格保护和资金预留严格按规格；不得因自成交拦截生成真实 Trade/SettlementDelta。FeeSchedule 为 durable/versioned 状态。
- Reason enums 是稳定数值协议契约，不临时发明未文档化字符串或 payload 字段。

### 持久化、分片与故障

- 不忽略 WAL、EventJournal、replication、CursorStore、ControlStore lease、manifest 错误；不跳过损坏的中间 WAL 记录。corruption/fencing/digest/invariant 错误按规格 fail-closed。
- ManifestManager 是 manifest 唯一写者；Snapshot/GC/segment 代码提交 intent，不自行改文件。
- StandbyDurable 不可用时停止/拒绝 mutation，不自动降级。GroupCommit 只推进 standby-durable contiguous prefix。
- Standby 在 promotion 前不得发布业务事件；promotion 获取 publisher_epoch，从 durable CursorStore 恢复游标，旧 primary 本地游标不作为权威。
- EventJournal GC 受 durable sink cursor checkpoint 约束，不采用内存 ACK 水位。
- 跨 shard User/Global control 走 durable ControlCoordinator + ControlStore；fence 与可选 cancel 为原子 shard mutation。任一 shard 已提交 fence 后不回滚，恢复必须继续完成。
- User FENCING 在 pre-seq 拒绝 Place/Amend/CancelReplace，Cancel/Query 仍允许。
- canonical market_event_seq 与各公开 feed_seq 分开；不以 source seq 检测过滤后 feed 的缺口。迁移保持 source/feed 连续性。

## 自动检查范围与依赖审查

根 [Cargo.toml](Cargo.toml) 统一声明 lint，六个成员通过 `[lints] workspace = true` 显式继承。以后添加成员时也必须检查继承，不能假定自动生效。

| 规则 | 等级 | 自动检查范围与局限 |
|---|---|---|
| `rust::unsafe_code` | `forbid` | 拒绝继承此规则的本项目代码中的 unsafe；不能在内部用 allow 降级。它不证明第三方依赖内部没有 unsafe。 |
| `rust::unused_must_use` | `deny` | 拒绝直接忽略 must-use 结果；显式丢弃、错误吞掉或错误映射是否正确，仍需 Review。 |
| `clippy::float_arithmetic` | `deny` | 拒绝浮点算术；不全面禁止浮点字段、常量、转换或所有浮点 API，金融类型仍须人工审查。 |
| `clippy::disallowed_methods` | `deny` | 按根 [clippy.toml](clippy.toml) 拒绝已列明的 API；当前禁止标准库无界 channel 构造函数 `std::sync::mpsc::channel`。它不证明其他队列都有界。 |

Clippy 规则在运行 Clippy 时检查，不能用 `cargo check` 代替。即使这些规则全绿，也必须审查 checked arithmetic、业务时间、HashMap 迭代顺序、单写者所有权、背压和错误语义。不得通过局部 allow、降低等级或更换等价 API 来规避规则；合法范围调整需说明原因并按当前 Task Review。

每次新增或更新依赖，Review 中必须记录：

1. 当前 Task 的具体需求、所属 crate，以及为什么现有代码/标准库不足以满足需求。
2. 启用的 features（含 default features）和 lockfile 中新增/变化的传递依赖。
3. 是否引入核心路径网络/数据库/异步运行时、wall clock、随机性、无界队列或其他确定性风险；依赖内部 unsafe 的情况不能由本项目 lint 推断。
4. 队列 API 的容量、满队列行为与背压策略；引入新队列库时同步审查并扩充 disallowed-methods，而不是把未列入名单当作允许无界队列。

保持当前 Task 按需引入依赖，不为未来预装，不自行升级依赖。T07 启动基线：matching-orderbook 已有 matching-domain 本地 path 依赖及仅用于测试的 proptest 1.11.0（默认 features，审查见 handoff 的 T02.15 记录）；其余五库无依赖。T07 首步不新增依赖，保留 Reference 的独立 Vec 模型及 Production 的 BTreeMap/HashMap/Arena/PriceLevel，不复用生产撮合逻辑来生成参考答案。不为了配置禁用列表而引入 Tokio/Crossbeam；禁用的标准库无界 API 不代表现在要实现 channel/runtime。

lint 配置或继承发生变化时，用独立临时副本/探针验证预期诊断与合法对照，不在业务源码保留违规探针，不把空库检查通过当作规则拦截证据。

## Review 与验证

每次 Review 检查：状态不变量、checked arithmetic、wall clock、非确定性迭代、FIFO/QueuePriority/StopPriority、replay、错误处理、I/O crash boundary、所需测试和是否越界到下一 Task。不涉及的项说明不适用，不伪造验证。

- 测试按风险保留证据，而非按每个方法机械增加。状态变更、分支选择、拒绝/失败原子性、checked 算术边界、价格/FIFO/确定性顺序、稳定 wire 编号与事件 payload 必须有针对性的 unit/edge 测试；Task/规格要求的 property、differential、command tape、replay、crash 测试同样不能用人工 review 替代。
- 纯值对象 derive、透明包装、crate re-export、无分支委托、构造/只读 getter，或与已有测试断言完全相同的 fixture，可以不新增单元测试，或与代表性测试合并。用户完成实现后，Codex 必须直接审查真实 diff、调用链和类型约束，并运行适用的定向测试、fmt 与受影响 crate 的 Clippy/test；workspace gate 仅在 Task 最终验收、构建配置/共享依赖变更，或 diff 已触及跨 crate 调用链时重跑。Review/Handoff 必须写明未单测项为何属于简单逻辑，以及哪项保留证据覆盖其行为。
- 已有历史 suite 只有在其测试文件、被测实现、依赖配置和调用链都未被当前 diff 触及，且 handoff 已记录可复核的通过证据时，才可不在当前小步重跑。不得把“测试文件未修改”当作跳过实际被修改行为路径的理由；一旦触及 Reference/Production、共享 domain API、状态不变量、依赖或构建配置，按影响面补跑相应 regression、differential/property 或 workspace gate。
- 清理既有测试时先列出“删除/合并项 → 仍保留的核心证据”映射；不得因减量移除 T02 规定的 buy/sell、exact/partial、multi-maker、cross-price、empty book、duplicate ID、cancel、同价 FIFO、确定性和适用性质验证。
- 持久化变更需 crash/truncation 测试；快照变更需 replay digest 测试。
- publisher epoch/cursor、迁移 source/feed 连续性、PreOpen 重校验等不变量在对应阶段加入 debug/test assertions。
- fmt 和默认 Clippy 不能替代全部业务语义审查，特别是 float、unsafe、无界队列和确定性约束。
- 纯文档小步检查内容、路径、diff 和范围，不为此新增 Rust 测试；实现或构建配置变化运行相关检查。

每个 Task 最终完成前至少实际运行：

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

任务书更强的 gate 同样必须满足；T04 先运行 matching-orderbook 定向测试，若变更 domain 契约也运行 domain 定向测试。T04 必须覆盖双侧 best price、空簿/单侧、按 ID 查询及 Cancel、头/中/尾取消与最后一单清档、slot 复用后索引一致、双向 exact/partial/multi-maker/cross-price/FIFO 成交和 GTC 余量挂单、重复 ID/checked 失败/priority high-water 边界，以及 mutation 结束后 active index/链表/聚合/uncrossed 一致性。order lookup 平均 O(1) 需有 HashMap 访问路径证据，不能把价档 O(log P) 删除描述成整个 Cancel 严格 O(1)。保留 T02 command tape/property 与 T03 结构/property/O(1) 尾插访问计数回归。无浮点金融类型、checked arithmetic 显式失败和方向独立数值排序仍须审查。引入的 reason 枚举按 §58 保持显式稳定编号并补 golden wire tests，不提前实现全量协议。任一 gate 未过，记录 blocker，不宣称 Task 完成，不进入下一 Task。

“完成”必须有文件、diff、命令退出码/测试结果、benchmark 结果或 commit hash 等证据。区分计划与实现、本地检查与远端 CI、零测试通过与业务正确性；历史结果必须标明验证时点。

T05 先运行 differential 定向及完整 matching-orderbook 回归，再运行上述 workspace gate。验收必须包含至少 100k 随机命令、可复现 seed/tape、逐命令的结果/有序 TradeEvent/open orders/depth 比较和当前适用 invariant assertions；不能仅比较终态或将事件排序后比较。适用范围为 §54 的 1–5、7–11、26、49，以及 16/28/29 的当前单市场部分；6 的 Stop/expiry、28–30 的 Stop 分支和 16 的跨 shard 全局唯一性不伪造验证，普通 GTC 只在实际 rest 时分配 priority 的规则仍须验证。未来 Stops/Expiry/digest 比较点记录为待扩展契约，不用恒空数据冒充已验证。

T05 最终本地验收证据为：21 项 differential（固定 seed 单条 100k、两组固定 seed 双次 10k、逐命令原序事件/状态/不变量）、229 项 matching-orderbook 单测、workspace 299 项单测和 2 项 doctest，以及 fmt、严格 Clippy、普通/cached diff check 全部 exit 0。此证据不覆盖 Stop/expiry、跨 shard TradeId、WAL/digest/crash 或远端 CI；它们只能在对应范围任务中实现和验证。

T06 已于 2026-10-16 本地验收：固定 seed 100k streamed differential、workspace domain 139 / orderbook 316 单测及 2 doctest、fmt、严格 Clippy 和普通/cached diff check 均通过。此为历史验证证据，不代表 T07 或新的远端 CI 已通过。

T07 每个小步先运行当前定向测试；domain 契约变化须运行 matching-domain 全量，Reference/Production 或其公共调用链变化须重跑相应 differential、完整 matching-orderbook 和必要 property；跨 crate 调用链变化运行 workspace gate。未受当前 diff 影响的历史 suite 可引用 handoff 的最近通过证据，不机械重跑；Task 最终验收必须完整运行 differential、matching-orderbook 与 workspace gate。须覆盖 IOC 双向价格/FIFO/empty/partial/余量终结与无 priority 消耗，FOK exact/multi-level/不足/价格不可执行/失败原子性及 all-or-zero property，PostOnly 双向等价交叉拒绝、非交叉挂单、拒绝零 mutation 和 never-taker property，以及固定 seed 重复 tape 原序事件/逐命令状态对比。保留 T02–T06 regression 和当前适用不变量；本段为 T07 历史 gate，T08 已额外加入 §64 Iceberg 验证。
