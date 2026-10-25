# T12 Task 7：未触发 Stop Amend 实施报告

> 最新状态：修复轮 1 scoped re-review 后的级联保护修复已实施，证据见末尾“修复轮 2/5”。
> 前文的 trigger-reference/protection deferred 边界与 DAY/GTD 仅模板状态已由该修复取代；
> 尚待父执行会话 scoped re-review，不能把首轮报告解读为最终验收结论。

## 范围与读取基线

本小步基于 `codex/t12-amend` 的 Task 0–6 未提交工作区，保留全部既有变更。
读取执行计划、Task 6 报告与独立审查、两模型候选/Stop/触发/到期调用链、领域数量与补丁协议、
真实 status/diff 和新增未跟踪实现。当前 worktree 缺少 ignored `assignment/`，因此读取主 checkout
`D:/rust_workspace/tide-matching-engine/assignment/` 的工作规则、唯一交接、T12 历史任务书及
v6 §20。用户冻结的零余量拒绝及事件表示由唯一交接和执行计划确认，沿用当前直接实现授权，
不恢复历史导师限制。本报告交给父执行会话维护唯一 handoff，不建立第二份权威交接。

本小步只实现 Stop child/触发价/触发前到期修改及相应失败原子性，并修复由测试证实的
生产聚合到期 shadow 清理遗漏。没有进入 Task 8 的 reservation 验签/覆盖量/apply 或冰山改单；
没有提交、推送、合并、依赖或领域协议变更。

## RED → GREEN 证据

1. 先写三个固定矩阵测试：child 数量/限价/TIF 修改保持 FIFO，触发价改变进入已有目标档尾部，
   quote budget 修改保持数量模式及 StopPriority。首次编译指出两模型缺少公开
   `amend_stop_with_events`。只建立返回 `InvalidOrderCombination` 的入口后，
   `cargo test -p matching-orderbook --locked amend_stop --lib --quiet` 实际 exit 1，
   0 passed / 3 failed：三个合法改单均因缺少行为遭拒绝。随后才实现两条业务路径。
2. 两个 child 测试转绿；触发价迁移的 GTD 真实聚合到期测试仍失败：订单已删除，
   `ProductionOrderBook::reservation_state` 却保留旧状态/version 11。
   新增九组触发前到期迁移矩阵同样复现残留，合计 3 passed / 2 failed。
3. 单独建立 DAY 聚合到期回归，运行
   `cargo test -p matching-orderbook --locked amend_stop_day_aggregate --lib --quiet`，
   exit 1、0 passed / 1 failed，同样在终结后残留影子状态。
4. 把 `expire_gtd_through` / `end_session` 的 Stop 分支从直接调用 StopBook 改为调用既有
   `expire_stop_gtd_through` / `expire_stop_day_session`，复用其终结清理。上述 6 项测试全绿。
   改动不触及输出排序和 reason；GTD 仍按时间/ID，DAY 仍按 ID 输出。
5. 补充完整候选纯度、高水位、真实 child 触发、Base 市价模式和 child TIF 矩阵；
   这些测试首先运行即通过已实现行为，不虚报独立业务 RED。过程中修正了一处
   QuoteBudget getter 名称的编译错误与一次测试模块位置的 Clippy 诊断；二者不是业务 RED。

## 实现与提交边界

- `reference_orderbook/amend_stop.rs` 独立扫描自身 Stop 源 Vec，复制并验证候选载荷，生成
  `ReferenceStopAction`。它包含源位置、完整新载荷、保留/重排决策、预定优先级、下一计数器及事件。
  全部时效、模式、缺失 shadow、未接入 token 和高水位失败均在不可变规划中处理。
  提交只替换原位置，或删除后追加源 Vec；到期继续按其自身载荷扫描。
- `production_orderbook/amend_stop.rs` 独立从生产 StopBook 读取候选并构造 child 模板与事件；
  `StopBook::plan_replacement` 校验 child TIF、触发前到期和分配器可行性，生成完整拥有载荷的
  `StopReplacementPlan`。`commit_replacement` 保持原位置或移入新触发价档尾部，并迁移位置与到期引用。
  提交入口不返回普通业务 `Err`；唯一 writer 的已预检分配及结构定位以不变量断言保障。
- 两模型分别调用自己的候选路径、运行态和提交逻辑，没有把生产输出输入参考模型，也没有共享
  业务状态迁移 helper。共享领域数量模式校验只定义公共协议。
- 纯 child 数量增减、child 限价/TIF、触发前有效期和相同触发价均保留 StopPriority 与原位置。
  实际改变触发价才消耗一个新 StopPriority 并进入目标档尾部；未触发改单始终不分配 QueuePriority。
- 成功只发出一个 `OrderAmended`；StopLimit 的 price 是 child 限价，StopMarket 为 `None`，
  数量通过 `BaseRemaining` / `QuoteBudget` 保留类型，`priority_retained` 对应 StopPriority。
  本步骤没有订单成交，也不把 StopPriority 复制成后续 child 的 QueuePriority。
- `new_qty` 对未触发 Stop 没有已成交量，直接表示 child 严格正目标总量。quote 模式只接受
  `new_quote_budget`，显式拒绝零预算、模式切换和市价 child 限价字段。

## 测试与完整状态证据

新增 11 项测试，包括 10 项生产模块下的真实双模型/生产验证和 1 项参考模型本地测试：

- 双向中间订单 child 减量、增量、改限价与 TIF：golden 断言完整事件、原序 ID 8→2→6、
  原 StopPriority 0→1→2、shadow 原内容/version 11 及普通 priority 零消耗。
- 双向触发价迁入已有两张订单的目标档，断言目标顺序 2→6→8，分配且只分配新 StopPriority 3；
  同时 GTD 20→30，真实聚合到期证明旧引用删除、新引用有效和终结 shadow 清理。
- 双向 quote budget 800→500→900：完整 typed event、FIFO/StopPriority 不变；错误数量模式、
  零预算、限价和非法市价 TIF 逐项拒绝。Base 市价 8→12 同样保序，并拒绝 quote/限价字段。
- 双向 GTC/GTD/DAY 触发前有效期完整九组迁移，包含 ClearToGtc、GTD A→B、DAY session、
  真实时间/时段终结及终结后 shadow 清理。另有独立 DAY 聚合回归。
- 双向 child GTC/GTD/DAY 完整九组迁移，证明 child TIF 与触发前 GTD 索引独立；
  设置相同触发价时不重排，不消耗任一分配器。
- 不可表示的 trigger reference、MarketProtection Keep/Clear/Set、显示量字段，错误数量模式、
  到期等于 now、错误 session、非法 IOC/FOK、用户/市场身份和调整 token 均拒绝；
  同时提出触发档位和到期迁移，证明没有部分提交。缺少 shadow 的 legacy Stop 单独拒绝，
  不编造默认 reservation。
- 两条最终提交计划在只读构造前后完整状态相等；生产 synthetic ceiling 与参考 u128 高水位下，
  触发价改变拒绝且状态不变，child 增量/纯到期清除仍可保序执行。
- 双向真实触发调用在 Trigger-R1 收到准确修改后的 child 模板；成交 2/目标 5，余量 3 实际 rest
  才获得普通 QueuePriority 1。两个模型原序事件完全一致，旧 Stop 到期引用不会再次终结订单。

双模型 `Pair::run` 直接比较原序完整事件与所有 Stop 原序载荷/priority；成功与失败均运行
生产 regular book、regular expiry 和 StopBook validator。每次拒绝额外比较完整失败快照：
参考源 Vec、完整 shadow/两个计数器/参考价格；生产双侧价档、Arena 槽位/free-list、按 ID 排序的
活动索引/shadow、完整 StopBook（含实际 expiry 索引）、普通 expiry、两个分配器和参考价格。
快照只是测试相等投影，不宣称 canonical digest 或 durable replay encoding。

## 运行态边界与后续契约

现有 `StopLimitOrder` / `StopMarketOrder` 没有保存 `trigger_reference` 或 MarketProtection，
既有触发流程也没有按每单 reference 分区的索引。因此当前对任何 `new_trigger_reference: Some`
以及 `new_market_protection: Some(Keep|Clear|Set)` 返回 `InvalidOrderCombination`，
避免静默丢弃、猜测默认来源或伪造已保存保护。已直接测试三种保护 patch 的拒绝和零 mutation。
这意味着 trigger-reference 修改后的新 StopPriority 与保护成功修改仍是显式 deferred，
不能宣称完整 v6 Stop Amend 全字段已支持；需要先建立相应运行态及触发执行契约。

所有成功活动改单保留已有 shadow 内容/version；`reservation_adjustment: Some` 返回
`InvalidReservation`。当前步骤不验签、不计算候选 reserve coverage，不声称扩大 exposure
已经获得生产资金授权。这些与常规 Amend 的既定阶段边界一致，Task 8 再整合真实授权应用。
实际触发时的 reservation-aware child 挂单生命周期沿用现状，没有在本小步提前改资金闭环。

## 本地验证与审读

2026-10-25 实际运行：

- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：11 passed，exit 0。
- `cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings`：exit 0，无诊断。
- `cargo fmt --check`：exit 0。
- `git diff --check`：exit 0，只有既有 LF→CRLF 提示；新增文件另做空白与末尾换行检查。
- `cargo test -p matching-orderbook --locked --quiet`：485 passed、0 failed，exit 0，31.60s；
  crate doctest 0。包含全部现有 differential/property、历史固定 seed 100k 和双次 10k tape；
  它们不等同于完整 Amend 100k tape。

新增实现与注释已审读：手写说明均为中文；没有数值换算公式变化、新依赖、浮点、unsafe、
wall clock、锁、无界队列、网络/RPC/数据库或 I/O。HashMap 仅身份查询，不驱动业务顺序。
业务数值边界复用严格正值对象与 checked StopPriority；唯一 writer 确保计划到提交之间无外部修改。
crash/WAL/snapshot/replay-digest 不适用。结构包装、事件容器和无分支 getter 由真实消费者覆盖，
不机械增加镜像单测。最终 T12 workspace gate、完整 Amend 100k tape 仍属于计划后续步骤。

本小步停止等待父执行会话独立审查，不进入 Task 8。

## 修复轮 1/5：触发源、保护执行与 DAY/GTD child

### 审查输入与范围裁定

读取 `task-7-stop-amend-review.md`，接受其 Critical 规格覆盖缺口与 Important 执行缺口，
按父执行会话指示补齐本步骤，不再将缺少历史字段作为合法成功路径的拒绝理由。
本地没有找到单独 ledger 文件，已向父执行会话报告；修复以该独立审查与父执行会话的明确指令为准。
本节取代前文“运行态边界”中 trigger-reference/protection deferred 的结论；
reservation coverage/验签/apply 仍留 Task 8，不因本次执行路径补齐而提前实施。

### RED → GREEN

1. 先写 `amend_stop_reference_changes_requeue_at_target_source_tail` 和
   `amend_stop_market_protection_keep_clear_and_set_preserve_trigger_priority`，分别运行定向命令，
   各 exit 1、0 passed / 1 failed，真实合法来源/保护改单被 `InvalidOrderCombination` 拒绝。
   然后才新增字段、来源分区和 patch 语义；两项转绿。
2. 再写 `amend_stop_timed_child_executes_then_regular_residual_expires` 与
   `amend_stop_protection_is_enforced_when_market_child_executes`。修正一个事件变体拼写错误后，
   分别真实 RED：DAY/GTD child 的 Trigger-R1 调用次数为 0（预期 1）；
   0 bps 市价 child 在越界 maker 101 上生成 Trade（当前触发价 100）。两项各 exit 1、0/1。
3. 两模型触发入口改为显式接收逻辑时间/session，限价 child 使用自身 timed placement，
   市价 child 使用实际保存的保护模板及冻结来源价调用自身 protected execution。两项转绿。
4. 新增完整来源/模式/非法时效与 Trigger-R1 拒绝证据后，18 项 Stop Amend 定向测试通过。
   旧测试把 Keep/Clear 和合法来源视作拒绝的断言不再成立，替换成相应成功矩阵；
   非法来源改用 StaticOpening 并继续比较完整失败快照。它们没有被删除或改弱为无条件通过。
   补充成交 maker 时曾漏给一个 TradeId，触发既有容量断言，修正测试输入后通过；
   此测试错误不作为业务 RED 证据。

### 实际实现

- 领域 `StopLimitOrder` 增加显式 `trigger_reference`；`StopMarketOrder` 增加显式
  `trigger_reference` 与 `market_protection`。两模型原始 Stop 载荷真实持有这些值，
  所有既有初始化 fixture 显式填入 LastTrade/None，运行时不自行推断来源或填造默认保护。
  市价保护只适用于市价 child；Limit 的 Keep/Clear 可保留/清空既有 None，Set 仍拒绝非法组合。
- Reference/Production Stop 候选分别保存 old/new reference 与 old/new protection。
  Keep/None 保留原保护，Clear 清空，Set 替换；合法保护 patch 保 StopPriority。
  两模型都在首次写入前拒绝 StaticOpening 与 bps >= 10,000。
- Production 的真实 StopLevel 键为 `(stop_price, trigger_reference)`，位置和 expiry 引用同步保存来源。
  改价或实际改来源均预检一次新 StopPriority、删除旧来源档位并加入目标来源档尾部；
  相同来源不重新排队。validator 反向验证载荷来源、价档键、位置及 expiry 一一对应。
  Reference 继续独立扫描源 Vec，以价格/来源/真实 priority 生成只读投影；不调用生产索引逻辑。
- 价格更新仅能触发相同来源的 Stop，LastTrade 级联不能触发 MarkPrice/IndexPrice 订单。
  两模型 pending 队列各自保存这次真实跨越的来源价格；后续 child 成交带来的来源更新
  不能改写已排队 child 的保护参考。Base/Quote 市价 child 分别调用自身既有保护执行器，
  原序保留 Trade 与 MarketProtectionTriggered 事件。
- `trigger_stops` 两模型签名现在要求显式 `now: LogicalTime` 和 `current_session_id`。
  所有仓库调用点已相应修改；没有使用来源序号、wall clock 或任意默认时间冒充业务时间。
  触发前先移出 Stop/expiry/shadow，然后按这两个参数重校验 child GTD/DAY；合法才进入 Trigger-R1。
  Trigger-R1 通过后，限价 child 调用自身 `place_limit_timed_with_events`，实际余量才获得
  QueuePriority 与普通 GTD/DAY 到期引用。过期/错 session 或 Trigger-R1 拒绝只产生既定终结链。

### 证据与不变量

- 来源迁移矩阵覆盖双向买卖、目标 MarkPrice/IndexPrice、目标已有订单、同源重复改单和旧 GTD 引用；
  断言目标原序 ID 2→1、priority 2→3、仅消耗一次新 priority、LastTrade 完整跨越不触发其他来源、
  正确来源才按原序收集，并清除旧 expiry 引用。
- 保护 Set→Keep→Clear 断言实际保存值、生产候选 old/new 快照及 retained priority；
  Base/Quote 两种 child 在 LastTrade/MarkPrice/IndexPrice 三种来源下真实执行 0 bps 保护，
  越界 maker 保持余量 8，无 Trade，产生真实保护终结事件，两模型原序事件一致。
- 真实 DAY/GTD child 在 Trigger-R1 后成交 2/8、余量 6 挂入普通簿，并通过真实 GTD/EndSession 终结；
  validator 检查实际 regular expiry 引用。GTD 等于触发 now、DAY session 错配均在 Trigger-R1 前拒绝，
  无普通 priority 消耗、无活动订单或 shadow 残留。另证合法 DAY child 的 Trigger-R1 资金拒绝
  不生成成交/挂单，只生成 StopTriggered→TriggeredOrderRejected→ReservationReleased。
- StaticOpening、非法 bps 的 placement/amend 均比较两模型自身完整失败快照；
  生产 synthetic ceiling 和参考 u128 high-water 都新增 source-only 改单拒绝证据，
  原来源/FIFO/字段/expiry/shadow/两个 allocator 完全不变。
- 前文快照和三个生产 validator 的检查继续保留，新来源/保护字段自然进入真实载荷和 Debug 投影；
  不把两个模型“共同丢掉新状态”当作一致。没有新增 I/O、依赖、原因编号、浮点、unsafe 或资金应用。
  保护数值公式复用已有 MarketMath/protected execution，公式契约未改变。

### 修复轮实际 gate

2026-10-25，最后业务/测试变更后运行：

- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：18 passed，exit 0。
- `cargo clippy --workspace --all-targets --locked -- -D warnings`：exit 0，无诊断。
- `cargo test --workspace --locked --quiet`：domain 153 unit + 7 Amend integration + 3 reservation integration，
  orderbook 492 unit，risk 8 integration，2 domain doctest，全部 exit 0；orderbook 用时 30.85s。
  其余 workspace crate unit/doctest 为 0。完整 orderbook 包含全部既有 differential/property/tape。
- `cargo fmt --check`、`git diff --check`：exit 0；diff 只有既有 CRLF 提示。
  新增 Rust 文件另做 UTF-8、末尾换行和无尾部空白检查。

本轮 gate 是当前代码的实际证据，不代替后续 Task 8–11 或完整 Amend 100k tape。
停止等待父执行会话 scoped re-review；没有提交、推送、合并或进入 Task 8。

## 修复轮 2/5：整个 root command 的不可变保护上下文

### 审查输入与 RED

读取 `task-7-stop-amend-rereview.md`，复核其中新增 Important 的调用链与 v6 §26.3–§26.5。
原修复轮 1 只避免已入队 child 的参考被改写，但真正后代仍重新使用前一 child 的成交价，
这不足以满足整个 root command 的冻结契约；本节取代修复轮 1 关于 pending 保护价的说明。

先新增两个固定 golden 测试：真正后代，以及首批已入队的对照场景。运行
`cargo test -p matching-orderbook --locked amend_stop_root_protection --lib --quiet`，
实际 exit 1、1 passed / 1 failed。后代测试在第一组精确复现：LastTrade 90→100，
A StopLimit 在 maker 110 上成交 1 并触发 B StopMarket，两个模型都输出 B 的第二笔 Trade(110,1)；
golden 要求 B 发出 `MarketProtectionTriggered(next_price=110,effective_worst_price=100,User)`。
首批对照测试首次即通过，不把它冒称为业务 RED。然后才修改两个模型的触发实现。

### 实际修复与 root 更新语义

- 两模型各自在 `trigger_stops` root 起点创建唯一、不可变的 `CommandStartProtectionContext`，
  并向本 root 的全部 child 与任意后代传递同一不可变借用。`execute_triggered_child` 不再接收
  单独的 descendant trigger price，也不再自行构造新的保护上下文。
- 此入口处理显式 root reference update：它以调用参数的更新后新价格一次冻结当前用户保护参考，
  不使用更新前基线，也不根据后续 child 的来源或成交价重新选择参考。
  LastTrade/MarkPrice/IndexPrice root 均有新值 100 的明确 golden。
  本次没有新增完整 PriceBand 配置/PreOpen 路由；既有 market_band_worst 为 None 的阶段边界不变，
  不以此边界允许 user bps 参考在本 root 内漂移。
- pending 恢复为订单、StopPriority 和触发事件三元组，不再保存供后代重冻结的价格。
  每笔 Trade 仍通过 `next_last_trade` 和来源隔离的 `collect_triggered_stops` 更新真实状态、
  检测实际跨越和追加后代；保护边界只从 root 上下文计算，不影响来源匹配与触发检测。
- 修改限定为两模型触发实现、固定差分 golden、参考模型 cfg(test) 只读来源投影与本报告。
  无 Amend 写屏障、reservation verify/coverage/apply、原因协议、依赖、公式或公开触发签名变更。

### Golden、差分与状态投影

新增 2 项测试各覆盖 Buy/Sell × Base/Quote × LastTrade/MarkPrice/IndexPrice；
分别检查首批两个 child 与 A 成交后才跨越触发的真正后代。后代矩阵双次独立构造并运行，
精确比较重复 transcript。所有结果先比较 Reference/Production，再与手工完整四事件 golden 比较：

`StopTriggered(A) → Trade(A,qty=1) → StopTriggered(B) → MarketProtectionTriggered(B,boundary=100)`。

Buy maker 为 110，Sell 镜像为 90；两种模式均只允许 A 一笔成交，B 不得成交。
保护事件逐字段锁定 B、next maker price、固定边界 100 与 User 来源，而非只计数或排序事件。
生产 maker 与参考 maker 的原始载荷、余量 1、priority 一致；两个 allocator 不因 B 改变。
两个 Stop 的身份/到期引用/shadow 全部终结，生产三个 validator 继续通过。

新增 `#[cfg(test)]` 的 Reference 只读来源投影，直接比较两模型完整 `PriceReferenceState`：
LastTrade 的实际最新价格仍是 A 的成交价 110/90，来源序号也按真实成交推进；
Mark/Index root 的更新后值仍为 100。由此证明冻结保护并未错误冻结触发检测状态。
这也区分了 root reference update 的更新后参考 100、旧基线 90/110 与后代成交参考 110/90。

### 修复轮实际 gate

2026-10-25，业务与测试变更后实际运行：

- `cargo test -p matching-orderbook --locked amend_stop_root_protection --lib --quiet`：2 passed，exit 0。
- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：20 passed，exit 0。
- `cargo test -p matching-orderbook --locked stop_trigger --lib --quiet`：9 passed，exit 0。
- `cargo clippy --workspace --all-targets --locked -- -D warnings`：exit 0，无诊断。
- `cargo test --workspace --locked --quiet`：domain 153 unit + 7 Amend integration + 3 reservation integration，
  orderbook 494 unit（30.73s），risk 8 integration，2 domain doctest，全部 exit 0；其他 crate 为 0。
- `cargo fmt --check`、`git diff --check`：exit 0，只有既有 LF→CRLF 提示。

最后审读保持独立模型、checked 算术、单 writer 与确定性原序事件；无 wall clock、I/O 或资金应用。
停止等待父执行会话再次 scoped re-review；没有进入 Task 8，没有提交、推送或合并。
