# T12 Task 6：常规改单差分与时效迁移报告

## 范围与基线

本轮在 `codex/t12-amend` 的既有未提交工作区完成 Task 6 小步，保留 Task 0–5 全部改动。
读取执行计划、Task 5 报告/独立审查、Task 4 报告、真实 Amend/expiry/差分源码与 diff/status；
worktree 没有 ignored `assignment/`，按前轮既定方式读取主 checkout
`D:/rust_workspace/tide-matching-engine/assignment/` 的规则、唯一交接、任务书及 v6 §20。
本报告提供给父执行会话更新唯一 handoff，不另建权威交接。

Task 5 已实现 GTC/GTD/DAY 候选预检及提交迁移；本轮新增双模型直接证据后，该业务实现不需修改。
实际业务修复仅涉及 Reference 常规 Amend 的目标价档 checked 聚合预检。
没有实现 Stop Amend、冰山自身 Amend、reservation 验签/apply、T13、提交、推送或合并。

## RED → GREEN 证据

1. 先新增改价余量进入已有目标档的极值差分：旧单数量 2，目标档数量 `u64::MAX - 1`。
   运行 `cargo test -p matching-orderbook --locked amend_regular_differential_reprice_target_aggregate_overflow_is_precommit --lib`，
   exit 1，0 passed / 1 failed。Reference 错误返回 `Ok([OrderAmended ...])`，Production 返回
   `Err(ArithmeticOverflow)`。此时尚未修改 Reference 业务实现。
2. 再新增同价增量差分：旧单数量 1，同档另一单 `u64::MAX - 1`，新绝对总量 2。
   运行 `cargo test -p matching-orderbook --locked amend_regular_differential_same_price_aggregate_overflow_is_precommit --lib`，
   exit 1，0 passed / 1 failed；同样观察到 Reference 成功、Production 拒绝。
3. Reference 新增独立 Vec 扫描，对逻辑删除旧单后的目标档逐项 checked 累加可见量及计数。
   同价增量在分配 priority 前检查；改价在冻结虚拟成交后、实际写入前检查实际 residual。
   再运行 `cargo test -p matching-orderbook --locked amend_regular_differential_ --lib`，两项转绿。
   后续把这些测试接入完整快照 harness，并叠加 TIF 变化，证明错误发生在旧到期引用迁移前。
4. 最终审读补强同时耗尽 priority 与目标档上溢的错误优先级：新增 Reference unit，先观察
   `ArithmeticOverflow` 与预期的既有 `PrioritySpaceExhaustion` 不一致（exit 1，0/1）。
   同价增量增加只读高水位检查，改价在 residual priority 预检后再检查目标档；两者仍在写屏障前。
   该 unit 的同价/改价两分支及全部 Amend 定向测试转绿。

其余新增固定场景及生成式差分首次运行即证明了 Task 4/5 已有行为，不冒称这些测试曾产生业务 RED。
测试开发中修正过一处临时值借用编译错误，以及错误预期普通 maker 额外 `OrderFilled` 的断言；
真实已有普通 maker 协议没有该事件，故保持现有协议，只锁定实际 taker 生命周期原序。
上述测试问题不作为业务 RED 证据。

## 实现与差分投影

- `production_orderbook/tests/differential/amend_regular.rs` 建立常规域独立
  `Command::{Rest, Place, Amend, Cancel, AdvanceTime, EndSession}`；`Amend` 载荷使用 Box
  满足严格 Clippy 的枚举大小检查。历史基础 GTC 生成器的版本、输入及适配不变。
- harness 分别调用两模型真实 public API，不把 Production 结果喂给 Reference，
  不调用另一模型的 candidate、撮合或提交 helper。TradeId 来自独立确定性测试 allocator；
  该普通域先检查活动订单总数不超过 128，每张普通 maker 最多一笔成交，统一准备足量 TradeId。
- 每条命令前后分别从 Reference 有序视图与 Production 真实侵入式链表生成 open orders、
  完整原始总量/remaining、FIFO/priority、price levels/depth 与 best price，按业务原序比较。
  命令结果的 `Vec<EngineEvent>` 逐项原序比较；从不排序事件。
- 时效和完整 shadow 状态/version 按同一业务订单顺序比较。直接比较两个 priority allocator；
  参考 allocator 由新增 `#[cfg(test)] pub(crate)` 快照辅助提供，不扩大运行时公开 API。
  完整已知 ID 集合另逐身份查询两套 shadow，断言终结订单无残留；活动预留保持原内容/version 11，
  不以两模型同时错误递增版本冒充一致。
- Production 每步执行结构/普通 expiry/Stop 三个 validator。
  Reference 没有生产二级 expiry 结构，采用自身订单 TIF 作为到期 oracle；
  Production validator 证明真实 GTD/session 引用与该 TIF 一一对应，再用真实到期入口比较有序输出。
- 所有业务拒绝同时比较完整失败快照。Reference 快照含源 Vec 的全部字段、排序后的完整 shadow、
  两个 allocator、Stop 源载荷与价格参考。Production 快照包含全部 Arena 槽位/free-list、
  排序 ID/shadow 映射、双侧价档元数据、普通 expiry、Stop、有序 priority 及价格参考。
  测试 Debug 字符串用于结构相等断言，不声称它是 durable digest 或规范化 replay encoding。

## 新增证据映射

本轮新增 10 项常规差分测试及 1 项 Reference 错误优先级 unit：

- 双向先成交 3/10，再减至绝对总量 8（余量 5）、增至绝对总量 12（余量 9）：
  减量保 FIFO/priority，增量移同价尾且仅分配一次；无数量的再次改价保持最新绝对总量 12。
  目标等于已成交量 3 及低于已成交量 2 均拒绝，完整状态不变，保留 Hard Gate A。
- 双向改价进入已有两张同侧订单的价档：余量排真实尾部、priority 为新值、count/depth 正确；
  同时 GTD 20→30，在 20 不到期、30 到期，清除全部原引用与终结 shadow。
- 双向同侧目标档聚合上溢、同价增量上溢：Reference/Production 同拒绝，
  Arena/索引/源 Vec/FIFO/priority/allocator/到期与 shadow 均不变。
- 双向两价档 partial/full reprice：golden 精确比较
  `OrderAmended → Trade(先价档) → Trade(后价档) → 目标部分/全成事件`，
  使用 maker ID 9→2 证明不是 ID 排序。改单事件记录重新撮合前候选余量；全成不分配 residual priority，
  partial 仅实际余量入簿。GTD/DAY maker 的到期引用和 shadow 随全成清理。
- 双向完整九组 GTC/GTD/DAY 类型迁移：纯 TIF 改单保持 FIFO/priority/allocator；
  原 GTD 20 引用不残留，目标 GTD 30 或当前 DAY 9 由真实到期入口终结，GTC 保留。
- GTD 30→20 与跨时间/同时间到期次序：输出先时间、再 ID，不能按 FIFO 全局排序。
- 非法 GTD（等于/早于 now）、DAY session 错配、IOC/FOK、quote 模式切换、市场/用户身份错误、
  未验签 token、缺失 shadow 及已取消身份拒绝；检查完整快照，包含非空双侧订单和不同到期条件。
  token Some 仍明确 `InvalidReservation`，未创建验签上下文或修改 shadow/version。
- 同一混合常规 tape 两次独立运行，逐命令原序事件及状态 transcript 完全一致；
  proptest 生成 1–39 次数量/价格/TIF 混合改单，覆盖成交、重排、失效身份及非法时效，
  每步比较并在最后执行真实 GTD/session 终结。
- 同时触及 priority 高水位和目标档上溢时先返回既有 priority 拒绝，
  对同价增量及改价均比较完整 Reference 快照，不引入失败 allocator 漂移。

## 本地验证

2026-10-25 实际运行：

- `cargo test -p matching-orderbook --locked amend_regular --lib --quiet`：34 passed，exit 0。
- `cargo test -p matching-orderbook --locked differential --lib --quiet`：66 passed，exit 0，29.79s；
  此独立运行发生在最后错误优先级补强前，补强后完整 orderbook 再次覆盖全部 66 项。
- `cargo test -p matching-domain --locked --quiet`：153 unit、7 Amend integration、
  3 reservation integration、2 doctest 全通过，exit 0。
- `cargo test -p matching-orderbook --locked --quiet`：474 passed，0 failed，exit 0，29.93s；crate doctest 0。
- `cargo clippy -p matching-orderbook -p matching-domain --all-targets --locked -- -D warnings`：exit 0，无诊断。
- `cargo fmt --check`：exit 0。
- `git diff --check`：exit 0，只有既有 LF→CRLF 提示；新增文件另检查行尾空白与最终换行。

全量 orderbook 含历史固定 seed 100k、两组重复 10k 和全部既有 differential/property；
它们不是完整 Amend 100k tape，后者属于 Task 10。没有新的 domain/risk 逻辑或依赖修改，
整个 workspace 最终 gate 仍属于 Task 11；不把当前小步通过解释为完整 T12 已验收或远端 CI 已通过。

## 审读与续接

新增业务算术均 checked，无 wall clock、网络、RPC、数据库、unsafe、锁、队列、WAL 或 snapshot 改动。
HashMap 仅查询身份，快照的身份排序只为相等断言；事件/成交/FIFO 始终保持实际业务顺序。
无新增公式语义，故不修改数值公式文档；没有 I/O，crash/replay-digest 测试不适用。
简单结构构造、投影 getter 和枚举包装由实际命令消费者测试覆盖，不机械增加镜像单测。

Task 5 deferred 的 GTD A→B 与已有目标价档 residual/overflow 已有直接证据。
Stop 聚合到期可能绕过 shadow 清理的历史事项仍留后续限定审查；本轮未触发或修订 Stop 到期实现。
本小步停止等待父执行会话的独立 review，不进入 Task 7/8；无提交、推送或合并。
