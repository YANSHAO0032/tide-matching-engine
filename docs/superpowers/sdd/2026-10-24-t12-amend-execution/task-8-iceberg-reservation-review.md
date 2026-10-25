# Task 8 独立审查：冰山改单与预留整合

审查日期：2026-10-25。结论：本步通过；Critical 0、Important 0、新增 Minor 0。保留账本中已有 Minor 1 项，必须在整个 T12 最终 gate 前补齐。本结论仅覆盖 Task 8，不代表 Task 9/10/11 或整个 T12 完成。

本次只新增本审查文件，没有修改 Rust、测试、依赖、ledger 或唯一 handoff，没有提交、合并或推送。

## 来源和审查范围

实读工作树计划、`.superpowers/sdd/2026-10-24-t12-amend-execution/progress.md` 的全部裁定、Task 8 实施报告、真实 git status/普通 diff，以及未跟踪的两个模型 `amend_candidate.rs`、`amend_iceberg.rs`、`amend_stop.rs`、执行上下文和相关测试。重点沿调用链检查 `order_node`、`price_level`、两模型片成交计划、Stop 触发接续及 matching-risk 的真实验签/应用。

绑定来源是 `C:/Users/10703/.codex/attachments/4b1d84b9-e4c2-4af6-a4af-1a0b2fc07723/已粘贴的文本.txt`，实读 §22–24 及紧邻失败测试要求。工作树没有 ignored `assignment/`，旧任务书、规则、handoff 及 v6 §20 从 `D:/rust_workspace/tide-matching-engine/assignment/` 读取；没有把旧导师任务书当作当前实施授权。采用 ledger 已冻结的严格正余量、typed `OrderAmended`、书内 shadow ownership 和显式资金上下文裁定。

## 规格与实现核对

### 独立冰山计划和优先级

Reference 由自身 `plan_slices` 在虚拟 Vec 中规划成交、补量与残量；Production 由自身 `preflight_iceberg_plan`、Arena/PriceLevel、RestPlan 规划并提交。两者没有调用对方状态迁移或以生产输出生成参考答案。共同领域绝对数量辅助函数及资金验证属于共同值契约。

两模型都按最近接受的绝对总目标减当前总余量计算已成交量，然后计算候选余量；目标小于或等于已成交量拒绝。峰值与绝对总目标比较，允许峰值大于尾部余量。保 priority 的条件为同价、总量不增、峰值不增；显示片裁剪为旧 visible、新 peak、新余量的最小值，隐藏量使用 checked 差值，因此不会提前公开隐藏量，并维持 visible + hidden = total remaining。Production 仅扣除公开深度变化。

总量增加、峰值增加及改价均失去 priority；即使减总量同时增峰也重新排队。改价按全部候选余量成交，只有实际正残量 rest 时分配一个新 priority；全成没有 residual priority。TIF 自身不参与 priority 丢失条件，实际 expiry 引用在成功边界迁移。目标档可见聚合、maker 补量所需 priority、TradeId 容量和残量 rest 的所有业务失败都在删除原单前处理；写屏障之后只消费计划。TradeId 不足沿既有约定使用 pre-mutation fail-stop，不能误称普通业务 Err。

六项 `amend_iceberg` 测试实际执行两模型，覆盖 Buy/Sell 矩阵、部分成交绝对目标、片裁剪、GTD 索引迁移、DAY 终结、改价部分/全成、原序事件、峰值/时效拒绝、TradeId 短缺和双向目标档聚合溢出。断言不仅比较两模型，还固定预期 quantities、priority、FIFO、事件 payload，并运行 Production 结构验证器。

### 资金输入与原子应用

`AmendExecutionContext` 明确要求调用者以本次候选订单及真实市场、资产、费率和取整配置计算完整 required-reserves，并固定 CommandId、费率版本/层级与预加载公钥。列表必须非空、严格资产升序、无重复、数量严格正。实现不从 token 复制需求、不填造资产或费率，不实现外部账户/settlement。这里是明确的可信调用者契约，不能宣称核心已经证明上游本金加最大费用计算正确。

两模型的 `amend_with_context` 先构造只读候选，然后 `prepare` 对 clone 做覆盖或签名校验和 checked 版本递增；真实簿仍需通过其独立完整后续 preflight。只有成功以后才替换仍活动的同一订单预留；失败丢弃 clone，终结订单不重插 shadow。普通、冰山、三种 Stop 容器均走同一个上下文边界。没有令牌且当前逐资产覆盖足够时保持原资金和版本；降低需求也保留原资金，没有伪造释放事件。

有令牌时真实调用 `verify_amend_reservation`，执行严格 Ed25519 验签，并绑定命令、预留、订单、用户、市场、当前版本、费率版本/层级、已加载 key ID 和完整候选需求。应用对象的身份及 expected version 再核对，版本 checked +1；成功令牌重放因版本不符拒绝。没有 token-consumed HashSet 或假验签。真实订单入口测试覆盖五种容器的成功、重放、下一版本、11 类错误令牌、有效签名后 priority/expiry 失败及版本溢出；坏身份重新签名，能够验证身份分支。

低层公开 `amend_regular_with_events` / `amend_stop_with_events` 明文约定调用者已完成覆盖校验并拒绝直接携 token；它们不应被上层当作完整资金命令入口。这符合当前 ledger 的显式上下文裁定，报告也准确保留了该边界。

### StopLimit child 接续

两模型分别在触发移除时取得 shadow，pending 项携带其 Option 状态。成功 child 执行后只在真实普通挂单存在时接续，同一个预留版本/数量在级联后代执行前已恢复。Trigger-R1 拒绝、底层 child Err、StopMarket 与 StopLimit 全成路径均不重新插入 shadow。

普通挂单 payload 恢复为已接受 StopLimit child 的总量，当前剩余量独立保存，避免后续绝对 Amend 丢失已成交量。固定测试实际覆盖签名总量 10→12、触发成交 2、残量 10、下一绝对目标 11 得残量 9，并检查 version-12 shadow 接续。没有使用残量 10 作为新的原始总目标。报告中的两次受控 RED 撤销是实施者记录的历史证据；本审查未重复改写源码做撤销，仅独立复核当前代码和 GREEN 测试。

## C / I / Minor

- **Critical：0。** 未发现资金提前提交、失败状态漂移、终结 shadow 重插、冰山守恒或 priority 实现错误。
- **Important：0。** 当前任务要求的两模型独立路径、真实签名、后续失败原子性和 StopLimit 接续已有可执行证据。
- **Minor：新增 0；继承 1。** `crates/matching-risk/tests/amend_reservation.rs` 仍缺针对底层 `verify_amend_reservation` 的 zero/duplicate/out-of-order `ExpectedAmendReservation.required_reserves` 独立用例。底层代码明确拒绝，账本已将其延后到最终 T12 gate；本步 context 的非规范列表测试会在调用 verifier 前拒绝，不能替代底层 signed-verifier 分支覆盖。该缺口不阻塞 Task 8，但最终 gate 前必须关闭。

Task 9/10 的全失败性质矩阵、成功状态投影和 Amend 专属长 tape 仍是后续计划，不用当前 66 项历史 differential 或本次聚焦测试冒充其完成。当前未引入 I/O，durable replay/crash/远端 CI 不在本次验证结论内。

## 文档、公式与证据边界

公式新增章节与实现一致：严格正候选余量、绝对总目标、非增显示片裁剪、隐藏量差值及 residual-only priority。当前 SHA-256 与实施报告的 `f7bb968cdca16876abe0a105a1acb581877fc368b05a0cf7e8a43e9cac79d438` 相符。

独立比较当前文档提取的 146 项 TeX/display 与 `target/t12-amend-formulas/formulas-results.json`：146/146 一致、每项 count=1、无 error、structureMatches=true。实读审计脚本及 report，确认禁用宏/HTML 截断负对照存在，报告明确 remote 对象是旧 116 项提交并存在 source/structure mismatch。不能据此宣称当前远端 GitHub 页面已通过。本审查未重新联网运行组件或完整视觉重验，结论是当前源码与已有本地渲染证据一致；最新页面发布后的验收仍待最终文档/发布阶段。

## 本次实际运行

| 命令或核对 | 结果 |
| --- | --- |
| `cargo test -p matching-orderbook --locked amend_ --lib --quiet` | exit 0；76 passed、0 failed，包含当前 6 项冰山与 10 项 reservation 测试 |
| `cargo test -p matching-risk --locked --quiet` | exit 0；8 integration passed；0 unit/doctest 不计业务覆盖 |
| 当前公式源与已有渲染 JSON 逐项比较 | 146 项，0 mismatch；当前文件 hash 相符 |
| `git diff --check` | exit 0；仅现有 LF→CRLF 提示 |

没有重复执行实施报告已经记录的完整 differential、fmt、严格 workspace Clippy 和 workspace tests，也不把其历史通过写成此次独立运行。父执行会话仍须统一记录最终工作树 gate 与唯一 handoff。Task 8 可以在采纳本审查后关闭，后续任务按原计划串行进行。
