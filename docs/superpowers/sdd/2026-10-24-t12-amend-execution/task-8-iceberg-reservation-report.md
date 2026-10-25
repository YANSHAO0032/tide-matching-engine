# T12 Task 8：冰山改单与预留整合实施报告

状态：实施和本地 gate 已执行，等待父执行会话独立审查；不能据此标记整个 T12 完成。日期为 2026-10-25。所有变更仍未提交；没有提交、合并、推送或进入 Task 9。

## 读取基线与范围

工作目录为 `C:/Users/10703/.codex/worktrees/t12-amend/tide-matching-engine`，分支为 `codex/t12-amend`，HEAD 为 `550fda02816c73a5704b4ffe836fcfb31ffff08f`。保留 Task 0–7 及中断前 Task 8 的全部既有改动，未用 HEAD 覆盖未提交成果。中断续接时，两个 StopLimit child 修复和相应测试已经存在；本次重新检查代码、通过受控撤销证明回归测试可捕获错误，再恢复原文件并执行新一轮完整 gate。

已读取本地 AGENTS、执行计划、进度账本、Task 7 报告、真实 git status/diff、相关未跟踪实现和测试。当前 worktree 缺少 ignored `assignment/`；规格和规则从 `D:/rust_workspace/tide-matching-engine/assignment/` 读取，包括唯一 handoff、CODEX_GUIDANCE_RULES、T12 历史任务书和 v6 §7.4.2–§7.4.3、§20。继续遵循已冻结的直接实施授权、严格正余量决策和 typed `OrderAmended` 协议，不恢复旧导师模式，不创建第二份权威 handoff。本报告供父执行会话同步唯一交接。

本步范围是冰山 Amend、真实本地 reservation coverage/验签/版本应用，以及 StopLimit 从触发身份接续到普通挂单时的 reservation 和绝对数量完整性。没有实现资产映射、费率账本、资金账户、结算、网络 RPC、完整命令路由、WAL、快照、持久化幂等或 CancelReplace。

## 冻结的资金输入边界

当前匹配核心没有资产映射、真实 FeeSchedule/费率及取整策略、当前 CommandId 或全局风险密钥配置，不能自行计算 principal + max fee。执行账本已经记录采用显式 `AmendExecutionContext` 的裁定：

- 调用者固定本次 `command_id`、`fee_schedule_version`、`fee_tier_id` 和已加载 `RiskKeySet`。
- `required_reserves` 必须由调用者根据实际候选订单与真实市场/资产/费用配置计算，包含本金、最大费用和取整安全量；必须非空、严格按资产升序、无重复且数量严格正。
- 订单簿不能从 token 自身复制所需预留或填入虚构配置。此输入是明确的信任边界，本步不宣称已验证上游资金需求计算器。
- 无 token 时逐资产校验当前资金覆盖量，成功保持已有 reserves 与 version；减少需求也保留原资金，不虚构部分释放事件。
- 有 token 时调用现有 `verify_amend_reservation`，严格 Ed25519 验签并核对命令、预留、订单、用户、市场、预期版本、费率版本/层级、公钥标识及目标资金覆盖量。

`AmendExecutionContext::prepare` 只克隆并修改可丢弃的 `ReservationState` 副本，版本增加使用 checked arithmetic。真实订单簿继续执行独立的完整预检，成功以后只对仍活动的同一订单替换预留。后续价格档聚合、priority 或时效失败会丢弃副本；全成终结不会重新插入 shadow。

低层 `amend_regular_with_events` / `amend_stop_with_events` 保持此前公开契约：调用者必须先完成覆盖校验，接口本身拒绝携带 token，不提供隐式验签。需要完整本地 coverage/验签整合的调用必须使用 `amend_with_context`。本步没有把低层入口误称为完整 R1 命令入口。

## 冰山状态与优先级矩阵

两模型使用各自 `amend_iceberg.rs`：Reference 以独立 Vec 规划全部片成交和剩余订单，Production 以 Arena、PriceLevel 和自身 IcebergPlan 规划。没有调用彼此的状态迁移实现；共享的领域协议/资金验证只处理共同的值语义。

候选先按绝对目标计算已成交量和新余量，严格拒绝目标小于或等于已成交量。新峰值必须严格小于新的绝对总目标，允许峰值大于尾部剩余量。每次成功维持 visible + hidden = total remaining，公开深度仅使用 visible。

| 修改 | QueuePriority 与显示片 |
| --- | --- |
| 同价、总量不增、峰值不增 | 保留原 FIFO/priority；visible 为原可见片、新峰值、新余量三者最小值，隐藏量为新余量减 visible |
| 同价增加总量 | 真正重新 rest 时分配一个新 priority，并移到同价队尾 |
| 同价增加峰值，即使总量减少 | 丢 priority；新片按峰值与余量取较小者 |
| 改价 | 先以候选全部余量正常撮合；仅实际剩余 rest 时分配新 priority |
| 仅合法 TIF 修改 | 本身不丢 priority；原子迁移实际 expiry/session 引用 |

减量不会提前公开隐藏量；峰值下降可将旧显示片的一部分重新隐藏。增量/改价路径在删除原单以前预检对手片成交、maker 补量、TradeIds、目标档 visible 聚合和 priority 容量。提交以后没有普通业务 `Err`；结构损坏及预先约定的 TradeId 容量不足属于不变量 fail-stop。TradeId 不足测试以 `catch_unwind` 证实写屏障前全部状态保持不变。

事件保留原序 `OrderAmended → Trade* → order events`，`OrderAmended.new_quantity` 是撮合前候选余量。完全成交不分配 residual priority，不保留 shadow；部分成交挂回订单保持绝对总目标与新 TIF/峰值，后续再 Amend 可正确扣除历史成交量。

## StopLimit child 接续与两项 RED 证据

触发收集先从 StopBook/expiry 和活动 shadow map 移除旧身份。待执行 FIFO 元素额外携带移出的 `Option<ReservationState>`，两模型分别取得它；pending FIFO 和触发排序不变。

通过 Trigger-R1 并成功执行后，只有真实挂入普通簿的 StopLimit child 接续同一个 ReservationState，包括已经提交的版本与资产量。Trigger-R1 拒绝、底层 child 拒绝、StopMarket 以及全成 StopLimit 都不重新插入 shadow。接续在处理该 child 成交引发的后续级联之前完成。

StopLimit 普通挂单原始载荷必须保存 child 已接受的总量，而不是仅保存剩余量。示例中签名 Amend 将 StopLimit 总量从 10 改为 12，触发后成交 2、余量 10；接续载荷保存总量 12，此后绝对目标 11 应得到剩余 9。

续接时没有把已存在实现直接当作正确。使用 `target/task8-resume-audit/` 保存两份当前文件的精确字节，依次实施受控撤销，运行同一个真实入口测试：

1. 撤去 child 成功后接续区块，运行 `cargo test -p matching-orderbook --locked amend_reservation_signed_stop_carries_version_into_resting_child_and_next_amend -- --nocapture`：shell exit 1，1 failed / 0 passed，断言得到 `None`，预期为 version 12、reserves 1250 的原 ReservationState。临时探针同时产生两个未使用变量警告，不把它们视为业务 RED。
2. 从精确备份恢复，只撤去 accepted total 恢复区块，保留 reservation 接续；同一命令 shell exit 1，1 failed / 0 passed，后续绝对目标 11 的结果为余量 11，预期 9。这一轮没有编译错误或警告，精确捕获累计成交丢失。
3. 恢复两文件精确字节，重新运行全部 `amend_reservation`：10 passed / 0 failed，exit 0。临时撤销没有留在源码中；没有删除或弱化断言。

中断前冰山/context 实施的最初 RED 输出没有写成可复核报告，本续接不追认未知历史结果。上述是本次实际重跑的两项可复核 RED→GREEN 证据；其余矩阵以当前真实测试执行及代码审查为据。

## 测试覆盖与审查证据

`production_orderbook/tests/amend_iceberg.rs` 的 6 项测试真实执行两条独立实现，并对生产结构运行 regular book、regular expiry、StopBook 验证器：

- Buy/Sell 的总量减少、总量增加、峰值减少、峰值增加、改价矩阵；检查完整 typed event、visible/hidden/total、原 FIFO 或同价尾部和新 priority。
- 部分成交后绝对目标更新、不公开隐藏量、GTD 20→30 实际索引迁移与终结 shadow 清理。
- 双向改价部分成交/全成、原序事件、maker shadow 清理、余量的峰值/TIF、新 priority 或零 residual priority 消耗，以及真实 DAY 终结。
- 非法峰值、GTD/DAY/IOC、TradeId 短缺、尾部 peak > remaining、目标小于/等于已成交量。
- Buy/Sell 的同价增峰及改价目标档 visible 聚合溢出：拒绝前后完整状态相等。

`production_orderbook/tests/amend_reservation.rs` 的 10 项测试覆盖普通、冰山、StopLimit、StopMarketByQuote、StopMarketByBase 五种容器：

- 真实签名的增长成功，version 11→12；同 token 重放拒绝且全状态不变；绑定新 CommandId/expected version 的下一令牌成功推进至 13。
- 11 类令牌错误：预期版本、CommandId、OrderId、UserId、MarketId、ReservationId、fee schedule、fee tier、未知 key、覆盖不足及损坏签名。坏身份/费用字段重新签名，避免把身份核验误测成单纯坏签名。
- 无 token 的足额增长和减量保持资金/version；覆盖不足拒绝；空、零、重复、逆序 required vector 拒绝且完整状态不变。
- 真实有效签名之后，普通/冰山 QueuePriority 或 StopPriority 高水位失败；有效签名后的非法 trigger expiry；预留版本 u64::MAX 增量溢出：全部不应用资金副本。
- 签名改价导致普通/冰山全成不重插 shadow；签名 StopLimit 部分成交接续及下一次绝对 Amend；Trigger-R1 拒绝/StopMarket/StopLimit 全成不残留 shadow。

失败快照包含双侧价档、Arena 槽位/free-list、排序后的活动 ID/shadow、StopBook 和真实 expiry 索引、普通 expiry、两种 priority 分配器、参考价格；Reference 使用自身 Vec、shadow、计数器与参考价格投影。它们是测试相等投影，不是 canonical digest 或持久化编码。HashMap 只供身份定位，不参与业务顺序。

简单载荷构造、透明参数转发和原位 setter 没有逐方法机械新增单测；上述真实订单操作、结构验证器、原序事件与全状态拒绝快照覆盖它们。没有通过 lint 例外、降低等级、绕过现有测试或共享生产答案使 gate 通过。新增说明注释为中文。业务路径无浮点金融算术、unsafe、wall clock、网络/数据库/RPC、锁或无界队列；本步没有 I/O，crash/durable replay 测试不适用。

## 依赖与公式

本步订单簿测试使用 `ed25519-dalek = { version = "3.0.0", default-features = false }` 作为 dev-dependency，为真实签名生成提供证据；生产订单簿消费既有本地 matching-risk。它复用用户已批准并锁定的版本，不引入另一个版本或新的 feature，未升级现有依赖。`cargo tree -p matching-orderbook --locked -e features --prefix none` 已核对 matching-risk/测试使用同一 ed25519-dalek 3.0.0。测试不启用 RNG、网络或异步运行时；第三方内部 unsafe 仍不能由项目 lint 推断，沿用 Task 1 依赖审查。

`docs/numerical_formulas.md` 已包含绝对总目标及冰山显示片裁剪公式。本次核对现有 `target/t12-amend-formulas/` 渲染成果与当前文档的 146 个公式逐项 TeX/display 完全一致，全部 count=1、无 error、MathML 结构一致；使用 GitHub 的 math-renderer / MathJax 3.2.0 组件，禁用宏与 HTML 截断负对照均被检测。当前文档 SHA-256 为 `f7bb968cdca16876abe0a105a1acb581877fc368b05a0cf7e8a43e9cac79d438`；本次又检查新增章节截图 `formula-section-9.png`，符号和结构无缺失。

这是复核已有本地渲染成果，不宣称本次重新访问了 GitHub。报告中的 remote 比较对象是历史提交 3fb7e2a，仅有 116 个公式，与当前新增内容自然不一致；不能把其 source/structure mismatch 解释为当前已推送页面通过。Task 11 和未来发布仍需分别核验最新文档及实际远端页面。

## 本次实际 gate

以下命令均在恢复最终源码后执行，除上述明确的 RED 命令外均 exit 0：

| 命令 | 结果 |
| --- | --- |
| `cargo test -p matching-orderbook --locked amend_reservation --lib` | 10 passed，0 failed |
| `cargo test -p matching-orderbook --locked amend_iceberg --lib` | 6 passed，0 failed |
| `cargo test -p matching-orderbook --locked differential --lib --quiet` | 66 passed，0 failed，29.17s |
| `cargo fmt --check` | 通过 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 通过，无诊断 |
| `cargo test --workspace --locked --quiet` | domain 153 unit + 10 integration、orderbook 510 unit、risk 8 integration、2 doctest 全部通过；其余当前空 crate 测试为 0，不把它们作为业务证据 |
| `git diff --check` / `git diff --cached --check` | 通过；普通 diff 仅提示既有 LF→CRLF 配置警告 |

workspace 共 681 项 unit/integration 测试和 2 项 doctest；orderbook 包含当前 66 项完整 differential 及既有固定 seed 100k/双次 10k tape。它们尚未包含 Task 10 的完整 Amend/Stop/冰山联合随机命令扩展，不能据此提前宣称 Task 10 完成。

## 待独立审查与后续

父执行会话须审查本步真实 diff 与上述资金输入信任边界；独立审查通过以前 Task 8 保持“等待审查”。Task 9/10 的完整失败性质矩阵、所有 Amend 命令及成功 transcript 的参考价/资金投影、Amend 专属 100k/重复 tape 和最终全分支审查仍未开始。Task 1 deferred 的底层 verifier `required_reserves` 独立拒绝测试仍需在最终 T12 gate 前核对，不用本步无 token context 测试冒充底层 signed-verifier 分支覆盖。

本次没有修改唯一 handoff，由父执行会话合并本报告事实到该唯一文档；未创建第二份 handoff。停在 Task 8 审查边界。
