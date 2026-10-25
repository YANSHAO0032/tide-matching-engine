# T12 Task 3 + Task 4 独立审查

## Spec Compliance

**实现可接受；失败原子性的测试证据需要补强。** 对照绑定任务书 §§13/15/16/23 与当前 diff，Reference 通过自身 `Vec`/clone 规划撮合，未复用 Production 的 arena、价档、索引或计划。Reference/Production 的 `build_amend_plan(&self, ...)` 仅读取运行态并返回 crate-private 计划；定向快照测试也通过。常规 Reference Amend 的绝对目标、部分成交后二次改量、同价队列行为、改价后仅余量挂单才取新 priority、事件原序与 GTC/GTD/DAY 路径均符合所审小步。预留整合仍属 Task 8；当前带 token 的常规入口显式拒绝并保持状态。

## Strengths

- `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:197–299` 使用 Reference `plan_slices`，该函数克隆 Reference 的 `orders` 后以 Vec 扫描和稳定排序规划，旧单因同侧过滤不会充当本次 maker。TradeId 数量断言、residual `Qty`、priority 高水位均发生在提交 `self.orders` 前；全成不构造 `Qty(0)`，且不消耗 residual priority。
- `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:138–154,157–181`：同价减量原位更新绝对总量/余量/TIF；增量删除旧 Vec 位置并追加尾部，获得新 priority。`crates/matching-orderbook/src/reference_orderbook/tests/amend_regular.rs:102–155` 验证部分成交后再次以绝对总量计算，避免沿用旧总量。
- `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:172–181,244–299`：`OrderAmended` 记录撮合前候选余量；成交与现有订单事件按 `OrderAmended → Trade* → Order*` 排列。定向测试覆盖部分成交及全成。
- `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:471–484` 校验 GTD 逻辑时间及 DAY 当前 session；Reference 到期路径按运行态 TIF 扫描，GTC→DAY 的实际 `end_session` 测试通过。缺失 shadow、未验签 token、零余量和 priority 枯竭均在写入前拒绝。
- `cargo test -p matching-orderbook --locked amend_ --lib` 本次退出 0，16/16 通过（其余 434 项过滤）；`git diff --check` 退出 0，仅报告现有 CRLF 转换警告。

## Findings

### Critical

无已确认的行为错误。

### Important

1. `crates/matching-orderbook/src/reference_orderbook/tests/amend_regular.rs:46–78`：失败原子性共用 `Snapshot` 只存订单 ID、价格、当前余量、priority、TIF，以及预留版本和下一个 priority；它遗漏原始总量、完整 `ReservationState`（身份、reserves）、Stop/下一 StopPriority 和其他可影响后续行为的状态。`amend_reduce_after_partial_fill` 正是因原始总量未同步而曾失败，说明此字段不能从快照省略。当前 `InvalidSession`、priority 枯竭、缺失 shadow 和 Hard Gate A 等失败测试的 `before == after` 不能证明任务书要求的完整 deterministic-state 失败原子性。建议复用/扩展 `tests/amend_candidate.rs` 的完整快照，覆盖常规 Amend 全部拒绝用例。token 用例另有完整目标 `ReservationState` 比较，但不弥补其他拒绝用例。

2. `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:220–224`：TradeId 数量不足会在提交前触发 fail-stop 断言，这沿用 Reference 既有 TradeId 调用方约定，并非已发现的半提交；但 `tests/amend_regular.rs` 尚无不足 TradeId 的定向用例。绑定任务书要求预检 TradeId 并列出其失败原子性边界。建议用 `catch_unwind` 证明断言先于写入，并以完整快照比较 panic 前后状态，或在上层命令路由接入时提供可审计的充足性证据。当前报告中“所有普通业务拒绝”的表述不覆盖这条 fail-stop 路径。

### Minor

- `crates/matching-orderbook/src/reference_orderbook/tests/amend_regular.rs:274–308`：TIF 测试覆盖 GTC→GTD、GTC→DAY 与无效 DAY；§16 所列 GTD→GTC、GTD A→GTD B、DAY→GTC/GTD 等尚无直接定向测试。Reference 基于运行态 TIF 扫描，代码路径对这些转换是统一的；这里是迁移证据范围的限制，后续完整 TIF gate 应补覆盖。

## Clippy 与范围

`cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings` 本次退出 1，仅有 13 条 `dead_code` 诊断。Production candidate 尚无 Task 5 consumer；Reference candidate/真实入口虽在本小步内部互相调用，但因尚无非测试命令路由，lib 分析也将整条路径标为未用。检查了新增 candidate/入口，没有 `allow(dead_code)`、`expect(dead_code)` 或 crate 外公开 preview API。严格 Clippy 因此**仍未通过**，不能将 Task 3+4 视为最终 gate 完成。

## Task quality

Reference 实现路径与冻结语义可接受，可继续 Production consumer 小步；请在最终验收前补齐上述完整快照和 TradeId 边界证据，并在接通真实命令路由后重跑严格 Clippy、完整 differential 和 workspace gate。此次审查未修改代码、索引或分支，仅新增本报告。
