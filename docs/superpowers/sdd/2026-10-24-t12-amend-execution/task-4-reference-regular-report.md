# T12 Task 4：Reference 常规改单报告

## 范围

本小步只实现独立 `Vec` ReferenceOrderBook 的常规限价改单入口。它消费
Reference 自己的只读 candidate，不调用 Production 的计划、arena、价位或到期辅助。
没有实现 Production Task5、Stop、冰山、差分命令或预留令牌应用。

## RED → GREEN

1. 新增 `amend_reprice_full_fill_clears_shadow_reservation_without_zero_qty_construction` 后，
   改价全成错误构造 `Qty(0)`，定向测试以 `InvalidQty` 失败。
   改为只有严格正的 residual 才构造 `Qty` 后转绿。
2. 审查中新增 `amend_reduce_after_partial_fill_rebases_the_next_absolute_target` 后，
   部分成交后的同价减量仍保存旧绝对总量，测试断言 `Qty(10) == Qty(8)` 失败。
   同价减量现在原子写入新的绝对总量、余量与 TIF，后续绝对目标计算转绿。
3. Task5 审读将改价全成的预留断言从“保留”改为“终结即清除”后，
   测试复现旧路径无条件重插 shadow `ReservationState` 的失败。
   提交阶段现在只在订单仍为活动订单时保留该状态；部分成交与拒绝的保留对照仍通过。

## 已实现行为

- 同价减量保留 Vec 位置和 QueuePriority；绝对总量、正余量与 TIF 同步更新。
- 同价增量删除旧位置并追加至 Vec 尾部，且只在提交时分配一个新 QueuePriority。
- 改价在可丢弃的 Reference Vec 虚拟状态中预先规划成交；只有严格正 residual
  才分配新 QueuePriority 并挂回 Vec。
- 事件固定为 `OrderAmended → Trade* → Order*`；`OrderAmended` 的新余量为重新撮合前候选余量。
- GTC/GTD/DAY 迁移复用逻辑时间/会话校验；已迁移 DAY 订单进入既有 session expiry 路径。
- 所有目标总量、余量和优先级递增使用 checked 路径；Hard Gate A 的“目标必须严格大于已成交量”由领域候选保持。
- 所有普通业务拒绝在 Vec、FIFO、余量、优先级、TIF 和影子预留状态上保持原子。

## 预留边界

本小步只要求目标订单有身份、用户、市场匹配的 shadow `ReservationState`。
成功的非终结改价和所有拒绝都保持该状态及其 version 原样；订单因改价成交终结时，
沿用既有终结清理规则删除该 shadow state。
不验证 `ReservationAdjustmentToken`，不计算 required reserve，不 apply reserves 或 version，
也不构造假的 `AmendExecutionContext`。带 token 的常规入口明确以 `InvalidReservation` 原子拒绝，
避免未验签授权被成功接受或被错误解释为准备金覆盖；Task8 负责接入真实验签和 version 变更。

## 覆盖

- 同价减量、部分成交后二次绝对目标、同价增量尾插和新 priority。
- 改价部分成交/全成交、事件顺序、只在 residual rest 时分配 priority。
- 缺失 shadow state、Hard Gate A、无效 DAY、priority exhaustion 与未验签 token 的失败原子性。
- GTC → GTD 与 GTC → DAY，并验证 DAY 走既有 session expiry。
- shadow reservation 在部分成交/失败时保留 version，在改价全成终结时清除。

## 审查补强

独立审查后，失败原子性快照已扩展为完整确定性业务投影：源 Vec 中全部常规订单的
原始载荷、可见/隐藏余量、冰山峰值、FIFO 顺序、priority 与 TIF；全部 Stop 及
StopPriority；按 OrderId 排序的完整 `ReservationState`（含身份、version、reserve）；
两类下一 priority 和完整价格参考状态。无效 DAY 与未验签 token 用例现在都在同时含有
双侧常规订单、未触发 Stop、非空 reserve 与价格参考的簿上比较该完整快照。

新增 `amend_reprice_insufficient_trade_ids_panics_before_any_deterministic_state_change`：
有效的改价候选提供零个 TradeId，使用 `catch_unwind` 验证既有 fail-stop 断言在提交前发生，
并比较完整快照，证明没有订单、FIFO、余量、TIF、两类 priority、Stop、参考价或 shadow
reservation 漂移。该路径是调用方 TradeId 容量契约，不是可返回的业务拒绝；因此没有改变
任何 mutation 语义。

## 验证证据

- `cargo test -p matching-orderbook --locked amend_regular --lib`：11 passed。
- `cargo test -p matching-domain --locked`：153 unit、7 integration、2 doctest passed。
- `cargo test -p matching-orderbook --locked`：451 passed（终结清理修复后的最终回归）。
- `cargo fmt --check`：通过。
- `git diff --check`：通过。
- `cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings`：按冻结的 Task3+4 lint coupling 预期失败；非测试 Production candidate 尚无 Task5 consumer，且 Reference 的 crate-private 真实入口尚无上层命令路由 consumer，触发 `dead_code`。未添加 `allow`、`expect(dead_code)` 或 crate 外公开 preview API。

## Task3 + Task4 lint coupling

Candidate/plan 必须保留在非测试代码且仅 `pub(crate)`，以供之后 no-fail commit 使用；但当前仓库尚未有 Amend 命令路由，因此 Rust 的非测试 lib 分析无法观察到调用。Reference 的真实入口已消费 candidate；Production 候选保持真实代码，等待 Task5 的 Production consumer。严格 Clippy 不能在不扩大 API 或压制 lint 的情况下绿，因此本小步不能宣称独立最终 gate 完成。

## 未覆盖与停止点

Production Task5、Iceberg/Stop Amend、真实 reservation verification/apply、跨模型 differential、完整命令路由和 T13+ 均未实现。完成本小步后等待独立 review。
