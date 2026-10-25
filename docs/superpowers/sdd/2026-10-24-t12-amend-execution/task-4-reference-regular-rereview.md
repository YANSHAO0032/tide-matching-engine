# T12 Task 4 限定复审

## 结论

上次两项 Important **均已 addressed**。本次限定范围内未发现新的 Critical 或 Important。

1. **完整 deterministic snapshot：已修复。** `crates/matching-orderbook/src/reference_orderbook/tests/amend_regular.rs:61–109` 现按原始 Vec 顺序保存全部常规订单载荷、可见/隐藏余量、峰值、QueuePriority 与 TIF；保存全部 Stop 及 StopPriority、按 OrderId 排序的完整 `ReservationState`、两个 priority 分配器和完整 `PriceReferenceState`。Vec 同时包含双侧订单，因此 FIFO 与两侧状态由顺序比较覆盖。`seed_unrelated_deterministic_state`（`:112–152`）在无效 DAY 和未验签 token 测试中实际布置了双侧订单、Stop、非空 reserve 与参考价（`:351–375,459–487`）；其他失败用例也改用同一个完整快照。Reference 的 GTD/DAY 到期由订单 TIF 扫描，不存在另一个待比较的常规 expiry index。

2. **TradeId 不足：已修复。** 新测试（同文件 `:302–322`）使改价候选需要一笔成交却提供零个 TradeId；`catch_unwind` 捕获 fail-stop，随后比较完整快照。实现中 `plan_slices(&self, ...)` 只规划克隆的虚拟 Vec，`trade_ids.len()` 断言位于 `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:220–223`，实际写入从 `:283–292` 才开始。因此短缺在任何 commit 前停止，订单、两类 priority、Stop、TIF、参考价及预留均无漂移。

## 定向验证

`cargo test -p matching-orderbook --locked amend_regular --lib`：exit 0，11/11 通过。本次未重跑完整 workspace gate；Task 4 报告记录的严格 Clippy `dead_code` coupling 仍待后续非测试 consumer 接入。
