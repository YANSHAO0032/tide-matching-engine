# T12 Task 4 terminal cleanup 限定复审

**Critical 修复已通过。** `crates/matching-orderbook/src/reference_orderbook/amend_candidate.rs:255–299` 在虚拟 Vec 中先移除旧挂单、只在 residual 严格为正时挂回新单；提交后 `reservations.retain` 清理所有已终结订单的 shadow state，且仅当目标 `OrderId` 仍在簿内才重插原 `ReservationState`。因此改价全成清除目标 shadow，部分成交仍保留原身份、内容和 version。

证据：`crates/matching-orderbook/src/reference_orderbook/tests/amend_regular.rs:255–299` 验证部分成交的目标 shadow version 保留、已全成 maker shadow 清除；`:324–348` 验证改价全成后目标订单及 shadow 均不存在。失败路径在写屏障 `amend_candidate.rs:283` 前返回或 fail-stop；完整快照测试覆盖无效 DAY、优先级枯竭、Hard Gate A、未验签 token 和 TradeId 不足后的状态不变（`tests/amend_regular.rs:302–322,350–375,401–491`）。

本次运行 `cargo test -p matching-orderbook --locked amend_regular --lib`：exit 0，11/11 通过。限定范围内没有新的 Critical 或 Important；未重跑完整 workspace gate。
