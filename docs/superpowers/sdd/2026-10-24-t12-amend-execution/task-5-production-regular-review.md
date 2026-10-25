# T12 Task 5：Production 常规改单独立审查

## 裁定

**Task 5 限定范围通过。Critical：0；Important：0；Minor：1（后续覆盖补强）。**

本次读取执行计划、Task 5 实施报告、Task 4 报告及三次审查记录，检查真实源码、未跟踪的 Amend 模块和测试、工作区 diff，以及主 checkout 中的唯一交接、任务书和 v6 §20。隔离 worktree 缺少 ignored `assignment/`，故从 `D:/rust_workspace/tide-matching-engine/assignment/` 读取；没有重建交接或修改 Rust。

这里通过的是 Production 常规订单的结构与撮合小步；真实 reservation coverage/验签/apply、Stop/冰山自身 Amend、Amend differential 和完整 T12 gate 仍未完成。当前成功入口保留 shadow 状态/version，带 token 明确拒绝，不能据此宣称完整 §20.1 资金授权已实现。

## 规格与实现检查

- **独立性与真实 API：** `production_orderbook/amend_candidate.rs:123` 是实际 `pub fn amend_regular_with_events(&mut self, ...)`，直接执行修改及输出事件；不是公开 preview、占位委托或测试专用入口。生产路径以 Arena、ID 索引、PriceLevel 和自己已有的 `preflight_iceberg_plan`/`commit_iceberg_plan` 工作，没有调用 Reference 候选、Vec 撮合或提交 helper。Reference 的同名公开入口也保留自己的 Vec 实现；共享领域数量分类不损害参考模型的流程独立性。
- **预检/提交边界：** 同价重排的 `preflight_amend_rest`（`:218–292`）先冻结删除旧单后的 count/aggregate/tail，并校验新 priority；改价的 `preflight_amend_reprice`（`:325–359`）先冻结全部片成交、每次冰山 refresh 的 priority、TradeId 容量及 residual RestPlan。`commit_amend_reduce`、`commit_amend_rest`、`commit_amend_reprice` 均不返回业务 `Result`；提交后的 `cancel`、`push_back`、priority allocation 仅以已有预检和独占访问保证的内部不变量断言收束，不存在成交后再返回普通业务拒绝的路径。
- **Arena/索引/聚合/FIFO：** 同价减量经 `PriceLevel::amend_regular_in_place` 原位减少聚合并保存最新绝对总量、正余量和 TIF，保持 slot、双向链接及 priority。增量先删旧节点再接目标档尾；旧单本为尾时，预检使用其 predecessor，单节点时改用 New 目标计划。旧单删除保证有一个可复用 Arena 槽位；对手侧 fills 不会增加本侧挂单容量要求。`commit_rest_target` 再核对冻结的 count/aggregate/tail，ID 映射和 allocator 随后同步提交。测试直接覆盖头/中/尾/单节点重排及双侧减量/增量。
- **改价与冰山 maker：** 生产预检实际沿价格序和 FIFO 规划每片成交，而不是用普通 maker 数量估算 TradeId；补量的可见聚合 checked-add、priority 高水位和 residual 分配都在旧单 unlink 前检查。普通与冰山 TradeId 短缺用 `catch_unwind` 证明 fail-stop 在写入前；refresh priority 耗尽同样保持完整快照。全成在 allocator ceiling 上无需 residual priority 的直接测试通过。
- **Hard Gate A 与绝对目标：** `build_regular_plan` 使用 `remaining_after_target_total`，等于已成交量的目标被 `Qty::try_new(0)` 拒绝，不形成活跃零余量；同价减量更新最新绝对总量，后续改单继续以实际累计成交量计算。全成改价的 residual 为 `None`，不构造 `Qty(0)`。
- **shadow 生命周期：** 重排和改价只在新订单仍活动时重插原完整 `ReservationState`；改价全成清除目标 shadow，maker 全成由生产片成交提交同步清除其 shadow 和普通 expiry 引用。部分成交、同价修改及拒绝保留内容/version。没有以空上下文、假验签或伪造 version 递增冒充资金整合。
- **时效迁移：** 候选先校验 GTD 严格晚于传入逻辑时间、DAY 当前 session 及可挂单 TIF。原位修改删除旧引用再登记新引用；重排/改价通过 cancel 清旧引用，只有实际 residual rest 才插入新引用。九组 GTC/GTD/DAY 类型迁移均经 validator 和实际 expiry/session 入口验证；纯 TIF 修改保持 slot/FIFO/priority。
- **事件：** `OrderAmended` 在所有 Trades 前，`new_quantity` 是重新撮合前候选余量。`commit_amend_reprice` 保持 Trade 原序，将现有 maker 生命周期/refresh 事件按原相对顺序置于 Trades 后，再追加目标部分/全成事件。无成交只产生改单事件；全成不虚构余量事件。
- **失败原子性与校验器：** `tests/amend_regular.rs:55–104` 快照覆盖全部 Arena 节点/free-list、排序后的 ID 索引、完整 shadow、双侧价档端点/count/aggregate、Stop 载荷/priority、两个 allocator、普通 expiry 和参考价。拒绝与 TradeId fail-stop 后比较快照，并运行 book、regular-expiry、Stop 校验器；成功后也运行同组校验器。无效时效/token 等测试实际布置了双侧订单、非空 reserve、GTD Stop 和参考价，未用空状态替代证据。

## Findings

### Critical

无。

### Important

无。

### Minor

1. **补强有状态目标迁移的直接覆盖。** `tests/amend_regular.rs:353–394` 的 GTD→GTD 用例使用相同的 `expire_at=20`，尚未直接证明 GTD A→GTD B 的旧时间引用清除；`:239–305` 的改价余量场景也尚未直接构造已有同侧目标价档。代码审阅确认两条路径分别执行 remove-old/insert-new，以及冻结目标档的真实尾/count/aggregate，未发现错误；建议 Task 6 增加 GTD 20→30 后在 20 不到期、30 到期，以及改价 residual 排到已有目标档尾的测试，并将目标档聚合溢出的拒绝纳入失败快照。此为覆盖补强，不阻断当前 Task 5。

## 质量与范围

本次审查的新增实现没有 lint suppression、公开 preview 或测试专用生产入口；严格 Clippy 已实际通过，先前 dead-code coupling 已由正式 API consumer 路径解除。新增手写说明为中文；数量、聚合和 priority 的业务算术使用 checked 路径。没有新增 wall clock、网络/RPC、随机业务选择、unsafe、锁、无界队列或 I/O。HashMap 仅定位身份；业务顺序由价档/FIFO 决定。当前变更没有 WAL/snapshot，crash/replay-digest 测试不适用，完整命令重放和 Amend differential 仍属后续 gate。

实施报告另述的 Stop 聚合到期 shadow 清理问题不在常规 Amend 的调用链上，本次没有将其归为 Task 5 finding，也没有修改该既有路径。

## 本次实际验证

2026-10-25，以下命令均 exit 0：

- `cargo test -p matching-orderbook --locked amend_regular --lib`：23 passed，0 failed（Production 12、Reference 11）。
- `cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings`：通过，无诊断。
- `cargo fmt --check`：通过。
- `git diff --check`：通过；只出现已有 LF→CRLF 提示。

完整 orderbook 463 项回归采用 Task 5 报告中的同一实现验证记录，本次未重复；未运行 workspace gate、远端 CI 或 Amend 100k tape，不将历史非 Amend differential 冒称 Amend 验收。
