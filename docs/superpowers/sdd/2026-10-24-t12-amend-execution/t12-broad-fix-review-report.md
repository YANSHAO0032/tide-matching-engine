# T12 broad fix 独立 scoped 审查报告

日期：2026-10-25。结论：**通过本次 scoped re-review；未发现 P0、P1 或 P2 问题。** 本结论只覆盖 broad fix 的四类缺陷及相关调用链，不代替整个 T12 的最终验收、发布授权或远端 CI。

## 范围与仓库事实

- 审查目录：`C:/Users/10703/.codex/worktrees/t12-amend/tide-matching-engine`。
- 分支：`codex/t12-amend`；基线 HEAD：`550fda02816c73a5704b4ffe836fcfb31ffff08f`。实际审查对象是 HEAD 上的未提交工作区，不是已提交版本。
- 已读取根 `AGENTS.md`、真实 git status/diff、`t12-broad-fix-report.md` 和相关实现/测试/说明。worktree 缺少 ignored `assignment/`；改从主 checkout `D:/rust_workspace/tide-matching-engine/assignment/` 读取指导、唯一交接、T12 任务书及 v6 §7.4.2–§7.4.3。交接中用户冻结的严格总量边界和完整事件协议优先于旧任务书。
- 本轮未修改业务源码、测试、依赖、Git index，未提交或推送，也未创建子代理。只新增本报告；临时公开 API 探针位于 ignored `target/t12-scoped-review-probe/`，运行后移除其源码和 manifest，编译产物仅留在 ignored target。

## 发现

| 严重性 | 发现 |
|---|---|
| P0 | 无 |
| P1 | 无 |
| P2 | 无 |
| P3 | 无需要跟进的 scoped 问题 |

## 四类修复的独立核对

### 1. 部分立即成交后的绝对总量与同价减量

生产 `production_orderbook/iceberg.rs:67` 和参考 `reference_orderbook/iceberg.rs:53` 均在调用原有 GTC 撮合前保存 accepted payload；只有实际正余量仍活动时，才恢复完整接收总量并安装 shadow。已全成路径不创建 shadow。普通和冰山 maker 两条执行路径都回到同一余量修复点，Reference 从自己的源 Vec 查找，不读取生产结果。

`OrderNode::amend_regular_in_place` 与 `ReferenceOrder::amend_regular_in_place` 不改 priority 或队列位置。生产同价减量经 `PriceLevel::amend_regular_in_place` checked 扣减深度，在原 Arena slot 更新；参考同价减量直接修改原 Vec 元素。两个候选均从 accepted total 减实际 remaining 推导 already-filled，因此 10 → 成交 3 → 余量 7 → 总目标 9 得到余量 6，分类为保序减量。

新增 `broad_review_partial_placement_preserves_absolute_total_for_later_amend` 覆盖买卖两侧 × 普通/冰山 maker。额外公开 API 探针在真实部分立即成交后追加同价订单，再执行完整上下文改单和对手单撮合，直接断言 maker 顺序为原目标 6、后单 1；双模型原序事件一致，原 priority/allocator 保持，目标全成时清理 shadow。没有仅以两个模型相等替代期望业务结果。

### 2. 初始化身份绑定

生产 `production_orderbook.rs:170`、参考 `reference_orderbook.rs:99` 的 helper 在首次撮合/挂单前校验实际 order_id/user_id、重复活动 ID 和 shadow 冲突。普通 rest、timed rest、GTC、冰山四条公开 reservation-aware 路径均传入真实订单 UserId。

两个 `stop_orders.rs` 的 `place_stop_with_reservation` 从真实 StopLimit/StopMarket 载荷读取 UserId，并把 `StopOrder::reservation_id()` 与 shadow ReservationId 比较；检查先于 StopBook/Vec 和 StopPriority 首次写入。后续 Stop Amend 克隆原载荷，只修改允许字段；触发批次先移除 Stop 及其 shadow，仅实际活动 StopLimit child 接续原 shadow。Reference 的批量 retain 同时保留未触发 Stop，生产按已终结 ID 清理。

新增普通错误用户回归包含可成交 maker，证明拒绝发生在成交前；StopLimit/StopMarket 分别拒绝错误用户与错误 ReservationId。完整生产快照包含 Arena/free-list、价档/ID、真实 Stop/expiry 引用、两个 allocator、参考价和完整 shadow；参考快照包含源 Vec、Stop、两个 allocator、参考价和完整 shadow。错误订单 ID/重复 ID 的既有回归保留。

初始化没有独立 MarketId 输入可与 shadow 比较，修复没有新增或伪造市场身份 API；`reservation_snapshot` 和 `AmendExecutionContext::prepare` 仍将后续 Amend 的市场、用户、订单身份绑定到已存 shadow。此信任边界在 `docs/amend.md` 明确记录。

### 3. 未触发 Stop 的固定费率与调整令牌

生产 `amend_candidate.rs:172/183` 与参考 `amend_candidate.rs:167/178` 的数值预检，将上下文 fee_schedule_version 与已保存 Stop 载荷比较，覆盖 StopLimit、StopMarketByBase 和 StopMarketByQuote。错误返回既有 `InvalidReservation`，早于 `context.prepare`、任何真实簿写入和 reservation version 更新。

`context.prepare` → `verify_amend_reservation` 继续校验真实签名、CommandId、订单/用户/市场/预留身份、expected version、上下文固定 fee version/tier 和目标资产覆盖；checked version 递增只在可丢弃副本发生。后续簿校验全部通过后，才为仍活动目标安装一次 planned state。Stop 替换保留原固定费率字段，没有把 context 写成新固定策略。

新增回归覆盖三类 Stop × 无 token/真实签名 token：即使错误 token 与 context 相互一致，也不能偏离原 Stop 固定版本；拒绝后完整状态相等。完整 suite 同时保留正确版本的签名成功、重放拒绝、后续 priority/expiry/版本溢出原子性、触发后 residual version 接续以及终结清理证据。低层无上下文接口要求调用方已校验并拒绝直接携 token，未把它宣称为完整 R1。

### 4. root LastTrade 上溢预检与批处理

生产 `stop_trigger.rs:154`、参考 `stop_trigger.rs:169` 在有界 `PriceReferenceState` 副本上先检查来源并 `advance(root)`，再从更新后 LastTrade 做 checked `source_seq + trade_ids.len()`。预检调用位于真实 `collect_triggered_stops` 之前；静态/陈旧来源与容量拒绝均不更新真实参考、不移除 Stop、不撮合、不改 shadow。

真实级联只为每个实际 Trade 使用一次 `next_last_trade`；每个 child 消耗的 TradeId 计入同一 used_trade_ids，并从剩余切片执行后续 child，所以输入 TradeId 容量是整个批次和后代成交数量的上界。修复沿用既有保守容量规则，没有在提交后回滚整簿。缺省 LastTrade 时首笔从 1 开始；已存在来源时 root 使用更新后的值；MarkPrice/IndexPrice root 保留既有 LastTrade 水位。

新增回归以完整快照固定 root=u64::MAX 且需一笔后代成交时的零写入拒绝，以及 MAX−1 恰好成交一次的成功对照。额外公开 API 探针使用 LastTrade=MAX−2、MarkPrice root 一次触发两个 child，确认两笔 Trade 原序相等、各 child 终结 shadow 清理，之后再次输入 LastTrade=MAX 被识别为 stale，独立证明跨来源整批恰好推进到最大值。既有真实后代级联和 root 冻结保护测试在完整回归中通过。

## 本轮实际验证

以下均为本审查代理实际执行的新证据，未把实施报告中的历史 gate 当作本轮命令结果。

| 命令 | 结果 |
|---|---|
| `cargo test -p matching-orderbook --locked broad_review --lib -- --nocapture` | exit 0；5 passed，0 failed |
| `cargo test -p matching-orderbook --locked --lib --quiet` | exit 0；534 passed，0 failed，38.60s；包括完整 differential/property、固定 seed 100k 与双次短 tape |
| `cargo run --offline --manifest-path target/t12-scoped-review-probe/Cargo.toml --quiet` | exit 0；上述双向 FIFO 和跨来源双 child 最大序号公开 API 断言全部通过 |
| `git diff --check`、`git diff --cached --check` | exit 0；输出仅为既有 CRLF 转换提示，没有空白错误 |

本轮是 scoped 只读审查，没有重跑 workspace Clippy、workspace test、Rustdoc 或公式渲染；这些最终 gate 的最近证据由修复实施报告和唯一交接维护，不能据本报告声称新的远端 CI/页面验证。

## 工程边界与交接

此次修复没有新依赖、浮点金融计算、unsafe、wall clock、网络/RPC、无界队列或协议字段/reason 扩展。新身份/费率检查都是只读比较；总量恢复是已有无失败载荷提交；参考副本预检不改变真实状态。HashMap 按 ID 访问，不决定业务顺序；两个模型保持各自的匹配、候选和提交实现。中文注释与实际职责一致。

本修复未改 I/O、WAL、snapshot 或 HA，crash/truncation 与 durable replay digest 不适用；本地 deterministic tape 不冒充持久化重放证明。简单 getter/内部参数转发由跨入口业务断言覆盖，无需机械增加同形单测。

根代理应把 scoped 通过与本轮命令证据同步到唯一 `D:/rust_workspace/tide-matching-engine/assignment/SESSION_HANDOFF.md`，再按整个 T12 的当前授权和最终验收流程收尾。本报告不授权提交、合并或推送。
