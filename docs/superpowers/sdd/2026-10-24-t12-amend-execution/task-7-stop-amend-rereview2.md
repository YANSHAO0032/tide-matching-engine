# T12 Task 7：修复轮 2 独立复审

## 裁定

**Task 7 限定范围通过。此前 root 保护参考漂移的 Important 已修复；Critical：0，Important：0，Minor：0。**

本次限定审查 `task-7-stop-amend-rereview.md` 的新增 Important 与修复轮 2 的实际改动，读取更新后的实施报告、两模型真实触发实现、完整 golden/helper、Reference 测试投影及实际工作区状态。原 Task 7 的 trigger-reference/protection 与 DAY/GTD Finding 已在上次复审确认直接缺口修复，本次确认 root 保护上下文修复未破坏其调用链。本次只新增此复审文件，没有修改 Rust、提交或推送。

## 原 Finding：已修复

- **唯一不可变 root 上下文：** Production `stop_trigger.rs:68` 与 Reference `stop_trigger.rs:72` 分别在 root 起点创建非 mut 的 `CommandStartProtectionContext`。该入口的 root 是显式参考价更新，用户保护参考一次采用更新后的 `reference.price`；后续每个 child 执行均传入同一 `&protection_context`。`execute_triggered_child` 不再接收 descendant trigger price，也不再构造新上下文，消除了后代成交价重冻结的路径。
- **后代与首批队列一致：** pending 仅持有订单、StopPriority、触发事件；初次收集与 Trade 后追加均走同一个循环和 child 执行分支，无法因为后代入队时点不同而采用另一保护参考。Base/Quote 两种市价 child 都向各自已有 protected executor 传入同一上下文。
- **触发状态继续推进：** 每笔真实 Trade 仍通过 `next_last_trade(trade.price)` 取得 checked 来源序号，再由各自的 `collect_triggered_stops` 更新参考状态、匹配来源及检测实际跨越。冻结的只是保护上下文，不是 LastTrade 的真实业务状态；没有为了修保护而阻断级联或来源隔离。
- **保留 root 更新语义：** golden 明确区分旧基线 90/110、root 新参考 100、child 成交价 110/90。LastTrade/MarkPrice/IndexPrice root 均使用更新后 100；Mark/Index 的 root 状态保持该新值，后代 LastTrade 检测仍由真实成交推进。完整 PriceBand 配置未接入、`market_band_worst=None` 的既定边界没有被冒称为最终 PriceBand 验收。

## Golden 与状态证据

`tests/amend_stop.rs:1370` 的 `root_frozen_cascade` 使用真实双模型 public Amend/trigger/protected execution，新增两项测试分别覆盖首批 child 与真正后代：

- Buy/Sell × Base/Quote × LastTrade/MarkPrice/IndexPrice 全矩阵；真正后代场景双次独立构造并比较 transcript。
- 首批 child 与后代先直接比较两模型原序事件，再与手写完整四事件 golden 比较：`StopTriggered(A) → Trade(A,1) → StopTriggered(B) → MarketProtectionTriggered(B)`。
- Buy 的 maker=110、Sell 的 maker=90；保护事件逐字段固定 B、next maker price、effective_worst_price=100、source=User。两种数量模式都禁止 B 的第二笔成交。该 golden 直接覆盖上次独立探针的反例，没有只依赖两个模型共同输出相等。
- 两模型 maker 原始载荷、真实余量 1、priority 相等；QueuePriority 仍为 1，StopPriority 下一值仍为 2，没有因被保护阻止的 B 再分配优先级。
- `ReferenceOrderBook::test_reference_state` 仅为 `#[cfg(test)] pub(crate)` 只读投影，直接比较完整来源状态。LastTrade 最终价格明确为实际成交 110/90，序号在 LastTrade root 时为 3、Mark/Index root 时为 2；Mark/Index root 最终仍为传入更新 100。由此证明实际跨越/来源序号没有随保护上下文一起冻结。
- A/B 的 Stop 身份、shadow 与旧 GTD 引用全部终结，真实到期调用不会再次删除；三个生产 validator 继续通过。保护事件保持原序，没有排序/过滤来掩盖多余成交。

## 回归与范围

本轮修改集中在两模型触发上下文、固定 golden 与测试只读投影。没有更改公开 trigger_stops 参数、逻辑时间/session 校验、Amend 写屏障、来源索引迁移、取消/到期 shadow 清理或 reservation 验签/apply。模型仍分别维护自身 Vec/StopBook 业务状态，没有引入相互调用或共享业务迁移实现。没有新增依赖、稳定原因编号、数值公式、浮点、unsafe、wall clock、网络/RPC、队列或 I/O。

成功 child residual 的 reservation 接续、完整 coverage/验签/apply、冰山自身 Amend、最终 Amend 100k tape 与 Task 8–11 的整体验收仍按原计划推进；本结论不是完整 T12 完成声明。WAL/crash/snapshot/durable digest 不属于本次修复，未宣称相应验证。

## 本次实际验证

2026-10-25 独立运行：

- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：20 passed，0 failed，474 filtered，exit 0；包含新增 root golden。
- `cargo test -p matching-orderbook --locked stop_trigger --lib --quiet`：9 passed，0 failed，485 filtered，exit 0。
- `cargo fmt --check`：无格式诊断；随后 `git diff --check` exit 0，只有既有 LF→CRLF 提示。

更新后报告中的严格 workspace Clippy、完整 workspace test/494 项 orderbook 结果是实施者另次实际运行证据，本复审未重复执行、不冒称独立运行。已读实际代码与本次定向结果足以关闭本轮唯一 Important；没有发现新的 Critical/Important。
