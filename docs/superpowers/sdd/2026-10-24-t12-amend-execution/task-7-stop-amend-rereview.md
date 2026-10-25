# T12 Task 7：修复轮 1 独立复审

## 裁定

**Task 7 仍未通过。原两项 Finding 的直接缺口已经修复；新增 Important：1，Critical：0，Minor：0。**

本次限定复审 `task-7-stop-amend-review.md` 的两个 Finding 与修复引入的来源、保护、时效执行路径，读取更新后的实施报告、真实 diff、领域 Stop 载荷、两模型 candidate/action、StopBook/expiry 及实际触发调用链。只新增此复审文件，没有修改仓库 Rust、提交或推送。原实施报告前文的 deferred 内容已由末尾修复轮及文件顶部声明明确取代。

## 原 Finding 裁定

| 原 Finding | 裁定 | 代码与证据 |
|---|---|---|
| Critical：trigger-reference/protection 全部拒绝 | **直接缺口已修复**；保护级联仍有下述新增 Important | StopLimitOrder/StopMarketOrder 实际持有 trigger_reference，StopMarketOrder 实际持有 market_protection；两模型 candidate 分别解析 Keep/Clear/Set，保存 old/new reference/protection；合法补丁不再一律拒绝。Production 的真实档位键、位置和 expiry 引用都包含来源，Reference 仍独立扫描 Vec。来源或价格实际改变才分配新 StopPriority；相同来源/纯保护修改保持原位置。 |
| Important：DAY/GTD 仅保存模板、触发时无条件拒绝 | **已修复** | 两模型 trigger_stops 显式接收 now/current_session_id，按 GTD 严格大于 now、DAY session 相等重校验；通过后执行各自 place_limit_timed_with_events。新增真实触发测试证明 Trigger-R1 被调用、成交 2/8、余量 6 携带正确 TIF 并由真实 GTD/EndSession 终结。非法时效在 Trigger-R1 前按既定终结事件链拒绝，无 Trade/rest/QueuePriority/shadow 残留。 |

## 新增 Finding

### Important：后代 Stop 重新冻结成交价，导致同一 root command 内的保护边界漂移

位置：`production_orderbook/stop_trigger.rs:118`、`reference_orderbook/stop_trigger.rs:120`，以及两模型 `execute_triggered_child` 中用 `trigger_price` 构造 `CommandStartProtectionContext` 的路径。

首批 pending child 保存 root reference.price，但前一 child 的 Trade 触发后代 Stop 时，代码给新队列项保存的是 `last_trade.price`。后代 child 随后用该成交价重新计算自身 bps 保护。因此，虽然一个**已经排队的** child 不会被另一个 child 改写参考，整个 root command 的后代 child 仍会采用变化后的参考。这不符合 v6 §18.4、§26.3–§26.5 的 root command 有效保护参考冻结契约：UpdateReferencePrice 可在 root 起点采用新配置来源价格，后续 Trade 只能更新触发检测状态，不能继续放宽本 root 的保护边界。

本次用当前已构建库执行独立 stdin 编译探针，在两个模型均复现：

1. LastTrade 基线 90，卖方 maker 为价格 110、数量 2。
2. 买方 StopLimit A：触发价 100，child 限价 110、数量 1。
3. 买方 StopMarket B：触发价 105，Base 数量 1，保护为 0 bps。
4. root 更新 LastTrade 90→100，只先触发 A。A 在 110 成交 1，随后 LastTrade 更新到 110，B 因跨越 105 入队。
5. 当前两个模型都让 B 在 110 再成交 1，总计 **2 笔 Trade**；root 冻结参考应为 100，B 的 0 bps 买方上界仍应为 100，所以应只有 A 的 **1 笔 Trade**，随后 B 发出 MarketProtectionTriggered，maker 留余量 1。

该探针调用公开真实 trigger_stops/protected execution，没有绕过任何候选、成交或保护逻辑。编译成功、运行 exit 0；两个模型原序输出都明确出现 B 为 taker 的 Trade(110,1)。这也说明 Reference/Production 一致并不能检出两个独立实现共同使用错误参考的情况。现有保护矩阵只测试首批 child，没有包含这种有成交的保护级联。

修复方向：在 root 起点冻结有效 `CommandStartProtectionContext`（若需要来源选择，冻结完整来源快照和明确的配置选择），供本 root 全部 child/后代使用；LastTrade 的不断更新仍仅驱动来源匹配与跨越检测。不得把 descendant 的成交价当作新的 root reference。补充上述固定 golden、Sell 镜像和 Base/Quote 模式，以及首批多个 child 与真正后代分别验证。保护边界与事件应使用明确预期值，不能只比较两个模型。完整 PriceBand 尚未接入的阶段边界不能成为允许 user bps 参考漂移的理由。

### Critical / Minor

无新增。

## 其余 scoped 检查

- **来源档位与 FIFO：** Production 以 `(stop_price, trigger_reference)` 分区，重排同时更新 locations 与 expiry reference 的完整来源位置；validator 核对载荷、键、位置、到期引用和档内 priority 单调性。改价或改来源使用同一个 requeue 判断，只分配一次 StopPriority。Reference 独立在源 Vec 替换/删除追加，以自身价格/来源/priority 视图观察。来源变更进已有目标档尾、重复相同来源保序、旧 GTD 引用不再生效都有实际测试；LastTrade 更新不再触发 Mark/Index 订单。
- **保护补丁与模式：** None/Keep 保留、Clear 清空、Set 替换实际 Market 模板；纯保护变化不分配任一 priority。Limit child 的 Keep/Clear 对 None 有效，Set 市价保护仍拒绝，符合市价保护字段的订单类型限制。StaticOpening 和 bps>=10,000 在 placement/amend 首次 mutation 前拒绝；同时携带来源/到期迁移的拒绝比较完整状态快照。Base/Quote child 均真实调用各自 protected execution，原序保留保护终结事件；首批 LastTrade/Mark/Index 的 0 bps 场景均有实际越界证据。
- **显式时间与 API：** 新签名在两模型均要求 `LogicalTime` 与 session，不从来源序号或 wall clock 推断。仓库全部 trigger_stops 调用点都已改为显式传入，Stop fixture 明确填写 LastTrade/None，不在运行时推测来源或伪造保护。DAY/GTD child 校验发生在 Trigger-R1 前，实际余量复用各自 timed placement/expiry 生命周期；没有把 StopPriority 当 QueuePriority。
- **失败原子性：** Amend 仍先形成不可变候选和完整替换动作，再进入无普通业务 Err 的 commit。新来源、保护字段进入真实载荷和快照，source-only 高水位拒绝不迁移来源/expiry、不改 shadow 或 allocator。Production/Reference 没有共享业务迁移实现；共享领域补丁枚举和保护数值 helper 属公共协议/算术契约。
- **终结清理：** 收集 Stop 时先删除 StopBook/expiry/shadow，再重校验和执行 child；非法 child 时效或 Trigger-R1 拒绝按 StopTriggered→TriggeredOrderRejected→ReservationReleased 输出，不产生第二个 CommandRejected 或成交。聚合 GTD/EndSession wrapper 的 shadow 清理保持有效。成功 child 实际 rest 的 reservation 接续仍属明确 Task 8 集成边界，本复审不冒称该资金闭环已完成。
- **工程范围：** 无新增依赖、reason 编号、金融浮点、unsafe、wall clock、网络/RPC、锁、无界队列或 I/O。新字段进入本地状态；未涉及 WAL/snapshot，未伪造 durable replay/digest 或远端 CI 证据。复用既有保护数值公式，不要求为了该修复新增公式文档。

## 本次实际验证

2026-10-25 独立执行：

- `cargo test -p matching-orderbook --locked amend_stop --lib --quiet`：18 passed，0 failed，474 filtered，exit 0。
- `cargo test -p matching-orderbook --locked stop_trigger --lib --quiet`：9 passed，0 failed，483 filtered，exit 0。
- `rustc --edition=2024 --crate-name t12_stop_root_protection_review - ...`：从 stdin 编译独立探针，链接当前实际 matching-domain/orderbook rlib，输出到系统临时目录；编译和运行 exit 0，Reference/Production 均实际输出 2 笔 Trade，确认上述保护漂移。

实施者另次完整 workspace test/Clippy/fmt 的成功记录见更新后的 Task 7 报告，本次没有重复执行或冒称为独立运行证据。既有 18+9 项定向全绿不能覆盖缺失的后代保护冻结 golden。修复新增 Important 后需再做 scoped review，不能开始以 Task 7 已通过为前提的下一步骤。
