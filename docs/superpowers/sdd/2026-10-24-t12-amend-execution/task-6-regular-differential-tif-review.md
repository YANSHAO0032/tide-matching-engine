# T12 Task 6：常规差分与时效迁移独立审查

## 裁定

**Task 6 限定范围通过。Critical：0；Important：0；Minor：2（证据精度与后续投影补强，均不阻断本小步）。**

本次读取执行计划、Task 6 实施报告、Task 5 审查、实际未提交 diff、未跟踪的两模型 Amend 实现及测试、共享观察器和 expiry 调用链。隔离 worktree 没有 ignored `assignment/`，已从 `D:/rust_workspace/tide-matching-engine/assignment/` 读取工作规则、唯一交接、任务书及 v6 §20。以交接中用户已冻结的 Hard Gate A、事件载荷及实施授权解释历史任务书，不恢复旧导师限制。本次只新增此审查文件，不修改 Rust、不提交。

## 规格与质量检查

- **真实独立差分：** `tests/differential/amend_regular.rs` 的 `Harness::run` 分别调用 Reference/Production 的真实 `rest_timed_with_reservation`、`place_limit_timed_with_events`、`amend_regular_with_events`、Cancel 和到期入口。没有把生产结果输入参考模型，没有调用另一模型的候选或提交 helper。Reference 继续使用独立 Vec 片成交规划，Production 使用自身 Arena/价档预检。共享领域数量分类及观察结构没有替代独立撮合流程。
- **事件原序与事件值：** `Outcome::Events` 直接比较完整 `Result<Vec<EngineEvent>, RejectReason>`，没有排序或过滤事件。双向、多价档、部分/完全成交的 golden 用 maker 9→2 锁定价格成交顺序，精确断言 `OrderAmended → Trade* → 生命周期事件`、交易 ID/价格/数量、撮合前候选余量。全成不分配 residual priority，部分成交只为实际余量分配。
- **逐命令状态：** 命令前后比较原始订单完整载荷、实际余量、价序/FIFO、每单 priority、档位 count/aggregate/depth、best price、TIF、完整 shadow 内容/version 和两个 priority allocator。Reference 的价序来自稳定 Vec 视图；Production 沿真实侵入式链表观察，在遍历前验证结构，不能用 ID 排序掩盖错序。
- **到期索引：** Reference 自身 TIF 是独立扫描 oracle；Production 每步运行 `validate_regular_expiry`，双向检查 GTD/DAY 引用与活动节点一一对应、GTC 无引用，结合逐单 TIF 相等和真实 expiry 输出，足以验证本小步普通二级索引的业务语义。没有要求 Reference 复制生产的二级索引结构。
- **拒绝原子性：** 所有业务错误额外比较两模型各自前后快照。Reference 包含源 Vec 全字段、排序后的完整 shadow、Stop 源载荷、两个 allocator 与价格参考；Production 包含双侧价档元数据、Arena 全节点/free-list、排序后的完整 ID/shadow 映射、普通 expiry、Stop 有序载荷、两个 allocator 与价格参考。Debug 仅用于同模型结构相等，不冒充 durable digest；身份映射排序不会改变事件、FIFO 或成交序。
- **完整 TIF 矩阵：** Buy/Sell × GTC/GTD/DAY 九组迁移共 18 种组合均有直接测试，纯 TIF 修改保持订单投影、FIFO 与 allocator。GTD 20→30 在 20 不终结、30 终结，覆盖旧引用删除；30→20 也有独立场景。到期输出显式锁定时间优先、同时间 ID 优先（9→1→2），不是全局 FIFO 或仅按 ID。DAY/GTD maker 完全成交后再运行旧到期/session 入口，证明终结引用及 shadow 已清理。
- **非法时效及 Hard Gate A：** GTD 等于/早于 now、DAY session 错配、IOC/FOK 均明确拒绝并保持完整失败快照。双向先成交 3/10，再以绝对总量 8/12 修改，证明累计已成交量没有因 Amend 重置；目标等于已成交量 3 或低于它的 2 均拒绝，不创建零余量活动 Qty。无数量补丁继续使用最新绝对总量。
- **已有价档与 checked 预检：** 改价 residual 加入已有两单目标档时直接检查真实尾插、priority、count 和深度。Reference 新增 `preflight_amend_level` 独立扫描逻辑删除旧单后的目标档，以 checked-add 累加 visible 和 count；同价增量在 priority 分配前检查，改价在虚拟成交及 residual priority 预检后、写屏障前检查实际 residual。目标档聚合溢出不能先迁移 expiry 或消耗 priority。新增 unit 同时布置高水位和聚合溢出，固定既有 priority 错误优先级及失败不变性。
- **测试接口与确定性：** `ReferenceOrderBook::test_deterministic_state` 是 `#[cfg(test)] pub(crate)`，不存在非测试运行时公开 API 扩张。快照中的 HashMap 先按唯一 ID 排序，价格参考自身为 BTreeMap；源 Vec、价档、Arena 和 expiry 投影有确定顺序。测试 TradeId allocator 独立、checked 且每次重跑从相同值开始；当前命令域限定普通 maker、活动订单数不超过 128，因此固定准备 128 个 ID 足够，不能将此证明外推到冰山 maker。
- **历史回归隔离：** 新命令枚举局限于独立 `amend_regular` 模块。历史基础 `Command`、生成器版本、生成输入和 tape 适配未改；tracked differential 入口只增加子模块声明。相同常规混合 tape 的两次结果按逐命令原序记录比较，proptest 每步比较状态和输出。历史 100k/重复 10k 的通过不被包装成 Amend 100k 证据。

## Findings

### Critical

无。

### Important

无。

### Minor

1. **实施报告的同价上溢覆盖描述过宽。** `tests/differential/amend_regular.rs:366` 的同价增量聚合上溢测试只构造 `Side::Buy`，且保持 GTC；报告“新增证据映射”将其与双向目标价档上溢一起写为“双向”。改价目标档上溢确实遍历 Buy/Sell，并带 GTD→DAY 补丁。应收窄报告描述，或补成双向并带时效迁移。数值检查本身按相同 predicate 扫描两侧，已有常规双向数量/TIF 测试通过，因此这项证据描述偏差不阻断 Task 6。

2. **成功 transcript 的价格参考覆盖应在完整差分阶段补齐。** `tests/differential/amend_regular.rs:73` 的 `State` 不含 `PriceReferenceState`；价格参考只出现在拒绝前后快照，且这个 harness 从默认空参考开始。现有 Task 4/5 单模型拒绝测试另有非空参考证据，当前普通 Amend 调用链也没有参考价写入，故未发现行为错误，Task 6 的列明 gate 已满足。但不宜把当前成功 transcript 描述成包含所有未来行为状态的完整 replay 投影；Task 9/10 应加入非空来源价格/序号并逐命令比较，随后将 Stop 内部引用与 allocator 配置纳入相应完整状态证明。

## 本次实际验证

2026-10-25，独立运行以下命令，均 exit 0：

- `cargo test -p matching-orderbook --locked amend_regular --lib --quiet`：34 passed，0 failed。
- `cargo test -p matching-orderbook --locked differential --lib --quiet`：66 passed，0 failed，31.07s。
- `cargo clippy -p matching-orderbook --all-targets --locked -- -D warnings`：无诊断。
- `cargo fmt --check`：通过。
- `git diff --check`：通过，只有已有 LF→CRLF 提示。

Task 6 报告中的完整 orderbook 474 项和 domain gate 是实施者的另次验证记录，本审查未重跑、不冒称为本次结果。完整 T12 workspace gate、Amend 100k tape、真实 reservation coverage/验签/apply、Stop/冰山自身 Amend 仍属后续步骤。当前成功入口保留 shadow/version，带 token 明确拒绝，不意味着 §20.1 资金整合已完成。没有新增依赖、公式语义、wall clock、网络、RPC、unsafe、队列或持久化；当前小步不适用 crash/replay-digest 测试，也没有远端 CI 证据。
