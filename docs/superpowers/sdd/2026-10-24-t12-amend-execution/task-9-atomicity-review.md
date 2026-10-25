# T12 Task 9 独立原子性审查

日期：2026-10-25。结论：**通过；Critical 0 / Important 0 / Minor 0**。仅关闭 Task 9 的失败原子性与数值上下文审查，不代表整个 T12 已验收，也不授权提交、推送或合并。

## 审查依据与范围

独立读取当前真实源码、未跟踪测试、相关 diff、Task 9 报告和执行计划；直接核对用户绑定实施任务书 `C:/Users/10703/.codex/attachments/4b1d84b9-e4c2-4af6-a4af-1a0b2fc07723/已粘贴的文本.txt` §25/§29，并向下检查实际成交规划、Stop 提交、快照投影和底层 verifier。worktree 无 ignored `assignment/`，规则与唯一交接从主 checkout 的 `D:/rust_workspace/tide-matching-engine/assignment/` 读取；旧 `T12_P4_amend.md` 不代替本次绑定实施任务书。

本审查没有修改业务代码、测试、配置或唯一 handoff，没有提交。报告中的既有 RED/变异记录作为实施者历史证据读取，本次没有重新注入故障；本次新鲜证据为实际源码审查和下述定向命令。

## 核验结果

1. **完整确定性状态。** Production 的 `production_snapshot` 覆盖自身全部业务字段：双侧档位 head/tail/count/visible 聚合、Arena 全部槽位与节点/链引用/free-list、按 OrderId 排序的活动索引和完整预留 map、Stop 队列/位置/到期引用、普通 GTD/DAY 索引、两个完整 allocator 和全部参考来源状态。StopBook 的 `locations` HashMap 显式排序，队列和到期引用保持实际顺序。Reference 快照覆盖自己的源 Vec、完整 Stop 载荷、完整排序预留、两个计数器及参考来源；没有把生产容器复制为参考答案。Arena 的测试访问计数不是业务状态，未纳入合理。快照是本地相等投影，不是持久化 digest。

2. **非空 fixture 与重复拒绝。** 固定矩阵实际遍历 5 容器 × 2 方向 × 33 槽位，即 330 个独立场景；每个场景在同一实例上连续执行两次拒绝，分别断言明确原因、完整前后状态和校验器。槽位依容器映射适用失败，不把报价模式强行当作基础数量模式。fixture 具有真实部分成交、同价伙伴、双侧多档、普通与 Stop 的 GTD/DAY 引用、多资产预留、LastTrade/Mark/Index 的非空价格和 source_seq，以及插入后取消留下的 Arena 空槽。固定 seed `0xA12_FA11`、64 cases 的性质测试进一步生成容器、方向、数量扰动及拒绝槽位；各生成场景重新建立状态，再对同一状态重复拒绝。报告准确区分该性质与后续混合长 tape。

3. **§25 失败类别。** 主矩阵加数值/容量专项覆盖数量目标等于或低于真实已成交量、数量模式冲突、tick/lot、峰值 lot、候选及实际 maker 本金溢出、GTD/DAY、保护、普通/Stop priority 高水位、触发迁移、触发前 expiry 迁移、资金验签/身份/版本/费率/覆盖及版本递增溢出。普通和冰山的双方向、同价及改价目标档聚合溢出均带 TIF 和有效签名令牌；真实冰山 maker 补量所需 priority 失败也比较全状态。TradeId 短缺按已冻结的 fail-stop 契约捕获 panic，并证明首次写入前全状态及资金均不变，没有将它记作业务 Reject。

4. **显式数值上下文及顺序。** `AmendExecutionContext.market_math` 是必填的已验证值，不填默认步长。两模型各自先构造不可变 candidate，再执行各自 `preflight_amend_numeric`，然后才 `context.prepare`、底层完整簿预检/提交，最后仅为仍活动订单安装预留目标。普通/冰山候选本金调用 `MarketMath::trade_notional`，因此同时检查价格 tick 与余量 lot；峰值另校验 lot。Stop 直接检查候选触发价及 worst_price；StopLimit 检查 child 价格、数量与本金；StopMarketByBase 检查实际基础数量；报价模式保留其既有严格正值和不可切换模式规则。

5. **实际成交的独立数值预检。** 改价时 Production 从自身价档和侵入式 FIFO 调用 `preflight_iceberg_plan`；Reference 从自身源 Vec 调用 `plan_slices`。两路径都带实际 `MarketMath`，检查实际 maker 价格/片数量的本金并 checked 累加。候选限价本金可表示、实际 Sell maker 本金上溢的专项测试确实经过这层规划。共享内容仅为数值值语义和既定数学规则，不共享 candidate、成交计划或状态迁移。context-free 低层 API 明确要求调用方先校验数值和资金，报告没有将它们宣称为完整数值入口。

6. **底层需求列表独立证据。** `matching-risk/tests/amend_reservation.rs` 的 zero/duplicate/out-of-order 三测试直接调用 `verify_amend_reservation`，绕开订单簿 context 的前置过滤。helper 先证明有效签名、身份和两资产覆盖成功，再仅替换 `required_reserves` 并断言 `InvalidReservation` 与原 state 不变。逆序两资产均有真实覆盖，重复项也不存在覆盖不足，能够归因于 canonical 列表校验。旧 empty-list 直接 verifier 测试仍保留。

7. **约束与边界。** 本步未新增依赖或变更稳定 reason/event 编号；新增 getter 是无分支只读访问，并有非单位 lot 消费者测试。检查到的新增说明为中文；没有新增 float、unsafe、业务 wall clock、I/O、网络、锁或无界队列。HashMap 仅用于身份定位或排序后快照，不决定新增业务顺序。WAL、crash、持久化 snapshot/digest 不在本步范围。

## 本次实际验证

以下命令均在本 worktree 实际运行，进程 exit 0：

| 命令 | 结果 |
| --- | --- |
| `cargo test -p matching-orderbook --locked amend_atomicity_ --lib --quiet` | 9 passed，0 failed，7.26s |
| `cargo test -p matching-risk --locked signed_verifier -- --nocapture` | 3 integration passed，0 failed；另有 0 项 unit，未当作覆盖证据 |
| `cargo test -p matching-orderbook --locked amend_ --lib --quiet` | 85 passed，0 failed，7.25s |

本次遵循定向审查范围，没有重跑 workspace gate，也没有把实施报告中的全量 gate 或变异结果写成本次运行结果。

## 后续验收边界

§29 中与本步相关的 failed Amend 全确定性状态不变、失败不推进 reservation version，以及失败后 FIFO/聚合/priority 上界保持有效，已有上述生成性质与 validator 证据。成功优先级、身份和版本的既有固定/性质证据仍需保留；§29 全部成功命令的联合生成覆盖、§27 全状态逐命令比较和 §30 的 Amend 100k/双次 10k tape 属执行计划 Task 10，尚不能据本报告宣称完成。此为明确后续 gate，不构成 Task 9 缺陷。
