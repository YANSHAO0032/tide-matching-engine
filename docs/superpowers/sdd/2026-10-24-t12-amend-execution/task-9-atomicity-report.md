# T12 Task 9：拒绝原子性性质与数值边界报告

日期：2026-10-25。状态：实施与本地 gate 已完成，等待独立审查；没有进入 Task 10，没有提交、推送或合并，不代表整个 T12 已验收。

## 范围与读取

本步在 `codex/t12-amend` 当前 worktree 保留全部 Task 0–8 未提交成果。读取执行计划、绑定实施任务书 §25/§29、Task 6–8 的报告、独立审查及 Stop 修复轮复审，并核对真实源码、未跟踪文件和 git 状态。worktree 没有 ignored `assignment/`，沿用主 checkout `D:/rust_workspace/tide-matching-engine/assignment/` 的规则和唯一 handoff。本报告供父执行会话同步唯一 handoff，不创建第二份权威交接。

范围为五种容器（普通限价、冰山、StopLimit、StopMarketByQuote、StopMarketByBase）的拒绝原子性、明确数值上下文，以及 Task 1 遗留的底层 verifier 独立需求列表证据。成功优先级/版本性质已有 Task 6–8 证据；§29 的完整成功命令联合性质及长 tape 仍由后续步骤完成。

## tick/lot 所有权裁定及实现

检查发现 `Price`/`Qty` 构造只验证严格正值；此前 Amend 入口没有实际市场数值配置，因此不能证明 tick/lot，也没有在改价实际 maker 成交上核验本金。父执行会话明确冻结：将最小已验证数值约束显式放进 `AmendExecutionContext`，不填默认 tick/lot，不虚构资产或费率映射；context-free legacy Amend 不作为 tick/lot 已覆盖的入口。

实际新增 `market_math: MarketMath` 必填字段。两个已有 context 基础 fixture 都显式使用原场景实际的 tick=1/lot=1；新边界测试使用 tick=5/lot=2 或显式大 scale。没有运行时默认配置或新的依赖。

两模型在 `amend_with_context` 各自取得自己的 immutable candidate，再各自执行数值预检，之后才进入资金副本准备和真实业务提交：

- 普通/冰山验证候选价格、候选基础余量与 checked 本金，冰山峰值另核对 lot。
- Stop 验证实际候选触发价、候选保护最差价的 tick；StopLimit 核对 child 候选限价、数量与本金；StopMarketByBase 核对 child 基础数量；报价模式保留严格正 quote budget 和禁止切换数量模式的既有校验。
- 改价可能成交时，Production 用自身 `preflight_iceberg_plan(..., Some(&market_math), ...)`，Reference 用自身 `plan_slices`/源 Vec，加上同一不可变数学规则，检查真实每笔 maker 本金和累计本金。候选限价本金可表示不代表实际 Sell maker 价格的本金可表示，因此保留实际成交规划。
- 新增 `MarketMath::lot_size` 只读 getter；其对非单位 lot 的实际消费者测试覆盖它，不为无分支 getter 机械补镜像测试。

共享 helper 仅表达价格/数量对齐的值语义；两模型没有调用另一模型的候选、价档、成交或状态迁移。数值规划完成后，既有完整低层计划仍再检查并提交；本步没有优化或合并这两层规划。公开低层接口文档明确由调用方先完成数值/资金校验，完整核验命令使用 `amend_with_context`。

## RED → GREEN 与变异对照

1. 先新增真实入口 tick/lot 矩阵，只添加必填 context 字段而不加校验。运行 `cargo test -p matching-orderbook --locked amend_atomicity_numeric_alignment --lib -- --nocapture`：shell exit 1，0 passed / 1 failed。tick=5 时新价 103 错误成功发出 `OrderAmended`，预期 `InvalidTick`。随后才加入两个模型的数值预检；同一矩阵转绿，且新增非单位 tick/lot 的五容器成功对照，排除无条件拒绝。
2. 底层 verifier 的 zero/duplicate/out-of-order 三个独立测试首次执行即通过既有保护，不冒称发现业务 bug。为证明分支敏感性，精确保存原字节，在 `try/finally` 内仅临时禁止 `required.atoms == 0 || required.asset <= previous` 条件；运行 `cargo test -p matching-risk --locked signed_verifier -- --nocapture`：exit 101，0 passed / 3 failed，三者均错误返回 `Ok(VerifiedReservationAdjustment)`。finally 恢复原字节，同一命令 3 passed、exit 0。签名及两资产覆盖的成功对照先通过，错误原因不是签名或覆盖不足。
3. 对完整快照做一次受控变异：在 Production `amend_with_context` 候选构造前临时分配一个 QueuePriority，原错误原因保持不变。运行 `cargo test -p matching-orderbook --locked amend_atomicity_every_reject --lib -- --nocapture`：exit 101，0 passed / 1 failed；完整前后快照准确发现 allocator next=5→6。finally 恢复精确字节，再运行全部原子性测试转绿。源码不保留探针或 lint 例外。

测试开发中有字段名 `explicit_worst_price`/实际 `worst_price`、不存在的 Production `orders` 方法、Cancel 返回类型的编译错误；均已修正，不作为业务 RED。另修正一次 trigger-expiry DAY 拒绝预期：既有协议对非法触发前 DAY 返回 `InvalidExpireTime`，普通/child DAY 返回 `InvalidSession`；本步没有更改这些既有稳定原因。

## 完整确定性快照与生成性质

新增 `production_orderbook/tests/amend_atomicity.rs` 共 9 项测试。

固定矩阵遍历 5 容器 × Buy/Sell × 33 个拒绝槽位，共 330 个独立场景，每个在同一实例上连续拒绝两次。槽位按类型映射相应合法失败类别，不把报价模式伪装成存在 lot：

| 类别 | 证据 |
| --- | --- |
| 身份和数量 | 错用户/市场/订单；同时 qty+quote；模式切换；普通/冰山先真实成交 3/10 后目标等于 3 或低于 3；零 quote；非法峰值/显示字段 |
| 时间和 Stop 迁移 | 非法 child/普通 IOC/FOK、GTD 等于 now、错 DAY session；非法触发前 GTD/DAY 同时请求触发价迁移；非法 StaticOpening 来源；StopMarket child 非法限价 |
| 保护和 priority | Limit/普通保护非法组合；Market bps=10000；QueuePriority/StopPriority 真实高水位；真实冰山 maker 补量需要的新 priority 失败 |
| 资金 | 真实签名后的 stale version、错误 CommandId/订单/用户/市场/预留身份、费率版本/层级、未知 key、损坏签名、目标覆盖不足、无 token 覆盖不足、空/零/重复/逆序 requirement、version=u64::MAX 增量上溢 |
| 数值和容量 | tick/lot、峰值 lot、保护最差价 tick、候选本金与实际 maker 本金溢出；双向普通/冰山同价增量及改价已有目标档聚合上溢；TradeId 短缺 |

固定 seed `0xA12_FA11` 的 proptest 使用 64 cases，生成容器、方向、非空簿数量/多资产影子值及 1–11 个拒绝场景索引。每个生成场景独立建立真实状态，并重复拒绝两次，证明任一失败不会改变确定性状态。它是生成式失败性质，不是 Task 10 的长混合命令 tape。

复杂 fixture 包含：同价 FIFO 伙伴、双侧多档深度、真实部分成交、普通 GTD/DAY 索引、Stop GTD/DAY 索引、LastTrade/Mark/Index 三个非空价格和 source_seq、完整多资产 ReservationState，以及真实插入/取消产生的 Arena 空闲槽。

Production 快照包含双侧档位元数据、Arena 全部槽位/节点/链引用/free-list、按 ID 排序的活动索引和完整 reservation map、真实 StopLevel 队列/位置/到期引用、普通 GTD/session 索引、两个完整 allocator（含 policy/guard/ceiling/next）及完整来源价格状态。新增 StopBook 的 `#[cfg(test)]` 投影显式排序 `locations` HashMap，业务队列与 expiry 引用保留原序。Reference 保留自身完整源 Vec、Stop 源载荷、排序后的完整 shadow、两个固定策略计数器和参考价；不复制生产数据结构。

每次场景初始化和每次成功/业务拒绝/被捕获 fail-stop 后运行 Production regular book、regular expiry、StopBook 三个 validator；另沿真实侵入式 FIFO 比较 Reference 的 price/FIFO 顺序、载荷、visible/hidden、TIF、priority 和活动预留。外部 `now`/session 是调用输入常量，不伪称簿内拥有时钟。快照是测试相等投影，不是 durable digest 或规范化持久化编码。

TradeId 短缺仍按已冻结契约在首次写入前 fail-stop；双模型 `catch_unwind` 验证签名令牌、TIF 迁移及复杂非空状态均不变，不把 panic 冒称正常业务 Reject。所有其他拒绝比较明确原因，不只比较两模型相等。

Task 6 Minor 中“双向同价聚合溢出”现在有直接双向、普通/冰山、同价/改价并带 TIF 补丁的补强证据。其成功 transcript 的完整参考价投影仍由 Task 10 完成；本步仅对全部拒绝保留非空完整参考价。

## 本步修改文件与约束

- `matching-domain/src/market_math.rs`：只读 lot getter。
- `matching-orderbook/src/amend_execution_context.rs`、两模型 `amend_candidate.rs`：显式市场配置与独立数值只读预检、接口边界文档。
- `production_orderbook/tests.rs`、新增 `tests/amend_atomicity.rs`：测试入口及上述矩阵/性质。
- `tests/amend_reservation.rs`：共享测试 fixture 显式市场配置；增加方向/version 参数，仅为真实场景构建；原 Task 8 测试保持原语义。
- `tests/amend_stop.rs`、`stop_book.rs`：完整确定性失败快照增强及测试专用有序投影。
- `matching-risk/tests/amend_reservation.rs`：三个底层 signed-verifier 独立拒绝测试，关闭 Task 1 遗留 Minor；风险业务代码没有永久变动。

新增说明注释为中文。没有新增或升级依赖、公式契约变化、float、unsafe、wall clock、网络/RPC、数据库、锁、无界队列或 I/O。已有 Cargo/lockfile/公式等 diff 属前序工作并保留；本步不将它们重新归属。无 I/O，crash/WAL/snapshot/durable digest 测试不适用。没有更改 reason 编号、事件协议或资金资产映射；没有实现 T13。

## 实际 gate

以下为恢复所有变异后的实际命令，均 exit 0：

| 命令 | 结果 |
| --- | --- |
| `cargo test -p matching-orderbook --locked amend_atomicity_ --lib --quiet` | 9 passed，0 failed，7.32s |
| `cargo test -p matching-orderbook --locked amend_ --lib --quiet` | 85 passed，0 failed，7.35s；覆盖前序 Amend 回归和全部新增性质 |
| `cargo test -p matching-risk --locked signed_verifier -- --nocapture` | 3 passed，0 failed |
| `cargo fmt --check` | 通过 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 通过，无诊断 |
| `cargo test --workspace --locked --quiet` | domain 153 unit + 10 integration，orderbook 519 unit，risk 11 integration，2 doctest；共 693 unit/integration + 2 doctest 全通过；orderbook 31.95s |
| `git diff --check` / `git diff --cached --check` | 通过；普通 diff 只有既有 LF→CRLF 配置提示 |

workspace 完整 orderbook 已包含全部现有 66 项 differential/property 和历史 100k/双次 10k tape；它们仍不是 Task 10 的完整 Amend 联合长 tape。本步停在独立 review 边界，父执行会话采纳审查前不要标记 Task 9 已关闭或进入后续任务。没有远端 CI 结论。
