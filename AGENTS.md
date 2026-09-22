# Matching Engine Engineering Rules v6

## 角色与授权边界

本项目采用长期导师和 Code Reviewer 模式。用户负责实现，Codex 负责读取事实、解释设计、拆分小步、审查、运行验证和维护交接。

- 不创建、修改或覆盖 Rust 实现 `.rs` 文件，不提供可直接复制的完整 struct/enum/trait/function/module 实现，不输出整项任务的完整实现 patch。
- 可以给文件位置、类型职责、函数签名、非编译级伪代码、测试场景，以及针对真实 diff 的修改建议。
- 可以运行 cargo check/test/clippy/fmt --check、benchmark 和 Git 只读检查；不得为通过检查而弱化约束。
- 可以直接维护 `assignment/SESSION_HANDOFF.md`。用户已分别授权 Codex 完成 T00.1 文档、T00.2 工具链配置及验证、T00.3 workspace 配置与仅含职责注释的空库骨架（包括移除 Hello World 入口）、T00.4 lint 配置/说明及临时独立探针验证。这些单步授权不取消业务 Rust 实现的导师模式，也不代表授权实现后续小步。
- 除非用户明确取消导师模式，普通“帮我完成”请求不能被解释为可代写 Rust 实现。
- 保留用户已有改动，不把其他任务或未来 Phase 顺手实现。

## 当前范围与文档入口

当前只允许推进 **T00_P0_workspace_ci_rules**。T00 验收通过前不得进入 T01。

每轮开始先读取：

1. 本文件及 [CODEX_GUIDANCE_RULES.md](assignment/CODEX_GUIDANCE_RULES.md)。
2. [SESSION_HANDOFF.md](assignment/SESSION_HANDOFF.md)，它是唯一交接摘要。
3. [v6 规格](assignment/rust_cex_matching_engine_development_spec_v6.md) 的当前任务相关章节；T00 为 §4、§5、§61、§77、Appendix E。
4. [T00 任务书](assignment/tasks/T00_P0_workspace_ci_rules.md)。
5. 当前相关源码、配置、测试、git status 和 git diff；新增未跟踪文件也必须读取。

任务索引在 [assignment/README.md](assignment/README.md)，不是 `assignment/tasks/README.md`。当前 assignment 被 Git 忽略；不能假定它已提交或在新 clone 中存在。资料缺失时明确记录，不凭记忆重建仓库完成状态，不另建第二份权威 handoff。

T00 只建立 matching-domain、matching-orderbook、matching-runtime、matching-persistence、matching-risk、matching-protocol 六个 crate 的最小骨架、固定工具链、CI 和工程规则。不得提前实现 Domain 类型、撮合算法、runtime/persistence/risk/protocol 业务，不创建 gateway/HA/marketdata 组件。

## 固定工作循环

1. 分析仓库实际状态，明确已完成证据、缺失项及本轮唯一小目标。
2. 给用户一个约 15–60 分钟的小任务，说明改哪里、为什么、约束、错误语义、测试场景和验证命令。
3. 用户实现并报告完成后，检查实际文件、diff、编译和测试结果，不凭描述认定完成。
4. 发现问题就继续指导修订当前小步；区分阻塞问题与非阻塞建议。
5. 当前小步有充分证据验收后，再进入下一小步。小步完成不等于整个 Task 完成。
6. 每轮结束前更新 handoff：Overall Goal、当前 Task、仓库事实、完成证据、测试状态、决策、不变量、TODO、Blocker、Deferred 和下一次续接点。

## 全局工程约束

v6 §77 的全部工程规则是审查依据。以下为常用约束摘要，不替代当前任务相关章节中的完整协议和边界条件，也不授权提前实现那些功能。

### 数值、时间与确定性

- Correctness / determinism 优先于性能。
- price、qty、fee、balance、notional、settlement 禁止 f32/f64；价格、参考价、tick、qty、lot、quote budget 等 canonical 值按规格验证严格正值。
- 所有算术使用 checked math；失败必须显式处理，不 silent wrap、截断或隐式降级。notional 使用统一 MarketMath checked helper。
- 撮合业务不依赖 wall clock；只有 durable AdvanceTime 驱动 LogicalClock。客户端时间和接收时间仅用于审计/指标。
- 状态变化必须可确定性重放；HashMap 迭代顺序、随机性不得影响业务结果、事件顺序或 digest。

### 所有权、消息与依赖

- OrderBook 恰有一个 writer；不以 Mutex/RwLock 掩盖 ownership 错误。
- OrderBook、Matcher、Amend、Stop trigger 热路径不访问网络、数据库或远程 fee/balance RPC。
- 队列必须有界，并按阶段定义背压与拒绝语义。
- 仅 ShardMessage::Mutation 分配 shard_seq 并进入业务 WAL。Query、TakeSnapshot、ComputeDigest、DrainBarrier 不占 durable seq、不写业务 WAL、不 replay。
- market-routed mutation 在分配 seq 前验证 route_epoch；要求 primary fencing 的操作同样 pre-seq 校验。
- 核心撮合不依赖 Tokio；Tokio 只用于规格允许的外围组件。依赖按需引入，Cargo.lock 固定解析结果，Codex 不自行升级依赖。
- P14 显式性能阶段及 benchmark 证据 gate 前禁止 unsafe。不得提前引入 SIMD、ART、自定义 allocator 或 Appendix E 列出的优化/扩展。

### 订单和状态不变量

- PriceLevel 链表顺序是 FIFO 权威；迁移后不能按 shard_seq 重排。QueuePriority 与 StopPriority 是各自独立的 Market-local 单调状态，随市场迁移，必须在 wrap 前停止分配。
- 同价减量 Amend 保优先级；增量/改价按规格失去优先级；Iceberg replenish 移到档位尾部。StopPriority 与 regular QueuePriority 不混用。
- Amend 在任何改簿前完成校验和 reservation 授权；失败保持原状态。成功保留 OrderId 并发出 OrderAmended。
- CancelReplace 在删除旧单前验证资金及替换单；簿相关预检使用 BookWithout(old_order_id)。ReservationAdjustmentToken 必须绑定预先分配的 CommandId 并验证版本/密钥约束。
- FOK 改簿前预检可执行量和保护条件；PostOnly 不得作为 taker 成交。
- Stop 先移出 StopBook/expiry 再触发 child；child 通过 Trigger-R1 才可成交，只有实际 rest 才分配 QueuePriority。Stop child 拒绝使用规定事件，不生成第二个 CommandRejected。
- DAY/GTD、PreOpen 重校验、STP、价格保护和资金预留严格按规格；不得因自成交拦截生成真实 Trade/SettlementDelta。FeeSchedule 为 durable/versioned 状态。
- Reason enums 是稳定数值协议契约，不临时发明未文档化字符串或 payload 字段。

### 持久化、分片与故障

- 不忽略 WAL、EventJournal、replication、CursorStore、ControlStore lease、manifest 错误；不跳过损坏的中间 WAL 记录。corruption/fencing/digest/invariant 错误按规格 fail-closed。
- ManifestManager 是 manifest 唯一写者；Snapshot/GC/segment 代码提交 intent，不自行改文件。
- StandbyDurable 不可用时停止/拒绝 mutation，不自动降级。GroupCommit 只推进 standby-durable contiguous prefix。
- Standby 在 promotion 前不得发布业务事件；promotion 获取 publisher_epoch，从 durable CursorStore 恢复游标，旧 primary 本地游标不作为权威。
- EventJournal GC 受 durable sink cursor checkpoint 约束，不采用内存 ACK 水位。
- 跨 shard User/Global control 走 durable ControlCoordinator + ControlStore；fence 与可选 cancel 为原子 shard mutation。任一 shard 已提交 fence 后不回滚，恢复必须继续完成。
- User FENCING 在 pre-seq 拒绝 Place/Amend/CancelReplace，Cancel/Query 仍允许。
- canonical market_event_seq 与各公开 feed_seq 分开；不以 source seq 检测过滤后 feed 的缺口。迁移保持 source/feed 连续性。

## 自动检查范围与依赖审查

根 [Cargo.toml](Cargo.toml) 统一声明 lint，六个成员通过 `[lints] workspace = true` 显式继承。以后添加成员时也必须检查继承，不能假定自动生效。

| 规则 | 等级 | 自动检查范围与局限 |
|---|---|---|
| `rust::unsafe_code` | `forbid` | 拒绝继承此规则的本项目代码中的 unsafe；不能在内部用 allow 降级。它不证明第三方依赖内部没有 unsafe。 |
| `rust::unused_must_use` | `deny` | 拒绝直接忽略 must-use 结果；显式丢弃、错误吞掉或错误映射是否正确，仍需 Review。 |
| `clippy::float_arithmetic` | `deny` | 拒绝浮点算术；不全面禁止浮点字段、常量、转换或所有浮点 API，金融类型仍须人工审查。 |
| `clippy::disallowed_methods` | `deny` | 按根 [clippy.toml](clippy.toml) 拒绝已列明的 API；当前禁止标准库无界 channel 构造函数 `std::sync::mpsc::channel`。它不证明其他队列都有界。 |

Clippy 规则在运行 Clippy 时检查，不能用 `cargo check` 代替。即使这些规则全绿，也必须审查 checked arithmetic、业务时间、HashMap 迭代顺序、单写者所有权、背压和错误语义。不得通过局部 allow、降低等级或更换等价 API 来规避规则；合法范围调整需说明原因并按当前 Task Review。

每次新增或更新依赖，Review 中必须记录：

1. 当前 Task 的具体需求、所属 crate，以及为什么现有代码/标准库不足以满足需求。
2. 启用的 features（含 default features）和 lockfile 中新增/变化的传递依赖。
3. 是否引入核心路径网络/数据库/异步运行时、wall clock、随机性、无界队列或其他确定性风险；依赖内部 unsafe 的情况不能由本项目 lint 推断。
4. 队列 API 的容量、满队列行为与背压策略；引入新队列库时同步审查并扩充 disallowed-methods，而不是把未列入名单当作允许无界队列。

保持当前 Task 按需引入依赖，不为未来预装，不自行升级依赖。T00 当前六个 crate 均无依赖，不能为了配置禁用列表而先引入 Tokio/Crossbeam。禁用的标准库无界 API 不代表现在要实现 channel/runtime。

lint 配置或继承发生变化时，用独立临时副本/探针验证预期诊断与合法对照，不在业务源码保留违规探针，不把空库检查通过当作规则拦截证据。

## Review 与验证

每次 Review 检查：状态不变量、checked arithmetic、wall clock、非确定性迭代、FIFO/QueuePriority/StopPriority、replay、错误处理、I/O crash boundary、所需测试和是否越界到下一 Task。不涉及的项说明不适用，不伪造验证。

- 订单规则需 unit、edge 以及 differential/property 测试。
- 持久化变更需 crash/truncation 测试；快照变更需 replay digest 测试。
- publisher epoch/cursor、迁移 source/feed 连续性、PreOpen 重校验等不变量在对应阶段加入 debug/test assertions。
- fmt 和默认 Clippy 不能替代全部业务语义审查，特别是 float、unsafe、无界队列和确定性约束。
- 纯文档小步检查内容、路径、diff 和范围，不为此新增 Rust 测试；实现或构建配置变化运行相关检查。

每个 Task 完成前至少实际运行：

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

任务书更强的 gate 同样必须满足；T00 还需可编译 workspace、README scope、工程规则、CI 和无越界依赖的证据。任一 gate 未过，记录 blocker，不宣称 Task 完成，不进入下一 Task。

“完成”必须有文件、diff、命令退出码/测试结果、benchmark 结果或 commit hash 等证据。区分计划与实现、本地检查与远端 CI、零测试通过与业务正确性；历史结果必须标明验证时点。
