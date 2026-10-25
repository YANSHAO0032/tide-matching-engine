# T12 Task 10：完整差分与确定性命令序列报告

日期：2026-10-25。状态：实施与本地 gate 已完成，等待独立审查；没有进入 Task 11，没有提交、合并或推送，不代表整个 T12 已验收。

## 范围与依据

读取执行计划 Task 10、用户绑定实施任务书 §28–§30、Task 9 报告和通过的独立审查、当前真实源码及未提交 diff。worktree 没有 ignored `assignment/`，规则和唯一 handoff 从主 checkout 的 `D:/rust_workspace/tide-matching-engine/assignment/` 读取。本报告供父执行会话同步唯一 handoff，不另建权威交接。

新增完整命令域，不修改或削弱历史 GTC、市场单、IOC/FOK/PostOnly、冰山、到期及 regular Amend 的生成器和断言。所有 Amend 都调用两个模型各自的 `amend_with_context`，带实际 `MarketMath`、命令身份、费率版本、两资产需求及真实 Ed25519 公钥。普通/冰山/StopLimit/StopMarketByQuote/StopMarketByBase 均进入完整上下文路径。

## 新增命令、投影与独立性

命令包括 reservation-aware 普通定时挂单、冰山 PostOnly、三类 Stop、普通限价撮合、带预留 GTC 撮合、完整 Amend、统一 Cancel、逻辑时间推进、EndSession、LastTrade/MarkPrice/IndexPrice 参考更新和 child 级联。输入及签名在回放前生成；任一模型的结果不生成另一方的输入或撮合答案。

每条命令前后比较：

- 原序 `Result` 和完整事件列表；绝不排序成交或事件。
- 买卖真实价序/FIFO、完整原始订单、visible/hidden/total/peak、TIF、QueuePriority、visible depth、best price。
- 完整 Stop 载荷、来源、触发价、保护、child TIF/数量模式、触发前到期和 StopPriority/FIFO。
- 全部本地预留身份、资产列表、atoms 和 version；不是只枚举活动普通订单。
- 两个下一优先级值，以及三个实际来源的完整价格和 source_seq。
- 普通真实 GTD/DAY 索引、Stop 真实到期桶引用及其 reference/side/stop_price 位置，与 Reference 从自身载荷派生的关联比较。只对身份索引投影排序，不改变队列或事件。
- 调用方持有的逻辑时间和当前 session。它们是外部输入，不伪称簿内时钟。

每步运行 Production regular book、regular expiry 和 StopBook validator。另断言数量守恒、精确聚合、FIFO、StopLevel 同价同来源 priority 严格递增、两个 next 高于活动优先级、无跨容器重复 ID 和无孤立预留。失败还分别比较两个模型的完整物理快照：Production 包括 Arena/free-list/节点链接/档位、全部身份索引、完整到期索引/StopBook、分配器策略和来源；Reference 包括自己的源 Vec/完整预留/两个计数器/来源。此投影不是 durable digest。

Reference 仍从自己的 Vec 及稳定排序得到答案，Production 从自己的 BTreeMap/Arena/侵入式 FIFO 得到答案。新增共享测试代码只构造输入、调用明确 API、观察状态和表达领域性质，不复用任一生产计划生成另一方答案。两个模型的 Trigger-R1 分别读取自己的命令开始预留，核对身份、冻结费率版本和本测试市场的两资产覆盖；没有恒成功验签器。

需求 `9000/11000` 是调用方测试 fixture 的保守资金上界，独立于 token 的 `12000` 目标。测试域内 qty<=30、价格<=115、quote budget<=3000；不将此 fixture 描述成真实资产映射、费率表或账户服务实现。

## 固定边界与成功性质

新增 7 项固定测试和 1 项生成性质，覆盖 §28 的 20 项要求：

| 要求 | 实际证据 |
| --- | --- |
| 1–3、5–8 | 双向真实部分成交后绝对目标减量、低于/等于 filled 拒绝、同价增量到伙伴尾部、非交叉改价、GTC/DAY/GTD 迁移及非法时间/session、真实到期清理 |
| 4 | 双向改价跨两档，分别完全成交/余量挂单；独立断言 `OrderAmended → Trade → Trade → lifecycle` 和仅 residual 分配 priority |
| 9–13 | 三种 Stop × 双方向的数量/budget/child 价格/TIF/保护修改、触发前 expiry Keep/Clear/Set、真实到期、stop_price 进入已有目标尾部、LastTrade/Mark/Index 来源迁移 |
| 14–16 | quote budget 减量、增量+真实签名和增长覆盖要求、new_qty 或双数量字段非法，模式保持不变 |
| 17–18 | 冰山减量及减小峰值保 priority；数量不变而峰值/暴露从 2→4 时新 priority |
| 19 | 双向 regular/iceberg，有效签名及目标资产足够，但随后真实目标档聚合上溢；完整状态/version 不变。同一签名安全减量可成功并仅应用一次，排除签名错误假拒绝 |
| 20 | 五容器 × 双方向的签名成功后原命令重放，明确 stale-version 拒绝且完整状态不变；后续新 version 令牌真正成功 |

另外固定验证：修改后的 Stop 以 MarkPrice 真正触发、部分成交并把原总量及 version=12 的 shadow 接续到 regular child；后续 Amend 仍按已成交量计算绝对目标，Cancel 清除预留。

固定 seed `0xA12_5ACC` 的 48-case proptest 生成五种容器、方向、数量和峰值，联合验证成功变更、真实签名应用、重放拒绝及改价/触发迁移。每条成功 Amend 还从输入和前态独立推导 event 数量模式、候选余量和 retained，验证：

- 同价减量保 priority；普通增量/改价及冰山暴露增长失去 priority；仅实际 rest 分配 amender 的 priority。next 的精确增量为真实 `IcebergRefreshed` 数量加实际 amender residual，不允许计数器漂移。
- Stop child/数量/budget/expiry 保 StopPriority；触发价/来源变化恰好分配一个新 StopPriority，普通 next 不动。
- 保持 OrderId 与数量模式；事件候选余量来自撮合前而非 residual。
- 无 token 的预留完整不变；有效 token 只递增一次 version 并应用精确目标；其他仍活动订单的预留不变。

失败原子性另沿用 Task 9 的全部边界性质，并在每条完整 tape 业务拒绝上检查完整前后快照。

## RED → GREEN：混合容器暴露的真实缺陷

首轮长 tape 发现：seed `[0xA2;32]` 第 25 条、seed `[0x3A;32]` 第 37 条普通 Place 后，Reference 误删仍未触发 Stop 的 shadow，Production 保留。完整映射投影检测到 Stop IDs 5–10 缺失；旧的只看普通活动单的投影不能发现它。

随后先新增固定 `amend_full_fixed_regular_mutations_preserve_unrelated_stop_reservations`，运行同名 filter：exit 1，0 passed/1 failed，在 4 次初始化后的第一次普通 Place 精确复现 Stop 预留丢失。这是业务 RED，不是编译错误。

根因是四个批量路径的 `reservations.retain` 只检查普通 `orders` Vec。修复为 Reference 自身的 `retain_active_reservations`，同时保留自己的普通和未触发 Stop 身份；应用于 slice commit、IOC/FOK 批量成交、regular reprice 和 iceberg reprice。固定测试依次覆盖 GTC、IOC、FOK、regular/iceberg reprice，检查三个无关 Stop 的完整预留保留。终结清理仍由已有回归验证。没有调用 Production 状态或重新登记虚构预留。

生成器开发还遇到一个测试输入域问题：随机调用低层 `rest_timed_with_reservation` 可能违反其“已确保不交叉”的先决条件，引发既有 fail-stop。生成器改用完整 reservation-aware GTC placement 来处理随机重复挂单；固定每轮空簿初始化仍使用低层定时 rest。没有改变底层业务语义、吞掉 panic 或过滤已生成命令。

## 长 tape 与重复运行

生成器版本 `amend-full-v1-ChaCha`。采用固定种子 ChaCha，128 条一轮：建五种容器的双向订单、成功减量、轮换容器的真实签名增长及重放、三个来源基线、75 条随机交错、LastTrade 上下跨越、GTD 到期、EndSession 和 24 个身份清理。生成不读取订单簿。

域最多 24 个活动身份、每个基础余量<=30、峰值>=1；完整 child 级联至多 720 片，调用前固定准备 1024 个唯一 TradeId。所有命令实际执行，不因模型结果跳过。100k 流式比较每步；10k 首次完整保存结果+全投影，第二次逐条取出严格相等比较，连同新生成输入 tape 也逐条相等；不是终态或 hash 比较。

| seed（32 个相同字节） | 命令数/运行 | 成功 Amend | 拒绝 Amend | 签名成功 | Stop 成功 Amend | 冰山成功 Amend | Trade | StopTriggered | 到期订单 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `[0xA2;32]` | 100000 ×1 | 19994 | 33265 | 782 | 13427 | 3604 | 4582 | 1838 | 5073 |
| `[0x3A;32]` | 10000 ×2 | 每次 1970 | 每次 3302 | 每次 78 | 每次 1337 | 每次 341 | 每次 451 | 每次 169 | 每次 514 |
| `[0xD7;32]` | 10000 ×2 | 每次 1970 | 每次 3306 | 每次 78 | 每次 1292 | 每次 362 | 每次 439 | 每次 172 | 每次 516 |

本地独立 100k run 36.91s；独立两组各双次 10k run 合计 14.65s。最终完整上下文/实际到期桶/精确分配器性质均启用后的 10 项联合运行 37.00s（Rust 测试并行调度，不能把该总时长当作各测试时间相加）。覆盖计数由实际命令结果统计，每次完整回放必须具有非零成功/失败、签名、Stop、冰山、成交、触发和到期。

## 修改文件与范围约束

- 新增 `production_orderbook/tests/differential/amend_full.rs` 及其 `scenarios.rs`、`tapes.rs`；`differential.rs` 仅注册新模块。
- `reference_orderbook.rs`：Reference 自己的活动预留清理 helper；只在 `cfg(test)` 下增加完整预留和 next 只读投影。
- `reference_orderbook/{iceberg,limit_execution,amend_candidate,amend_iceberg}.rs`：四个错误清理点改用上述 Reference helper。
- `production_orderbook/expiry.rs`、`stop_book.rs`：只在 `cfg(test)` 下读取真实到期索引。
- 两模型根模块的预留字段中文注释更正为普通及未触发止损的共同身份范围。

只读投影 getter、模块注册不机械新增镜像测试；完整 tape 和固定真实索引/映射比较覆盖其消费者。所有新增手写注释为中文。无新依赖/升级、reason/event wire 变更、数值公式变更、float、unsafe、系统时间、网络/RPC/数据库、锁或无界队列；不修改公式文档。已有 Cargo/lockfile 等 diff 属前序成果。无 I/O，crash/WAL/snapshot/durable digest 不适用，没有 T13 实现。

## 实际 gate

恢复最终源码后以下均 exit 0：

| 命令 | 结果 |
| --- | --- |
| `cargo test -p matching-orderbook --locked amend_full_fixed --lib --quiet` | 最终 7 passed，0 failed，0.58s |
| `cargo test -p matching-orderbook --locked amend_full --lib -- --nocapture` | 最终 10 passed、0 failed，37.00s；含 7 固定+1 property+2 长 tape 测试 |
| `cargo test -p matching-orderbook --locked differential --lib --quiet` | 完整 76 passed、0 failed，37.09s；含全部历史 differential/property |
| `cargo fmt --check` | 通过 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 通过，无诊断 |
| `cargo test --workspace --locked --quiet` | domain 153 unit + 10 integration，orderbook 529 unit（37.34s），risk 11 integration，2 doctest；共 703 unit/integration + 2 doctest，0 failed |
| `git diff --check` / `git diff --cached --check` | 通过；普通 diff 只有既有 LF→CRLF 提示 |

本步止于独立审查边界。父执行会话采纳独立 review 前不关闭 Task 10；远端 CI 没有核验，T12 最终文档/整体审查仍由 Task 11 完成。
