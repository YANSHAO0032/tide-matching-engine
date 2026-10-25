# T12 Task 11：文档与最终本地 gate

日期：2026-10-25。**本小步的文档维护与本地 gate 通过；停在整个未提交分支的广泛 review 之前，尚不宣称整个 T12 完成。** 无提交、暂存、合并或推送，不进入 T13。

## 来源、范围与文件

工作目录 `C:/Users/10703/.codex/worktrees/t12-amend/tide-matching-engine`，分支 `codex/t12-amend`，HEAD `550fda02816c73a5704b4ffe836fcfb31ffff08f`。保留 Task 0–10 的全部未提交实现；本小步没有修改 Rust、Cargo、lockfile 或测试。实读 AGENTS、主 checkout 的指导规则和唯一 handoff、绑定实施任务书的最终 gate、执行计划、全部小步报告/审查与后续复审、相关源码和真实 diff/status。worktree 不含 ignored assignment；读取 `D:/rust_workspace/tide-matching-engine/assignment/`，不另建 handoff。

本步更新：

- 新增 `docs/amend.md`：真实 API、绝对目标、typed 事件、普通/冰山/Stop priority 矩阵、时效、预留生命周期、完整上下文与低层接口前提、失败边界及明确 deferred 范围。
- 更新 `docs/orderbook_modules.md`：真实 expiry/Stop/trigger/Amend 文件、资金与领域边界、全量差分及性质测试入口。
- 更新 `docs/logic_flows.md`：新增两个中文改单流程图，补真实 Stop 来源隔离、显式时间、child 接续与 root 保护冻结说明。未把任务编号用作图标题或节点标识。
- 更新 `docs/iceberg.md`、`docs/limit_execution.md`：移除将已实现 Amend/普通 DAY/GTD/Stop 一律描述为未来功能的过期文字，区分基础 schema helper 与实际 timed/Stop 入口。
- 更新唯一 `D:/rust_workspace/tide-matching-engine/assignment/SESSION_HANDOFF.md`：当前目标、真实分支/基线、所有冻结裁定、实现/测试证据、已关闭审查事项、依赖、TODO、Blocker、Deferred 与当前续接点。旧历史记录保留并明确其优先级。

`docs/numerical_formulas.md` 保留前序实现的 146 项公式与原字节，本步未改变公式语义。原始 v6 未改写；用户后续裁定明确写入业务说明与唯一交接。

## 冻结裁定与实现结论

1. 用户选择 Hard Gate A 的 A：`new_total_qty > already_filled_qty`；等于或低于已成交量拒绝，零余量用 Cancel。活动 Qty 仍严格正，取代旧 v6 的 `>=`。无数量补丁保留最近接受的总目标，不能把 partial residual 误写为新的原始总量。
2. 用户冻结 `OrderAmended` 的可选 old/new price 与 `AmendedQuantity::{BaseRemaining(Qty), QuoteBudget(QuoteAmount)}`，保留数量模式。候选数量在重新撮合前取值，改价事件固定 `OrderAmended → Trade* → 订单事件`，所有成交和后续订单事件分别保留实际相对顺序。
3. 用户冻结 `AssetId(pub u32)` 与四字节大端签名编码；token 其余字段按固定声明顺序、定宽大端编码，资产列表严格升序、无重复、atoms 正。
4. 用户选择两本簿拥有按 OrderId 定位的 shadow reservation；新增 reservation-aware 初始化原子关联实际活动余量，legacy 入口不伪造 shadow，缺失 shadow 的活动订单无法 Amend。
5. 两模型分别实现候选、完整成交/补量/目标档/priority/expiry 预检与状态提交。Reference 继续 Vec/稳定排序，Production 使用 Arena/价档/侵入式 FIFO；未以生产答案生成参考输出。
6. 普通同价减量保序、增量重排，改价先撮合且仅真实 residual rest 分配目标 priority；冰山非增总量和峰值时裁剪原 visible，增量/增峰/改价失 priority，始终 visible+hidden=total。纯 TIF 变化自身不丢 priority，真实 expiry/session 引用同步迁移。
7. Stop child/保护/expiry 修改保 StopPriority；触发价或来源实际改变只分配一次新 StopPriority 并进入目标来源价档尾部。来源实际存储、分区并路由；DAY/GTD child 真实可执行并维护 residual 到期。root reference update 的更新后新价一次冻结整个级联的用户保护上下文，后代成交只推进真实触发检测。
8. 完整数值/资金入口为 `amend_with_context`，必填实际 MarketMath、最终 CommandId、费率版本/层级、已加载公钥与候选 required-reserves。上游按真实资产/费用/取整配置计算需求，当前核心不实现该计算器。无 token 检查已有覆盖并保持 reserves/version；真实 Ed25519 token 验证与 checked version 先作用于副本，后续簿失败不应用，成功只替换仍活动目标。低层 context-free API 要求调用方先完成数值与资金校验，并拒绝直接携 token，不能当作完整 R1。
9. 终结清理包括 Cancel、Fill、Expire 和 Stop；StopLimit 实际 residual 接续已接受总目标及原 shadow。Reference 批量清理同时保留未触发 Stop，避免误删共享身份 map。TradeId 不足沿既有容量契约在首次写入前 fail-stop；不把它描述为客户业务 Reject。

## 前序审查事项关闭核对

| 历史事项 | 后续证据 |
|---|---|
| Task 1 verifier 零/重复/逆序 required vector Minor | Task 9 的三个直接 signed-verifier 测试与变异对照；本轮 risk 11 项包含这些测试 |
| Task 2 精确字段名、Option 层级和 QuoteAmount 偏差 | Task 2 repair/rereview 关闭；本轮 domain Amend 7 项通过 |
| Task 3 原子 reservation-aware 初始化和格式失败 | Task 3 rereview 确认实质修复；本轮 fmt/Clippy/workspace 全绿 |
| Task 4 全成 shadow 重插、完整快照及 TradeId 证据 | terminal rereview 与完整 snapshot 修复；本轮 Amend 专项和全量回归覆盖 |
| Task 5 GTD A→B/已有目标档 residual 与 overflow Minor | Task 6 差分直接证据；Task 9 双向普通/冰山聚合失败补强 |
| Task 6 Buy-only 场景报告表述、成功 transcript 缺少来源状态 | 保留该历史报告的当时精度限制；Task 9 双向矩阵与 Task 10 全来源投影已提供最终证据，不把历史场景冒称双向 |
| Task 7 来源/保护全拒绝、DAY/GTD 仅模板、后代保护参考漂移 | 两轮修复与第二轮 rereview 关闭；固定 golden 包含 Buy/Sell×Base/Quote×三来源与真正后代 |
| Task 8 StopLimit 接续与 accepted-total | 两项受控 RED 历史证据及独立 review；本轮真实签名/触发/后续 Amend 固定场景覆盖 |
| Task 10 Reference 普通批量操作误删无关 Stop shadow | 固定 RED→GREEN、完整映射投影及独立 review；本轮固定/长 tape 再通过 |

各步修复前的失败记录是有效历史证据；后续复审已关闭后，不能将旧“等待/尚未实施”当作当前状态。此次不重新注入业务故障，不追认缺失的历史 RED 输出。

## 本次实际执行

以下命令均在本 worktree、最终未变的 Rust 源码上执行并确认退出码。没有把前序报告中的测试运行冒充本次证据。

| 命令 | exit / 结果 |
|---|---|
| `cargo test -p matching-domain --locked --test amend_order --test reservation_adjustment --quiet` | 0；7+3 integration passed |
| `cargo test -p matching-risk --locked --test amend_reservation --quiet` | 0；11 integration passed |
| `cargo test -p matching-orderbook --locked amend_ --lib --quiet` | 0；95 passed，0 failed，36.96s |
| `cargo test -p matching-orderbook --locked differential --lib --quiet` | 0；76 passed，0 failed，36.98s |
| `cargo test -p matching-orderbook --locked amend_full --lib -- --nocapture` | 0；10 passed，0 failed，36.98s；7 固定+1 success property+2 tape 测试 |
| `cargo fmt --check` | 0；无格式诊断 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 0；无警告或 lint 例外 |
| `cargo test --workspace --locked --quiet` | 0；domain 153 unit+10 integration、orderbook 529 unit、risk 11 integration，合计 **703 unit/integration+2 doctest**，0 failed；orderbook 37.09s |
| `$env:RUSTDOCFLAGS='-D warnings'; cargo doc --workspace --no-deps --document-private-items --locked` | 0；六个库文档成功；随后清除本次临时环境变量 |
| `cargo tree -p matching-risk --locked -e features` | 0；真实 feature 树与批准配置相符 |
| `cargo tree -p matching-domain --locked -e features` | 0；smallvec 默认 feature，无运行时传递依赖 |
| `python target/task11-audit/final_audit.py` | 0；完整 scope/依赖/注释候选/公式证据/77 个实际业务文档相对链接、UTF-8、尾随空白与末尾换行检查通过 |
| `git diff --check` | 0；仅既有 LF→CRLF 提示，无空白错误 |
| `git diff --cached --check` | 0；index 无本任务暂存变更 |

`amend_` 95 项覆盖 Reference/Production 常规候选与提交、完整 TIF/expiry 迁移、Stop、冰山、真实预留、完整失败性质和新全量 tape。workspace 的其余三个骨架库是零测试，不计为已证明业务能力。完整 orderbook 与 differential 同时保留历史价格/FIFO、IOC/FOK/PostOnly、市场单、冰山、Stop/到期、100k 与重复 tape 回归，没有通过删除断言或降低 cases 使 gate 变绿。

本地文档审计的首次临时 Python 片段因 Windows 默认 GBK 解码 git UTF-8 diff 报错；已在独立审计脚本显式指定 UTF-8，重新执行 exit 0。它不是 Rust 或业务失败，未修改业务源码，也未隐藏该工具层失败。

## 本次长 tape 输出

生成器 `amend-full-v1-ChaCha`；每命令比较完整原序结果/事件、普通/Stop 载荷、FIFO/深度、真实 expiry、两个 allocator、完整来源价格/source_seq 以及完整预留/version；每步运行三个 Production validator。失败另比较两模型各自完整物理快照，身份 map 投影排序不会重排业务 FIFO 或事件。

| seed | 实际命令/运行 | 成功/拒绝 Amend | 签名成功 | Stop/冰山成功 Amend | Trade/触发/到期 |
|---|---|---|---|---|---|
| `[0xA2;32]` | 100000×1 | 19994/33265 | 782 | 13427/3604 | 4582/1838/5073 |
| `[0x3A;32]` | 10000×2 | 每次 1970/3302 | 每次 78 | 每次 1337/341 | 每次 451/169/514 |
| `[0xD7;32]` | 10000×2 | 每次 1970/3306 | 每次 78 | 每次 1292/362 | 每次 439/172/516 |

两组短 tape 都重新生成相同命令输入，第二次逐步比较完整 `Record { outcome, state }` 并确认 transcript 耗尽，计数只是附加证据。失败性质另有 330 个重复拒绝场景及 seed `0xA12_FA11` 的 64-case property；成功性质有 seed `0xA12_5ACC` 的 48-case 五容器矩阵。100k 的每命令投影不是 WAL replay 或持久化 digest。

## 全 diff 范围、依赖与注释

普通 diff 与新增文件范围覆盖 65 个 Rust 文件，涉及 domain/orderbook/risk；现有 runtime/persistence/protocol、root lint、toolchain 和 CI 没有实现改动。实现作用点与上述模块职责一致：预留初始化/终结清理、数量原始目标、原位/重排结构原语、来源分区/保护执行与测试观察。所有源码和测试的新文件都纳入 UTF-8/空白审计；完整路径清单保存在 ignored `target/task11-audit/final-audit.json`。这次为最终 scope/文档审计，不替代下一步全分支广泛代码审查。

检查新增/修改源码模式未发现 unsafe、金融 float、业务 SystemTime/Instant、锁、网络/异步运行时、CancelReplace/BookWithout、unchecked/wrapping/saturating math API 或 lint allow/expect。模式扫描不证明任意普通整数表达式自动正确；checked 算术、FIFO、HashMap 业务顺序和预检/提交调用链仍由既有逐步审查及下一轮广泛 review 检查。HashMap 在当前实现只按身份定位/清理/校验，业务顺序仍来自价格与 FIFO；测试身份映射先排序后比较。

扫描全部 145 个 Rust 文件的说明注释，并另查行尾与块注释。4 个无中文候选是已存在的 `first.remaining = 5`、`second.remaining = u64::MAX` 和两行 Vec 类型示意，属于准确标识符/数值表达式；未发现新增英文说明句。新增 API/状态说明为中文，Rustdoc 严格构建及 2 个 doctest 成功。透明构造/重导出、无分支 getter、参数转发和测试投影由真实命令消费者覆盖，不另加机械镜像测试；没有删除历史测试。

依赖复核：domain 的 smallvec 1.15.1 是用户批准的精确 `SmallVec<[AssetReserve;3]>` 字段需求，默认 feature，无可选 feature/runtime transitives；risk 的 ed25519-dalek 3.0.0 禁默认 features，标准库没有 Ed25519。orderbook→risk、risk→domain 为本地 path 依赖，orderbook dev-dependency 使用相同禁默认 features 的 Dalek 做真实签名。锁文件新增 17 包：block-buffer、cpufeatures、crypto-common、curve25519-dalek、curve25519-dalek-derive、digest、ed25519、ed25519-dalek、fiat-crypto、hybrid-array、rustc_version、semver、sha2、signature、smallvec、subtle、typenum；既有包版本没有升级。

实际树启用 curve25519-dalek digest、subtle const-generics、digest block-api 等 upstream 功能及 proc-macro/build 链；不启用 Dalek batch/hazmat/RNG。没有网络/数据库/业务时钟/新队列，因此无容量或背压变更。cpufeatures 可选择平台实现路径，不改变算法输入语义；项目 lint 不证明第三方内部没有 unsafe，沿用用户已批准的依赖风险边界。六成员继续显式继承 workspace lint，没有局部降级。

## 公式和流程证据

当前公式 SHA-256 为 `f7bb968cdca16876abe0a105a1acb581877fc368b05a0cf7e8a43e9cac79d438`。146 项（40 block、106 inline）与已有 `target/t12-amend-formulas/formulas-results.json` 逐项 TeX/display 相等，每项 count=1、error 为空、MathML structureMatches=true；禁用宏/HTML 截断负对照为 true。再次查看 `formula-section-9.png`，确认严格大于、绝对目标减法与 visible/hidden 公式的符号和结构。

这是当前源码与已有 GitHub math-renderer/MathJax 3.2.0 本地审计成果的一致性复核；本轮未重新联网运行组件。历史 remote 对象只有 116 项，不代表当前页面发布或通过；不宣称远端 CI 或当前 GitHub 数学页面已核验。两张新流程图已逐节点与真实调用顺序核对，围栏/本地链接检查通过；本轮未另运行 Mermaid 图形渲染器，不把源码审查当作新的截图验收。

## 剩余限制与停止点

本地 gate 无失败 blocker；尚待父执行会话对整个分支作广泛代码 review，审查通过并处理任何 finding 后，才可判断整个 T12 验收。若后续修改 Rust 或共享配置，按影响面重跑相应 gate。

当前不含资产映射/真实费率与 rounding-safety 需求计算器、真实账户冻结或释放/Settlement、完整 MarketState/KillSwitch/route fencing、PriceBand/STP/PreOpen、WAL/快照/HA、durable envelope、完整命令去重、跨 shard TradeId 全局分配或 CancelReplace。无 I/O，因此 crash/truncation、durable replay/snapshot digest 测试不适用，未伪造相应证据。降低需求不自动释放 reserve；context-free 入口有明确前提；这些限制已写入实际业务文档。

本小步停止于 review 边界。唯一交接已同步；不提交、合并、推送或启动 T13。
