# tide-matching-engine

使用 Rust 构建的 CEX Matching Core / Matching Node 项目，长期目标是确定性（deterministic）、事件溯源（event-sourced）和多分片（multi-shard）的撮合核心。

## 当前状态

2026-10-28：T13 Atomic CancelReplace 已在 `codex/t13-cancel-replace` 完成实现、分步审查、文档、独立总复审和最终本地 gate（849 项测试 + 2 doctest、fmt、严格 workspace Clippy、私有项 Rustdoc），并已创建本地交付提交；尚未获授权合并或推送。支持普通限价/冰山/未触发 StopLimit 旧单，以及普通限价 GTC/IOC/FOK/PostOnly/DAY/GTD、冰山 GTC/PostOnly 新单。真实预留登记表区分金融 Active 与 ActiveOrder/PendingR2 归属；终结不发资金释放事件，也未实现 R2/结算。支持矩阵、原序事件、信任边界和未支持组合见[原子撤单替换与预留归属](docs/cancel_replace.md)。以下 T12 及之前记录为已交付历史。

截至 2026-10-26，T00–T12 已交付；T12 Amend 由 `d694974` 与 P2 收尾提交 `b763984` 合入并推送至 `origin/main`。本地完整验证已经通过；远端 CI/GitHub Actions 与 GitHub 页面实际公式渲染尚未在本轮核验。当前能力还包含订单簿模块整理、StopBook/确定性止损级联、LogicalClock 驱动的常规与未触发 Stop 的 DAY/GTD 到期，以及 EndSession。文件职责和测试入口见 [订单簿模块与代码入口](docs/orderbook_modules.md)。

**T12 Amend 已交付。**两个独立订单簿已具备普通、冰山和未触发 Stop 的 Amend 候选/提交路径、typed `OrderAmended` 事件和本地版本化预留令牌验证；用户冻结的数量规则为绝对总目标必须严格大于已成交量，零余量改用 Cancel。P2 已将 terminal OrderId 的稳定原因恢复为 `OrderNotFound (2010)`，并保留活动订单缺失 shadow 时的 `InvalidReservation (4002)`；该修复已通过 scoped re-review、完整本地 gate，并随 `b763984` 推送，不代表远端 CI、完整账户/结算或持久化协议已经完成。

**T11 LogicalClock / DAY / GTD / EndSession 已交付。**实现提交 `36d4a4d` 已快进合入 `main`；随后 `ReservationReleaseReason` 的文件组织整理由 PR #3 合入 `550fda0`，保持 crate-root 公开导入路径不变。逻辑时间只接受显式单调推进；GTD 必须严格晚于当前 LogicalTime，DAY 必须绑定当前 session。生产簿使用有序到期索引，参考簿以独立 Vec 扫描与显式排序实现相同顺序；EndSession 会终结对应的常规 DAY 与未触发 Stop-DAY 订单。最终本地 gate：domain 152、orderbook 419、2 项 doctest，fmt、严格 Clippy 与测试均通过；远端 CI 未在本记录中重新核验。

**历史本地验收 — T08_P3_iceberg**：分支 `codex/t08-iceberg`，基线为已合并 T07 的 `7048296`。2026-10-22 本地验收通过：workspace 538 项单元测试（domain 143、orderbook 395）及 2 项 doctest；完整 differential 52 项、fmt、严格 workspace Clippy 均通过。包含冰山固定 seed 100k 混合命令与两组双次 10k 重复 tape。 已实现可见/隐藏量守恒、补量取得新优先级并移到队尾、total remaining 事件、完整取消与既有成交入口兼容。API 见 [冰山订单](docs/iceberg.md)。交付分支为 `codex/t08-iceberg`，本次包含 T00–T08 中文注释规范修订；上述结果为历史本地验收，远端 CI 状态单独核验。以下旧测试数字保留为历史证据。

**历史本地验收 — T07_P3_ioc_fok_postonly（2026-10-21）**。2026-10-21 用户已解除导师模式，授权 Codex 直接完成实现和验收；已在 `codex/t07-ioc-fok-postonly` 分支完成本地验收：495 项单元测试、2 项 doctest，以及 fmt、严格 Clippy 全部通过；包含固定 seed 100k 混合命令和重复 tape 验证。交付分支为 `codex/t07-ioc-fok-postonly`；上述结果为历史本地验证，远端 CI 未核验。API 与事件契约见 [限价订单执行约束](docs/limit_execution.md)。

当前已交付 **T00–T12**，T13 的实现和最终验收状态见本节开头。Reference 与 Production 保持独立实现，由差分、不变量、固定 tape 与性质测试共同验证。资金 reservation/fee/settlement 的完整账户闭环、runtime、持久化、HA 与 market data 尚未实现。数值规格已明确独立 quote_scale、配置验证与本金精确不取整，详见 v6 §6.5/§13/§46.1；当前事实和验证边界以 [唯一交接摘要](assignment/SESSION_HANDOFF.md) 为准。

当前包含六个 library crate 的 virtual workspace（edition 2024），工具链固定为 Rust 1.98.0。matching-domain 提供当前订单/事件/数值与签名载荷；matching-orderbook 提供独立双模型；matching-risk 已有真实本地 Ed25519 预留核验与登记表计划。runtime、persistence、protocol 仍为骨架。当前轮无依赖升级，远端 CI 未核验。

本说明中的撮合、持久化和多分片能力均为规划目标，不代表当前已经实现或经过生产验证。已完成事项与实际验证结果以 [SESSION_HANDOFF.md](assignment/SESSION_HANDOFF.md) 中的证据为准。

价格、数量、名义额与预算反推的冻结公式见 [数值计算公式](docs/numerical_formulas.md)。

生产簿与参考簿的文件职责、内部模块关系及测试入口见 [订单簿模块与代码入口](docs/orderbook_modules.md)。

## 项目范围

长期范围包括价格时间优先撮合、订单生命周期与交易规则、单写者执行模型、有界背压、命令幂等、WAL、快照、确定性重放，以及后续阶段的多分片和主备复制。

账户中心、充值提现、KYC、财务总账、清结算数据库和 Web 前端不属于撮合核心。核心与外围资金、订单、行情和清算系统的集成协议，以及参考风控/资金实现，按规格对应阶段推进。

T02 已用 `Vec + sort` 建立易审计的参考模型，并验证价格/FIFO、maker-price、相同 command tape 的事件与终态、数量守恒和取消性质。T03–T08 分别完成生产簿、差分验证、市价、执行约束和冰山生命周期。T09 建立独立 StopBook 与触发前到期索引；T10 实现确定性止损触发级联和本地 Trigger-R1 边界；T11 将常规 DAY/GTD 到期和 EndSession 纳入 LogicalClock/session 驱动；T12 Amend 已通过本地完整验证并推送至 `origin/main`。T13 CancelReplace 的本轮状态见开头；runtime、Gateway、HA、MarketData 和完整资金闭环仍属于后续范围。

## Crate 边界

以下六个 crate 位于 `crates/` 下。本表列出规划职责；matching-domain、matching-orderbook 和 matching-risk 已有上述当前实现，其余三个库仍为骨架。依赖按实际小步需求添加。

| Crate | 职责 |
|---|---|
| `matching-domain` | 领域 ID、整数数值类型、订单/市场语义、命令、事件与错误模型 |
| `matching-orderbook` | 订单簿、价格档位、FIFO、撮合规则、参考模型和状态不变量 |
| `matching-runtime` | 单写者 shard 执行、有界消息入口、路由、排序及处理流程编排 |
| `matching-persistence` | WAL、快照、manifest、恢复与持久性边界 |
| `matching-risk` | 交易前校验、价格保护、reservation 与参考资金规则 |
| `matching-protocol` | 外部协议帧、编解码、版本和稳定的 reason code 契约 |

这些最小可编译骨架已在 T00 验收，领域类型、参考模型、生产簿底层结构和 T04 生产簿行为已在 T01/T02/T03/T04 验收。规格中的完整目录是长期目标，不是当前创建清单。

根 [Cargo.toml](Cargo.toml) 显式列出六个成员，使用 resolver 3，并通过 `workspace.package` 统一版本 0.1.0 和 edition 2024；各成员显式继承。workspace 共用根目录的 Cargo.lock 和默认 target 目录。

## 本地检查与验收

项目通过 [rust-toolchain.toml](rust-toolchain.toml) 固定 Rust 1.98.0，使用 minimal profile 并显式声明 rustfmt、clippy。使用 rustup 时，在项目目录运行命令会选择此工具链；首次运行可能需要联网安装。可用 `rustup show active-toolchain` 确认来源指向本项目文件，用 `rustup component list --installed` 检查组件。

在仓库根目录运行：

```sh
cargo check --workspace
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

其中 fmt、clippy、test 是每个 Task 的最低 gate。任务书要求更强检查时，还必须执行对应检查。

已编写 [Rust CI workflow](.github/workflows/ci.yml)：push 到 main、目标为 main 的 PR 和手动触发时，在 Ubuntu 24.04 中读取项目工具链，分别执行 fmt、clippy 和 test；后两条命令还使用 `--locked`。2026-09-22 对提交 `12a89a39ea7b08c610159a4adbf03733dfd60819` 的本地复核中，actionlint 1.7.12、cargo check 和三条 CI 命令均通过。[首次远端运行 Rust CI #1](https://github.com/YANSHAO0032/tide-matching-engine/actions/runs/35734785743) 与该提交对应，run 和 ci job 均显示成功；当时通过公开页面核验结果，未读取需登录的逐步日志。

2026-09-29 在 T04 启动基线 `f96e81b` 上重新执行 `cargo test -p matching-orderbook --locked --quiet`（126 unit）、`cargo fmt --check`、`cargo clippy --workspace --all-targets --locked -- -D warnings`、`cargo test --workspace --locked --quiet`，全部 exit 0。workspace 合计 **196 个单元测试 + 2 个 compile-fail doctest**（domain 70 unit，orderbook 126 unit，其余四库 0）。T03 的结构 property、双向链接/聚合/priority 边界和 O(1) 尾插访问计数证据已验收；本次复核支持启动 T04，不代表 T04 行为已实现或新的远端 CI 已通过。

核心金融计算禁止浮点数，算术必须 checked；业务时间使用 LogicalClock；OrderBook 保持单写者；队列必须有界；P14 gate 前禁止 unsafe。这些约束需要结合 lint、代码审查和相应测试验证，不能假设默认 Clippy 自动覆盖全部业务语义。

六个 crate 已继承统一 lint：`unsafe_code = forbid`、`unused_must_use = deny`、`clippy::float_arithmetic = deny`、`clippy::disallowed_methods = deny`。[clippy.toml](clippy.toml) 当前禁止 `std::sync::mpsc::channel`。2026-09-22 已用独立临时副本验证六个成员的四项违规探针均被相应 lint 拒绝，合法对照通过；项目四条 Cargo 检查也通过。覆盖范围、局限与依赖审查要求见 [AGENTS.md](AGENTS.md)。

## 开发方式与文档入口

用户已授权 Codex 直接完成当前任务的实现、代码审查、验证和交接。详细协作与工程约束见 [AGENTS.md](AGENTS.md)。

[关键逻辑流程图](docs/logic_flows.md) 是长期维护的流程图入口；当前说明工程门、数值校验、参考撮合、Arena/FIFO 生命周期、取消、生产 Limit GTC、市价保护、IOC/FOK/PostOnly、冰山、止损级联、LogicalClock 到期与独立差分验证。

- [工作规则](assignment/CODEX_GUIDANCE_RULES.md)
- [当前交接记录](assignment/SESSION_HANDOFF.md)
- [Rust 撮合系统 v6 规格](assignment/rust_cex_matching_engine_development_spec_v6.md)
- [任务索引](assignment/README.md)
- [当前 T13 原始任务书](assignment/tasks/T13_P4_cancel_replace.md)（用户独立执行任务书及 PendingR2 新裁定优先）
- [已验收的 T07 任务书](assignment/tasks/T07_P3_ioc_fok_postonly.md)
- [已验收的 T06 任务书](assignment/tasks/T06_P3_market_orders.md)
- [已验收的 T05 任务书](assignment/tasks/T05_P2_differential_invariants.md)

当前 `.gitignore` 忽略整个 `assignment/`，上述资料存在于本机但没有受 Git 跟踪；新 clone 不会自动带上这些文件。版本管理策略仍待用户决定，交接文件继续使用此唯一位置。
