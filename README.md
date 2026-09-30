# tide-matching-engine

使用 Rust 构建的 CEX Matching Core / Matching Node 项目，长期目标是确定性（deterministic）、事件溯源（event-sourced）和多分片（multi-shard）的撮合核心。

## 当前状态

当前已验收 **T00–T04**，包括 ReferenceOrderBook、Arena / PriceLevel / intrusive FIFO 与 ProductionOrderBook 的 Limit GTC / Cancel 行为；T03 提交为 `f96e81b`。T04 于 2026-09-30 完成本地验收：production 定向 73、Arena 定向 17、workspace 278 个单元测试与 2 个 doctest，以及 fmt、严格 Clippy 均通过；未查询新的远端 CI。导师模式保持；下一任务 T05 仍需用户明确授权，小步进度与最新验证以 [唯一交接摘要](assignment/SESSION_HANDOFF.md) 为准。

当前已建立包含六个 library crate 的 virtual workspace（edition 2024），项目工具链固定为 Rust 1.98.0。matching-domain 已有领域类型、QueuePriority 与基础错误模型；matching-orderbook 已有 ReferenceOrder、IncomingOrder、ReferenceOrderBook 的挂单、双向成交、Cancel 和 TradeEvent，T02 提交为 `161abf7`；其余四库仍为职责注释骨架。orderbook 的运行依赖只有本地 domain，测试依赖为 proptest 1.11.0。CI 的 T00 提交已有远端成功证据，本轮只核验本地 gate，未查询新的远端 CI。

本说明中的撮合、持久化和多分片能力均为规划目标，不代表当前已经实现或经过生产验证。已完成事项与实际验证结果以 [SESSION_HANDOFF.md](assignment/SESSION_HANDOFF.md) 中的证据为准。

## 项目范围

长期范围包括价格时间优先撮合、订单生命周期与交易规则、单写者执行模型、有界背压、命令幂等、WAL、快照、确定性重放，以及后续阶段的多分片和主备复制。

账户中心、充值提现、KYC、财务总账、清结算数据库和 Web 前端不属于撮合核心。核心与外围资金、订单、行情和清算系统的集成协议，以及参考风控/资金实现，按规格对应阶段推进。

T02 已用 `Vec + sort` 建立易审计的参考模型，并验证价格/FIFO、maker-price、相同 command tape 的事件与终态、数量守恒和取消性质。该 API 不自动 rest incoming 余量，尚无完整生命周期事件/命令协议。T03 已完成 OrderIndex、OrderArena slot/free-list、PriceLevel 双向 FIFO 与 QueuePriority/high-water 不变量。T04 已组合生产簿的双侧价格索引和订单 ID 索引，完成 Limit GTC/Cancel、余量挂单及 mutation 后不变量；100k differential 和完整集成 gate 属于 T05。Market/IOC/FOK 等 P3 订单规则、runtime、Gateway、HA 和 MarketData 均不属于当前范围。

## Crate 边界

以下六个 crate 已创建在 `crates/` 下。本表列出规划职责，matching-domain 已完成当前所需领域子集，matching-orderbook 已完成 T02 参考模型、T03 生产簿底层结构和 T04 Limit GTC / Cancel；其余四库业务尚未实现。依赖按实际小步需求添加。

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

用户负责实现，Codex 负责导师指导、代码审查、验证和交接；每次推进一个约 15–60 分钟的小步骤。详细协作与工程约束见 [AGENTS.md](AGENTS.md)。

[关键逻辑流程图](docs/logic_flows.md) 是长期维护的流程图入口；当前使用 ASCII 图说明已验收的工程门、领域值边界、参考撮合、Arena/FIFO 生命周期和生产 Limit GTC 流程，后续任务的关键流程也会在此补充。

- [导师模式规则](assignment/CODEX_GUIDANCE_RULES.md)
- [当前交接记录](assignment/SESSION_HANDOFF.md)
- [Rust 撮合系统 v6 规格](assignment/rust_cex_matching_engine_development_spec_v6.md)
- [任务索引](assignment/README.md)
- [当前 T04 任务书](assignment/tasks/T04_P2_production_orderbook.md)

当前 `.gitignore` 忽略整个 `assignment/`，上述资料存在于本机但没有受 Git 跟踪；新 clone 不会自动带上这些文件。版本管理策略仍待用户决定，交接文件继续使用此唯一位置。
