# tide-matching-engine

使用 Rust 构建的 CEX Matching Core / Matching Node 项目，长期目标是确定性（deterministic）、事件溯源（event-sourced）和多分片（multi-shard）的撮合核心。

## 当前状态

当前阶段为 **P0 / T00：Workspace、CI 与工程规则**。

当前已建立包含六个无依赖 library crate 的 virtual workspace（edition 2024），项目工具链固定为 Rust 1.98.0。各库目前只有职责文档注释，没有业务实现或业务测试；根目录不再保留 Hello World binary。CI workflow 已编写并通过本地 Review，尚未取得远端执行证据。

本说明中的撮合、持久化和多分片能力均为规划目标，不代表当前已经实现或经过生产验证。已完成事项与实际验证结果以 [SESSION_HANDOFF.md](assignment/SESSION_HANDOFF.md) 中的证据为准。

## 项目范围

长期范围包括价格时间优先撮合、订单生命周期与交易规则、单写者执行模型、有界背压、命令幂等、WAL、快照、确定性重放，以及后续阶段的多分片和主备复制。

账户中心、充值提现、KYC、财务总账、清结算数据库和 Web 前端不属于撮合核心。核心与外围资金、订单、行情和清算系统的集成协议，以及参考风控/资金实现，按规格对应阶段推进。

T00 只建立工程基础，不实现 OrderBook 算法，不引入 Tokio 到撮合核心，不提前增加 Gateway、HA 或 MarketData 组件。

## T00 crate 边界

以下六个 crate 的最小骨架已创建在 `crates/` 下。本表列出规划职责，业务功能尚未实现；当前没有第三方或 crate 间依赖，后续按实际需求添加。

| Crate | 职责 |
|---|---|
| `matching-domain` | 领域 ID、整数数值类型、订单/市场语义、命令、事件与错误模型 |
| `matching-orderbook` | 订单簿、价格档位、FIFO、撮合规则、参考模型和状态不变量 |
| `matching-runtime` | 单写者 shard 执行、有界消息入口、路由、排序及处理流程编排 |
| `matching-persistence` | WAL、快照、manifest、恢复与持久性边界 |
| `matching-risk` | 交易前校验、价格保护、reservation 与参考资金规则 |
| `matching-protocol` | 外部协议帧、编解码、版本和稳定的 reason code 契约 |

T00 只需要这些 crate 的最小可编译骨架，其业务实现属于后续任务。规格中的完整目录是长期目标，不是当前创建清单。

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

已编写 [Rust CI workflow](.github/workflows/ci.yml)：push 到 main、目标为 main 的 PR 和手动触发时，在 Ubuntu 24.04 中读取项目工具链，分别执行 fmt、clippy 和 test；后两条命令还使用 `--locked`。2026-09-22 本地 Review 中 actionlint 1.7.12 和三条 CI 命令均通过，远端运行尚待验证。

workspace 骨架验收已确认恰好六个 library 成员，且 `cargo check --workspace` 通过。六个库的单元测试和文档测试实际运行数均为 **0**；这只证明骨架可构建，不能据此宣称撮合正确或 T00 已完成。本地通过与远端 CI 通过需要分别记录证据。

核心金融计算禁止浮点数，算术必须 checked；业务时间使用 LogicalClock；OrderBook 保持单写者；队列必须有界；P14 gate 前禁止 unsafe。这些约束需要结合 lint、代码审查和相应测试验证，不能假设默认 Clippy 自动覆盖全部业务语义。

六个 crate 已继承统一 lint：`unsafe_code = forbid`、`unused_must_use = deny`、`clippy::float_arithmetic = deny`、`clippy::disallowed_methods = deny`。[clippy.toml](clippy.toml) 当前禁止 `std::sync::mpsc::channel`。2026-09-22 已用独立临时副本验证六个成员的四项违规探针均被相应 lint 拒绝，合法对照通过；项目四条 Cargo 检查也通过。覆盖范围、局限与依赖审查要求见 [AGENTS.md](AGENTS.md)。

## 开发方式与文档入口

用户负责实现，Codex 负责导师指导、代码审查、验证和交接；每次推进一个约 15–60 分钟的小步骤。详细协作与工程约束见 [AGENTS.md](AGENTS.md)。

- [导师模式规则](assignment/CODEX_GUIDANCE_RULES.md)
- [当前交接记录](assignment/SESSION_HANDOFF.md)
- [Rust 撮合系统 v6 规格](assignment/rust_cex_matching_engine_development_spec_v6.md)
- [任务索引](assignment/README.md)
- [当前 T00 任务书](assignment/tasks/T00_P0_workspace_ci_rules.md)

当前 `.gitignore` 忽略整个 `assignment/`，上述资料存在于本机但没有受 Git 跟踪；新 clone 不会自动带上这些文件。版本管理策略待 T00 后续工程检查确认，交接文件继续使用此唯一位置。
