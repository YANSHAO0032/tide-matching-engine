# tide-matching-engine

使用 Rust 实现的中心化交易所现货撮合内核，提供价格时间优先的订单簿、撮合规则和进程内单写者运行时。

> 当前为技术演示和持续开发项目：订单簿与进程内运行时已实现；正式命令提供限定开发环境的本地 WAL、快照恢复与有限去重；完整资金闭环、高可用和行情发布尚未实现，不能据此部署为生产交易所。

## 核心特性

**已实现的内核能力**

- 独立的生产订单簿和参考订单簿；生产簿使用价格索引、订单索引与价档 FIFO，参考簿使用独立的 `Vec + sort` 模型。
- 限价单、市价单、GTC、IOC、FOK、PostOnly、冰山订单、止损触发、改单、撤单替换和批量撤单；具体支持组合见 [订单簿模块说明](docs/orderbook_modules.md) 和 [撤单替换支持矩阵](docs/cancel_replace.md)。
- 价格优先、同价 FIFO、maker 价格成交；整数价格与数量、checked 算术及独立的市场数值规则。
- 用户市价保护、市场价格带及自成交防护（STP）；逻辑时间驱动 DAY/GTD 和会话到期。
- 进程内单写者分片、有界队列、显式市场路由、分片内序号、线性化查询和排空关闭。低层 runtime 暴露 GTC Place、Cancel 和基础数量市价；登记表模式另支持市场范围正式批量撤单、经可信市场配置的普通限价 GTC/PostOnly、分片内用户 FenceOnly 及部分持久市场管理命令，提供有限身份去重与事件封装。具体范围见 [命令与事件身份](docs/command_event_protocol.md)。
- 独立 Reference/Production 差分、固定种子命令磁带、性质和并发测试。

**边界与规划**

撮合内核负责订单匹配、订单簿状态和业务事件。账户与余额校验、资金冻结及释放、费率决策、完整风控、清结算、前端和网关服务属于外围系统。仓库的 `matching-risk` 包含本地预留令牌验证与登记表计划，用于验证接口和归属；它不是完整账户、风控或结算系统。当前订单终结后的 `PendingR2` 只表示归属迁移，不表示资金已释放。

开发环境可从空状态启动正式命令 WAL 逐条同步或本地 GroupCommit 路径，并从可信快照或初始状态重放已提交记录、恢复有限去重窗口及本地持久续写。稳定事件传输协议、完整跨重启幂等闭环、行情快照及增量发布尚未实现。

## 架构总览

```mermaid
flowchart LR
    G[订单网关与资金校验<br/>外部系统] -. 规划接入 .-> Q[消息队列<br/>外部系统 规划]
    Q -. 规划接入 .-> R[有界分片入口]
    R --> S[单写者撮合线程]
    S --> B[市场本地订单簿]
    S --> E[进程内业务事件]
    S -->|开发模式正式命令| W[本地 WAL 与 manifest 同步]
    S --> P[快照屏障与持久提交]
    P --> H[本地快照与 WAL 重放续写]
    W --> H
    E -. 规划投递 .-> F[资金与对账服务]
    E -. 规划投递 .-> M[行情服务]
```

实线表示当前代码中可用的关系；WAL 路径仅适用于开发环境的指定正式入口。虚线表示规划中的外围接入；MQ 和事件发布尚未实现。余额校验、预留授权和费用决策由调用方或外围系统负责。

workspace 包含 `matching-domain`、`matching-orderbook`、`matching-runtime`、`matching-risk`、`matching-persistence` 和 `matching-protocol`；`matching-persistence` 提供 WAL 物理组件及开发环境运行时使用的同步能力，`matching-protocol` 仍是骨架。每个生产订单簿是市场本地状态，runtime 使用显式路由将市场交给唯一分片线程；跨进程所有权、迁移和高可用尚未实现。

## 订单状态

领域层提供 `Accepted`、`PartiallyFilled`、`Filled`、`Canceled`、`Expired` 和 `Rejected` 状态标签；该枚举本身不校验状态迁移。典型业务关系如下，具体入口以实际返回的结果和 `EngineEvent` 为准：

- 通过接收检查后进入 `Accepted`；接收前校验失败可表现为 `Rejected`。
- 已接收订单部分成交进入 `PartiallyFilled`，之后可以继续部分成交、全部成交、撤销或到期。
- 全部成交进入 `Filled`；撤销或剩余量按规则终结进入 `Canceled`；有效期到达进入 `Expired`。
- `Filled`、`Canceled`、`Expired` 和 `Rejected` 是上述模型的终结状态。当前尚无覆盖所有入口的统一持久化订单状态机。

## 撮合规则

- **价格优先、时间优先**：买方更高价、卖方更低价优先；同价按真实挂单队列 FIFO。成交价格取 maker 价格。
- **数值校验**：`Price`、`Qty` 使用整数；完整市场规则通过 `MarketMath` 校验 tick、lot、精确本金和溢出。低层入口可能要求调用方先完成市场规则校验，不能假定每个入口都自动执行相同的 R1 校验。
- **STP**：支持已实现的 CancelMaker、CancelTaker、CancelBoth 等策略；不把自成交拦截构造成真实 Trade。各订单入口的支持范围以代码和测试为准。
- **IOC**：尽可能立即成交，未成交余量终结，不入簿。
- **FOK**：先在候选计划中检查完整可执行量与约束；不足时不提交成交或订单簿变更。
- **市价保护**：在命令开始固定参考价，合并用户保护和市场价格带；首个越界 maker 不成交。配置与舍入规则见 [数值公式](docs/numerical_formulas.md)。

余额、费率及完整风控由调用方或外围资金系统提供；撮合不能将业务余量当作资金释放。

## 快速开始

项目通过 [`rust-toolchain.toml`](rust-toolchain.toml) 固定 Rust **1.98.0**，使用 Rust 2024 edition、`rustfmt` 和 `clippy`。在仓库根目录执行：

```bash
rustup show active-toolchain
cargo build --workspace
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

当前仓库没有网络服务或可独立启动的撮合进程，因此没有 `cargo run` 服务启动命令。可在另一个 Rust 程序中以库方式依赖 `matching-domain` 和 `matching-orderbook`。例如在该程序的 `Cargo.toml` 中填写实际路径：

```toml
[dependencies]
matching-domain = { path = "../tide-matching-engine/crates/matching-domain" }
matching-orderbook = { path = "../tide-matching-engine/crates/matching-orderbook" }
```

```rust
use matching_domain::{
    ClusterId, LimitOrder, OrderId, Price, Qty, ShardId, Side, StpKey, StpMode,
    TradeIdCursor, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};

fn order(id: u128, user: u64, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(user),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn main() {
    let policy = QueuePriorityPolicy::try_new(1024).expect("有效的优先级策略");
    let mut book = ProductionOrderBook::new(policy);
    book.rest(order(1, 10, Side::Sell, 100, 2))
        .expect("挂入卖单");

    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(1), 1);
    let events = book
        .place_limit_gtc_with_cursor(order(2, 20, Side::Buy, 101, 1), &mut cursor)
        .expect("执行买单");
    for event in events {
        println!("{event:?}");
    }
}
```

示例展示直接调用订单簿：卖单为 maker，买单为 taker，成交价为 `100`。示例未接入账户、资金或生产订单入口，调用方须自行提供有效身份并执行适用的市场和资金校验。使用单写者线程和显式路由的接口见 [runtime 说明](docs/runtime.md)。

## 配置说明

当前没有 TOML 配置文件解析器；市场数值规则、队列容量、等待策略和路由通过 Rust 类型显式构造。下面仅示意外围应用可以提供的配置结构，**不能直接作为本项目配置文件加载**：

```toml
[market]
market_id = 1
price_scale = 0
qty_scale = 0
quote_scale = 0
tick_size = 1
lot_size = 1

[shard]
shard_id = 1
route_epoch = 1
capacity = 1024
mutation_reserved_slots = 1
max_query_inflight = 512
max_control_inflight = 128
wait_strategy = "Hybrid"
```

应用须自行解析和验证配置，再构造 `MarketMath`、`MarketRoute` 和 `ShardConfig`；不同数值 scale 与最小 tick × lot 的精确本金约束见 [数值公式](docs/numerical_formulas.md)。

## 持久化与崩溃恢复

**开发环境正式入口支持 WAL 本地同步、快照、已提交前缀回放和 LocalDurable 批量续写。** 恢复时在严格清单锁下校验快照和 WAL，先重放、认领所有权，再修复尾部并按原水位续写；调用方负责独占 WAL 和快照目录并提供可信启动配置。无快照的市场恢复还须提供与原启动一致的优先级策略；备节点持久和跨进程租约不在当前实现中。正式入口在分片的有界窗口按 `CommandId` 去重；快照和已提交 WAL 重放恢复该窗口，旧低层入口及窗口外重试不具备该保证。物理格式和截断边界见 [WAL 说明](docs/wal.md)。

当前恢复按严格清单选择有效快照，重放已提交的连续日志并恢复去重窗口；文件损坏和缺失记录按恢复边界拒绝。事件摘要用于比较恢复结果；对外事件发布及备节点晋升仍须在相应阶段实现。

v6 §55.6 的 Stop、Amend、CancelReplace、PreOpen 重校验及市场迁移额外回放测试均予保留，须在相关功能与持久命令路径具备后补齐，并在最终验收前通过；当前回放覆盖及其边界见 [快照说明](docs/snapshot.md)。

## Benchmark

当前没有可引用的性能基准结果或吞吐承诺。未来基准应分别记录硬件、构建参数、订单分布、簿深、线程配置和样本量：

| 指标 | 含义 | 结果 |
|---|---|---|
| 撮合延迟 p50 / p99 | 内核接收已准备命令到完成撮合的延迟分位数，不含网络和 MQ | 待测 |
| 撮合吞吐量 | 指定负载下每秒完成的订单或命令数量 | 待测 |
| 入口繁忙率 | 有界入口因容量、配额或准入竞争返回 Busy 的比例 | 待测 |

端到端延迟应单独统计，不能把网络、消息队列或持久化时间混入纯撮合内核指标。

## 测试体系

- **单元与边界测试**：数值校验、订单簿结构、FIFO、订单规则、STP、价格保护和失败原子性。
- **差分与性质测试**：独立 Reference/Production 逐命令比较原序事件和状态；包含固定种子 100k 混合命令磁带及重复运行场景。这是进程内确定性验证，不是 WAL 崩溃回放。
- **runtime 测试**：有界入口、路由、序号、查询与屏障顺序、关闭、故障传播和 100 个并发生产者。
- **持久化与恢复测试**：当前覆盖 WAL 物理记录、提交水位、快照文件中断边界、损坏、截断和受可信快照边界约束的段回收；运行时状态回放和崩溃重启验收仍属后续工作。
- **输入变异测试**：WAL 记录和扫描分别运行固定种子变异测试；尚未执行覆盖率引导的 fuzz 测试。

完整检查命令及近期本地验证证据见 [工程规则](AGENTS.md)；测试结果不等于生产安全性。

## 事件协议

当前事件及 `EventEnvelope` 为 Rust 进程内类型；正式限价下单、批量撤单和用户栅栏可产生带根命令身份和连续索引的事件。尚无稳定二进制编码、网络协议或事件日志格式。

| 类型 | 关键内容 | 当前状态 |
|---|---|---|
| `TradeEvent` | `trade_id`、`maker_order_id`、`taker_order_id`、maker 价格 `price`、成交数量 `qty`、`maker_side` | 已实现；不含费用、结算或持久化元数据 |
| 订单生命周期事件 | `EngineEvent` 包含接收、挂单、部分成交、完成、取消、改单等变体；取消余量区分 Base / Quote | 已实现部分事件载荷；并非所有低层入口都生成完整生命周期事件 |
| `BookSnapshotEvent` | 市场、价格档位、聚合数量和快照序列等 | 规划中，尚无稳定类型或发布 |
| `BookDiffEvent` | 价格档位增量及行情序列等 | 规划中，尚无稳定类型或发布 |

maker 是提供挂单流动性的一方，taker 是主动与其成交的一方。对外可靠投递、行情序列、资金对账和去重应由后续协议与外围系统实现，不能假定当前事件已持久化。

## 设计取舍与已知限制

- 生产簿与参考簿独立实现，以差分测试换取可审查性；不能把测试通过等同于形式化证明。
- 工作线程独占订单簿，入口有界并可能返回 Busy。请求入队后若等待响应超时，执行结果未知；旧低层入口不去重；正式入口使用有限窗口去重；可信快照与已提交 WAL 可恢复窗口，但窗口淘汰后重试可能重复执行。`ArrayQueue` 本身是 MPMC，本项目通过唯一消费者约束按 MPSC 使用。
- 当前 runtime 登记表模式开放市场范围正式批量撤单、可信配置的普通限价 GTC/PostOnly、分片内 FenceOnly 及部分市场管理命令；正式改单、止损入口，以及完整深度或订单列表查询尚未接入。低层 Cancel 返回移除节点，不伪造资金释放。
- 费率版本更新已支持本地 WAL 和快照；启用费率簿后，已接入的正式限价单使用版本费率预留与成交结算，批量撤单和市场终结可释放真实剩余。其余订单和正式命令范围见[风险与结算管线](docs/risk_pipeline.md)。
- `TradeIdCursor` 按集群、分片、分片序号和命令内成交索引编码；只有实际 Trade 消耗索引。本地持久前缀可恢复分片序列；缺少跨进程所有权和完整 fencing 时，不能保证跨进程唯一。
- 完整 MarketState/PreOpen、账户资金、手续费核算、Settlement、跨分片去重、HA、市场迁移和对外行情发布尚未完成；本地 WAL 回放和续写仅覆盖已接入的正式命令。

## 生产部署建议与监控

当前版本不具备生产部署条件。上线前应完成账户与资金对接、持久化及恢复、幂等、权限与隔离、主节点 fencing、行情和事件可靠投递，并经过独立代码审计、故障演练、性能压测与资金对账。

未来部署可监控队列占用与 Busy 比例、命令处理延迟、分片线程存活和失败、路由版本拒绝、业务拒绝率、序号水位，以及 WAL 持久化延迟、回放状态、事件积压和对账差异。队列持续饱和、线程异常退出、持久化失败、序号异常或对账不一致应触发告警；这些是建议指标，不代表当前已接入监控系统。

## CHANGELOG

- **T26**：增加市场创建、配置更新、交易时段和状态转换的持久命令及重放；关市清理普通单和 Stop 单并将预留转入 PendingR2。真实 R2、动态参考价及其保留回放测试仍由后续任务完成，具体边界见[市场生命周期](docs/market_lifecycle.md)。
- **T25**：为当前正式命令增加快照及可信初始状态后的 WAL 重放、状态与事件摘要、本地持久续写；v6 §55.6 其他场景按对应功能任务保留，须在最终验收前补齐。
- **T24**：新增分片快照屏障、不可变镜像、后台原子文件与清单提交、规范状态摘要和恢复校验；当时完整 WAL 回放及持久续写仍属后续范围。
- **T23**：增加本地持久水位恢复、未确认尾部修复和受可信快照边界约束的 WAL 段回收；当时运行时状态重放及持久化重启续写仍是后续范围。
- **T22**：增加双槽 manifest、连续持久提交水位及开发环境的本地逐条同步和 GroupCommit 正式命令入口；当时崩溃恢复和真实备节点复制仍待实现。
- **T21**：增加独立 WAL 记录、CRC32C、segment 轮转与扫描及尾部截断基础；当时尚未接入运行时持久化与恢复。
- **T20**：增加分片内有限命令去重、客户端订单身份与用户控制，并接入可信配置的正式限价下单；当时持久化和跨分片保证仍待后续实现。
- **T19**：新增进程内命令与事件封装，登记表模式支持市场范围正式批量撤单；交付状态以 Git 和交接记录为准。
- **T18**：完成单写者运行时查询、屏障、背压和关闭的本地收口验证，补充并发、回复丢失与超时边界测试；交付状态以 Git 记录为准。
- **当前工作区**：本地实现并验收进程内单写者分片 runtime、显式路由和命令级 TradeId 游标；交付状态以 Git 记录为准。
- **T16**：本地提交市场价格带及用户保护集成，提交 `625bdd2`。
- **T15 / T14**：分别完成自成交防护、批量撤单的本地交付。
- **此前阶段**：完成订单簿、限价和市价、冰山、止损、逻辑时间和改单等基础能力。

## 免责声明

本项目仅用于技术演示和工程研究，不构成可直接上线的交易所或投资建议。任何实际部署前都必须完成独立安全审计、压力测试、故障恢复演练和资金对账验证。

## 术语

- **tick_size / lot_size**：价格最小步长 / 基础数量最小步长。
- **WAL**：预写日志，用于记录状态变更；开发环境的指定正式入口支持本地同步、已提交前缀恢复和 LocalDurable 续写。
- **IOC / FOK**：立即成交剩余取消 / 仅在可全部立即成交时执行。
- **maker / taker**：提供挂单流动性的一方 / 主动消耗流动性的一方。
