//! 单写者分片执行、有界消息入口、路由、序号及有限正式命令编排。

mod ownership;
mod queue;
mod route_table;
mod sequencer;
mod wait_strategy;
mod worker;

/// 显式路由与配置错误。
pub use route_table::{DuplicateMarketRoute, MarketRoute, RouteState, RouteTable};
/// 工作线程空闲等待策略。
pub use wait_strategy::WaitStrategy;
/// 单写者工作线程及其结果。
pub use worker::{
    Execution, FormalExecution, FormalOutcome, Mutation, MutationEnvelope, MutationOutcome, Shard,
    ShardConfig, ShardError, ShardHandle,
};
mod router;

/// 基于显式快照的多分片路由及其错误。
pub use router::{Router, RouterError};
