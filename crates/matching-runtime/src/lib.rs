//! 单写者分片执行、有界消息入口、路由、序号及有限正式命令编排。

mod client_order_id;
mod event_hash;
mod idempotency;
/// 原序业务事件的确定性摘要。
pub use event_hash::EventHasher;
mod ownership;
mod place_candidate;
mod place_config;
mod queue;
mod recovery;
mod route_table;
mod sequencer;
mod user_control;
mod wait_strategy;
mod worker;

/// 分片内有界幂等窗口配置。
pub use idempotency::IdempotencyConfig;
/// 正式限价下单使用的可信市场策略与资源上限。
pub use place_config::{PlaceConfigError, PlaceLimits, PlaceSupportConfig};
/// 本地持久前缀中的正式命令。
pub use recovery::{
    RecoveredCommand, SnapshotRecoveryRecords, SnapshotReplayResult, recover_local_commands,
    recover_snapshot_records, recover_snapshot_records_with_genesis,
};
/// 显式路由与配置错误。
pub use route_table::{DuplicateMarketRoute, MarketRoute, RouteState, RouteTable};
/// 工作线程空闲等待策略。
pub use wait_strategy::WaitStrategy;
/// 单写者工作线程及其结果。
pub use worker::{
    Execution, FormalError, FormalExecution, FormalOutcome, Mutation, MutationEnvelope,
    MutationOutcome, Shard, ShardConfig, ShardError, ShardHandle, SnapshotError, SnapshotImage,
    SnapshotRecoveryCandidate, SnapshotRestoreConfig, SnapshotWriter, SnapshotWriterError,
};
mod router;

/// 基于显式快照的多分片路由及其错误。
pub use router::{Router, RouterError};
