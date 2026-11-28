//! 单写者分片执行、有界消息入口、路由、序号及有限正式命令编排。

mod audit;
/// 管理审计的固定字段和本地持久日志。
pub use audit::{AuditAction, AuditDispatchError, AuditLog, AuditOutcome, AuditRecord};
mod client_order_id;
mod control_store;
pub mod cross_control;
/// 可信文件目录中的单机排他主节点租约。
pub use control_store::{ControlCoordinator, ControlStore, LeaseError, PrimaryLease};
mod event_hash;
mod health;
/// 进程健康依赖和分片入口汇总。
pub use health::{Health, HealthDependency};
mod idempotency;
/// 原序业务事件的确定性摘要。
pub use event_hash::EventHasher;
mod migration;
/// 停市迁移的持久交接与切换结果。
pub use migration::{MigrationId, MigrationResult, MigrationStorage};
mod ownership;
mod place_candidate;
mod place_config;
mod queue;
mod recovery;
mod route_store;
mod route_table;
mod sequencer;
#[cfg(test)]
#[path = "../tests/t22_support/mod.rs"]
mod test_directory;
mod user_control;
mod wait_strategy;
mod worker;

/// 分片内有界幂等窗口配置。
pub use idempotency::IdempotencyConfig;
/// 正式限价下单使用的可信市场策略与资源上限。
pub use place_config::{PlaceConfigError, PlaceLimits, PlaceSupportConfig};
/// 本地持久命令、快照和行情恢复入口。
pub use recovery::{
    CommittedMarketDataReplay, RecoveredCommand, ReplayCheckpoint, ReplicatedRecoveryCoordinator,
    SnapshotRecoveryRecords, SnapshotReplayResult, recover_local_commands,
    recover_market_data_from_genesis, recover_snapshot_records,
    recover_snapshot_records_with_genesis, recover_verified_genesis_records,
    recover_verified_snapshot_records_with_genesis,
};
/// 单机可信目录中的持久路由控制存储。
pub use route_store::{
    LocalRouteAuthority, MigrationRouteChange, RouteAuthority, RouteControlStore, RouteCoordinator,
    RouteMonitor, decode_route_snapshot, encode_route_snapshot,
};
/// 显式路由与配置错误。
pub use route_table::{DuplicateMarketRoute, MarketRoute, RouteState, RouteTable};
/// 工作线程空闲等待策略。
pub use wait_strategy::WaitStrategy;
/// 单写者工作线程及其结果。
pub use worker::{
    DiagnosticEvent, DiagnosticKind, Execution, FormalError, FormalExecution, FormalOutcome,
    MarketDataOpenDetail, Mutation, MutationEnvelope, MutationOutcome, Shard, ShardConfig,
    ShardError, ShardHandle, ShardObservation, SnapshotError, SnapshotImage,
    SnapshotRecoveryCandidate, SnapshotRestoreConfig, SnapshotWriter, SnapshotWriterError,
};
mod router;

/// 基于显式快照的多分片路由及其错误。
pub use router::{Router, RouterError};

mod feed_monitor;
mod feeds;
/// 行情构建器水位和缺口的有界观测。
pub use feed_monitor::{FeedCursorMonitor, FeedGapTelemetry, FeedKind, FeedMonitor, FeedTelemetry};
mod market_data;
/// 独立公开行情流及其构建器。
pub use feeds::{
    BboMessage, BestLevel, FeedBuilder, FeedCursor, FeedOutput, FeedSeqSnapshot, L2Client,
    L2Snapshot, L3Client, L3Snapshot, MarketMigrationBoundary, MbpMessage, PublicTrade,
    ResumeError,
};
/// 市场行情源与逐订单构建器。
pub use market_data::{
    MarketDataState, MarketProjector, MarketSource, MboBuilder, MboChange, MboError, MboMessage,
    MboOrder, SourceEvent, SourcePayload,
};

mod session_control;
/// 本机会话断连意图及其持久目标。
pub use session_control::{
    SessionControlJournal, SessionDispatchError, SessionTarget, dispatch_session_cancel,
    dispatch_session_cancels,
};

mod gateway;
/// 网关会话认证和限价意图准入。
pub use gateway::{
    GatewayAuthenticator, GatewayError, GatewayPrincipal, GatewaySession, PlaceIntent,
    PreparedPlace,
};

mod gateway_intents;
/// 本机持久网关意图身份表。
pub use gateway_intents::GatewayIntentStore;

mod gateway_transport;
/// 已认证连接的同步有界帧适配器。
pub use gateway_transport::{GatewayShared, GatewayTransport, GatewayTransportError};
