//! 控制平面的线性一致存储适配，不进入撮合线程。
mod authenticated_recovery;
mod cursor;
mod etcd;
mod fenced_log;
mod replica_tls;
mod replicated_appender;
mod replication;
mod route;

/// 使用认证复制链协调创世 WAL 重放和接任。
pub use authenticated_recovery::AuthenticatedRecovery;
/// 发布者租约及分片消费者的持久游标。
pub use cursor::{
    CursorCheckpoint, CursorPosition, CursorStore, PublisherLease, PublisherToken, RequiredSinks,
};
/// 有界 etcd 读写和比较版本。
pub use etcd::{EtcdClient, KvVersion};
/// 原子写权与跨主机连续持久日志。
pub use fenced_log::{DigestCheckpoint, FencedLog, FencedLogWriter, ReplicaWriterIdentity};
/// 完成双向 TLS 与固定证书校验的复制连接。
pub use replica_tls::{TlsReplicaStream, replica_client_config, replica_server_config};
/// 本地同步后通过外围线程等待备节点确认的写入器。
pub use replicated_appender::{
    ReplicaCommitFence, ReplicaHealth, ReplicatedAppender, ReplicationStream,
};
/// 在调用方认证并设置期限的流上交换有界 WAL 复制批次。
pub use replication::{
    ReplicaBatchSink, ReplicaIntentVerifier, ReplicaPrefixBoundary, activate_replica_writer,
    confirm_authoritative_replica, read_replica_prefix, receive_authenticated_replica_resume,
    receive_authenticated_replication_batch, receive_replica_fence, receive_replica_resume,
    receive_replication_batch, send_replica_fence, send_replica_prefix, send_replica_resume,
    send_replication_batch, serve_authenticated_fenced_prefix, serve_fenced_prefix,
};
/// etcd 中的路由快照比较写入适配器。
pub use route::EtcdRouteAuthority;
