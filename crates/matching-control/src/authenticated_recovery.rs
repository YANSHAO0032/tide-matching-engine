//! 证书绑定写权下的无快照分片接任协调。
use crate::{FencedLogWriter, ReplicatedAppender, TlsReplicaStream};
use matching_persistence::{FileManifestStorage, ManifestManager, RegisteredAppender};
use matching_runtime::{
    ReplicatedRecoveryCoordinator, SnapshotRecoveryRecords, SnapshotReplayResult,
    SnapshotRestoreConfig,
};
use std::{
    io,
    path::Path,
    time::{Duration, Instant},
};

/// 在指定可信状态和事件摘要下协调权威前缀、业务重放及认证复制。
///
/// 当前只接受完整创世 WAL；可信摘要由外围提供或从权威检查点读取。
/// 本地落后时核验并补齐权威前缀；本地超前时在重放验证后协调尾部。
/// 本类型不负责选主或路由租约。
pub struct AuthenticatedRecovery {
    /// 与本次接任绑定的权威写者。
    writer: FencedLogWriter,
    /// 已完成证书认证的备节点连接。
    stream: TlsReplicaStream,
    /// 外围提供或从权威检查点读取的状态摘要。
    state_digest: [u8; 32],
    /// 外围提供或从权威检查点读取的累计事件摘要。
    event_hash: [u8; 32],
    /// 是否在恢复时读取最新权威检查点。
    authority_digest: bool,
    /// 已取得的权威检查点序号。
    checkpoint_seq: Option<u64>,
    /// 没有摘要时是否已验证首次回复前摘要协议的创世标记。
    genesis_verified: bool,
    /// 计算状态摘要的容量上限。
    max_state_bytes: usize,
    /// 单个复制帧的容量上限。
    max_frame: usize,
    /// 单阶段外围操作的总期限。
    timeout: Duration,
    /// 已由备节点和权威日志协调的连续水位。
    baseline: Option<u64>,
    /// 是否已经核对重放生成的状态及事件。
    replay_verified: bool,
}

impl AuthenticatedRecovery {
    /// 构造尚未执行协调的恢复器；摘要和 TLS 配置由可信外围提供。
    pub fn new(
        writer: FencedLogWriter,
        stream: TlsReplicaStream,
        (state_digest, event_hash): ([u8; 32], [u8; 32]),
        max_state_bytes: usize,
        max_frame: usize,
        timeout: Duration,
    ) -> io::Result<Self> {
        crate::replication::validate_limit(max_frame)?;
        if max_state_bytes == 0
            || timeout.is_zero()
            || Instant::now().checked_add(timeout).is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "恢复容量或期限无效",
            ));
        }
        Ok(Self {
            writer,
            stream,
            state_digest,
            event_hash,
            authority_digest: false,
            checkpoint_seq: None,
            genesis_verified: false,
            max_state_bytes,
            max_frame,
            timeout,
            baseline: None,
            replay_verified: false,
        })
    }
    /// 构造使用权威摘要的恢复器；协调时读取最新检查点。
    ///
    /// 没有检查点时，必须验证创世协议标记并从完整创世 WAL 重放。
    pub fn from_authority(
        writer: FencedLogWriter,
        stream: TlsReplicaStream,
        max_state_bytes: usize,
        max_frame: usize,
        timeout: Duration,
    ) -> io::Result<Self> {
        let mut recovery = Self::new(
            writer,
            stream,
            ([0; 32], [0; 32]),
            max_state_bytes,
            max_frame,
            timeout,
        )?;
        recovery.authority_digest = true;
        Ok(recovery)
    }
    fn deadline(&self) -> io::Result<Instant> {
        Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "恢复期限溢出"))
    }
}

impl ReplicatedRecoveryCoordinator for AuthenticatedRecovery {
    type Appender = ReplicatedAppender<RegisteredAppender<FileManifestStorage>>;
    fn replay_checkpoint(&self) -> Option<matching_runtime::ReplayCheckpoint> {
        self.checkpoint_seq
            .map(|shard_seq| matching_runtime::ReplayCheckpoint {
                shard_seq,
                state_digest: self.state_digest,
                event_hash: self.event_hash,
                max_state_bytes: self.max_state_bytes,
            })
    }
    fn recover(
        &mut self,
        wal: &Path,
        _snapshots: &Path,
        manager: &mut ManifestManager<FileManifestStorage>,
        trusted: &SnapshotRestoreConfig,
        policies: &[(
            matching_domain::MarketId,
            matching_orderbook::QueuePriorityPolicy,
        )],
    ) -> io::Result<SnapshotRecoveryRecords> {
        if self.baseline.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "恢复器只支持一次创世前缀协调",
            ));
        }
        let deadline = self.deadline()?;
        let baseline =
            self.writer
                .reconcile_authenticated_remote_prefix(&mut self.stream, 0, deadline)?;
        if self.authority_digest {
            if let Some(checkpoint) = self.writer.load_latest_digest(deadline)? {
                if checkpoint.shard_seq > baseline {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "摘要与备节点水位不一致",
                    ));
                }
                self.state_digest = checkpoint.state_digest;
                self.event_hash = checkpoint.event_hash;
                self.checkpoint_seq = Some(checkpoint.shard_seq);
            } else {
                self.writer.verify_genesis_protocol(deadline)?;
                self.genesis_verified = true;
            }
        }
        if manager.state().local_durable_seq < baseline {
            self.writer
                .extend_verified_local_prefix(wal, manager, 64 * 1024 * 1024, deadline)?;
        }
        let result = self
            .writer
            .recover_verified_genesis_prefix(wal, manager, trusted, policies, deadline)?;
        if result.snapshot().committed_through != baseline {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "本地与备节点水位不一致",
            ));
        }
        self.baseline = Some(baseline);
        Ok(result)
    }
    fn verify_replay(&mut self, replay: &SnapshotReplayResult) -> io::Result<()> {
        if self.baseline.is_none()
            || self.replay_verified
            || (self.checkpoint_seq.is_none()
                && !self.genesis_verified
                && (replay
                    .image
                    .state_digest(self.max_state_bytes)
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "恢复状态摘要计算失败")
                    })?
                    != self.state_digest
                    || replay.event_hash != self.event_hash))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "恢复状态或事件摘要不一致",
            ));
        }
        self.writer.verify_bound_lease(self.deadline()?)?;
        self.replay_verified = true;
        Ok(())
    }
    fn reconcile_local(
        &mut self,
        wal: &Path,
        manager: &mut ManifestManager<FileManifestStorage>,
        trusted: &SnapshotRestoreConfig,
        baseline: u64,
    ) -> io::Result<()> {
        if !self.replay_verified || self.baseline != Some(baseline) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "重放尚未验证"));
        }
        if manager.state().local_durable_seq > baseline {
            let deadline = self.deadline()?;
            matching_persistence::reconcile_local_prefix(
                wal,
                manager,
                baseline,
                trusted.cluster.get(),
                trusted.shard.get(),
                |records, target| {
                    self.writer
                        .verify_reconciliation_prefix(records, target, deadline)
                },
            )?;
        }
        if manager.state().local_durable_seq != baseline {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "本地与权威水位不一致",
            ));
        }
        Ok(())
    }
    fn finish(
        mut self,
        local: RegisteredAppender<FileManifestStorage>,
        baseline: u64,
        _replay: &SnapshotReplayResult,
    ) -> io::Result<Self::Appender> {
        if !self.replay_verified || self.baseline != Some(baseline) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "恢复阶段或水位不一致",
            ));
        }
        let deadline = self.deadline()?;
        self.writer.verify_bound_lease(deadline)?;
        crate::send_replica_resume(
            &mut self.stream,
            self.writer.replica_identity(),
            baseline,
            deadline,
        )?;
        self.writer.verify_bound_lease(deadline)?;
        ReplicatedAppender::new_authenticated(
            local,
            self.stream,
            self.writer,
            self.max_frame,
            self.timeout,
        )
    }
}
