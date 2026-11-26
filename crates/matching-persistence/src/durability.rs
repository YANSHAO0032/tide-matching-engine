//! 持久性配置、连续前缀水位及 WAL 同步与 manifest 登记编排。
use crate::{
    ManifestIntent, ManifestManager, ManifestState, ManifestStorage, SegmentWriter, WalRecord,
};
use std::io;
use std::time::{Duration, Instant};

/// 运行配置环境。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// 本地开发环境。
    Development,
    /// 性能测量环境。
    Benchmark,
    /// 生产环境。
    Production,
}

/// 本地写入同步方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityMode {
    /// 只保存在内存中。
    MemoryOnly,
    /// 每次写入后同步文件。
    FsyncEach,
    /// 按容量与最大等待时间分批同步。
    GroupCommit {
        /// 每批最大记录数。
        max_batch: usize,
        /// 批次最长等待微秒数。
        max_delay_micros: u64,
    },
}

/// 提交所需的持久副本。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationMode {
    /// 本地文件同步完成后可提交。
    LocalDurable,
    /// 本地和备节点的连续前缀均持久化后可提交。
    StandbyDurable,
}

/// 配置不合法或持久前缀不连续。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityError;

/// 显式运行环境与持久性约束。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityConfig {
    /// 运行环境。
    pub profile: Profile,
    /// 本地同步策略。
    pub mode: DurabilityMode,
    /// 提交所需副本。
    pub replication: ReplicationMode,
    /// 是否要求崩溃恢复。
    pub crash_recovery_required: bool,
    /// 是否启用高可用。
    pub ha_enabled: bool,
    /// 是否明确接受故障切换时丢失已确认数据。
    pub allow_data_loss_on_failover: bool,
}
impl DurabilityConfig {
    /// 校验内存模式、复制策略和批次参数的组合。
    pub fn validate(&self) -> Result<(), DurabilityError> {
        if self.mode == DurabilityMode::MemoryOnly
            && (self.profile == Profile::Production
                || self.ha_enabled
                || self.crash_recovery_required
                || self.replication == ReplicationMode::StandbyDurable)
        {
            return Err(DurabilityError);
        }
        if self.ha_enabled
            && self.replication != ReplicationMode::StandbyDurable
            && !self.allow_data_loss_on_failover
        {
            return Err(DurabilityError);
        }
        if let DurabilityMode::GroupCommit {
            max_batch,
            max_delay_micros,
        } = self.mode
            && (max_batch == 0 || max_delay_micros == 0)
        {
            return Err(DurabilityError);
        }
        Ok(())
    }
}

/// 已由调用方确认实际持久化的连续本地和备节点前缀。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitWatermark {
    /// 持久副本策略。
    mode: ReplicationMode,
    /// 本地连续持久前缀。
    local: u64,
    /// 备节点连续持久前缀。
    standby: u64,
}
impl CommitWatermark {
    /// 从已验证的共同基线构造水位。
    pub const fn new(mode: ReplicationMode, baseline: u64) -> Self {
        Self {
            mode,
            local: baseline,
            standby: baseline,
        }
    }
    /// 本地同步完成后推进连续范围。
    pub fn advance_local(&mut self, first: u64, last: u64) -> Result<(), DurabilityError> {
        if self.local.checked_add(1) != Some(first) || last < first {
            return Err(DurabilityError);
        }
        self.local = last;
        Ok(())
    }
    /// 在收到已验证的备节点持久前缀后推进连续范围。
    pub fn advance_standby(&mut self, first: u64, last: u64) -> Result<(), DurabilityError> {
        if self.mode != ReplicationMode::StandbyDurable
            || self.standby.checked_add(1) != Some(first)
            || last < first
            || last > self.local
        {
            return Err(DurabilityError);
        }
        self.standby = last;
        Ok(())
    }
    /// 返回符合当前副本策略的连续提交水位。
    pub const fn committed(&self) -> u64 {
        match self.mode {
            ReplicationMode::LocalDurable => self.local,
            ReplicationMode::StandbyDurable => {
                if self.local < self.standby {
                    self.local
                } else {
                    self.standby
                }
            }
        }
    }
}

/// 有序追加与同步接口；`sync` 返回成功前必须完成实际同步。
pub trait DurableAppender {
    /// 判断下一条记录是否需要关闭当前 segment。
    fn would_rotate(&self, record: &WalRecord) -> io::Result<bool>;
    /// 按顺序追加记录。
    fn append(&mut self, record: &WalRecord) -> io::Result<()>;
    /// 同步当前文件以及新目录项。
    fn sync(&mut self) -> io::Result<()>;
    /// 取走已核验的备节点持久确认；普通本地写入器不提供确认。
    fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
        None
    }
    /// 返回已知复制链健康状态；本地写入器不提供此观测。
    fn replication_ready(&self) -> Option<bool> {
        None
    }
    /// 返回是否要求工作线程在对外回复前登记业务摘要。
    fn digest_checkpoint_required(&self) -> bool {
        false
    }
    /// 在可信外围登记已提交前缀的状态与有序事件摘要。
    fn checkpoint_digest(
        &mut self,
        _seq: u64,
        _state: [u8; 32],
        _events: [u8; 32],
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "写入器不支持摘要检查点",
        ))
    }
    /// 当前清单是否只以该迁移快照为恢复锚点。
    fn is_migration_snapshot(&self, _id: u64, _seq: u64) -> bool {
        false
    }
    /// 在当前完整前缀登记迁移基线；复制写入器须另行实现相应的持久保证。
    fn commit_migration_snapshot(&mut self, _id: u64, _seq: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "写入器不支持迁移快照",
        ))
    }
    /// 通过同一个清单写者登记已同步的快照文件。
    fn commit_snapshot(&mut self, _snapshot_id: u64, _snapshot_seq: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "写入器不支持快照清单",
        ))
    }
}

/// 类型擦除后的持久写入器，保留原写入器及其持久状态。
pub struct BoxedDurableAppender(Box<dyn DurableAppender + Send>);
impl DurableAppender for BoxedDurableAppender {
    fn would_rotate(&self, record: &WalRecord) -> io::Result<bool> {
        self.0.would_rotate(record)
    }
    fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        self.0.append(record)
    }
    fn sync(&mut self) -> io::Result<()> {
        self.0.sync()
    }
    fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
        self.0.take_replica_ack()
    }
    fn replication_ready(&self) -> Option<bool> {
        self.0.replication_ready()
    }
    fn digest_checkpoint_required(&self) -> bool {
        self.0.digest_checkpoint_required()
    }
    fn checkpoint_digest(&mut self, seq: u64, state: [u8; 32], events: [u8; 32]) -> io::Result<()> {
        self.0.checkpoint_digest(seq, state, events)
    }
    fn commit_snapshot(&mut self, snapshot_id: u64, snapshot_seq: u64) -> io::Result<()> {
        self.0.commit_snapshot(snapshot_id, snapshot_seq)
    }
    fn is_migration_snapshot(&self, id: u64, seq: u64) -> bool {
        self.0.is_migration_snapshot(id, seq)
    }
    fn commit_migration_snapshot(&mut self, id: u64, seq: u64) -> io::Result<()> {
        self.0.commit_migration_snapshot(id, seq)
    }
}

/// 负责批量同步与持久前缀推进的单写者。
pub struct DurableBatcher<A> {
    /// 具有真实同步语义的顺序写入器。
    appender: A,
    /// WAL 追加、同步和批量提交指标，不参与提交决策。
    metrics: std::sync::Arc<crate::DurabilityMetrics>,
    /// 本地同步策略。
    mode: DurabilityMode,
    /// 启动时固定的主节点纪元，需由可信租约上下文提供。
    expected_epoch: u64,
    /// 连续持久及提交水位。
    watermark: CommitWatermark,
    /// 最近成功追加的序号。
    last_appended: u64,
    /// 未同步批次首序号。
    pending_first: Option<u64>,
    /// 未同步批次固定的集群与分片身份。
    pending_identity: Option<(u16, u16)>,
    /// 未同步记录数量。
    pending_count: usize,
    /// 调用方提供的批次首条记录追加时刻，用于判断批次期限。
    pending_since: Option<Instant>,
    /// 首条记录追加成功后的单调时钟起点，仅用于观测。
    pending_observed_since: Option<Instant>,
    /// 备节点是否可接收新的变更。
    standby_available: bool,
    /// I/O 或序号错误后永久停止。
    halted: bool,
}
impl<A: DurableAppender> DurableBatcher<A> {
    /// 在不更换写入器和水位的前提下擦除其具体类型。
    pub fn into_boxed(self) -> DurableBatcher<BoxedDurableAppender>
    where
        A: Send + 'static,
    {
        DurableBatcher {
            appender: BoxedDurableAppender(Box::new(self.appender)),
            metrics: self.metrics,
            mode: self.mode,
            expected_epoch: self.expected_epoch,
            watermark: self.watermark,
            last_appended: self.last_appended,
            pending_first: self.pending_first,
            pending_identity: self.pending_identity,
            pending_count: self.pending_count,
            pending_since: self.pending_since,
            pending_observed_since: self.pending_observed_since,
            standby_available: self.standby_available,
            halted: self.halted,
        }
    }
    /// 校验完整持久性配置后创建磁盘同步批处理器；备节点认证仍由调用方负责。
    pub fn new_with_config(
        appender: A,
        config: DurabilityConfig,
        expected_epoch: u64,
    ) -> io::Result<Self> {
        config
            .validate()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "无效持久性配置"))?;
        let mode = config.mode;
        let replication = config.replication;
        match mode {
            DurabilityMode::MemoryOnly => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "内存模式不提供持久确认",
                ));
            }
            DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            } if max_batch == 0 || max_delay_micros == 0 => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "无效批次配置"));
            }
            _ => {}
        }
        Ok(Self {
            appender,
            metrics: std::sync::Arc::new(crate::DurabilityMetrics::default()),
            mode,
            expected_epoch,
            watermark: CommitWatermark::new(replication, 0),
            last_appended: 0,
            pending_first: None,
            pending_identity: None,
            pending_count: 0,
            pending_since: None,
            pending_observed_since: None,
            standby_available: replication == ReplicationMode::LocalDurable,
            halted: false,
        })
    }
    /// 按外部备节点健康状态开放或关闭新追加，不能自动改变持久策略。
    pub fn set_standby_available(&mut self, available: bool) {
        self.standby_available = available;
    }
    /// 获取共享的持久操作观测句柄。
    pub fn metrics(&self) -> std::sync::Arc<crate::DurabilityMetrics> {
        std::sync::Arc::clone(&self.metrics)
    }
    /// 返回实际本地同步成功的连续前缀。
    pub const fn local_durable(&self) -> u64 {
        self.watermark.local
    }
    /// 返回当前策略允许对外提交的前缀。
    pub const fn committed(&self) -> u64 {
        self.watermark.committed()
    }
    /// 返回写入器是否要求每批提交后登记摘要。
    pub fn digest_checkpoint_required(&self) -> bool {
        self.appender.digest_checkpoint_required()
    }
    /// 只在备节点持久策略的完整提交顺序点登记摘要；失败后停止后续写入。
    pub fn checkpoint_digest(
        &mut self,
        seq: u64,
        state: [u8; 32],
        events: [u8; 32],
    ) -> io::Result<()> {
        if self.halted
            || self.watermark.mode != ReplicationMode::StandbyDurable
            || self.pending_count != 0
            || seq == 0
            || seq != self.committed()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "摘要不属于当前完整提交前缀",
            ));
        }
        let result = self.appender.checkpoint_digest(seq, state, events);
        if result.is_err() {
            self.halted = true;
        }
        result
    }
    /// 在提交前缀内登记已同步快照；非 `InvalidInput` 的清单错误会停止持久写入。
    pub fn commit_snapshot(&mut self, snapshot_id: u64, snapshot_seq: u64) -> io::Result<()> {
        if self.halted || self.pending_count != 0 || snapshot_seq > self.committed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "快照不属于已提交前缀",
            ));
        }
        let result = self.appender.commit_snapshot(snapshot_id, snapshot_seq);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::InvalidInput)
        {
            self.halted = true;
        }
        result
    }
    /// 检查批次是否处于本地持久模式、没有待同步记录且写入器未停用。
    pub fn supports_local_migration(&self) -> bool {
        self.watermark.mode == ReplicationMode::LocalDurable
            && self.pending_count == 0
            && !self.halted
    }
    /// 通过分片唯一清单写者提交迁移快照；未知结果要求重启恢复。
    pub fn commit_migration_snapshot(&mut self, id: u64, seq: u64) -> io::Result<()> {
        if !self.supports_local_migration() || seq != self.committed() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "迁移要求静止的本地持久前缀",
            ));
        }
        let result = self.appender.commit_migration_snapshot(id, seq);
        if result
            .as_ref()
            .is_err_and(|e| e.kind() != io::ErrorKind::InvalidInput)
        {
            self.halted = true;
        }
        result
    }
    /// 只读查看底层写入器与持久元数据状态。
    pub fn appender(&self) -> &A {
        &self.appender
    }
    /// 顺序追加记录并按配置同步；`now` 只用于判断批次时限。
    pub fn append_at(&mut self, record: &WalRecord, now: Instant) -> io::Result<()> {
        if self.halted {
            return Err(io::Error::other("持久写入器已停止"));
        }
        if self.watermark.mode == ReplicationMode::StandbyDurable
            && (!self.standby_available || self.appender.replication_ready() == Some(false))
        {
            return Err(io::Error::other("备节点不可用"));
        }
        if record.epoch != self.expected_epoch {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "主节点纪元不匹配",
            ));
        }
        if self.last_appended.checked_add(1) != Some(record.shard_seq) {
            self.halted = true;
            return Err(io::Error::new(io::ErrorKind::InvalidData, "写入序号不连续"));
        }
        self.flush_due(now)?;
        let rotate = match self.appender.would_rotate(record) {
            Ok(value) => value,
            Err(error) => {
                self.halted = true;
                return Err(error);
            }
        };
        if rotate && self.pending_count != 0 {
            self.flush()?;
        }
        if self
            .pending_identity
            .is_some_and(|identity| identity != (record.cluster_id, record.shard_id))
        {
            self.halted = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "批次分片身份不一致",
            ));
        }
        let next_count = self
            .pending_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("批次计数耗尽"))?;
        let started = Instant::now();
        let append_result = self.appender.append(record);
        self.metrics.observe_append(started.elapsed());
        if let Err(error) = append_result {
            self.halted = true;
            return Err(error);
        }
        if self.pending_count == 0 {
            self.pending_first = Some(record.shard_seq);
            self.pending_identity = Some((record.cluster_id, record.shard_id));
            self.pending_since = Some(now);
            self.pending_observed_since = Some(Instant::now());
        }
        self.pending_count = next_count;
        self.last_appended = record.shard_seq;
        if self.mode == DurabilityMode::FsyncEach
            || matches!(self.mode, DurabilityMode::GroupCommit { max_batch, .. } if self.pending_count >= max_batch)
        {
            self.flush()?;
        }
        Ok(())
    }
    /// 到达批次时限时同步；调用方须安排定期调用。
    pub fn flush_due(&mut self, now: Instant) -> io::Result<()> {
        if self.halted {
            return Err(io::Error::other("持久写入器已停止"));
        }
        if let (
            Some(since),
            DurabilityMode::GroupCommit {
                max_delay_micros, ..
            },
        ) = (self.pending_since, self.mode)
        {
            let elapsed = now
                .checked_duration_since(since)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "批次时钟回退"))?;
            if elapsed >= Duration::from_micros(max_delay_micros) {
                self.flush()?;
            }
        }
        Ok(())
    }
    /// 显式同步当前非空批次；只有同步成功才能推进本地前缀。
    pub fn flush(&mut self) -> io::Result<()> {
        if self.halted {
            return Err(io::Error::other("持久写入器已停止"));
        }
        let Some(first) = self.pending_first else {
            return Ok(());
        };
        if matches!(self.mode, DurabilityMode::GroupCommit { .. })
            && let Some(since) = self.pending_observed_since
        {
            self.metrics
                .observe_batch(self.pending_count, since.elapsed());
        }
        let started = Instant::now();
        let sync_result = self.appender.sync();
        self.metrics.observe_sync(started.elapsed());
        if let Err(error) = sync_result {
            self.halted = true;
            return Err(error);
        }
        if self
            .watermark
            .advance_local(first, self.last_appended)
            .is_err()
        {
            self.halted = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "本地持久前缀不连续",
            ));
        }
        if let Some(ack) = self.appender.take_replica_ack()
            && (self.watermark.mode != ReplicationMode::StandbyDurable
                || self.pending_identity != Some((ack.cluster_id, ack.shard_id))
                || ack.epoch != self.expected_epoch
                || ack.first_seq != first
                || ack.through_seq != self.last_appended
                || self
                    .watermark
                    .advance_standby(ack.first_seq, ack.through_seq)
                    .is_err())
        {
            self.halted = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "备节点确认身份或连续前缀无效",
            ));
        }
        self.pending_first = None;
        self.pending_identity = None;
        self.pending_count = 0;
        self.pending_since = None;
        self.pending_observed_since = None;
        Ok(())
    }
    /// 在可信复制适配器验证持久确认后推进备节点连续前缀。
    pub fn ack_standby_epoch(
        &mut self,
        epoch: u64,
        first: u64,
        last: u64,
    ) -> Result<(), DurabilityError> {
        if self.halted || !self.standby_available || epoch != self.expected_epoch {
            return Err(DurabilityError);
        }
        self.watermark.advance_standby(first, last)
    }
}

/// 备节点完成实际同步后返回的连续持久前缀确认。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaDurableAck {
    /// 确认所属的集群身份。
    pub cluster_id: u16,
    /// 确认所属的分片身份。
    pub shard_id: u16,
    /// 当前复制会话绑定的主节点纪元。
    pub epoch: u64,
    /// 本次确认的第一个连续序号。
    pub first_seq: u64,
    /// 本次确认的最后一个连续序号。
    pub through_seq: u64,
}

/// 备节点本地 WAL 写入与连续持久确认；不执行或发布业务事件。
pub struct StandbyReplica<A> {
    /// 备节点独占的本地持久批处理器。
    batcher: DurableBatcher<A>,
    /// 复制记录必须匹配的集群身份。
    cluster: u16,
    /// 复制记录必须匹配的分片身份。
    shard: u16,
    /// 复制会话固定的主节点纪元。
    epoch: u64,
    /// 单次允许的最大记录数量。
    max_batch: usize,
    /// 发生持久错误后永久停止确认。
    halted: bool,
}

impl<A: DurableAppender> StandbyReplica<A> {
    /// 使用已验证本地持久边界建立备节点写入器。
    pub fn new(
        batcher: DurableBatcher<A>,
        cluster: u16,
        shard: u16,
        epoch: u64,
        max_batch: usize,
    ) -> io::Result<Self> {
        if epoch == 0
            || max_batch == 0
            || batcher.expected_epoch != epoch
            || batcher.halted
            || batcher.pending_count != 0
            || batcher.watermark.mode != ReplicationMode::LocalDurable
            || batcher.last_appended != batcher.watermark.local
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "备节点持久边界或配置无效",
            ));
        }
        Ok(Self {
            batcher,
            cluster,
            shard,
            epoch,
            max_batch,
            halted: false,
        })
    }

    /// 验证完整连续批次，实际同步后返回该前缀的确认。
    pub fn append_batch(
        &mut self,
        records: &[WalRecord],
        now: Instant,
    ) -> io::Result<ReplicaDurableAck> {
        if self.halted {
            return Err(io::Error::other("备节点持久写入已停止"));
        }
        if records.is_empty() || records.len() > self.max_batch {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制批次大小无效",
            ));
        }
        let first = self
            .batcher
            .committed()
            .checked_add(1)
            .ok_or_else(|| io::Error::other("备节点序号耗尽"))?;
        let mut expected = first;
        for (index, record) in records.iter().enumerate() {
            if record.cluster_id != self.cluster
                || record.shard_id != self.shard
                || record.epoch != self.epoch
                || record.shard_seq != expected
                || record.encode().is_err()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "复制记录身份、格式或序号无效",
                ));
            }
            if index + 1 < records.len() {
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("备节点序号耗尽"))?;
            }
        }
        let result = (|| {
            for record in records {
                self.batcher.append_at(record, now)?;
            }
            self.batcher.flush()?;
            if self.batcher.committed() != expected {
                return Err(io::Error::other("备节点持久前缀不连续"));
            }
            Ok(ReplicaDurableAck {
                cluster_id: self.cluster,
                shard_id: self.shard,
                epoch: self.epoch,
                first_seq: first,
                through_seq: expected,
            })
        })();
        if result.is_err() {
            self.halted = true;
        }
        result
    }
}

/// 串行化备节点写入与持久纪元栅栏；远端写权和复制意图仍须由调用方核验。
pub struct FencedStandbyReplica<A> {
    /// 与栅栏共享独占顺序的本地备节点。
    replica: StandbyReplica<A>,
    /// 可恢复的已安装纪元。
    epochs: crate::ReplicaEpochStore,
    /// 当前进程是否已重新核验权威。
    active: bool,
    /// 当前进程是否已经安装并验证接任栅栏。
    fenced: bool,
    /// 栅栏写入失败后的不确定状态只能通过重新恢复处理。
    halted: bool,
}

impl<A: DurableAppender> FencedStandbyReplica<A> {
    /// 暂停复制并保留已安装栅栏，供调用方重新核验恢复。
    pub fn suspend(&mut self) {
        self.active = false;
    }
    /// 核对栅栏身份和已恢复的真实本地前缀；每次创建均从未激活状态开始。
    pub fn new(replica: StandbyReplica<A>, epochs: crate::ReplicaEpochStore) -> io::Result<Self> {
        let state = epochs.state();
        if replica.cluster != state.cluster_id
            || replica.shard != state.shard_id
            || replica.epoch != state.epoch
            || replica.batcher.committed() < state.durable_seq
            || replica.halted
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "备节点与持久栅栏不一致",
            ));
        }
        Ok(Self {
            replica,
            epochs,
            active: false,
            fenced: false,
            halted: false,
        })
    }

    /// 验证当前权威并串行安装纪元，返回栅栏时真实本地持久水位；验证失败时暂停复制。
    pub fn activate(
        &mut self,
        epoch: u64,
        verify: impl FnOnce(u16, u16, u64) -> io::Result<()>,
    ) -> io::Result<u64> {
        self.active = false;
        self.fenced = false;
        let state = self.epochs.state();
        if epoch < state.epoch || self.replica.halted || self.halted {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "备节点纪元回退或已停止",
            ));
        }
        verify(state.cluster_id, state.shard_id, epoch)?;
        if epoch > state.epoch {
            if let Err(error) =
                self.epochs
                    .advance(state.epoch, epoch, self.replica.batcher.committed())
            {
                self.halted = true;
                return Err(error);
            }
            self.replica.epoch = epoch;
            self.replica.batcher.expected_epoch = epoch;
        }
        self.fenced = true;
        Ok(self.replica.batcher.committed())
    }

    /// 在调用方完成权威前缀核对后开放复制；检查时须重新验证远端写权。
    pub fn confirm_reconciled(
        &mut self,
        durable_seq: u64,
        verify: impl FnOnce(u16, u16, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        self.active = false;
        if !self.fenced
            || self.halted
            || self.replica.halted
            || self.replica.batcher.committed() != durable_seq
        {
            return Err(io::Error::other("备节点栅栏或协调水位无效"));
        }
        let state = self.epochs.state();
        verify(state.cluster_id, state.shard_id, state.epoch)?;
        self.active = true;
        Ok(())
    }

    /// 在调用方验证复制意图后写入；栅栏和批次均通过独占借用排序。
    pub fn append_batch(
        &mut self,
        records: &[WalRecord],
        now: Instant,
    ) -> io::Result<ReplicaDurableAck> {
        if !self.active {
            return Err(io::Error::other("备节点尚未通过权威校验"));
        }
        self.replica.append_batch(records, now)
    }
}

/// 同步 segment 并登记持久 manifest 后才能报告同步成功的写入器。
pub struct RegisteredAppender<S> {
    /// 单写者 WAL segment。
    writer: SegmentWriter,
    /// 独占的单写者 manifest 管理器。
    manifest: ManifestManager<S>,
}

impl RegisteredAppender<crate::FileManifestStorage> {
    /// 持有严格清单锁校验并修复尾部，再重开活动段；调用方须独占目录并先完成业务重放。
    ///
    /// 此入口仅验证持久前缀，不验证业务镜像或重放结果；仅适用于本地持久策略。
    pub fn recover_local(
        wal: impl AsRef<std::path::Path>,
        max_size: u64,
        manifest: ManifestManager<crate::FileManifestStorage>,
        snapshot_seq: u64,
        cluster: u16,
        shard: u16,
    ) -> io::Result<Self> {
        crate::repair_local_wal_with_manifest(&wal, &manifest, snapshot_seq, cluster, shard)?;
        let state = manifest.state();
        if state.generation < 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "清单未初始化"));
        }
        let writer = SegmentWriter::open_repaired(
            &wal,
            max_size,
            state.active_segment,
            cluster,
            shard,
            state.local_durable_seq,
        )?;
        Ok(Self { writer, manifest })
    }
}

impl FencedStandbyReplica<RegisteredAppender<crate::FileManifestStorage>> {
    /// 在栅栏顺序点读取清单确认的备节点前缀；调用方须独占目录并验证快照。
    pub fn fenced_records(&self, snapshot_seq: u64) -> io::Result<Vec<WalRecord>> {
        let batcher = &self.replica.batcher;
        let appender = &batcher.appender;
        if !self.fenced
            || self.active
            || self.halted
            || self.replica.halted
            || batcher.halted
            || batcher.pending_count != 0
            || batcher.committed() != appender.manifest.state().local_durable_seq
            || batcher.last_appended != batcher.committed()
        {
            return Err(io::Error::other("备节点尚未处于可核对的持久栅栏"));
        }
        crate::recovery::recover_local_state(
            appender.writer.directory(),
            appender.manifest.state(),
            snapshot_seq,
            self.replica.cluster,
            self.replica.shard,
        )
    }
}

impl DurableBatcher<RegisteredAppender<crate::FileManifestStorage>> {
    /// 以严格清单确认的本地前缀初始化备节点持久续写。
    ///
    /// 可信工厂必须保留传入的本地写入器，并独立完成同一基线的权威日志、
    /// 备节点、租约和业务重放协调；健康标志本身不构成这些事实的证明。
    /// 该低层入口不负责快照校验、路由准入或修复超出权威前缀的本地尾部。
    pub fn resume_replicated_with<A, F>(
        appender: RegisteredAppender<crate::FileManifestStorage>,
        config: DurabilityConfig,
        expected_epoch: u64,
        factory: F,
    ) -> io::Result<DurableBatcher<A>>
    where
        A: DurableAppender,
        F: FnOnce(RegisteredAppender<crate::FileManifestStorage>, u64) -> io::Result<A>,
    {
        if !appender.manifest.recovery_checked()
            || config.replication != ReplicationMode::StandbyDurable
            || expected_epoch == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制续写需要严格清单和备节点持久策略",
            ));
        }
        config
            .validate()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "无效持久性配置"))?;
        let baseline = appender.manifest.state().local_durable_seq;
        let appender = factory(appender, baseline)?;
        if appender.replication_ready() != Some(true) {
            return Err(io::Error::other("复制链尚未就绪"));
        }
        let mut batcher = DurableBatcher::new_with_config(appender, config, expected_epoch)?;
        batcher.watermark = CommitWatermark::new(ReplicationMode::StandbyDurable, baseline);
        batcher.last_appended = baseline;
        batcher.standby_available = true;
        Ok(batcher)
    }

    /// 仅以严格验证和修复后的本地持久水位初始化续写批处理器。
    pub fn resume_local(
        appender: RegisteredAppender<crate::FileManifestStorage>,
        config: DurabilityConfig,
        expected_epoch: u64,
    ) -> io::Result<Self> {
        if !appender.manifest.recovery_checked()
            || config.replication != ReplicationMode::LocalDurable
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "续写需要严格清单和本地持久策略",
            ));
        }
        let baseline = appender.manifest.state().local_durable_seq;
        let mut batcher = Self::new_with_config(appender, config, expected_epoch)?;
        batcher.watermark = CommitWatermark::new(ReplicationMode::LocalDurable, baseline);
        batcher.last_appended = baseline;
        Ok(batcher)
    }
}
impl<S: ManifestStorage> RegisteredAppender<S> {
    /// 只从空 WAL 和默认清单启动并持久初始化双槽；已有 WAL 使用恢复入口。
    pub fn new(writer: SegmentWriter, mut manifest: ManifestManager<S>) -> io::Result<Self> {
        if !writer.is_pristine() || manifest.state() != &ManifestState::default() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "非空状态需要恢复流程",
            ));
        }
        manifest.submit(ManifestIntent::Initialize)?;
        manifest.submit(ManifestIntent::RecoveryAnchor)?;
        Ok(Self { writer, manifest })
    }
    /// 返回已完成持久化的 manifest 前态。
    pub fn manifest_state(&self) -> &ManifestState {
        self.manifest.state()
    }
}
impl<S: ManifestStorage> DurableAppender for RegisteredAppender<S> {
    fn is_migration_snapshot(&self, id: u64, seq: u64) -> bool {
        let state = self.manifest.state();
        state.latest_snapshot == Some((id, seq)) && state.previous_snapshot.is_none()
    }
    fn commit_migration_snapshot(&mut self, id: u64, seq: u64) -> io::Result<()> {
        self.manifest
            .submit(ManifestIntent::MigrationSnapshotCommitted {
                snapshot_id: id,
                snapshot_seq: seq,
            })
    }
    fn commit_snapshot(&mut self, snapshot_id: u64, snapshot_seq: u64) -> io::Result<()> {
        if self.manifest.state().generation < 2
            || snapshot_seq > self.manifest.state().local_durable_seq
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "快照不属于本地持久前缀",
            ));
        }
        self.manifest.submit(ManifestIntent::SnapshotCommitted {
            snapshot_id,
            snapshot_seq,
        })
    }
    /// 在同步前识别将发生的文件轮转。
    fn would_rotate(&self, record: &WalRecord) -> io::Result<bool> {
        self.writer.durable_would_rotate(record)
    }
    /// 将记录追加到活动 WAL 文件。
    fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        self.writer.append(record)
    }
    /// 先同步 WAL 文件及目录，再持久登记活动 segment。
    fn sync(&mut self) -> io::Result<()> {
        self.writer.durable_sync()?;
        let first_seq = self
            .writer
            .active_first()
            .ok_or_else(|| io::Error::other("活动 segment 不存在"))?;
        let through_seq = self
            .writer
            .last_seq()
            .ok_or_else(|| io::Error::other("没有可登记的记录"))?;
        self.manifest.submit(ManifestIntent::LocalDurableAdvanced {
            through_seq,
            first_seq,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod fenced_standby_tests {
    use super::*;

    struct EmptyAppender;
    impl DurableAppender for EmptyAppender {
        fn would_rotate(&self, _: &WalRecord) -> io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn unknown_fence_sync_forbids_reactivation_of_old_epoch() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("tide-epoch-sync-{}-{stamp}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let mut epochs = crate::ReplicaEpochStore::create(root.join("epoch"), 1, 2, 3, 0).unwrap();
        epochs.fail_next_sync();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: true,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let batcher = DurableBatcher::new_with_config(EmptyAppender, config, 3).unwrap();
        let replica = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
        let mut fenced = FencedStandbyReplica::new(replica, epochs).unwrap();
        fenced.activate(3, |_, _, _| Ok(())).unwrap();
        assert!(fenced.activate(4, |_, _, _| Ok(())).is_err());
        assert!(fenced.activate(3, |_, _, _| Ok(())).is_err());
        assert!(fenced.activate(4, |_, _, _| Ok(())).is_err());
        drop(fenced);
        let store = crate::ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
        assert_eq!(store.state().epoch, 4);
        drop(store);
        let target = root.canonicalize().unwrap();
        assert_eq!(
            target.parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tide-epoch-sync-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }
}
