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
    /// 拒绝内存模式与持久恢复、高可用或生产环境组合。
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

/// 有序追加及真实同步接口；实现者不得在未同步时返回成功。
pub trait DurableAppender {
    /// 判断下一条记录是否需要关闭当前 segment。
    fn would_rotate(&self, record: &WalRecord) -> io::Result<bool>;
    /// 按顺序追加记录。
    fn append(&mut self, record: &WalRecord) -> io::Result<()>;
    /// 同步当前文件以及新目录项。
    fn sync(&mut self) -> io::Result<()>;
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
    fn commit_snapshot(&mut self, snapshot_id: u64, snapshot_seq: u64) -> io::Result<()> {
        self.0.commit_snapshot(snapshot_id, snapshot_seq)
    }
}

/// 负责批量同步与持久前缀推进的单写者。
pub struct DurableBatcher<A> {
    /// 具有真实同步语义的顺序写入器。
    appender: A,
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
    /// 未同步记录数量。
    pending_count: usize,
    /// 批次第一条记录入队时的单调时刻。
    pending_since: Option<Instant>,
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
            mode: self.mode,
            expected_epoch: self.expected_epoch,
            watermark: self.watermark,
            last_appended: self.last_appended,
            pending_first: self.pending_first,
            pending_count: self.pending_count,
            pending_since: self.pending_since,
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
            mode,
            expected_epoch,
            watermark: CommitWatermark::new(replication, 0),
            last_appended: 0,
            pending_first: None,
            pending_count: 0,
            pending_since: None,
            standby_available: replication == ReplicationMode::LocalDurable,
            halted: false,
        })
    }
    /// 按外部备节点健康状态开放或关闭新追加，不能自动改变持久策略。
    pub fn set_standby_available(&mut self, available: bool) {
        self.standby_available = available;
    }
    /// 返回实际本地同步成功的连续前缀。
    pub const fn local_durable(&self) -> u64 {
        self.watermark.local
    }
    /// 返回当前策略允许对外提交的前缀。
    pub const fn committed(&self) -> u64 {
        self.watermark.committed()
    }
    /// 在提交前缀内登记已同步快照；清单写入失败后停止持久写入。
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
    /// 只读查看底层写入器与持久元数据状态。
    pub fn appender(&self) -> &A {
        &self.appender
    }
    /// 顺序追加记录，达到批次上限时同步；`now` 只决定批次时限。
    pub fn append_at(&mut self, record: &WalRecord, now: Instant) -> io::Result<()> {
        if self.halted {
            return Err(io::Error::other("持久写入器已停止"));
        }
        if self.watermark.mode == ReplicationMode::StandbyDurable && !self.standby_available {
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
        let next_count = self
            .pending_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("批次计数耗尽"))?;
        if let Err(error) = self.appender.append(record) {
            self.halted = true;
            return Err(error);
        }
        if self.pending_count == 0 {
            self.pending_first = Some(record.shard_seq);
            self.pending_since = Some(now);
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
        if let Err(error) = self.appender.sync() {
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
        self.pending_first = None;
        self.pending_count = 0;
        self.pending_since = None;
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

impl DurableBatcher<RegisteredAppender<crate::FileManifestStorage>> {
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
