//! 从持久化前缀恢复命令、快照及行情重放状态。
use matching_domain::{CommandEnvelope, decode_wal_command};
use std::{io, path::Path};

/// 外围已验证的创世重放中间检查点。
#[derive(Clone, Copy)]
pub struct ReplayCheckpoint {
    /// 检查点覆盖的分片序号。
    pub shard_seq: u64,
    /// 对应的状态摘要。
    pub state_digest: [u8; 32],
    /// 创世至检查点的有序事件摘要。
    pub event_hash: [u8; 32],
    /// 状态编码容量上限。
    pub max_state_bytes: usize,
}

/// 由可信外围协调接任的权威前缀、业务重放和认证复制。
///
/// 实现须验证快照、租约与备节点，且完成重放验证后才能恢复复制；
/// 返回的写入器必须持有传入的本地 WAL 和清单写者。
pub trait ReplicatedRecoveryCoordinator {
    /// 保留本地持久写者的认证复制写入器。
    type Appender: matching_persistence::DurableAppender + Send + 'static;
    /// 提供已认证的中间检查点；为空时由协调器自行核验最终重放结果。
    fn replay_checkpoint(&self) -> Option<ReplayCheckpoint> {
        None
    }
    /// 在独占目录及严格清单锁下取得已核验的完整业务前缀。
    fn recover(
        &mut self,
        wal: &Path,
        snapshot_dir: &Path,
        manager: &mut matching_persistence::ManifestManager<
            matching_persistence::FileManifestStorage,
        >,
        trusted: &crate::SnapshotRestoreConfig,
        policies: &[(
            matching_domain::MarketId,
            matching_orderbook::QueuePriorityPolicy,
        )],
    ) -> io::Result<SnapshotRecoveryRecords>;
    /// 在修复 WAL 和开放备节点前比对重放镜像及事件边界。
    fn verify_replay(&mut self, replay: &SnapshotReplayResult) -> io::Result<()>;
    /// 重放核验后协调本地 WAL；默认只接受与权威水位完全一致的本地前缀。
    fn reconcile_local(
        &mut self,
        _wal: &Path,
        manager: &mut matching_persistence::ManifestManager<
            matching_persistence::FileManifestStorage,
        >,
        _trusted: &crate::SnapshotRestoreConfig,
        baseline: u64,
    ) -> io::Result<()> {
        if manager.state().local_durable_seq != baseline {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "本地和权威水位不一致",
            ));
        }
        Ok(())
    }
    /// 确认备节点后，构造拥有本地写入器的复制链。
    fn finish(
        self,
        local: matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
        baseline: u64,
        replay: &SnapshotReplayResult,
    ) -> io::Result<Self::Appender>;
}

/// 持有快照候选与其后的连续正式命令；尚未执行重放。
pub struct SnapshotRecoveryRecords {
    /// 已校验的快照或可信初始候选及权威提交水位。
    snapshot: crate::SnapshotRecoveryCandidate,
    /// 严格按分片序号排列的后续正式命令。
    commands: Vec<RecoveredCommand>,
}

/// 重放完成的进程内镜像及逐命令结果；不代表 WAL 已续写。
pub struct SnapshotReplayResult {
    /// 完整应用提交前缀后的独立镜像。
    pub image: crate::SnapshotImage,
    /// 重放生成的结果及按原序排列的事件。
    pub executions: Vec<crate::FormalExecution>,
    /// 后续已提交命令按原序生成的事件摘要。
    pub event_hash: [u8; 32],
    /// 完整创世事件的可续写摘要状态；不作为快照持久载荷。
    pub(crate) event_hasher: crate::EventHasher,
}

/// 由清单验证的快照和后续已提交 WAL 重建的单市场行情片段。
pub struct CommittedMarketDataReplay {
    /// 快照位置的逐订单视图。
    initial: crate::MboBuilder,
    /// 快照后按市场原序重建的源事件。
    events: Vec<crate::SourceEvent>,
    /// 消费完整提交后缀后的逐订单视图。
    final_builder: crate::MboBuilder,
}
impl CommittedMarketDataReplay {
    /// 返回已验证快照中的初始视图。
    pub fn initial_builder(&self) -> &crate::MboBuilder {
        &self.initial
    }
    /// 返回已提交后缀生成的源事件。
    pub fn events(&self) -> &[crate::SourceEvent] {
        &self.events
    }
    /// 返回消费源事件后的视图。
    pub fn final_builder(&self) -> &crate::MboBuilder {
        &self.final_builder
    }
}

impl SnapshotRecoveryRecords {
    /// 借用已验证的快照或初始候选。
    pub fn snapshot(&self) -> &crate::SnapshotRecoveryCandidate {
        &self.snapshot
    }
    /// 借用按序号排列的后续命令。
    pub fn commands(&self) -> &[RecoveredCommand] {
        &self.commands
    }
    /// 在隔离候选中重放全部已提交命令；失败不返回部分状态。
    pub fn replay(self) -> io::Result<SnapshotReplayResult> {
        crate::worker::replay_snapshot_records(self.snapshot, self.commands)
    }
    /// 仅供完成外围权威核验的复制接任路径使用新写者纪元。
    pub(crate) fn replay_replicated(self, epoch: u64) -> io::Result<SnapshotReplayResult> {
        crate::worker::replay_replicated_snapshot_records(self.snapshot, self.commands, epoch, None)
    }
    /// 从创世按序重放，在外围验证的中间检查点核对状态及事件后继续后缀。
    ///
    /// 调用方负责验证检查点的权威来源和写权，此入口不开放业务。
    pub fn replay_with_checkpoint(
        self,
        epoch: u64,
        checkpoint: ReplayCheckpoint,
    ) -> io::Result<SnapshotReplayResult> {
        if self.snapshot.image.snapshot_seq() != 0
            || checkpoint.shard_seq == 0
            || checkpoint.shard_seq > self.snapshot.committed_through
            || checkpoint.max_state_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "创世检查点边界无效",
            ));
        }
        crate::worker::replay_replicated_snapshot_records(
            self.snapshot,
            self.commands,
            epoch,
            Some(checkpoint),
        )
    }
    /// 从可信快照及完整已提交 WAL 后缀重建并核验单市场行情。
    ///
    /// 旧快照未包含行情视图时拒绝；结果不包含快照以前的源历史。
    pub fn replay_market_data(
        self,
        market: matching_domain::MarketId,
    ) -> io::Result<CommittedMarketDataReplay> {
        let initial = self
            .snapshot
            .image
            .market_data_builder(market)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "快照缺少市场行情视图"))?;
        let replayed = self.replay()?;
        let mut builder = initial.clone();
        let mut events = Vec::new();
        for execution in &replayed.executions {
            if let Some(batch) = execution.market_data_source.get(&market) {
                events
                    .try_reserve(batch.len())
                    .map_err(|_| io::Error::other("行情重放容量不足"))?;
                for event in batch {
                    builder.apply(event).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "行情源重放失败")
                    })?;
                    events.push(event.clone());
                }
            }
        }
        let expected = replayed.image.market_data_builder(market).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "重放镜像缺少市场行情视图")
        })?;
        if builder != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "行情重放状态不一致",
            ));
        }
        Ok(CommittedMarketDataReplay {
            initial,
            events,
            final_builder: builder,
        })
    }
}

/// 从完整创世 WAL 重建已提交行情源；任何已回收的前缀均拒绝。
///
/// 调用方须独占 WAL 目录，并提供与最初启动完全一致的可信配置和市场策略。
/// 已登记过快照的分片被拒绝，避免将旧版本快照升级后的源历史错误重建为创世历史。
/// 此入口不保存消费者游标，也不能替代独立持久行情日志。
pub fn recover_market_data_from_genesis(
    wal: impl AsRef<Path>,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
    policies: &[(
        matching_domain::MarketId,
        matching_orderbook::QueuePriorityPolicy,
    )],
) -> io::Result<SnapshotReplayResult> {
    if config.place.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "缺少行情所需的下单配置",
        ));
    }
    if manager.state().latest_snapshot.is_some() || manager.state().previous_snapshot.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "已登记快照，无法证明行情源从创世连续生成",
        ));
    }
    if let Some(lease) = config
        .place
        .as_ref()
        .and_then(|place| place.primary_lease.as_ref())
    {
        lease
            .verify(config.cluster, config.shard, config.primary_epoch)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "可信主节点租约身份或纪元不匹配",
                )
            })?;
    }
    let records = matching_persistence::recover_local_with_manifest(
        wal,
        manager,
        0,
        config.cluster.get(),
        config.shard.get(),
    )?;
    let candidate = crate::worker::empty_recovery_candidate(
        config,
        policies,
        manager.state().local_durable_seq,
    )?;
    crate::worker::replay_snapshot_records(candidate, decode_records(records)?)
}

/// 持有同一严格清单锁选择快照并读取后续 WAL，不执行命令或更改文件。
///
/// 调用方须独占 WAL 和快照目录；仅支持可信空市场基线的无快照恢复。
pub fn recover_snapshot_records(
    wal: impl AsRef<Path>,
    snapshot_dir: impl AsRef<Path>,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
) -> io::Result<Option<SnapshotRecoveryRecords>> {
    recover_snapshot_records_with_genesis(wal, snapshot_dir, manager, config, &[])
}

/// 使用可信市场初始策略构造无快照基线，并在同一清单锁下读取 WAL。
///
/// 调用方必须独占 WAL 和快照目录，并保证策略与市场最初启动时一致。
pub fn recover_snapshot_records_with_genesis(
    wal: impl AsRef<Path>,
    snapshot_dir: impl AsRef<Path>,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
    policies: &[(
        matching_domain::MarketId,
        matching_orderbook::QueuePriorityPolicy,
    )],
) -> io::Result<Option<SnapshotRecoveryRecords>> {
    recover_snapshot_records_inner(
        wal.as_ref(),
        snapshot_dir.as_ref(),
        manager,
        config,
        policies,
        false,
    )
}

/// 在本地或复制接任边界下选择快照并读取严格清单中的完整后缀。
fn recover_snapshot_records_inner(
    wal: &Path,
    snapshot_dir: &Path,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
    policies: &[(
        matching_domain::MarketId,
        matching_orderbook::QueuePriorityPolicy,
    )],
    replicated: bool,
) -> io::Result<Option<SnapshotRecoveryRecords>> {
    if let Some(lease) = config
        .place
        .as_ref()
        .and_then(|place| place.primary_lease.as_ref())
    {
        lease
            .verify(config.cluster, config.shard, config.primary_epoch)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "可信主节点租约身份或纪元不匹配",
                )
            })?;
    }
    let candidate = if replicated {
        crate::SnapshotImage::read_replicated_recovery_candidate(snapshot_dir, manager, config)?
    } else {
        crate::SnapshotImage::read_recovery_candidate(snapshot_dir, manager, config)?
    };
    let snapshot = match candidate {
        Some(snapshot) => snapshot,
        None if manager.state().latest_snapshot.is_none()
            && manager.state().previous_snapshot.is_none() =>
        {
            crate::worker::empty_recovery_candidate(
                config,
                policies,
                manager.state().local_durable_seq,
            )?
        }
        None => return Ok(None),
    };
    let records = matching_persistence::recover_local_with_manifest(
        wal,
        manager,
        snapshot.image.snapshot_seq(),
        config.cluster.get(),
        config.shard.get(),
    )?;
    let commands = decode_records(records)?;
    Ok(Some(SnapshotRecoveryRecords { snapshot, commands }))
}

/// 在本地严格清单锁下，只选择经调用方权威逐条核验的提交前缀。
///
/// 调用方须独占 WAL 和快照目录、验证快照以及 `committed` 的权威来源；
/// `verify` 必须比较完整 WAL 内容和有效写权。该入口不修复 WAL、不启用分片。
pub fn recover_verified_snapshot_records_with_genesis(
    wal: impl AsRef<Path>,
    snapshot_dir: impl AsRef<Path>,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
    policies: &[(
        matching_domain::MarketId,
        matching_orderbook::QueuePriorityPolicy,
    )],
    committed: u64,
    mut verify: impl FnMut(&matching_persistence::WalRecord) -> io::Result<()>,
) -> io::Result<Option<SnapshotRecoveryRecords>> {
    let Some(mut result) = recover_snapshot_records_inner(
        wal.as_ref(),
        snapshot_dir.as_ref(),
        manager,
        config,
        policies,
        true,
    )?
    else {
        return Ok(None);
    };
    let snapshot_seq = result.snapshot.image.snapshot_seq();
    if committed < snapshot_seq || committed > result.snapshot.committed_through {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "权威提交水位超出本地可恢复范围",
        ));
    }
    let records = matching_persistence::recover_local_with_manifest(
        &wal,
        manager,
        snapshot_seq,
        config.cluster.get(),
        config.shard.get(),
    )?;
    for record in records
        .iter()
        .take_while(|record| record.shard_seq <= committed)
    {
        verify(record)?;
    }
    result
        .commands
        .retain(|command| command.shard_seq <= committed);
    if result
        .commands
        .last()
        .map_or(snapshot_seq, |command| command.shard_seq)
        != committed
    {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "权威前缀不连续"));
    }
    result.snapshot.committed_through = committed;
    Ok(Some(result))
}

/// 从完整创世 WAL 选择经调用方逐条核验的提交前缀，忽略已登记快照。
///
/// 调用方须独占目录、核验水位和完整记录及有效写权，提供最初的可信市场策略；
/// WAL 前缀已回收时严格拒绝。该入口不证明摘要或允许分片启动。
pub fn recover_verified_genesis_records(
    wal: impl AsRef<Path>,
    manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    config: &crate::SnapshotRestoreConfig,
    policies: &[(
        matching_domain::MarketId,
        matching_orderbook::QueuePriorityPolicy,
    )],
    committed: u64,
    mut verify: impl FnMut(&matching_persistence::WalRecord) -> io::Result<()>,
) -> io::Result<SnapshotRecoveryRecords> {
    if committed > manager.state().local_durable_seq {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "权威水位超过本地持久水位",
        ));
    }
    let records = matching_persistence::recover_local_with_manifest(
        wal,
        manager,
        0,
        config.cluster.get(),
        config.shard.get(),
    )?;
    let mut prefix = Vec::new();
    for record in records
        .into_iter()
        .take_while(|record| record.shard_seq <= committed)
    {
        verify(&record)?;
        prefix.push(record);
    }
    if prefix.last().map_or(0, |record| record.shard_seq) != committed {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "创世前缀不连续"));
    }
    let snapshot = crate::worker::empty_recovery_candidate(config, policies, committed)?;
    Ok(SnapshotRecoveryRecords {
        snapshot,
        commands: decode_records(prefix)?,
    })
}

/// 核对记录与正式命令身份，并保留顺序及原始元数据。
fn decode_records(
    records: Vec<matching_persistence::WalRecord>,
) -> io::Result<Vec<RecoveredCommand>> {
    records
        .into_iter()
        .map(|record| {
            let command = decode_wal_command(&record.payload)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "WAL 命令载荷无效"))?;
            if command.command_id.0 != record.command_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL 命令身份不一致",
                ));
            }
            Ok(RecoveredCommand {
                shard_seq: record.shard_seq,
                epoch: record.epoch,
                engine_time: record.engine_time,
                command,
            })
        })
        .collect()
}

/// 一条按 WAL 顺序恢复的正式命令及其重放所需元数据。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredCommand {
    /// 分片业务序号。
    pub shard_seq: u64,
    /// 原始主节点纪元。
    pub epoch: u64,
    /// 命令执行时的逻辑时间。
    pub engine_time: u64,
    /// 解码后的完整正式命令。
    pub command: CommandEnvelope,
}

/// 使用严格 manifest 和已验证快照序号恢复正式命令，不执行重放。
///
/// 调用方须独占 WAL 目录并验证快照；该入口仅适用于 LocalDurable。
pub fn recover_local_commands(
    wal: impl AsRef<Path>,
    manifest: impl AsRef<Path>,
    snapshot_seq: u64,
    cluster: u16,
    shard: u16,
) -> io::Result<Vec<RecoveredCommand>> {
    decode_records(matching_persistence::recover_local(
        wal,
        manifest,
        snapshot_seq,
        cluster,
        shard,
    )?)
}
