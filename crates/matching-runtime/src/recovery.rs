//! 将持久化前缀转换为已验证身份的正式命令。
use matching_domain::{CommandEnvelope, decode_wal_command};
use std::{io, path::Path};

/// 持有快照候选与其后的连续正式命令；尚未执行重放。
pub struct SnapshotRecoveryRecords {
    /// 已校验的最新或回退快照及权威提交水位。
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
}

impl SnapshotRecoveryRecords {
    /// 借用已验证的快照候选。
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
    let snapshot =
        match crate::SnapshotImage::read_recovery_candidate(snapshot_dir, manager, config)? {
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
