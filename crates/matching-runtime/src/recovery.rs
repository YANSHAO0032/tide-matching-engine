//! 将持久化前缀转换为已验证身份的正式命令。
use matching_domain::{CommandEnvelope, decode_wal_command};
use std::{io, path::Path};

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
    matching_persistence::recover_local(wal, manifest, snapshot_seq, cluster, shard)?
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
