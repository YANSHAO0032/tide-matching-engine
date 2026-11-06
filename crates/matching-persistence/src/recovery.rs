//! 按外部可信提交边界选择可恢复记录。
use crate::{WalRecord, scan_segments};
use std::{io, path::Path};

/// 校验调用方已验证快照的序号属于清单记录且未落在已回收前缀内。
fn valid_snapshot_boundary(state: &crate::ManifestState, seq: u64) -> bool {
    seq >= state.wal_gc_safe_seq
        && (seq == 0
            || state.latest_snapshot.is_some_and(|(_, value)| value == seq)
            || state
                .previous_snapshot
                .is_some_and(|(_, value)| value == seq))
}

/// 在目录独占且提交水位由调用方可靠持久化的前提下，选择快照之后的提交记录。
///
/// 本函数不验证快照或提交水位的来源，不修改文件，也不执行命令。
/// 缺少已提交记录、分片身份错误或任何不可恢复的损坏均返回错误。
pub fn recover_committed(
    directory: impl AsRef<Path>,
    snapshot_seq: u64,
    committed_seq: u64,
    cluster_id: u16,
    shard_id: u16,
) -> io::Result<Vec<WalRecord>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "恢复边界或分片身份不一致");
    if committed_seq < snapshot_seq {
        return Err(invalid());
    }
    let scan = scan_segments(directory)?;
    if scan
        .records
        .iter()
        .any(|r| r.cluster_id != cluster_id || r.shard_id != shard_id)
    {
        return Err(invalid());
    }
    let selected: Vec<_> = scan
        .records
        .into_iter()
        .filter(|r| r.shard_seq > snapshot_seq && r.shard_seq <= committed_seq)
        .collect();
    if committed_seq > snapshot_seq
        && (selected.first().map(|r| r.shard_seq) != snapshot_seq.checked_add(1)
            || selected.last().map(|r| r.shard_seq) != Some(committed_seq))
    {
        return Err(invalid());
    }
    Ok(selected)
}

/// 从严格校验的 manifest 读取本地同步水位，再筛选已提交记录。
///
/// 调用方须独占 WAL 目录并验证快照内容；本入口只适用于 LocalDurable。
pub fn recover_local(
    wal: impl AsRef<Path>,
    manifest: impl AsRef<Path>,
    snapshot_seq: u64,
    cluster_id: u16,
    shard_id: u16,
) -> io::Result<Vec<WalRecord>> {
    let manager = crate::ManifestManager::<crate::FileManifestStorage>::open_recovery(manifest)?;
    recover_local_with_manifest(wal, &manager, snapshot_seq, cluster_id, shard_id)
}

/// 复用调用方独占且严格验证的清单，避免快照读取与 WAL 选择间释放锁。
///
/// 调用方仍须独占 WAL 目录并验证所选快照内容；本入口只适用于 LocalDurable。
pub fn recover_local_with_manifest(
    wal: impl AsRef<Path>,
    manager: &crate::ManifestManager<crate::FileManifestStorage>,
    snapshot_seq: u64,
    cluster_id: u16,
    shard_id: u16,
) -> io::Result<Vec<WalRecord>> {
    if !manager.recovery_checked() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "清单未通过严格恢复检查",
        ));
    }
    let state = manager.state();
    if state.generation == 0
        && std::fs::read_dir(wal.as_ref())?
            .next()
            .transpose()?
            .is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "存在 WAL 但缺少清单",
        ));
    }
    if !state.gc_plan.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "回收计划尚未恢复",
        ));
    }
    if !valid_snapshot_boundary(state, snapshot_seq) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "快照与清单不一致",
        ));
    }
    let records = recover_committed(
        &wal,
        snapshot_seq,
        state.local_durable_seq,
        cluster_id,
        shard_id,
    )?;
    if state.local_durable_seq != 0 {
        let scan = scan_segments(wal)?;
        let active = state
            .active_segment
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "缺少活动段"))?;
        if active > state.local_durable_seq
            || scan
                .segments
                .iter()
                .find(|s| s.first_seq == active)
                .is_none_or(|s| s.last_seq.is_none_or(|last| last < state.local_durable_seq))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "活动段不包含持久水位",
            ));
        }
    }
    Ok(records)
}

/// 在调用方独占 WAL 目录时，持久移除本地提交水位之后的完整或撕裂尾部。
///
/// 先校验全部已提交记录及清单，再逆序删除多余段并同步目录，最后截断活动段并同步文件。
/// 中途 I/O 失败必须停止启动，允许在相同独占条件下重新调用。
pub fn repair_local_wal(
    wal: impl AsRef<Path>,
    manifest: impl AsRef<Path>,
    snapshot_seq: u64,
    cluster_id: u16,
    shard_id: u16,
) -> io::Result<Vec<WalRecord>> {
    let manager = crate::ManifestManager::<crate::FileManifestStorage>::open_recovery(manifest)?;
    repair_local_wal_with_manifest(wal, &manager, snapshot_seq, cluster_id, shard_id)
}

/// 复用严格清单锁修复未提交 WAL 尾部；调用方必须独占 WAL 目录。
pub fn repair_local_wal_with_manifest(
    wal: impl AsRef<Path>,
    manager: &crate::ManifestManager<crate::FileManifestStorage>,
    snapshot_seq: u64,
    cluster_id: u16,
    shard_id: u16,
) -> io::Result<Vec<WalRecord>> {
    use std::fs::{self, OpenOptions};
    if !manager.recovery_checked() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "清单未通过严格恢复检查",
        ));
    }
    let wal = wal.as_ref();
    let state = manager.state();
    if !state.gc_plan.is_empty() || !valid_snapshot_boundary(state, snapshot_seq) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "快照或回收计划状态无效",
        ));
    }
    let scan = scan_segments(wal)?;
    if state.generation == 0 && !scan.segments.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "存在 WAL 但缺少清单",
        ));
    }
    let records = recover_committed(
        wal,
        snapshot_seq,
        state.local_durable_seq,
        cluster_id,
        shard_id,
    )?;
    if state.local_durable_seq == 0 {
        for segment in scan.segments.iter().rev() {
            fs::remove_file(wal.join(format!("{:020}.wal", segment.first_seq)))?;
            crate::manifest_file::sync_directory(wal)?;
        }
        return Ok(records);
    }
    let active = state
        .active_segment
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "缺少活动段"))?;
    let segment = scan
        .segments
        .iter()
        .find(|s| s.first_seq == active)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "活动段缺失"))?;
    let mut keep_len = 0_u64;
    let mut found = false;
    for record in scan
        .records
        .iter()
        .filter(|r| r.shard_seq >= active && r.shard_seq <= state.local_durable_seq)
    {
        let len = u64::try_from(
            record
                .encode()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "记录不能编码"))?
                .len(),
        )
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "记录长度溢出"))?;
        keep_len = keep_len
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "偏移溢出"))?;
        found |= record.shard_seq == state.local_durable_seq;
    }
    if !found || keep_len > segment.valid_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "已提交活动段不完整",
        ));
    }
    for tail in scan.segments.iter().rev().filter(|s| s.first_seq > active) {
        fs::remove_file(wal.join(format!("{:020}.wal", tail.first_seq)))?;
        crate::manifest_file::sync_directory(wal)?;
    }
    if keep_len != segment.size {
        let file = OpenOptions::new()
            .write(true)
            .open(wal.join(format!("{active:020}.wal")))?;
        if file.metadata()?.len() != segment.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "截断前活动段发生变化",
            ));
        }
        file.set_len(keep_len)?;
        file.sync_all()?;
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_boundary_must_not_precede_gc_watermark() {
        let state = crate::ManifestState {
            latest_snapshot: Some((2, 10)),
            previous_snapshot: Some((1, 4)),
            wal_gc_safe_seq: 5,
            ..Default::default()
        };
        assert!(!valid_snapshot_boundary(&state, 0));
        assert!(!valid_snapshot_boundary(&state, 4));
        assert!(valid_snapshot_boundary(&state, 10));
        assert!(!valid_snapshot_boundary(&state, 6));
    }
}
