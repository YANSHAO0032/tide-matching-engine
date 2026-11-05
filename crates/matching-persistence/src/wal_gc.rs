//! WAL 段回收的安全筛选与可重试的持久化步骤。
use crate::{ManifestIntent, ManifestManager, ManifestStorage, scan_segments};
use std::{
    fs, io,
    path::Path,
    time::{Duration, SystemTime},
};

/// 由快照和保留策略所有者验证的回收边界。
///
/// 调用方须验证 manifest 和至少两个持久快照属于指定集群及分片，并独占 WAL 与回收目录；
/// `oldest_snapshot_seq` 是仍需保留的最旧快照序号。
pub struct WalGcPolicy {
    /// 与快照归属一致的集群身份。
    pub cluster_id: u16,
    /// 与快照归属一致的分片身份。
    pub shard_id: u16,
    /// 最旧可恢复快照的序号。
    pub oldest_snapshot_seq: u64,
    /// 最新持久快照的身份。
    pub latest_snapshot_id: u64,
    /// 最新持久快照的序号。
    pub latest_snapshot_seq: u64,
    /// HA 模式下可信的复制安全前缀；非 HA 时为空。
    pub replication_safe_seq: Option<u64>,
    /// 恢复及导出策略允许删除的最高序号。
    pub recovery_floor: u64,
    /// 文件最短保留时间。
    pub min_retention: Duration,
}

/// 依据已验证的快照与保留条件计划并执行回收；已有计划优先幂等完成。
///
/// manifest 本身不持久绑定分片身份，调用方须验证其与快照和策略身份一致。
///
/// 调用方必须确保所有 `WalGcPlanned` 意图均由本流程提交，且两个目录在操作中独占。
/// 当计划已持久化后，文件缺失代表相应删除步骤可能已经完成。
pub fn run_wal_gc<S: ManifestStorage>(
    wal: impl AsRef<Path>,
    trash: impl AsRef<Path>,
    manager: &mut ManifestManager<S>,
    policy: &WalGcPolicy,
) -> io::Result<Vec<u64>> {
    if !manager.recovery_checked() {
        return Err(invalid("回收需要严格校验的清单管理器"));
    }
    let wal = wal.as_ref();
    let trash = trash.as_ref();
    if fs::canonicalize(wal)? == fs::canonicalize(trash)? {
        return Err(invalid("回收目录不能等于 WAL 目录"));
    }
    if manager.state().latest_snapshot
        != Some((policy.latest_snapshot_id, policy.latest_snapshot_seq))
        || policy.oldest_snapshot_seq >= policy.latest_snapshot_seq
        || policy.latest_snapshot_seq > manager.state().local_durable_seq
        || policy
            .replication_safe_seq
            .is_some_and(|s| s > manager.state().replication_safe_seq)
    {
        return Err(invalid("快照或复制安全边界不一致"));
    }
    if !manager.state().gc_plan.is_empty() {
        return finish(wal, trash, manager, policy);
    }
    let scan = scan_segments(wal)?;
    validate_wal_identity(&scan, manager.state().local_durable_seq, policy)?;
    let active = manager
        .state()
        .active_segment
        .ok_or_else(|| invalid("活动段不存在"))?;
    if scan.segments.last().map(|s| s.first_seq) != Some(active) {
        return Err(invalid("活动段与清单不一致"));
    }
    let keep = scan
        .segments
        .iter()
        .rev()
        .find(|s| s.last_seq.is_some_and(|n| n < policy.oldest_snapshot_seq))
        .map(|s| s.first_seq);
    let floor = policy
        .oldest_snapshot_seq
        .min(policy.recovery_floor)
        .min(policy.replication_safe_seq.unwrap_or(u64::MAX));
    let now = SystemTime::now();
    let mut planned = Vec::new();
    for segment in &scan.segments {
        let Some(last) = segment.last_seq else {
            break;
        };
        if segment.first_seq == active
            || Some(segment.first_seq) == keep
            || last > floor
            || segment.torn_tail
        {
            break;
        }
        let modified =
            fs::metadata(wal.join(format!("{:020}.wal", segment.first_seq)))?.modified()?;
        let age = now
            .duration_since(modified)
            .map_err(|_| invalid("文件时间晚于当前时间"))?;
        if age < policy.min_retention {
            break;
        }
        planned.push(segment.first_seq);
    }
    if planned.is_empty() {
        return Ok(planned);
    }
    let retained_first = scan
        .segments
        .iter()
        .find(|s| !planned.contains(&s.first_seq))
        .ok_or_else(|| invalid("回收计划缺少保留段"))?
        .first_seq;
    manager.submit(ManifestIntent::WalGcPlanned {
        segments: planned,
        retained_first,
        cluster_id: policy.cluster_id,
        shard_id: policy.shard_id,
    })?;
    finish(wal, trash, manager, policy)
}

/// 完成已持久化计划；每个目录变更均在下一步骤前同步。
fn finish<S: ManifestStorage>(
    wal: &Path,
    trash: &Path,
    manager: &mut ManifestManager<S>,
    policy: &WalGcPolicy,
) -> io::Result<Vec<u64>> {
    let plan = manager.state().gc_plan.clone();
    let last_planned = *plan.last().ok_or_else(|| invalid("回收计划为空"))?;
    if manager.state().gc_identity != Some((policy.cluster_id, policy.shard_id)) {
        return Err(invalid("回收计划分片身份不一致"));
    }
    let scan = scan_segments(wal)?;
    validate_wal_identity(&scan, manager.state().local_durable_seq, policy)?;
    let active = manager
        .state()
        .active_segment
        .ok_or_else(|| invalid("活动段不存在"))?;
    if scan.segments.last().map(|s| s.first_seq) != Some(active) || last_planned >= active {
        return Err(invalid("回收计划包含活动段或活动段不一致"));
    }
    let first_retained = scan
        .segments
        .iter()
        .find(|s| !plan.contains(&s.first_seq))
        .ok_or_else(|| invalid("回收计划缺少保留段"))?
        .first_seq;
    if first_retained <= last_planned || manager.state().gc_retained_first != Some(first_retained) {
        return Err(invalid("回收计划不是连续前缀"));
    }
    let through = first_retained
        .checked_sub(1)
        .ok_or_else(|| invalid("回收水位无效"))?;
    let floor = policy
        .oldest_snapshot_seq
        .min(policy.recovery_floor)
        .min(policy.replication_safe_seq.unwrap_or(u64::MAX));
    if through > floor {
        return Err(invalid("回收计划超出安全水位"));
    }
    let keep = scan
        .segments
        .iter()
        .map(|s| s.first_seq)
        .chain(plan.iter().copied())
        .filter(|first| *first < policy.oldest_snapshot_seq)
        .max();
    if keep.is_some_and(|first| plan.contains(&first)) {
        return Err(invalid("回收计划未保留快照前一段"));
    }
    for seq in &plan {
        let target = trash.join(format!("{seq:020}.wal"));
        if target.try_exists()? {
            validate_trash(&target, *seq, policy)?;
        }
    }
    for seq in &plan {
        let name = format!("{seq:020}.wal");
        let source = wal.join(&name);
        let target = trash.join(&name);
        let source_exists = source.try_exists()?;
        let target_exists = target.try_exists()?;
        if source_exists && target_exists {
            return Err(invalid("回收源与目标同时存在"));
        }
        if source_exists {
            fs::rename(&source, &target)?;
            crate::manifest_file::sync_directory(wal)?;
            crate::manifest_file::sync_directory(trash)?;
        }
        if target.try_exists()? {
            fs::remove_file(&target)?;
            crate::manifest_file::sync_directory(trash)?;
        }
    }
    let remaining = scan_segments(wal)?;
    let first_remaining = remaining
        .segments
        .first()
        .ok_or_else(|| invalid("回收后缺少保留段"))?
        .first_seq;
    if plan.last().is_none_or(|last| *last >= first_remaining)
        || manager.state().gc_retained_first != Some(first_remaining)
    {
        return Err(invalid("回收计划不是连续前缀"));
    }
    let through_seq = first_remaining
        .checked_sub(1)
        .ok_or_else(|| invalid("回收水位无效"))?;
    manager.submit(ManifestIntent::WalGcCompleted {
        segments: plan.clone(),
        through_seq,
    })?;
    Ok(plan)
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// 验证现存 WAL 与调用方已验证快照的分片身份及本地持久水位一致。
fn validate_wal_identity(
    scan: &crate::SegmentScan,
    durable_seq: u64,
    policy: &WalGcPolicy,
) -> io::Result<()> {
    if scan
        .records
        .iter()
        .any(|record| record.cluster_id != policy.cluster_id || record.shard_id != policy.shard_id)
        || scan
            .records
            .last()
            .is_none_or(|record| record.shard_seq < durable_seq)
    {
        return Err(invalid("WAL 分片身份或本地持久前缀不一致"));
    }
    Ok(())
}

/// 删除前校验回收目录中已重命名段的身份与记录完整性。
fn validate_trash(path: &Path, first: u64, policy: &WalGcPolicy) -> io::Result<()> {
    use std::io::Read;
    let file = fs::File::open(path)?;
    let size = file.metadata()?.len();
    if size > 64 * 1024 * 1024 {
        return Err(invalid("回收段过大"));
    }
    let mut bytes = Vec::new();
    file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).ok() != Some(size) {
        return Err(invalid("回收段发生变化"));
    }
    let scan = crate::scan_segment(&bytes, false).map_err(|_| invalid("回收段损坏"))?;
    if scan
        .records
        .first()
        .is_none_or(|record| record.shard_seq != first)
        || scan.records.iter().any(|record| {
            record.cluster_id != policy.cluster_id || record.shard_id != policy.shard_id
        })
    {
        return Err(invalid("回收段分片身份不一致"));
    }
    Ok(())
}
