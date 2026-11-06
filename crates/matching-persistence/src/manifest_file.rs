//! 双槽 manifest 文件、校验及独占写者锁。
use crate::{ManifestManager, ManifestState, ManifestStorage, crc32c};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// 单分片双槽文件的独占持有者。
pub struct FileManifestStorage {
    /// manifest 所在目录。
    directory: PathBuf,
    /// 持有期间保持的文件锁。
    _lock: File,
    /// 尚未同步的临时文件。
    pending_file: Option<File>,
    /// 临时文件和目标槽位。
    pending_paths: Option<(PathBuf, PathBuf)>,
    /// 从已验证槽位恢复的权威状态。
    committed: ManifestState,
    /// 已有槽损坏或缺失时禁止在回退状态上写入。
    unsafe_baseline: bool,
    /// 是否存在不含本地水位的旧版本槽。
    legacy_slot: bool,
    /// 正在持久化的候选状态。
    pending_state: Option<ManifestState>,
}
impl FileManifestStorage {
    /// 获取独占锁，验证双槽并选择最高合法代际。
    pub(crate) fn open(directory: impl AsRef<Path>) -> io::Result<(Self, ManifestState)> {
        Self::open_inner(directory, false)
    }

    /// 在读取已确认水位时拒绝缺失或损坏的历史槽。
    fn open_inner(directory: impl AsRef<Path>, strict: bool) -> io::Result<(Self, ManifestState)> {
        let directory = directory.as_ref().to_path_buf();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("manifest.lock"))?;
        lock.try_lock()
            .map_err(|error| io::Error::other(format!("manifest 锁不可用: {error}")))?;
        let mut best: Option<ManifestState> = None;
        let mut existing = false;
        let mut invalid_slot = false;
        let mut generations = Vec::new();
        let mut legacy_slot = false;
        for name in ["manifest-a", "manifest-b"] {
            match File::open(directory.join(name)) {
                Ok(file) => {
                    existing = true;
                    let mut bytes = Vec::new();
                    file.take(600_001).read_to_end(&mut bytes)?;
                    if let Some(state) = decode(&bytes)
                        && ((name == "manifest-a") == (state.generation % 2 == 1))
                    {
                        legacy_slot |= bytes[4..6] == 1u16.to_be_bytes();
                        generations.push(state.generation);
                        if let Some(current) = &best
                            && current.generation == state.generation
                            && current != &state
                        {
                            return Err(invalid("相同代际内容冲突"));
                        }
                        if best
                            .as_ref()
                            .is_none_or(|current| state.generation > current.generation)
                        {
                            best = Some(state);
                        }
                    } else {
                        invalid_slot = true;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        if existing && best.is_none() {
            return Err(invalid("没有合法 manifest 槽位"));
        }
        if !existing && directory.join("manifest.tmp").try_exists()? {
            return Err(invalid("初始 manifest 存在未完成临时文件"));
        }
        generations.sort_unstable();
        let valid_slots = match generations.as_slice() {
            [] if !existing => true,
            [1] => true,
            [a, b] => a.checked_add(1) == Some(*b),
            _ => false,
        };
        let unsafe_baseline = invalid_slot || !valid_slots || (existing && generations.len() != 2);
        if strict && existing && (unsafe_baseline || legacy_slot || generations.len() != 2) {
            return Err(invalid("无法证明已确认水位，拒绝回退"));
        }
        let state = best.unwrap_or_default();
        Ok((
            Self {
                directory,
                _lock: lock,
                pending_file: None,
                pending_paths: None,
                committed: state.clone(),
                unsafe_baseline,
                legacy_slot,
                pending_state: None,
            },
            state,
        ))
    }
}

impl ManifestManager<FileManifestStorage> {
    /// 打开双槽文件并获取该分片的独占写者锁。
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let (storage, state) = FileManifestStorage::open(directory)?;
        Ok(Self::new(storage, state))
    }
    /// 严格检查双槽，禁止将损坏槽回退作为已确认水位。
    pub fn open_recovery(directory: impl AsRef<Path>) -> io::Result<Self> {
        let (storage, state) = FileManifestStorage::open_inner(directory, true)?;
        let mut manager = Self::new(storage, state);
        manager.mark_recovery_checked();
        Ok(manager)
    }
}
impl ManifestStorage for FileManifestStorage {
    fn validate_base(&self, state: &ManifestState) -> io::Result<()> {
        if self.unsafe_baseline {
            return Err(invalid("清单槽损坏或缺失，禁止在回退状态上写入"));
        }
        if self.legacy_slot {
            return Err(invalid("旧版清单缺少可信提交水位，禁止直接升级"));
        }
        if state != &self.committed {
            return Err(invalid("管理器前态与持久槽位不一致"));
        }
        Ok(())
    }
    fn write_inactive_tmp(&mut self, generation: u64, state: &ManifestState) -> io::Result<()> {
        if generation != state.generation
            || self.committed.generation.checked_add(1) != Some(generation)
            || self.pending_file.is_some()
            || self.pending_state.is_some()
        {
            return Err(invalid("manifest 写入状态不一致"));
        }
        let bytes = encode(state)?;
        let tmp = self.directory.join("manifest.tmp");
        let slot = self.directory.join(if generation % 2 == 1 {
            "manifest-a"
        } else {
            "manifest-b"
        });
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        self.pending_file = Some(file);
        self.pending_paths = Some((tmp, slot));
        self.pending_state = Some(state.clone());
        Ok(())
    }
    fn sync_tmp(&mut self) -> io::Result<()> {
        let file = self
            .pending_file
            .take()
            .ok_or_else(|| invalid("临时文件不存在"))?;
        file.sync_all()
    }
    fn rename_inactive_slot(&mut self) -> io::Result<()> {
        let (tmp, slot) = self
            .pending_paths
            .take()
            .ok_or_else(|| invalid("临时路径不存在"))?;
        fs::rename(tmp, slot)
    }
    fn sync_directory(&mut self) -> io::Result<()> {
        sync_directory(&self.directory)?;
        self.committed = self
            .pending_state
            .take()
            .ok_or_else(|| invalid("候选状态不存在"))?;
        Ok(())
    }
}

/// 同步目录项；Windows 目录句柄需显式目录访问标志。
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x0200_0000)
            .open(path)?
            .sync_all()
    }
    #[cfg(not(windows))]
    {
        File::open(path)?.sync_all()
    }
}

/// 以固定大端顺序编码完整状态并附加 CRC32C。
fn encode(state: &ManifestState) -> io::Result<Vec<u8>> {
    if state.gc_plan.len() > 65_536
        || state.previous_snapshot.is_some_and(|(id, seq)| {
            state
                .latest_snapshot
                .is_none_or(|(latest_id, latest_seq)| id == latest_id || seq >= latest_seq)
        })
        || state.gc_plan.contains(&0)
        || state.gc_plan.windows(2).any(|w| w[0] >= w[1])
        || (state.gc_plan.is_empty() != state.gc_retained_first.is_none())
        || (state.gc_plan.is_empty() != state.gc_identity.is_none())
        || state
            .gc_retained_first
            .is_some_and(|first| state.gc_plan.last().is_none_or(|last| first <= *last))
    {
        return Err(invalid("无效回收计划"));
    }
    let mut out = Vec::new();
    out.extend_from_slice(b"MNF1");
    out.extend_from_slice(&3u16.to_be_bytes());
    out.extend_from_slice(&state.generation.to_be_bytes());
    out.extend_from_slice(&state.replication_safe_seq.to_be_bytes());
    out.push(u8::from(state.latest_snapshot.is_some()));
    let (id, seq) = state.latest_snapshot.unwrap_or((0, 0));
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.push(u8::from(state.active_segment.is_some()));
    out.extend_from_slice(&state.active_segment.unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&state.wal_gc_safe_seq.to_be_bytes());
    out.extend_from_slice(&(state.gc_plan.len() as u32).to_be_bytes());
    for id in &state.gc_plan {
        out.extend_from_slice(&id.to_be_bytes());
    }
    out.extend_from_slice(&state.local_durable_seq.to_be_bytes());
    out.extend_from_slice(&state.gc_retained_first.unwrap_or(0).to_be_bytes());
    let (cluster, shard) = state.gc_identity.unwrap_or((0, 0));
    out.extend_from_slice(&cluster.to_be_bytes());
    out.extend_from_slice(&shard.to_be_bytes());
    out.push(u8::from(state.previous_snapshot.is_some()));
    let (previous_id, previous_seq) = state.previous_snapshot.unwrap_or((0, 0));
    out.extend_from_slice(&previous_id.to_be_bytes());
    out.extend_from_slice(&previous_seq.to_be_bytes());
    out.extend_from_slice(&crc32c(&out).to_be_bytes());
    Ok(out)
}

/// 验证长度、版本、校验值和字段后解析完整状态。
fn decode(bytes: &[u8]) -> Option<ManifestState> {
    if bytes.len() < 64
        || bytes.len() > 600_000
        || &bytes[..4] != b"MNF1"
        || ![1u16, 2u16, 3u16].contains(&u16::from_be_bytes(bytes[4..6].try_into().ok()?))
    {
        return None;
    }
    let data_end = bytes.len().checked_sub(4)?;
    if crc32c(&bytes[..data_end]) != u32::from_be_bytes(bytes[data_end..].try_into().ok()?) {
        return None;
    }
    let u64_at = |i| u64::from_be_bytes(bytes[i..i + 8].try_into().expect("已验证固定长度"));
    let generation = u64_at(6);
    if generation == 0 {
        return None;
    }
    let replication_safe_seq = u64_at(14);
    let snapshot = match bytes[22] {
        0 if u64_at(23) == 0 && u64_at(31) == 0 => None,
        1 => Some((u64_at(23), u64_at(31))),
        _ => return None,
    };
    let active = match bytes[39] {
        0 if u64_at(40) == 0 => None,
        1 if u64_at(40) != 0 => Some(u64_at(40)),
        _ => return None,
    };
    let wal_gc_safe_seq = u64_at(48);
    let count = u32::from_be_bytes(bytes[56..60].try_into().ok()?) as usize;
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let plan_end = 60usize.checked_add(count.checked_mul(8)?)?;
    let expected_end = plan_end.checked_add(match version {
        1 => 0,
        2 => 20,
        3 => 37,
        _ => return None,
    })?;
    if count > 65_536 || expected_end != data_end {
        return None;
    }
    let mut gc_plan = Vec::with_capacity(count);
    for i in 0..count {
        gc_plan.push(u64_at(60 + i * 8));
    }
    if gc_plan.contains(&0) || gc_plan.windows(2).any(|w| w[0] >= w[1]) {
        return None;
    }
    let retained_first = if version >= 2 {
        u64_at(plan_end + 8)
    } else {
        0
    };
    if (count == 0 && retained_first != 0)
        || (count != 0 && version >= 2 && gc_plan.last().is_none_or(|last| retained_first <= *last))
    {
        return None;
    }
    let gc_identity = if version >= 2 {
        let cluster = u16::from_be_bytes(bytes[plan_end + 16..plan_end + 18].try_into().ok()?);
        let shard = u16::from_be_bytes(bytes[plan_end + 18..plan_end + 20].try_into().ok()?);
        if count == 0 && (cluster != 0 || shard != 0) {
            return None;
        }
        (count != 0).then_some((cluster, shard))
    } else {
        None
    };
    let previous_snapshot = if version == 3 {
        match bytes[plan_end + 20] {
            0 if u64_at(plan_end + 21) == 0 && u64_at(plan_end + 29) == 0 => None,
            1 => Some((u64_at(plan_end + 21), u64_at(plan_end + 29))),
            _ => return None,
        }
    } else {
        None
    };
    if previous_snapshot.is_some_and(|(id, seq)| {
        snapshot.is_none_or(|(latest_id, latest_seq)| id == latest_id || seq >= latest_seq)
    }) {
        return None;
    }
    Some(ManifestState {
        generation,
        local_durable_seq: if version >= 2 { u64_at(plan_end) } else { 0 },
        replication_safe_seq,
        latest_snapshot: snapshot,
        previous_snapshot,
        active_segment: active,
        wal_gc_safe_seq,
        gc_plan,
        gc_retained_first: (retained_first != 0).then_some(retained_first),
        gc_identity,
    })
}

/// 将损坏或协议冲突映射为可传播的 I/O 错误。
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    //! 验证磁盘存储与管理器的权威前态绑定。
    use super::*;
    use crate::ManifestIntent;
    #[test]
    fn disk_storage_rejects_manager_baseline_not_matching_recovered_state() {
        let dir = std::env::temp_dir().join(format!("manifest-baseline-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let (storage, _) = FileManifestStorage::open(&dir).unwrap();
        let mut manager = ManifestManager::new(
            storage,
            ManifestState {
                generation: 6,
                ..ManifestState::default()
            },
        );
        assert!(
            manager
                .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 1 })
                .is_err()
        );
        drop(manager);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reopened_slots_after_each_write_boundary_choose_valid_generation() {
        for boundary in 0..4 {
            let dir = std::env::temp_dir().join(format!(
                "manifest-boundary-{}-{boundary}",
                std::process::id()
            ));
            std::fs::create_dir(&dir).unwrap();
            let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
            manager
                .submit(ManifestIntent::SegmentRolled { first_seq: 1 })
                .unwrap();
            let before = manager.state().clone();
            drop(manager);
            let (mut storage, _) = FileManifestStorage::open(&dir).unwrap();
            let mut after = before.clone();
            after.generation = 2;
            after.replication_safe_seq = 1;
            storage.write_inactive_tmp(2, &after).unwrap();
            if boundary >= 1 {
                storage.sync_tmp().unwrap();
            }
            if boundary >= 2 {
                storage.rename_inactive_slot().unwrap();
            }
            if boundary >= 3 {
                storage.sync_directory().unwrap();
            }
            drop(storage);
            let reopened = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
            assert_eq!(
                reopened.state(),
                if boundary < 2 { &before } else { &after }
            );
            drop(reopened);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn strict_recovery_preserves_confirmed_watermark_at_manifest_write_boundaries() {
        for boundary in 0..4 {
            let dir = std::env::temp_dir().join(format!(
                "manifest-strict-boundary-{}-{boundary}",
                std::process::id()
            ));
            std::fs::create_dir(&dir).unwrap();
            let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
            manager
                .submit(ManifestIntent::LocalDurableAdvanced {
                    through_seq: 1,
                    first_seq: 1,
                })
                .unwrap();
            manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
            let before = manager.state().clone();
            drop(manager);
            let (mut storage, _) = FileManifestStorage::open(&dir).unwrap();
            let mut after = before.clone();
            after.generation = 3;
            after.local_durable_seq = 2;
            storage.write_inactive_tmp(3, &after).unwrap();
            if boundary >= 1 {
                storage.sync_tmp().unwrap();
            }
            if boundary >= 2 {
                storage.rename_inactive_slot().unwrap();
            }
            if boundary >= 3 {
                storage.sync_directory().unwrap();
            }
            drop(storage);
            let reopened = ManifestManager::<FileManifestStorage>::open_recovery(&dir).unwrap();
            assert_eq!(
                reopened.state(),
                if boundary < 2 { &before } else { &after }
            );
            assert!(reopened.state().local_durable_seq >= 1);
            drop(reopened);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn version_two_manifest_has_stable_golden_bytes() {
        let hex = "4d4e463100020000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c4c102e0";
        let bytes: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let digits = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(digits, 16).unwrap()
            })
            .collect();
        let state = ManifestState {
            generation: 1,
            ..ManifestState::default()
        };
        assert_eq!(decode(&bytes), Some(state.clone()));
        let mut version_three = bytes[..bytes.len() - 4].to_vec();
        version_three[5] = 3;
        version_three.extend_from_slice(&[0; 17]);
        version_three.extend_from_slice(&[0x4a, 0x98, 0x4d, 0x80]);
        assert_eq!(encode(&state).unwrap(), version_three);
        assert_eq!(decode(&version_three), Some(state));
    }
}
