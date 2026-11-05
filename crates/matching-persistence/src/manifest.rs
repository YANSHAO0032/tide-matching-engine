//! 单写者 manifest 状态及持久化步骤编排。
use std::io;

/// 可恢复的 manifest 状态；字段变更须通过管理器提交。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestState {
    /// 已完成持久化的代际。
    pub generation: u64,
    /// 已确认的备节点安全前缀。
    pub replication_safe_seq: u64,
    /// 最近持久化快照身份及其分片序号。
    pub latest_snapshot: Option<(u64, u64)>,
    /// 当前活动 segment 的首序号。
    pub active_segment: Option<u64>,
    /// 已确认的 WAL 回收安全前缀。
    pub wal_gc_safe_seq: u64,
    /// 已计划但尚未完成的 segment 首序号。
    pub gc_plan: Vec<u64>,
}

/// 向单写者提交的状态变更意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestIntent {
    /// 登记已创建的活动 segment。
    SegmentRolled {
        /// 新 segment 首序号。
        first_seq: u64,
    },
    /// 登记已持久化的快照。
    SnapshotCommitted {
        /// 快照身份。
        snapshot_id: u64,
        /// 快照覆盖的分片序号。
        snapshot_seq: u64,
    },
    /// 提升备节点安全前缀。
    ReplicationSafeSeqAdvanced {
        /// 新的连续持久前缀。
        seq: u64,
    },
    /// 登记待回收的 segment 集合。
    WalGcPlanned {
        /// 严格递增的 segment 首序号。
        segments: Vec<u64>,
    },
    /// 完成当前已登记的回收计划。
    WalGcCompleted {
        /// 与当前计划一致的 segment 首序号。
        segments: Vec<u64>,
    },
}

/// 由具体存储实现提供的有序持久化原语。
///
/// 调用方须保证每个分片只有一个持久化写者；本接口不提供跨进程锁。
pub trait ManifestStorage {
    /// 校验管理器前态与存储持有的权威前态一致。
    fn validate_base(&self, _state: &ManifestState) -> io::Result<()> {
        Ok(())
    }
    /// 向非活动槽位的临时文件写入完整下一代。
    fn write_inactive_tmp(&mut self, generation: u64, state: &ManifestState) -> io::Result<()>;
    /// 同步临时文件数据。
    fn sync_tmp(&mut self) -> io::Result<()>;
    /// 将临时文件原子重命名至非活动槽位。
    fn rename_inactive_slot(&mut self) -> io::Result<()>;
    /// 同步 manifest 目录元数据。
    fn sync_directory(&mut self) -> io::Result<()>;
}

/// 独占持久化原语及当前状态的单写者管理器。
pub struct ManifestManager<S> {
    /// 当前已完成持久化的状态。
    state: ManifestState,
    /// 由本管理器独占的存储。
    storage: S,
    /// 写入过程出错后保持关闭。
    failed: bool,
}
impl<S: ManifestStorage> ManifestManager<S> {
    /// 使用调用方已经验证的状态和独占存储创建管理器。
    pub fn new(storage: S, state: ManifestState) -> Self {
        Self {
            state,
            storage,
            failed: false,
        }
    }
    /// 返回最近完成持久化的状态。
    pub fn state(&self) -> &ManifestState {
        &self.state
    }
    /// 借用存储供诊断，不提供可变访问。
    pub fn storage(&self) -> &S {
        &self.storage
    }
    /// 先冻结下一代，全部同步步骤成功后才公开该状态。
    pub fn submit(&mut self, intent: ManifestIntent) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("manifest 管理器已停止"));
        }
        if let Err(error) = self.storage.validate_base(&self.state) {
            self.failed = true;
            return Err(error);
        }
        let mut next = self.state.clone();
        match intent {
            ManifestIntent::SegmentRolled { first_seq } => {
                if first_seq == 0
                    || next
                        .active_segment
                        .is_some_and(|current| first_seq <= current)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "活动 segment 不递增",
                    ));
                }
                next.active_segment = Some(first_seq);
            }
            ManifestIntent::SnapshotCommitted {
                snapshot_id,
                snapshot_seq,
            } => {
                if next
                    .latest_snapshot
                    .is_some_and(|(_, seq)| snapshot_seq <= seq)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "快照序号不递增",
                    ));
                }
                next.latest_snapshot = Some((snapshot_id, snapshot_seq));
            }
            ManifestIntent::ReplicationSafeSeqAdvanced { seq } => {
                if seq < next.replication_safe_seq {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "复制前缀不得回退",
                    ));
                }
                next.replication_safe_seq = seq;
            }
            ManifestIntent::WalGcPlanned { segments } => {
                if !next.gc_plan.is_empty()
                    || segments.is_empty()
                    || segments.len() > 65_536
                    || segments[0] == 0
                    || segments.windows(2).any(|w| w[0] >= w[1])
                {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "无效回收计划"));
                }
                next.gc_plan = segments;
            }
            ManifestIntent::WalGcCompleted { segments } => {
                if next.gc_plan.is_empty() || next.gc_plan != segments {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "回收计划不匹配",
                    ));
                }
                next.gc_plan.clear();
            }
        }
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("manifest 代际耗尽"))?;
        let result = self
            .storage
            .write_inactive_tmp(next.generation, &next)
            .and_then(|()| self.storage.sync_tmp())
            .and_then(|()| self.storage.rename_inactive_slot())
            .and_then(|()| self.storage.sync_directory());
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        self.state = next;
        Ok(())
    }
}
