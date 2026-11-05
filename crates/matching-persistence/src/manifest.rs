//! 单写者 manifest 状态及持久化步骤编排。
use std::io;

/// 可恢复的 manifest 状态；字段变更须通过管理器提交。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestState {
    /// 已完成持久化的代际。
    pub generation: u64,
    /// 已完成本地文件及目录同步的连续前缀。
    pub local_durable_seq: u64,
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
    /// 计划完成后应保留的首个 segment 首序号。
    pub gc_retained_first: Option<u64>,
    /// 已登记回收计划所属的集群与分片身份。
    pub gc_identity: Option<(u16, u16)>,
}

/// 向单写者提交的状态变更意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestIntent {
    /// 建立空状态的首代恢复锚点。
    Initialize,
    /// 登记已创建的活动 segment。
    SegmentRolled {
        /// 新 segment 首序号。
        first_seq: u64,
    },
    /// 原子登记本地持久前缀和活动段。
    LocalDurableAdvanced {
        /// 已完成同步的连续前缀。
        through_seq: u64,
        /// 当前活动段的首序号。
        first_seq: u64,
    },
    /// 首次持久确认前将首代状态复制到另一槽。
    RecoveryAnchor,
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
        /// 计划后首个保留段的首序号。
        retained_first: u64,
        /// 快照与 WAL 所属的集群身份。
        cluster_id: u16,
        /// 快照与 WAL 所属的分片身份。
        shard_id: u16,
    },
    /// 完成回收并持久登记已删除的连续序号前缀。
    WalGcCompleted {
        /// 与当前计划一致的 segment 首序号。
        segments: Vec<u64>,
        /// 已删除段覆盖的最高序号。
        through_seq: u64,
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
    /// 是否经过严格槽位恢复校验。
    recovery_checked: bool,
    /// 写入过程出错后保持关闭。
    failed: bool,
}
impl<S: ManifestStorage> ManifestManager<S> {
    /// 使用调用方已经验证的状态和独占存储创建管理器。
    pub fn new(storage: S, state: ManifestState) -> Self {
        Self {
            state,
            storage,
            recovery_checked: false,
            failed: false,
        }
    }
    /// 标记由文件存储执行过严格的槽位检查。
    pub(crate) fn mark_recovery_checked(&mut self) {
        self.recovery_checked = true;
    }
    /// 返回是否已执行严格恢复检查。
    pub(crate) fn recovery_checked(&self) -> bool {
        self.recovery_checked
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
            ManifestIntent::Initialize => {
                if next != ManifestState::default() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "清单不为空"));
                }
            }
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
            ManifestIntent::LocalDurableAdvanced {
                through_seq,
                first_seq,
            } => {
                if first_seq == 0
                    || first_seq > through_seq
                    || through_seq <= next.local_durable_seq
                    || next
                        .active_segment
                        .is_some_and(|current| first_seq < current)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "本地持久前缀不递增",
                    ));
                }
                next.active_segment = Some(first_seq);
                next.local_durable_seq = through_seq;
            }
            ManifestIntent::RecoveryAnchor => {
                if next.generation != 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "恢复锚点状态无效",
                    ));
                }
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
            ManifestIntent::WalGcPlanned {
                segments,
                retained_first,
                cluster_id,
                shard_id,
            } => {
                if !next.gc_plan.is_empty()
                    || segments.is_empty()
                    || segments.len() > 65_536
                    || segments[0] == 0
                    || segments.windows(2).any(|w| w[0] >= w[1])
                    || segments.last().is_none_or(|last| retained_first <= *last)
                    || next
                        .active_segment
                        .is_none_or(|active| retained_first > active)
                    || next.latest_snapshot.is_none()
                    || next
                        .active_segment
                        .is_none_or(|active| segments.last().is_none_or(|last| *last >= active))
                {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "无效回收计划"));
                }
                next.gc_plan = segments;
                next.gc_retained_first = Some(retained_first);
                next.gc_identity = Some((cluster_id, shard_id));
            }
            ManifestIntent::WalGcCompleted {
                segments,
                through_seq,
            } => {
                if next.gc_plan.is_empty()
                    || next.gc_plan != segments
                    || segments.last().is_none_or(|last| *last > through_seq)
                    || next.gc_retained_first.and_then(|seq| seq.checked_sub(1))
                        != Some(through_seq)
                    || through_seq < next.wal_gc_safe_seq
                    || next
                        .latest_snapshot
                        .is_none_or(|(_, seq)| through_seq > seq)
                    || next
                        .active_segment
                        .is_none_or(|active| through_seq >= active)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "回收完成水位无效",
                    ));
                }
                next.wal_gc_safe_seq = through_seq;
                next.gc_plan.clear();
                next.gc_retained_first = None;
                next.gc_identity = None;
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
