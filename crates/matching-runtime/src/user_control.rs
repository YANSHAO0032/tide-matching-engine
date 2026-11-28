//! 分片保存有界的用户纪元下限，以及各用户和全局最近一次控制的持久回执。

use matching_domain::{CommandId, RejectReason, UserFenceAction, UserId};
use std::collections::BTreeMap;

/// 同一作用域最近一次已提交控制的持久回执。作用域由映射键区分。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FenceReceipt {
    /// 已应用的控制纪元。
    pub epoch: u64,
    /// 已应用的父操作身份。
    pub parent: CommandId,
    /// 首次执行的动作，用于区分重试与意图冲突。
    pub action: UserFenceAction,
    /// 当前作用域开关状态，单纯撤单沿用此前状态。
    pub killed: bool,
    /// 首次应用时的本地分片序号。
    pub seq: u64,
    /// 首次实际终结的订单数。
    pub canceled: u64,
    /// 首次执行的最后事件索引。
    pub last_event: u32,
}

/// 核验跨分片控制回执：开关动作先发一条事件，每笔撤单可附一条资金释放，最后发纪元事件。
pub(crate) fn control_events_match(action: UserFenceAction, canceled: u64, last: u32) -> bool {
    let switch_event = match action {
        UserFenceAction::FenceAndCancelAll => 0,
        UserFenceAction::Kill {
            cancel_all: false, ..
        } => return canceled == 0 && last == 1,
        UserFenceAction::Kill {
            active: true,
            cancel_all: true,
        } => 1,
        _ => return false,
    };
    u64::from(last)
        .checked_sub(switch_event)
        .and_then(|orders_and_releases| orders_and_releases.checked_sub(canceled))
        .is_some_and(|releases| releases <= canceled)
}

/// 用户下限和最近控制回执留在本分片。淘汰用户下限会使过期命令重新生效。
/// 调用方负责在去重、序号分配前校验主节点纪元，并将候选状态与撤单一起原子提交。
/// 本类型支持快照编解码，不自行提交持久文件或提供跨分片协调。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UserControlEpochs {
    /// 非零用户下限条目的最大数量。
    capacity: usize,
    /// 按登记顺序保存用户及其非零纪元下限。
    entries: Vec<(UserId, u64)>,
    /// 每个受控用户至多一条回执，加上一条独立的全局回执。
    receipts: BTreeMap<Option<UserId>, FenceReceipt>,
}

impl UserControlEpochs {
    /// 接管市场时合并已观察到的用户下限，保留目标已有的更高纪元。
    pub(crate) fn inherit_floors(&mut self, source: &Self) -> Result<(), RejectReason> {
        // 迁移途中不得将尚未撤销的订单移入已经确认相应控制操作的分片。
        let compatible = self.receipts.len() == source.receipts.len()
            && self.receipts.iter().all(|(key, r)| {
                source.receipts.get(key).is_some_and(|s| {
                    (r.epoch, r.parent, r.action, r.killed)
                        == (s.epoch, s.parent, s.action, s.killed)
                })
            });
        if !compatible {
            return Err(RejectReason::EngineBusy);
        }
        for &(user, floor) in &source.entries {
            self.advance(user, floor)?;
        }
        Ok(())
    }

    /// 按用户身份排序编码下限，消除登记先后对状态比较的影响。
    pub(crate) fn write_canonical_state(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        let mut sorted = self
            .try_clone_preallocated()
            .map_err(|_| matching_domain::snapshot_wire::SnapshotWireError)?;
        sorted.entries.sort_unstable_by_key(|entry| entry.0);
        sorted.write_snapshot(writer)
    }
    /// 先按登记顺序写入用户下限；已执行过协调控制时再追加各作用域的最近回执。
    pub(crate) fn write_snapshot(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{SnapshotEncoder, SnapshotWireError};
        let mut body = SnapshotEncoder::new(256 * 1024 * 1024);
        body.u64(u64::try_from(self.capacity).map_err(|_| SnapshotWireError)?)?;
        body.u32(u32::try_from(self.entries.len()).map_err(|_| SnapshotWireError)?)?;
        for (user, epoch) in &self.entries {
            body.u64(user.get())?;
            body.u64(*epoch)?;
        }
        // 空扩展保持旧快照与旧摘要逐字节兼容。
        if !self.receipts.is_empty() {
            body.u8(1)?;
            body.u32(u32::try_from(self.receipts.len()).map_err(|_| SnapshotWireError)?)?;
            for (user, r) in &self.receipts {
                body.u8(u8::from(user.is_some()))?;
                if let Some(user) = user {
                    body.u64(user.get())?;
                }
                body.u64(r.epoch)?;
                body.u128(r.parent.get())?;
                body.u8(r.action.wire_code().ok_or(SnapshotWireError)?)?;
                body.u8(u8::from(r.killed))?;
                body.u64(r.seq)?;
                body.u64(r.canceled)?;
                body.u32(r.last_event)?;
            }
        }
        writer.bytes(&body.finish())
    }
    /// 按可信容量恢复用户下限和可选回执，核验作用域、纪元、动作、开关状态及事件范围。
    pub(crate) fn read_snapshot(
        reader: &mut matching_domain::snapshot_wire::SnapshotDecoder<'_>,
        capacity: usize,
    ) -> Result<Self, matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotWireError};
        let bytes = reader.bytes()?;
        let mut body = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
        if usize::try_from(body.u64()?).ok() != Some(capacity) {
            return Err(SnapshotWireError);
        }
        let count = usize::try_from(body.u32()?).map_err(|_| SnapshotWireError)?;
        if count > capacity || count > body.remaining() / 16 {
            return Err(SnapshotWireError);
        }
        let mut state = Self::new(capacity).map_err(|_| SnapshotWireError)?;
        for _ in 0..count {
            let user = UserId::new(body.u64()?);
            let epoch = body.u64()?;
            if epoch == 0 || state.entries.iter().any(|(id, _)| *id == user) {
                return Err(SnapshotWireError);
            }
            state.entries.push((user, epoch));
        }
        if body.remaining() != 0 {
            if body.u8()? != 1 {
                return Err(SnapshotWireError);
            }
            let size = usize::try_from(body.u32()?).map_err(|_| SnapshotWireError)?;
            if size == 0 || size.saturating_sub(1) > count {
                return Err(SnapshotWireError);
            }
            for _ in 0..size {
                let user = match body.u8()? {
                    0 => None,
                    1 => Some(UserId(body.u64()?)),
                    _ => return Err(SnapshotWireError),
                };
                let epoch = body.u64()?;
                let parent = CommandId(body.u128()?);
                let action = UserFenceAction::from_wire(body.u8()?).ok_or(SnapshotWireError)?;
                let killed = match body.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(SnapshotWireError),
                };
                let seq = body.u64()?;
                let canceled = body.u64()?;
                let last_event = body.u32()?;
                if seq == 0
                    || epoch == 0
                    || !control_events_match(action, canceled, last_event)
                    || matches!(action, UserFenceAction::Kill { active, .. } if active != killed)
                    || user
                        .is_some_and(|id| !state.entries.iter().any(|e| e.0 == id && e.1 >= epoch))
                    || state
                        .receipts
                        .insert(
                            user,
                            FenceReceipt {
                                epoch,
                                parent,
                                action,
                                killed,
                                seq,
                                canceled,
                                last_event,
                            },
                        )
                        .is_some()
                {
                    return Err(SnapshotWireError);
                }
            }
        }
        body.finish()?;
        Ok(state)
    }
    /// 在启动时预留有界存储，分配失败由调用方处理。
    pub(crate) fn new(capacity: usize) -> Result<Self, ()> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(capacity).map_err(|_| ())?;
        Ok(Self {
            capacity,
            entries,
            receipts: BTreeMap::new(),
        })
    }

    /// 复制用户下限和各作用域回执；候选用户表保留原有容量。
    pub(crate) fn try_clone_preallocated(&self) -> Result<Self, ()> {
        let mut copy = Self::new(self.capacity)?;
        copy.entries.extend(self.entries.iter().copied());
        copy.receipts.clone_from(&self.receipts);
        Ok(copy)
    }
    /// 当前分片已应用的全局控制纪元。
    pub(crate) fn global_floor(&self) -> u64 {
        self.receipts.get(&None).map_or(0, |r| r.epoch)
    }
    /// 已执行过跨分片控制的状态在恢复时必须重新绑定控制权威。
    pub(crate) fn requires_control_admission(&self) -> bool {
        !self.receipts.is_empty()
    }
    /// 快照不能包含尚未提交的控制回执。
    pub(crate) fn receipts_fit_prefix(&self, seq: u64) -> bool {
        self.receipts.values().all(|r| r.seq <= seq)
    }
    /// 返回作用域的最近一次持久回执。
    pub(crate) fn receipt(&self, user: Option<UserId>) -> Option<FenceReceipt> {
        self.receipts.get(&user).copied()
    }
    /// 在预检成功的候选上记录回执。
    pub(crate) fn record_receipt(&mut self, user: Option<UserId>, receipt: FenceReceipt) {
        self.receipts.insert(user, receipt);
    }
    /// 当前用户或全局是否处于已激活的持久熔断状态。
    pub(crate) fn killed(&self, user: UserId) -> bool {
        [None, Some(user)]
            .into_iter()
            .any(|key| self.receipts.get(&key).is_some_and(|r| r.killed))
    }
    /// 按持久登记顺序枚举非零下限，供停写升级时归并。
    pub(crate) fn floors(&self) -> impl Iterator<Item = (UserId, u64)> + '_ {
        self.entries.iter().copied()
    }
    /// 查找该用户已提交的分片下限，包括历史单机下限。
    pub(crate) fn floor(&self, user: UserId) -> u64 {
        self.entries
            .iter()
            .find(|entry| entry.0 == user)
            .map_or(0, |entry| entry.1)
    }
    /// 未出现的用户下限为零，等于下限的命令有效。
    pub(crate) fn check(&self, user: UserId, epoch: u64) -> Result<(), RejectReason> {
        if epoch < self.floor(user) {
            Err(RejectReason::StaleUserControlEpoch)
        } else {
            Ok(())
        }
    }

    /// 在候选状态更新下限，容量不足时保持原状态。
    pub(crate) fn advance(&mut self, user: UserId, epoch: u64) -> Result<u64, RejectReason> {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.0 == user) {
            entry.1 = entry.1.max(epoch);
            return Ok(entry.1);
        }
        if epoch == 0 {
            return Ok(0);
        }
        if self.entries.len() == self.capacity {
            return Err(RejectReason::EngineBusy);
        }
        self.entries.push((user, epoch));
        Ok(epoch)
    }
}

/// 验证纪元单调性、用户隔离与容量拒绝时的原子性。
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_state_is_independent_of_user_registration_order() {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        let mut first = UserControlEpochs::new(3).unwrap();
        first.advance(UserId(7), 9).unwrap();
        first.advance(UserId(1), 3).unwrap();
        let mut second = UserControlEpochs::new(3).unwrap();
        second.advance(UserId(1), 3).unwrap();
        second.advance(UserId(7), 9).unwrap();
        let mut a = SnapshotEncoder::new(100);
        let mut b = SnapshotEncoder::new(100);
        first.write_canonical_state(&mut a).unwrap();
        second.write_canonical_state(&mut b).unwrap();
        assert_eq!(a.finish(), b.finish());
    }
    #[test]
    fn snapshot_preserves_capacity_fences_and_rejects_duplicate_user() {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
        let mut state = UserControlEpochs::new(3).unwrap();
        state.advance(UserId(7), 9).unwrap();
        state.advance(UserId(1), 3).unwrap();
        let mut w = SnapshotEncoder::new(100);
        state.write_snapshot(&mut w).unwrap();
        let bytes = w.finish();
        let mut r = SnapshotDecoder::new(&bytes, 100).unwrap();
        assert_eq!(UserControlEpochs::read_snapshot(&mut r, 3).unwrap(), state);
        r.finish().unwrap();
        let mut bad = bytes.clone();
        bad[32..40].copy_from_slice(&7_u64.to_be_bytes());
        let mut r = SnapshotDecoder::new(&bad, 100).unwrap();
        assert!(UserControlEpochs::read_snapshot(&mut r, 3).is_err());
        let mut r = SnapshotDecoder::new(&bytes, 100).unwrap();
        assert!(UserControlEpochs::read_snapshot(&mut r, 2).is_err());
        for end in 0..bytes.len() {
            let mut r = SnapshotDecoder::new(&bytes[..end], 100).unwrap();
            assert!(UserControlEpochs::read_snapshot(&mut r, 3).is_err());
        }
    }
    #[test]
    fn fence_is_monotonic_and_isolated() {
        let mut state = UserControlEpochs::new(2).unwrap();
        state.advance(UserId(1), 5).unwrap();
        state.advance(UserId(1), 3).unwrap();
        assert_eq!(
            state.check(UserId(1), 4),
            Err(RejectReason::StaleUserControlEpoch)
        );
        assert_eq!(state.check(UserId(1), 5), Ok(()));
        assert_eq!(state.check(UserId(2), 0), Ok(()));
    }
    /// 直接构造已提交回执的快照字节，验证开关、撤单和释放对应的事件边界。
    #[test]
    fn control_snapshot_rejects_impossible_receipt_counts() {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
        let keep = UserFenceAction::Kill {
            active: true,
            cancel_all: false,
        };
        let clear = UserFenceAction::Kill {
            active: false,
            cancel_all: false,
        };
        let kill = UserFenceAction::Kill {
            active: true,
            cancel_all: true,
        };
        let cancel = UserFenceAction::FenceAndCancelAll;
        for (action, canceled, last, valid) in [
            (keep, 1, 1, false),
            (clear, 0, 0, false),
            (keep, 0, 2, false),
            (keep, 0, 1, true),
            (clear, 0, 1, true),
            (kill, 1, 1, false),
            (kill, 1, 2, true),
            (kill, 1, 3, true),
            (kill, 1, 4, false),
            (cancel, 0, 0, true),
            (cancel, 0, 1, false),
            (cancel, 2, 2, true),
            (cancel, 2, 3, true),
            (cancel, 2, 4, true),
            (cancel, 2, 5, false),
            (cancel, u64::MAX, u32::MAX, false),
        ] {
            let mut body = SnapshotEncoder::new(128);
            body.u64(1).unwrap();
            body.u32(0).unwrap();
            body.u8(1).unwrap();
            body.u32(1).unwrap();
            body.u8(0).unwrap();
            body.u64(1).unwrap();
            body.u128(1).unwrap();
            body.u8(action.wire_code().unwrap()).unwrap();
            body.u8(u8::from(matches!(
                action,
                UserFenceAction::Kill { active: true, .. }
            )))
            .unwrap();
            body.u64(1).unwrap();
            body.u64(canceled).unwrap();
            body.u32(last).unwrap();
            let mut bytes = SnapshotEncoder::new(128);
            bytes.bytes(&body.finish()).unwrap();
            let raw = bytes.finish();
            let mut reader = SnapshotDecoder::new(&raw, 128).unwrap();
            assert_eq!(
                UserControlEpochs::read_snapshot(&mut reader, 1).is_ok(),
                valid,
                "{action:?}, {canceled}, {last}"
            );
        }
    }

    #[test]
    fn full_state_rejects_new_user_without_mutation() {
        let mut state = UserControlEpochs::new(1).unwrap();
        state.advance(UserId(1), 2).unwrap();
        let before = state.clone();
        assert_eq!(state.advance(UserId(2), 1), Err(RejectReason::EngineBusy));
        assert_eq!(state, before);
        assert_eq!(state.advance(UserId(1), 3), Ok(3));
    }
}
