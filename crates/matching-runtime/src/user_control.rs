//! 单分片内有界的用户控制纪元状态。

use matching_domain::{RejectReason, UserId};

/// 仅保存非零下限；不得淘汰，否则旧命令可能重新生效。
/// 调用方负责在去重、序号分配前校验主节点纪元，并将候选状态与撤单一起原子提交。
/// 本类型支持快照编解码，不自行提交持久文件或提供跨分片协调。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UserControlEpochs {
    /// 非零用户下限条目的最大数量。
    capacity: usize,
    /// 按登记顺序保存用户及其非零纪元下限。
    entries: Vec<(UserId, u64)>,
}

impl UserControlEpochs {
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
    /// 按登记顺序保存容量和非零用户纪元。
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
        writer.bytes(&body.finish())
    }
    /// 按可信容量恢复，拒绝零纪元及重复用户。
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
        body.finish()?;
        Ok(state)
    }
    /// 在启动时预留有界存储，分配失败由调用方处理。
    pub(crate) fn new(capacity: usize) -> Result<Self, ()> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(capacity).map_err(|_| ())?;
        Ok(Self { capacity, entries })
    }

    /// 复制候选下限并预留原有容量；分配失败时不修改真实状态。
    pub(crate) fn try_clone_preallocated(&self) -> Result<Self, ()> {
        let mut copy = Self::new(self.capacity)?;
        copy.entries.extend(self.entries.iter().copied());
        Ok(copy)
    }
    /// 未出现的用户下限为零，等于下限的命令有效。
    pub(crate) fn check(&self, user: UserId, epoch: u64) -> Result<(), RejectReason> {
        let minimum = self
            .entries
            .iter()
            .find(|entry| entry.0 == user)
            .map_or(0, |entry| entry.1);
        if epoch < minimum {
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
