//! 单分片内有界的用户控制纪元状态。

use matching_domain::{RejectReason, UserId};

/// 仅保存非零下限；不得淘汰，否则旧命令可能重新生效。
/// 调用方负责在去重、序号分配前校验主节点纪元，并将候选状态与撤单一起原子提交。
/// 本类型不提供跨分片协调或持久化。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UserControlEpochs {
    /// 非零用户下限条目的最大数量。
    capacity: usize,
    /// 按登记顺序保存用户及其非零纪元下限。
    entries: Vec<(UserId, u64)>,
}

impl UserControlEpochs {
    /// 在启动时预留有界存储，分配失败由调用方处理。
    pub(crate) fn new(capacity: usize) -> Result<Self, ()> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(capacity).map_err(|_| ())?;
        Ok(Self { capacity, entries })
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
