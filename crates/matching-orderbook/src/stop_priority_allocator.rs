use crate::QueuePriorityPolicy;
use matching_domain::{RejectReason, StopPriority};

/// 从持久状态恢复 StopPriority 计数器时的配置错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopPriorityInitError {
    /// 快照中的下一序号已经超过当前策略允许的边界。
    NextBeyondCeiling,
}

/// 未触发 Stop 的市场本地优先级分配器。
///
/// 它复用与 QueuePriority 相同的高水位策略，但维护独立计数器，不能为普通挂单分配优先级。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopPriorityAllocator {
    /// 创建后不可变的高水位策略。
    policy: QueuePriorityPolicy,
    /// 下一次成功分配应返回的 StopPriority 原始数值。
    next: u128,
}

impl StopPriorityAllocator {
    /// 从零创建独立 StopPriority 计数器。
    pub fn new(policy: QueuePriorityPolicy) -> Self {
        Self { policy, next: 0 }
    }

    /// 编码优先级策略和下一止损订单优先级。
    pub fn write_snapshot(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        let mut body = matching_domain::snapshot_wire::SnapshotEncoder::new(64);
        self.policy.write_snapshot(&mut body)?;
        body.u128(self.next)?;
        writer.bytes(&body.finish())
    }
    /// 恢复策略和下一止损优先级，并拒绝超出安全边界的值。
    pub fn read_snapshot(
        reader: &mut matching_domain::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotWireError};
        let mut body = SnapshotDecoder::new(reader.bytes()?, 64)?;
        let policy = QueuePriorityPolicy::read_snapshot(&mut body)?;
        let next = body.u128()?;
        body.finish()?;
        Self::try_restore(policy, next).map_err(|_| SnapshotWireError)
    }
    /// 返回此分配器使用的不可变高水位策略。
    pub fn policy(&self) -> &QueuePriorityPolicy {
        &self.policy
    }

    /// 从快照或迁移状态恢复，不自动推进计数器。
    pub fn try_restore(
        policy: QueuePriorityPolicy,
        next: u128,
    ) -> Result<Self, StopPriorityInitError> {
        if next > policy.effective_ceiling() {
            return Err(StopPriorityInitError::NextBeyondCeiling);
        }
        Ok(Self { policy, next })
    }

    /// 预检下一次分配是否可执行，不修改计数器。
    pub fn preflight_allocate(&self) -> Result<(), RejectReason> {
        if self.next >= self.policy.effective_ceiling() {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        Ok(())
    }

    /// 分配一项 StopPriority；失败时计数器保持不变。
    pub fn allocate(&mut self) -> Result<StopPriority, RejectReason> {
        self.preflight_allocate()?;
        let allocated = self.next;
        self.next = allocated
            .checked_add(1)
            .expect("StopPriority below effective ceiling must increment");
        Ok(StopPriority::new(allocated))
    }

    /// 返回下一次成功分配将使用的原始值。
    pub fn next(&self) -> u128 {
        self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustion_does_not_advance_stop_priority() {
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(2)).unwrap();
        let mut allocator = StopPriorityAllocator::new(policy);
        assert_eq!(allocator.allocate(), Ok(StopPriority::new(0)));
        assert_eq!(allocator.allocate(), Ok(StopPriority::new(1)));
        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(allocator.next(), 2);
    }

    #[test]
    fn restore_at_ceiling_is_valid_but_cannot_allocate() {
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(3)).unwrap();
        let mut allocator = StopPriorityAllocator::try_restore(policy, 3).unwrap();
        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(allocator.next(), 3);
    }

    #[test]
    fn snapshot_restores_stop_allocator_without_advancing() {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(3)).unwrap();
        let allocator = StopPriorityAllocator::try_restore(policy, 3).unwrap();
        let mut writer = SnapshotEncoder::new(128);
        allocator.write_snapshot(&mut writer).unwrap();
        let bytes = writer.finish();
        let mut reader = SnapshotDecoder::new(&bytes, 128).unwrap();
        let mut restored = StopPriorityAllocator::read_snapshot(&mut reader).unwrap();
        reader.finish().unwrap();
        assert_eq!(restored, allocator);
        assert_eq!(
            restored.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
    }
}
