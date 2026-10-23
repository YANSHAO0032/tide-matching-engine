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
}
