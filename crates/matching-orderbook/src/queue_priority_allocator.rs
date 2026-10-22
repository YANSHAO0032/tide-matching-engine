use matching_domain::{QueuePriority, RejectReason};

/// QueuePriority 分配策略。
///
/// 策略在分配器创建后保持不可变。
///
/// # 边界语义
///
/// `effective_ceiling` 表示第一个禁止分配的 QueuePriority：
///
/// ```text
/// next < effective_ceiling  -> 可以分配
/// next >= effective_ceiling -> PrioritySpaceExhaustion
/// ```
///
/// 实际生效边界取：
///
/// ```text
/// min(real_ceiling, synthetic_ceiling)
/// ```
///
/// 其中：
///
/// ```text
/// real_ceiling = u128::MAX - guard
/// ```
///
/// synthetic_ceiling 主要用于测试或受控环境模拟优先级空间耗尽。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuePriorityPolicy {
    /// 真实 `u128` 空间尾部保留的不可分配安全区，构造时必须非零且有效。
    guard: u128,
    /// 可选的更早耗尽边界，仅能收紧真实上限，不能扩大可分配空间。
    synthetic_ceiling: Option<u128>,
}

impl QueuePriorityPolicy {
    /// 创建生产策略。
    ///
    /// `guard` 表示 u128 最大空间尾部必须保留的安全区。
    ///
    /// 例如：
    ///
    /// ```text
    /// guard = 1024
    /// real_ceiling = u128::MAX - 1024
    /// ```
    ///
    /// `real_ceiling` 本身禁止分配。
    pub fn try_new(guard: u128) -> Result<Self, QueuePriorityInitError> {
        Self::try_with_synthetic_ceiling(guard, None)
    }

    /// 创建带测试用上限的策略。
    ///
    /// 测试用上限不允许扩大真实安全空间，只允许提前耗尽：
    ///
    /// ```text
    /// effective_ceiling =
    ///     min(u128::MAX - guard, synthetic_ceiling)
    /// ```
    pub fn try_with_synthetic_ceiling(
        guard: u128,
        synthetic_ceiling: Option<u128>,
    ) -> Result<Self, QueuePriorityInitError> {
        if guard == 0 {
            return Err(QueuePriorityInitError::ZeroGuard);
        }

        let real_ceiling = u128::MAX
            .checked_sub(guard)
            .ok_or(QueuePriorityInitError::InvalidGuard)?;

        // 至少必须存在一个可分配优先级。
        //
        // 上限 == 0 表示连优先级=0 都不能分配，
        // 这种策略没有实际意义，直接拒绝。
        if real_ceiling == 0 {
            return Err(QueuePriorityInitError::EmptyPrioritySpace);
        }

        if matches!(synthetic_ceiling, Some(0)) {
            return Err(QueuePriorityInitError::EmptyPrioritySpace);
        }

        Ok(Self {
            guard,
            synthetic_ceiling,
        })
    }

    /// 返回不可分配尾部的真实预留边界配置。
    pub fn guard(&self) -> u128 {
        self.guard
    }

    /// 返回可选测试用上限；`None` 表示只使用真实边界。
    pub fn synthetic_ceiling(&self) -> Option<u128> {
        self.synthetic_ceiling
    }

    /// 真实高水位上限。
    ///
    /// 该值本身禁止分配。
    pub fn real_ceiling(&self) -> u128 {
        // 构造阶段已经保证预留边界合法。
        u128::MAX - self.guard
    }

    /// 实际生效的高水位上限。
    ///
    /// 真实边界和测试边界中，取较早到达者。
    pub fn effective_ceiling(&self) -> u128 {
        let real = self.real_ceiling();

        match self.synthetic_ceiling {
            Some(synthetic) => real.min(synthetic),
            None => real,
        }
    }
}

/// QueuePriorityAllocator 构造 / 恢复错误。
///
/// 这些错误属于配置或持久化状态错误，不属于正常撮合拒绝，
/// 因此不复用 RejectReason。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePriorityInitError {
    /// 预留边界不能为 0。
    ZeroGuard,

    /// 预留边界本身非法。
    InvalidGuard,

    /// 策略导致不存在任何可分配优先级。
    EmptyPrioritySpace,

    /// 从快照恢复时，`next` 已经超过有效上限。
    ///
    /// 注意：
    ///
    /// ```text
    /// next == effective_ceiling
    /// ```
    ///
    /// 是合法状态，表示分配器已耗尽；
    /// 恢复成功，但下一次分配会返回
    /// PrioritySpaceExhaustion。
    NextBeyondCeiling,
}

/// 市场内 QueuePriority 分配器。
///
/// 每个市场持有独立分配器。
///
/// 分配器仅维护：
///
/// - 不可变分配策略；
/// - 下一次准备分配的 QueuePriority。
///
/// 它不感知：
///
/// - 订单 `Order`
/// - 方向 `Side`
/// - 价格 `Price`
/// - 价档 `PriceLevel`
/// - 节点存储 `OrderArena`
///
/// 因此不同市场之间的优先级空间相互独立。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePriorityAllocator {
    /// 创建后不可变的真实/测试用高水位策略。
    policy: QueuePriorityPolicy,
    /// 下一次成功分配应返回的市场内优先级，不是最近已分配值。
    next: u128,
}

impl QueuePriorityAllocator {
    /// 从零创建新的市场内分配器。
    ///
    /// 策略已在构造时验证；此函数不共享或重置任何其他市场的计数器。
    pub fn new(policy: QueuePriorityPolicy) -> Self {
        Self { policy, next: 0 }
    }

    /// 从持久化快照 / 市场迁移状态恢复分配器。
    ///
    /// `next_queue_priority` 的语义是：
    ///
    /// > 下一次成功分配应该返回的优先级。
    ///
    /// 因此：
    ///
    /// ```text
    /// snapshot.next_queue_priority = 42
    ///
    /// 恢复
    ///   -> 首次分配返回值 = 42
    ///   -> next = 43
    /// ```
    ///
    /// 不应在恢复阶段自动 +1。
    pub fn try_restore(
        policy: QueuePriorityPolicy,
        next_queue_priority: u128,
    ) -> Result<Self, QueuePriorityInitError> {
        if next_queue_priority > policy.effective_ceiling() {
            return Err(QueuePriorityInitError::NextBeyondCeiling);
        }

        Ok(Self {
            policy,
            next: next_queue_priority,
        })
    }

    /// 分配一个新的 QueuePriority。
    ///
    /// 成功：
    ///
    /// ```text
    /// result = next
    /// next   = next + 1
    /// ```
    ///
    /// 耗尽：
    ///
    /// ```text
    /// next >= effective_ceiling
    /// ```
    ///
    /// 返回既有的：
    ///
    /// ```text
    /// `RejectReason::PrioritySpaceExhaustion`
    /// ```
    ///
    /// 并且失败不得修改分配器的任何状态。
    ///
    /// 分配器本身不执行市场停市、告警、订单挂单或持久化；其所有者
    /// 必须在拒绝后按上层协议处理这些行为。
    pub fn allocate(&mut self) -> Result<QueuePriority, RejectReason> {
        let ceiling = self.policy.effective_ceiling();

        // 第一阶段：只读预检，失败时立即停止处理。
        //
        // 失败必须发生在任何状态写入之前。
        if self.next >= ceiling {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }

        let allocated = self.next;

        // 第二阶段：提前计算新状态。
        //
        // 由于已分配 < 上限，
        // 且有效上限最大也小于 `u128::MAX`，
        // 正常情况下这里必定可以 +1。
        let new_next = allocated
            .checked_add(1)
            .expect("QueuePriority below ceiling must be incrementable");

        // 第三阶段：所有可能失败的检查全部完成后才提交。
        self.next = new_next;

        Ok(QueuePriority::new(allocated))
    }

    /// 返回下一次成功分配将使用的原始数值。
    ///
    /// 此值适合由未来持久化所有者保存和恢复；读取它不推进分配器。
    pub fn next(&self) -> u128 {
        self.next
    }

    /// 借用创建时的不可变策略，不允许调用方改变当前边界。
    pub fn policy(&self) -> &QueuePriorityPolicy {
        &self.policy
    }

    /// 判断下一次分配是否会因达到有效上限而被拒绝。
    ///
    /// 该查询不执行分配，也不等同于上层市场的停市状态。
    pub fn is_exhausted(&self) -> bool {
        self.next >= self.policy.effective_ceiling()
    }
}

#[cfg(test)]
mod tests {
    //! 策略、恢复与高水位故障时停止处理边界的值对象验证。

    use super::*;

    const DEFAULT_GUARD: u128 = 1024;

    /// 构造测试使用的正常生产预留边界，不使用测试用上限。
    fn production_policy() -> QueuePriorityPolicy {
        QueuePriorityPolicy::try_new(DEFAULT_GUARD).unwrap()
    }

    #[test]
    fn allocates_monotonically_from_zero() {
        let policy = production_policy();
        let mut allocator = QueuePriorityAllocator::new(policy);

        assert_eq!(allocator.next(), 0);
        assert!(!allocator.is_exhausted());

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(allocator.next(), 1);

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(1)));

        assert_eq!(allocator.next(), 2);

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(2)));

        assert_eq!(allocator.next(), 3);

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(3)));

        assert_eq!(allocator.next(), 4);
    }

    #[test]
    fn real_ceiling_rejects_at_guard_boundary() {
        let policy = production_policy();

        let real_ceiling = u128::MAX - DEFAULT_GUARD;

        assert_eq!(policy.real_ceiling(), real_ceiling);
        assert_eq!(policy.effective_ceiling(), real_ceiling);

        // 直接从真实边界前一位恢复，避免循环。
        let mut allocator = QueuePriorityAllocator::try_restore(policy, real_ceiling - 1).unwrap();

        // 最后一个合法优先级。
        assert_eq!(
            allocator.allocate(),
            Ok(QueuePriority::new(real_ceiling - 1))
        );

        assert_eq!(allocator.next(), real_ceiling);
        assert!(allocator.is_exhausted());

        let before = allocator.clone();

        // real_ceiling 本身禁止分配。
        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        // 完整状态不变。
        assert_eq!(allocator, before);
    }

    #[test]
    fn earlier_synthetic_ceiling_wins_over_real_ceiling() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(3)).unwrap();

        assert_eq!(policy.real_ceiling(), u128::MAX - DEFAULT_GUARD);

        assert_eq!(policy.synthetic_ceiling(), Some(3));

        // 测试用=3 明显比真实上限更早。
        assert_eq!(policy.effective_ceiling(), 3);

        let mut allocator = QueuePriorityAllocator::new(policy);

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(1)));

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(2)));

        assert_eq!(allocator.next(), 3);

        let before = allocator.clone();

        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(allocator, before);
    }

    #[test]
    fn earlier_real_ceiling_wins_over_synthetic_ceiling() {
        let real_ceiling = u128::MAX - DEFAULT_GUARD;

        // 测试用上限比真实边界晚，
        // 不能突破真实预留边界的保护区。
        let synthetic_ceiling = real_ceiling + 10;

        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(synthetic_ceiling))
                .unwrap();

        assert_eq!(policy.real_ceiling(), real_ceiling);
        assert_eq!(policy.synthetic_ceiling(), Some(synthetic_ceiling));

        // 应取更早的真实边界。
        assert_eq!(policy.effective_ceiling(), real_ceiling);

        let mut allocator = QueuePriorityAllocator::try_restore(policy, real_ceiling - 1).unwrap();

        assert_eq!(
            allocator.allocate(),
            Ok(QueuePriority::new(real_ceiling - 1))
        );

        assert_eq!(allocator.next(), real_ceiling);

        let before = allocator.clone();

        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(allocator, before);
    }

    #[test]
    fn rejects_invalid_policy() {
        // 预留边界=0 不提供安全区。
        assert_eq!(
            QueuePriorityPolicy::try_new(0),
            Err(QueuePriorityInitError::ZeroGuard)
        );

        // 预留边界=`u128::MAX` 导致 real_ceiling=0，
        // 不存在任何可分配优先级。
        assert_eq!(
            QueuePriorityPolicy::try_new(u128::MAX),
            Err(QueuePriorityInitError::EmptyPrioritySpace)
        );

        // 测试用=0 同样不存在可分配空间。
        assert_eq!(
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(0),),
            Err(QueuePriorityInitError::EmptyPrioritySpace)
        );
    }

    #[test]
    fn synthetic_ceiling_equal_to_real_ceiling_is_valid() {
        let real_ceiling = u128::MAX - DEFAULT_GUARD;

        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(real_ceiling))
                .unwrap();

        assert_eq!(policy.effective_ceiling(), real_ceiling);
    }

    #[test]
    fn restored_nonzero_next_is_first_allocated_priority() {
        let policy = production_policy();

        let mut allocator = QueuePriorityAllocator::try_restore(policy, 42).unwrap();

        assert_eq!(allocator.next(), 42);

        // 恢复后的第一个分配必须正好是 `snapshot.next`，
        // 不能跳成 43。
        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(42)));

        assert_eq!(allocator.next(), 43);

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(43)));

        assert_eq!(allocator.next(), 44);
    }

    #[test]
    fn restore_at_effective_ceiling_is_valid_but_exhausted() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(10)).unwrap();

        // `next` == 上限是合法持久化状态：
        // 表示保存快照时分配器已经耗尽。
        let mut allocator = QueuePriorityAllocator::try_restore(policy, 10).unwrap();

        assert_eq!(allocator.next(), 10);
        assert!(allocator.is_exhausted());

        let before = allocator.clone();

        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(allocator, before);
    }

    #[test]
    fn restore_rejects_next_beyond_effective_ceiling() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(10)).unwrap();

        assert_eq!(
            QueuePriorityAllocator::try_restore(policy, 11,),
            Err(QueuePriorityInitError::NextBeyondCeiling)
        );
    }

    #[test]
    fn restore_uses_earlier_synthetic_ceiling_for_validation() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(10)).unwrap();

        // 虽然真实 u128 空间远未耗尽，
        // 但测试用上限更早，因此 11 非法。
        assert_eq!(
            QueuePriorityAllocator::try_restore(policy, 11,),
            Err(QueuePriorityInitError::NextBeyondCeiling)
        );

        assert!(QueuePriorityAllocator::try_restore(policy, 10,).is_ok());
    }

    #[test]
    fn restore_uses_earlier_real_ceiling_for_validation() {
        let real_ceiling = u128::MAX - DEFAULT_GUARD;

        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(real_ceiling + 10))
                .unwrap();

        // 测试用虽然允许继续增长，
        // 但真实预留边界更早。
        assert_eq!(
            QueuePriorityAllocator::try_restore(policy, real_ceiling + 1,),
            Err(QueuePriorityInitError::NextBeyondCeiling)
        );

        assert!(QueuePriorityAllocator::try_restore(policy, real_ceiling,).is_ok());
    }

    #[test]
    fn exhaustion_preserves_complete_allocator_state() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(7)).unwrap();

        let mut allocator = QueuePriorityAllocator::try_restore(policy, 7).unwrap();

        let before = allocator.clone();

        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        // 不只是 `next`：
        // 策略 + `next` 整个分配器都必须完全不变。
        assert_eq!(allocator, before);

        // 重试同样必须是稳定、确定性的失败。
        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(allocator, before);
    }

    #[test]
    fn allocation_reaching_ceiling_commits_only_successful_increment() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(3)).unwrap();

        let mut allocator = QueuePriorityAllocator::try_restore(policy, 2).unwrap();

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(2)));

        // 成功分配 2 后，`next` 正好进入已耗尽状态。
        assert_eq!(allocator.next(), 3);
        assert!(allocator.is_exhausted());

        let before = allocator.clone();

        assert_eq!(
            allocator.allocate(),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(allocator, before);
    }

    #[test]
    fn independent_market_allocators_do_not_affect_each_other() {
        let policy = production_policy();

        let mut btc = QueuePriorityAllocator::new(policy);

        let mut eth = QueuePriorityAllocator::new(policy);

        assert_eq!(btc.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(btc.allocate(), Ok(QueuePriority::new(1)));

        // ETH 拥有独立市场内优先级空间。
        assert_eq!(eth.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(btc.allocate(), Ok(QueuePriority::new(2)));

        assert_eq!(eth.allocate(), Ok(QueuePriority::new(1)));

        assert_eq!(btc.next(), 3);
        assert_eq!(eth.next(), 2);
    }

    #[test]
    fn exhausting_one_allocator_does_not_affect_another() {
        let policy =
            QueuePriorityPolicy::try_with_synthetic_ceiling(DEFAULT_GUARD, Some(2)).unwrap();

        let mut first = QueuePriorityAllocator::new(policy);

        let mut second = QueuePriorityAllocator::new(policy);

        assert_eq!(first.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(first.allocate(), Ok(QueuePriority::new(1)));

        let first_before = first.clone();

        assert_eq!(first.allocate(), Err(RejectReason::PrioritySpaceExhaustion));

        assert_eq!(first, first_before);

        // 第一个耗尽不会推进第二个。
        assert_eq!(second.next(), 0);

        assert_eq!(second.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(second.next(), 1);
    }

    #[test]
    fn policy_is_preserved_during_allocation() {
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(2048, Some(100)).unwrap();

        let mut allocator = QueuePriorityAllocator::new(policy);

        let before_policy = *allocator.policy();

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(0)));

        assert_eq!(allocator.allocate(), Ok(QueuePriority::new(1)));

        assert_eq!(*allocator.policy(), before_policy);
    }
}
