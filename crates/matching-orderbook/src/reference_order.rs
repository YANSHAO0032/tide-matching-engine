use matching_domain::{LimitGtcOrder, Qty, QueuePriority, RejectReason};

/// 参考订单簿使用的最小挂单订单运行态。
///
/// 此对象保存剩余量与市场内优先级。它不维护生产簿的侵入式先进先出链接；
/// 参考模型的同价先进先出由源 `Vec` 的稳定顺序表达。
#[derive(Clone, Debug)]
pub struct ReferenceOrder {
    /// 不随成交扣减改变的原始订单载荷。
    original: LimitGtcOrder,
    /// 当前可见片的剩余量，可以在移除或补量前短暂为零。
    remaining: u64,
    /// 当前尚未展示的冰山隐藏基础数量。
    hidden_remaining: u64,
    /// 冰山订单的可选单次展示峰值。
    peak_size: Option<Qty>,
    /// 实际挂单或冰山补量时分配的市场本地队列优先级。
    priority: QueuePriority,
}

impl ReferenceOrder {
    /// 用已分配的优先级构造挂单运行态。
    ///
    /// 该库内私有构造器供参考簿和测试数据使用；实际挂单必须经
    /// [`crate::ReferenceOrderBook::rest`] 完成重复标识符与高水位校验。
    pub(crate) fn new(original: LimitGtcOrder, priority: QueuePriority) -> Self {
        let remaining = original.qty.get();
        Self {
            original,
            remaining,
            hidden_remaining: 0,
            peak_size: None,
            priority,
        }
    }

    /// 根据已验证的冰山余量构造运行态，同时保留原始输入。
    pub(crate) fn iceberg(
        original: LimitGtcOrder,
        remaining: u64,
        peak: Qty,
        priority: QueuePriority,
    ) -> Self {
        let visible = remaining.min(peak.get());
        Self {
            original,
            remaining: visible,
            hidden_remaining: remaining
                .checked_sub(visible)
                .expect("visible bounded by total"),
            peak_size: Some(peak),
            priority,
        }
    }
    /// 公开深度只统计当前可见片的数量。
    pub fn visible_remaining(&self) -> u64 {
        self.remaining
    }
    /// 尚未公开、等待后续补量的隐藏数量。
    pub fn hidden_remaining(&self) -> u64 {
        self.hidden_remaining
    }
    /// 包含全部可见片和隐藏片的剩余基础资产数量。
    pub fn total_remaining(&self) -> u64 {
        self.remaining
            .checked_add(self.hidden_remaining)
            .expect("reference total invariant")
    }
    /// 配置的显示上限；普通订单没有该上限。
    pub fn peak_size(&self) -> Option<Qty> {
        self.peak_size
    }
    /// 调用方须预先检查优先级分配和同价档数量聚合。
    pub(crate) fn refresh(&mut self, priority: QueuePriority) -> Qty {
        let visible = self
            .hidden_remaining
            .min(self.peak_size.expect("iceberg peak").get());
        self.hidden_remaining = self
            .hidden_remaining
            .checked_sub(visible)
            .expect("refresh hidden underflow");
        self.remaining = visible;
        self.priority = priority;
        Qty::try_new(visible).expect("positive refresh")
    }

    /// 借用不可变的原始订单载荷。
    pub fn original_order(&self) -> &LimitGtcOrder {
        &self.original
    }

    /// 返回当前可见剩余量；订单总剩余量见 `total_remaining`。
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// 返回该订单获得的市场本地队列优先级。
    pub fn priority(&self) -> QueuePriority {
        self.priority
    }

    /// 扣减一次成交数量。
    ///
    /// 超量扣减返回 ArithmeticOverflow，
    /// 且不会修改原始订单或当前剩余量。
    pub fn apply_fill(&mut self, qty: Qty) -> Result<(), RejectReason> {
        let next_remaining = self
            .remaining
            .checked_sub(qty.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        // checked_sub 成功后，才提交新的运行态
        self.remaining = next_remaining;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 验证参考挂单订单的剩余量、优先级和失败原子性。
    use matching_domain::{
        LimitGtcOrder, OrderId, Price, Qty, QueuePriority, RejectReason, Side, UserId,
    };

    use super::ReferenceOrder;

    /// 构造合法的买入订单测试数据。
    fn make_order(qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    #[test]
    fn initializes_remaining_from_original_qty() {
        let original = make_order(10);
        let order = ReferenceOrder::new(original.clone(), QueuePriority::new(0));

        assert_eq!(order.original_order(), &original);
        assert_eq!(order.remaining(), 10);
    }

    #[test]
    fn partial_fill_reduces_remaining() {
        let mut order = ReferenceOrder::new(make_order(10), QueuePriority::new(0));

        assert_eq!(order.apply_fill(Qty::try_new(3).unwrap()), Ok(()));
        assert_eq!(order.remaining(), 7);
    }

    #[test]
    fn full_fill_allows_zero_remaining() {
        let mut order = ReferenceOrder::new(make_order(10), QueuePriority::new(0));

        assert_eq!(order.apply_fill(Qty::try_new(10).unwrap()), Ok(()));
        assert_eq!(order.remaining(), 0);
        assert_eq!(order.original_order().qty.get(), 10);
    }

    #[test]
    fn excessive_fill_preserves_entire_state() {
        let original = make_order(10);
        let mut order = ReferenceOrder::new(original.clone(), QueuePriority::new(0));

        let result = order.apply_fill(Qty::try_new(11).unwrap());

        assert_eq!(result, Err(RejectReason::ArithmeticOverflow));
        assert_eq!(order.remaining(), 10);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn fill_after_exhaustion_fails_without_mutation() {
        let original = make_order(10);
        let mut order = ReferenceOrder::new(original.clone(), QueuePriority::new(0));

        assert_eq!(order.apply_fill(Qty::try_new(10).unwrap()), Ok(()));

        let result = order.apply_fill(Qty::try_new(1).unwrap());

        assert_eq!(result, Err(RejectReason::ArithmeticOverflow));
        assert_eq!(order.remaining(), 0);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn supports_u64_max_boundary() {
        let original = make_order(u64::MAX);
        let mut order = ReferenceOrder::new(original.clone(), QueuePriority::new(0));

        assert_eq!(order.remaining(), u64::MAX);

        assert_eq!(order.apply_fill(Qty::try_new(1).unwrap()), Ok(()));
        assert_eq!(order.remaining(), u64::MAX - 1);

        assert_eq!(
            order.apply_fill(Qty::try_new(u64::MAX - 1).unwrap()),
            Ok(())
        );
        assert_eq!(order.remaining(), 0);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn consecutive_fills_preserve_original_quantity() {
        let original = make_order(10);
        let mut order = ReferenceOrder::new(original.clone(), QueuePriority::new(0));

        for (fill, expected_remaining) in [(3, 7), (2, 5), (4, 1), (1, 0)] {
            assert_eq!(order.apply_fill(Qty::try_new(fill).unwrap()), Ok(()));
            assert_eq!(order.remaining(), expected_remaining);
            assert_eq!(order.original_order(), &original);
            assert_eq!(order.original_order().qty.get(), 10);
        }
    }
}
