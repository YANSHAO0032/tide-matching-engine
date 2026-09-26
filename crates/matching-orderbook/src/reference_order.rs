use matching_domain::{LimitGtcOrder, Qty, QueuePriority, RejectReason};

/// 订单簿使用的最小运行态订单
#[derive(Debug)]
pub struct ReferenceOrder {
    original: LimitGtcOrder,
    remaining: u64,
    priority: QueuePriority,
}

impl ReferenceOrder {
    //真实挂单统一通过 ReferenceOrderBook::rest() 分配 priority
    pub(crate) fn new(original: LimitGtcOrder, priority: QueuePriority) -> Self {
        let remaining = original.qty.get();
        Self {
            original,
            remaining,
            priority,
        }
    }

    //只读访问原始订单
    pub fn original_order(&self) -> &LimitGtcOrder {
        &self.original
    }

    // 返回剩余量
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    // 优先级计数器
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
    use matching_domain::{
        LimitGtcOrder, OrderId, Price, Qty, QueuePriority, RejectReason, Side, UserId,
    };

    use super::ReferenceOrder;

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
