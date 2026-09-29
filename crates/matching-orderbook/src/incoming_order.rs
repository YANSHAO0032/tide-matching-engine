use matching_domain::{LimitGtcOrder, Qty, RejectReason};

/// 正在作为 taker 处理的最小运行态订单。
///
/// 原始订单不可变；`remaining` 只记录本次进入撮合流程后尚未成交的数量，且不会自动 rest。
#[derive(Debug)]
pub struct IncomingOrder {
    /// 调用方提交的不可变订单载荷。
    original: LimitGtcOrder,
    /// 当前尚未成交的 lots，可以在完全成交后为零。
    remaining: u64,
}

impl IncomingOrder {
    /// 从原始订单初始化全部剩余量的 incoming 运行态。
    pub fn new(original: LimitGtcOrder) -> Self {
        let remaining = original.qty.get();
        Self {
            original,
            remaining,
        }
    }

    /// 借用不可变的原始订单载荷。
    pub fn original_order(&self) -> &LimitGtcOrder {
        &self.original
    }

    /// 返回当前尚未成交的 lots；零表示此 incoming 已耗尽。
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// 扣减一次成交数量。
    ///
    /// 使用 checked_sub 保证扣减失败时不修改运行态。
    /// 原始订单在整个生命周期内保持不变。
    pub fn apply_fill(&mut self, fill_qty: Qty) -> Result<(), RejectReason> {
        let new_remaining = self
            .remaining
            .checked_sub(fill_qty.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        self.remaining = new_remaining;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 验证 incoming 初始化保留原始订单和初始数量。
    use super::*;
    use matching_domain::{OrderId, Price, Qty, Side, UserId};

    /// 构造合法的买入订单 fixture。
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
    fn initializes_from_original_order() {
        let original = make_order(10);
        let incoming = IncomingOrder::new(original.clone());

        assert_eq!(incoming.original_order(), &original);
        assert_eq!(incoming.remaining(), 10);
        assert_eq!(incoming.original_order().qty.get(), 10);
    }

    #[test]
    fn preserves_maximum_original_quantity() {
        let original = make_order(u64::MAX);
        let incoming = IncomingOrder::new(original.clone());

        assert_eq!(incoming.original_order(), &original);
        assert_eq!(incoming.remaining(), u64::MAX);
    }
}

#[cfg(test)]
mod fill_tests {
    //! 验证成交扣减的原子性与原始订单不可变性。
    use super::*;
    use matching_domain::{OrderId, Price, Side, UserId};

    /// 构造严格正的测试数量。
    fn qty(value: u64) -> Qty {
        Qty::try_new(value).expect("test quantity must be positive")
    }

    /// 构造可直接覆盖 remaining 边界的卖出 incoming fixture。
    fn make_incoming(qty: u64) -> IncomingOrder {
        IncomingOrder {
            original: LimitGtcOrder {
                order_id: OrderId::new(11),
                user_id: UserId::new(11u64),
                side: Side::Sell,
                price: Price::try_new(110).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            },
            remaining: qty,
        }
    }

    #[test]
    fn partial_fill() {
        let mut order = make_incoming(10);
        let original = order.original_order().clone();

        assert_eq!(order.apply_fill(qty(3)), Ok(()));
        assert_eq!(order.remaining(), 7);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn full_fill() {
        let mut order = make_incoming(10);
        let original = order.original_order().clone();

        assert_eq!(order.apply_fill(qty(10)), Ok(()));
        assert_eq!(order.remaining(), 0);

        assert_eq!(order.original_order().qty, qty(10));
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn excessive_fill_is_atomic() {
        let mut order = make_incoming(10);
        let original = order.original_order().clone();

        assert_eq!(
            order.apply_fill(qty(11)),
            Err(RejectReason::ArithmeticOverflow)
        );

        assert_eq!(order.remaining(), 10);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn exhausted_fill_is_atomic() {
        let mut order = make_incoming(10);
        let original = order.original_order().clone();

        assert_eq!(order.apply_fill(qty(10)), Ok(()));
        assert_eq!(order.remaining(), 0);

        assert_eq!(
            order.apply_fill(qty(1)),
            Err(RejectReason::ArithmeticOverflow)
        );

        assert_eq!(order.remaining(), 0);
        assert_eq!(order.original_order(), &original);
    }

    #[test]
    fn max_qty_incremental_fill() {
        let mut order = make_incoming(u64::MAX);
        let original = order.original_order().clone();

        assert_eq!(order.apply_fill(qty(1)), Ok(()));
        assert_eq!(order.remaining(), u64::MAX - 1);

        assert_eq!(order.apply_fill(qty(u64::MAX - 1)), Ok(()));

        assert_eq!(order.remaining(), 0);
        assert_eq!(order.original_order(), &original);
    }
}
