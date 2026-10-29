use crate::{OrderId, Price, Qty, Side, UserId};

/// 限价订单的最小身份、价格与数量载荷。
///
/// 它不含市场、有效期、标志、状态或资金预留；GTC/IOC/FOK/PostOnly 由所调用的
/// 显式订单簿入口选择，本载荷本身不携带执行策略。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitOrder {
    /// 在订单簿内唯一定位该订单的业务身份。
    pub order_id: OrderId,
    /// 提交订单的用户身份。
    pub user_id: UserId,
    /// 账户层持久化的自成交防护分组身份；零值不参与跨用户分组匹配。
    pub stp_key: crate::StpKey,
    /// 此订单以后作为吃单时采用的自成交防护策略；改单不得改变该字段。
    pub stp_mode: crate::StpMode,
    /// 买入或卖出方向。
    pub side: Side,
    /// 严格正的价格原始整数；市场价格步长对齐须由使用该载荷的校验层保证。
    pub price: Price,
    /// 严格正的原始下单数量；市场数量步长对齐须由使用该载荷的校验层保证。
    pub qty: Qty,
}

impl LimitOrder {
    /// 在任何订单簿可见写入前拒绝尚未冻结执行规则的自成交模式。
    pub fn validate_stp_mode(&self) -> Result<(), crate::RejectReason> {
        self.stp_mode.submission_rejection().map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    //! 验证最小限价载荷的字段及值语义，不决定执行策略。
    use super::*;
    use crate::{OrderId, Price, Qty, Side, UserId};

    #[test]
    fn identical_orders_are_equal() {
        let a = LimitOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            stp_key: crate::StpKey::new(0),
            stp_mode: crate::StpMode::None,
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };

        let b = LimitOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            stp_key: crate::StpKey::new(0),
            stp_mode: crate::StpMode::None,
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };

        assert_eq!(a, b);
    }

    /// 构造可复用的合法订单测试数据。
    fn sample_order() -> LimitOrder {
        LimitOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            stp_key: crate::StpKey::new(0),
            stp_mode: crate::StpMode::None,
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        }
    }

    #[test]
    fn changing_any_field_makes_orders_unequal() {
        let original = sample_order();

        let changed_orders = [
            LimitOrder {
                order_id: OrderId::new(3),
                ..sample_order()
            },
            LimitOrder {
                user_id: UserId::new(4),
                stp_key: crate::StpKey::new(0),
                stp_mode: crate::StpMode::None,
                ..sample_order()
            },
            LimitOrder {
                side: Side::Sell,
                ..sample_order()
            },
            LimitOrder {
                price: Price::try_new(101).unwrap(),
                ..sample_order()
            },
            LimitOrder {
                qty: Qty::try_new(11).unwrap(),
                ..sample_order()
            },
        ];

        for changed in changed_orders {
            assert_ne!(original, changed);
        }
    }

    #[test]
    fn supports_clone() {
        let original = sample_order();
        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    #[test]
    fn supports_debug() {
        let order = sample_order();
        let debug = format!("{order:?}");

        assert!(debug.contains("LimitOrder"));
        assert!(debug.contains("OrderId(1)"));
        assert!(debug.contains("UserId(2)"));
        assert!(debug.contains("Buy"));
        assert!(debug.contains("Price(100)"));
        assert!(debug.contains("Qty(10)"));
    }
}
