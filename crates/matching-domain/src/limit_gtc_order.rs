use crate::{OrderId, Price, Qty, Side, UserId};

/// 最小 GTC 限价订单值对象。
///
/// 它不含市场、有效期、标志、状态或资金预留；这些契约由后续命令模型承载。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitGtcOrder {
    /// 在订单簿内唯一定位该订单的业务身份。
    pub order_id: OrderId,
    /// 提交订单的用户身份。
    pub user_id: UserId,
    /// 买入或卖出方向。
    pub side: Side,
    /// 限价价格，必须为严格正 ticks。
    pub price: Price,
    /// 原始下单数量，必须为严格正 lots。
    pub qty: Qty,
}

#[cfg(test)]
mod tests {
    //! 验证最小 GTC 订单的字段级值语义。
    use super::*;
    use crate::{OrderId, Price, Qty, Side, UserId};

    #[test]
    fn identical_orders_are_equal() {
        let a = LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };

        let b = LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };

        assert_eq!(a, b);
    }

    /// 构造可复用的合法订单 fixture。
    fn sample_order() -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(2),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        }
    }

    #[test]
    fn changing_any_field_makes_orders_unequal() {
        let original = sample_order();

        let changed_orders = [
            LimitGtcOrder {
                order_id: OrderId::new(3),
                ..sample_order()
            },
            LimitGtcOrder {
                user_id: UserId::new(4),
                ..sample_order()
            },
            LimitGtcOrder {
                side: Side::Sell,
                ..sample_order()
            },
            LimitGtcOrder {
                price: Price::try_new(101).unwrap(),
                ..sample_order()
            },
            LimitGtcOrder {
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

        assert!(debug.contains("LimitGtcOrder"));
        assert!(debug.contains("OrderId(1)"));
        assert!(debug.contains("UserId(2)"));
        assert!(debug.contains("Buy"));
        assert!(debug.contains("Price(100)"));
        assert!(debug.contains("Qty(10)"));
    }
}
