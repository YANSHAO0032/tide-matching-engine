use crate::{OrderId, Price, Qty, Side, UserId};

/// 最小 GTC 限价订单值对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitGtcOrder {
    pub order_id: OrderId,
    pub user_id: UserId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

#[cfg(test)]
mod tests {
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
