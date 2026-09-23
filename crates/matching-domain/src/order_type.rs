/// 核心订单类型。
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OrderType {
    //限价单
    Limit,
    //市价单
    Market,
    //止损市价单
    StopMarket,
    //止损限价单
    StopLimit,
    //冰山订单
    Iceberg,
}

#[cfg(test)]
mod tests {
    use super::OrderType;

    #[test]
    fn order_types_are_equal_to_themselves() {
        for order_type in [
            OrderType::Limit,
            OrderType::Market,
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::Iceberg,
        ] {
            assert_eq!(order_type, order_type);
        }
    }

    #[test]
    fn different_order_types_are_not_equal() {
        let variants = [
            OrderType::Limit,
            OrderType::Market,
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::Iceberg,
        ];

        for (i, lhs) in variants.iter().enumerate() {
            for (j, rhs) in variants.iter().enumerate() {
                if i != j {
                    assert_ne!(lhs, rhs);
                }
            }
        }
    }

    #[test]
    fn order_types_support_copy_and_clone() {
        let original = OrderType::Iceberg;
        let copied = original;

        assert_eq!(original, copied);
    }

    #[test]
    fn order_types_support_debug() {
        assert_eq!(format!("{:?}", OrderType::Limit), "Limit");
        assert_eq!(format!("{:?}", OrderType::Market), "Market");
        assert_eq!(format!("{:?}", OrderType::StopMarket), "StopMarket");
        assert_eq!(format!("{:?}", OrderType::StopLimit), "StopLimit");
        assert_eq!(format!("{:?}", OrderType::Iceberg), "Iceberg");
    }
}
