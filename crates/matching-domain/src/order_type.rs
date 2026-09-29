/// 订单类型标签。
///
/// 该枚举只承载声明的类型；各类型的下单、触发和撮合行为在对应阶段实现。
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OrderType {
    /// 带价格约束的限价订单。
    Limit,
    /// 以当时可得流动性为目标的市价订单。
    Market,
    /// 触发后形成市价 child 的止损订单。
    StopMarket,
    /// 触发后形成限价 child 的止损订单。
    StopLimit,
    /// 具有可见量与隐藏量语义的冰山订单。
    Iceberg,
}

#[cfg(test)]
mod tests {
    //! 验证订单类型标签的值语义，不覆盖未来订单行为。
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
