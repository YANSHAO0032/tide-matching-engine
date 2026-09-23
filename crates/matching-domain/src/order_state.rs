///订单状态
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    // 已接受
    Accepted,
    // 部分成交
    PartiallyFilled,
    // 完全成交
    Filled,
    // 已撤销
    Canceled,
    // 已过期
    Expired,
    // 已拒绝
    Rejected,
}

#[cfg(test)]
mod tests {
    use super::OrderState;

    #[test]
    fn order_states_are_equal_to_themselves() {
        for state in [
            OrderState::Accepted,
            OrderState::PartiallyFilled,
            OrderState::Filled,
            OrderState::Canceled,
            OrderState::Expired,
            OrderState::Rejected,
        ] {
            assert_eq!(state, state);
        }
    }

    #[test]
    fn different_order_states_are_not_equal() {
        let variants = [
            OrderState::Accepted,
            OrderState::PartiallyFilled,
            OrderState::Filled,
            OrderState::Canceled,
            OrderState::Expired,
            OrderState::Rejected,
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
    fn order_states_support_copy_and_clone() {
        let original = OrderState::PartiallyFilled;
        let copied = original;

        assert_eq!(original, copied);
    }

    #[test]
    fn order_states_support_debug() {
        assert_eq!(format!("{:?}", OrderState::Accepted), "Accepted");
        assert_eq!(
            format!("{:?}", OrderState::PartiallyFilled),
            "PartiallyFilled"
        );
        assert_eq!(format!("{:?}", OrderState::Filled), "Filled");
        assert_eq!(format!("{:?}", OrderState::Canceled), "Canceled");
        assert_eq!(format!("{:?}", OrderState::Expired), "Expired");
        assert_eq!(format!("{:?}", OrderState::Rejected), "Rejected");
    }
}
