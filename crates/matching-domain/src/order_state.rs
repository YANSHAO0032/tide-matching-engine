/// 订单生命周期状态标签。
///
/// 该值对象不定义允许的状态迁移，迁移验证属于命令和撮合流程。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    /// 已通过接收阶段。
    Accepted,
    /// 已成交一部分，仍有剩余数量。
    PartiallyFilled,
    /// 已无剩余数量，因全部成交结束。
    Filled,
    /// 因撤单结束。
    Canceled,
    /// 因有效期规则结束。
    Expired,
    /// 在进入生命周期前被拒绝。
    Rejected,
}

#[cfg(test)]
mod tests {
    //! 验证订单状态标签的基础值语义。
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
