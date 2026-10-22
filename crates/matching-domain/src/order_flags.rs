/// 订单附加约束标志。
///
/// 该结构只保存声明值；基础组合由 validate_basic_order_combination 校验，
/// 完整命令接入及 ReduceOnly 风险执行仍由后续层承担。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderFlags {
    /// 要求订单不得作为吃单方立即成交的约束。
    pub post_only: bool,
    /// 要求订单仅降低现有风险敞口的约束。
    pub reduce_only: bool,
}

#[cfg(test)]
mod tests {
    //! 验证标志字段的独立值语义。
    use super::OrderFlags;

    #[test]
    fn supports_all_four_combinations() {
        let combinations = [
            OrderFlags {
                post_only: false,
                reduce_only: false,
            },
            OrderFlags {
                post_only: false,
                reduce_only: true,
            },
            OrderFlags {
                post_only: true,
                reduce_only: false,
            },
            OrderFlags {
                post_only: true,
                reduce_only: true,
            },
        ];

        assert_eq!(
            (combinations[0].post_only, combinations[0].reduce_only),
            (false, false)
        );
        assert_eq!(
            (combinations[1].post_only, combinations[1].reduce_only),
            (false, true)
        );
        assert_eq!(
            (combinations[2].post_only, combinations[2].reduce_only),
            (true, false)
        );
        assert_eq!(
            (combinations[3].post_only, combinations[3].reduce_only),
            (true, true)
        );
    }

    #[test]
    fn supports_value_equality_and_inequality() {
        let a = OrderFlags {
            post_only: true,
            reduce_only: false,
        };
        let b = OrderFlags {
            post_only: true,
            reduce_only: false,
        };
        let c = OrderFlags {
            post_only: false,
            reduce_only: true,
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn supports_copy() {
        let original = OrderFlags {
            post_only: true,
            reduce_only: false,
        };

        let copied = original;

        assert_eq!(original, copied);
    }

    #[test]
    fn supports_debug() {
        let flags = OrderFlags {
            post_only: true,
            reduce_only: false,
        };

        assert_eq!(
            format!("{flags:?}"),
            "OrderFlags { post_only: true, reduce_only: false }"
        );
    }
}
