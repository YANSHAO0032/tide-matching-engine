/// 订单标志
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderFlags {
    pub post_only: bool,
    pub reduce_only: bool,
}

#[cfg(test)]
mod tests {
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
