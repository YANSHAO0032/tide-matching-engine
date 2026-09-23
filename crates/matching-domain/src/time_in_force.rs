use crate::logical_time::LogicalTime;
/// 订单有效期策略
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce {
    // Good-Til-Canceled
    Gtc,
    // Immediate-Or-Cancel
    Ioc,
    // Fill-Or-Kill
    Fok,
    // 在指定 session 内有效
    Day { session_id: u64 },
    // 在指定逻辑时间到期
    Gtd { expire_at: LogicalTime },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_variants_are_equal_to_themselves_and_distinct() {
        assert_eq!(TimeInForce::Gtc, TimeInForce::Gtc);
        assert_eq!(TimeInForce::Ioc, TimeInForce::Ioc);
        assert_eq!(TimeInForce::Fok, TimeInForce::Fok);

        assert_ne!(TimeInForce::Gtc, TimeInForce::Ioc);
        assert_ne!(TimeInForce::Gtc, TimeInForce::Fok);
        assert_ne!(TimeInForce::Ioc, TimeInForce::Fok);
    }

    #[test]
    fn all_variant_kinds_are_distinct() {
        let gtc = TimeInForce::Gtc;
        let ioc = TimeInForce::Ioc;
        let fok = TimeInForce::Fok;
        let day = TimeInForce::Day { session_id: 1 };
        let gtd = TimeInForce::Gtd {
            expire_at: LogicalTime::new(1),
        };

        assert_ne!(gtc, ioc);
        assert_ne!(gtc, fok);
        assert_ne!(gtc, day);
        assert_ne!(gtc, gtd);

        assert_ne!(ioc, fok);
        assert_ne!(ioc, day);
        assert_ne!(ioc, gtd);

        assert_ne!(fok, day);
        assert_ne!(fok, gtd);

        assert_ne!(day, gtd);
    }

    #[test]
    fn day_session_id_participates_in_equality() {
        assert_eq!(
            TimeInForce::Day { session_id: 7 },
            TimeInForce::Day { session_id: 7 }
        );

        assert_ne!(
            TimeInForce::Day { session_id: 7 },
            TimeInForce::Day { session_id: 8 }
        );
    }

    #[test]
    fn gtd_expire_at_participates_in_equality() {
        assert_eq!(
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(100),
            },
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(100),
            }
        );

        assert_ne!(
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(100),
            },
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(101),
            }
        );
    }

    #[test]
    fn supports_copy() {
        let original = TimeInForce::Gtd {
            expire_at: LogicalTime::new(42),
        };

        let copied = original;

        assert_eq!(original, copied);
    }

    #[test]
    fn supports_debug() {
        assert_eq!(format!("{:?}", TimeInForce::Gtc), "Gtc");

        assert_eq!(
            format!("{:?}", TimeInForce::Day { session_id: 7 }),
            "Day { session_id: 7 }"
        );

        assert_eq!(
            format!(
                "{:?}",
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(42),
                }
            ),
            "Gtd { expire_at: LogicalTime(42) }"
        );
    }
}
