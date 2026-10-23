use crate::LogicalTime;

/// 未触发 Stop 在触发前的独立存活期。
///
/// 它不能由触发后 child 的 `TimeInForce` 推导；只有逻辑时间或会话结束命令能终结它。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerExpiry {
    /// 未触发 Stop 持续有效，直到被取消或触发。
    Gtc,
    /// Stop 只在指定交易会话内有效。
    Day {
        /// Stop 所属且必须与市场当前会话一致的会话标识符。
        session_id: u64,
    },
    /// Stop 在到达指定逻辑时间时失效。
    Gtd {
        /// 触发前存活期结束的逻辑时间。
        expire_at: LogicalTime,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_expiry_keeps_day_and_gtd_values() {
        assert_eq!(TriggerExpiry::Gtc, TriggerExpiry::Gtc);
        assert_ne!(
            TriggerExpiry::Day { session_id: 1 },
            TriggerExpiry::Day { session_id: 2 }
        );
        assert_ne!(
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(3)
            },
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(4)
            }
        );
    }
}
