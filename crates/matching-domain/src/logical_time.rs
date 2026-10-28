/// 撮合域逻辑时间。
///
/// 该计数值不读取或表示系统时钟；推进仅能由持久化的时间命令完成。
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct LogicalTime(u64);

impl LogicalTime {
    /// 从已确定的逻辑计数构造值对象，不推进任何时钟。
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 读取底层逻辑计数。
    pub fn get(self) -> u64 {
        self.0
    }
}

/// 只由显式业务命令推进的单调逻辑时钟。
///
/// 该状态不读取系统时钟；调用方负责将成功推进后的时间分派给订单簿的到期处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalClock {
    /// 当前已由成功时间命令推进到的逻辑时间。
    now: LogicalTime,
}

impl LogicalClock {
    /// 使用给定的逻辑时间创建时钟。
    pub const fn new(now: LogicalTime) -> Self {
        Self { now }
    }

    /// 读取当前逻辑时间。
    pub const fn now(&self) -> LogicalTime {
        self.now
    }

    /// 将时钟推进到不早于当前值的新时间。
    ///
    /// 相同时间是可重放的幂等操作；回退时间以 `InvalidExpireTime` 拒绝，
    /// 并保证时钟状态完全不变。
    pub fn advance_to(&mut self, new_time: LogicalTime) -> Result<(), crate::RejectReason> {
        if new_time < self.now {
            return Err(crate::RejectReason::InvalidExpireTime);
        }
        self.now = new_time;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! 验证逻辑时间的透明值语义，不测试时间推进协议。
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn preserves_zero_one_and_max() {
        assert_eq!(LogicalTime::new(0).get(), 0);
        assert_eq!(LogicalTime::new(1).get(), 1);
        assert_eq!(LogicalTime::new(u64::MAX).get(), u64::MAX);
    }

    #[test]
    fn supports_value_equality_and_natural_ordering() {
        let zero = LogicalTime::new(0);
        let one = LogicalTime::new(1);
        let max = LogicalTime::new(u64::MAX);

        assert_eq!(one, LogicalTime::new(1));
        assert_ne!(zero, one);

        let mut values = [max, one, zero];
        values.sort();

        assert_eq!(values, [zero, one, max]);
    }

    #[test]
    fn can_be_used_as_btree_map_key() {
        let mut map = BTreeMap::new();

        map.insert(LogicalTime::new(0), "zero");
        map.insert(LogicalTime::new(10), "ten");
        map.insert(LogicalTime::new(u64::MAX), "max");

        assert_eq!(map.get(&LogicalTime::new(0)), Some(&"zero"));
        assert_eq!(map.get(&LogicalTime::new(10)), Some(&"ten"));
        assert_eq!(map.get(&LogicalTime::new(u64::MAX)), Some(&"max"));
        assert_eq!(map.get(&LogicalTime::new(11)), None);
    }

    #[test]
    fn supports_copy_and_debug() {
        let original = LogicalTime::new(42);
        let copied = original;

        assert_eq!(original, copied);
        assert_eq!(format!("{original:?}"), "LogicalTime(42)");
    }

    #[test]
    fn transparent_representation_matches_u64() {
        assert_eq!(
            std::mem::size_of::<LogicalTime>(),
            std::mem::size_of::<u64>()
        );
    }

    #[test]
    fn clock_advances_monotonically_and_keeps_its_value_on_regression() {
        let mut clock = LogicalClock::new(LogicalTime::new(10));

        assert_eq!(clock.now(), LogicalTime::new(10));
        assert_eq!(clock.advance_to(LogicalTime::new(25)), Ok(()));
        assert_eq!(clock.now(), LogicalTime::new(25));
        assert_eq!(clock.advance_to(LogicalTime::new(25)), Ok(()));
        assert_eq!(clock.now(), LogicalTime::new(25));
        assert_eq!(
            clock.advance_to(LogicalTime::new(24)),
            Err(crate::RejectReason::InvalidExpireTime)
        );
        assert_eq!(clock.now(), LogicalTime::new(25));
    }
}
