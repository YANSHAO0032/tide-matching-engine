/// 单市场内的 FIFO 队列优先级。
///
/// 数值越小，表示同价位下越早获得队列优先级。它不代表全局身份、时间戳或订单 ID；
/// 生产簿中实际的同价顺序仍以 PriceLevel 链表为权威。
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueuePriority(u128);

impl QueuePriority {
    /// 从市场本地已分配的优先级数值构造值对象。
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// 读取市场本地优先级数值。
    pub const fn get(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    //! 验证 priority 的整数边界、排序和哈希值语义。
    use super::QueuePriority;
    use std::collections::HashSet;

    #[test]
    fn preserves_numeric_boundaries() {
        assert_eq!(QueuePriority::new(0).get(), 0);
        assert_eq!(QueuePriority::new(1).get(), 1);
        assert_eq!(QueuePriority::new(u128::MAX).get(), u128::MAX);
    }

    #[test]
    fn supports_natural_ordering_and_hash() {
        let early = QueuePriority::new(10);
        let late = QueuePriority::new(20);

        assert_eq!(early, QueuePriority::new(10));
        assert_ne!(early, late);
        assert!(early < late);

        let mut priorities = [late, early];
        priorities.sort();
        assert_eq!(priorities, [early, late]);

        let mut set = HashSet::new();
        assert!(set.insert(early));
        assert!(!set.insert(QueuePriority::new(10)));
    }

    #[test]
    fn supports_copy_and_debug() {
        let original = QueuePriority::new(42);
        let copied = original;

        assert_eq!(original, copied);
        assert_eq!(format!("{original:?}"), "QueuePriority(42)");
    }
}
