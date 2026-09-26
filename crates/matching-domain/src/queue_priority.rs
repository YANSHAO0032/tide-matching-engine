/// 单市场内的 FIFO 队列优先级
///
/// 数值越小，表示同价位下越早获得队列优先级
/// 不代表全局身份、时间戳或订单 ID
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueuePriority(u128);

impl QueuePriority {
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
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
