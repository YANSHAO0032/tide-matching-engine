/// 撮合域逻辑时间
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct LogicalTime(u64);

impl LogicalTime {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
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
}
