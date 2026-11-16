//! 持久写入操作的无锁时延观测，不参与提交决策。
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// 互斥延迟桶的边界，单位为微秒。
pub const LATENCY_BOUNDS_US: [u64; 6] = [10, 100, 1_000, 10_000, 100_000, 1_000_000];

/// 进程内直方图快照；并发读取不构成原子切面。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatencySnapshot {
    /// 各互斥桶的次数，最后一个桶无上界。
    pub buckets: [u64; 7],
    /// 总微秒数。
    pub sum_us: u64,
    /// 任一计数耗尽。
    pub overflow: bool,
}

/// 批次数量直方图快照；桶为互斥计数。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BatchSizeSnapshot {
    /// 每个数量区间的批次数。
    pub buckets: [u64; 7],
    /// 所有批次中的记录数量总和。
    pub sum: u64,
    /// 任一计数耗尽。
    pub overflow: bool,
}

/// 非业务时延统计，数值耗尽时保留原值并显式标记。
#[derive(Default)]
struct Histogram {
    /// 互斥桶。
    buckets: [AtomicU64; 7],
    /// 观察耗时总和。
    sum: AtomicU64,
    /// 溢出标志。
    overflow: AtomicBool,
}
impl Histogram {
    /// 记录一次操作，包括失败操作。
    fn observe(&self, duration: Duration) {
        let Ok(micros) = u64::try_from(duration.as_micros()) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        self.observe_value(micros, LATENCY_BOUNDS_US);
    }
    /// 以指定上界记录非负整数。
    fn observe_value(&self, value: u64, bounds: [u64; 6]) {
        let index = bounds.iter().position(|limit| value <= *limit).unwrap_or(6);
        for (counter, amount) in [(&self.buckets[index], 1), (&self.sum, value)] {
            if counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    old.checked_add(amount)
                })
                .is_err()
            {
                self.overflow.store(true, Ordering::Relaxed);
            }
        }
    }
    /// 读取当前数值。
    fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            buckets: std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
            sum_us: self.sum.load(Ordering::Relaxed),
            overflow: self.overflow.load(Ordering::Relaxed),
        }
    }
}

/// WAL 底层追加和同步操作的耗时。
#[derive(Default)]
pub struct DurabilityMetrics {
    /// 写入器追加耗时。
    append: Histogram,
    /// 写入器同步耗时，含清单登记。
    sync: Histogram,
    /// 批量提交的记录数。
    batch_size: Histogram,
    /// 第一条记录等待批次同步的实际时间。
    batch_wait: Histogram,
}
impl DurabilityMetrics {
    /// 获取追加操作快照。
    pub fn append(&self) -> LatencySnapshot {
        self.append.snapshot()
    }
    /// 获取同步操作快照。
    pub fn sync(&self) -> LatencySnapshot {
        self.sync.snapshot()
    }
    /// 获取实际非空批次的数量分布。
    pub fn batch_size(&self) -> BatchSizeSnapshot {
        let sample = self.batch_size.snapshot();
        BatchSizeSnapshot {
            buckets: sample.buckets,
            sum: sample.sum_us,
            overflow: sample.overflow,
        }
    }
    /// 获取实际非空批次的等待时延。
    pub fn batch_wait(&self) -> LatencySnapshot {
        self.batch_wait.snapshot()
    }
    /// 记录一次批量同步尝试，不影响同步结果。
    pub(crate) fn observe_batch(&self, size: usize, wait: Duration) {
        if let Ok(value) = u64::try_from(size) {
            self.batch_size.observe_value(value, [1, 2, 4, 8, 16, 64]);
        } else {
            self.batch_size.overflow.store(true, Ordering::Relaxed);
        }
        self.batch_wait.observe(wait);
    }
    /// 内部记录一次追加。
    pub(crate) fn observe_append(&self, elapsed: Duration) {
        self.append.observe(elapsed);
    }
    /// 内部记录一次同步。
    pub(crate) fn observe_sync(&self, elapsed: Duration) {
        self.sync.observe(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_records_boundaries_and_explicit_overflow() {
        let histogram = Histogram::default();
        for value in [0, 10, 11, 100, 101, 1_000_001] {
            histogram.observe(Duration::from_micros(value));
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.buckets, [2, 2, 1, 0, 0, 0, 1]);
        assert_eq!(snapshot.sum_us, 1_000_223);
        histogram.sum.store(u64::MAX, Ordering::Relaxed);
        histogram.observe(Duration::from_micros(1));
        assert!(histogram.snapshot().overflow);
        assert_eq!(histogram.snapshot().sum_us, u64::MAX);
    }
}
