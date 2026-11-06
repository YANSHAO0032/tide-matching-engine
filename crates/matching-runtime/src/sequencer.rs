//! 分片本地连续序号分配及耗尽保护。

/// 序号空间耗尽；调用方必须停止后续业务写入。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceExhausted;

/// 单个分片拥有的进程内连续序号分配器。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sequencer {
    /// 下一个待分配序号；最大值保留为耗尽哨兵。
    next: u64,
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl Sequencer {
    /// 从首个业务序号创建空分配器。
    pub fn new() -> Self {
        Self { next: 1 }
    }

    /// 从已验证的最后应用序号恢复，不允许序号回绕。
    pub fn from_last_applied(last: u64) -> Result<Self, SequenceExhausted> {
        Ok(Self {
            next: last.checked_add(1).ok_or(SequenceExhausted)?,
        })
    }

    /// 返回最近分配的序号；消费端完成业务后可用作完成水位。
    pub fn last_applied(&self) -> u64 {
        self.next - 1
    }

    /// 分配连续序号；无法安全前进时保持原状态并返回耗尽。
    pub fn allocate(&mut self) -> Result<u64, SequenceExhausted> {
        let next = self.next.checked_add(1).ok_or(SequenceExhausted)?;
        let allocated = self.next;
        self.next = next;
        Ok(allocated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_starts_at_one_and_advances_without_gaps() {
        let mut sequencer = Sequencer::new();
        assert_eq!(sequencer.last_applied(), 0);
        assert_eq!(sequencer.allocate(), Ok(1));
        assert_eq!(sequencer.allocate(), Ok(2));
        assert_eq!(sequencer.last_applied(), 2);
    }

    #[test]
    fn overflow_fails_closed_without_wrapping() {
        let mut sequencer = Sequencer { next: u64::MAX };
        assert_eq!(sequencer.allocate(), Err(SequenceExhausted));
        assert_eq!(sequencer.allocate(), Err(SequenceExhausted));
        assert_eq!(sequencer.last_applied(), u64::MAX - 1);
    }
    #[test]
    fn restore_advances_after_snapshot_and_rejects_unrepresentable_state() {
        let mut seq = Sequencer::from_last_applied(41).unwrap();
        assert_eq!(seq.last_applied(), 41);
        assert_eq!(seq.allocate(), Ok(42));
        assert!(Sequencer::from_last_applied(u64::MAX).is_err());
        let mut exhausted = Sequencer::from_last_applied(u64::MAX - 1).unwrap();
        assert_eq!(exhausted.allocate(), Err(SequenceExhausted));
    }
}
