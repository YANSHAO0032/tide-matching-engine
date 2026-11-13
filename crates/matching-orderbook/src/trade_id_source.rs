//! 成交身份来源的兼容适配。

use matching_domain::{RejectReason, TradeId, TradeIdCursor};

/// 兼容调用方预置序列或根命令内的延迟分配游标。
#[derive(Clone)]
pub(crate) enum TradeIdSource<'a> {
    /// 既有接口保持调用者保证长度的契约。
    /// `ids` 是调用方预先提供的成交身份序列，`position` 是下一次读取的位置。
    Legacy { ids: &'a [TradeId], position: usize },
    /// 仅对实际成交分配身份，记录游标耗尽。
    Cursor {
        /// 候选根命令独占的成交身份游标。
        cursor: TradeIdCursor,
        /// 是否曾因游标耗尽导致根命令必须失败。
        exhausted: bool,
        /// 单根命令剩余的成交安全额度；为空表示仅受游标本身限制。
        remaining: Option<u32>,
    },
}

impl<'a> TradeIdSource<'a> {
    /// 创建使用既有身份切片的来源。
    pub(crate) fn legacy(ids: &'a [TradeId]) -> Self {
        Self::Legacy { ids, position: 0 }
    }

    /// 创建独占根命令游标的来源。
    pub(crate) fn cursor(cursor: TradeIdCursor) -> Self {
        Self::Cursor {
            cursor,
            exhausted: false,
            remaining: None,
        }
    }

    /// 创建带成交上限的根命令游标；超限属于必须停止分片的安全错误。
    pub(crate) fn cursor_limited(cursor: TradeIdCursor, limit: u32) -> Self {
        Self::Cursor {
            cursor,
            exhausted: false,
            remaining: Some(limit),
        }
    }

    /// 为下一笔已规划的真实成交分配身份。
    pub(crate) fn next_id(&mut self) -> Result<TradeId, RejectReason> {
        match self {
            Self::Legacy { ids, position } => {
                let id = *ids.get(*position).expect("预置 TradeId 数量不足");
                *position = position.checked_add(1).expect("预置 TradeId 索引溢出");
                Ok(id)
            }
            Self::Cursor {
                cursor,
                exhausted,
                remaining,
            } => {
                assert!(*remaining != Some(0), "止损级联超过成交上限，分片停止");
                let result = cursor.next_trade_id();
                if result.is_err() {
                    *exhausted = true;
                } else if let Some(left) = remaining {
                    *left = left.checked_sub(1).expect("成交额度已经预检");
                }
                result
            }
        }
    }

    /// 仅在真实成交计划已知时批量分配对应数量的身份。
    pub(crate) fn reserve(&mut self, count: usize) -> Result<Vec<TradeId>, RejectReason> {
        (0..count).map(|_| self.next_id()).collect()
    }

    /// 返回旧接口尚可使用的身份数量；游标没有预置长度。
    pub(crate) fn legacy_remaining(&self) -> Option<usize> {
        match self {
            Self::Legacy { ids, position } => Some(ids.len() - position),
            Self::Cursor { .. } => None,
        }
    }

    /// 判断根命令是否因成交身份空间耗尽而必须整体拒绝。
    pub(crate) fn cursor_exhausted(&self) -> bool {
        matches!(
            self,
            Self::Cursor {
                exhausted: true,
                ..
            }
        )
    }

    /// 取回游标，供候选根命令成功时提交推进后的身份状态。
    pub(crate) fn into_cursor(self) -> Option<TradeIdCursor> {
        match self {
            Self::Cursor { cursor, .. } => Some(cursor),
            Self::Legacy { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{ClusterId, ShardId};

    #[test]
    fn legacy_source_keeps_supplied_order_and_remaining() {
        let ids = [TradeId(12), TradeId(33)];
        let mut source = TradeIdSource::legacy(&ids);
        assert_eq!(source.legacy_remaining(), Some(2));
        assert_eq!(source.reserve(2).unwrap(), ids);
        assert_eq!(source.legacy_remaining(), Some(0));
        assert!(!source.cursor_exhausted());
    }

    #[test]
    #[should_panic]
    fn legacy_exhaustion_remains_an_invariant_panic() {
        let mut source = TradeIdSource::legacy(&[]);
        let _result = source.next_id();
    }

    #[test]
    fn planned_batch_overflow_is_reported_without_fabricating_ids() {
        let cursor =
            TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u64::from(u32::MAX))
                .unwrap();
        let mut source = TradeIdSource::cursor(cursor);
        assert_eq!(source.reserve(2), Err(RejectReason::ArithmeticOverflow));
        assert!(source.cursor_exhausted());
        assert_eq!(
            source.into_cursor().unwrap().next_index(),
            u64::from(u32::MAX) + 1
        );
    }

    #[test]
    fn cursor_exhaustion_is_marked_and_cursor_can_be_recovered() {
        let cursor =
            TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u64::from(u32::MAX))
                .unwrap();
        let mut source = TradeIdSource::cursor(cursor);
        assert_eq!(source.legacy_remaining(), None);
        assert!(source.next_id().is_ok());
        assert_eq!(source.next_id(), Err(RejectReason::ArithmeticOverflow));
        assert!(source.cursor_exhausted());
        assert_eq!(
            source.into_cursor().unwrap().next_index(),
            u64::from(u32::MAX) + 1
        );
    }
}
