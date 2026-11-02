//! 根命令成交身份的确定性编码与带溢出检查的游标。

use crate::{ClusterId, RejectReason, ShardId, TradeId};

/// 为一个根变更按实际成交顺序生成成交身份。
///
/// 游标只能在计划真实 Trade 时推进；子单沿用同一个游标。
/// 序列号与游标本身不提供持久化或跨重启唯一性。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeIdCursor {
    /// 根命令所属集群身份。
    cluster_id: ClusterId,
    /// 根命令所属分片身份。
    shard_id: ShardId,
    /// 已分配给根命令的分片序号。
    shard_seq: u64,
    /// 下一笔成交的索引；`u32::MAX + 1` 表示索引空间已耗尽。
    next_index: u64,
}

impl TradeIdCursor {
    /// 创建从索引零开始的根命令游标。
    pub fn new(cluster_id: ClusterId, shard_id: ShardId, shard_seq: u64) -> Self {
        Self {
            cluster_id,
            shard_id,
            shard_seq,
            next_index: 0,
        }
    }

    /// 从已知索引恢复命令内游标；允许恢复耗尽状态，不允许越过该状态。
    pub fn with_next_index(
        cluster_id: ClusterId,
        shard_id: ShardId,
        shard_seq: u64,
        next_index: u64,
    ) -> Result<Self, RejectReason> {
        if next_index > u64::from(u32::MAX) + 1 {
            return Err(RejectReason::ArithmeticOverflow);
        }
        Ok(Self {
            cluster_id,
            shard_id,
            shard_seq,
            next_index,
        })
    }

    /// 查询下一笔成交的索引或耗尽标记，不推进游标。
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// 为计划中的一笔真实成交生成标识；耗尽时不修改游标。
    pub fn next_trade_id(&mut self) -> Result<TradeId, RejectReason> {
        let index = u32::try_from(self.next_index).map_err(|_| RejectReason::ArithmeticOverflow)?;
        let next = self
            .next_index
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let id = (u128::from(self.cluster_id.0) << 112)
            | (u128::from(self.shard_id.0) << 96)
            | (u128::from(self.shard_seq) << 32)
            | u128::from(index);
        self.next_index = next;
        Ok(TradeId(id))
    }
}
