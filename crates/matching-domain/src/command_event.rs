//! 命令与业务事件的进程内身份封装。
use crate::{ClusterId, CommandId, EngineEvent, MarketId, MassCancelShard, ShardId, UserId};

/// 当前具有实际载荷的正式业务命令。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// 对分片内符合范围的活动订单执行批量撤单。
    MassCancelShard(
        /// 批量撤单的目标范围。
        MassCancelShard,
    ),
}

/// 外部业务命令及其路由、会话与审计信息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    /// 外部业务意图身份；当前不代表已实现去重。
    pub command_id: CommandId,
    /// 接入会话身份；当前正式入口不按会话执行去重或顺序校验。
    pub session_id: u64,
    /// 调用方提供的会话内客户端序号；当前不校验其连续性。
    pub client_seq: u64,
    /// 需要市场路由的命令所绑定的市场。
    pub market_id: Option<MarketId>,
    /// 发起业务命令的可选用户身份。
    pub user_id: Option<UserId>,
    /// 市场路由版本；不经市场路由的命令可为空。
    pub route_epoch: Option<u64>,
    /// 客户端时间，仅用于审计。
    pub client_timestamp: Option<u64>,
    /// Unix 时间的纳秒接收时间，仅用于审计。
    pub received_at_unix_ns: Option<u64>,
    /// 实际业务命令载荷。
    pub payload: Command,
}

/// 根命令产生的一条业务事件及其进程内确定性身份。
///
/// 身份只在当前分片运行生命周期内保证唯一；未恢复序号时重启可能复用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEnvelope {
    /// 集群身份。
    pub cluster_id: ClusterId,
    /// 执行命令的分片身份。
    pub shard_id: ShardId,
    /// 根变更分配的分片序号。
    pub shard_seq: u64,
    /// 根命令内从零开始的有序事件索引。
    pub event_index: u32,
    /// 产生该事件的根命令身份。
    pub command_id: CommandId,
    /// 事件产生时的分片逻辑时间。
    pub engine_time: u64,
    /// 该事件所属的市场；不属于市场的事件可为空。
    pub market_id: Option<MarketId>,
    /// 保持业务原序的事件载荷。
    pub payload: EngineEvent,
}
