//! 命令与业务事件的进程内身份封装。
use crate::{
    ClusterId, CommandId, EngineEvent, LimitOrder, MarketId, MassCancelShard, RejectReason,
    ReservationToken, ShardId, UserId,
};

/// 正式普通限价下单载荷；仅显式配置的分片入口支持执行。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceLimitOrder {
    /// 订单身份、方向、价格、数量和自成交策略。
    pub order: LimitOrder,
    /// 用户提供的可选客户端订单身份。
    pub client_order_id: Option<String>,
    /// 用户控制状态版本，由分片在分配序号前校验。
    pub user_control_epoch: u64,
    /// 只挂单标志；交叉价格在改簿前拒绝。
    pub post_only: bool,
    /// 绑定根命令和订单的独立预留授权。
    pub reservation: ReservationToken,
}

/// 命令执行摘要中的内部状态；尚无稳定二进制编号。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandStatus {
    /// 命令已成功执行。
    Succeeded,
    /// 进入业务执行后被拒绝。
    Rejected,
}

/// 首次业务执行的有界摘要，不包含事件批次。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandResultSummary {
    /// 业务执行状态。
    pub status: CommandStatus,
    /// 首个事件的索引；无事件时为空。
    pub first_event_index: Option<u32>,
    /// 最后一个事件的索引；无事件时为空。
    pub last_event_index: Option<u32>,
    /// 已分配的分片序号；是否持久化取决于运行时入口及其提交策略。
    pub durable_seq: Option<u64>,
    /// 被业务拒绝时的稳定原因。
    pub reject_reason: Option<RejectReason>,
}

/// 用户控制操作的分片内动作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserFenceAction {
    /// 仅推进用户下限。
    FenceOnly,
    /// 推进下限并取消该用户订单。
    FenceAndCancelAll,
}

/// 协调者向分片发送的用户控制栅栏。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyUserControlFenceShard {
    /// 父命令身份。
    pub parent_command_id: CommandId,
    /// 受控用户。
    pub user_id: UserId,
    /// 目标下限。
    pub new_epoch: u64,
    /// 待执行动作。
    pub action: UserFenceAction,
    /// 预期的本地主节点纪元。
    pub expected_primary_epoch: u64,
}

/// 在已关闭市场启动新交易时段的持久命令载荷。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartSession {
    /// 要启动时段的市场身份。
    pub market_id: MarketId,
    /// 新交易时段身份。
    pub new_session_id: u64,
}

/// 创建市场的完整可信候选配置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMarket {
    /// 待创建市场及其初始配置。
    pub config: crate::MarketConfig,
}
/// 更新已存在市场配置的目标值。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMarket {
    /// 更新后的完整市场配置。
    pub config: crate::MarketConfig,
}
/// 开放指定市场的当前交易时段。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenMarket {
    /// 市场身份。
    pub market_id: MarketId,
    /// 待开放的当前时段身份。
    pub session_id: u64,
}
/// 人工暂停指定市场。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspendMarket {
    /// 市场身份。
    pub market_id: MarketId,
}
/// 恢复暂停或停止的市场。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeMarket {
    /// 市场身份。
    pub market_id: MarketId,
}
/// 结束指定市场的交易时段。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndSession {
    /// 市场身份。
    pub market_id: MarketId,
    /// 待结束的当前时段身份。
    pub session_id: u64,
}
/// 关闭市场并按照配置终结活动订单。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseMarket {
    /// 市场身份。
    pub market_id: MarketId,
}

/// 当前具有实际载荷的正式业务命令。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// 创建市场及其初始配置。
    CreateMarket(
        /// 完整初始配置。
        Box<CreateMarket>,
    ),
    /// 更新市场配置。
    UpdateMarket(
        /// 完整目标配置。
        Box<UpdateMarket>,
    ),
    /// 在关闭的市场启动新的交易时段。
    StartSession(
        /// 市场与新时段身份。
        StartSession,
    ),
    /// 开放指定交易时段。
    OpenMarket(
        /// 市场及时段载荷。
        OpenMarket,
    ),
    /// 人工暂停市场。
    SuspendMarket(
        /// 市场身份载荷。
        SuspendMarket,
    ),
    /// 恢复暂停市场。
    ResumeMarket(
        /// 市场身份载荷。
        ResumeMarket,
    ),
    /// 结束当前时段。
    EndSession(
        /// 市场及时段载荷。
        EndSession,
    ),
    /// 关闭市场。
    CloseMarket(
        /// 市场身份载荷。
        CloseMarket,
    ),
    /// 处理单分片用户栅栏。
    ApplyUserControlFenceShard(
        /// 分片内用户控制操作的完整载荷。
        ApplyUserControlFenceShard,
    ),
    /// 普通 GTC 限价订单，具体支持范围由分片入口定义。
    PlaceLimit(
        /// 限价订单及其预留授权。
        Box<PlaceLimitOrder>,
    ),
    /// 对分片内符合范围的活动订单执行批量撤单。
    MassCancelShard(
        /// 批量撤单的目标范围。
        MassCancelShard,
    ),
}

/// 外部业务命令及其路由、会话与审计信息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    /// 外部业务意图身份；正式入口的分片内窗口用于有限期去重。
    pub command_id: CommandId,
    /// 接入会话身份；与客户端序号共同用于分片内的有限别名检查。
    pub session_id: u64,
    /// 调用方提供的会话内客户端序号；窗口外的旧序号不单独拒绝。
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
