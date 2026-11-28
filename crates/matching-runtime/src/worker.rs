//! 分片工作线程及进程内消息。
mod cross_fence;
mod market_transfer;
mod migration_worker;
mod snapshot;
mod snapshot_writer;
use migration_worker::{MigrationInstall, SnapshotReply};

use crate::client_order_id::ClientOrderIds;
use crate::idempotency::{DedupWindow, IdempotencyConfig};
use crate::place_candidate::{PlacePlanningContext, place_candidate};
use crate::place_config::PlaceSupportConfig;
use crate::queue::{IngressClass, IngressLimits, Producer, PushError, bounded_queue_with_limits};
use crate::route_table::{RouteState, RouteTable};
use crate::sequencer::Sequencer;
use crate::user_control::UserControlEpochs;
use crate::wait_strategy::{WaitStrategy, Waiter};
use matching_domain::{
    ClusterId, Command, CommandEnvelope, CommandId, CommandResultSummary, CommandStatus,
    EngineEvent, EventEnvelope, LimitOrder, LogicalClock, LogicalTime, MarketId, OrderId,
    RejectReason, ReservationId, ShardCancelScope, ShardId, ShardMassCancelResult, TradeIdCursor,
};
use matching_orderbook::{OrderNode, ProductionOrderBook};
use matching_risk::{ReservationRegistry, ReservationState};
pub use snapshot::{SnapshotRecoveryCandidate, SnapshotRestoreConfig};
pub use snapshot_writer::{SnapshotWriter, SnapshotWriterError};
use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

/// 无登记表的低层模式可执行的订单变更；登记表模式拒绝该入口。
#[derive(Clone, Debug)]
pub enum Mutation {
    /// 执行普通限价单。
    Place(
        /// 待执行的普通限价订单。
        LimitOrder,
    ),
    /// 取消普通挂单。
    Cancel(
        /// 待取消的普通订单身份。
        OrderId,
    ),
    /// 执行基础数量市价单及其市场数值规则。
    MarketByBase(
        /// 按基础数量执行的市价订单。
        matching_domain::MarketByBase,
        /// 该市场用于数值校验及本金计算的规则。
        matching_domain::MarketMath,
    ),
}

/// 进程内变更封装；回复句柄不进入持久化或线协议。
#[derive(Clone, Debug)]
pub struct MutationEnvelope {
    /// 调用方提供的命令身份。
    pub command_id: CommandId,
    /// 显式路由的市场身份。
    pub market_id: MarketId,
    /// 调用方提供的路由版本。
    pub route_epoch: u64,
    /// 当前已接入的业务载荷。
    pub payload: Mutation,
}

/// 订单簿低层执行结果，取消返回被移除节点。
#[derive(Debug, PartialEq, Eq)]
pub enum MutationOutcome {
    /// 按原序返回限价撮合事件。
    Placed(
        /// 限价订单执行后按原序产生的事件。
        Vec<EngineEvent>,
    ),
    /// 返回基础数量市价单的实际执行结果。
    MarketByBase(
        /// 基础数量市价订单的实际执行结果。
        matching_domain::MarketByBaseExecution,
    ),
    /// 返回已移除的普通订单节点，不伪造生命周期事件。
    Canceled(
        /// 从订单簿中移除的普通订单节点。
        OrderNode,
    ),
}

/// 已进入业务执行的结果及其分片序号。
#[derive(Debug)]
pub struct Execution {
    /// 对应的原始命令身份。
    pub command_id: CommandId,
    /// 本次变更消耗的序号。
    pub seq: u64,
    /// 已执行事件或业务拒绝。
    pub result: Result<MutationOutcome, RejectReason>,
}

/// 正式挂单事件在实际入簿时的可见订单详情。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketDataOpenDetail {
    /// 对应根命令中的业务事件索引。
    pub event_index: u32,
    /// 实际入簿的订单身份。
    pub order_id: OrderId,
    /// 入簿方向。
    pub side: matching_domain::Side,
    /// 入簿价格。
    pub price: matching_domain::Price,
    /// 入簿时可见的剩余数量。
    pub visible: matching_domain::Qty,
}

/// 正式命令的进程内执行结果。
#[derive(Debug)]
pub struct FormalExecution {
    /// 对应的根命令身份；零事件命令也保留该身份。
    pub command_id: CommandId,
    /// 首次执行时分配的分片序号；去重命中时沿用原值。
    pub seq: u64,
    /// 与命令类型对应的业务结果或业务拒绝。
    pub outcome: FormalOutcome,
    /// 首次执行的逻辑摘要；不代表已持久化。
    pub summary: CommandResultSummary,
    /// 按业务原序封装的事件。
    pub events: Vec<EventEnvelope>,
    /// 止损级联的中间挂单详情；去重响应不重发该轨迹。
    pub market_data_child_opens: Vec<matching_orderbook::production_orderbook::TriggeredChildOpen>,
    /// 正式挂单事件对应的真实可见详情；去重响应不重发。
    pub market_data_open_details: Vec<MarketDataOpenDetail>,
    /// 按市场分组的行情源候选；仅持久提交后可对外消费。
    pub market_data_source: BTreeMap<MarketId, Vec<crate::SourceEvent>>,
    /// 首次执行事件的闭区间；零事件时为空。
    pub event_range: Option<(u32, u32)>,
    /// 此次响应是否来自已执行命令的窗口缓存。
    pub deduplicated: bool,
}

/// 正式命令的有类型结果，不作为业务事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormalOutcome {
    /// 用户成功撤销的单笔活动订单身份。
    Canceled(OrderId),
    /// 分片范围控制操作已执行。
    ShardControl,
    /// 费率版本已更新。
    FeeScheduleUpdated(
        /// 已生效的版本号。
        u64,
    ),
    /// 市场管理命令成功，详细变化按事件顺序返回。
    MarketManaged,
    /// 正式限价命令成功接收的订单身份。
    Placed(
        /// 已接受的订单身份。
        OrderId,
    ),
    /// 用户控制下限的生效值。
    UserFence(
        /// 执行后生效的用户控制下限。
        u64,
    ),
    /// 分片批量撤单或协调控制实际撤销的订单数。
    MassCancel(
        /// 成功终结的订单数量。
        ShardMassCancelResult,
    ),
    /// 已进入业务执行的拒绝原因。
    Rejected(
        /// 该命令的稳定业务拒绝原因。
        RejectReason,
    ),
}

/// 正式命令的入口拒绝或协议身份冲突。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormalError {
    /// 现有稳定入口拒绝原因。
    Rejected(
        /// 按原协议保留的稳定入口拒绝原因。
        RejectReason,
    ),
    /// 命令身份或会话序号与先前业务意图冲突。
    ProtocolError,
}
impl PartialEq<RejectReason> for FormalError {
    fn eq(&self, other: &RejectReason) -> bool {
        matches!(self, Self::Rejected(reason) if reason == other)
    }
}

/// 入口或运行状态错误。
#[derive(Debug, PartialEq, Eq)]
pub enum ShardError {
    /// 入口繁忙。
    Busy,
    /// 入口已关闭。
    Closed,
    /// 未能读取权威控制状态。
    ControlUnavailable,
    /// 截止时间已到。
    Timeout,
    /// 线程异常退出。
    Failed,
    /// 启动配置不一致。
    InvalidConfig,
}

impl ShardError {
    /// 将入口过载映射为稳定拒绝码；其他运行故障不伪造业务原因。
    pub fn ingress_reason(&self) -> Option<RejectReason> {
        match self {
            Self::Busy => Some(RejectReason::EngineBusy),
            Self::Closed
            | Self::Timeout
            | Self::Failed
            | Self::InvalidConfig
            | Self::ControlUnavailable => None,
        }
    }
}

/// 一致性镜像构造失败。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// 无法为有界状态副本预留容量。
    Capacity,
}

/// 分片线程在队列顺序点构建的独立内存镜像。
///
/// 镜像可编码并由后台写入器提交；仅内存捕获不代表快照已持久提交。
pub struct SnapshotImage {
    /// 镜像记录的历史路由，供恢复时核对迁移切面并约束后续 WAL 的路由纪元。
    replay_route_floor: Option<Arc<RouteTable>>,
    /// 无快照恢复时构造的空基线；不参与编码和业务摘要。
    genesis_baseline: bool,
    /// 本进程屏障来源凭据；从磁盘恢复的镜像没有提交权限。
    origin: Option<Arc<()>>,
    /// 已证明镜像可作为本地进程内启动点；落后于持久水位的候选不得启动。
    start_allowed: bool,
    /// 调用方的请求身份。
    request_id: u128,
    /// 镜像覆盖的最后已应用分片序号。
    snapshot_seq: u64,
    /// 所属集群。
    cluster: ClusterId,
    /// 所属分片。
    shard: ShardId,
    /// 捕获时的逻辑时间。
    now: LogicalTime,
    /// 捕获时的主节点纪元。
    primary_epoch: u64,
    /// 市场路由的不可变副本。
    routes: Arc<RouteTable>,
    /// 各市场完整订单簿副本。
    markets: BTreeMap<MarketId, ProductionOrderBook>,
    /// 已启用管理状态机的市场及其持久配置。
    managed_markets: BTreeMap<MarketId, matching_domain::ManagedMarket>,
    /// 完整的预留登记表副本。
    registry: Option<ReservationRegistry>,
    /// 分片用户纪元下限及用户、全局最近控制回执的副本。
    user_epochs: UserControlEpochs,
    /// 分片命令去重窗口副本。
    dedup: Option<DedupWindow>,
    /// 下单配置、客户端身份索引、费率簿、分片熔断状态及分市场行情候选的副本。
    place: Option<PlaceRuntimeState>,
}
impl SnapshotImage {
    /// 获取快照中的逐订单视图副本；市场或行情状态缺失时返回空。
    /// 只有经清单验证的快照才能证明其持久性。
    pub fn market_data_builder(&self, market: MarketId) -> Option<crate::MboBuilder> {
        self.place.as_ref()?.market_data.as_ref()?.builder(market)
    }
    /// 返回市场快照中的最后行情源序号；市场或行情状态缺失时返回空。
    pub fn market_event_seq(&self, market: MarketId) -> Option<u64> {
        self.place
            .as_ref()?
            .market_data
            .as_ref()?
            .market_event_seq(market)
    }
    /// 返回请求身份。
    pub fn request_id(&self) -> u128 {
        self.request_id
    }
    /// 返回已应用分片序号。
    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }
    /// 返回所属集群与分片。
    pub fn identity(&self) -> (ClusterId, ShardId) {
        (self.cluster, self.shard)
    }
    /// 返回捕获时逻辑时间和主节点纪元。
    pub fn clock_and_epoch(&self) -> (LogicalTime, u64) {
        (self.now, self.primary_epoch)
    }
    /// 借用路由快照。
    pub fn routes(&self) -> &RouteTable {
        &self.routes
    }
    /// 查询镜像中的普通订单。
    pub fn order(&self, market: MarketId, id: OrderId) -> Result<Option<&OrderNode>, RejectReason> {
        self.markets
            .get(&market)
            .map(|book| book.order(id))
            .ok_or(RejectReason::MarketNotFound)
    }
    /// 查询镜像中的未触发止损订单及其优先级。
    pub fn stop_order(
        &self,
        market: MarketId,
        id: OrderId,
    ) -> Result<Option<(&matching_domain::StopOrder, matching_domain::StopPriority)>, RejectReason>
    {
        self.markets
            .get(&market)
            .map(|book| book.stop_order(id))
            .ok_or(RejectReason::MarketNotFound)
    }
    /// 返回市场内下一普通订单和止损订单的优先级。
    pub fn next_priorities(&self, market: MarketId) -> Result<(u128, u128), RejectReason> {
        self.markets
            .get(&market)
            .map(|book| (book.next_queue_priority(), book.next_stop_priority()))
            .ok_or(RejectReason::MarketNotFound)
    }
    /// 查询镜像中保留的预留金融状态。
    pub fn reservation(&self, id: ReservationId) -> Option<&ReservationState> {
        self.registry.as_ref().and_then(|registry| registry.get(id))
    }
    /// 在已捕获的镜像中枚举所有非零用户下限，供停写升级时按用户取各分片最大值。
    pub fn user_control_floors(&self) -> impl Iterator<Item = (matching_domain::UserId, u64)> + '_ {
        self.user_epochs.floors()
    }
    /// 检查用户纪元是否满足镜像中的下限。
    pub fn check_user_epoch(
        &self,
        user: matching_domain::UserId,
        epoch: u64,
    ) -> Result<(), RejectReason> {
        self.user_epochs.check(user, epoch)
    }
    /// 查询该正式命令是否已在捕获时保留在去重窗口。
    pub fn contains_command(&self, command: &CommandEnvelope) -> Result<bool, FormalError> {
        self.dedup.as_ref().map_or(Ok(false), |window| {
            window
                .lookup(command)
                .map(|entry| entry.is_some())
                .map_err(|()| FormalError::ProtocolError)
        })
    }
    /// 按用户和客户端身份查询镜像中的活动或保留终结订单。
    pub fn client_order(
        &self,
        user: matching_domain::UserId,
        client_id: &str,
    ) -> Option<(MarketId, OrderId)> {
        self.place
            .as_ref()
            .and_then(|state| state.ids.lookup(user, client_id))
    }
}

/// 命令回复携带入队尝试起点；只对最终产生的回复采样。
struct CommandReply<T> {
    /// 有界的一次性回复端。
    sender: SyncSender<T>,
    /// 单调观测起点，不参与业务处理。
    started: Instant,
    /// 实际分片的延迟统计。
    metrics: Option<Arc<ExecutionMetrics>>,
}
impl<T> CommandReply<T> {
    /// 创建待投递的命令回复端。
    fn new(sender: SyncSender<T>, metrics: Option<Arc<ExecutionMetrics>>) -> Self {
        Self {
            sender,
            started: Instant::now(),
            metrics,
        }
    }
    /// 在交付回复前记录完整队列、执行及持久化耗时。
    fn send(self, value: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        if let Some(metrics) = &self.metrics {
            metrics.observe_command(self.started.elapsed());
        }
        self.sender.send(value)
    }
}

/// 只在本进程队列内传递的变更、查询与控制消息。
enum Message {
    /// 到达顺序点时用第一个通道通知测试线程，再等待第二个通道放行。
    #[cfg(test)]
    TestBlock(SyncSender<()>, Receiver<()>),
    /// 在队列顺序点暂停或恢复本分片的业务变更，包括分片控制命令。
    RouteAdmission(
        /// 是否暂停本分片的业务变更。
        bool,
        /// 返回控制操作结果。
        SyncSender<Result<(), ShardError>>,
    ),
    /// 在队列顺序点安装可信路由，不分配业务序号。
    InstallRoutes(
        /// 已从控制权威读取的完整路由。
        RouteTable,
        /// 返回安装结果。
        SyncSender<Result<(), ShardError>>,
    ),
    /// 包含调用方身份、路由版本、业务载荷及一次性回复端的变更。
    Mutation(
        /// 调用方提供的命令身份。
        CommandId,
        /// 显式目标市场。
        MarketId,
        /// 调用方提供的路由版本。
        u64,
        /// 已接入的业务变更载荷。
        Mutation,
        /// 只在本进程使用的一次性执行回复端。
        CommandReply<Result<Execution, RejectReason>>,
    ),
    /// 按正式协议派发下单、撤单、市场管理或分片控制等命令。
    Formal(
        /// 含路由及业务身份的命令。
        CommandEnvelope,
        /// 受保护命令在权威准入时观察到的全局纪元。
        Option<u64>,
        /// 返回执行结果或前置拒绝的一次性回复端。
        CommandReply<Result<FormalExecution, FormalError>>,
    ),
    /// 在工作线程顺序点读取预留及其待处理归属。
    Reservation(
        /// 待读取的预留身份。
        ReservationId,
        /// 返回金融快照与待处理状态的一次性回复端。
        SyncSender<Option<(ReservationState, bool)>>,
    ),
    /// 在队列顺序点查询指定市场的订单节点。
    Order(
        /// 待读取的市场身份。
        MarketId,
        /// 待读取的订单身份。
        OrderId,
        /// 只在本进程使用的一次性查询回复端。
        SyncSender<Result<Option<OrderNode>, RejectReason>>,
    ),
    /// 按需读取本分片全部市场的订单和价档规模。
    BookCounts(
        /// 数量溢出时返回空值。
        SyncSender<Option<(usize, usize)>>,
    ),
    /// 按需读取配置市场的两个优先级分配器。
    PriorityCounts(
        /// 标签数量受外部配置约束。
        Vec<MarketId>,
        /// 返回此分片实际持有的市场状态。
        SyncSender<Vec<PriorityObservation>>,
    ),
    /// 确认此前已入队消息处理完毕；载荷为请求身份和返回已完成序号水位的回复端。
    Barrier(
        /// 调用方提供的屏障请求身份。
        u128,
        /// 返回请求身份与已完成变更序号水位的一次性回复端。
        SyncSender<(u128, u64)>,
    ),
    /// 在队列顺序点复制分片状态，不分配业务序号。
    Snapshot(
        /// 调用方提供的屏障请求身份。
        u128,
        /// 返回独立镜像或容量错误的一次性回复端。
        SnapshotReply,
    ),
    /// 安装指定迁移的分片镜像，并通过请求中的回复通道报告结果。
    InstallMigration(Box<MigrationInstall>),
    /// 对已同步快照文件向唯一清单写者提交意图。
    SnapshotCommitted(
        /// 已同步文件的快照身份。
        u64,
        /// 快照捕获的最后已应用序号。
        u64,
        /// 镜像所属集群身份。
        ClusterId,
        /// 镜像所属分片身份。
        ShardId,
        /// 返回清单持久提交结果的一次性回复端。
        SyncSender<std::io::Result<()>>,
    ),
    /// 外部入口关闭后入队的最终排空屏障，载荷为确认到达屏障的回复端。
    Shutdown(
        /// 工作线程到达终止屏障后发送确认的一次性回复端。
        SyncSender<()>,
    ),
}

/// 分片入口容量和空闲等待配置。
#[derive(Clone, Copy, Debug)]
pub struct ShardConfig {
    /// 队列的最大消息数量。
    pub capacity: usize,
    /// 只供变更使用的预留容量。
    pub mutation_reserved_slots: usize,
    /// 最多排队的查询数量。
    pub max_query_inflight: usize,
    /// 最多排队的控制消息数量。
    pub max_control_inflight: usize,
    /// 工作线程空闲策略。
    pub wait_strategy: WaitStrategy,
    /// 启动时暂停变更，等待路由权威确认。
    pub require_route_authority: bool,
}
impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            mutation_reserved_slots: 1,
            max_query_inflight: 512,
            max_control_inflight: 128,
            wait_strategy: WaitStrategy::Hybrid,
            require_route_authority: false,
        }
    }
}

/// 分片入口与线程所有权；订单簿仅存在于工作线程中。
pub struct Shard {
    /// 工作线程是否从路由暂停状态启动。
    route_gated_at_start: bool,
    /// 启动配置中的分片主节点纪元，协调控制在执行前另行核对实际写权。
    primary_epoch: u64,
    /// 工作线程最近确认的路由准入状态。
    route_ready: Arc<AtomicBool>,
    /// 正式业务入口是否在回复前完成本地持久提交。
    durable_formal: bool,
    /// 已绑定到本分片的控制权威，不随市场迁移。
    control_admission: Option<crate::place_config::AdmissionBinding>,
    /// 具备受清单保护的本地市场迁移顺序点。
    local_migration: bool,
    /// 持久写入器的独立时延观测。
    wal_metrics: Option<Arc<matching_persistence::DurabilityMetrics>>,
    /// 工作线程配置中的主节点租约弱引用，不延长租约生命周期。
    primary_lease: Option<std::sync::Weak<crate::PrimaryLease>>,
    /// 当前工作线程专属的快照来源凭据。
    snapshot_origin: Arc<()>,
    /// 限制本分片同时存在的快照后台写入器数量。
    snapshot_writer_active: Arc<AtomicBool>,
    /// 外部与内部消息的有界入口。
    producer: Producer<Message>,
    /// 唤醒工作线程的句柄。
    thread: Thread,
    /// 等待工作线程结束的所有权。
    join: Option<JoinHandle<()>>,
    /// 已入队关闭屏障的单次回复。
    shutdown_reply: Option<Receiver<()>>,
    /// 关闭屏障是否已经得到确认。
    barrier_reached: bool,
    /// 工作线程异常退出标志。
    failed: Arc<AtomicBool>,
    /// 工作线程尚未退出。
    running: Arc<AtomicBool>,
    /// 分片实际处理结果的无锁观测计数。
    metrics: Arc<ExecutionMetrics>,
    /// 有界的分片故障诊断队列。
    diagnostics: Arc<DiagnosticBuffer>,
    /// 所有者丢弃时请求排空退出。
    drop_requested: Arc<AtomicBool>,
    /// 当前工作线程所属分片。
    shard_id: ShardId,
    /// 工作线程所属集群。
    cluster_id: ClusterId,
    /// 工作线程与入口共享的路由快照。
    routes: Arc<RwLock<Arc<RouteTable>>>,
}

impl Shard {
    /// 在可信外围协调下重放分片并恢复备节点持久续写。
    ///
    /// 恢复期间独占 WAL 和清单；协调器须验证快照、权威前缀、写权与备节点，
    /// 并在重放完成后确认复制。分片启动时保持暂停，由路由协调器核验权威后开放准入；完整 HA 接任还需要有效写权与已认证的持久前缀。
    pub fn recover_replicated_group_with_genesis<C: crate::ReplicatedRecoveryCoordinator>(
        wal: impl AsRef<std::path::Path>,
        manifest: impl AsRef<std::path::Path>,
        snapshot_dir: impl AsRef<std::path::Path>,
        trusted: &SnapshotRestoreConfig,
        policies: &[(MarketId, matching_orderbook::QueuePriorityPolicy)],
        (max_batch, max_delay_micros): (usize, u64),
        mut coordinator: C,
    ) -> Result<Self, ShardError> {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
            Profile, RegisteredAppender, ReplicationMode,
        };
        if trusted.primary_epoch == 0
            || trusted
                .place
                .as_ref()
                .is_none_or(|place| !place.require_route_authority)
            || max_batch == 0
            || max_batch > ShardConfig::default().capacity
            || max_delay_micros == 0
            || Instant::now()
                .checked_add(Duration::from_micros(max_delay_micros))
                .is_none()
        {
            return Err(ShardError::InvalidConfig);
        }
        validate_durable_directories(wal.as_ref(), manifest.as_ref())?;
        validate_durable_directories(wal.as_ref(), snapshot_dir.as_ref())?;
        validate_durable_directories(manifest.as_ref(), snapshot_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal.as_ref())?;
        let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(manifest)
            .map_err(|_| ShardError::InvalidConfig)?;
        let records = coordinator
            .recover(
                wal.as_ref(),
                snapshot_dir.as_ref(),
                &mut manager,
                trusted,
                policies,
            )
            .map_err(|_| ShardError::InvalidConfig)?;
        let snapshot_seq = records.snapshot().image.snapshot_seq();
        let baseline = records.snapshot().committed_through;
        if baseline > manager.state().local_durable_seq {
            return Err(ShardError::InvalidConfig);
        }
        let replay_count = records.commands().len();
        let replay_started = Instant::now();
        let replay = if let Some(checkpoint) = coordinator.replay_checkpoint() {
            records.replay_with_checkpoint(trusted.primary_epoch, checkpoint)
        } else {
            records.replay_replicated(trusted.primary_epoch)
        }
        .map_err(|_| ShardError::InvalidConfig)?;
        let replay_duration = replay_started.elapsed();
        coordinator
            .verify_replay(&replay)
            .map_err(|_| ShardError::InvalidConfig)?;
        coordinator
            .reconcile_local(wal.as_ref(), &mut manager, trusted, baseline)
            .map_err(|_| ShardError::InvalidConfig)?;
        if !replay.image.managed_markets.is_empty() && replay.image.place.is_none() {
            return Err(ShardError::InvalidConfig);
        }
        let ownership = crate::ownership::MarketOwnership::acquire(
            replay.image.cluster,
            replay.image.shard,
            replay.image.markets.keys().copied(),
        )
        .ok_or(ShardError::InvalidConfig)?;
        let appender = RegisteredAppender::recover_local(
            &wal,
            64 * 1024 * 1024,
            manager,
            snapshot_seq,
            trusted.cluster.get(),
            trusted.shard.get(),
        )
        .map_err(|_| ShardError::InvalidConfig)?;
        let config = DurabilityConfig {
            profile: Profile::Production,
            mode: DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            },
            replication: ReplicationMode::StandbyDurable,
            crash_recovery_required: true,
            ha_enabled: true,
            allow_data_loss_on_failover: false,
        };
        let batcher = DurableBatcher::resume_replicated_with(
            appender,
            config,
            trusted.primary_epoch,
            |local, baseline| coordinator.finish(local, baseline, &replay),
        )
        .map_err(|_| ShardError::InvalidConfig)?
        .into_boxed();
        let event_hasher = if batcher.digest_checkpoint_required() {
            if snapshot_seq != 0 {
                return Err(ShardError::InvalidConfig);
            }
            Some(replay.event_hasher.clone())
        } else {
            None
        };
        let mut image = replay.image;
        image.primary_epoch = trusted.primary_epoch;
        let books = image
            .markets
            .iter()
            .map(|(id, book)| (*id, book.clone()))
            .collect();
        let registry = image.registry.clone();
        let recovered = Self::start_inner_with_snapshot(
            (image.shard, image.cluster),
            trusted.routes.clone(),
            books,
            ShardConfig::default(),
            registry,
            (
                image.now,
                trusted.idempotency,
                image.primary_epoch,
                trusted.user_capacity,
                None,
                Some(LocalDurableCommit {
                    event_hasher,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch: trusted.primary_epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
            (Some(image), Some(ownership)),
        )?;
        recovered
            .metrics
            .observe_replay(replay_count, replay_duration);
        Ok(recovered)
    }

    /// 从空状态启动要求备节点持久确认的正式命令分片。
    ///
    /// 工厂由可信外围提供，必须保留传入的本地 WAL 写入器，并实现认证复制与写权事务。
    /// 此入口仅处理空状态启动，不提供接任恢复；分片在路由协调前保持暂停。
    pub fn start_replicated_group_place_support<P, A, F>(
        (shard, cluster): (ShardId, ClusterId),
        routes: RouteTable,
        policies: Vec<(MarketId, matching_orderbook::QueuePriorityPolicy)>,
        (registry, now): (ReservationRegistry, LogicalTime),
        (idempotency, epoch, user_capacity, place): (
            IdempotencyConfig,
            u64,
            usize,
            PlaceSupportConfig,
        ),
        ((wal_dir, manifest_dir), (max_batch, max_delay_micros)): ((P, P), (usize, u64)),
        factory: F,
    ) -> Result<Self, ShardError>
    where
        P: AsRef<std::path::Path>,
        A: matching_persistence::DurableAppender + Send + 'static,
        F: FnOnce(
            matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
        ) -> std::io::Result<A>,
    {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
            Profile, RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        if now.get() != 0
            || epoch == 0
            || !place.require_route_authority
            || registry != ReservationRegistry::new([]).map_err(|_| ShardError::InvalidConfig)?
            || max_batch > ShardConfig::default().capacity
            || Instant::now()
                .checked_add(Duration::from_micros(max_delay_micros))
                .is_none()
        {
            return Err(ShardError::InvalidConfig);
        }
        let config = DurabilityConfig {
            profile: Profile::Production,
            mode: DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            },
            replication: ReplicationMode::StandbyDurable,
            crash_recovery_required: true,
            ha_enabled: true,
            allow_data_loss_on_failover: false,
        };
        config.validate().map_err(|_| ShardError::InvalidConfig)?;
        let mut books = Vec::with_capacity(policies.len());
        for (market, policy) in policies {
            let mut book = ProductionOrderBook::new(policy);
            book.bind_market(market)
                .map_err(|_| ShardError::InvalidConfig)?;
            books.push((market, book));
        }
        validate_durable_directories(wal_dir.as_ref(), manifest_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal_dir.as_ref())?;
        let writer =
            SegmentWriter::new(wal_dir, 64 * 1024 * 1024).map_err(|_| ShardError::InvalidConfig)?;
        let manifest = ManifestManager::<FileManifestStorage>::open(manifest_dir)
            .map_err(|_| ShardError::InvalidConfig)?;
        let registered =
            RegisteredAppender::new(writer, manifest).map_err(|_| ShardError::InvalidConfig)?;
        let appender = factory(registered).map_err(|_| ShardError::InvalidConfig)?;
        if appender.replication_ready() != Some(true) {
            return Err(ShardError::InvalidConfig);
        }
        let mut batcher = DurableBatcher::new_with_config(appender, config, epoch)
            .map_err(|_| ShardError::InvalidConfig)?;
        batcher.set_standby_available(true);
        let event_hasher = batcher
            .digest_checkpoint_required()
            .then(crate::EventHasher::new);
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                epoch,
                user_capacity,
                Some(place),
                Some(LocalDurableCommit {
                    event_hasher,
                    _wal_lease: Some(wal_lease),
                    batcher: batcher.into_boxed(),
                    epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
        )
    }
    /// 从严格清单及快照恢复本地批量持久分片；调用方须独占 WAL 和快照目录。
    ///
    /// 先在隔离镜像完成提交前缀重放，成功后才修复尾部和恢复持久追加；不支持备节点持久策略。
    pub fn recover_group_local(
        wal: impl AsRef<std::path::Path>,
        manifest: impl AsRef<std::path::Path>,
        snapshot_dir: impl AsRef<std::path::Path>,
        trusted: &SnapshotRestoreConfig,
        (max_batch, max_delay_micros): (usize, u64),
    ) -> Result<Self, ShardError> {
        Self::recover_group_local_with_genesis(
            wal,
            manifest,
            snapshot_dir,
            trusted,
            &[],
            (max_batch, max_delay_micros),
        )
    }

    /// 无快照时以调用方提供的可信市场初始策略重建空市场后回放。
    pub fn recover_group_local_with_genesis(
        wal: impl AsRef<std::path::Path>,
        manifest: impl AsRef<std::path::Path>,
        snapshot_dir: impl AsRef<std::path::Path>,
        trusted: &SnapshotRestoreConfig,
        policies: &[(MarketId, matching_orderbook::QueuePriorityPolicy)],
        (max_batch, max_delay_micros): (usize, u64),
    ) -> Result<Self, ShardError> {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
            Profile, RegisteredAppender, ReplicationMode,
        };
        if max_batch == 0
            || max_batch > ShardConfig::default().capacity
            || max_delay_micros == 0
            || Instant::now()
                .checked_add(Duration::from_micros(max_delay_micros))
                .is_none()
        {
            return Err(ShardError::InvalidConfig);
        }
        validate_durable_directories(wal.as_ref(), manifest.as_ref())?;
        validate_durable_directories(wal.as_ref(), snapshot_dir.as_ref())?;
        validate_durable_directories(manifest.as_ref(), snapshot_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal.as_ref())?;
        let manager = ManifestManager::<FileManifestStorage>::open_recovery(manifest)
            .map_err(|_| ShardError::InvalidConfig)?;
        let records = crate::recovery::recover_snapshot_records_with_genesis(
            &wal,
            snapshot_dir,
            &manager,
            trusted,
            policies,
        )
        .map_err(|_| ShardError::InvalidConfig)?
        .ok_or(ShardError::InvalidConfig)?;
        let snapshot_seq = records.snapshot().image.snapshot_seq();
        let replay_count = records.commands().len();
        let replay_started = Instant::now();
        let image = records
            .replay()
            .map_err(|_| ShardError::InvalidConfig)?
            .image;
        let replay_duration = replay_started.elapsed();
        if !image.managed_markets.is_empty() && image.place.is_none() {
            return Err(ShardError::InvalidConfig);
        }
        let ownership = crate::ownership::MarketOwnership::acquire(
            image.cluster,
            image.shard,
            image.markets.keys().copied().filter(|market| {
                trusted
                    .routes
                    .get(*market)
                    .is_some_and(|r| r.shard_id == image.shard)
            }),
        )
        .ok_or(ShardError::InvalidConfig)?;
        let appender = RegisteredAppender::recover_local(
            &wal,
            64 * 1024 * 1024,
            manager,
            snapshot_seq,
            trusted.cluster.get(),
            trusted.shard.get(),
        )
        .map_err(|_| ShardError::InvalidConfig)?;
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: true,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let batcher = DurableBatcher::resume_local(appender, config, trusted.primary_epoch)
            .map_err(|_| ShardError::InvalidConfig)?
            .into_boxed();
        let books = image
            .markets
            .iter()
            .map(|(id, book)| (*id, book.clone()))
            .collect();
        let registry = image.registry.clone();
        let recovered = Self::start_inner_with_snapshot(
            (image.shard, image.cluster),
            trusted.routes.clone(),
            books,
            ShardConfig::default(),
            registry,
            (
                image.now,
                trusted.idempotency,
                image.primary_epoch,
                trusted.user_capacity,
                None,
                Some(LocalDurableCommit {
                    event_hasher: None,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch: trusted.primary_epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
            (Some(image), Some(ownership)),
        )?;
        recovered
            .metrics
            .observe_replay(replay_count, replay_duration);
        Ok(recovered)
    }

    /// 从明确分片路由和已有单市场订单簿启动。
    pub fn start(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
    ) -> Result<Self, ShardError> {
        Self::start_with_config(shard, cluster, routes, books, ShardConfig::default())
    }

    /// 使用明确容量和等待策略启动分片。
    pub fn start_with_config(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        config: ShardConfig,
    ) -> Result<Self, ShardError> {
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            config,
            None,
            (
                LogicalTime::new(0),
                IdempotencyConfig::default(),
                0,
                4096,
                None,
                None,
            ),
        )
    }

    /// 将变更尝试入队，调用方通过单次回复获取业务结果。
    pub fn submit(
        &self,
        command_id: CommandId,
        market: MarketId,
        epoch: u64,
        mutation: Mutation,
    ) -> Result<Receiver<Result<Execution, RejectReason>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(
            Message::Mutation(
                command_id,
                market,
                epoch,
                mutation,
                CommandReply::new(tx, Some(Arc::clone(&self.metrics))),
            ),
            IngressClass::Mutation,
        )?;
        Ok(rx)
    }

    /// 在线程的消息队列中查询订单。
    pub fn order(
        &self,
        market: MarketId,
        id: OrderId,
    ) -> Result<Receiver<Result<Option<OrderNode>, RejectReason>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Order(market, id, tx), IngressClass::Query)?;
        Ok(rx)
    }

    /// 创建可并发克隆的外部生产者句柄。
    pub fn handle(&self) -> ShardHandle {
        ShardHandle {
            primary_epoch: self.primary_epoch,
            route_gated_at_start: self.route_gated_at_start,
            route_ready: Arc::clone(&self.route_ready),
            durable_formal: self.durable_formal,
            local_migration: self.local_migration,
            control_admission: self.control_admission.clone(),
            wal_metrics: self.wal_metrics.clone(),
            primary_lease: self.primary_lease.clone(),
            snapshot_origin: Arc::clone(&self.snapshot_origin),
            snapshot_writer_active: Arc::clone(&self.snapshot_writer_active),
            producer: self.producer.clone(),
            thread: self.thread.clone(),
            failed: Arc::clone(&self.failed),
            running: Arc::clone(&self.running),
            metrics: Arc::clone(&self.metrics),
            diagnostics: Arc::clone(&self.diagnostics),
            shard_id: self.shard_id,
            cluster_id: self.cluster_id,
            routes: Arc::clone(&self.routes),
        }
    }

    /// 入队屏障，在此前所有已入队消息完成后返回序号水位。
    pub fn barrier(&self, request_id: u128) -> Result<Receiver<(u128, u64)>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Barrier(request_id, tx), IngressClass::Control)?;
        Ok(rx)
    }

    fn push(&self, message: Message, class: IngressClass) -> Result<(), ShardError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(ShardError::Failed);
        }
        self.producer
            .try_push_classified(message, class)
            .map_err(|error| match error {
                PushError::Full(_) => ShardError::Busy,
                PushError::Closed(_) if self.failed.load(Ordering::Acquire) => ShardError::Failed,
                PushError::Closed(_) => ShardError::Closed,
            })?;
        self.thread.unpark();
        Ok(())
    }

    /// 关闭准入、排空已接受消息并在截止时间前等待工作线程退出。
    pub fn shutdown(&mut self, timeout: Duration) -> Result<(), ShardError> {
        if self.join.is_none() {
            return Err(ShardError::Closed);
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(ShardError::Failed);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ShardError::Timeout)?;
        if self.shutdown_reply.is_none() && !self.barrier_reached {
            if !self.producer.close_ingress_until(deadline) {
                return Err(ShardError::Timeout);
            }
            let (tx, rx) = sync_channel(1);
            self.producer
                .push_internal_until(Message::Shutdown(tx), deadline)
                .map_err(|_| ShardError::Timeout)?;
            self.shutdown_reply = Some(rx);
            self.thread.unpark();
        }
        if let Some(rx) = &self.shutdown_reply {
            loop {
                if self.failed.load(Ordering::Acquire) {
                    return Err(ShardError::Failed);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(ShardError::Timeout);
                }
                match rx.recv_timeout(remaining.min(Duration::from_millis(10))) {
                    Ok(()) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(ShardError::Failed);
                    }
                }
            }
            self.shutdown_reply = None;
            self.barrier_reached = true;
        }
        while !self.join.as_ref().expect("工作线程句柄存在").is_finished() {
            if Instant::now() >= deadline {
                return Err(ShardError::Timeout);
            }
            thread::yield_now();
        }
        self.join
            .take()
            .expect("工作线程句柄存在")
            .join()
            .map_err(|_| ShardError::Failed)
    }
}

/// 文件创建前确认两个现存目录具有不同的底层文件身份。
fn validate_durable_directories(
    wal: &std::path::Path,
    manifest: &std::path::Path,
) -> Result<(), ShardError> {
    let wal = std::fs::canonicalize(wal).map_err(|_| ShardError::InvalidConfig)?;
    let manifest = std::fs::canonicalize(manifest).map_err(|_| ShardError::InvalidConfig)?;
    if !wal.is_dir()
        || !manifest.is_dir()
        || wal == manifest
        || same_file::is_same_file(&wal, &manifest).map_err(|_| ShardError::InvalidConfig)?
    {
        return Err(ShardError::InvalidConfig);
    }
    Ok(())
}

/// 在 WAL 目录的规范父目录中持有独占锁；锁文件必须保持原位。
fn acquire_wal_directory(wal: &std::path::Path) -> Result<std::fs::File, ShardError> {
    let wal = std::fs::canonicalize(wal).map_err(|_| ShardError::InvalidConfig)?;
    let parent = wal.parent().ok_or(ShardError::InvalidConfig)?;
    let mut name = wal
        .file_name()
        .ok_or(ShardError::InvalidConfig)?
        .to_os_string();
    name.push(".tide-wal.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(parent.join(name))
        .map_err(|_| ShardError::InvalidConfig)?;
    file.try_lock().map_err(|_| ShardError::InvalidConfig)?;
    Ok(file)
}

/// 由分片独占的持久写入原语。
type OwnedAppender = matching_persistence::BoxedDurableAppender;
/// 分片独占的 WAL 提交状态，按持久配置逐条或分组提交，并在需要时等待备机确认。
struct LocalDurableCommit {
    /// 从创世以来的可续写事件摘要；仅认证复制需要。
    event_hasher: Option<crate::EventHasher>,
    /// 单写者批处理器。
    batcher: matching_persistence::DurableBatcher<OwnedAppender>,
    /// 启动时固定的主节点纪元。
    epoch: u64,
    /// 可选批量大小和最长等待微秒数；适用于批量持久提交模式。
    group: Option<(usize, u64)>,
    /// 批处理器关闭后才释放 WAL 排他锁；内存模拟器不使用文件锁。
    _wal_lease: Option<std::fs::File>,
}
impl LocalDurableCommit {
    /// 写入并同步后核对连续提交水位。
    fn commit(
        &mut self,
        cluster: ClusterId,
        shard: ShardId,
        time: u64,
        command: CommandId,
        seq: u64,
        payload: Vec<u8>,
    ) -> std::io::Result<()> {
        let record = matching_persistence::WalRecord {
            cluster_id: cluster.0,
            shard_id: shard.0,
            flags: 0,
            epoch: self.epoch,
            shard_seq: seq,
            engine_time: time,
            command_id: command.0,
            payload,
        };
        self.batcher.append_at(&record, Instant::now())?;
        if self.batcher.committed() != seq {
            return Err(std::io::Error::other("提交水位未推进"));
        }
        Ok(())
    }
}
impl Shard {
    /// 从空状态启动有界批量本地提交的用户栅栏分片。
    pub fn start_group_fence_only<P: AsRef<std::path::Path>>(
        shard: ShardId,
        cluster: ClusterId,
        registry: ReservationRegistry,
        now: LogicalTime,
        (idempotency, epoch, user_capacity): (IdempotencyConfig, u64, usize),
        (wal_dir, manifest_dir): (P, P),
        (max_batch, max_delay_micros): (usize, u64),
    ) -> Result<Self, ShardError> {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, ManifestManager, Profile,
            RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        if now.get() != 0
            || registry != ReservationRegistry::new([]).map_err(|_| ShardError::InvalidConfig)?
            || max_batch > ShardConfig::default().capacity
            || Instant::now()
                .checked_add(Duration::from_micros(max_delay_micros))
                .is_none()
        {
            return Err(ShardError::InvalidConfig);
        }
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        config.validate().map_err(|_| ShardError::InvalidConfig)?;
        validate_durable_directories(wal_dir.as_ref(), manifest_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal_dir.as_ref())?;
        let writer =
            SegmentWriter::new(wal_dir, 64 * 1024 * 1024).map_err(|_| ShardError::InvalidConfig)?;
        let manifest =
            ManifestManager::open(manifest_dir).map_err(|_| ShardError::InvalidConfig)?;
        let registered =
            RegisteredAppender::new(writer, manifest).map_err(|_| ShardError::InvalidConfig)?;
        let batcher = DurableBatcher::new_with_config(registered, config, epoch)
            .map_err(|_| ShardError::InvalidConfig)?
            .into_boxed();
        Self::start_inner(
            shard,
            cluster,
            RouteTable::new([]).map_err(|_| ShardError::InvalidConfig)?,
            vec![],
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                epoch,
                user_capacity,
                None,
                Some(LocalDurableCommit {
                    event_hasher: None,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
        )
    }
    /// 从空状态启动正式命令有界批量本地提交。
    pub fn start_group_place_support<P: AsRef<std::path::Path>>(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        policies: Vec<(MarketId, matching_orderbook::QueuePriorityPolicy)>,
        (registry, now): (ReservationRegistry, LogicalTime),
        (idempotency, epoch, user_capacity, place): (
            IdempotencyConfig,
            u64,
            usize,
            PlaceSupportConfig,
        ),
        ((wal_dir, manifest_dir), (max_batch, max_delay_micros)): ((P, P), (usize, u64)),
    ) -> Result<Self, ShardError> {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, ManifestManager, Profile,
            RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        if now.get() != 0
            || registry != ReservationRegistry::new([]).map_err(|_| ShardError::InvalidConfig)?
            || max_batch > ShardConfig::default().capacity
            || Instant::now()
                .checked_add(Duration::from_micros(max_delay_micros))
                .is_none()
        {
            return Err(ShardError::InvalidConfig);
        }
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch,
                max_delay_micros,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        config.validate().map_err(|_| ShardError::InvalidConfig)?;
        let mut books = Vec::with_capacity(policies.len());
        for (market, policy) in policies {
            let mut book = ProductionOrderBook::new(policy);
            book.bind_market(market)
                .map_err(|_| ShardError::InvalidConfig)?;
            books.push((market, book));
        }
        validate_durable_directories(wal_dir.as_ref(), manifest_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal_dir.as_ref())?;
        let writer =
            SegmentWriter::new(wal_dir, 64 * 1024 * 1024).map_err(|_| ShardError::InvalidConfig)?;
        let manifest =
            ManifestManager::open(manifest_dir).map_err(|_| ShardError::InvalidConfig)?;
        let appender =
            RegisteredAppender::new(writer, manifest).map_err(|_| ShardError::InvalidConfig)?;
        let batcher = DurableBatcher::new_with_config(appender, config, epoch)
            .map_err(|_| ShardError::InvalidConfig)?
            .into_boxed();
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                epoch,
                user_capacity,
                Some(place),
                Some(LocalDurableCommit {
                    event_hasher: None,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
        )
    }
    /// 从空状态启动支持正式命令的本地同步分片；恢复须使用专门入口。
    pub fn start_with_durable_place_support<P: AsRef<std::path::Path>>(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        policies: Vec<(MarketId, matching_orderbook::QueuePriorityPolicy)>,
        (registry, now): (ReservationRegistry, LogicalTime),
        (idempotency, epoch, user_capacity, place): (
            IdempotencyConfig,
            u64,
            usize,
            PlaceSupportConfig,
        ),
        (wal_dir, manifest_dir): (P, P),
    ) -> Result<Self, ShardError> {
        if now.get() != 0
            || registry != ReservationRegistry::new([]).map_err(|_| ShardError::InvalidConfig)?
        {
            return Err(ShardError::InvalidConfig);
        }
        let mut books = Vec::with_capacity(policies.len());
        for (market, policy) in policies {
            let mut book = ProductionOrderBook::new(policy);
            book.bind_market(market)
                .map_err(|_| ShardError::InvalidConfig)?;
            books.push((market, book));
        }
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, ManifestManager, Profile,
            RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        validate_durable_directories(wal_dir.as_ref(), manifest_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal_dir.as_ref())?;
        let writer =
            SegmentWriter::new(wal_dir, 64 * 1024 * 1024).map_err(|_| ShardError::InvalidConfig)?;
        let manifest =
            ManifestManager::open(manifest_dir).map_err(|_| ShardError::InvalidConfig)?;
        let registered =
            RegisteredAppender::new(writer, manifest).map_err(|_| ShardError::InvalidConfig)?;
        let batcher = DurableBatcher::new_with_config(registered, config, epoch)
            .map_err(|_| ShardError::InvalidConfig)?
            .into_boxed();
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                epoch,
                user_capacity,
                Some(place),
                Some(LocalDurableCommit {
                    event_hasher: None,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch,
                    group: None,
                }),
            ),
        )
    }
    /// 从空 WAL 启动仅支持 FenceOnly 的本地同步分片；恢复须使用专门入口。
    pub fn start_durable_fence_only(
        shard: ShardId,
        cluster: ClusterId,
        registry: ReservationRegistry,
        now: LogicalTime,
        (idempotency, epoch, user_capacity): (IdempotencyConfig, u64, usize),
        wal_dir: impl AsRef<std::path::Path>,
        manifest_dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, ShardError> {
        if now.get() != 0
            || registry != ReservationRegistry::new([]).map_err(|_| ShardError::InvalidConfig)?
        {
            return Err(ShardError::InvalidConfig);
        }
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, ManifestManager, Profile,
            RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        config.validate().map_err(|_| ShardError::InvalidConfig)?;
        validate_durable_directories(wal_dir.as_ref(), manifest_dir.as_ref())?;
        let wal_lease = acquire_wal_directory(wal_dir.as_ref())?;
        let writer =
            SegmentWriter::new(wal_dir, 64 * 1024 * 1024).map_err(|_| ShardError::InvalidConfig)?;
        let manifest =
            ManifestManager::open(manifest_dir).map_err(|_| ShardError::InvalidConfig)?;
        let registered =
            RegisteredAppender::new(writer, manifest).map_err(|_| ShardError::InvalidConfig)?;
        let batcher = DurableBatcher::new_with_config(registered, config, epoch)
            .map_err(|_| ShardError::InvalidConfig)?
            .into_boxed();
        Self::start_inner(
            shard,
            cluster,
            RouteTable::new([]).map_err(|_| ShardError::InvalidConfig)?,
            vec![],
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                epoch,
                user_capacity,
                None,
                Some(LocalDurableCommit {
                    event_hasher: None,
                    _wal_lease: Some(wal_lease),
                    batcher,
                    epoch,
                    group: None,
                }),
            ),
        )
    }
}
impl Shard {
    /// 使用真实预留登记表启动仅允许正式命令的分片。
    ///
    /// 调用方必须保证初始逻辑时间与订单前态来自同一可信执行点，
    /// 并保证 DAY 订单的交易时段仍有效；此入口启动时不提供受管理市场的会话前态。
    /// 市场范围批量撤单的授权由可信调用方负责，本接口不执行管理身份认证。
    pub fn start_with_registry(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        registry: ReservationRegistry,
        now: LogicalTime,
    ) -> Result<Self, ShardError> {
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (now, IdempotencyConfig::default(), 0, 4096, None, None),
        )
    }

    /// 使用显式容量启动登记表模式；其他可信前态要求与 `start_with_registry` 相同。
    pub fn start_with_registry_and_idempotency(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        registry: ReservationRegistry,
        now: LogicalTime,
        idempotency: IdempotencyConfig,
    ) -> Result<Self, ShardError> {
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (now, idempotency, 0, 4096, None, None),
        )
    }

    /// 以显式进程内主节点纪元与用户下限容量启动；不提供租约或持久化。
    pub fn start_with_user_control(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        registry: ReservationRegistry,
        now: LogicalTime,
        (idempotency, primary_epoch, user_capacity): (IdempotencyConfig, u64, usize),
    ) -> Result<Self, ShardError> {
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (now, idempotency, primary_epoch, user_capacity, None, None),
        )
    }

    /// 从空活动订单状态启动限价入口；策略和费率由可信本地配置提供。
    ///
    /// 当前客户端身份索引无法从旧订单恢复，因此拒绝包含活动订单的初始簿。
    /// 用户控制纪元、订单及命令去重均只在本进程生命周期内生效。
    pub fn start_with_place_support(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        registry: ReservationRegistry,
        now: LogicalTime,
        (idempotency, primary_epoch, user_capacity, place): (
            IdempotencyConfig,
            u64,
            usize,
            PlaceSupportConfig,
        ),
    ) -> Result<Self, ShardError> {
        Self::start_inner(
            shard,
            cluster,
            routes,
            books,
            ShardConfig::default(),
            Some(registry),
            (
                now,
                idempotency,
                primary_epoch,
                user_capacity,
                Some(place),
                None,
            ),
        )
    }

    /// 校验分片初始状态后建立唯一工作线程和有界入口。
    fn start_inner(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        config: ShardConfig,
        registry: Option<ReservationRegistry>,
        options: (
            LogicalTime,
            IdempotencyConfig,
            u64,
            usize,
            Option<PlaceSupportConfig>,
            Option<LocalDurableCommit>,
        ),
    ) -> Result<Self, ShardError> {
        Self::start_inner_with_snapshot(
            (shard, cluster),
            routes,
            books,
            config,
            registry,
            options,
            (None, None),
        )
    }

    /// 以验证过的快照恢复进程内分片，不恢复 WAL 或续写持久化。
    pub fn start_in_memory_from_snapshot(
        image: SnapshotImage,
        trusted: &SnapshotRestoreConfig,
    ) -> Result<Self, ShardError> {
        if !image.start_allowed || !image.managed_markets.is_empty() {
            return Err(ShardError::InvalidConfig);
        }
        let bytes = image
            .encode_snapshot(trusted.max_bytes)
            .map_err(|_| ShardError::InvalidConfig)?;
        let image = SnapshotImage::decode_snapshot(&bytes, trusted)
            .map_err(|_| ShardError::InvalidConfig)?;
        let books = image
            .markets
            .iter()
            .map(|(id, book)| (*id, book.clone()))
            .collect();
        let registry = image.registry.clone();
        Self::start_inner_with_snapshot(
            (image.shard, image.cluster),
            trusted.routes.clone(),
            books,
            ShardConfig::default(),
            registry,
            (
                image.now,
                trusted.idempotency,
                image.primary_epoch,
                trusted.user_capacity,
                None,
                None,
            ),
            (Some(image), None),
        )
    }

    /// 在正常初始化或可信镜像状态上构建唯一工作线程。
    fn start_inner_with_snapshot(
        (shard, cluster): (ShardId, ClusterId),
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        config: ShardConfig,
        mut registry: Option<ReservationRegistry>,
        (now, idempotency, primary_epoch, user_capacity, place_config, mut durable): (
            LogicalTime,
            IdempotencyConfig,
            u64,
            usize,
            Option<PlaceSupportConfig>,
            Option<LocalDurableCommit>,
        ),
        (restored, ownership): (
            Option<SnapshotImage>,
            Option<crate::ownership::MarketOwnership>,
        ),
    ) -> Result<Self, ShardError> {
        if durable.is_none()
            && (place_config
                .as_ref()
                .is_some_and(|config| config.managed_only)
                || restored
                    .as_ref()
                    .and_then(|image| image.place.as_ref())
                    .is_some_and(|state| state.config.managed_only))
        {
            return Err(ShardError::InvalidConfig);
        }
        let mut user_epochs =
            UserControlEpochs::new(user_capacity).map_err(|()| ShardError::InvalidConfig)?;
        if !idempotency.valid() {
            return Err(ShardError::InvalidConfig);
        }
        let mut dedup = if registry.is_some() {
            Some(DedupWindow::new(idempotency).map_err(|()| ShardError::InvalidConfig)?)
        } else {
            None
        };
        let mut markets = BTreeMap::new();
        for (market, book) in books {
            // 低层模式不接管预留影子；登记表模式必须使用与市场一致的已绑定订单簿。
            if (registry.is_none() && !book.reservation_snapshots().is_empty())
                || book.market_id().is_some_and(|bound| bound != market)
                || (registry.is_some() && book.market_id() != Some(market))
            {
                return Err(ShardError::InvalidConfig);
            }
            if let Some(existing) = &registry {
                let mut expiry_probe = book.clone();
                if !expiry_probe.expire_gtd_through(now).is_empty() {
                    return Err(ShardError::InvalidConfig);
                }
                book.plan_mass_cancel(ShardCancelScope::Market { market_id: market }, existing)
                    .map_err(|_| ShardError::InvalidConfig)?;
            }
            if routes
                .get(market)
                .is_none_or(|route| route.shard_id != shard && !(restored.is_some() && matches!(route.state, RouteState::Migrating { target,.. } if target == shard)))
                || markets.insert(market, book).is_some()
            {
                return Err(ShardError::InvalidConfig);
            }
        }
        if let Some(existing) = &registry {
            existing
                .validate_order_bindings(
                    markets
                        .values()
                        .flat_map(|book| book.reservation_snapshots()),
                )
                .map_err(|_| ShardError::InvalidConfig)?;
        }
        let mut place_state = if let Some(place) = place_config {
            if markets
                .values()
                .any(|book| !book.reservation_snapshots().is_empty())
                || place.policies.iter().any(|(market, policy)| {
                    markets.get(market).is_none_or(|book| {
                        book.price_band_config()
                            .is_some_and(|(_, math)| math != policy.market_math())
                    })
                })
            {
                return Err(ShardError::InvalidConfig);
            }
            let ids = ClientOrderIds::with_max_id_bytes(
                place.limits.active_orders,
                place.limits.terminal_history,
                place.limits.client_id_bytes,
            )
            .map_err(|()| ShardError::InvalidConfig)?;
            Some(PlaceRuntimeState {
                market_data: if durable.is_some() {
                    Some(
                        crate::MarketDataState::new(markets.keys().copied())
                            .map_err(|_| ShardError::InvalidConfig)?,
                    )
                } else {
                    None
                },
                config: place,
                ids,
                fee_book: None,
                shard_kill_active: false,
            })
        } else {
            None
        };
        let mut initial_sequence = Sequencer::new();
        let mut managed_markets = BTreeMap::new();
        if let Some(image) = restored {
            initial_sequence = Sequencer::from_last_applied(image.snapshot_seq)
                .map_err(|_| ShardError::InvalidConfig)?;
            markets = image.markets;
            managed_markets = image.managed_markets;
            registry = image.registry;
            user_epochs = image.user_epochs;
            dedup = image.dedup;
            place_state = image.place;
        }
        if let Some(lease) = place_state
            .as_ref()
            .and_then(|state| state.config.primary_lease.as_ref())
        {
            lease
                .verify(cluster, shard, primary_epoch)
                .map_err(|_| ShardError::InvalidConfig)?;
        }
        if routes.routes().any(|route| {
            route.shard_id == shard
                && route.state == RouteState::Active
                && !markets.contains_key(&route.market_id)
        }) {
            return Err(ShardError::InvalidConfig);
        }
        let ownership = match ownership {
            Some(ownership) => ownership,
            None => {
                crate::ownership::MarketOwnership::acquire(cluster, shard, markets.keys().copied())
                    .ok_or(ShardError::InvalidConfig)?
            }
        };
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: config.capacity,
            mutation_reserved_slots: config.mutation_reserved_slots,
            max_query_inflight: config.max_query_inflight,
            max_control_inflight: config.max_control_inflight,
        })
        .map_err(|_| ShardError::InvalidConfig)?;
        let mut worker_routes = Arc::new(routes);
        let route_snapshot = Arc::new(RwLock::new(Arc::clone(&worker_routes)));
        let shared_routes = Arc::clone(&route_snapshot);
        let snapshot_origin = Arc::new(());
        let worker_snapshot_origin = Arc::clone(&snapshot_origin);
        let snapshot_writer_active = Arc::new(AtomicBool::new(false));
        let close = producer.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let running = Arc::new(AtomicBool::new(true));
        let worker_running = Arc::clone(&running);
        let metrics = Arc::new(ExecutionMetrics::default());
        metrics.shard_halted.store(
            place_state
                .as_ref()
                .is_some_and(|state| state.shard_kill_active),
            Ordering::Release,
        );
        let worker_metrics = Arc::clone(&metrics);
        let diagnostics = Arc::new(DiagnosticBuffer::new());
        let worker_diagnostics = Arc::clone(&diagnostics);
        let drop_requested = Arc::new(AtomicBool::new(false));
        let worker_drop = Arc::clone(&drop_requested);
        let durable_formal = durable.is_some() && place_state.is_some();
        let control_admission = place_state
            .as_ref()
            .and_then(|state| state.config.control_admission.clone());
        if user_epochs.requires_control_admission() && control_admission.is_none() {
            return Err(ShardError::InvalidConfig);
        }
        if control_admission
            .as_ref()
            .is_some_and(|binding| !durable_formal || binding.0.cluster_id() != cluster)
        {
            return Err(ShardError::InvalidConfig);
        }
        let local_migration = durable_formal
            && durable.as_ref().is_some_and(|gate| {
                gate._wal_lease.is_some() && gate.batcher.supports_local_migration()
            });
        let start_route_paused = config.require_route_authority
            || place_state
                .as_ref()
                .is_some_and(|state| state.config.require_route_authority);
        let route_ready = Arc::new(AtomicBool::new(!start_route_paused));
        let worker_route_ready = Arc::clone(&route_ready);
        let wal_metrics = durable.as_ref().map(|gate| gate.batcher.metrics());
        let primary_lease = place_state
            .as_ref()
            .and_then(|state| state.config.primary_lease.as_ref())
            .map(Arc::downgrade);
        let join = thread::spawn(move || {
            // 所有权与工作线程同寿命，排空后才释放。
            let mut ownership = ownership;
            let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut seq = initial_sequence;
                let clock = LogicalClock::new(now);
                let mut waiter = Waiter::new(config.wait_strategy);
                let mut deferred = None;
                let mut route_paused = start_route_paused;
                let mut migrating = worker_routes.migration_participant(shard);
                loop {
                    let Some(msg) = deferred.take().or_else(|| consumer.pop()) else {
                        if worker_drop.load(Ordering::Acquire) && consumer.is_closed_and_empty() {
                            break;
                        }
                        waiter.idle();
                        continue;
                    };
                    waiter.reset();
                    match msg {
                        #[cfg(test)]
                        Message::TestBlock(entered, release) => {
                            let _ = entered.send(());
                            let _ = release.recv();
                        }
                        Message::RouteAdmission(paused, reply) => {
                            route_paused = paused;
                            worker_route_ready.store(!paused, Ordering::Release);
                            let _ = reply.send(Ok(()));
                        }
                        Message::Mutation(_, market, epoch, _, reply)
                            if migrating && !consumer.routes_fenced() =>
                        {
                            let reason = migration_worker::frozen_reason(
                                &worker_routes,
                                shard,
                                Some(market),
                                Some(epoch),
                            );
                            let _ = reply.send(Err(reason));
                        }
                        Message::Formal(command, _, reply)
                            if migrating && !consumer.routes_fenced() =>
                        {
                            let reason = migration_worker::frozen_reason(
                                &worker_routes,
                                shard,
                                command.market_id,
                                command.route_epoch,
                            );
                            let _ = reply.send(Err(FormalError::Rejected(reason)));
                        }
                        Message::Mutation(_, _, _, _, reply)
                            if route_paused || consumer.routes_fenced() =>
                        {
                            let _ = reply.send(Err(RejectReason::EngineBusy));
                        }
                        Message::Formal(_, _, reply)
                            if route_paused || consumer.routes_fenced() =>
                        {
                            let _ =
                                reply.send(Err(FormalError::Rejected(RejectReason::EngineBusy)));
                        }
                        Message::InstallRoutes(next, reply) => {
                            let valid = !consumer.routes_fenced()
                                && migration_worker::routes_installable(
                                    shard,
                                    route_paused,
                                    &worker_routes,
                                    &next,
                                    &markets,
                                );
                            let result = if !valid {
                                Err(ShardError::InvalidConfig)
                            } else if let Ok(mut visible) = shared_routes.write() {
                                if !ownership.replace(markets.keys().copied().filter(|market| {
                                    next.get(*market).is_some_and(|r| r.shard_id == shard)
                                })) {
                                    let _ = reply.send(Err(ShardError::InvalidConfig));
                                    continue;
                                }
                                migrating = next.migration_participant(shard);
                                worker_routes = Arc::new(next);
                                *visible = Arc::clone(&worker_routes);
                                Ok(())
                            } else {
                                Err(ShardError::Failed)
                            };
                            let _ = reply.send(result);
                        }
                        Message::Formal(command, admitted, reply)
                            if durable.as_ref().is_some_and(|gate| gate.group.is_some())
                                || (durable.is_some() && place_state.is_some()) =>
                        {
                            let gate = durable.as_mut().expect("已检查批处理模式");
                            if place_state.is_some() {
                                deferred = execute_formal_group(
                                    (command, admitted, reply),
                                    &mut consumer,
                                    gate,
                                    FormalGroupContext {
                                        epochs: &mut user_epochs,
                                        dedup: dedup.as_mut().expect("已配置幂等窗口"),
                                        seq: &mut seq,
                                        markets: &mut markets,
                                        managed_markets: &mut managed_markets,
                                        registry: &mut registry,
                                        place: &mut place_state,
                                        routes: &worker_routes,
                                        shard,
                                        cluster,
                                        now: clock.now(),
                                        metrics: Some(&worker_metrics),
                                    },
                                );
                            } else {
                                deferred = execute_fence_group(
                                    (command, reply),
                                    &mut consumer,
                                    gate,
                                    (
                                        &mut user_epochs,
                                        dedup.as_mut().expect("已配置幂等窗口"),
                                        &mut seq,
                                    ),
                                    (shard, cluster, clock.now()),
                                    Some(&worker_metrics),
                                );
                            }
                        }
                        Message::Mutation(command_id, market, epoch, mutation, reply) => {
                            let match_started = matches!(
                                &mutation,
                                Mutation::Place(_) | Mutation::MarketByBase(..)
                            )
                            .then(Instant::now);
                            let result =
                                worker_routes.validate(market, shard, epoch).and_then(|()| {
                                    if registry.is_some() {
                                        return Err(RejectReason::InvalidOrderCombination);
                                    }
                                    let book = markets
                                        .get_mut(&market)
                                        .ok_or(RejectReason::StaleRouteEpoch)?;
                                    let n = seq.allocate().expect("分片序号耗尽，停止处理");
                                    let mut cursor = TradeIdCursor::new(cluster, shard, n);
                                    let result = match mutation {
                                        Mutation::Place(order) => book
                                            .place_limit_gtc_with_cursor(order, &mut cursor)
                                            .map(MutationOutcome::Placed),
                                        Mutation::MarketByBase(order, math) => book
                                            .execute_market_by_base_with_cursor(
                                                order,
                                                &math,
                                                &mut cursor,
                                            )
                                            .map(MutationOutcome::MarketByBase),
                                        Mutation::Cancel(id) => {
                                            book.cancel(id).map(MutationOutcome::Canceled)
                                        }
                                    };
                                    Ok(Execution {
                                        command_id,
                                        seq: n,
                                        result,
                                    })
                                });
                            if let Some(started) = match_started {
                                worker_metrics.observe_match(started.elapsed());
                            }
                            worker_metrics.observe_mutation(&result);
                            let _ = reply.send(result);
                        }
                        Message::Formal(command, _, reply) => {
                            let match_started = matches!(&command.payload, Command::PlaceLimit(_))
                                .then(Instant::now);
                            let result = if let Command::ApplyUserControlFenceShard(ref fence) =
                                command.payload
                            {
                                if fence.expected_primary_epoch != primary_epoch {
                                    Err(FormalError::Rejected(RejectReason::StalePrimaryEpoch))
                                } else if let Some(window) = dedup.as_mut() {
                                    execute_fence(
                                        &mut user_epochs,
                                        window,
                                        &mut seq,
                                        &clock,
                                        &mut durable,
                                        FormalIdentity {
                                            shard,
                                            cluster,
                                            event_capacity: u64::from(u32::MAX) + 1,
                                        },
                                        command,
                                    )
                                } else {
                                    Err(FormalError::Rejected(
                                        RejectReason::InvalidOrderCombination,
                                    ))
                                }
                            } else if durable.is_some() && place_state.is_none() {
                                Err(FormalError::Rejected(RejectReason::InvalidOrderCombination))
                            } else if let Command::PlaceLimit(ref place) = command.payload {
                                let preflight = (|| {
                                    let market = command
                                        .market_id
                                        .ok_or(RejectReason::InvalidOrderCombination)?;
                                    let epoch =
                                        command.route_epoch.ok_or(RejectReason::StaleRouteEpoch)?;
                                    worker_routes.validate(market, shard, epoch)?;
                                    if !markets.contains_key(&market) || registry.is_none() {
                                        return Err(RejectReason::InvalidOrderCombination);
                                    }
                                    user_epochs
                                        .check(place.order.user_id, place.user_control_epoch)?;
                                    Ok(())
                                })();
                                match (preflight, dedup.as_mut(), place_state.as_mut()) {
                                    (Err(reason), _, _) => Err(FormalError::Rejected(reason)),
                                    (Ok(()), Some(window), Some(state)) => execute_place(
                                        PlaceExecutionContext {
                                            control_killed: user_epochs.killed(place.order.user_id),
                                            managed: command
                                                .market_id
                                                .and_then(|id| managed_markets.get(&id)),
                                            identity: FormalIdentity {
                                                shard,
                                                cluster,
                                                event_capacity: u64::from(u32::MAX) + 1,
                                            },
                                            seq: &mut seq,
                                            clock: &clock,
                                            markets: &mut markets,
                                            registry: registry.as_mut().expect("已验证登记表"),
                                            window,
                                            state,
                                            durable: &mut durable,
                                        },
                                        command,
                                    ),
                                    _ => Err(FormalError::Rejected(
                                        RejectReason::InvalidOrderCombination,
                                    )),
                                }
                            } else {
                                match dedup.as_mut() {
                                    Some(window) => execute_formal_idempotent(
                                        &worker_routes,
                                        FormalIdentity {
                                            shard,
                                            cluster,
                                            event_capacity: u64::from(u32::MAX) + 1,
                                        },
                                        &mut seq,
                                        &clock,
                                        &mut markets,
                                        &mut registry,
                                        (command, window, &mut place_state, &mut durable),
                                    ),
                                    None => execute_formal(
                                        &worker_routes,
                                        FormalIdentity {
                                            shard,
                                            cluster,
                                            event_capacity: u64::from(u32::MAX) + 1,
                                        },
                                        &mut seq,
                                        &clock,
                                        &mut markets,
                                        &mut registry,
                                        (command, &mut durable, false, false),
                                    )
                                    .map_err(FormalError::Rejected),
                                }
                            };
                            if let Some(started) = match_started {
                                worker_metrics.observe_match(started.elapsed());
                            }
                            worker_metrics.shard_halted.store(
                                place_state
                                    .as_ref()
                                    .is_some_and(|state| state.shard_kill_active),
                                Ordering::Release,
                            );
                            worker_metrics.observe_formal(&result);
                            let _ = reply.send(result);
                        }
                        Message::Reservation(id, reply) => {
                            let result = registry.as_ref().and_then(|r| {
                                r.get(id).cloned().map(|state| (state, r.is_pending_r2(id)))
                            });
                            let _ = reply.send(result);
                        }
                        Message::Order(market, id, reply) => {
                            let exists = !place_state
                                .as_ref()
                                .is_some_and(|state| state.config.managed_only)
                                || managed_markets.contains_key(&market);
                            let result = if exists {
                                markets
                                    .get(&market)
                                    .map(|book| book.order(id).cloned())
                                    .ok_or(RejectReason::MarketNotFound)
                            } else {
                                Err(RejectReason::MarketNotFound)
                            };
                            let _ = reply.send(result);
                        }
                        Message::BookCounts(reply) => {
                            let counts = markets.values().try_fold(
                                (0_usize, 0_usize),
                                |(orders, levels), book| {
                                    let (book_orders, book_levels) = book.observation_counts()?;
                                    Some((
                                        orders.checked_add(book_orders)?,
                                        levels.checked_add(book_levels)?,
                                    ))
                                },
                            );
                            let _ = reply.send(counts);
                        }
                        Message::PriorityCounts(requested, reply) => {
                            let result = requested
                                .into_iter()
                                .filter_map(|market| {
                                    markets.get(&market).map(|book| PriorityObservation {
                                        market,
                                        queue: book.next_queue_priority(),
                                        stop: book.next_stop_priority(),
                                        ceiling: book.priority_policy().effective_ceiling(),
                                    })
                                })
                                .collect();
                            let _ = reply.send(result);
                        }
                        Message::Barrier(id, reply) => {
                            let _ = reply.send((id, seq.last_applied()));
                        }
                        Message::Snapshot(request_id, reply) => {
                            if let SnapshotReply::Migration(id, _) = &reply
                                && (!local_migration
                                    || consumer.routes_fenced()
                                    || !migration_worker::matches_freeze(
                                        *id,
                                        cluster,
                                        shard,
                                        &worker_routes,
                                    )
                                    || !durable.as_ref().is_some_and(|gate| {
                                        gate.batcher.supports_local_migration()
                                            && gate.batcher.committed() == seq.last_applied()
                                    }))
                            {
                                reply.reject_migration();
                                continue;
                            }
                            let image = (|| {
                                let user_epochs = user_epochs
                                    .try_clone_preallocated()
                                    .map_err(|()| SnapshotError::Capacity)?;
                                let dedup = dedup
                                    .as_ref()
                                    .map(DedupWindow::try_clone_preallocated)
                                    .transpose()
                                    .map_err(|()| SnapshotError::Capacity)?;
                                let place = place_state
                                    .as_ref()
                                    .map(|state| {
                                        Ok::<_, SnapshotError>(PlaceRuntimeState {
                                            market_data: state.market_data.clone(),
                                            config: state.config.clone(),
                                            fee_book: state.fee_book.clone(),
                                            shard_kill_active: state.shard_kill_active,
                                            ids: state
                                                .ids
                                                .try_clone_preallocated()
                                                .map_err(|()| SnapshotError::Capacity)?,
                                        })
                                    })
                                    .transpose()?;
                                Ok(SnapshotImage {
                                    replay_route_floor: None,
                                    genesis_baseline: false,
                                    origin: Some(Arc::clone(&worker_snapshot_origin)),
                                    start_allowed: durable.is_none(),
                                    request_id,
                                    snapshot_seq: seq.last_applied(),
                                    cluster,
                                    shard,
                                    now: clock.now(),
                                    primary_epoch,
                                    routes: Arc::clone(&worker_routes),
                                    markets: markets.clone(),
                                    managed_markets: managed_markets.clone(),
                                    registry: registry.clone(),
                                    user_epochs,
                                    dedup,
                                    place,
                                })
                            })();
                            reply.send(image);
                        }
                        Message::InstallMigration(request) => {
                            let result = (|| {
                                let id = request.migration;
                                if !local_migration
                                    || consumer.routes_fenced()
                                    || !migration_worker::matches_freeze(
                                        id,
                                        cluster,
                                        shard,
                                        &worker_routes,
                                    )
                                {
                                    return Err(std::io::Error::other("没有对应的迁移停写屏障"));
                                }
                                let gate = durable
                                    .as_mut()
                                    .ok_or_else(|| std::io::Error::other("缺少迁移持久化写者"))?;
                                let config = SnapshotRestoreConfig {
                                    cluster,
                                    shard,
                                    primary_epoch,
                                    routes: migration_worker::final_routes(id, &worker_routes)?,
                                    user_capacity,
                                    idempotency,
                                    expect_registry: registry.is_some(),
                                    place: place_state.as_ref().map(|s| s.config.clone()),
                                    max_markets: worker_routes.routes().count(),
                                    max_bytes: request.limit,
                                };
                                let installed = request.install(
                                    &config,
                                    seq.last_applied(),
                                    clock.now(),
                                    gate,
                                    markets.contains_key(&id.market) == (id.target == shard),
                                )?;
                                if !ownership.replace(installed.markets.keys().copied().filter(
                                    |market| {
                                        worker_routes
                                            .get(*market)
                                            .is_some_and(|r| r.shard_id == shard)
                                    },
                                )) {
                                    return Err(std::io::Error::other("迁移后市场认领冲突"));
                                }
                                markets = installed.markets;
                                managed_markets = installed.managed_markets;
                                place_state = installed.place;
                                registry = installed.registry;
                                user_epochs = installed.user_epochs;
                                dedup = installed.dedup;
                                Ok(())
                            })();
                            let _ = request.reply.send(result);
                        }
                        Message::SnapshotCommitted(
                            id,
                            snapshot_seq,
                            image_cluster,
                            image_shard,
                            reply,
                        ) => {
                            let result = if migrating
                                || image_cluster != cluster
                                || image_shard != shard
                                || snapshot_seq > seq.last_applied()
                            {
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "快照身份或应用水位不一致",
                                ))
                            } else if let Some(gate) = durable.as_mut() {
                                gate.batcher.commit_snapshot(id, snapshot_seq)
                            } else {
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::Unsupported,
                                    "分片没有持久化写入器",
                                ))
                            };
                            let halt = result.as_ref().is_err_and(|error| {
                                error.kind() != std::io::ErrorKind::InvalidInput
                                    && error.kind() != std::io::ErrorKind::Unsupported
                            });
                            if halt {
                                worker_diagnostics.record(DiagnosticEvent {
                                    kind: DiagnosticKind::SnapshotCommit,
                                    cluster_id: cluster,
                                    shard_id: shard,
                                    command_id: None,
                                    shard_seq: Some(snapshot_seq),
                                    io_kind: result.as_ref().err().map(std::io::Error::kind),
                                });
                            }
                            let _ = reply.send(result);
                            if halt {
                                worker_failed.store(true, Ordering::Release);
                                break;
                            }
                        }
                        Message::Shutdown(reply) => {
                            let _ = reply.send(());
                            break;
                        }
                    }
                }
            }));
            if let Err(payload) = &run {
                let fault = payload.downcast_ref::<WalFault>();
                worker_diagnostics.record(DiagnosticEvent {
                    kind: fault.map_or(DiagnosticKind::WorkerPanic, |f| f.kind),
                    cluster_id: cluster,
                    shard_id: shard,
                    command_id: fault.and_then(|f| f.command_id),
                    shard_seq: fault.and_then(|f| f.shard_seq),
                    io_kind: fault.map(|f| f.io_kind),
                });
                worker_failed.store(true, Ordering::Release);
            }
            close.close_ingress();
            worker_running.store(false, Ordering::Release);
            // 线程异常时释放所有尚未处理消息的回复端，使接收方能够观察断连。
            if run.is_err() {
                while consumer.pop().is_some() {}
            }
            if let Err(payload) = run {
                std::panic::resume_unwind(payload);
            }
        });
        let thread = join.thread().clone();
        Ok(Self {
            primary_epoch,
            route_gated_at_start: start_route_paused,
            route_ready,
            durable_formal,
            local_migration,
            control_admission,
            wal_metrics,
            primary_lease,
            snapshot_origin,
            snapshot_writer_active,
            producer,
            thread,
            join: Some(join),
            shutdown_reply: None,
            barrier_reached: false,
            failed,
            running,
            metrics,
            diagnostics,
            drop_requested,
            shard_id: shard,
            cluster_id: cluster,
            routes: route_snapshot,
        })
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        if self.join.is_some() {
            // 显式关闭入口后由工作线程排空已接受消息；不等待或伪造屏障确认。
            self.producer.close_ingress();
            self.drop_requested.store(true, Ordering::Release);
            self.thread.unpark();
        }
    }
}

/// 正式命令执行时固定的集群与分片身份。
struct FormalIdentity {
    /// 命令所属分片。
    shard: ShardId,
    /// 命令所属集群。
    cluster: ClusterId,
    /// 根命令可编码的最大事件数量；测试可使用较小边界。
    event_capacity: u64,
}

/// 分片单写者持有的下单配置、客户端身份索引、费率簿、熔断开关和行情候选。
struct PlaceRuntimeState {
    /// 分片的可信下单配置。市场预留策略随市场迁移，验签密钥和容量留在本分片。
    config: PlaceSupportConfig,
    /// 与活动订单及终结历史同步的客户端身份。
    ids: ClientOrderIds,
    /// 单写者持有的持久费率版本候选。
    fee_book: Option<matching_domain::FeeScheduleBook>,
    /// 分片范围熔断开关的持久状态。
    shard_kill_active: bool,
    /// 与持久命令候选共同推进的行情状态；旧快照可暂时没有此状态。
    market_data: Option<crate::MarketDataState>,
}

/// 正式限价命令所需的单写者状态。
struct PlaceExecutionContext<'a> {
    /// 当前用户或全局控制开关是否已激活。
    control_killed: bool,
    /// 市场管理状态；未启用市场管理的既有入口为空。
    managed: Option<&'a matching_domain::ManagedMarket>,
    /// 根事件身份与事件容量。
    identity: FormalIdentity,
    /// 根命令序号分配器。
    seq: &'a mut Sequencer,
    /// 分片业务逻辑时间。
    clock: &'a LogicalClock,
    /// 本分片拥有的市场簿。
    markets: &'a mut BTreeMap<MarketId, ProductionOrderBook>,
    /// 真实预留登记表。
    registry: &'a mut ReservationRegistry,
    /// 已执行命令的有界窗口。
    window: &'a mut DedupWindow,
    /// 可信限价配置和客户端身份索引。
    state: &'a mut PlaceRuntimeState,
    /// WAL 提交屏障；空值表示进程内模式。
    durable: &'a mut Option<LocalDurableCommit>,
}

/// 在批量持久提交前隔离用户控制、幂等和序号的候选状态。
struct FenceBatchCandidate {
    /// 候选用户控制下限及用户、全局回执。
    epochs: UserControlEpochs,
    /// 候选幂等窗口。
    dedup: DedupWindow,
    /// 候选业务序号。
    seq: Sequencer,
    /// 启动时固定的主节点纪元。
    expected_epoch: u64,
    /// 根事件所属分片。
    shard: ShardId,
    /// 根事件所属集群。
    cluster: ClusterId,
    /// 当前固定逻辑时间。
    clock: LogicalClock,
}
impl FenceBatchCandidate {
    /// 复制当前状态；候选安装由提交成功后的调用方负责。
    fn new(
        epochs: &UserControlEpochs,
        dedup: &DedupWindow,
        seq: &Sequencer,
        expected_epoch: u64,
        shard: ShardId,
        cluster: ClusterId,
        now: LogicalTime,
    ) -> Result<Self, ()> {
        Ok(Self {
            epochs: epochs.try_clone_preallocated()?,
            dedup: dedup.try_clone_preallocated()?,
            seq: seq.clone(),
            expected_epoch,
            shard,
            cluster,
            clock: LogicalClock::new(now),
        })
    }
    /// 在候选状态中校验用户 fence 命令的主节点纪元，再按顺序去重和执行。
    fn stage(&mut self, command: CommandEnvelope) -> Result<FormalExecution, FormalError> {
        match &command.payload {
            Command::ApplyUserControlFenceShard(fence)
                if fence.expected_primary_epoch == self.expected_epoch => {}
            Command::ApplyUserControlFenceShard(_) => {
                return Err(FormalError::Rejected(RejectReason::StalePrimaryEpoch));
            }
            _ => return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination)),
        }
        execute_fence(
            &mut self.epochs,
            &mut self.dedup,
            &mut self.seq,
            &self.clock,
            &mut None,
            FormalIdentity {
                shard: self.shard,
                cluster: self.cluster,
                event_capacity: u64::from(u32::MAX) + 1,
            },
            command,
        )
    }
}

/// 满批或达到等待期限时停止接收当前批次。
fn group_should_flush(
    count: usize,
    limit: usize,
    now: Instant,
    deadline: Instant,
    fenced: bool,
) -> bool {
    fenced || count >= limit || now >= deadline
}

/// 顺序规划并提交一批围栏；非正式消息留给工作线程在提交后处理。
fn execute_fence_group(
    first: (
        CommandEnvelope,
        CommandReply<Result<FormalExecution, FormalError>>,
    ),
    consumer: &mut crate::queue::Consumer<Message>,
    gate: &mut LocalDurableCommit,
    (epochs, dedup, seq): (&mut UserControlEpochs, &mut DedupWindow, &mut Sequencer),
    (shard, cluster, now): (ShardId, ClusterId, LogicalTime),
    metrics: Option<&ExecutionMetrics>,
) -> Option<Message> {
    let (limit, delay) = gate.group.expect("仅批量模式调用");
    let start = Instant::now();
    let deadline = start
        .checked_add(Duration::from_micros(delay))
        .expect("批量时限越界");
    let mut candidate =
        FenceBatchCandidate::new(epochs, dedup, seq, gate.epoch, shard, cluster, now)
            .expect("批量候选状态分配失败");
    let mut replies = Vec::new();
    replies
        .try_reserve_exact(limit)
        .expect("批量回复空间分配失败");
    let mut pending = Some(first);
    let mut deferred = None;
    loop {
        if let Some((command, reply)) = pending.take() {
            if consumer.routes_fenced() {
                replies.push((reply, Err(FormalError::Rejected(RejectReason::EngineBusy))));
                break;
            }
            let payload = matching_domain::encode_wal_command(&command);
            let result = match payload {
                Err(_) => Err(FormalError::Rejected(RejectReason::InvalidOrderCombination)),
                Ok(payload) => {
                    let command_id = command.command_id;
                    let planned = candidate.stage(command);
                    if let Ok(result) = &planned
                        && !result.deduplicated
                    {
                        let record = matching_persistence::WalRecord {
                            cluster_id: cluster.0,
                            shard_id: shard.0,
                            flags: 0,
                            epoch: gate.epoch,
                            shard_seq: result.seq,
                            engine_time: now.get(),
                            command_id: command_id.0,
                            payload,
                        };
                        wal_or_halt(
                            gate.batcher.append_at(&record, Instant::now()),
                            DiagnosticKind::WalAppend,
                            Some(command_id),
                            Some(result.seq),
                        );
                    }
                    planned
                }
            };
            replies.push((reply, result));
        }
        if group_should_flush(replies.len(), limit, Instant::now(), deadline, false) {
            break;
        }
        match consumer.pop() {
            Some(Message::Formal(command, _, reply)) => {
                pending = Some((command, reply));
            }
            Some(other) => {
                deferred = Some(other);
                break;
            }
            None => {
                let now = Instant::now();
                if group_should_flush(
                    replies.len(),
                    limit,
                    now,
                    deadline,
                    consumer.routes_fenced(),
                ) {
                    break;
                }
                std::thread::park_timeout(deadline.duration_since(now));
            }
        }
    }
    wal_or_halt(
        gate.batcher.flush(),
        DiagnosticKind::WalSync,
        None,
        Some(candidate.seq.last_applied()),
    );
    assert_eq!(
        gate.batcher.committed(),
        candidate.seq.last_applied(),
        "批量提交水位不连续"
    );
    *epochs = candidate.epochs;
    *dedup = candidate.dedup;
    *seq = candidate.seq;
    for (reply, result) in replies {
        if let Some(metrics) = metrics {
            metrics.observe_formal(&result);
        }
        let _ = reply.send(result);
    }
    deferred
}

/// 正式批次真实状态的独占借用。
struct FormalGroupContext<'a> {
    /// 真实用户下限。
    epochs: &'a mut UserControlEpochs,
    /// 真实幂等窗口。
    dedup: &'a mut DedupWindow,
    /// 真实序号。
    seq: &'a mut Sequencer,
    /// 真实订单簿集合。
    markets: &'a mut BTreeMap<MarketId, ProductionOrderBook>,
    /// 真实市场管理候选状态。
    managed_markets: &'a mut BTreeMap<MarketId, matching_domain::ManagedMarket>,
    /// 真实预留登记表。
    registry: &'a mut Option<ReservationRegistry>,
    /// 真实限价接入状态。
    place: &'a mut Option<PlaceRuntimeState>,
    /// 固定路由视图。
    routes: &'a RouteTable,
    /// 分片身份。
    shard: ShardId,
    /// 集群身份。
    cluster: ClusterId,
    /// 固定业务时间。
    now: LogicalTime,
    /// 实际提交后记录的计数；测试候选可不提供。
    metrics: Option<&'a ExecutionMetrics>,
}
/// 完整分片批次的候选状态，提交前不可见。
struct FormalBatchCandidate {
    /// 用户、幂等与序号候选。
    fence: FenceBatchCandidate,
    /// 订单簿候选。
    markets: BTreeMap<MarketId, ProductionOrderBook>,
    /// 批次内独立的市场管理状态。
    managed_markets: BTreeMap<MarketId, matching_domain::ManagedMarket>,
    /// 预留候选。
    registry: Option<ReservationRegistry>,
    /// 客户端身份及可信限价配置候选。
    place: Option<PlaceRuntimeState>,
}
impl FormalBatchCandidate {
    /// 将批次候选复制为摘要镜像；此镜像不具备快照提交权限。
    fn digest_image(&self, context: &FormalGroupContext<'_>) -> Result<SnapshotImage, ()> {
        let place = self
            .place
            .as_ref()
            .map(|state| {
                Ok::<_, ()>(PlaceRuntimeState {
                    market_data: state.market_data.clone(),
                    config: state.config.clone(),
                    ids: state.ids.try_clone_preallocated()?,
                    fee_book: state.fee_book.clone(),
                    shard_kill_active: state.shard_kill_active,
                })
            })
            .transpose()?;
        Ok(SnapshotImage {
            replay_route_floor: None,
            genesis_baseline: false,
            origin: None,
            start_allowed: false,
            request_id: 0,
            snapshot_seq: self.fence.seq.last_applied(),
            cluster: context.cluster,
            shard: context.shard,
            now: context.now,
            primary_epoch: self.fence.expected_epoch,
            routes: Arc::new(context.routes.clone()),
            markets: self.markets.clone(),
            managed_markets: self.managed_markets.clone(),
            registry: self.registry.clone(),
            user_epochs: self.fence.epochs.try_clone_preallocated()?,
            dedup: Some(self.fence.dedup.try_clone_preallocated()?),
            place,
        })
    }
    /// 从真实状态构造独立候选；容量预留失败则停止当前批次。
    fn new(context: &FormalGroupContext<'_>, epoch: u64) -> Result<Self, ()> {
        let place = context
            .place
            .as_ref()
            .map(|state| {
                Ok(PlaceRuntimeState {
                    market_data: state.market_data.clone(),
                    config: state.config.clone(),
                    ids: state.ids.try_clone_preallocated()?,
                    fee_book: state.fee_book.clone(),
                    shard_kill_active: state.shard_kill_active,
                })
            })
            .transpose()?;
        Ok(Self {
            fence: FenceBatchCandidate::new(
                context.epochs,
                context.dedup,
                context.seq,
                epoch,
                context.shard,
                context.cluster,
                context.now,
            )?,
            markets: context.markets.clone(),
            managed_markets: context.managed_markets.clone(),
            registry: context.registry.clone(),
            place,
        })
    }
    /// 入队的下单按单写者顺序复核全局纪元和用户下限，再检查去重并分配序号。重放沿用已提交记录。
    fn check_live_admission(
        &self,
        command: &CommandEnvelope,
        admitted: Option<u64>,
    ) -> Result<(), FormalError> {
        let Command::PlaceLimit(place) = &command.payload else {
            return Ok(());
        };
        if self
            .place
            .as_ref()
            .is_none_or(|s| s.config.control_admission.is_none())
        {
            return Ok(());
        }
        if admitted != Some(self.fence.epochs.global_floor()) {
            return Err(FormalError::Rejected(RejectReason::ControlFenceActive));
        }
        self.fence
            .epochs
            .check(place.order.user_id, place.user_control_epoch)
            .map_err(FormalError::Rejected)
    }

    /// 仅重现无法编码的下单输入在分配序号前的检查与去重顺序。
    fn reject_uncodable(
        &mut self,
        command: &CommandEnvelope,
        routes: &RouteTable,
    ) -> Result<FormalExecution, FormalError> {
        let Command::PlaceLimit(place) = &command.payload else {
            return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
        };
        let market = command
            .market_id
            .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
        let epoch = command
            .route_epoch
            .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
        routes
            .validate(market, self.fence.shard, epoch)
            .map_err(FormalError::Rejected)?;
        if !self.markets.contains_key(&market) || self.registry.is_none() {
            return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
        }
        self.fence
            .epochs
            .check(place.order.user_id, place.user_control_epoch)
            .map_err(FormalError::Rejected)?;
        let state = self
            .place
            .as_ref()
            .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
        if let Some(result) = self
            .fence
            .dedup
            .lookup(command)
            .map_err(|()| FormalError::ProtocolError)?
        {
            self.fence.dedup.remember_alias(command);
            return Ok(result);
        }
        if command.user_id != Some(place.order.user_id)
            || place
                .client_order_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > state.config.limits.client_id_bytes)
            || place.reservation.reserves.len() > 3
        {
            return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
        }
        if !state.config.policies.contains_key(&market) {
            return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
        }
        let active = self
            .markets
            .values()
            .try_fold(0usize, |sum, book| {
                sum.checked_add(book.reservation_snapshots().len())
            })
            .ok_or(FormalError::Rejected(RejectReason::EngineBusy))?;
        if active >= state.config.limits.active_orders {
            return Err(FormalError::Rejected(RejectReason::EngineBusy));
        }
        state
            .ids
            .try_clone_preallocated()
            .map_err(|()| FormalError::Rejected(RejectReason::EngineBusy))?;
        Err(FormalError::Rejected(RejectReason::InvalidOrderCombination))
    }
    /// 按单写者实际顺序在候选状态中预检与执行命令。
    fn stage(
        &mut self,
        command: CommandEnvelope,
        routes: &RouteTable,
    ) -> Result<FormalExecution, FormalError> {
        let bootstrap = if self
            .place
            .as_ref()
            .is_some_and(|state| state.market_data.is_none())
        {
            Some(
                crate::MarketDataState::bootstrap_legacy(
                    &self.markets,
                    self.fence.seq.last_applied(),
                )
                .expect("旧快照行情初始化失败，分片停止"),
            )
        } else {
            None
        };
        let mut result = self.stage_inner(command, routes)?;
        let mut bootstrap_events = BTreeMap::new();
        if !result.deduplicated
            && let Some((data, events)) = bootstrap
        {
            self.place.as_mut().expect("已检查下单状态").market_data = Some(data);
            bootstrap_events = events;
        }
        if let Some(data) = self
            .place
            .as_mut()
            .and_then(|state| state.market_data.as_mut())
        {
            let updates = data
                .apply(&result)
                .expect("行情候选与撮合结果不一致，分片停止");
            for (market, events) in updates {
                bootstrap_events
                    .entry(market)
                    .or_insert_with(Vec::new)
                    .extend(events);
            }
            result.market_data_source = bootstrap_events;
        }
        Ok(result)
    }
    /// 执行业务候选；行情候选由调用方在同一批次中推进。
    fn stage_inner(
        &mut self,
        command: CommandEnvelope,
        routes: &RouteTable,
    ) -> Result<FormalExecution, FormalError> {
        if matches!(command.payload, Command::ShardKillSwitch(_))
            || cross_fence::coordinated(&command.payload)
        {
            let context = ShardKillContext {
                identity: FormalIdentity {
                    shard: self.fence.shard,
                    cluster: self.fence.cluster,
                    event_capacity: u64::from(u32::MAX) + 1,
                },
                epoch: self.fence.expected_epoch,
                seq: &mut self.fence.seq,
                clock: &self.fence.clock,
                markets: &mut self.markets,
                registry: &mut self.registry,
                place: &mut self.place,
                dedup: &mut self.fence.dedup,
                require_lease: true,
            };
            return if cross_fence::coordinated(&command.payload) {
                cross_fence::execute(context, &mut self.fence.epochs, command)
            } else {
                execute_shard_kill(context, command)
            };
        }
        if matches!(command.payload, Command::ApplyUserControlFenceShard(_)) {
            return self.fence.stage(command);
        }
        if matches!(command.payload, Command::UpdateFeeSchedule(_)) {
            return execute_fee_schedule_update(
                FormalIdentity {
                    shard: self.fence.shard,
                    cluster: self.fence.cluster,
                    event_capacity: u64::from(u32::MAX) + 1,
                },
                routes,
                &mut self.fence.seq,
                &self.fence.clock,
                &mut self.fence.dedup,
                FeeUpdateState {
                    place: &mut self.place,
                    managed_markets: &self.managed_markets,
                    markets: &self.markets,
                    registry: &self.registry,
                },
                command,
            );
        }
        if matches!(
            command.payload,
            Command::CreateMarket(_)
                | Command::TriggerCircuitBreaker(_)
                | Command::MarketKillSwitch(_)
                | Command::UpdateReferencePrice(_)
                | Command::UpdateMarket(_)
                | Command::StartSession(_)
                | Command::OpenMarket(_)
                | Command::SuspendMarket(_)
                | Command::ResumeMarket(_)
                | Command::EndSession(_)
                | Command::CloseMarket(_)
        ) {
            return execute_market_management(
                MarketManagementContext {
                    controls: Some(&self.fence.epochs),
                    identity: FormalIdentity {
                        shard: self.fence.shard,
                        cluster: self.fence.cluster,
                        event_capacity: u64::from(u32::MAX) + 1,
                    },
                    routes,
                    seq: &mut self.fence.seq,
                    clock: &self.fence.clock,
                    markets: &mut self.markets,
                    managed_markets: &mut self.managed_markets,
                    dedup: &mut self.fence.dedup,
                    place: &mut self.place,
                    registry: &mut self.registry,
                },
                command,
            );
        }
        if let Command::PlaceLimit(ref place) = command.payload {
            let market = command
                .market_id
                .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
            let epoch = command
                .route_epoch
                .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
            routes
                .validate(market, self.fence.shard, epoch)
                .map_err(FormalError::Rejected)?;
            if !self.markets.contains_key(&market) || self.registry.is_none() {
                return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
            }
            self.fence
                .epochs
                .check(place.order.user_id, place.user_control_epoch)
                .map_err(FormalError::Rejected)?;
            let state = self
                .place
                .as_mut()
                .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
            let result = execute_place(
                PlaceExecutionContext {
                    control_killed: self.fence.epochs.killed(place.order.user_id),
                    managed: self.managed_markets.get(&market),
                    identity: FormalIdentity {
                        shard: self.fence.shard,
                        cluster: self.fence.cluster,
                        event_capacity: u64::from(u32::MAX) + 1,
                    },
                    seq: &mut self.fence.seq,
                    clock: &self.fence.clock,
                    markets: &mut self.markets,
                    registry: self.registry.as_mut().expect("已验证登记表"),
                    window: &mut self.fence.dedup,
                    state,
                    durable: &mut None,
                },
                command,
            );
            if matches!(&result, Ok(execution) if matches!(execution.outcome, FormalOutcome::Placed(_)))
                && let Some(managed) = self.managed_markets.get_mut(&market)
            {
                managed.record_referenced_history();
            }
            return result;
        }
        execute_formal_guarded(
            routes,
            FormalIdentity {
                shard: self.fence.shard,
                cluster: self.fence.cluster,
                event_capacity: u64::from(u32::MAX) + 1,
            },
            &mut self.fence.seq,
            &self.fence.clock,
            (&mut self.markets, &self.managed_markets),
            &mut self.registry,
            (command, &mut self.fence.dedup, &mut self.place, &mut None),
        )
    }
}
/// 在完整候选状态中执行有界批次，持久提交后一次安装并顺序回复。
fn execute_formal_group(
    first: (
        CommandEnvelope,
        Option<u64>,
        CommandReply<Result<FormalExecution, FormalError>>,
    ),
    consumer: &mut crate::queue::Consumer<Message>,
    gate: &mut LocalDurableCommit,
    context: FormalGroupContext<'_>,
) -> Option<Message> {
    let (limit, delay) = gate.group.unwrap_or((1, 1));
    let deadline = Instant::now()
        .checked_add(Duration::from_micros(delay))
        .expect("批量时限越界");
    let mut candidate =
        FormalBatchCandidate::new(&context, gate.epoch).expect("批量候选状态分配失败");
    let mut replies = Vec::new();
    replies
        .try_reserve_exact(limit)
        .expect("批量回复空间分配失败");
    let mut pending = Some(first);
    let mut deferred = None;
    loop {
        if let Some((command, admitted, reply)) = pending.take() {
            if consumer.routes_fenced() {
                replies.push((reply, Err(FormalError::Rejected(RejectReason::EngineBusy))));
                break;
            }
            let result = candidate
                .check_live_admission(&command, admitted)
                .and_then(|()| match matching_domain::encode_wal_command(&command) {
                    Err(_) => candidate.reject_uncodable(&command, context.routes),
                    Ok(payload) => {
                        let command_id = command.command_id;
                        let match_started =
                            matches!(&command.payload, Command::PlaceLimit(_)).then(Instant::now);
                        let planned = cross_fence::verify_owner(
                            &command.payload,
                            gate,
                            candidate.place.as_ref(),
                            (context.cluster, context.shard),
                        )
                        .and_then(|()| candidate.stage(command, context.routes));
                        if let (Some(metrics), Some(started)) = (context.metrics, match_started) {
                            metrics.observe_match(started.elapsed());
                        }
                        if let Ok(result) = &planned
                            && !result.deduplicated
                        {
                            let record = matching_persistence::WalRecord {
                                cluster_id: context.cluster.0,
                                shard_id: context.shard.0,
                                flags: 0,
                                epoch: gate.epoch,
                                shard_seq: result.seq,
                                engine_time: context.now.get(),
                                command_id: command_id.0,
                                payload,
                            };
                            wal_or_halt(
                                gate.batcher.append_at(&record, Instant::now()),
                                DiagnosticKind::WalAppend,
                                Some(command_id),
                                Some(result.seq),
                            );
                        }
                        planned
                    }
                });
            replies.push((reply, result));
        }
        if group_should_flush(replies.len(), limit, Instant::now(), deadline, false) {
            break;
        }
        match consumer.pop() {
            Some(Message::Formal(command, admitted, reply)) => {
                pending = Some((command, admitted, reply))
            }
            Some(other) => {
                deferred = Some(other);
                break;
            }
            None => {
                let now = Instant::now();
                if group_should_flush(
                    replies.len(),
                    limit,
                    now,
                    deadline,
                    consumer.routes_fenced(),
                ) {
                    break;
                }
                std::thread::park_timeout(deadline.duration_since(now));
            }
        }
    }
    wal_or_halt(
        gate.batcher.flush(),
        DiagnosticKind::WalSync,
        None,
        Some(candidate.fence.seq.last_applied()),
    );
    assert_eq!(
        gate.batcher.committed(),
        candidate.fence.seq.last_applied(),
        "批量提交水位不连续"
    );
    if candidate.fence.seq.last_applied() > context.seq.last_applied()
        && let Some(event_hasher) = gate.event_hasher.as_mut()
    {
        for (_, result) in &replies {
            if let Ok(execution) = result
                && !execution.deduplicated
            {
                for event in &execution.events {
                    wal_or_halt(
                        event_hasher
                            .update(event)
                            .map_err(|_| std::io::Error::other("事件摘要编码失败")),
                        DiagnosticKind::WalSync,
                        Some(execution.command_id),
                        Some(execution.seq),
                    );
                }
            }
        }
        let image = candidate
            .digest_image(&context)
            .expect("摘要候选状态分配失败");
        let state = image
            .state_digest(256 * 1024 * 1024)
            .expect("状态摘要编码失败");
        let events = event_hasher.clone().finish();
        wal_or_halt(
            gate.batcher
                .checkpoint_digest(candidate.fence.seq.last_applied(), state, events),
            DiagnosticKind::WalSync,
            None,
            Some(candidate.fence.seq.last_applied()),
        );
    }
    *context.epochs = candidate.fence.epochs;
    *context.dedup = candidate.fence.dedup;
    *context.seq = candidate.fence.seq;
    *context.markets = candidate.markets;
    *context.managed_markets = candidate.managed_markets;
    *context.registry = candidate.registry;
    *context.place = candidate.place;
    if let Some(metrics) = context.metrics {
        metrics.shard_halted.store(
            context
                .place
                .as_ref()
                .is_some_and(|state| state.shard_kill_active),
            Ordering::Release,
        );
    }
    for (reply, result) in replies {
        if let Some(metrics) = context.metrics {
            metrics.observe_formal(&result);
        }
        let _ = reply.send(result);
    }
    deferred
}

/// 活单存在时只允许移除价格带或在同一参考来源上放宽偏离范围。
fn active_band_update_safe(
    old: Option<matching_domain::PriceBandConfig>,
    next: Option<matching_domain::PriceBandConfig>,
) -> bool {
    match (old, next) {
        (_, None) => true,
        (Some(old), Some(next)) => {
            old.reference_kind == next.reference_kind
                && next.max_deviation_bps >= old.max_deviation_bps
        }
        (None, Some(_)) => false,
    }
}

/// 根据新参考来源的价格带，确认全部已有价格均在新边界内。
fn active_band_revalidation_safe(
    book: &ProductionOrderBook,
    next: Option<matching_domain::PriceBandConfig>,
    math: matching_domain::MarketMath,
) -> Result<bool, RejectReason> {
    let Some(next) = next else {
        return Ok(false);
    };
    if !book.has_reference_price(next.reference_kind) {
        return Ok(false);
    }
    let candidate = prepare_price_band_update(book, Some(next), math)?;
    let Some((lower, upper)) = candidate.price_band_bounds()? else {
        return Ok(false);
    };
    Ok(book.reservation_snapshots().iter().all(|(id, _)| {
        if let Some(node) = book.order(*id) {
            let price = node.original_order().price;
            return price >= lower && price <= upper;
        }
        match book.stop_order(*id) {
            Some((matching_domain::StopOrder::Limit(order), _)) => {
                order.price >= lower && order.price <= upper
            }
            Some((matching_domain::StopOrder::Market(_), _)) => true,
            None => false,
        }
    }))
}

/// 在副本中发布价格带，并在参考价已存在时预检边界算术。
fn prepare_price_band_update(
    book: &ProductionOrderBook,
    band: Option<matching_domain::PriceBandConfig>,
    math: matching_domain::MarketMath,
) -> Result<ProductionOrderBook, RejectReason> {
    let mut candidate = book.clone();
    candidate.set_price_band_config(band.map(|value| (value, math)))?;
    if band.is_some_and(|value| candidate.has_reference_price(value.reference_kind)) {
        candidate.price_band_bounds()?;
    }
    Ok(candidate)
}

/// 校验已经确认存在的参考价能生成可表示的价格带边界。
fn checked_price_band_reference(
    book: &ProductionOrderBook,
    band: Option<matching_domain::PriceBandConfig>,
) -> Result<(), RejectReason> {
    if let Some(band) = band {
        assert!(
            book.price_band_config()
                .is_some_and(|(current, _)| current == band),
            "市场价格带状态不一致"
        );
        book.price_band_bounds()?;
    }
    Ok(())
}

/// 校验未触发止损单的目标限制；未知成交金额使用原限制或报价预算作为上界。
fn stop_order_fits_limits(
    stop: &matching_domain::StopOrder,
    old: matching_domain::OrderLimits,
    next: matching_domain::OrderLimits,
    math: matching_domain::MarketMath,
) -> bool {
    if stop.stop_price().get() % math.tick_size() != 0 {
        return false;
    }
    match stop {
        matching_domain::StopOrder::Limit(order) => {
            order.qty >= next.min_qty
                && order.qty <= next.max_qty
                && math
                    .trade_notional(order.price, order.qty)
                    .is_ok_and(|amount| amount >= next.min_notional && amount <= next.max_notional)
        }
        matching_domain::StopOrder::Market(order) => match order.quantity {
            matching_domain::StopMarketQuantity::Base(qty) => {
                next.min_notional <= old.min_notional
                    && next.max_notional >= old.max_notional
                    && qty >= next.min_qty
                    && qty <= next.max_qty
                    && qty.get() % math.lot_size() == 0
            }
            matching_domain::StopMarketQuantity::Quote(budget) => {
                next.min_qty <= old.min_qty
                    && next.max_qty >= old.max_qty
                    && next.min_notional <= old.min_notional
                    && (next.max_notional >= old.max_notional
                        || next.max_notional.get() >= budget.get())
            }
        },
    }
}

/// 校验受管理市场的状态、数量和本金限制，不改变订单簿。
fn managed_place_preflight(
    managed: Option<&matching_domain::ManagedMarket>,
    managed_only: bool,
    place: &matching_domain::PlaceLimitOrder,
    book: &ProductionOrderBook,
) -> Result<(), RejectReason> {
    let Some(managed) = managed else {
        return if managed_only {
            Err(RejectReason::MarketNotFound)
        } else {
            Ok(())
        };
    };
    if managed.kill_switch_active() {
        return Err(RejectReason::KillSwitchActive);
    }
    let order = &place.order;
    let crosses = match order.side {
        matching_domain::Side::Buy => book
            .best_price(matching_domain::Side::Sell)
            .is_some_and(|price| order.price >= price),
        matching_domain::Side::Sell => book
            .best_price(matching_domain::Side::Buy)
            .is_some_and(|price| order.price <= price),
    };
    managed
        .lifecycle()
        .check_limit_tif(matching_domain::TimeInForce::Gtc, crosses)?;
    let config = managed.config();
    if order.qty < config.order_limits.min_qty || order.qty > config.order_limits.max_qty {
        return Err(RejectReason::InvalidQty);
    }
    let math = config.validate().expect("已持久化市场配置必须有效");
    let notional = math
        .trade_notional(order.price, order.qty)
        .map_err(|error| {
            error
                .to_reject_reason()
                .expect("有效市场配置必须产生整数本金")
        })?;
    if notional < config.order_limits.min_notional {
        return Err(RejectReason::MinNotional);
    }
    if notional > config.order_limits.max_notional {
        return Err(RejectReason::MaxNotional);
    }
    Ok(())
}

/// 完成预检后执行限价候选，事件容量有效时才一次安装全部后态。
fn execute_place(
    context: PlaceExecutionContext<'_>,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let PlaceExecutionContext {
        control_killed,
        managed,
        identity,
        seq,
        clock,
        markets,
        registry,
        window,
        state,
        durable,
    } = context;
    let Command::PlaceLimit(ref place) = command.payload else {
        unreachable!()
    };
    let market = command.market_id.expect("已验证市场路由");
    if let Some(result) = window
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        window.remember_alias(&command);
        return Ok(result);
    }
    if command.user_id != Some(place.order.user_id) {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    if place
        .client_order_id
        .as_ref()
        .is_some_and(|id| id.is_empty() || id.len() > state.config.limits.client_id_bytes)
        || place.reservation.reserves.len() > 3
    {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let policy = state
        .config
        .policies
        .get(&market)
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    let active = markets
        .values()
        .try_fold(0usize, |sum, book| {
            sum.checked_add(book.reservation_snapshots().len())
        })
        .ok_or(FormalError::Rejected(RejectReason::EngineBusy))?;
    if active >= state.config.limits.active_orders {
        return Err(FormalError::Rejected(RejectReason::EngineBusy));
    }
    let ids = state
        .ids
        .try_clone_preallocated()
        .map_err(|()| FormalError::Rejected(RejectReason::EngineBusy))?;
    let prepared = window.prepare(&command);
    let bytes = if durable.is_some() {
        Some(
            matching_domain::encode_wal_command(&command)
                .map_err(|_| FormalError::Rejected(RejectReason::InvalidOrderCombination))?,
        )
    } else {
        None
    };
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    if let (Some(gate), Some(bytes)) = (durable.as_mut(), bytes) {
        wal_or_halt(
            gate.commit(
                identity.cluster,
                identity.shard,
                clock.now().get(),
                command.command_id,
                n,
                bytes,
            ),
            DiagnosticKind::WalCommit,
            Some(command.command_id),
            Some(n),
        );
    }
    let cursor = TradeIdCursor::new(identity.cluster, identity.shard, n);
    let book = markets.get_mut(&market).expect("已验证市场存在");
    let planned = (if state.shard_kill_active || control_killed {
        Err(RejectReason::KillSwitchActive)
    } else {
        Ok(())
    })
    .and_then(|()| managed_place_preflight(managed, state.config.managed_only, place, book))
    .and_then(|()| {
        let context = PlacePlanningContext {
            policy,
            keys: &state.config.keys,
            command_id: command.command_id,
            session: command.session_id,
            now: clock.now(),
        };
        if let Some(fees) = &state.fee_book {
            crate::place_candidate::place_candidate_with_fees(
                (
                    context,
                    fees,
                    managed.map_or(fees.active_version(), |m| {
                        m.config().active_fee_schedule_version
                    }),
                ),
                place,
                book,
                registry,
                ids,
                &cursor,
            )
        } else {
            place_candidate(context, place, book, registry, ids, &cursor)
        }
    });
    if let (Some(managed), Ok(candidate)) = (managed, &planned) {
        let trade_count = candidate
            .events
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count();
        assert!(
            u64::try_from(trade_count).is_ok_and(|count| count
                <= u64::from(managed.config().execution_limits.max_trades_per_command)),
            "单命令成交数量超过市场安全上限，分片停止"
        );
    }
    let (mut outcome, mut payloads, candidate) = match planned {
        Ok(candidate) => (
            FormalOutcome::Placed(place.order.order_id),
            candidate.events.clone(),
            Some(candidate),
        ),
        Err(reason) => (
            FormalOutcome::Rejected(reason),
            vec![EngineEvent::CommandRejected { reason }],
            None,
        ),
    };
    let overflow = u64::try_from(payloads.len()).map_or(true, |count| {
        !event_count_fits(count, identity.event_capacity)
    });
    if overflow {
        outcome = FormalOutcome::Rejected(RejectReason::ArithmeticOverflow);
        payloads = vec![EngineEvent::CommandRejected {
            reason: RejectReason::ArithmeticOverflow,
        }];
    }
    let events: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| EventEnvelope {
            cluster_id: identity.cluster,
            shard_id: identity.shard,
            shard_seq: n,
            event_index: u32::try_from(index).expect("事件数量已预检"),
            command_id: command.command_id,
            engine_time: clock.now().get(),
            market_id: Some(market),
            payload,
        })
        .collect();
    let (status, reject_reason) = match outcome {
        FormalOutcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason)),
        _ => (CommandStatus::Succeeded, None),
    };
    let mut open_details = Vec::new();
    if !overflow && let Some(candidate) = &candidate {
        for event in &events {
            if let EngineEvent::OrderOpened { order_id } = event.payload {
                let node = candidate
                    .book
                    .order(order_id)
                    .expect("挂单事件必须有对应候选订单");
                open_details.push(MarketDataOpenDetail {
                    event_index: event.event_index,
                    order_id,
                    side: node.original_order().side,
                    price: node.original_order().price,
                    visible: matching_domain::Qty::try_new(node.visible_remaining())
                        .expect("挂单可见量必须为正"),
                });
            }
        }
    }
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome,
        summary: CommandResultSummary {
            status,
            first_event_index: events.first().map(|event| event.event_index),
            last_event_index: events.last().map(|event| event.event_index),
            durable_seq: Some(n),
            reject_reason,
        },
        event_range: events.last().map(|event| (0, event.event_index)),
        events,
        market_data_child_opens: Vec::new(),
        market_data_open_details: open_details,
        market_data_source: Default::default(),
        deduplicated: false,
    };
    if !overflow && let Some(candidate) = candidate {
        *book = candidate.book;
        *registry = candidate.registry;
        state.ids = candidate.ids;
        let _cursor = candidate.cursor;
    }
    window.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 费率更新需要检查的分片状态。
struct FeeUpdateState<'a> {
    /// 保存候选费率簿的下单配置。
    place: &'a mut Option<PlaceRuntimeState>,
    /// 受管理市场及其版本引用。
    managed_markets: &'a BTreeMap<MarketId, matching_domain::ManagedMarket>,
    /// 用于核验活动订单影子的市场簿。
    markets: &'a BTreeMap<MarketId, ProductionOrderBook>,
    /// 活动及等待后置处理的预留。
    registry: &'a Option<ReservationRegistry>,
}

/// 在隔离候选中发布费率版本，并保持前置拒绝不占序号。
fn execute_fee_schedule_update(
    identity: FormalIdentity,
    routes: &RouteTable,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    dedup: &mut DedupWindow,
    FeeUpdateState {
        place,
        managed_markets,
        markets,
        registry,
    }: FeeUpdateState<'_>,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let market = command
        .market_id
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    let epoch = command
        .route_epoch
        .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
    routes
        .validate(market, identity.shard, epoch)
        .map_err(FormalError::Rejected)?;
    if command.user_id.is_some() {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let Command::UpdateFeeSchedule(input) = &command.payload else {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    };
    let state = place
        .as_mut()
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    if !state.config.policies.contains_key(&market) {
        return Err(FormalError::Rejected(RejectReason::MarketNotFound));
    }
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    let prepared = dedup.prepare(&command);
    let version = input.schedule.version();
    let candidate = match &state.fee_book {
        None => matching_domain::FeeScheduleBook::from_history(
            input.history.clone(),
            input.schedule.clone(),
        ),
        Some(_) if !input.history.is_empty() => Err(RejectReason::InvalidOrderCombination),
        Some(book) => {
            let mut next = book.clone();
            next.activate(input.schedule.clone()).map(|()| next)
        }
    };
    let candidate = candidate.and_then(|book| {
        if registry
            .as_ref()
            .is_some_and(|registry| registry.validate_fee_references(&book).is_err())
            || managed_markets.values().any(|market| {
                book.schedule(market.config().active_fee_schedule_version)
                    .is_err()
            })
            || markets.values().any(|market| {
                market
                    .reservation_snapshots()
                    .iter()
                    .any(|(_, reservation)| {
                        book.resolve(reservation.fee_schedule_version, reservation.fee_tier_id)
                            .is_err()
                    })
            })
        {
            return Err(RejectReason::FeeScheduleMismatch);
        }
        Ok(book)
    });
    let (outcome, payload, next_book) = match candidate {
        Ok(book) => (
            FormalOutcome::FeeScheduleUpdated(version),
            EngineEvent::FeeScheduleUpdated { version },
            Some(book),
        ),
        Err(reason) => (
            FormalOutcome::Rejected(reason),
            EngineEvent::CommandRejected { reason },
            None,
        ),
    };
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    let event = EventEnvelope {
        cluster_id: identity.cluster,
        shard_id: identity.shard,
        shard_seq: n,
        event_index: 0,
        command_id: command.command_id,
        engine_time: clock.now().get(),
        market_id: Some(market),
        payload,
    };
    let (status, reject_reason) = match outcome {
        FormalOutcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason)),
        _ => (CommandStatus::Succeeded, None),
    };
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome,
        summary: CommandResultSummary {
            status,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(n),
            reject_reason,
        },
        event_range: Some((0, 0)),
        events: vec![event],
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    if let Some(book) = next_book {
        state.fee_book = Some(book);
    }
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 分片控制操作在候选和恢复路径中使用的上下文。
struct ShardKillContext<'a> {
    /// 当前分片的集群与分片身份。
    identity: FormalIdentity,
    /// 当前执行上下文持有的主节点纪元。
    epoch: u64,
    /// 为通过前置检查的变更分配分片序号。
    seq: &'a mut Sequencer,
    /// 根事件使用的确定性逻辑时间。
    clock: &'a LogicalClock,
    /// 按市场身份排序的分片本地订单簿。
    markets: &'a mut BTreeMap<MarketId, ProductionOrderBook>,
    /// 可选预留登记表及其候选提交位置。
    registry: &'a mut Option<ReservationRegistry>,
    /// 可选正式下单状态及其租约和身份索引。
    place: &'a mut Option<PlaceRuntimeState>,
    /// 记录已执行命令结果的有限去重窗口。
    dedup: &'a mut DedupWindow,
    /// 实时执行时要求校验所持租约；回放使用已验证的持久纪元。
    require_lease: bool,
}

/// 校验租约并在同一候选中应用分片开关及可选撤单。
fn execute_shard_kill(
    context: ShardKillContext<'_>,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let ShardKillContext {
        identity,
        epoch,
        seq,
        clock,
        markets,
        registry,
        place,
        dedup,
        require_lease,
    } = context;
    let Command::ShardKillSwitch(input) = &command.payload else {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    };
    let state = place
        .as_ref()
        .ok_or(FormalError::Rejected(RejectReason::StalePrimaryEpoch))?;
    if input.target_shard_id != identity.shard
        || input.expected_primary_epoch != epoch
        || (require_lease
            && state.config.primary_lease.as_ref().is_none_or(|lease| {
                lease
                    .verify(identity.cluster, identity.shard, epoch)
                    .is_err()
            }))
    {
        return Err(FormalError::Rejected(RejectReason::StalePrimaryEpoch));
    }
    if command.market_id.is_some()
        || command.route_epoch.is_some()
        || command.user_id.is_some()
        || command.command_id
            != crate::ControlCoordinator::derive_shard_kill_id(
                identity.cluster,
                identity.shard,
                input.parent_command_id,
                input.active,
            )
    {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    let prepared = dedup.prepare(&command);
    let mut next_books = markets.clone();
    let mut next_registry = registry.clone();
    let mut next_ids = state
        .ids
        .try_clone_preallocated()
        .map_err(|()| FormalError::Rejected(RejectReason::EngineBusy))?;
    let mut payloads = Vec::new();
    let plan: Result<(), RejectReason> = (|| {
        if !input.active && input.cancel_all {
            return Err(RejectReason::InvalidOrderCombination);
        }
        let scope = matching_domain::KillScope::Shard(identity.shard);
        payloads.push((
            None,
            if input.active {
                EngineEvent::KillSwitchActivated { scope }
            } else {
                EngineEvent::KillSwitchCleared { scope }
            },
        ));
        if input.active && input.cancel_all {
            let candidate_registry = next_registry
                .as_mut()
                .ok_or(RejectReason::InvalidReservation)?;
            for (market, book) in &mut next_books {
                let before = book.clone();
                let plan = book.plan_kill_switch_cancel(candidate_registry)?;
                let (_, canceled) = book
                    .apply_mass_cancel(candidate_registry, plan)
                    .expect("独立分片撤单候选不能漂移");
                for event in &canceled {
                    if let EngineEvent::OrderCanceled { order_id, .. } = event {
                        next_ids.terminal_in_market(*market, *order_id);
                    }
                }
                payloads.extend(
                    crate::place_candidate::release_terminal_reservations(
                        &before,
                        book,
                        candidate_registry,
                        canceled,
                    )?
                    .into_iter()
                    .map(|event| (Some(*market), event)),
                );
            }
        }
        Ok(())
    })();
    let reason = match plan {
        Err(reason) => Some(reason),
        Ok(())
            if !u64::try_from(payloads.len())
                .is_ok_and(|n| event_count_fits(n, identity.event_capacity)) =>
        {
            Some(RejectReason::ArithmeticOverflow)
        }
        Ok(()) => None,
    };
    if let Some(reason) = reason {
        payloads = vec![(None, EngineEvent::CommandRejected { reason })];
    }
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    let events: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, (market_id, payload))| EventEnvelope {
            cluster_id: identity.cluster,
            shard_id: identity.shard,
            shard_seq: n,
            event_index: u32::try_from(index).expect("事件数量已预检"),
            command_id: command.command_id,
            engine_time: clock.now().get(),
            market_id,
            payload,
        })
        .collect();
    let outcome = reason.map_or(FormalOutcome::ShardControl, FormalOutcome::Rejected);
    let event_range = events.last().map(|event| (0, event.event_index));
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome,
        summary: CommandResultSummary {
            status: if reason.is_some() {
                CommandStatus::Rejected
            } else {
                CommandStatus::Succeeded
            },
            first_event_index: events.first().map(|event| event.event_index),
            last_event_index: events.last().map(|event| event.event_index),
            durable_seq: Some(n),
            reject_reason: reason,
        },
        event_range,
        events,
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    if reason.is_none() {
        *markets = next_books;
        *registry = next_registry;
        let state = place.as_mut().expect("已验证分片控制配置");
        state.ids = next_ids;
        state.shard_kill_active = input.active;
    }
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 市场管理命令在当前独占候选内需要的上下文。
struct MarketManagementContext<'a> {
    /// 分片持久用户及全局开关的当前视图，供 Stop 子单重校验。
    controls: Option<&'a UserControlEpochs>,
    /// 根事件的分片和容量约束。
    identity: FormalIdentity,
    /// 不可变的路由视图。
    routes: &'a RouteTable,
    /// 候选序号分配器。
    seq: &'a mut Sequencer,
    /// 当前逻辑时钟。
    clock: &'a LogicalClock,
    /// 候选普通订单簿与止损簿。
    markets: &'a mut BTreeMap<MarketId, ProductionOrderBook>,
    /// 候选市场配置和生命周期。
    managed_markets: &'a mut BTreeMap<MarketId, matching_domain::ManagedMarket>,
    /// 候选命令幂等窗口。
    dedup: &'a mut DedupWindow,
    /// 可信下单风险策略。
    place: &'a mut Option<PlaceRuntimeState>,
    /// 活动订单关联的预留登记表。
    registry: &'a mut Option<ReservationRegistry>,
}

/// 在独立候选中执行已接入的市场管理命令；外层负责 WAL 提交与安装。
fn execute_market_management(
    context: MarketManagementContext<'_>,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let MarketManagementContext {
        controls,
        identity,
        routes,
        seq,
        clock,
        markets,
        managed_markets,
        dedup,
        place,
        registry,
    } = context;
    let market = command
        .market_id
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    let route_epoch = command
        .route_epoch
        .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
    routes
        .validate(market, identity.shard, route_epoch)
        .map_err(FormalError::Rejected)?;
    let payload_market = match &command.payload {
        Command::CreateMarket(input) => input.config.market_id,
        Command::UpdateMarket(input) => input.config.market_id,
        Command::StartSession(input) => input.market_id,
        Command::OpenMarket(input) => input.market_id,
        Command::SuspendMarket(input) => input.market_id,
        Command::ResumeMarket(input) => input.market_id,
        Command::EndSession(input) => input.market_id,
        Command::CloseMarket(input) => input.market_id,
        Command::UpdateReferencePrice(input) => input.market_id,
        Command::MarketKillSwitch(input) => input.market_id,
        Command::TriggerCircuitBreaker(input) => input.market_id,
        _ => return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination)),
    };
    if payload_market != market || command.user_id.is_some() {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let book = markets
        .get(&market)
        .ok_or(FormalError::Rejected(RejectReason::MarketNotFound))?;
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    let prepared = dedup.prepare(&command);
    let empty = book.best_price(matching_domain::Side::Buy).is_none()
        && book.best_price(matching_domain::Side::Sell).is_none()
        && book.stop_orders(matching_domain::Side::Buy).is_empty()
        && book.stop_orders(matching_domain::Side::Sell).is_empty();
    let mut cleanup = None;
    let mut child_opens = Vec::new();
    let mut policy_update = None;
    let plan: Result<_, RejectReason> = match &command.payload {
        Command::TriggerCircuitBreaker(input) => match managed_markets.get(&market) {
            Some(state) => {
                let mut candidate = state.clone();
                candidate
                    .trigger_circuit_breaker(input.reason)
                    .map(|events| (candidate, None, events))
            }
            None => Err(RejectReason::MarketNotFound),
        },
        Command::MarketKillSwitch(input) => (|| {
            if !input.active && input.cancel_all {
                return Err(RejectReason::InvalidOrderCombination);
            }
            let state = managed_markets
                .get(&market)
                .ok_or(RejectReason::MarketNotFound)?;
            let mut candidate = state.clone();
            candidate.set_kill_switch(input.active);
            let scope = matching_domain::KillScope::Market(market);
            let mut events = vec![if input.active {
                EngineEvent::KillSwitchActivated { scope }
            } else {
                EngineEvent::KillSwitchCleared { scope }
            }];
            let mut candidate_book = None;
            if input.active && input.cancel_all {
                let mut next_registry = registry
                    .as_ref()
                    .ok_or(RejectReason::InvalidReservation)?
                    .clone();
                let mut next_ids = place
                    .as_ref()
                    .ok_or(RejectReason::InvalidOrderCombination)?
                    .ids
                    .try_clone_preallocated()
                    .map_err(|()| RejectReason::EngineBusy)?;
                let mut after = book.clone();
                let plan = after.plan_kill_switch_cancel(&next_registry)?;
                let (_, canceled) = after
                    .apply_mass_cancel(&mut next_registry, plan)
                    .expect("独立熔断撤单候选不能漂移");
                for event in &canceled {
                    if let EngineEvent::OrderCanceled { order_id, .. } = event {
                        next_ids.terminal_in_market(market, *order_id);
                    }
                }
                events.extend(crate::place_candidate::release_terminal_reservations(
                    book,
                    &after,
                    &mut next_registry,
                    canceled,
                )?);
                cleanup = Some((next_registry, next_ids));
                candidate_book = Some(after);
            }
            Ok((candidate, candidate_book, events))
        })(),
        Command::UpdateReferencePrice(input) => (|| {
            let state = managed_markets
                .get(&market)
                .ok_or(RejectReason::MarketNotFound)?;
            let reference = matching_domain::PriceReference {
                kind: input.kind,
                price: input.price,
                source_seq: input.source_seq,
            };
            let without_trigger = book.plan_external_reference_without_trigger(reference);
            let has_crossing =
                matches!(&without_trigger, Err(RejectReason::InvalidOrderCombination));
            let mut events = Vec::new();
            let mut revalidated = false;
            let mut candidate_book = if has_crossing
                && matches!(
                    reference.kind,
                    matching_domain::PriceReferenceKind::MarkPrice
                        | matching_domain::PriceReferenceKind::IndexPrice
                )
                && matches!(
                    state.lifecycle().state(),
                    matching_domain::MarketState::Open
                        | matching_domain::MarketState::PreOpen
                        | matching_domain::MarketState::Halted
                        | matching_domain::MarketState::Suspended
                ) {
                let state_place = place
                    .as_ref()
                    .ok_or(RejectReason::InvalidOrderCombination)?;
                let policy = state_place
                    .config
                    .policies
                    .get(&market)
                    .ok_or(RejectReason::InvalidReservation)?;
                let fees = state_place
                    .fee_book
                    .as_ref()
                    .ok_or(RejectReason::FeeScheduleNotFound)?;
                let original_registry =
                    registry.as_ref().ok_or(RejectReason::InvalidReservation)?;
                let mut candidate_registry = original_registry.clone();
                let mut candidate_ids = state_place
                    .ids
                    .try_clone_preallocated()
                    .map_err(|()| RejectReason::EngineBusy)?;
                let math = policy.market_math();
                let (base, quote) = policy.assets();
                let bounds = match state.config().price_band {
                    Some(band) if band.reference_kind == reference.kind => {
                        Some(band.bounds(reference.price, math.tick_size())?)
                    }
                    Some(_) => book.price_band_bounds()?,
                    None => None,
                };
                let mut working = book.clone();
                if state.lifecycle().state() == matching_domain::MarketState::PreOpen
                    && state
                        .config()
                        .price_band
                        .is_some_and(|band| band.reference_kind == reference.kind)
                {
                    let (lower, upper) = bounds.ok_or(RejectReason::ReferencePriceUnavailable)?;
                    let revalidation =
                        working.plan_preopen_revalidation(lower, upper, &candidate_registry)?;
                    let (_, canceled) = working
                        .apply_mass_cancel(&mut candidate_registry, revalidation)
                        .expect("独立重校验候选不能漂移");
                    for event in &canceled {
                        if let EngineEvent::OrderCanceled { order_id, .. } = event {
                            candidate_ids.terminal_in_market(market, *order_id);
                        }
                    }
                    events = crate::place_candidate::release_terminal_reservations(
                        book,
                        &working,
                        &mut candidate_registry,
                        canceled,
                    )?;
                    revalidated = true;
                }
                let root_seq = seq
                    .last_applied()
                    .checked_add(1)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                let cursor = TradeIdCursor::new(identity.cluster, identity.shard, root_seq);
                let session = state
                    .lifecycle()
                    .session_id()
                    .ok_or(RejectReason::InvalidSession)?;
                let limits = state.config().execution_limits;
                let (plan, _, mut opens) = working.plan_trigger_stops_with_r2_and_cursor_traced_limited(
                    reference,
                    matching_orderbook::TriggerRegistryContext { now: clock.now(), current_session_id: session, math: &math, trade_ids: &[] },
                    matching_risk::TradeSettlementContext { market_id: market, math, base: matching_risk::SettlementAsset { asset: base, scale: math.qty_scale() }, quote: matching_risk::SettlementAsset { asset: quote, scale: math.quote_scale() }, fees },
                    &candidate_registry, (&cursor, (limits.max_internal_actions_per_command, limits.max_trades_per_command)),
                    |stop, current| {
                        let user = match stop { matching_domain::StopOrder::Limit(order) => order.user_id, matching_domain::StopOrder::Market(order) => order.user_id };
                        if state.kill_switch_active() || state_place.shard_kill_active || controls.is_some_and(|c| c.killed(user)) { return Err(RejectReason::KillSwitchActive); }
                        match stop {
                        matching_domain::StopOrder::Limit(child) => {
                            let crosses = match child.side {
                                matching_domain::Side::Buy => current.best_price(matching_domain::Side::Sell).is_some_and(|price| child.price >= price),
                                matching_domain::Side::Sell => current.best_price(matching_domain::Side::Buy).is_some_and(|price| child.price <= price),
                            };
                            state.lifecycle().check_limit_tif(child.child_tif, crosses)?;
                            matching_risk::check_stop_limit_reservation(policy, fees, &candidate_registry, market, child)
                        },
                        matching_domain::StopOrder::Market(child) => {
                            state.lifecycle().check_market_order()?;
                            let context = matching_domain::market_order::CommandStartProtectionContext { reference_price_at_command_start: reference.price, market_band_worst: bounds.map(|(lower, upper)| if child.side == matching_domain::Side::Buy { upper } else { lower }) };
                            let worst = matching_domain::market_order::effective_protection_at_command_start(child.market_protection.as_ref(), &context, child.side, &math)?;
                            matching_risk::check_stop_market_reservation(policy, fees, &candidate_registry, market, child, worst)
                        }
                    }},
                )?;
                let mut after = working.clone();
                let triggered = after
                    .apply_trigger_stops_with_registry(&mut candidate_registry, plan)
                    .expect("独立止损候选不能漂移");
                for open in &mut opens {
                    open.after_event_count = open
                        .after_event_count
                        .checked_add(events.len())
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                }
                child_opens = opens;
                assert!(
                    triggered
                        .iter()
                        .filter(|event| matches!(event, EngineEvent::StopTriggered { .. }))
                        .count()
                        <= limits.max_internal_actions_per_command as usize,
                    "止损级联超过内部动作上限，分片停止"
                );
                assert!(
                    triggered
                        .iter()
                        .filter(|event| matches!(event, EngineEvent::Trade(_)))
                        .count()
                        <= limits.max_trades_per_command as usize,
                    "止损级联超过成交上限，分片停止"
                );
                for (id, _) in working.reservation_snapshots() {
                    if after.reservation_state(id).is_none() {
                        candidate_ids.terminal_in_market(market, id);
                    }
                }
                events.extend(triggered);
                cleanup = Some((candidate_registry, candidate_ids));
                after
            } else {
                without_trigger?
            };
            let mut candidate_state = state.clone();
            candidate_state.record_referenced_history();
            if !revalidated
                && state.lifecycle().state() == matching_domain::MarketState::PreOpen
                && state
                    .config()
                    .price_band
                    .is_some_and(|band| band.reference_kind == input.kind)
            {
                let (lower, upper) = candidate_book
                    .price_band_bounds()?
                    .ok_or(RejectReason::ReferencePriceUnavailable)?;
                let mut candidate_registry = registry
                    .as_ref()
                    .ok_or(RejectReason::InvalidReservation)?
                    .clone();
                let mut candidate_ids = place
                    .as_ref()
                    .ok_or(RejectReason::InvalidOrderCombination)?
                    .ids
                    .try_clone_preallocated()
                    .map_err(|()| RejectReason::EngineBusy)?;
                let revalidation =
                    candidate_book.plan_preopen_revalidation(lower, upper, &candidate_registry)?;
                let (_, canceled) = candidate_book
                    .apply_mass_cancel(&mut candidate_registry, revalidation)
                    .expect("独立重校验候选不能漂移");
                for event in &canceled {
                    if let EngineEvent::OrderCanceled { order_id, .. } = event {
                        candidate_ids.terminal_in_market(market, *order_id);
                    }
                }
                events = crate::place_candidate::release_terminal_reservations(
                    book,
                    &candidate_book,
                    &mut candidate_registry,
                    canceled,
                )?;
                cleanup = Some((candidate_registry, candidate_ids));
            }
            Ok((candidate_state, Some(candidate_book), events))
        })(),
        Command::CreateMarket(input) => {
            if managed_markets.contains_key(&market)
                || !empty
                || book.next_queue_priority() != 0
                || book.next_stop_priority() != 0
            {
                Err(RejectReason::InvalidMarketStateTransition)
            } else {
                let config = &input.config;
                let policy = place.as_ref().and_then(|p| p.config.policies.get(&market));
                let version_available = place
                    .as_ref()
                    .and_then(|p| p.fee_book.as_ref())
                    .map(|fees| fees.schedule(config.active_fee_schedule_version).is_ok())
                    .unwrap_or_else(|| {
                        policy.is_some_and(|p| {
                            p.fee_schedule_version() == config.active_fee_schedule_version
                        })
                    });
                let expected_priority =
                    matching_orderbook::QueuePriorityPolicy::try_with_synthetic_ceiling(
                        config.execution_limits.priority_exhaustion_guard,
                        config.execution_limits.synthetic_priority_ceiling,
                    );
                match (config.validate(), policy, expected_priority) {
                    (Err(error), _, _) => Err(error.to_reject_reason()),
                    (Ok(math), Some(policy), Ok(priority))
                        if policy.market_math() == math
                            && version_available
                            && book.priority_policy() == priority =>
                    {
                        prepare_price_band_update(book, config.price_band, math)
                            .and_then(|candidate| {
                                matching_domain::ManagedMarket::new(config.clone())
                                    .map_err(|_| RejectReason::InvalidOrderCombination)
                                    .map(|state| (state, candidate))
                            })
                            .map(|(state, candidate)| {
                                (
                                    state,
                                    Some(candidate),
                                    vec![EngineEvent::MarketCreated { market_id: market }],
                                )
                            })
                    }
                    _ => Err(RejectReason::InvalidOrderCombination),
                }
            }
        }
        Command::UpdateMarket(input) => (|| {
            let state = managed_markets
                .get(&market)
                .ok_or(RejectReason::MarketNotFound)?;
            let config = &input.config;
            let math = config
                .validate()
                .map_err(matching_domain::MarketConfigError::to_reject_reason)?;
            let policy = place
                .as_ref()
                .and_then(|p| p.config.policies.get(&market))
                .ok_or(RejectReason::InvalidOrderCombination)?;
            let priority = matching_orderbook::QueuePriorityPolicy::try_with_synthetic_ceiling(
                config.execution_limits.priority_exhaustion_guard,
                config.execution_limits.synthetic_priority_ceiling,
            )
            .map_err(|_| RejectReason::InvalidOrderCombination)?;
            let next_policy = policy
                .with_market_math(math)
                .map_err(|_| RejectReason::InvalidOrderCombination)?;
            let version_available = place
                .as_ref()
                .and_then(|p| p.fee_book.as_ref())
                .map(|fees| fees.schedule(config.active_fee_schedule_version).is_ok())
                .unwrap_or_else(|| {
                    policy.fee_schedule_version() == config.active_fee_schedule_version
                });
            if !version_available {
                return Err(RejectReason::InvalidOrderCombination);
            }
            if !empty {
                book.plan_mass_cancel(
                    matching_domain::ShardCancelScope::Market { market_id: market },
                    registry.as_ref().ok_or(RejectReason::InvalidReservation)?,
                )?;
                if !active_band_update_safe(state.config().price_band, config.price_band)
                    && !active_band_revalidation_safe(book, config.price_band, math)?
                {
                    return Err(RejectReason::InvalidOrderCombination);
                }
            }
            let reservations = book.reservation_snapshots();
            let existing_orders_valid = if empty {
                true
            } else {
                !reservations.is_empty()
                    && reservations.iter().all(|(id, _)| {
                        let Some(node) = book.order(*id) else {
                            return book.stop_order(*id).is_some_and(|(stop, _)| {
                                stop_order_fits_limits(
                                    stop,
                                    state.config().order_limits,
                                    config.order_limits,
                                    math,
                                )
                            });
                        };
                        let order = node.original_order();
                        let limits = config.order_limits;
                        order.qty >= limits.min_qty
                            && order.qty <= limits.max_qty
                            && math
                                .trade_notional(order.price, order.qty)
                                .is_ok_and(|notional| {
                                    notional >= limits.min_notional
                                        && notional <= limits.max_notional
                                })
                    })
            };
            let mut candidate = state.clone();
            let has_referenced_history = state.has_referenced_history()
                || book.next_queue_priority() != 0
                || book.next_stop_priority() != 0
                || [
                    matching_domain::PriceReferenceKind::LastTrade,
                    matching_domain::PriceReferenceKind::MarkPrice,
                    matching_domain::PriceReferenceKind::IndexPrice,
                    matching_domain::PriceReferenceKind::StaticOpening,
                ]
                .into_iter()
                .any(|kind| book.has_reference_price(kind));
            let events = candidate
                .update_config(
                    config.clone(),
                    matching_domain::MarketConfigUpdateChecks {
                        has_active_orders: !empty,
                        has_referenced_history,
                        existing_orders_valid,
                        fee_version_available: true,
                    },
                )
                .map_err(|_| RejectReason::InvalidOrderCombination)?;
            let mut candidate_book = prepare_price_band_update(book, config.price_band, math)?;
            candidate_book.update_priority_policy(priority)?;
            policy_update = Some(next_policy);
            Ok((candidate, Some(candidate_book), events))
        })(),
        Command::StartSession(input) => match managed_markets.get(&market) {
            Some(state) => {
                let mut candidate = state.clone();
                candidate
                    .start_session(input.new_session_id, empty)
                    .map(|events| (candidate, None, events))
            }
            None => Err(RejectReason::MarketNotFound),
        },
        Command::OpenMarket(input) => match managed_markets.get(&market) {
            Some(state) => {
                let mut candidate = state.clone();
                let reference = state
                    .config()
                    .price_band
                    .is_none_or(|band| book.has_reference_price(band.reference_kind));
                let revalidated = if empty || state.config().price_band.is_none() {
                    Ok(true)
                } else if !reference {
                    Ok(false)
                } else {
                    registry
                        .as_ref()
                        .ok_or(RejectReason::InvalidReservation)
                        .and_then(|registry| {
                            book.plan_mass_cancel(
                                matching_domain::ShardCancelScope::Market { market_id: market },
                                registry,
                            )
                        })
                        .and_then(|_| {
                            active_band_revalidation_safe(
                                book,
                                state.config().price_band,
                                state.config().validate().expect("已持久化市场配置必须有效"),
                            )
                        })
                };
                revalidated.and_then(|complete| {
                    candidate
                        .open(input.session_id, reference, complete)
                        .and_then(|events| {
                            checked_price_band_reference(book, state.config().price_band)
                                .map(|()| (candidate, None, events))
                        })
                })
            }
            None => Err(RejectReason::MarketNotFound),
        },
        Command::SuspendMarket(_) => match managed_markets.get(&market) {
            Some(state) => {
                let mut candidate = state.clone();
                candidate.suspend().map(|events| (candidate, None, events))
            }
            None => Err(RejectReason::MarketNotFound),
        },
        Command::ResumeMarket(_) => match managed_markets.get(&market) {
            Some(state) => {
                let mut candidate = state.clone();
                let reference = state
                    .config()
                    .price_band
                    .is_none_or(|band| book.has_reference_price(band.reference_kind));
                (|| {
                    let mut complete = true;
                    if state.lifecycle().preopen_revalidation_required()
                        && !empty
                        && state.config().price_band.is_some()
                        && reference
                    {
                        let bound_registry =
                            registry.as_ref().ok_or(RejectReason::InvalidReservation)?;
                        book.plan_mass_cancel(
                            matching_domain::ShardCancelScope::Market { market_id: market },
                            bound_registry,
                        )?;
                        complete = active_band_revalidation_safe(
                            book,
                            state.config().price_band,
                            state.config().validate().expect("已持久化市场配置必须有效"),
                        )?;
                    }
                    let events = candidate.resume_after_revalidation(reference, complete)?;
                    checked_price_band_reference(book, state.config().price_band)?;
                    Ok((candidate, None, events))
                })()
            }
            None => Err(RejectReason::MarketNotFound),
        },
        Command::EndSession(input) => (|| {
            let state = managed_markets
                .get(&market)
                .ok_or(RejectReason::MarketNotFound)?;
            let mut candidate = state.clone();
            candidate.end_session(input.session_id)?;
            let mut candidate_registry = registry
                .as_ref()
                .ok_or(RejectReason::InvalidReservation)?
                .clone();
            let mut candidate_ids = place
                .as_ref()
                .ok_or(RejectReason::InvalidOrderCombination)?
                .ids
                .try_clone_preallocated()
                .map_err(|()| RejectReason::EngineBusy)?;
            let mut candidate_book = book.clone();
            let plan = candidate_book.plan_end_session(
                input.session_id,
                input.session_id,
                &candidate_registry,
            )?;
            let (_, mut events) = candidate_book
                .apply_mass_cancel(&mut candidate_registry, plan)
                .expect("独立候选的时段终结计划不能漂移");
            for event in &events {
                if let EngineEvent::OrderExpired { order_id, .. } = event {
                    candidate_ids.terminal_in_market(market, *order_id);
                }
            }
            if place.as_ref().is_some_and(|p| p.fee_book.is_some()) {
                events = crate::place_candidate::release_terminal_reservations(
                    book,
                    &candidate_book,
                    &mut candidate_registry,
                    events,
                )?;
            }
            events.push(EngineEvent::SessionEnded {
                market_id: market,
                session_id: input.session_id,
            });
            cleanup = Some((candidate_registry, candidate_ids));
            Ok((candidate, Some(candidate_book), events))
        })(),
        Command::CloseMarket(_) => (|| {
            let state = managed_markets
                .get(&market)
                .ok_or(RejectReason::MarketNotFound)?;
            let mut candidate = state.clone();
            let state_events = candidate.close(true)?;
            let session = state
                .lifecycle()
                .session_id()
                .ok_or(RejectReason::InvalidSession)?;
            let mut candidate_registry = registry
                .as_ref()
                .ok_or(RejectReason::InvalidReservation)?
                .clone();
            let mut candidate_ids = place
                .as_ref()
                .ok_or(RejectReason::InvalidOrderCombination)?
                .ids
                .try_clone_preallocated()
                .map_err(|()| RejectReason::EngineBusy)?;
            let mut candidate_book = book.clone();
            let close_plan = candidate_book.plan_market_close(session, &candidate_registry)?;
            let (_, mut events) = candidate_book
                .apply_mass_cancel(&mut candidate_registry, close_plan)
                .expect("独立候选的关市计划不能漂移");
            for event in &events {
                match event {
                    EngineEvent::OrderCanceled { order_id, .. }
                    | EngineEvent::OrderExpired { order_id, .. } => {
                        candidate_ids.terminal_in_market(market, *order_id)
                    }
                    _ => unreachable!("关市计划只生成订单终结事件"),
                }
            }
            if place.as_ref().is_some_and(|p| p.fee_book.is_some()) {
                events = crate::place_candidate::release_terminal_reservations(
                    book,
                    &candidate_book,
                    &mut candidate_registry,
                    events,
                )?;
            }
            if state.lifecycle().session_active() {
                events.push(EngineEvent::SessionEnded {
                    market_id: market,
                    session_id: session,
                });
            }
            events.extend(state_events);
            cleanup = Some((candidate_registry, candidate_ids));
            Ok((candidate, Some(candidate_book), events))
        })(),
        _ => unreachable!("调用方仅传入已接入的市场命令"),
    };
    let (outcome, payloads, planned) = match plan {
        Ok((state, book, events))
            if u64::try_from(events.len())
                .is_ok_and(|n| event_count_fits(n, identity.event_capacity)) =>
        {
            (FormalOutcome::MarketManaged, events, Some((state, book)))
        }
        Ok(_) => (
            FormalOutcome::Rejected(RejectReason::ArithmeticOverflow),
            vec![EngineEvent::CommandRejected {
                reason: RejectReason::ArithmeticOverflow,
            }],
            None,
        ),
        Err(reason) => (
            FormalOutcome::Rejected(reason),
            vec![EngineEvent::CommandRejected { reason }],
            None,
        ),
    };
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    let events: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| EventEnvelope {
            cluster_id: identity.cluster,
            shard_id: identity.shard,
            shard_seq: n,
            event_index: u32::try_from(index).expect("事件数量已预检"),
            command_id: command.command_id,
            engine_time: clock.now().get(),
            market_id: Some(market),
            payload,
        })
        .collect();
    let (status, reject_reason) = match outcome {
        FormalOutcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason)),
        _ => (CommandStatus::Succeeded, None),
    };
    let event_range = events.last().map(|event| (0, event.event_index));
    if planned.is_none() {
        child_opens.clear();
    }
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome,
        summary: CommandResultSummary {
            status,
            first_event_index: events.first().map(|event| event.event_index),
            last_event_index: events.last().map(|event| event.event_index),
            durable_seq: Some(n),
            reject_reason,
        },
        event_range,
        events,
        market_data_child_opens: child_opens,
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    if let Some((state, candidate_book)) = planned {
        if let Some(candidate_book) = candidate_book {
            markets.insert(market, candidate_book);
        }
        managed_markets.insert(market, state);
        if let Some(policy) = policy_update {
            place
                .as_mut()
                .expect("更新候选已验证下单配置")
                .config
                .policies
                .insert(market, policy);
        }
        if let Some((candidate_registry, candidate_ids)) = cleanup {
            *registry = Some(candidate_registry);
            place.as_mut().expect("关市候选已验证下单状态").ids = candidate_ids;
        }
    }
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 栅栏在去重后先构造候选状态，再分配序号并原子安装。
fn execute_fence(
    epochs: &mut UserControlEpochs,
    dedup: &mut DedupWindow,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    durable: &mut Option<LocalDurableCommit>,
    identity: FormalIdentity,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let Command::ApplyUserControlFenceShard(ref fence) = command.payload else {
        unreachable!()
    };
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    if command.market_id.is_some()
        || command.route_epoch.is_some()
        || command.user_id != Some(fence.user_id)
        || fence.action != matching_domain::UserFenceAction::FenceOnly
    {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let mut candidate = epochs.clone();
    let epoch = candidate
        .advance(fence.user_id, fence.new_epoch)
        .map_err(FormalError::Rejected)?;
    let prepared = dedup.prepare(&command);
    let bytes = if durable.is_some() {
        Some(
            matching_domain::encode_wal_command(&command)
                .map_err(|_| FormalError::Rejected(RejectReason::InvalidOrderCombination))?,
        )
    } else {
        None
    };
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    if let (Some(gate), Some(bytes)) = (durable.as_mut(), bytes) {
        wal_or_halt(
            gate.commit(
                identity.cluster,
                identity.shard,
                clock.now().get(),
                command.command_id,
                n,
                bytes,
            ),
            DiagnosticKind::WalCommit,
            Some(command.command_id),
            Some(n),
        );
    }
    let event = EventEnvelope {
        cluster_id: identity.cluster,
        shard_id: identity.shard,
        shard_seq: n,
        event_index: 0,
        command_id: command.command_id,
        engine_time: clock.now().get(),
        market_id: None,
        payload: EngineEvent::UserControlEpochAdvanced {
            user_id: fence.user_id,
            epoch,
        },
    };
    let summary = CommandResultSummary {
        status: CommandStatus::Succeeded,
        first_event_index: Some(0),
        last_event_index: Some(0),
        durable_seq: Some(n),
        reject_reason: None,
    };
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome: FormalOutcome::UserFence(epoch),
        summary,
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    *epochs = candidate;
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 先核对市场路由、撤单身份和市场生命周期，再将单笔或批量撤单交给候选执行。
fn execute_formal_guarded(
    routes: &RouteTable,
    identity: FormalIdentity,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    (markets, managed): (
        &mut BTreeMap<MarketId, ProductionOrderBook>,
        &BTreeMap<MarketId, matching_domain::ManagedMarket>,
    ),
    registry: &mut Option<ReservationRegistry>,
    (command, dedup, place, durable): (
        CommandEnvelope,
        &mut DedupWindow,
        &mut Option<PlaceRuntimeState>,
        &mut Option<LocalDurableCommit>,
    ),
) -> Result<FormalExecution, FormalError> {
    let market = command
        .market_id
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    let epoch = command
        .route_epoch
        .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
    routes
        .validate(market, identity.shard, epoch)
        .map_err(FormalError::Rejected)?;
    if !markets.contains_key(&market) || registry.is_none() {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    if !valid_cancel_envelope(&command, market, place.as_ref()) {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let reason = match managed.get(&market) {
        Some(state) => state.lifecycle().check_cancel().err(),
        None if place
            .as_ref()
            .is_some_and(|state| state.config.managed_only) =>
        {
            Some(RejectReason::MarketNotFound)
        }
        None => None,
    };
    let Some(reason) = reason else {
        return execute_formal_idempotent(
            routes,
            identity,
            seq,
            clock,
            markets,
            registry,
            (command, dedup, place, durable),
        );
    };
    assert!(durable.is_none(), "受管理撤单拒绝必须在持久化候选中执行");
    let prepared = dedup.prepare(&command);
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    let event = EventEnvelope {
        cluster_id: identity.cluster,
        shard_id: identity.shard,
        shard_seq: n,
        event_index: 0,
        command_id: command.command_id,
        engine_time: clock.now().get(),
        market_id: Some(market),
        payload: EngineEvent::CommandRejected { reason },
    };
    let result = FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome: FormalOutcome::Rejected(reason),
        summary: CommandResultSummary {
            status: CommandStatus::Rejected,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(n),
            reject_reason: Some(reason),
        },
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 在去重窗口命中时返回原摘要；首次执行校验可选客户端别名，并按终结事件更新身份索引。
fn execute_formal_idempotent(
    routes: &RouteTable,
    identity: FormalIdentity,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    markets: &mut BTreeMap<MarketId, ProductionOrderBook>,
    registry: &mut Option<ReservationRegistry>,
    (command, dedup, place_state, durable): (
        CommandEnvelope,
        &mut DedupWindow,
        &mut Option<PlaceRuntimeState>,
        &mut Option<LocalDurableCommit>,
    ),
) -> Result<FormalExecution, FormalError> {
    let market = command
        .market_id
        .ok_or(FormalError::Rejected(RejectReason::InvalidOrderCombination))?;
    let epoch = command
        .route_epoch
        .ok_or(FormalError::Rejected(RejectReason::StaleRouteEpoch))?;
    routes
        .validate(market, identity.shard, epoch)
        .map_err(FormalError::Rejected)?;
    if !markets.contains_key(&market) || registry.is_none() {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    if let Some(result) = dedup
        .lookup(&command)
        .map_err(|()| FormalError::ProtocolError)?
    {
        dedup.remember_alias(&command);
        return Ok(result);
    }
    if !valid_cancel_envelope(&command, market, place_state.as_ref()) {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    }
    let mut candidate_ids = place_state
        .as_ref()
        .map(|state| state.ids.try_clone_preallocated())
        .transpose()
        .map_err(|()| FormalError::Rejected(RejectReason::EngineBusy))?;
    let prepared = dedup.prepare(&command);
    let valid_alias = match &command.payload {
        Command::Cancel(input) => input.client_order_id.as_ref().is_none_or(|alias| {
            candidate_ids
                .as_ref()
                .is_some_and(|ids| ids.matches_active(input.user_id, alias, market, input.order_id))
        }),
        _ => true,
    };
    let result = execute_formal(
        routes,
        identity,
        seq,
        clock,
        markets,
        registry,
        (
            command.clone(),
            durable,
            place_state
                .as_ref()
                .is_some_and(|state| state.fee_book.is_some()),
            valid_alias,
        ),
    )
    .map_err(FormalError::Rejected)?;
    if let (Some(ids), Some(state)) = (candidate_ids.as_mut(), place_state.as_mut()) {
        for event in &result.events {
            if let EngineEvent::OrderCanceled { order_id, .. } = event.payload {
                ids.terminal_in_market(market, order_id);
            }
        }
        state.ids = candidate_ids.expect("候选身份索引存在");
    }
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 正式单笔撤单绑定信封身份，批量撤单沿用已有管理员范围约束。
fn valid_cancel_envelope(
    command: &CommandEnvelope,
    market: MarketId,
    place: Option<&PlaceRuntimeState>,
) -> bool {
    match &command.payload {
        Command::Cancel(input) => {
            input.market_id == market
                && command.user_id == Some(input.user_id)
                && input.client_order_id.as_ref().is_none_or(|id| {
                    !id.is_empty()
                        && place.is_none_or(|state| id.len() <= state.config.limits.client_id_bytes)
                })
        }
        Command::MassCancelShard(input) => {
            valid_market_cancel_scope(input.scope, market, command.user_id)
        }
        _ => false,
    }
}

/// 仅允许可信控制入口按市场执行本市场撤单或指定用户来源会话撤单。
fn valid_market_cancel_scope(
    scope: ShardCancelScope,
    market: MarketId,
    user: Option<matching_domain::UserId>,
) -> bool {
    user.is_none()
        && match scope {
            ShardCancelScope::Market { market_id } => market_id == market,
            ShardCancelScope::Session { .. } => true,
            _ => false,
        }
}

/// 在候选状态中完成业务执行及事件封装。
fn execute_formal(
    routes: &RouteTable,
    identity: FormalIdentity,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    markets: &mut BTreeMap<MarketId, ProductionOrderBook>,
    registry: &mut Option<ReservationRegistry>,
    (command, durable, settle_r2, valid_alias): (
        CommandEnvelope,
        &mut Option<LocalDurableCommit>,
        bool,
        bool,
    ),
) -> Result<FormalExecution, RejectReason> {
    let market = command
        .market_id
        .ok_or(RejectReason::InvalidOrderCombination)?;
    let epoch = command.route_epoch.ok_or(RejectReason::StaleRouteEpoch)?;
    routes.validate(market, identity.shard, epoch)?;
    if !valid_cancel_envelope(&command, market, None) {
        return Err(RejectReason::InvalidOrderCombination);
    }
    let book = markets
        .get_mut(&market)
        .ok_or(RejectReason::StaleRouteEpoch)?;
    let registry = registry
        .as_mut()
        .ok_or(RejectReason::InvalidOrderCombination)?;
    let bytes = if durable.is_some() {
        Some(
            matching_domain::encode_wal_command(&command)
                .map_err(|_| RejectReason::InvalidOrderCombination)?,
        )
    } else {
        None
    };
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
    if let (Some(gate), Some(bytes)) = (durable.as_mut(), bytes) {
        wal_or_halt(
            gate.commit(
                identity.cluster,
                identity.shard,
                clock.now().get(),
                command.command_id,
                n,
                bytes,
            ),
            DiagnosticKind::WalCommit,
            Some(command.command_id),
            Some(n),
        );
    }
    let mut candidate = book.clone();
    let mut candidate_registry = registry.clone();
    let plan = match &command.payload {
        Command::Cancel(input) if valid_alias || input.client_order_id.is_none() => {
            candidate.plan_user_cancel(input, &candidate_registry)
        }
        Command::Cancel(_) => Err(RejectReason::OrderNotFound),
        Command::MassCancelShard(input) => {
            candidate.plan_mass_cancel(input.scope, &candidate_registry)
        }
        _ => unreachable!("已核验撤单载荷"),
    };
    let (mut outcome, events) = match plan {
        Ok(plan) => {
            let (result, mut events) = candidate
                .apply_mass_cancel(&mut candidate_registry, plan)
                .expect("新生成的撤单计划发生状态漂移");
            if settle_r2 || matches!(command.payload, Command::Cancel(_)) {
                events = crate::place_candidate::release_terminal_reservations(
                    book,
                    &candidate,
                    &mut candidate_registry,
                    events,
                )
                .expect("撤单真实预留释放失败，分片停止");
            }
            let outcome = match command.payload {
                Command::Cancel(ref input) => FormalOutcome::Canceled(input.order_id),
                _ => FormalOutcome::MassCancel(result),
            };
            (outcome, events)
        }
        Err(RejectReason::InvalidReservation | RejectReason::InvalidQty) => {
            panic!("正式撤单发现预留或订单状态不变量损坏")
        }
        Err(reason) => (
            FormalOutcome::Rejected(reason),
            vec![EngineEvent::CommandRejected { reason }],
        ),
    };
    let overflow = u64::try_from(events.len()).map_or(true, |count| {
        !event_count_fits(count, identity.event_capacity)
    });
    let events = if overflow {
        outcome = FormalOutcome::Rejected(RejectReason::ArithmeticOverflow);
        vec![EngineEvent::CommandRejected {
            reason: RejectReason::ArithmeticOverflow,
        }]
    } else {
        events
    };
    let mut wrapped = Vec::with_capacity(events.len());
    for (index, payload) in events.into_iter().enumerate() {
        wrapped.push(EventEnvelope {
            cluster_id: identity.cluster,
            shard_id: identity.shard,
            shard_seq: n,
            event_index: u32::try_from(index).expect("事件数量已预检"),
            command_id: command.command_id,
            engine_time: clock.now().get(),
            market_id: Some(market),
            payload,
        });
    }
    if !overflow {
        *book = candidate;
        *registry = candidate_registry;
    }
    let (status, reject_reason) = match &outcome {
        FormalOutcome::Canceled(_)
        | FormalOutcome::MassCancel(_)
        | FormalOutcome::UserFence(_)
        | FormalOutcome::Placed(_)
        | FormalOutcome::FeeScheduleUpdated(_)
        | FormalOutcome::ShardControl
        | FormalOutcome::MarketManaged => (CommandStatus::Succeeded, None),
        FormalOutcome::Rejected(reason) => (CommandStatus::Rejected, Some(*reason)),
    };
    let summary = CommandResultSummary {
        status,
        first_event_index: wrapped.first().map(|event| event.event_index),
        last_event_index: wrapped.last().map(|event| event.event_index),
        durable_seq: Some(n),
        reject_reason,
    };
    Ok(FormalExecution {
        command_id: command.command_id,
        seq: n,
        outcome,
        summary,
        event_range: wrapped.last().map(|event| (0, event.event_index)),
        events: wrapped,
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    })
}

/// 判断根命令的事件数量是否能够由给定索引容量完整表示。
fn event_count_fits(count: u64, capacity: u64) -> bool {
    count <= capacity
}

/// 分片故障的稳定类别，不包含凭据或原始命令。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticKind {
    /// WAL 写入器追加失败。
    WalAppend,
    /// WAL 批次同步失败。
    WalSync,
    /// WAL 单条提交失败。
    WalCommit,
    /// 已写入快照的清单登记失败。
    SnapshotCommit,
    /// 未归类的线程异常或业务不变量失败。
    WorkerPanic,
}

/// 有界错误诊断的固定字段；不包含命令载荷和任意错误文本。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticEvent {
    /// 故障类别。
    pub kind: DiagnosticKind,
    /// 集群身份。
    pub cluster_id: ClusterId,
    /// 分片身份。
    pub shard_id: ShardId,
    /// 已知的命令身份；批量同步时可能没有单一身份。
    pub command_id: Option<CommandId>,
    /// 发生故障时的相关分片序号。
    pub shard_seq: Option<u64>,
    /// 底层输入输出错误类别。
    pub io_kind: Option<std::io::ErrorKind>,
}

/// 只在工作线程内携带的持久写入故障上下文。
#[derive(Debug)]
struct WalFault {
    /// 稳定操作阶段。
    kind: DiagnosticKind,
    /// 已知命令身份。
    command_id: Option<CommandId>,
    /// 已知分片序号。
    shard_seq: Option<u64>,
    /// 输入输出错误类别。
    io_kind: std::io::ErrorKind,
}

/// 保留持久失败上下文并停止分片，不暴露原始错误内容。
fn wal_or_halt(
    result: std::io::Result<()>,
    kind: DiagnosticKind,
    command_id: Option<CommandId>,
    shard_seq: Option<u64>,
) {
    if let Err(error) = result {
        std::panic::panic_any(WalFault {
            kind,
            command_id,
            shard_seq,
            io_kind: error.kind(),
        });
    }
}

/// 固定容量的非阻塞诊断通道，丢弃计数不会绕回。
struct DiagnosticBuffer {
    /// 等待外部消费者读取的事件。
    events: crossbeam_queue::ArrayQueue<DiagnosticEvent>,
    /// 因容量不足而丢弃的诊断数。
    dropped: AtomicU64,
    /// 丢弃计数耗尽标志。
    exhausted: AtomicBool,
}
impl DiagnosticBuffer {
    /// 创建固定容量通道。
    fn new() -> Self {
        Self {
            events: crossbeam_queue::ArrayQueue::new(64),
            dropped: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
        }
    }
    /// 在撮合线程只尝试一次，不等待消费者。
    fn record(&self, event: DiagnosticEvent) {
        if self.events.push(event).is_err()
            && self
                .dropped
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .is_err()
        {
            self.exhausted.store(true, Ordering::Relaxed);
        }
    }
}

/// 单写者发布的非业务计数；耗尽时保留原值并显式标记。
#[derive(Default)]
struct ExecutionMetrics {
    /// 已安装的分片级熔断状态。
    shard_halted: AtomicBool,
    /// 已有最终结果的命令。
    commands: AtomicU64,
    /// 被拒绝的命令。
    rejects: AtomicU64,
    /// 因优先级空间耗尽而实际拒绝的命令。
    priority_exhaustions: AtomicU64,
    /// 已提交的真实成交。
    trades: AtomicU64,
    /// 互斥延迟桶，边界单位为微秒，最后一个桶无上界。
    match_buckets: [AtomicU64; 7],
    /// 已观察的撮合候选耗时总微秒数。
    match_sum_us: AtomicU64,
    /// 命令入口到最终回复的时延互斥桶。
    command_buckets: [AtomicU64; 7],
    /// 命令时延微秒总数。
    command_sum_us: AtomicU64,
    /// 快照后台写入与登记的互斥时延桶，单位毫秒。
    snapshot_buckets: [AtomicU64; 7],
    /// 快照处理耗时总毫秒数。
    snapshot_sum_ms: AtomicU64,
    /// 快照后台处理失败次数。
    snapshot_failures: AtomicU64,
    /// 成功恢复时重放的记录数。
    replay_records: AtomicU64,
    /// 恢复时重放耗时的互斥桶。
    replay_buckets: [AtomicU64; 7],
    /// 重放总耗时，单位毫秒。
    replay_sum_ms: AtomicU64,
    /// 计数耗尽的可观测标志。
    overflow: AtomicBool,
}
impl ExecutionMetrics {
    /// 记录成功入队并产生最终回复的命令时延。
    fn observe_command(&self, duration: Duration) {
        let Ok(micros) = u64::try_from(duration.as_micros()) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        let bucket = [10_u64, 100, 1_000, 10_000, 100_000, 1_000_000]
            .iter()
            .position(|limit| micros <= *limit)
            .unwrap_or(6);
        self.add(&self.command_buckets[bucket], 1);
        if let Ok(value) = usize::try_from(micros) {
            self.add(&self.command_sum_us, value);
        } else {
            self.overflow.store(true, Ordering::Relaxed);
        }
    }
    /// 记录成功恢复所用的已提交记录及重放耗时。
    fn observe_replay(&self, records: usize, elapsed: Duration) {
        self.add(&self.replay_records, records);
        let Ok(millis) = u64::try_from(elapsed.as_millis()) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        let bucket = [1_u64, 10, 100, 1_000, 10_000, 60_000]
            .iter()
            .position(|limit| millis <= *limit)
            .unwrap_or(6);
        self.add(&self.replay_buckets[bucket], 1);
        if let Ok(value) = usize::try_from(millis) {
            self.add(&self.replay_sum_ms, value);
        } else {
            self.overflow.store(true, Ordering::Relaxed);
        }
    }
    /// 记录已入队快照的文件写入和清单登记结果。
    fn observe_snapshot(&self, elapsed: Duration, failed: bool) {
        let Ok(millis) = u64::try_from(elapsed.as_millis()) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        let bucket = [1_u64, 10, 100, 1_000, 10_000, 60_000]
            .iter()
            .position(|limit| millis <= *limit)
            .unwrap_or(6);
        self.add(&self.snapshot_buckets[bucket], 1);
        let Ok(value) = usize::try_from(millis) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        self.add(&self.snapshot_sum_ms, value);
        if failed {
            self.add(&self.snapshot_failures, 1);
        }
    }
    /// 按单调时钟记录候选执行耗时，不用于业务时间或撮合决策。
    fn observe_match(&self, duration: Duration) {
        let Ok(micros) = u64::try_from(duration.as_micros()) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        let bucket = [10_u64, 100, 1_000, 10_000, 100_000, 1_000_000]
            .iter()
            .position(|limit| micros <= *limit)
            .unwrap_or(6);
        self.add(&self.match_buckets[bucket], 1);
        let Ok(value) = usize::try_from(micros) else {
            self.overflow.store(true, Ordering::Relaxed);
            return;
        };
        self.add(&self.match_sum_us, value);
    }
    /// 检查计数后更新，不让诊断溢出改变业务行为。
    fn add(&self, counter: &AtomicU64, amount: usize) {
        let updated = u64::try_from(amount).ok().and_then(|n| {
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    old.checked_add(n)
                })
                .ok()
        });
        if updated.is_none() {
            self.overflow.store(true, Ordering::Relaxed);
        }
    }
    /// 只在正式结果已确定后记录，重试不会重复产生成交计数。
    fn observe_formal(&self, result: &Result<FormalExecution, FormalError>) {
        self.add(&self.commands, 1);
        if matches!(
            result,
            Err(FormalError::Rejected(RejectReason::PrioritySpaceExhaustion))
        ) || matches!(result, Ok(value) if !value.deduplicated && matches!(value.outcome, FormalOutcome::Rejected(RejectReason::PrioritySpaceExhaustion)))
        {
            self.add(&self.priority_exhaustions, 1);
        }
        match result {
            Err(_) => self.add(&self.rejects, 1),
            Ok(value) => {
                if matches!(value.outcome, FormalOutcome::Rejected(_)) {
                    self.add(&self.rejects, 1);
                }
                if !value.deduplicated {
                    self.add(
                        &self.trades,
                        value
                            .events
                            .iter()
                            .filter(|event| matches!(event.payload, EngineEvent::Trade(_)))
                            .count(),
                    );
                }
            }
        }
    }
    /// 低层变更同样计入结果，但仅在真实事件中计数成交。
    fn observe_mutation(&self, result: &Result<Execution, RejectReason>) {
        self.add(&self.commands, 1);
        if matches!(
            result,
            Err(RejectReason::PrioritySpaceExhaustion)
                | Ok(Execution {
                    result: Err(RejectReason::PrioritySpaceExhaustion),
                    ..
                })
        ) {
            self.add(&self.priority_exhaustions, 1);
        }
        match result {
            Err(_) | Ok(Execution { result: Err(_), .. }) => self.add(&self.rejects, 1),
            Ok(Execution {
                result: Ok(MutationOutcome::Placed(events)),
                ..
            }) => self.add(
                &self.trades,
                events
                    .iter()
                    .filter(|event| matches!(event, EngineEvent::Trade(_)))
                    .count(),
            ),
            Ok(Execution {
                result: Ok(MutationOutcome::MarketByBase(value)),
                ..
            }) => self.add(&self.trades, value.trades().len()),
            Ok(_) => (),
        }
    }
}

/// 分片入口和线程的瞬时观测，队列长度可能随并发操作变化。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardObservation {
    /// 工作线程尚未退出。
    pub thread_running: bool,
    /// 外部入口尚未关闭。
    pub ingress_open: bool,
    /// 工作线程已确认路由变更准入开放。
    pub route_ready: bool,
    /// 分片级熔断开关已激活。
    pub shard_halted: bool,
    /// 工作线程配置中经过启动校验的本机主节点租约仍被持有。
    pub primary_lease_held: bool,
    /// 工作线程遇到不可恢复错误。
    pub failed: bool,
    /// 有界队列当前消息数。
    pub queue_depth: usize,
    /// 有界队列的固定容量。
    pub queue_capacity: usize,
    /// 已出队消息的入队等待直方图。
    pub queue_wait: matching_persistence::LatencySnapshot,
    /// 已产生最终回复的命令数，包含去重命中。
    pub commands: u64,
    /// 最终拒绝的命令数。
    pub rejects: u64,
    /// 优先级耗尽拒绝次数，不重复计入正式幂等命中。
    pub priority_exhaustions: u64,
    /// 实际产生的成交数，不重复计算去重回复。
    pub trades: u64,
    /// 任一指标计数已耗尽。
    pub metrics_overflow: bool,
    /// 因有界诊断队列已满而丢弃的事件数。
    pub diagnostics_dropped: u64,
    /// 互斥的候选执行延迟桶及耗时总和。
    pub match_latency_buckets: [u64; 7],
    /// 撮合候选耗时总微秒数。
    pub match_latency_sum_us: u64,
    /// 命令总时延互斥桶。
    pub command_latency_buckets: [u64; 7],
    /// 命令总时延微秒数。
    pub command_latency_sum_us: u64,
    /// 快照处理的互斥时延桶。
    pub snapshot_latency_buckets: [u64; 7],
    /// 快照处理耗时总毫秒数。
    pub snapshot_latency_sum_ms: u64,
    /// 快照处理失败次数。
    pub snapshot_failures: u64,
    /// 成功恢复时重放的记录总数。
    pub replay_records: u64,
    /// 成功恢复的重放时延互斥桶。
    pub replay_buckets: [u64; 7],
    /// 成功恢复的重放时延总毫秒数。
    pub replay_sum_ms: u64,
    /// WAL 追加耗时，内存分片没有该观测。
    pub wal_append: Option<matching_persistence::LatencySnapshot>,
    /// WAL 同步耗时，内存分片没有该观测。
    pub wal_sync: Option<matching_persistence::LatencySnapshot>,
    /// 批量提交的记录数分布。
    pub wal_batch_size: Option<matching_persistence::BatchSizeSnapshot>,
    /// 批量提交的首条记录等待时延。
    pub wal_batch_wait: Option<matching_persistence::LatencySnapshot>,
}

/// 可克隆的有界生产者，只能投递消息，不可访问撮合状态。
#[derive(Clone)]
pub struct ShardHandle {
    /// 工作线程是否从路由暂停状态启动。
    route_gated_at_start: bool,
    /// 启动配置中的分片主节点纪元，协调控制在执行前另行核对实际写权。
    primary_epoch: u64,
    /// 工作线程最近确认的路由准入状态。
    route_ready: Arc<AtomicBool>,
    /// 该入口是否提供正式命令的本地持久回复。
    durable_formal: bool,
    /// 已绑定到本分片的控制权威，不随市场迁移。
    control_admission: Option<crate::place_config::AdmissionBinding>,
    /// 具备受清单保护的本地市场迁移顺序点。
    local_migration: bool,
    /// 持久写入器的独立时延观测。
    wal_metrics: Option<Arc<matching_persistence::DurabilityMetrics>>,
    /// 经过启动校验的租约弱引用。
    primary_lease: Option<std::sync::Weak<crate::PrimaryLease>>,
    /// 用于验证镜像来自本工作线程的凭据。
    snapshot_origin: Arc<()>,
    /// 当前分片写入器持有标志。
    snapshot_writer_active: Arc<AtomicBool>,
    /// 有界消息入口。
    producer: Producer<Message>,
    /// 入队后使用的工作线程唤醒句柄。
    thread: Thread,
    /// 工作线程异常退出标志。
    failed: Arc<AtomicBool>,
    /// 工作线程尚未退出。
    running: Arc<AtomicBool>,
    /// 分片执行计数。
    metrics: Arc<ExecutionMetrics>,
    /// 非阻塞、有界的错误诊断队列。
    diagnostics: Arc<DiagnosticBuffer>,
    /// 句柄对应的分片身份。
    shard_id: ShardId,
    /// 句柄对应的集群身份。
    cluster_id: ClusterId,
    /// 工作线程可替换的路由快照，读取者持有各自取得的版本。
    routes: Arc<RwLock<Arc<RouteTable>>>,
}

/// 一次顺序查询得到的市场优先级。
pub(crate) struct PriorityObservation {
    /// 市场身份。
    pub(crate) market: MarketId,
    /// 下一普通优先级。
    pub(crate) queue: u128,
    /// 下一止损优先级。
    pub(crate) stop: u128,
    /// 首个不可分配的共同上限。
    pub(crate) ceiling: u128,
}

impl ShardHandle {
    /// 返回工作线程创建时是否已启用路由暂停。
    pub(crate) fn route_gated_at_start(&self) -> bool {
        self.route_gated_at_start
    }
    #[cfg(test)]
    pub(crate) fn block_for_test(
        &self,
        entered: SyncSender<()>,
        release: Receiver<()>,
    ) -> Result<(), ShardError> {
        self.push(Message::TestBlock(entered, release), IngressClass::Control)
    }
    #[cfg(test)]
    pub(crate) fn routes_are_fenced(&self) -> bool {
        self.producer.routes_are_fenced()
    }
    /// 请求配置市场的优先级状态。
    pub(crate) fn priority_counts(
        &self,
        markets: Vec<MarketId>,
    ) -> Result<Receiver<Vec<PriorityObservation>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::PriorityCounts(markets, tx), IngressClass::Query)?;
        Ok(rx)
    }
    /// 在有界查询队列中请求当前订单簿规模。
    pub(crate) fn book_counts(&self) -> Result<Receiver<Option<(usize, usize)>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::BookCounts(tx), IngressClass::Query)?;
        Ok(rx)
    }
    /// 取走当前已记录的错误；诊断不能替代业务 WAL。
    pub fn take_diagnostics(&self) -> Vec<DiagnosticEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.diagnostics.events.pop() {
            events.push(event);
        }
        events
    }

    /// 判断两个句柄是否属于同一工作线程实例。
    pub(crate) fn same_worker(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.running, &other.running)
    }

    /// 读取分片线程和入口状态；队列观测不阻塞撮合线程。
    pub fn observation(&self) -> ShardObservation {
        let (queue_depth, queue_capacity, ingress_open) = self.producer.observation();
        ShardObservation {
            thread_running: self.running.load(Ordering::Acquire),
            ingress_open,
            route_ready: self.route_ready.load(Ordering::Acquire),
            shard_halted: self.metrics.shard_halted.load(Ordering::Acquire),
            primary_lease_held: self
                .primary_lease
                .as_ref()
                .is_some_and(|lease| lease.strong_count() > 0),
            failed: self.failed.load(Ordering::Acquire),
            queue_depth,
            queue_capacity,
            queue_wait: self.producer.wait_observation(),
            commands: self.metrics.commands.load(Ordering::Relaxed),
            rejects: self.metrics.rejects.load(Ordering::Relaxed),
            priority_exhaustions: self.metrics.priority_exhaustions.load(Ordering::Relaxed),
            trades: self.metrics.trades.load(Ordering::Relaxed),
            metrics_overflow: self.metrics.overflow.load(Ordering::Relaxed)
                || self.diagnostics.exhausted.load(Ordering::Relaxed),
            diagnostics_dropped: self.diagnostics.dropped.load(Ordering::Relaxed),
            match_latency_buckets: std::array::from_fn(|i| {
                self.metrics.match_buckets[i].load(Ordering::Relaxed)
            }),
            match_latency_sum_us: self.metrics.match_sum_us.load(Ordering::Relaxed),
            command_latency_buckets: std::array::from_fn(|i| {
                self.metrics.command_buckets[i].load(Ordering::Relaxed)
            }),
            command_latency_sum_us: self.metrics.command_sum_us.load(Ordering::Relaxed),
            snapshot_latency_buckets: std::array::from_fn(|i| {
                self.metrics.snapshot_buckets[i].load(Ordering::Relaxed)
            }),
            snapshot_latency_sum_ms: self.metrics.snapshot_sum_ms.load(Ordering::Relaxed),
            snapshot_failures: self.metrics.snapshot_failures.load(Ordering::Relaxed),
            replay_records: self.metrics.replay_records.load(Ordering::Relaxed),
            replay_buckets: std::array::from_fn(|i| {
                self.metrics.replay_buckets[i].load(Ordering::Relaxed)
            }),
            replay_sum_ms: self.metrics.replay_sum_ms.load(Ordering::Relaxed),
            wal_append: self.wal_metrics.as_ref().map(|metrics| metrics.append()),
            wal_sync: self.wal_metrics.as_ref().map(|metrics| metrics.sync()),
            wal_batch_size: self
                .wal_metrics
                .as_ref()
                .map(|metrics| metrics.batch_size()),
            wal_batch_wait: self
                .wal_metrics
                .as_ref()
                .map(|metrics| metrics.batch_wait()),
        }
    }

    /// 返回投递控制子命令时使用的目标主节点纪元。
    pub(crate) fn control_primary_epoch(&self) -> u64 {
        self.primary_epoch
    }
    /// 会话协调仅能使用带持久正式业务入口的句柄。
    pub(crate) fn has_durable_formal(&self) -> bool {
        self.durable_formal
    }
    /// 读取句柄归属分片。
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// 返回分片所属的集群身份。
    pub(crate) fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }
    /// 控制权威退出时关闭此分片的外部入口。
    pub(crate) fn close_route_ingress(&self) {
        self.producer.close_ingress();
        self.thread.unpark();
    }
    /// 失权时同时阻止尚未开始处理的队列变更。
    pub(crate) fn fence_route_ingress(&self) {
        self.producer.fence_routes();
        self.close_route_ingress();
    }
    /// 入口关闭后等待此前已接受的消息完成；工作线程退出也代表不再执行。
    pub(crate) fn drain_closed_route_ingress(&self) {
        loop {
            if !self.running.load(Ordering::Acquire) {
                return;
            }
            let (tx, rx) = sync_channel(1);
            let Some(deadline) = Instant::now().checked_add(Duration::from_secs(1)) else {
                continue;
            };
            if self
                .producer
                .push_internal_until(Message::Barrier(0, tx), deadline)
                .is_err()
            {
                continue;
            }
            self.thread.unpark();
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(_) => return,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                        if !self.running.load(Ordering::Acquire) =>
                    {
                        return;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => (),
                }
            }
        }
    }
    /// 取得工作线程最近公布的路由快照，已取得的快照保持原版本。
    pub(crate) fn routes(&self) -> Result<Arc<RouteTable>, ShardError> {
        self.routes
            .read()
            .map(|routes| Arc::clone(&routes))
            .map_err(|_| ShardError::Failed)
    }

    /// 在队列顺序点核验并安装可信路由；迁移时由协调器另行派发镜像安装请求。
    pub(crate) fn install_routes(
        &self,
        routes: RouteTable,
    ) -> Result<Receiver<Result<(), ShardError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::InstallRoutes(routes, tx), IngressClass::Control)?;
        Ok(rx)
    }

    /// 暂停此分片的业务变更，队列中更早的操作先完成。
    pub(crate) fn pause_route_mutations(
        &self,
    ) -> Result<Receiver<Result<(), ShardError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::RouteAdmission(true, tx), IngressClass::Control)?;
        Ok(rx)
    }

    /// 解除队列暂停。路由表仍将迁移中的源、目标分片保持在停写状态。
    pub(crate) fn resume_route_mutations(
        &self,
    ) -> Result<Receiver<Result<(), ShardError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::RouteAdmission(false, tx), IngressClass::Control)?;
        Ok(rx)
    }

    /// 在线程队列中查询当前订单节点及业务余量。
    pub fn order(
        &self,
        market: MarketId,
        id: OrderId,
    ) -> Result<Receiver<Result<Option<OrderNode>, RejectReason>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Order(market, id, tx), IngressClass::Query)?;
        Ok(rx)
    }
    /// 在队列顺序点请求独立内存镜像，不分配业务序号。
    pub fn take_snapshot(
        &self,
        id: u128,
    ) -> Result<Receiver<Result<SnapshotImage, SnapshotError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(
            Message::Snapshot(id, SnapshotReply::Ordinary(tx)),
            IngressClass::Control,
        )?;
        Ok(rx)
    }
    /// 请求线性化屏障。
    pub fn barrier(&self, id: u128) -> Result<Receiver<(u128, u64)>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Barrier(id, tx), IngressClass::Control)?;
        Ok(rx)
    }
    fn push(&self, msg: Message, class: IngressClass) -> Result<(), ShardError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(ShardError::Failed);
        }
        self.producer
            .try_push_classified(msg, class)
            .map_err(|e| match e {
                PushError::Full(_) => ShardError::Busy,
                PushError::Closed(_) if self.failed.load(Ordering::Acquire) => ShardError::Failed,
                PushError::Closed(_) => ShardError::Closed,
            })?;
        self.thread.unpark();
        Ok(())
    }
    /// 投递携带调用方身份和路由版本的变更。
    pub fn submit_envelope(
        &self,
        envelope: MutationEnvelope,
    ) -> Result<Receiver<Result<Execution, RejectReason>>, ShardError> {
        self.submit(
            envelope.command_id,
            envelope.market_id,
            envelope.route_epoch,
            envelope.payload,
        )
    }

    /// 尝试投递一次变更，繁忙时由调用方决定后续操作。
    pub fn submit(
        &self,
        command_id: CommandId,
        market: MarketId,
        epoch: u64,
        mutation: Mutation,
    ) -> Result<Receiver<Result<Execution, RejectReason>>, ShardError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(ShardError::Failed);
        }
        let (tx, rx) = sync_channel(1);
        self.producer
            .try_push_classified(
                Message::Mutation(
                    command_id,
                    market,
                    epoch,
                    mutation,
                    CommandReply::new(tx, Some(Arc::clone(&self.metrics))),
                ),
                IngressClass::Mutation,
            )
            .map_err(|error| match error {
                PushError::Full(_) => ShardError::Busy,
                PushError::Closed(_) if self.failed.load(Ordering::Acquire) => ShardError::Failed,
                PushError::Closed(_) => ShardError::Closed,
            })?;
        self.thread.unpark();
        Ok(rx)
    }
    /// 提交正式业务命令。下单先经过控制准入，单笔撤单仍可入队；跨分片控制子命令须由协调器派发。
    pub fn submit_command(
        &self,
        command: CommandEnvelope,
    ) -> Result<Receiver<Result<FormalExecution, FormalError>>, ShardError> {
        let coordinator_only = match &command.payload {
            Command::ApplyGlobalControlFenceShard(_) => true,
            Command::ApplyUserControlFenceShard(fence) => {
                self.control_admission.is_some()
                    || (self.durable_formal
                        && fence.action != matching_domain::UserFenceAction::FenceOnly)
            }
            _ => false,
        };
        if coordinator_only {
            return Err(ShardError::InvalidConfig);
        }
        if let Command::PlaceLimit(place) = &command.payload {
            let status = self.admission_status(place.order.user_id)?;
            if let Some(reason) = status.rejection(place.user_control_epoch) {
                let (tx, rx) = sync_channel(1);
                tx.send(Err(FormalError::Rejected(reason)))
                    .map_err(|_| ShardError::Failed)?;
                return Ok(rx);
            }
            return self.enqueue_command(command, status.global_epoch());
        }
        self.enqueue_command(command, None)
    }

    /// 读取已发布的全局纪元、指定用户纪元和当前拒绝原因。
    pub(crate) fn admission_status(
        &self,
        user: matching_domain::UserId,
    ) -> Result<crate::place_config::AdmissionStatus, ShardError> {
        let Some(binding) = &self.control_admission else {
            return Ok(Default::default());
        };
        let (global, user, reason) = binding
            .0
            .check(user)
            .map_err(|_| ShardError::ControlUnavailable)?;
        Ok(crate::place_config::AdmissionStatus {
            epochs: Some((global, user)),
            reason,
        })
    }

    /// 取得已绑定的控制命名空间，供协调器和网关核对目标。
    pub(crate) fn control_namespace(&self) -> Option<String> {
        self.control_admission
            .as_ref()
            .map(|binding| binding.0.namespace())
    }

    /// 只有绑定同一权威的协调器才可派发用户或全局控制。
    pub(crate) fn submit_control_command(
        &self,
        namespace: &str,
        command: CommandEnvelope,
    ) -> Result<Receiver<Result<FormalExecution, FormalError>>, ShardError> {
        if !self.durable_formal
            || self.control_namespace().as_deref() != Some(namespace)
            || !matches!(
                command.payload,
                Command::ApplyUserControlFenceShard(_) | Command::ApplyGlobalControlFenceShard(_)
            )
        {
            return Err(ShardError::InvalidConfig);
        }
        self.enqueue_command(command, None)
    }

    /// 将已完成入口核准的命令及原始全局纪元一并放入单写者队列。
    pub(crate) fn enqueue_command(
        &self,
        command: CommandEnvelope,
        admitted: Option<u64>,
    ) -> Result<Receiver<Result<FormalExecution, FormalError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(
            Message::Formal(
                command,
                admitted,
                CommandReply::new(tx, Some(Arc::clone(&self.metrics))),
            ),
            IngressClass::Mutation,
        )?;
        Ok(rx)
    }

    /// 在线程顺序点读取预留金融快照及其 PendingR2 归属。
    ///
    /// 没有登记表或指定预留身份不存在时均返回 `None`。
    pub fn reservation(
        &self,
        id: ReservationId,
    ) -> Result<Receiver<Option<(ReservationState, bool)>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Reservation(id, tx), IngressClass::Query)?;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_table::MarketRoute;
    use matching_orderbook::QueuePriorityPolicy;

    #[test]
    fn exhausted_observation_counter_is_reported_without_wrapping() {
        let metrics = ExecutionMetrics::default();
        metrics.commands.store(u64::MAX, Ordering::Relaxed);
        metrics.add(&metrics.commands, 1);
        assert_eq!(metrics.commands.load(Ordering::Relaxed), u64::MAX);
        assert!(metrics.overflow.load(Ordering::Relaxed));
    }

    #[test]
    fn priority_exhaustion_metric_counts_actual_rejections() {
        let metrics = ExecutionMetrics::default();
        metrics.observe_mutation(&Err(RejectReason::PrioritySpaceExhaustion));
        metrics.observe_formal(&Err(FormalError::Rejected(
            RejectReason::PrioritySpaceExhaustion,
        )));
        metrics.observe_formal(&Err(FormalError::Rejected(RejectReason::MarketNotFound)));
        assert_eq!(metrics.priority_exhaustions.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn diagnostics_are_bounded_and_count_overflow_without_blocking() {
        let buffer = DiagnosticBuffer::new();
        let event = DiagnosticEvent {
            kind: DiagnosticKind::WorkerPanic,
            cluster_id: ClusterId(1),
            shard_id: ShardId(2),
            command_id: None,
            shard_seq: None,
            io_kind: None,
        };
        for _ in 0..65 {
            buffer.record(event.clone());
        }
        assert_eq!(buffer.events.len(), 64);
        assert_eq!(buffer.dropped.load(Ordering::Relaxed), 1);
        buffer.dropped.store(u64::MAX, Ordering::Relaxed);
        buffer.record(event);
        assert_eq!(buffer.dropped.load(Ordering::Relaxed), u64::MAX);
        assert!(buffer.exhausted.load(Ordering::Relaxed));
    }

    #[test]
    fn match_latency_buckets_include_boundaries_and_preserve_exact_sum() {
        let metrics = ExecutionMetrics::default();
        for micros in [0, 10, 11, 100, 101, 1_000_001] {
            metrics.observe_match(Duration::from_micros(micros));
        }
        let buckets: [u64; 7] =
            std::array::from_fn(|i| metrics.match_buckets[i].load(Ordering::Relaxed));
        assert_eq!(buckets, [2, 2, 1, 0, 0, 0, 1]);
        assert_eq!(metrics.match_sum_us.load(Ordering::Relaxed), 1_000_223);
        assert!(!metrics.overflow.load(Ordering::Relaxed));
    }

    #[test]
    fn reference_update_is_a_market_managed_candidate_and_rejects_stale() {
        use matching_domain::{
            ClosePolicy, ExecutionLimits, MarketConfig, OrderLimits, Price, PriceReferenceKind,
            Qty, QuoteAmount, UpdateReferencePrice,
        };
        let market = MarketId(90);
        let config = MarketConfig {
            market_id: market,
            price_scale: 0,
            qty_scale: 0,
            quote_scale: 0,
            tick_size: 1,
            lot_size: 1,
            order_limits: OrderLimits {
                min_qty: Qty::try_new(1).unwrap(),
                max_qty: Qty::try_new(10).unwrap(),
                min_notional: QuoteAmount::new(1),
                max_notional: QuoteAmount::new(100),
            },
            price_band: None,
            active_fee_schedule_version: 1,
            close_policy: ClosePolicy::CancelAll,
            execution_limits: ExecutionLimits {
                max_internal_actions_per_command: 10,
                max_trades_per_command: 10,
                priority_exhaustion_guard: 1,
                synthetic_priority_ceiling: None,
            },
        };
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        book.bind_market(market).unwrap();
        let mut books = BTreeMap::from([(market, book)]);
        let mut managed =
            BTreeMap::from([(market, matching_domain::ManagedMarket::new(config).unwrap())]);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(1),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let mut registry = None;
        let mut place = None;
        let command = |id| CommandEnvelope {
            command_id: CommandId(id),
            session_id: 1,
            client_seq: id as u64,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::UpdateReferencePrice(UpdateReferencePrice {
                market_id: market,
                kind: PriceReferenceKind::StaticOpening,
                price: Price::try_new(10).unwrap(),
                source_seq: 1,
            }),
        };
        for (id, expected) in [
            (1, FormalOutcome::MarketManaged),
            (
                2,
                FormalOutcome::Rejected(RejectReason::StaleReferencePriceUpdate),
            ),
        ] {
            let result = execute_market_management(
                MarketManagementContext {
                    controls: None,
                    identity: FormalIdentity {
                        shard: ShardId(1),
                        cluster: ClusterId(1),
                        event_capacity: 10,
                    },
                    routes: &routes,
                    seq: &mut seq,
                    clock: &LogicalClock::new(LogicalTime::new(0)),
                    markets: &mut books,
                    managed_markets: &mut managed,
                    dedup: &mut dedup,
                    place: &mut place,
                    registry: &mut registry,
                },
                command(id),
            )
            .unwrap();
            assert_eq!(result.outcome, expected);
            assert!(books[&market].has_reference_price(PriceReferenceKind::StaticOpening));
        }
    }

    #[test]
    fn create_market_rejects_each_invalid_math_configuration_without_mutation() {
        use matching_domain::{
            ClosePolicy, ExecutionLimits, MarketConfig, OrderLimits, Qty, QuoteAmount,
        };
        let market = MarketId(90);
        let original = MarketConfig {
            market_id: market,
            price_scale: 0,
            qty_scale: 0,
            quote_scale: 0,
            tick_size: 1,
            lot_size: 1,
            order_limits: OrderLimits {
                min_qty: Qty::try_new(1).unwrap(),
                max_qty: Qty::try_new(10).unwrap(),
                min_notional: QuoteAmount::new(1),
                max_notional: QuoteAmount::new(100),
            },
            price_band: None,
            active_fee_schedule_version: 1,
            close_policy: ClosePolicy::CancelAll,
            execution_limits: ExecutionLimits {
                max_internal_actions_per_command: 10,
                max_trades_per_command: 10,
                priority_exhaustion_guard: 1,
                synthetic_priority_ceiling: None,
            },
        };
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        book.bind_market(market).unwrap();
        let mut books = BTreeMap::from([(market, book.clone())]);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(1),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut managed = BTreeMap::new();
        let mut registry = None;
        let mut place = None;
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        for (index, reason) in [
            RejectReason::MarketConfigInvalidTick,
            RejectReason::MarketConfigInvalidLot,
            RejectReason::MarketConfigUnsupportedScale,
            RejectReason::MarketConfigNonIntegralQuoteUnit,
            RejectReason::MarketConfigArithmeticOverflow,
        ]
        .into_iter()
        .enumerate()
        {
            let mut config = original.clone();
            match index {
                0 => config.tick_size = 0,
                1 => config.lot_size = 0,
                2 => config.quote_scale = 39,
                3 => config.price_scale = 1,
                4 => {
                    config.quote_scale = 38;
                    config.tick_size = 4;
                }
                _ => unreachable!(),
            }
            let id = index as u128 + 1;
            let command = CommandEnvelope {
                command_id: CommandId(id),
                session_id: 1,
                client_seq: id as u64,
                market_id: Some(market),
                user_id: None,
                route_epoch: Some(1),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::CreateMarket(Box::new(matching_domain::CreateMarket { config })),
            };
            let outcome = execute_market_management(
                MarketManagementContext {
                    controls: None,
                    identity: FormalIdentity {
                        shard: ShardId(1),
                        cluster: ClusterId(1),
                        event_capacity: 10,
                    },
                    routes: &routes,
                    seq: &mut seq,
                    clock: &LogicalClock::new(LogicalTime::new(0)),
                    markets: &mut books,
                    managed_markets: &mut managed,
                    dedup: &mut dedup,
                    place: &mut place,
                    registry: &mut registry,
                },
                command,
            )
            .unwrap();
            assert_eq!(outcome.outcome, FormalOutcome::Rejected(reason));
            assert_eq!(books.get(&market), Some(&book));
            assert!(managed.is_empty());
        }
    }

    #[test]
    fn stop_limit_config_validation_checks_full_quantity_and_notional() {
        use matching_domain::{
            OrderLimits, Price, Qty, QuoteAmount, StopLimitOrder, StopOrder, TimeInForce,
            TriggerExpiry,
        };
        let limits = OrderLimits {
            min_qty: Qty::try_new(1).unwrap(),
            max_qty: Qty::try_new(5).unwrap(),
            min_notional: QuoteAmount::new(10),
            max_notional: QuoteAmount::new(50),
        };
        let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let stop = StopOrder::Limit(StopLimitOrder {
            trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
            order_id: matching_domain::OrderId(1),
            user_id: matching_domain::UserId(1),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            reservation_id: matching_domain::ReservationId(1),
            fee_schedule_version: 1,
            side: matching_domain::Side::Buy,
            stop_price: Price::try_new(12).unwrap(),
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(5).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        assert!(stop_order_fits_limits(&stop, limits, limits, math));
        let mut tighter = limits;
        tighter.max_notional = QuoteAmount::new(49);
        assert!(!stop_order_fits_limits(&stop, limits, tighter, math));
        let market = StopOrder::Market(matching_domain::StopMarketOrder {
            trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: matching_domain::OrderId(2),
            user_id: matching_domain::UserId(1),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            reservation_id: matching_domain::ReservationId(2),
            fee_schedule_version: 1,
            side: matching_domain::Side::Buy,
            stop_price: Price::try_new(12).unwrap(),
            quantity: matching_domain::StopMarketQuantity::Base(Qty::try_new(5).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        });
        assert!(stop_order_fits_limits(&market, limits, limits, math));
        assert!(!stop_order_fits_limits(&market, limits, tighter, math));
        let mut expanded = limits;
        expanded.max_qty = Qty::try_new(6).unwrap();
        assert!(stop_order_fits_limits(&market, limits, expanded, math));
        expanded.max_qty = Qty::try_new(4).unwrap();
        assert!(!stop_order_fits_limits(&market, limits, expanded, math));
        let mut quote = market.clone();
        if let StopOrder::Market(order) = &mut quote {
            order.quantity = matching_domain::StopMarketQuantity::Quote(
                matching_domain::QuoteBudget::try_new(40).unwrap(),
            );
        }
        let mut relaxed = limits;
        relaxed.max_qty = Qty::try_new(6).unwrap();
        relaxed.max_notional = QuoteAmount::new(60);
        assert!(stop_order_fits_limits(&quote, limits, relaxed, math));
        assert!(stop_order_fits_limits(&market, limits, relaxed, math));
        let mut narrowed = limits;
        narrowed.max_notional = QuoteAmount::new(40);
        assert!(stop_order_fits_limits(&quote, limits, narrowed, math));
        narrowed.max_notional = QuoteAmount::new(39);
        assert!(!stop_order_fits_limits(&quote, limits, narrowed, math));
        relaxed.min_notional = QuoteAmount::new(11);
        assert!(!stop_order_fits_limits(&quote, limits, relaxed, math));
        assert!(!stop_order_fits_limits(&market, limits, relaxed, math));
    }

    #[test]
    fn active_price_band_update_only_accepts_safe_relaxations() {
        let old = matching_domain::PriceBandConfig {
            reference_kind: matching_domain::PriceReferenceKind::LastTrade,
            max_deviation_bps: 100,
        };
        let mut next = old;
        next.max_deviation_bps = 101;
        assert!(active_band_update_safe(Some(old), Some(next)));
        assert!(active_band_update_safe(Some(old), None));
        next.max_deviation_bps = 99;
        assert!(!active_band_update_safe(Some(old), Some(next)));
        next = old;
        next.reference_kind = matching_domain::PriceReferenceKind::MarkPrice;
        assert!(!active_band_update_safe(Some(old), Some(next)));
        assert!(!active_band_update_safe(None, Some(old)));
        assert!(active_band_update_safe(None, None));
        let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let mut narrower = old;
        narrower.max_deviation_bps = 0;
        assert_eq!(
            active_band_revalidation_safe(&book, Some(narrower), math),
            Ok(false)
        );
    }

    #[test]
    fn price_band_update_rejects_reference_boundary_overflow_atomically() {
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        book.trigger_stops(
            matching_domain::PriceReference {
                kind: matching_domain::PriceReferenceKind::LastTrade,
                source_seq: 1,
                price: matching_domain::Price::try_new(i64::MAX).unwrap(),
            },
            LogicalTime::new(0),
            1,
            &math,
            &[],
            |_| Ok(()),
        )
        .unwrap();
        let before = book.clone();
        let band = matching_domain::PriceBandConfig {
            reference_kind: matching_domain::PriceReferenceKind::LastTrade,
            max_deviation_bps: 1,
        };
        assert_eq!(
            prepare_price_band_update(&book, Some(band), math).err(),
            Some(RejectReason::ArithmeticOverflow)
        );
        let mut activated = book.clone();
        activated.set_price_band_config(Some((band, math))).unwrap();
        assert_eq!(
            checked_price_band_reference(&activated, Some(band)),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(book, before);
    }

    #[test]
    fn stop_limit_update_rejection_preserves_book_and_reservation() {
        stop_management_replays(false, false, 0);
    }

    #[test]
    fn stop_day_session_end_replays_expiry_and_reservation_transition() {
        stop_management_replays(true, false, 0);
    }

    #[test]
    fn stop_quote_configuration_update_and_close_replay() {
        stop_management_replays(false, true, 0);
    }

    #[test]
    fn active_market_can_remove_price_band_and_replay() {
        stop_management_replays(false, false, 1);
    }

    #[test]
    fn active_market_can_widen_price_band_and_replay() {
        stop_management_replays(false, false, 2);
    }

    #[test]
    fn active_market_can_narrow_price_band_when_stop_limit_remains_inside() {
        stop_management_replays(false, false, 3);
    }

    #[test]
    fn active_market_rejects_narrow_band_excluding_stop_limit_and_replays() {
        stop_management_replays(false, false, 4);
    }

    #[test]
    fn active_market_rejects_narrow_band_excluding_resting_order_and_replays() {
        stop_management_replays(false, false, 5);
    }

    #[test]
    fn active_market_can_narrow_band_when_resting_order_remains_inside() {
        stop_management_replays(false, false, 6);
    }

    #[test]
    fn active_market_can_enable_band_when_existing_stop_limit_fits() {
        stop_management_replays(false, false, 7);
    }

    #[test]
    fn active_market_can_change_band_reference_when_existing_orders_fit() {
        stop_management_replays(false, false, 8);
    }

    #[test]
    fn active_market_rejects_band_reference_without_price_and_replays() {
        stop_management_replays(false, false, 9);
    }

    #[test]
    fn active_market_reports_overflow_for_unrepresentable_new_band() {
        stop_management_replays(false, false, 12);
    }

    #[test]
    fn preopen_with_valid_existing_orders_can_open_with_price_band() {
        stop_management_replays(false, false, 10);
    }

    #[test]
    fn preopen_rejects_open_when_existing_price_is_outside_band() {
        stop_management_replays(false, false, 11);
    }

    #[test]
    fn preopen_rejects_open_with_broken_reservation_binding() {
        stop_management_replays(false, false, 13);
    }

    #[test]
    fn dynamic_reference_triggers_stop_in_open_candidate() {
        stop_management_replays(false, false, 14);
    }
    #[test]
    fn dynamic_reference_settles_triggered_trade_and_replays() {
        stop_management_replays(false, false, 15);
    }
    #[test]
    fn dynamic_reference_uses_configured_other_source_for_band() {
        stop_management_replays(false, false, 16);
    }
    #[test]
    fn dynamic_reference_uses_updated_band_for_triggered_child() {
        stop_management_replays(false, false, 17);
    }
    #[test]
    fn preopen_trigger_rests_non_crossing_child() {
        stop_management_replays(false, false, 18);
    }
    #[test]
    fn preopen_trigger_rejects_crossing_child_and_replays() {
        stop_management_replays(false, false, 19);
    }
    #[test]
    fn preopen_band_update_revalidates_before_triggered_child() {
        stop_management_replays(false, false, 20);
    }
    #[test]
    fn suspended_market_rejects_triggered_child_and_replays() {
        stop_management_replays(false, false, 21);
    }
    #[test]
    fn unrelated_reference_update_does_not_require_fee_book() {
        stop_management_replays(false, false, 22);
    }
    #[test]
    fn stop_trade_limit_halts_before_installing_any_candidate() {
        stop_management_replays(false, false, 23);
    }

    fn stop_management_replays(day: bool, quote: bool, band: u8) {
        use matching_domain::{
            AssetId, AssetReserve, ClosePolicy, ExecutionLimits, MarketConfig, OrderLimits,
            OriginSessionId, Price, Qty, QuoteAmount, Side, StopLimitOrder, StopOrder, TimeInForce,
            TriggerExpiry, UserId,
        };
        use matching_risk::{LimitReservePolicy, ReservationStatus, RiskKeySet};
        let market = MarketId(81);
        let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let config = MarketConfig {
            market_id: market,
            price_scale: 0,
            qty_scale: 0,
            quote_scale: 0,
            tick_size: 1,
            lot_size: 1,
            order_limits: OrderLimits {
                min_qty: Qty::try_new(1).unwrap(),
                max_qty: Qty::try_new(5).unwrap(),
                min_notional: QuoteAmount::new(1),
                max_notional: QuoteAmount::new(50),
            },
            price_band: (band != 0
                && band != 7
                && band != 14
                && band != 15
                && band != 18
                && band != 19
                && band != 21
                && band != 22
                && band != 23)
                .then_some(matching_domain::PriceBandConfig {
                    reference_kind: if band == 17 || band == 20 {
                        matching_domain::PriceReferenceKind::MarkPrice
                    } else {
                        matching_domain::PriceReferenceKind::LastTrade
                    },
                    max_deviation_bps: if band == 4 || band == 5 {
                        1000
                    } else if band == 16 || band == 17 || band == 20 {
                        0
                    } else {
                        100
                    },
                }),
            active_fee_schedule_version: 1,
            close_policy: ClosePolicy::CancelAll,
            execution_limits: ExecutionLimits {
                max_internal_actions_per_command: 10,
                max_trades_per_command: if band == 23 { 1 } else { 10 },
                priority_exhaustion_guard: 1,
                synthetic_priority_ceiling: None,
            },
        };
        let mut state = matching_domain::ManagedMarket::new(config.clone()).unwrap();
        state.start_session(1, true).unwrap();
        if (14..=17).contains(&band) || band == 21 || band == 23 {
            state.open(1, true, true).unwrap();
        }
        if band == 21 {
            state.suspend().unwrap();
        }
        state.record_referenced_history();
        let reservation = ReservationState {
            reservation_id: ReservationId(1),
            order_id: OrderId(1),
            user_id: UserId(1),
            market_id: market,
            origin_session_id: OriginSessionId(1),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms: 50,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let stop = if quote {
            StopOrder::Market(matching_domain::StopMarketOrder {
                trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
                market_protection: None,
                order_id: OrderId(1),
                user_id: UserId(1),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                reservation_id: ReservationId(1),
                fee_schedule_version: 1,
                side: Side::Buy,
                stop_price: Price::try_new(12).unwrap(),
                quantity: matching_domain::StopMarketQuantity::Quote(
                    matching_domain::QuoteBudget::try_new(50).unwrap(),
                ),
                child_tif: TimeInForce::Ioc,
                trigger_expiry: TriggerExpiry::Gtc,
            })
        } else {
            StopOrder::Limit(StopLimitOrder {
                trigger_reference: if (14..=23).contains(&band) {
                    matching_domain::PriceReferenceKind::MarkPrice
                } else {
                    matching_domain::PriceReferenceKind::LastTrade
                },
                order_id: OrderId(1),
                user_id: UserId(1),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                reservation_id: ReservationId(1),
                fee_schedule_version: 1,
                side: Side::Buy,
                stop_price: Price::try_new(12).unwrap(),
                price: Price::try_new(if band == 4 { 11 } else { 10 }).unwrap(),
                qty: Qty::try_new(if band == 4 { 4 } else { 5 }).unwrap(),
                child_tif: TimeInForce::Gtc,
                trigger_expiry: if day {
                    TriggerExpiry::Day { session_id: 1 }
                } else {
                    TriggerExpiry::Gtc
                },
            })
        };
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        book.bind_market(market).unwrap();
        if let Some(band) = config.price_band {
            book.set_price_band_config(Some((band, math))).unwrap();
        }
        if config.price_band.is_some() || band == 7 {
            book.trigger_stops(
                matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::LastTrade,
                    source_seq: 1,
                    price: Price::try_new(10).unwrap(),
                },
                LogicalTime::new(0),
                1,
                &math,
                &[],
                |_| Ok(()),
            )
            .unwrap();
        }
        if band == 8 || band == 12 {
            book.trigger_stops(
                matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::MarkPrice,
                    source_seq: 1,
                    price: Price::try_new(if band == 12 { i64::MAX } else { 10 }).unwrap(),
                },
                LogicalTime::new(0),
                1,
                &math,
                &[],
                |_| Ok(()),
            )
            .unwrap();
        }
        if (14..=23).contains(&band) {
            book.trigger_stops(
                matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::MarkPrice,
                    source_seq: 1,
                    price: Price::try_new(10).unwrap(),
                },
                LogicalTime::new(0),
                1,
                &math,
                &[],
                |_| Ok(()),
            )
            .unwrap();
        }
        if band == 5 || band == 6 {
            book.rest_with_reservation(
                matching_domain::LimitOrder {
                    order_id: OrderId(1),
                    user_id: UserId(1),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(if band == 5 { 11 } else { 10 }).unwrap(),
                    qty: Qty::try_new(4).unwrap(),
                },
                reservation.clone(),
            )
            .unwrap();
        } else {
            book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, reservation.clone())
                .unwrap();
        }
        if band == 11 {
            book.trigger_stops(
                matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::LastTrade,
                    source_seq: 2,
                    price: Price::try_new(9).unwrap(),
                },
                LogicalTime::new(0),
                1,
                &math,
                &[],
                |_| Ok(()),
            )
            .unwrap();
        }
        let mut reservations = vec![reservation];
        if band == 15 || band == 19 || band == 20 || band == 23 {
            let maker = ReservationState {
                reservation_id: ReservationId(2),
                order_id: OrderId(2),
                user_id: UserId(2),
                market_id: market,
                origin_session_id: OriginSessionId(1),
                version: 0,
                reserves: vec![AssetReserve {
                    asset: AssetId(1),
                    atoms: if band == 23 { 2 } else { 5 },
                }],
                status: ReservationStatus::Active,
                fee_schedule_version: 1,
                fee_tier_id: 1,
            };
            book.rest_with_reservation(
                matching_domain::LimitOrder {
                    order_id: OrderId(2),
                    user_id: UserId(2),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Sell,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(if band == 23 { 2 } else { 5 }).unwrap(),
                },
                maker.clone(),
            )
            .unwrap();
            reservations.push(maker);
            if band == 23 {
                let maker = ReservationState {
                    reservation_id: ReservationId(3),
                    order_id: OrderId(3),
                    user_id: UserId(3),
                    market_id: market,
                    origin_session_id: OriginSessionId(1),
                    version: 0,
                    reserves: vec![AssetReserve {
                        asset: AssetId(1),
                        atoms: 3,
                    }],
                    status: ReservationStatus::Active,
                    fee_schedule_version: 1,
                    fee_tier_id: 1,
                };
                book.rest_with_reservation(
                    matching_domain::LimitOrder {
                        order_id: OrderId(3),
                        user_id: UserId(3),
                        stp_key: matching_domain::StpKey::new(0),
                        stp_mode: matching_domain::StpMode::None,
                        side: Side::Sell,
                        price: Price::try_new(10).unwrap(),
                        qty: Qty::try_new(3).unwrap(),
                    },
                    maker.clone(),
                )
                .unwrap();
                reservations.push(maker);
            }
        }
        let mut books = BTreeMap::from([(market, book.clone())]);
        let mut registry = Some(ReservationRegistry::new(reservations).unwrap());
        let before_registry = registry.clone();
        let mut managed = BTreeMap::from([(market, state.clone())]);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(1),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let settings = crate::PlaceSupportConfig::new(
            RiskKeySet::new(1, []).unwrap(),
            [(market, policy)],
            crate::PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap();
        let trusted = crate::SnapshotRestoreConfig {
            cluster: ClusterId(1),
            shard: ShardId(1),
            primary_epoch: 7,
            routes: routes.clone(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(settings.clone()),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let mut place = Some(PlaceRuntimeState {
            market_data: None,
            config: settings,
            ids: ClientOrderIds::with_max_id_bytes(8, 8, 32).unwrap(),
            fee_book: None,
            shard_kill_active: false,
        });
        let mut window = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        if (14..=23).contains(&band) {
            if band != 22 {
                place.as_mut().unwrap().fee_book = Some(matching_domain::FeeScheduleBook::new(
                    matching_domain::FeeSchedule::new(
                        1,
                        matching_domain::FeeAssetPolicy::Quote,
                        [(
                            1,
                            matching_domain::FeeTier {
                                maker_bps: 0,
                                taker_bps: 0,
                                rounding: matching_domain::FeeRounding::Floor,
                            },
                        )],
                    )
                    .unwrap(),
                ));
            }
            let command = CommandEnvelope {
                command_id: CommandId(1),
                session_id: 1,
                client_seq: 1,
                market_id: Some(market),
                user_id: None,
                route_epoch: Some(1),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::UpdateReferencePrice(matching_domain::UpdateReferencePrice {
                    market_id: market,
                    kind: if band == 22 {
                        matching_domain::PriceReferenceKind::IndexPrice
                    } else {
                        matching_domain::PriceReferenceKind::MarkPrice
                    },
                    price: Price::try_new(13).unwrap(),
                    source_seq: 2,
                }),
            };
            let mut replay_candidate = empty_recovery_candidate(
                &trusted,
                &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
                1,
            )
            .unwrap();
            replay_candidate.image.markets = books.clone();
            replay_candidate.image.managed_markets = managed.clone();
            replay_candidate.image.registry = registry.clone();
            let state_place = place.as_ref().unwrap();
            replay_candidate.image.place = Some(PlaceRuntimeState {
                market_data: state_place.market_data.clone(),
                config: state_place.config.clone(),
                ids: state_place.ids.try_clone_preallocated().unwrap(),
                fee_book: state_place.fee_book.clone(),
                shard_kill_active: state_place.shard_kill_active,
            });
            let bytes = replay_candidate
                .image
                .encode_snapshot(trusted.max_bytes)
                .unwrap();
            replay_candidate.image = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
            replay_candidate.committed_through = 1;
            let run = || {
                execute_market_management(
                    MarketManagementContext {
                        controls: None,
                        identity: FormalIdentity {
                            shard: ShardId(1),
                            cluster: ClusterId(1),
                            event_capacity: 10,
                        },
                        routes: &routes,
                        seq: &mut seq,
                        clock: &LogicalClock::new(LogicalTime::new(0)),
                        markets: &mut books,
                        managed_markets: &mut managed,
                        dedup: &mut window,
                        place: &mut place,
                        registry: &mut registry,
                    },
                    command.clone(),
                )
            };
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
            if band == 23 {
                assert!(caught.is_err());
                assert_eq!(books.get(&market), Some(&book));
                assert_eq!(registry, before_registry);
                assert_eq!(seq.last_applied(), 0);
                return;
            }
            let result = caught.unwrap().unwrap();
            assert_eq!(result.outcome, FormalOutcome::MarketManaged);
            if band != 22 {
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::StopTriggered {
                        order_id: OrderId(1),
                        ..
                    }
                )));
            }
            if band == 14 || band == 16 || band == 18 {
                assert!(books[&market].order(OrderId(1)).is_some());
                assert_eq!(result.market_data_child_opens.len(), 1);
                assert_eq!(result.market_data_child_opens[0].order_id, OrderId(1));
                assert!(result.market_data_child_opens[0].after_event_count <= result.events.len());
                let mut projector = crate::MarketProjector::new(market);
                let source = projector
                    .apply_formal_execution(&result, |_, _| None)
                    .unwrap();
                assert!(source.iter().any(|event| matches!(
                    event.payload,
                    crate::SourcePayload::OrderAdded {
                        order_id: OrderId(1),
                        ..
                    }
                )));
            } else if band == 15 {
                assert!(
                    result
                        .events
                        .iter()
                        .any(|e| matches!(e.payload, EngineEvent::Trade(_)))
                );
                assert!(
                    result
                        .events
                        .iter()
                        .any(|e| matches!(e.payload, EngineEvent::SettlementDelta(_)))
                );
                assert!(books[&market].order(OrderId(1)).is_none());
                assert!(books[&market].order(OrderId(2)).is_none());
            } else if band == 17 {
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::TriggeredOrderRejected {
                        reason: RejectReason::PriceBandExceeded,
                        ..
                    }
                )));
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::ReservationReleased {
                        reason: matching_domain::ReservationReleaseReason::TriggerRejected,
                        released: Some(_),
                        ..
                    }
                )));
                assert!(books[&market].order(OrderId(1)).is_none());
            } else if band == 19 {
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::TriggeredOrderRejected {
                        reason: RejectReason::PreOpenWouldCross,
                        ..
                    }
                )));
                assert!(books[&market].order(OrderId(1)).is_none());
                assert!(books[&market].order(OrderId(2)).is_some());
            } else if band == 20 {
                assert!(matches!(
                    result.events.first().map(|e| &e.payload),
                    Some(EngineEvent::OrderCanceled {
                        order_id: OrderId(2),
                        reason: matching_domain::CancelReason::PriceBandRevalidation,
                        ..
                    })
                ));
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::TriggeredOrderRejected {
                        reason: RejectReason::PriceBandExceeded,
                        ..
                    }
                )));
                assert!(books[&market].order(OrderId(2)).is_none());
            } else if band == 21 {
                assert!(result.events.iter().any(|e| matches!(
                    e.payload,
                    EngineEvent::TriggeredOrderRejected {
                        reason: RejectReason::MarketSuspended,
                        ..
                    }
                )));
                assert!(books[&market].order(OrderId(1)).is_none());
            } else {
                assert!(result.events.is_empty());
                assert!(books[&market].stop_order(OrderId(1)).is_some());
            }
            let replayed = replay_snapshot_records(
                replay_candidate,
                vec![crate::RecoveredCommand {
                    shard_seq: 1,
                    epoch: 7,
                    engine_time: 0,
                    command: matching_domain::decode_wal_command(
                        &matching_domain::encode_wal_command(&command).unwrap(),
                    )
                    .unwrap(),
                }],
            )
            .unwrap();
            assert_eq!(replayed.executions[0].events, result.events);
            assert_eq!(replayed.image.markets, books);
            assert_eq!(replayed.image.registry, registry);
            let mut live = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
            // 直接调用管理函数的夹具自行执行与正式分片相同的旧快照行情初始化。
            let (mut live_data, _) =
                crate::MarketDataState::bootstrap_legacy(&live.markets, live.snapshot_seq).unwrap();
            live_data.apply(&result).unwrap();
            live.snapshot_seq = seq.last_applied();
            live.markets = books;
            live.registry = registry;
            live.managed_markets = managed;
            live.dedup = Some(window);
            live.place = place;
            live.place.as_mut().unwrap().market_data = Some(live_data);
            assert_eq!(
                replayed.image.state_digest(trusted.max_bytes).unwrap(),
                live.state_digest(trusted.max_bytes).unwrap()
            );
            let mut hash = crate::EventHasher::new();
            for event in &result.events {
                hash.update(event).unwrap();
            }
            assert_eq!(replayed.event_hash, hash.finish());
            return;
        }
        let mut next = config;
        next.order_limits.max_notional = QuoteAmount::new(49);
        if band == 6 {
            next.order_limits.max_notional = QuoteAmount::new(39);
        }
        if band == 4 || band == 5 {
            next.order_limits.max_notional = QuoteAmount::new(60);
            next.price_band.as_mut().unwrap().max_deviation_bps = 0;
        }
        if band == 9 || band == 12 {
            next.order_limits.max_notional = QuoteAmount::new(60);
            next.price_band.as_mut().unwrap().reference_kind =
                matching_domain::PriceReferenceKind::MarkPrice;
            if band == 12 {
                next.price_band.as_mut().unwrap().max_deviation_bps = 1;
            }
        }
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
                config: next,
            })),
        };
        let mut replay_candidate = empty_recovery_candidate(
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
            1,
        )
        .unwrap();
        replay_candidate.image.markets = books.clone();
        replay_candidate.image.managed_markets = managed.clone();
        replay_candidate.image.registry = registry.clone();
        // 此夹具直接注入已有活单且没有行情历史，按旧快照路径验证管理重放。
        replay_candidate.image.place.as_mut().unwrap().market_data = None;
        let bytes = replay_candidate
            .image
            .encode_snapshot(trusted.max_bytes)
            .unwrap();
        replay_candidate.image = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
        let wal_payload = matching_domain::encode_wal_command(&command).unwrap();
        let decoded_command = matching_domain::decode_wal_command(&wal_payload).unwrap();
        let result = execute_market_management(
            MarketManagementContext {
                controls: None,
                identity: FormalIdentity {
                    shard: ShardId(1),
                    cluster: ClusterId(1),
                    event_capacity: 10,
                },
                routes: &routes,
                seq: &mut seq,
                clock: &LogicalClock::new(LogicalTime::new(0)),
                markets: &mut books,
                managed_markets: &mut managed,
                dedup: &mut window,
                place: &mut place,
                registry: &mut registry,
            },
            command,
        )
        .unwrap();
        assert_eq!(
            result.outcome,
            FormalOutcome::Rejected(if band == 12 {
                RejectReason::ArithmeticOverflow
            } else {
                RejectReason::InvalidOrderCombination
            })
        );
        assert_eq!(books.get(&market), Some(&book));
        assert_eq!(registry, before_registry);
        assert_eq!(managed.get(&market), Some(&state));
        if band == 13 {
            let mut broken_registry = Some(ReservationRegistry::new([]).unwrap());
            let invalid_open = CommandEnvelope {
                command_id: CommandId(2),
                session_id: 1,
                client_seq: 2,
                market_id: Some(market),
                user_id: None,
                route_epoch: Some(1),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::OpenMarket(matching_domain::OpenMarket {
                    market_id: market,
                    session_id: 1,
                }),
            };
            let rejected = execute_market_management(
                MarketManagementContext {
                    controls: None,
                    identity: FormalIdentity {
                        shard: ShardId(1),
                        cluster: ClusterId(1),
                        event_capacity: 10,
                    },
                    routes: &routes,
                    seq: &mut seq,
                    clock: &LogicalClock::new(LogicalTime::new(0)),
                    markets: &mut books,
                    managed_markets: &mut managed,
                    dedup: &mut window,
                    place: &mut place,
                    registry: &mut broken_registry,
                },
                invalid_open,
            )
            .unwrap();
            assert_eq!(
                rejected.outcome,
                FormalOutcome::Rejected(RejectReason::InvalidReservation)
            );
            assert_eq!(books.get(&market), Some(&book));
            assert_eq!(managed.get(&market), Some(&state));
            assert_eq!(broken_registry, Some(ReservationRegistry::new([]).unwrap()));
            return;
        }
        let mut allowed = state.config().clone();
        allowed.order_limits.max_notional = QuoteAmount::new(60);
        if band == 1 {
            allowed.price_band = None;
        }
        if band == 2 {
            allowed.price_band.as_mut().unwrap().max_deviation_bps = 101;
        }
        if band == 3 || band == 6 {
            allowed.price_band.as_mut().unwrap().max_deviation_bps = 0;
        }
        if band == 4 || band == 5 || band == 9 || band == 12 {
            allowed.price_band = None;
        }
        if band == 7 {
            allowed.price_band = Some(matching_domain::PriceBandConfig {
                reference_kind: matching_domain::PriceReferenceKind::LastTrade,
                max_deviation_bps: 0,
            });
        }
        if band == 8 {
            let price_band = allowed.price_band.as_mut().unwrap();
            price_band.reference_kind = matching_domain::PriceReferenceKind::MarkPrice;
            price_band.max_deviation_bps = 0;
        }
        let accepted_command = CommandEnvelope {
            command_id: CommandId(2),
            session_id: 1,
            client_seq: 2,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
                config: allowed,
            })),
        };
        let close_command = CommandEnvelope {
            command_id: CommandId(3),
            session_id: 1,
            client_seq: 3,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: if band == 10 || band == 11 {
                Command::OpenMarket(matching_domain::OpenMarket {
                    market_id: market,
                    session_id: 1,
                })
            } else if day {
                Command::EndSession(matching_domain::EndSession {
                    market_id: market,
                    session_id: 1,
                })
            } else {
                Command::CloseMarket(matching_domain::CloseMarket { market_id: market })
            },
        };
        let mut executions = Vec::new();
        for next_command in [accepted_command.clone(), close_command.clone()] {
            executions.push(
                execute_market_management(
                    MarketManagementContext {
                        controls: None,
                        identity: FormalIdentity {
                            shard: ShardId(1),
                            cluster: ClusterId(1),
                            event_capacity: 10,
                        },
                        routes: &routes,
                        seq: &mut seq,
                        clock: &LogicalClock::new(LogicalTime::new(0)),
                        markets: &mut books,
                        managed_markets: &mut managed,
                        dedup: &mut window,
                        place: &mut place,
                        registry: &mut registry,
                    },
                    next_command,
                )
                .unwrap(),
            );
        }
        assert_eq!(executions[0].outcome, FormalOutcome::MarketManaged);
        if band == 10 {
            assert_eq!(executions[1].outcome, FormalOutcome::MarketManaged);
        } else if band == 11 {
            assert_eq!(
                executions[1].outcome,
                FormalOutcome::Rejected(RejectReason::InvalidMarketStateTransition)
            );
        } else if day {
            assert!(matches!(
                executions[1].events[0].payload,
                EngineEvent::OrderExpired {
                    order_id: OrderId(1),
                    reason: matching_domain::ExpireReason::StopTriggerSessionEnded
                }
            ));
        } else {
            assert!(matches!(
                executions[1].events[0].payload,
                EngineEvent::OrderCanceled {
                    order_id: OrderId(1),
                    reason: matching_domain::CancelReason::MarketClosed,
                    ..
                }
            ));
        }
        if band == 10 || band == 11 {
            assert!(books[&market].stop_order(OrderId(1)).is_some());
        } else {
            assert!(books[&market].stop_order(OrderId(1)).is_none());
            assert!(registry.as_ref().unwrap().is_pending_r2(ReservationId(1)));
        }
        replay_candidate.committed_through = 3;
        let replayed = replay_snapshot_records(
            replay_candidate,
            vec![
                crate::RecoveredCommand {
                    shard_seq: 1,
                    epoch: 7,
                    engine_time: 0,
                    command: decoded_command,
                },
                crate::RecoveredCommand {
                    shard_seq: 2,
                    epoch: 7,
                    engine_time: 0,
                    command: matching_domain::decode_wal_command(
                        &matching_domain::encode_wal_command(&accepted_command).unwrap(),
                    )
                    .unwrap(),
                },
                crate::RecoveredCommand {
                    shard_seq: 3,
                    epoch: 7,
                    engine_time: 0,
                    command: matching_domain::decode_wal_command(
                        &matching_domain::encode_wal_command(&close_command).unwrap(),
                    )
                    .unwrap(),
                },
            ],
        )
        .unwrap();
        assert_eq!(replayed.executions[0].events, result.events);
        assert_eq!(replayed.executions[1].events, executions[0].events);
        assert_eq!(replayed.executions[2].events, executions[1].events);
        assert_eq!(replayed.image.markets, books);
        assert_eq!(replayed.image.registry, registry);
        assert_eq!(replayed.image.managed_markets, managed);
        let mut live_image = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
        // 直接调用管理函数的夹具重现旧快照显式行情初始化及逐命令推进。
        let (mut live_data, _) =
            crate::MarketDataState::bootstrap_legacy(&live_image.markets, live_image.snapshot_seq)
                .unwrap();
        live_data.apply(&result).unwrap();
        for execution in &executions {
            live_data.apply(execution).unwrap();
        }
        live_image.snapshot_seq = seq.last_applied();
        live_image.markets = books;
        live_image.registry = registry;
        live_image.managed_markets = managed;
        live_image.dedup = Some(window);
        live_image.place = place;
        live_image.place.as_mut().unwrap().market_data = Some(live_data);
        assert_eq!(
            replayed.image.state_digest(trusted.max_bytes).unwrap(),
            live_image.state_digest(trusted.max_bytes).unwrap()
        );
        let mut live_hash = crate::EventHasher::new();
        for event in result
            .events
            .iter()
            .chain(executions.iter().flat_map(|entry| entry.events.iter()))
        {
            live_hash.update(event).unwrap();
        }
        assert_eq!(replayed.event_hash, live_hash.finish());
    }

    fn retry_busy<T>(mut f: impl FnMut() -> Result<T, ShardError>) -> T {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match f() {
                Ok(value) => return value,
                Err(ShardError::Busy) if Instant::now() < deadline => thread::yield_now(),
                Err(error) => panic!("入口失败: {error:?}"),
            }
        }
    }
    #[test]
    fn missing_owned_market_is_rejected_and_shutdown_is_repeatable() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        assert!(matches!(
            Shard::start(ShardId(1), ClusterId(1000), routes.clone(), vec![]),
            Err(ShardError::InvalidConfig)
        ));
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1000),
            routes,
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        shard.shutdown(Duration::from_secs(1)).unwrap();
        assert_eq!(
            shard.shutdown(Duration::from_secs(1)),
            Err(ShardError::Closed)
        );
    }

    #[test]
    fn concurrent_producers_and_barrier_have_contiguous_accepted_sequences() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1001),
            routes,
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let start = std::sync::Arc::new(std::sync::Barrier::new(101));
        let mut producers = Vec::new();
        for n in 0..100 {
            let handle = shard.handle();
            let start = start.clone();
            producers.push(thread::spawn(move || {
                start.wait();
                handle
                    .submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(n)))
                    .ok()
            }));
        }
        start.wait();
        let mut seqs = Vec::new();
        for producer in producers {
            if let Some(rx) = producer.join().unwrap() {
                seqs.push(rx.recv().unwrap().unwrap().seq);
            }
        }
        seqs.sort_unstable();
        assert!(!seqs.is_empty());
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
        let (id, last) = retry_busy(|| shard.barrier(55)).recv().unwrap();
        assert_eq!((id, last), (55, seqs.len() as u64));
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn dropping_owner_closes_admission_and_drains_accepted_message() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        let shard = Shard::start(
            ShardId(1),
            ClusterId(1002),
            routes,
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let accepted = handle
            .submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(9)))
            .unwrap();
        drop(shard);
        assert_eq!(
            accepted
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap()
                .seq,
            1
        );
        assert!(matches!(
            handle.submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(10))),
            Err(ShardError::Closed)
        ));
    }

    #[test]
    fn real_math_invariant_panic_fails_closed() {
        use matching_domain::{
            MarketByBase, MarketMath, PriceBandConfig, PriceReferenceKind, Qty, Side, StpKey,
            StpMode, UserId,
        };
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        book.set_price_band_config(Some((
            PriceBandConfig {
                reference_kind: PriceReferenceKind::LastTrade,
                max_deviation_bps: 0,
            },
            math,
        )))
        .unwrap();
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1003),
            routes,
            vec![(MarketId(3), book)],
        )
        .unwrap();
        let handle = shard.handle();
        let health = crate::Health::new(vec![handle.clone()]).unwrap();
        for dependency in [
            crate::HealthDependency::Recovery,
            crate::HealthDependency::Persistence,
            crate::HealthDependency::Control,
        ] {
            health.set_healthy(dependency, true);
        }
        assert!(health.ready());
        let input = MarketByBase {
            order_id: OrderId(5),
            user_id: UserId(3),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            qty: Qty::try_new(2).unwrap(),
            market_protection: None,
        };
        let wrong_math = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
        let rx = retry_busy(|| {
            handle.submit(
                CommandId(1),
                MarketId(3),
                7,
                Mutation::MarketByBase(input.clone(), wrong_math),
            )
        });
        assert!(rx.recv_timeout(Duration::from_secs(2)).is_err());
        let deadline = Instant::now() + Duration::from_secs(2);
        while !handle.failed.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(!health.ready());
        assert!(!health.live());
        assert!(matches!(
            handle.submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(9))),
            Err(ShardError::Failed)
        ));
        assert_eq!(
            shard.shutdown(Duration::from_secs(1)),
            Err(ShardError::Failed)
        );
    }

    #[test]
    fn configured_worker_handles_query_and_barrier() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start_with_config(
            ShardId(1),
            ClusterId(1004),
            routes,
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
            ShardConfig {
                capacity: 8,
                mutation_reserved_slots: 1,
                max_query_inflight: 3,
                max_control_inflight: 3,
                wait_strategy: WaitStrategy::Park,
                require_route_authority: false,
            },
        )
        .unwrap();
        let handle = shard.handle();
        assert!(
            handle
                .order(MarketId(3), OrderId(8))
                .unwrap()
                .recv()
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(handle.barrier(42).unwrap().recv().unwrap(), (42, 0));
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn business_reject_consumes_seq_and_query_does_not() {
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1005),
            RouteTable::new([MarketRoute {
                market_id: MarketId(3),
                shard_id: ShardId(1),
                route_epoch: 7,
                state: RouteState::Active,
            }])
            .unwrap(),
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let a =
            retry_busy(|| shard.submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(9))))
                .recv()
                .unwrap()
                .unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(a.result, Err(RejectReason::OrderNotFound));
        assert!(
            retry_busy(|| shard.order(MarketId(3), OrderId(9)))
                .recv()
                .unwrap()
                .unwrap()
                .is_none()
        );
        let b =
            retry_busy(|| shard.submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(9))))
                .recv()
                .unwrap()
                .unwrap();
        assert_eq!(b.seq, 2);
        shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn timeout_after_admission_closed_keeps_ingress_closed_and_can_retry() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1017),
            routes,
            vec![(
                MarketId(3),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        shard.producer.close_ingress();
        assert_eq!(shard.shutdown(Duration::ZERO), Err(ShardError::Timeout));
        assert!(matches!(
            handle.order(MarketId(3), OrderId(1)),
            Err(ShardError::Closed)
        ));
        assert!(matches!(handle.barrier(1), Err(ShardError::Closed)));
        assert!(matches!(
            handle.submit(CommandId(1), MarketId(3), 7, Mutation::Cancel(OrderId(1))),
            Err(ShardError::Closed)
        ));
        assert_eq!(shard.shutdown(Duration::from_secs(2)), Ok(()));
    }

    #[test]
    fn formal_event_capacity_failure_discards_order_and_reservation_changes() {
        use matching_domain::{
            AssetId, AssetReserve, MassCancelShard, OriginSessionId, Price, Qty, Side, StpKey,
            StpMode, UserId,
        };
        use matching_risk::ReservationStatus;
        let market = MarketId(71);
        let state = ReservationState {
            reservation_id: ReservationId(9),
            order_id: OrderId(8),
            user_id: UserId(7),
            market_id: market,
            origin_session_id: OriginSessionId(6),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(1),
                atoms: 100,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let mut book = ProductionOrderBook::new(
            matching_orderbook::QueuePriorityPolicy::try_new(1024).unwrap(),
        );
        book.bind_market(market).unwrap();
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(8),
                user_id: UserId(7),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            state.clone(),
        )
        .unwrap();
        let mut books = BTreeMap::from([(market, book.clone())]);
        let mut registry = Some(ReservationRegistry::new([state]).unwrap());
        let before = registry.clone();
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(2),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        };
        let mut sequence = Sequencer::new();
        let mut window = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let outcome = execute_formal_idempotent(
            &routes,
            FormalIdentity {
                shard: ShardId(2),
                cluster: ClusterId(1),
                event_capacity: 0,
            },
            &mut sequence,
            &LogicalClock::new(LogicalTime::new(0)),
            &mut books,
            &mut registry,
            (command.clone(), &mut window, &mut None, &mut None),
        )
        .unwrap();
        assert_eq!(sequence.last_applied(), 1);
        assert_eq!(books.get(&market), Some(&book));
        assert_eq!(registry, before);
        assert_eq!(outcome.events.len(), 1);
        assert_eq!(
            outcome.outcome,
            FormalOutcome::Rejected(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(outcome.events[0].event_index, 0);
        assert_eq!(
            outcome.events[0].payload,
            EngineEvent::CommandRejected {
                reason: RejectReason::ArithmeticOverflow
            }
        );
        let retry = execute_formal_idempotent(
            &routes,
            FormalIdentity {
                shard: ShardId(2),
                cluster: ClusterId(1),
                event_capacity: 0,
            },
            &mut sequence,
            &LogicalClock::new(LogicalTime::new(0)),
            &mut books,
            &mut registry,
            (command, &mut window, &mut None, &mut None),
        )
        .unwrap();
        assert!(retry.deduplicated);
        assert_eq!(retry.summary, outcome.summary);
        assert!(retry.events.is_empty());
        assert_eq!(sequence.last_applied(), 1);
        assert_eq!(books.get(&market), Some(&book));
        assert_eq!(registry, before);
    }

    #[test]
    fn event_capacity_allows_every_u32_index_including_maximum() {
        let capacity = u64::from(u32::MAX) + 1;
        assert!(event_count_fits(capacity, capacity));
        assert!(!event_count_fits(capacity + 1, capacity));
    }

    #[test]
    fn formal_registry_invariant_failure_stops_without_installing_candidate() {
        use matching_domain::{
            AssetId, AssetReserve, MassCancelShard, OriginSessionId, Price, Qty, Side, StpKey,
            StpMode, UserId,
        };
        use matching_risk::ReservationStatus;
        let market = MarketId(72);
        let state = ReservationState {
            reservation_id: ReservationId(19),
            order_id: OrderId(18),
            user_id: UserId(17),
            market_id: market,
            origin_session_id: OriginSessionId(16),
            version: 0,
            reserves: vec![AssetReserve {
                asset: AssetId(1),
                atoms: 100,
            }],
            status: ReservationStatus::Active,
            fee_schedule_version: 1,
            fee_tier_id: 1,
        };
        let mut book = ProductionOrderBook::new(
            matching_orderbook::QueuePriorityPolicy::try_new(1024).unwrap(),
        );
        book.bind_market(market).unwrap();
        book.rest_with_reservation(
            LimitOrder {
                order_id: OrderId(18),
                user_id: UserId(17),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            state,
        )
        .unwrap();
        let mut books = BTreeMap::from([(market, book.clone())]);
        // 故意构造与订单影子不一致的登记表，验证不能降级为普通业务拒绝。
        let mut registry = Some(ReservationRegistry::new([]).unwrap());
        let before = registry.clone();
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(2),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        };
        let mut sequence = Sequencer::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_formal(
                &routes,
                FormalIdentity {
                    shard: ShardId(2),
                    cluster: ClusterId(1),
                    event_capacity: u64::from(u32::MAX) + 1,
                },
                &mut sequence,
                &LogicalClock::new(LogicalTime::new(0)),
                &mut books,
                &mut registry,
                (command, &mut None, false, false),
            )
        }));
        assert!(result.is_err());
        assert_eq!(sequence.last_applied(), 1);
        assert_eq!(books.get(&market), Some(&book));
        assert_eq!(registry, before);
    }
}

#[cfg(test)]
mod concurrent_real_orders {
    use super::*;
    use crate::{MarketRoute, RouteState};
    use matching_domain::{Price, Qty, Side, StpKey, StpMode, UserId};
    use matching_orderbook::QueuePriorityPolicy;
    use std::sync::Barrier;

    #[test]
    fn hundred_producers_each_apply_one_real_order_exactly_once() {
        let route = MarketRoute {
            market_id: MarketId(3),
            shard_id: ShardId(1),
            route_epoch: 7,
            state: RouteState::Active,
        };
        let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        let mut shard = Shard::start(
            ShardId(1),
            ClusterId(1006),
            RouteTable::new([route]).unwrap(),
            vec![(MarketId(3), book)],
        )
        .unwrap();
        let start = Arc::new(Barrier::new(101));
        let mut workers = Vec::new();
        for n in 0..100u128 {
            let handle = shard.handle();
            let start = Arc::clone(&start);
            workers.push(thread::spawn(move || {
                let order = LimitOrder {
                    order_id: OrderId(n + 1),
                    user_id: UserId(n as u64 + 1),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(100).unwrap(),
                    qty: Qty::try_new(1).unwrap(),
                };
                start.wait();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    match handle.submit(
                        CommandId(n + 1),
                        MarketId(3),
                        7,
                        Mutation::Place(order.clone()),
                    ) {
                        Ok(reply) => {
                            return reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
                        }
                        Err(ShardError::Busy) if Instant::now() < deadline => thread::yield_now(),
                        Err(error) => panic!("入口未接收订单: {error:?}"),
                    }
                }
            }));
        }
        start.wait();
        let mut sequences = Vec::new();
        for worker in workers {
            let result = worker.join().unwrap();
            assert!(result.result.is_ok());
            sequences.push(result.seq);
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=100).collect::<Vec<_>>());
        let handle = shard.handle();
        for id in 1..=100u128 {
            let deadline = Instant::now() + Duration::from_secs(5);
            let response = loop {
                match handle.order(MarketId(3), OrderId(id)) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if Instant::now() < deadline => thread::yield_now(),
                    Err(error) => panic!("查询入口失败: {error:?}"),
                }
            };
            assert_eq!(
                response
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .original_order()
                    .order_id,
                OrderId(id)
            );
        }
        shard.shutdown(Duration::from_secs(5)).unwrap();
    }
}

/// 验证限价单事件索引容量不足时的根命令原子性。
#[cfg(test)]
mod place_event_capacity_tests {
    use super::*;
    use crate::place_config::PlaceLimits;
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetId, AssetReserve, MarketMath, PlaceLimitOrder, Price, Qty, ReservationToken, Side,
        StpKey, StpMode, UserId,
    };
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, RiskKeySet};

    #[test]
    fn capacity_overflow_discards_signed_place_and_caches_rejection() {
        let market = MarketId(88);
        let signing = SigningKey::from_bytes(&[7; 32]);
        let keys = RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap();
        let policy = LimitReservePolicy::new(
            (AssetId(1), 0),
            (AssetId(2), 0),
            MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            1,
            1,
            0,
        )
        .unwrap();
        let config = PlaceSupportConfig::new(
            keys,
            [(market, policy)],
            PlaceLimits {
                active_orders: 2,
                terminal_history: 2,
                client_id_bytes: 32,
            },
        )
        .unwrap();
        let ids = ClientOrderIds::with_max_id_bytes(2, 2, 32).unwrap();
        let mut state = PlaceRuntimeState {
            market_data: None,
            config,
            ids,
            fee_book: None,
            shard_kill_active: false,
        };
        let ids_before = state.ids.clone();
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
        book.bind_market(market).unwrap();
        let priority_before = book.next_queue_priority();
        let mut markets = BTreeMap::from([(market, book)]);
        let mut registry = ReservationRegistry::new([]).unwrap();
        let registry_before = registry.clone();
        let mut token = ReservationToken {
            token_id: 1,
            command_id: CommandId(1),
            reservation_id: ReservationId(1),
            order_id: OrderId(1),
            user_id: UserId(7),
            market_id: market,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms: 20,
            }]
            .into(),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_key_id: 1,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let place = PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(1),
                user_id: UserId(7),
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
            },
            client_order_id: Some("client-1".into()),
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        };
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 11,
            client_seq: 1,
            market_id: Some(market),
            user_id: Some(UserId(7)),
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(place.clone())),
        };
        let cursor = TradeIdCursor::new(ClusterId(1), ShardId(1), 1);
        let candidate = place_candidate(
            PlacePlanningContext {
                policy: state.config.policies.get(&market).unwrap(),
                keys: &state.config.keys,
                command_id: command.command_id,
                session: command.session_id,
                now: LogicalTime::new(0),
            },
            &place,
            markets.get(&market).unwrap(),
            &registry,
            state.ids.try_clone_preallocated().unwrap(),
            &cursor,
        )
        .unwrap();
        assert!(candidate.events.iter().any(
            |e| matches!(e, EngineEvent::OrderAccepted { order_id } if *order_id == OrderId(1))
        ));
        // 使用已耗尽游标与真实对手挂单，验证候选成交无法留下部分状态。
        let mut sell = place.clone();
        sell.order.order_id = OrderId(2);
        sell.order.user_id = UserId(8);
        sell.order.side = Side::Sell;
        sell.client_order_id = Some("seller".into());
        sell.reservation.token_id = 2;
        sell.reservation.command_id = CommandId(2);
        sell.reservation.reservation_id = ReservationId(2);
        sell.reservation.order_id = OrderId(2);
        sell.reservation.user_id = UserId(8);
        sell.reservation.reserves = vec![AssetReserve {
            asset: AssetId(1),
            atoms: 2,
        }]
        .into();
        sell.reservation.signature = signing
            .sign(&sell.reservation.canonical_signing_payload().unwrap())
            .to_bytes();
        let exhausted =
            TradeIdCursor::with_next_index(ClusterId(1), ShardId(1), 2, u64::from(u32::MAX) + 1)
                .unwrap();
        let maker_before = candidate.book.order(OrderId(1)).cloned();
        let registry_before_overflow = candidate.registry.clone();
        let ids_before_overflow = candidate.ids.clone();
        let cursor_before = exhausted.clone();
        let result = place_candidate(
            PlacePlanningContext {
                policy: state.config.policies.get(&market).unwrap(),
                keys: &state.config.keys,
                command_id: CommandId(2),
                session: 12,
                now: LogicalTime::new(0),
            },
            &sell,
            &candidate.book,
            &candidate.registry,
            candidate.ids.try_clone_preallocated().unwrap(),
            &exhausted,
        );
        assert_eq!(result.err(), Some(RejectReason::ArithmeticOverflow));
        assert_eq!(candidate.book.order(OrderId(1)).cloned(), maker_before);
        assert!(candidate.book.order(OrderId(2)).is_none());
        assert_eq!(candidate.registry, registry_before_overflow);
        assert_eq!(candidate.ids, ids_before_overflow);
        assert_eq!(exhausted, cursor_before);
        let mut window = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let clock = LogicalClock::new(LogicalTime::new(0));
        let first = execute_place(
            PlaceExecutionContext {
                control_killed: false,
                managed: None,
                identity: FormalIdentity {
                    cluster: ClusterId(1),
                    shard: ShardId(1),
                    event_capacity: 0,
                },
                seq: &mut seq,
                clock: &clock,
                markets: &mut markets,
                registry: &mut registry,
                window: &mut window,
                state: &mut state,
                durable: &mut None,
            },
            command.clone(),
        )
        .unwrap();
        assert_eq!(first.seq, 1);
        assert_eq!(seq.last_applied(), 1);
        assert_eq!(
            first.outcome,
            FormalOutcome::Rejected(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(first.events.len(), 1);
        assert_eq!(
            first.events[0].payload,
            EngineEvent::CommandRejected {
                reason: RejectReason::ArithmeticOverflow
            }
        );
        assert!(markets[&market].order(OrderId(1)).is_none());
        assert!(markets[&market].reservation_snapshots().is_empty());
        assert_eq!(markets[&market].next_queue_priority(), priority_before);
        assert_eq!(registry, registry_before);
        assert_eq!(state.ids, ids_before);
        let second = execute_place(
            PlaceExecutionContext {
                control_killed: false,
                managed: None,
                identity: FormalIdentity {
                    cluster: ClusterId(1),
                    shard: ShardId(1),
                    event_capacity: 0,
                },
                seq: &mut seq,
                clock: &clock,
                markets: &mut markets,
                registry: &mut registry,
                window: &mut window,
                state: &mut state,
                durable: &mut None,
            },
            command,
        )
        .unwrap();
        assert!(second.deduplicated);
        assert!(second.events.is_empty());
        assert_eq!(second.seq, 1);
        assert_eq!(second.summary, first.summary);
        assert_eq!(second.event_range, first.event_range);
        assert_eq!(seq.last_applied(), 1);
    }
}

#[cfg(test)]
mod durability_atomicity_tests {
    //! 注入真实 WAL 文件创建失败，核对持久提交前候选状态不安装。
    use super::*;
    use matching_domain::{ApplyUserControlFenceShard, UserFenceAction, UserId};
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, ManifestManager, Profile,
        RegisteredAppender, ReplicationMode, SegmentWriter,
    };
    #[test]
    fn failed_wal_does_not_install_fence_or_dedup_entry() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "matching-fence-atomic-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir(&base).unwrap();
        std::fs::create_dir(base.join("wal")).unwrap();
        std::fs::create_dir(base.join("manifest")).unwrap();
        let writer = SegmentWriter::new(base.join("wal"), 4096).unwrap();
        let manifest = ManifestManager::open(base.join("manifest")).unwrap();
        let appender = RegisteredAppender::new(writer, manifest).unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut durable = Some(LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(appender, config, 7)
                .unwrap()
                .into_boxed(),
            epoch: 7,
            group: None,
        });
        std::fs::write(
            base.join("wal").join("00000000000000000001.wal"),
            b"collision",
        )
        .unwrap();
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: None,
            user_id: Some(UserId(2)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(3),
                user_id: UserId(2),
                new_epoch: 5,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: 7,
            }),
        };
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let before = epochs.clone();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_fence(
                &mut epochs,
                &mut dedup,
                &mut seq,
                &LogicalClock::new(LogicalTime::new(0)),
                &mut durable,
                FormalIdentity {
                    shard: ShardId(1),
                    cluster: ClusterId(1),
                    event_capacity: u64::from(u32::MAX) + 1,
                },
                command.clone(),
            )
        }));
        assert!(result.is_err());
        assert_eq!(epochs, before);
        assert!(dedup.lookup(&command).unwrap().is_none());
        assert_eq!(seq.last_applied(), 1);
        drop(durable);
        std::fs::remove_dir_all(base).unwrap();
    }
}

#[cfg(test)]
mod group_candidate_tests {
    //! 批内候选围栏与命令去重不能提前改变真实状态。
    use super::*;
    use matching_domain::{ApplyUserControlFenceShard, UserFenceAction, UserId};
    #[test]
    fn duplicate_and_epoch_follow_candidate_without_publishing() {
        let epochs = UserControlEpochs::new(8).unwrap();
        let dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let seq = Sequencer::new();
        let mut candidate = FenceBatchCandidate::new(
            &epochs,
            &dedup,
            &seq,
            7,
            ShardId(1),
            ClusterId(1),
            LogicalTime::new(0),
        )
        .unwrap();
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: None,
            user_id: Some(UserId(2)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(3),
                user_id: UserId(2),
                new_epoch: 5,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: 7,
            }),
        };
        assert_eq!(candidate.stage(command.clone()).unwrap().seq, 1);
        assert!(candidate.stage(command).unwrap().deduplicated);
        assert_eq!(candidate.seq.last_applied(), 1);
        assert!(epochs.check(UserId(2), 0).is_ok());
        assert!(candidate.epochs.check(UserId(2), 0).is_err());
        assert_eq!(seq.last_applied(), 0);
    }
}

#[cfg(test)]
mod group_sync_tests {
    //! 用可观测同步原语验证批次同步次数和失败原子性。
    use super::*;
    use matching_domain::{ApplyUserControlFenceShard, UserFenceAction, UserId};
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableAppender, DurableBatcher, Profile,
        ReplicationMode, WalRecord,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Probe {
        /// 已调用的同步次数，供测试线程观察。
        syncs: Arc<AtomicUsize>,
        /// 是否在同步时注入失败。
        fail: bool,
    }
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(std::io::Error::other("注入同步失败"))
            } else {
                Ok(())
            }
        }
    }
    fn command(id: u128) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId(id),
            session_id: 1,
            client_seq: id as u64,
            market_id: None,
            user_id: Some(UserId(2)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(3),
                user_id: UserId(2),
                new_epoch: id as u64,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: 7,
            }),
        }
    }
    #[test]
    fn deadline_stops_batch_even_when_more_messages_are_available() {
        let start = Instant::now();
        let deadline = start + Duration::from_micros(10);
        assert!(!group_should_flush(1, 8, start, deadline, false));
        assert!(group_should_flush(1, 8, deadline, deadline, false));
        assert!(group_should_flush(8, 8, start, deadline, false));
        assert!(group_should_flush(1, 8, start, deadline, true));
    }
    #[test]
    fn single_sync_covers_batch_and_failure_keeps_real_state() {
        for (fail, fenced) in [(false, false), (true, false), (false, true)] {
            let syncs = Arc::new(AtomicUsize::new(0));
            let config = DurabilityConfig {
                profile: Profile::Development,
                mode: DurabilityMode::GroupCommit {
                    max_batch: 2,
                    max_delay_micros: 100_000,
                },
                replication: ReplicationMode::LocalDurable,
                crash_recovery_required: false,
                ha_enabled: false,
                allow_data_loss_on_failover: false,
            };
            let mut gate = LocalDurableCommit {
                event_hasher: None,
                _wal_lease: None,
                batcher: DurableBatcher::new_with_config(
                    Probe {
                        syncs: syncs.clone(),
                        fail,
                    },
                    config,
                    7,
                )
                .unwrap()
                .into_boxed(),
                epoch: 7,
                group: Some((2, 100_000)),
            };
            let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
                capacity: 4,
                mutation_reserved_slots: 1,
                max_query_inflight: 1,
                max_control_inflight: 1,
            })
            .unwrap();
            let (first_tx, first_rx) = sync_channel(1);
            let (second_tx, second_rx) = sync_channel(1);
            assert!(
                producer
                    .try_push_classified(
                        Message::Formal(command(2), None, CommandReply::new(second_tx, None)),
                        IngressClass::Mutation
                    )
                    .is_ok()
            );
            let mut epochs = UserControlEpochs::new(4).unwrap();
            let before = epochs.clone();
            let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
            let mut seq = Sequencer::new();
            if fenced {
                producer.fence_routes();
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_fence_group(
                    (command(1), CommandReply::new(first_tx, None)),
                    &mut consumer,
                    &mut gate,
                    (&mut epochs, &mut dedup, &mut seq),
                    (ShardId(1), ClusterId(1), LogicalTime::new(0)),
                    None,
                )
            }));
            if fenced {
                assert!(result.is_ok());
                assert_eq!(seq.last_applied(), 0);
                assert!(matches!(
                    first_rx.recv().unwrap(),
                    Err(FormalError::Rejected(RejectReason::EngineBusy))
                ));
                assert!(second_rx.try_recv().is_err());
            } else if fail {
                assert_eq!(syncs.load(Ordering::SeqCst), 1);
                assert!(result.is_err());
                assert_eq!(epochs, before);
                assert_eq!(seq.last_applied(), 0);
                assert!(dedup.lookup(&command(1)).unwrap().is_none());
                assert!(first_rx.recv().is_err());
                assert!(second_rx.recv().is_err());
            } else {
                assert_eq!(syncs.load(Ordering::SeqCst), 1);
                assert!(result.is_ok());
                assert_eq!(seq.last_applied(), 2);
                assert_eq!(
                    epochs.check(UserId(2), 1),
                    Err(RejectReason::StaleUserControlEpoch)
                );
                assert_eq!(first_rx.recv().unwrap().unwrap().seq, 1);
                assert_eq!(second_rx.recv().unwrap().unwrap().seq, 2);
            }
        }
    }
    /// 在前缀同步后故意令下一次同步失败。
    struct PrefixProbe(Arc<AtomicUsize>);
    impl DurableAppender for PrefixProbe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(true)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::other("注入后续同步失败"))
            }
        }
    }
    /// 用于在追加后触发栅栏的队列入口。
    struct FenceAfterAppend(crate::queue::Producer<Message>);
    impl DurableAppender for FenceAfterAppend {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            self.0.fence_routes();
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn fencing_during_batch_stops_next_candidate_without_consuming_sequence() {
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 4,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 1,
        })
        .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 100_000,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut gate = LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(FenceAfterAppend(producer.clone()), config, 7)
                .unwrap()
                .into_boxed(),
            epoch: 7,
            group: Some((2, 100_000)),
        };
        let (tx1, rx1) = sync_channel(1);
        let (tx2, rx2) = sync_channel(1);
        assert!(
            producer
                .try_push_classified(
                    Message::Formal(command(2), None, CommandReply::new(tx2, None)),
                    IngressClass::Mutation
                )
                .is_ok()
        );
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        execute_fence_group(
            (command(1), CommandReply::new(tx1, None)),
            &mut consumer,
            &mut gate,
            (&mut epochs, &mut dedup, &mut seq),
            (ShardId(1), ClusterId(1), LogicalTime::new(0)),
            None,
        );
        assert_eq!(rx1.recv().unwrap().unwrap().seq, 1);
        assert!(matches!(
            rx2.recv().unwrap(),
            Err(FormalError::Rejected(RejectReason::EngineBusy))
        ));
        assert_eq!(seq.last_applied(), 1);
        assert_eq!(gate.batcher.committed(), 1);
        assert!(dedup.lookup(&command(2)).unwrap().is_none());
    }

    #[test]
    fn fencing_empty_batch_queue_does_not_wait_for_batch_deadline() {
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 4,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 1,
        })
        .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 2_000_000,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut gate = LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(FenceAfterAppend(producer), config, 7)
                .unwrap()
                .into_boxed(),
            epoch: 7,
            group: Some((2, 2_000_000)),
        };
        let (tx, rx) = sync_channel(1);
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let start = Instant::now();
        execute_fence_group(
            (command(1), CommandReply::new(tx, None)),
            &mut consumer,
            &mut gate,
            (&mut epochs, &mut dedup, &mut seq),
            (ShardId(1), ClusterId(1), LogicalTime::new(0)),
            None,
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "失权后仍等待批次截止时间"
        );
        assert_eq!(rx.recv().unwrap().unwrap().seq, 1);
    }

    #[test]
    fn fencing_empty_formal_batch_queue_does_not_wait_for_batch_deadline() {
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 4,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 1,
        })
        .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 2_000_000,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut gate = LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(FenceAfterAppend(producer), config, 7)
                .unwrap()
                .into_boxed(),
            epoch: 7,
            group: Some((2, 2_000_000)),
        };
        let (tx, rx) = sync_channel(1);
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let mut markets = BTreeMap::new();
        let mut managed_markets = BTreeMap::new();
        let mut registry = Some(ReservationRegistry::new([]).unwrap());
        let mut place = None;
        let routes = RouteTable::new([]).unwrap();
        let start = Instant::now();
        execute_formal_group(
            (command(1), None, CommandReply::new(tx, None)),
            &mut consumer,
            &mut gate,
            FormalGroupContext {
                epochs: &mut epochs,
                dedup: &mut dedup,
                seq: &mut seq,
                markets: &mut markets,
                managed_markets: &mut managed_markets,
                registry: &mut registry,
                place: &mut place,
                routes: &routes,
                shard: ShardId(1),
                cluster: ClusterId(1),
                now: LogicalTime::new(0),
                metrics: None,
            },
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "失权后仍等待批次截止时间"
        );
        assert_eq!(rx.recv().unwrap().unwrap().seq, 1);
    }

    #[test]
    fn fenced_formal_batch_rejects_before_sequence_and_wal() {
        let syncs = Arc::new(AtomicUsize::new(0));
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 100_000,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut gate = LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(
                Probe {
                    syncs: syncs.clone(),
                    fail: false,
                },
                config,
                7,
            )
            .unwrap()
            .into_boxed(),
            epoch: 7,
            group: Some((2, 100_000)),
        };
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 4,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 1,
        })
        .unwrap();
        let (tx, rx) = sync_channel(1);
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let before = epochs.clone();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let mut markets = BTreeMap::new();
        let mut managed_markets = BTreeMap::new();
        let mut registry = Some(ReservationRegistry::new([]).unwrap());
        let registry_before = registry.clone();
        let mut place = None;
        let routes = RouteTable::new([]).unwrap();
        producer.fence_routes();
        let deferred = execute_formal_group(
            (command(1), None, CommandReply::new(tx, None)),
            &mut consumer,
            &mut gate,
            FormalGroupContext {
                epochs: &mut epochs,
                dedup: &mut dedup,
                seq: &mut seq,
                markets: &mut markets,
                managed_markets: &mut managed_markets,
                registry: &mut registry,
                place: &mut place,
                routes: &routes,
                shard: ShardId(1),
                cluster: ClusterId(1),
                now: LogicalTime::new(0),
                metrics: None,
            },
        );
        assert!(deferred.is_none());
        assert!(matches!(
            rx.recv().unwrap(),
            Err(FormalError::Rejected(RejectReason::EngineBusy))
        ));
        assert_eq!(seq.last_applied(), 0);
        assert_eq!(gate.batcher.committed(), 0);
        assert_eq!(syncs.load(Ordering::SeqCst), 0);
        assert_eq!(epochs, before);
        assert_eq!(registry, registry_before);
        assert!(dedup.lookup(&command(1)).unwrap().is_none());
    }

    #[test]
    fn synced_prefix_then_failure_installs_no_candidate_state() {
        let syncs = Arc::new(AtomicUsize::new(0));
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 100_000,
            },
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: false,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut gate = LocalDurableCommit {
            event_hasher: None,
            _wal_lease: None,
            batcher: DurableBatcher::new_with_config(PrefixProbe(syncs.clone()), config, 7)
                .unwrap()
                .into_boxed(),
            epoch: 7,
            group: Some((2, 100_000)),
        };
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 4,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 1,
        })
        .unwrap();
        let (first_tx, first_rx) = sync_channel(1);
        let (second_tx, second_rx) = sync_channel(1);
        assert!(
            producer
                .try_push_classified(
                    Message::Formal(command(2), None, CommandReply::new(second_tx, None)),
                    IngressClass::Mutation
                )
                .is_ok()
        );
        let mut epochs = UserControlEpochs::new(4).unwrap();
        let before = epochs.clone();
        let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let mut seq = Sequencer::new();
        let mut markets = BTreeMap::new();
        let mut managed_markets = BTreeMap::new();
        let mut registry = Some(ReservationRegistry::new([]).unwrap());
        let registry_before = registry.clone();
        let mut place = None;
        let routes = RouteTable::new([]).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_formal_group(
                (command(1), None, CommandReply::new(first_tx, None)),
                &mut consumer,
                &mut gate,
                FormalGroupContext {
                    epochs: &mut epochs,
                    dedup: &mut dedup,
                    seq: &mut seq,
                    markets: &mut markets,
                    managed_markets: &mut managed_markets,
                    registry: &mut registry,
                    place: &mut place,
                    routes: &routes,
                    shard: ShardId(1),
                    cluster: ClusterId(1),
                    now: LogicalTime::new(0),
                    metrics: None,
                },
            )
        }));
        assert!(result.is_err());
        assert_eq!(syncs.load(Ordering::SeqCst), 2);
        assert_eq!(gate.batcher.committed(), 1);
        assert_eq!(epochs, before);
        assert_eq!(seq.last_applied(), 0);
        assert_eq!(registry, registry_before);
        assert!(markets.is_empty());
        assert!(dedup.lookup(&command(1)).unwrap().is_none());
        assert!(first_rx.recv().is_err());
        assert!(second_rx.recv().is_err());
    }
}

/// 按可信恢复配置和逐市场提供的优先级策略建立空簿，作为从创世 WAL 回放的起点。
pub(crate) fn empty_recovery_candidate(
    trusted: &SnapshotRestoreConfig,
    policies: &[(MarketId, matching_orderbook::QueuePriorityPolicy)],
    committed_through: u64,
) -> std::io::Result<SnapshotRecoveryCandidate> {
    if !trusted.expect_registry
        || policies.len() > trusted.max_markets
        || !trusted.idempotency.valid()
        || (trusted.place.is_none() && !policies.is_empty())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "无效初始基线配置",
        ));
    }
    let mut markets = BTreeMap::new();
    for &(market, policy) in policies {
        let mut book = ProductionOrderBook::new(policy);
        book.bind_market(market)
            .map_err(|_| std::io::Error::other("市场绑定无效"))?;
        if trusted
            .routes
            .get(market)
            .is_none_or(|route| route.shard_id != trusted.shard)
            || markets.insert(market, book).is_some()
        {
            return Err(std::io::Error::other("初始市场路由无效或重复"));
        }
    }
    if trusted
        .routes
        .routes()
        .any(|route| route.shard_id == trusted.shard && !markets.contains_key(&route.market_id))
    {
        return Err(std::io::Error::other("初始市场策略缺失"));
    }
    let place = if let Some(config) = &trusted.place {
        if config
            .policies
            .keys()
            .any(|market| !markets.contains_key(market))
        {
            return Err(std::io::Error::other("下单市场策略缺失"));
        }
        let ids = ClientOrderIds::with_max_id_bytes(
            config.limits.active_orders,
            config.limits.terminal_history,
            config.limits.client_id_bytes,
        )
        .map_err(|()| std::io::Error::other("客户端身份容量无效"))?;
        Some(PlaceRuntimeState {
            market_data: Some(
                crate::MarketDataState::new(markets.keys().copied())
                    .map_err(|_| std::io::Error::other("行情市场无效"))?,
            ),
            config: config.clone(),
            ids,
            fee_book: None,
            shard_kill_active: false,
        })
    } else {
        None
    };
    let user_epochs = UserControlEpochs::new(trusted.user_capacity)
        .map_err(|()| std::io::Error::other("用户容量无效"))?;
    let dedup = DedupWindow::new(trusted.idempotency)
        .map_err(|()| std::io::Error::other("去重容量无效"))?;
    let registry =
        ReservationRegistry::new([]).map_err(|_| std::io::Error::other("预留基线无效"))?;
    let image = SnapshotImage {
        replay_route_floor: None,
        genesis_baseline: true,
        origin: None,
        start_allowed: false,
        request_id: 0,
        snapshot_seq: 0,
        cluster: trusted.cluster,
        shard: trusted.shard,
        now: LogicalTime::new(0),
        primary_epoch: trusted.primary_epoch,
        routes: Arc::new(trusted.routes.clone()),
        markets,
        managed_markets: BTreeMap::new(),
        registry: Some(registry),
        user_epochs,
        dedup: Some(dedup),
        place,
    };
    image
        .canonical_state(trusted.max_bytes)
        .map_err(|_| std::io::Error::other("空基线超过容量"))?;
    Ok(SnapshotRecoveryCandidate {
        image,
        committed_through,
        used_previous: false,
    })
}

/// 仅在独立候选镜像中重放已验证 WAL，失败时丢弃整个候选。
pub(crate) fn replay_snapshot_records(
    candidate: SnapshotRecoveryCandidate,
    records: Vec<crate::RecoveredCommand>,
) -> std::io::Result<crate::SnapshotReplayResult> {
    replay_snapshot_records_inner(candidate, records, None, None)
}

/// 以外围核验过的写者纪元为上界重放旧纪元的连续前缀。
pub(crate) fn replay_replicated_snapshot_records(
    candidate: SnapshotRecoveryCandidate,
    records: Vec<crate::RecoveredCommand>,
    epoch: u64,
    checkpoint: Option<crate::ReplayCheckpoint>,
) -> std::io::Result<crate::SnapshotReplayResult> {
    if epoch == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "写者纪元为零",
        ));
    }
    replay_snapshot_records_inner(candidate, records, Some(epoch), checkpoint)
}

fn replay_snapshot_records_inner(
    candidate: SnapshotRecoveryCandidate,
    records: Vec<crate::RecoveredCommand>,
    replicated_epoch: Option<u64>,
    checkpoint: Option<crate::ReplayCheckpoint>,
) -> std::io::Result<crate::SnapshotReplayResult> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut image = candidate.image;
        // 冻结期间一旦安装切换后镜像，双方不得再存在任何已提交后缀。
        if candidate.committed_through != image.snapshot_seq
            && image.routes.routes().any(|route| {
                matches!(route.state, RouteState::Migrating { source, target }
                    if (source == image.shard || target == image.shard)
                        && image.replay_route_floor.as_ref()
                            .and_then(|past| past.get(route.market_id))
                            .is_some_and(|past| past.shard_id == target
                                && past.state == RouteState::Active
                                && past.route_epoch == route.route_epoch))
            })
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "已冻结并安装的分片包含额外业务记录",
            ));
        }
        let mut seq = Sequencer::from_last_applied(image.snapshot_seq)
            .map_err(|_| std::io::Error::other("重放序号耗尽"))?;
        let clock = LogicalClock::new(image.now);
        let mut durable: Option<LocalDurableCommit> = None;
        let mut executions = Vec::new();
        executions
            .try_reserve(records.len())
            .map_err(|_| std::io::Error::other("重放结果容量不足"))?;
        let mut event_hasher = crate::EventHasher::new();
        let mut checkpoint_seen = checkpoint.is_none();
        let lease_epoch = image
            .place
            .as_ref()
            .and_then(|place| place.config.primary_lease.as_ref())
            .and_then(|lease| {
                lease
                    .verify(image.cluster, image.shard, lease.epoch())
                    .ok()
                    .map(|()| lease.epoch())
            });
        if replicated_epoch.is_some() && lease_epoch.is_some() && replicated_epoch != lease_epoch {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "恢复纪元与本地租约不一致",
            ));
        }
        let lease_epoch = replicated_epoch.or(lease_epoch);
        if !image.genesis_baseline && lease_epoch.is_some_and(|epoch| image.primary_epoch > epoch) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "快照纪元超过恢复写权",
            ));
        }
        let mut previous_epoch = if image.genesis_baseline && lease_epoch.is_some() {
            None
        } else {
            Some(image.primary_epoch)
        };
        let mut previous_routes: BTreeMap<MarketId, u64> = image
            .replay_route_floor
            .as_ref()
            .map(|routes| {
                routes
                    .routes()
                    .filter(|route| route.shard_id == image.shard)
                    .map(|route| (route.market_id, route.route_epoch))
                    .collect()
            })
            .unwrap_or_default();
        for record in records {
            let epoch_valid = if let Some(limit) = lease_epoch {
                record.epoch > 0
                    && record.epoch <= limit
                    && previous_epoch.is_none_or(|previous| record.epoch >= previous)
            } else {
                record.epoch == image.primary_epoch
            };
            if seq.last_applied().checked_add(1) != Some(record.shard_seq)
                || record.shard_seq == u64::MAX
                || !epoch_valid
                || record.engine_time != clock.now().get()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "重放序号、纪元或逻辑时间不一致",
                ));
            }
            previous_epoch = Some(record.epoch);
            image.primary_epoch = record.epoch;
            let bootstrap = if image
                .place
                .as_ref()
                .is_some_and(|place| place.market_data.is_none())
            {
                Some(
                    crate::MarketDataState::bootstrap_legacy(&image.markets, seq.last_applied())
                        .map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "旧快照行情初始化失败",
                            )
                        })?,
                )
            } else {
                None
            };
            let identity = FormalIdentity {
                cluster: image.cluster,
                shard: image.shard,
                event_capacity: u64::from(u32::MAX) + 1,
            };
            let command = record.command;
            // 路由校验发生在写入 WAL 之前；重放使用记录中的历史版本，仍约束当前可信所有权与版本上限。
            let replay_routes = if let Some(market) = command.market_id {
                let epoch = command.route_epoch.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "已提交命令重放时被前置拒绝：缺少路由版本",
                    )
                })?;
                let current = image.routes.get(market).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "已提交命令重放时被前置拒绝：市场路由不存在",
                    )
                })?;
                let epoch_allowed = match current.state {
                    RouteState::Active => epoch <= current.route_epoch,
                    RouteState::Migrating { source, .. } => {
                        source == image.shard && epoch < current.route_epoch
                    }
                };
                if current.shard_id != image.shard
                    || !epoch_allowed
                    || previous_routes
                        .get(&market)
                        .is_some_and(|previous| epoch < *previous)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "已提交命令重放时被前置拒绝：路由所有权或版本无效",
                    ));
                }
                previous_routes.insert(market, epoch);
                RouteTable::new(image.routes.routes().copied().map(|mut route| {
                    if route.market_id == market {
                        route.route_epoch = epoch;
                        route.state = RouteState::Active;
                    }
                    route
                }))
                .map_err(|_| std::io::Error::other("重放路由重复"))?
            } else {
                (*image.routes).clone()
            };
            let mut result = match &command.payload {
                payload if cross_fence::coordinated(payload) => {
                    let window = image.dedup.as_mut().ok_or_else(|| std::io::Error::other("缺少控制命令去重窗口"))?;
                    cross_fence::execute(ShardKillContext { identity, epoch: image.primary_epoch, seq: &mut seq, clock: &clock, markets: &mut image.markets, registry: &mut image.registry, place: &mut image.place, dedup: window, require_lease: false }, &mut image.user_epochs, command)
                }
                Command::ApplyGlobalControlFenceShard(_) => return Err(std::io::Error::other("无效的全局控制载荷")),
                Command::ShardKillSwitch(_) => {
                    let window = image.dedup.as_mut().ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_shard_kill(ShardKillContext { identity, epoch: image.primary_epoch, seq: &mut seq, clock: &clock, markets: &mut image.markets, registry: &mut image.registry, place: &mut image.place, dedup: window, require_lease: false }, command)
                }
                Command::UpdateFeeSchedule(_) => {
                    let window = image.dedup.as_mut().ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_fee_schedule_update(identity, &replay_routes, &mut seq, &clock, window,
                        FeeUpdateState { place: &mut image.place, managed_markets: &image.managed_markets,
                            markets: &image.markets, registry: &image.registry }, command)
                }
                Command::CreateMarket(_)
                | Command::TriggerCircuitBreaker(_)
                | Command::MarketKillSwitch(_)
                | Command::UpdateReferencePrice(_)
                | Command::UpdateMarket(_)
                | Command::StartSession(_)
                | Command::OpenMarket(_)
                | Command::SuspendMarket(_)
                | Command::ResumeMarket(_)
                | Command::EndSession(_)
                | Command::CloseMarket(_) => {
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_market_management(
                        MarketManagementContext {
                            controls: Some(&image.user_epochs),
                identity,
                            routes: &replay_routes,
                            seq: &mut seq,
                            clock: &clock,
                            markets: &mut image.markets,
                            managed_markets: &mut image.managed_markets,
                            dedup: window,
                            place: &mut image.place,
                            registry: &mut image.registry,
                        },
                        command,
                    )
                }
                Command::ApplyUserControlFenceShard(fence) => {
                    if fence.expected_primary_epoch != image.primary_epoch {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "重放栅栏纪元不一致",
                        ));
                    }
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_fence(
                        &mut image.user_epochs,
                        window,
                        &mut seq,
                        &clock,
                        &mut durable,
                        identity,
                        command,
                    )
                }
                Command::PlaceLimit(place) => {
                    let market = command
                        .market_id
                        .ok_or_else(|| std::io::Error::other("重放下单缺少市场"))?;
                    let epoch = command
                        .route_epoch
                        .ok_or_else(|| std::io::Error::other("重放下单缺少路由版本"))?;
                    replay_routes
                        .validate(market, image.shard, epoch)
                        .map_err(|_| std::io::Error::other("重放下单路由无效"))?;
                    image
                        .user_epochs
                        .check(place.order.user_id, place.user_control_epoch)
                        .map_err(|_| std::io::Error::other("重放下单用户纪元无效"))?;
                    let registry = image
                        .registry
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放预留登记表"))?;
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    let state = image
                        .place
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放下单配置"))?;
                    if !image.markets.contains_key(&market) {
                        return Err(std::io::Error::other("重放市场不存在"));
                    }
                    let execution = execute_place(
                        PlaceExecutionContext {
                            control_killed: image.user_epochs.killed(place.order.user_id),
                managed: image.managed_markets.get(&market),
                            identity,
                            seq: &mut seq,
                            clock: &clock,
                            markets: &mut image.markets,
                            registry,
                            window,
                            state,
                            durable: &mut durable,
                        },
                        command,
                    );
                    if matches!(&execution, Ok(value) if matches!(value.outcome, FormalOutcome::Placed(_)))
                        && let Some(managed) = image.managed_markets.get_mut(&market)
                    {
                        managed.record_referenced_history();
                    }
                    execution
                }
                Command::Cancel(_) | Command::MassCancelShard(_) => {
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_formal_guarded(
                        &replay_routes,
                        identity,
                        &mut seq,
                        &clock,
                        (&mut image.markets, &image.managed_markets),
                        &mut image.registry,
                        (command, window, &mut image.place, &mut durable),
                    )
                }
            }
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "已提交命令重放时被前置拒绝",
                )
            })?;
            if result.deduplicated || result.seq != record.shard_seq {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "重放结果序号或去重状态不一致",
                ));
            }
            let mut bootstrap_events = BTreeMap::new();
            if let Some((data, events)) = bootstrap {
                image.place.as_mut().expect("已检查下单状态").market_data = Some(data);
                bootstrap_events = events;
            }
            if let Some(data) = image
                .place
                .as_mut()
                .and_then(|place| place.market_data.as_mut())
            {
                let updates = data.apply(&result).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "行情候选重放失败")
                })?;
                for (market, events) in updates {
                    bootstrap_events
                        .entry(market)
                        .or_insert_with(Vec::new)
                        .extend(events);
                }
                result.market_data_source = bootstrap_events;
            }
            for event in &result.events {
                event_hasher
                    .update(event)
                    .map_err(|_| std::io::Error::other("重放事件摘要编码失败"))?;
            }
            image.snapshot_seq = result.seq;
            if let Some(checkpoint) = checkpoint
                && result.seq == checkpoint.shard_seq
            {
                let digest = image
                    .state_digest(checkpoint.max_state_bytes)
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "检查点状态摘要编码失败",
                        )
                    })?;
                if digest != checkpoint.state_digest
                    || event_hasher.clone().finish() != checkpoint.event_hash
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "重放检查点摘要不一致",
                    ));
                }
                checkpoint_seen = true;
            }
            executions.push(result);
        }
        if !checkpoint_seen {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "重放未到达检查点",
            ));
        }
        if seq.last_applied() != candidate.committed_through {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "重放未覆盖持久前缀",
            ));
        }
        if replicated_epoch.is_none()
            && let Some(epoch) = lease_epoch
        {
            image.primary_epoch = epoch;
        }
        image.start_allowed = true;
        image.genesis_baseline = false;
        image.replay_route_floor = None;
        Ok(crate::SnapshotReplayResult {
            image,
            executions,
            event_hash: event_hasher.clone().finish(),
            event_hasher,
        })
    }));
    result.map_err(|_| std::io::Error::other("重放触发内部不变量失败"))?
}
