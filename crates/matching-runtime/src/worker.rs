//! 分片工作线程及进程内消息。
mod snapshot;
mod snapshot_writer;

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
    Arc,
    atomic::{AtomicBool, Ordering},
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
    /// 首次执行事件的闭区间；零事件时为空。
    pub event_range: Option<(u32, u32)>,
    /// 此次响应是否来自已执行命令的窗口缓存。
    pub deduplicated: bool,
}

/// 正式命令的有类型结果，不作为业务事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormalOutcome {
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
    /// 分片本地批量撤单的汇总结果。
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
            Self::Closed | Self::Timeout | Self::Failed | Self::InvalidConfig => None,
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
    /// 分片用户控制下限副本。
    user_epochs: UserControlEpochs,
    /// 分片命令去重窗口副本。
    dedup: Option<DedupWindow>,
    /// 可信下单配置和客户端身份索引副本。
    place: Option<PlaceRuntimeState>,
}
impl SnapshotImage {
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

/// 只在本进程队列内传递的变更、查询与控制消息。
enum Message {
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
        SyncSender<Result<Execution, RejectReason>>,
    ),
    /// 通过完整预留归属路径执行正式命令。
    Formal(
        /// 含路由及业务身份的命令。
        CommandEnvelope,
        /// 返回执行结果或前置拒绝的一次性回复端。
        SyncSender<Result<FormalExecution, FormalError>>,
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
        SyncSender<Result<SnapshotImage, SnapshotError>>,
    ),
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
}
impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            mutation_reserved_slots: 1,
            max_query_inflight: 512,
            max_control_inflight: 128,
            wait_strategy: WaitStrategy::Hybrid,
        }
    }
}

/// 分片入口与线程所有权；订单簿仅存在于工作线程中。
pub struct Shard {
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
    /// 所有者丢弃时请求排空退出。
    drop_requested: Arc<AtomicBool>,
    /// 当前工作线程所属分片。
    shard_id: ShardId,
    /// 工作线程与入口共享的路由快照。
    routes: Arc<RouteTable>,
}

impl Shard {
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
        let image = records
            .replay()
            .map_err(|_| ShardError::InvalidConfig)?
            .image;
        if !image.managed_markets.is_empty() && image.place.is_none() {
            return Err(ShardError::InvalidConfig);
        }
        let ownership = crate::ownership::MarketOwnership::acquire(
            image.cluster,
            image.shard,
            image.markets.keys().copied(),
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
                Some(LocalDurableCommit {
                    batcher,
                    epoch: trusted.primary_epoch,
                    group: Some((max_batch, max_delay_micros)),
                }),
            ),
            (Some(image), Some(ownership)),
        )
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
            Message::Mutation(command_id, market, epoch, mutation, tx),
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
            snapshot_origin: Arc::clone(&self.snapshot_origin),
            snapshot_writer_active: Arc::clone(&self.snapshot_writer_active),
            producer: self.producer.clone(),
            thread: self.thread.clone(),
            failed: Arc::clone(&self.failed),
            shard_id: self.shard_id,
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

/// 由分片独占的持久写入原语。
type OwnedAppender = matching_persistence::BoxedDurableAppender;
/// 支持本地逐条或批量同步提交的 WAL 写入器。
struct LocalDurableCommit {
    /// 单写者批处理器。
    batcher: matching_persistence::DurableBatcher<OwnedAppender>,
    /// 启动时固定的主节点纪元。
    epoch: u64,
    /// 可选批量大小和最长等待微秒数；适用于批量持久提交模式。
    group: Option<(usize, u64)>,
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
    /// 并保证 DAY 订单的交易时段仍有效；当前没有完整市场会话状态。
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
                .is_none_or(|route| route.shard_id != shard)
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
        let route_snapshot = Arc::new(routes);
        let worker_routes = Arc::clone(&route_snapshot);
        let snapshot_origin = Arc::new(());
        let worker_snapshot_origin = Arc::clone(&snapshot_origin);
        let snapshot_writer_active = Arc::new(AtomicBool::new(false));
        let close = producer.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let drop_requested = Arc::new(AtomicBool::new(false));
        let worker_drop = Arc::clone(&drop_requested);
        let join = thread::spawn(move || {
            // 所有权与工作线程同寿命，排空后才释放。
            let _ownership = ownership;
            let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut seq = initial_sequence;
                let clock = LogicalClock::new(now);
                let mut waiter = Waiter::new(config.wait_strategy);
                let mut deferred = None;
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
                        Message::Formal(command, reply)
                            if durable.as_ref().is_some_and(|gate| gate.group.is_some())
                                || (durable.is_some()
                                    && place_state.is_some()
                                    && (place_state
                                        .as_ref()
                                        .is_some_and(|state| state.config.managed_only)
                                        || !managed_markets.is_empty()
                                        || matches!(
                                            &command.payload,
                                            Command::CreateMarket(_)
                                                | Command::ShardKillSwitch(_)
                                                | Command::TriggerCircuitBreaker(_)
                                                | Command::MarketKillSwitch(_)
                                                | Command::UpdateReferencePrice(_)
                                                | Command::UpdateFeeSchedule(_)
                                                | Command::UpdateMarket(_)
                                                | Command::StartSession(_)
                                                | Command::OpenMarket(_)
                                                | Command::SuspendMarket(_)
                                                | Command::ResumeMarket(_)
                                                | Command::EndSession(_)
                                                | Command::CloseMarket(_)
                                        ))) =>
                        {
                            let gate = durable.as_mut().expect("已检查批处理模式");
                            if place_state.is_some() {
                                deferred = execute_formal_group(
                                    (command, reply),
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
                                );
                            }
                        }
                        Message::Mutation(command_id, market, epoch, mutation, reply) => {
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
                            let _ = reply.send(result);
                        }
                        Message::Formal(command, reply) => {
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
                                        (command, &mut durable, false),
                                    )
                                    .map_err(FormalError::Rejected),
                                }
                            };
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
                        Message::Barrier(id, reply) => {
                            let _ = reply.send((id, seq.last_applied()));
                        }
                        Message::Snapshot(request_id, reply) => {
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
                            let _ = reply.send(image);
                        }
                        Message::SnapshotCommitted(
                            id,
                            snapshot_seq,
                            image_cluster,
                            image_shard,
                            reply,
                        ) => {
                            let result = if image_cluster != cluster
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
            if run.is_err() {
                worker_failed.store(true, Ordering::Release);
            }
            close.close_ingress();
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
            snapshot_origin,
            snapshot_writer_active,
            producer,
            thread,
            join: Some(join),
            shutdown_reply: None,
            barrier_reached: false,
            failed,
            drop_requested,
            shard_id: shard,
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

/// 仅允许单写者访问的限价配置及客户端身份状态。
struct PlaceRuntimeState {
    /// 启动时固定的可信市场策略和容量。
    config: PlaceSupportConfig,
    /// 与活动订单及终结历史同步的客户端身份。
    ids: ClientOrderIds,
    /// 单写者持有的持久费率版本候选。
    fee_book: Option<matching_domain::FeeScheduleBook>,
    /// 分片范围熔断开关的持久状态。
    shard_kill_active: bool,
}

/// 正式限价命令所需的单写者状态。
struct PlaceExecutionContext<'a> {
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
    /// 候选用户控制下限。
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
    /// 在候选状态执行 fence 前置校验、去重及有序业务计划。
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
fn group_should_flush(count: usize, limit: usize, now: Instant, deadline: Instant) -> bool {
    count >= limit || now >= deadline
}

/// 顺序规划并提交一批围栏；非正式消息留给工作线程在提交后处理。
fn execute_fence_group(
    first: (
        CommandEnvelope,
        SyncSender<Result<FormalExecution, FormalError>>,
    ),
    consumer: &mut crate::queue::Consumer<Message>,
    gate: &mut LocalDurableCommit,
    (epochs, dedup, seq): (&mut UserControlEpochs, &mut DedupWindow, &mut Sequencer),
    (shard, cluster, now): (ShardId, ClusterId, LogicalTime),
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
                        gate.batcher
                            .append_at(&record, Instant::now())
                            .expect("WAL 追加失败，分片停止");
                    }
                    planned
                }
            };
            replies.push((reply, result));
        }
        if group_should_flush(replies.len(), limit, Instant::now(), deadline) {
            break;
        }
        match consumer.pop() {
            Some(Message::Formal(command, reply)) => {
                pending = Some((command, reply));
            }
            Some(other) => {
                deferred = Some(other);
                break;
            }
            None => {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::park_timeout(deadline.duration_since(now));
            }
        }
    }
    gate.batcher.flush().expect("WAL 批量同步失败，分片停止");
    assert_eq!(
        gate.batcher.committed(),
        candidate.seq.last_applied(),
        "批量提交水位不连续"
    );
    *epochs = candidate.epochs;
    *dedup = candidate.dedup;
    *seq = candidate.seq;
    for (reply, result) in replies {
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
    /// 从真实状态构造独立候选；容量预留失败则停止当前批次。
    fn new(context: &FormalGroupContext<'_>, epoch: u64) -> Result<Self, ()> {
        let place = context
            .place
            .as_ref()
            .map(|state| {
                Ok(PlaceRuntimeState {
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
        if matches!(command.payload, Command::ShardKillSwitch(_)) {
            return execute_shard_kill(
                ShardKillContext {
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
                },
                command,
            );
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
        SyncSender<Result<FormalExecution, FormalError>>,
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
        if let Some((command, reply)) = pending.take() {
            let payload = matching_domain::encode_wal_command(&command);
            let result = match payload {
                Err(_) => candidate.reject_uncodable(&command, context.routes),
                Ok(payload) => {
                    let command_id = command.command_id;
                    let planned = candidate.stage(command, context.routes);
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
                        gate.batcher
                            .append_at(&record, Instant::now())
                            .expect("WAL 追加失败，分片停止");
                    }
                    planned
                }
            };
            replies.push((reply, result));
        }
        if group_should_flush(replies.len(), limit, Instant::now(), deadline) {
            break;
        }
        match consumer.pop() {
            Some(Message::Formal(command, reply)) => pending = Some((command, reply)),
            Some(other) => {
                deferred = Some(other);
                break;
            }
            None => {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::park_timeout(deadline.duration_since(now));
            }
        }
    }
    gate.batcher.flush().expect("WAL 批量同步失败，分片停止");
    assert_eq!(
        gate.batcher.committed(),
        candidate.fence.seq.last_applied(),
        "批量提交水位不连续"
    );
    *context.epochs = candidate.fence.epochs;
    *context.dedup = candidate.fence.dedup;
    *context.seq = candidate.fence.seq;
    *context.markets = candidate.markets;
    *context.managed_markets = candidate.managed_markets;
    *context.registry = candidate.registry;
    *context.place = candidate.place;
    for (reply, result) in replies {
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
        gate.commit(
            identity.cluster,
            identity.shard,
            clock.now().get(),
            command.command_id,
            n,
            bytes,
        )
        .expect("WAL 持久提交失败，分片停止");
    }
    let cursor = TradeIdCursor::new(identity.cluster, identity.shard, n);
    let book = markets.get_mut(&market).expect("已验证市场存在");
    let planned = (if state.shard_kill_active {
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
                let (plan, _) = working.plan_trigger_stops_with_r2_and_cursor_limited(
                    reference,
                    matching_orderbook::TriggerRegistryContext { now: clock.now(), current_session_id: session, math: &math, trade_ids: &[] },
                    matching_risk::TradeSettlementContext { market_id: market, math, base: matching_risk::SettlementAsset { asset: base, scale: math.qty_scale() }, quote: matching_risk::SettlementAsset { asset: quote, scale: math.quote_scale() }, fees },
                    &candidate_registry, (&cursor, (limits.max_internal_actions_per_command, limits.max_trades_per_command)),
                    |stop, current| {
                        if state.kill_switch_active() || state_place.shard_kill_active { return Err(RejectReason::KillSwitchActive); }
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
        gate.commit(
            identity.cluster,
            identity.shard,
            clock.now().get(),
            command.command_id,
            n,
            bytes,
        )
        .expect("WAL 持久提交失败，分片停止");
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
        deduplicated: false,
    };
    *epochs = candidate;
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 在候选订单簿及登记表上执行正式命令，完成事件封装后安装后态。
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
    let Command::MassCancelShard(ref input) = command.payload else {
        return Err(FormalError::Rejected(RejectReason::InvalidOrderCombination));
    };
    if input.scope != (ShardCancelScope::Market { market_id: market }) || command.user_id.is_some()
    {
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
        deduplicated: false,
    };
    dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

/// 在候选订单簿及登记表上执行正式命令，完成事件封装后安装后态。
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
    let mut candidate_ids = place_state
        .as_ref()
        .map(|state| state.ids.try_clone_preallocated())
        .transpose()
        .map_err(|()| FormalError::Rejected(RejectReason::EngineBusy))?;
    let prepared = dedup.prepare(&command);
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

/// 在候选状态中完成业务执行及事件封装。
fn execute_formal(
    routes: &RouteTable,
    identity: FormalIdentity,
    seq: &mut Sequencer,
    clock: &LogicalClock,
    markets: &mut BTreeMap<MarketId, ProductionOrderBook>,
    registry: &mut Option<ReservationRegistry>,
    (command, durable, settle_r2): (CommandEnvelope, &mut Option<LocalDurableCommit>, bool),
) -> Result<FormalExecution, RejectReason> {
    let market = command
        .market_id
        .ok_or(RejectReason::InvalidOrderCombination)?;
    let epoch = command.route_epoch.ok_or(RejectReason::StaleRouteEpoch)?;
    routes.validate(market, identity.shard, epoch)?;
    let Command::MassCancelShard(input) = command.payload else {
        return Err(RejectReason::InvalidOrderCombination);
    };
    if input.scope != (ShardCancelScope::Market { market_id: market }) || command.user_id.is_some()
    {
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
        gate.commit(
            identity.cluster,
            identity.shard,
            clock.now().get(),
            command.command_id,
            n,
            bytes,
        )
        .expect("WAL 持久提交失败，分片停止");
    }
    let mut candidate = book.clone();
    let mut candidate_registry = registry.clone();
    let (mut outcome, events) = match candidate.plan_mass_cancel(input.scope, &candidate_registry) {
        Ok(plan) => {
            let (result, mut events) = candidate
                .apply_mass_cancel(&mut candidate_registry, plan)
                .expect("新生成的批量撤单计划发生状态漂移");
            if settle_r2 {
                events = crate::place_candidate::release_terminal_reservations(
                    book,
                    &candidate,
                    &mut candidate_registry,
                    events,
                )
                .expect("批量撤单真实预留释放失败，分片停止");
            }
            (FormalOutcome::MassCancel(result), events)
        }
        Err(RejectReason::InvalidReservation | RejectReason::InvalidQty) => {
            panic!("正式批量撤单发现预留或订单状态不变量损坏")
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
        FormalOutcome::MassCancel(_)
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
        deduplicated: false,
    })
}

/// 判断根命令的事件数量是否能够由给定索引容量完整表示。
fn event_count_fits(count: u64, capacity: u64) -> bool {
    count <= capacity
}

/// 可克隆的有界生产者，只能投递消息，不可访问撮合状态。
#[derive(Clone)]
pub struct ShardHandle {
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
    /// 句柄对应的分片身份。
    shard_id: ShardId,
    /// 句柄对应的不可变路由快照。
    routes: Arc<RouteTable>,
}

impl ShardHandle {
    /// 读取句柄归属分片。
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// 读取本句柄绑定的不可变路由快照。
    pub(crate) fn routes(&self) -> &RouteTable {
        &self.routes
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
        self.push(Message::Snapshot(id, tx), IngressClass::Control)?;
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
                Message::Mutation(command_id, market, epoch, mutation, tx),
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
    /// 在分片顺序队列提交正式业务命令。
    pub fn submit_command(
        &self,
        command: CommandEnvelope,
    ) -> Result<Receiver<Result<FormalExecution, FormalError>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(Message::Formal(command, tx), IngressClass::Mutation)?;
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
            live.snapshot_seq = seq.last_applied();
            live.markets = books;
            live.registry = registry;
            live.managed_markets = managed;
            live.dedup = Some(window);
            live.place = place;
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
        let bytes = replay_candidate
            .image
            .encode_snapshot(trusted.max_bytes)
            .unwrap();
        replay_candidate.image = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
        let wal_payload = matching_domain::encode_wal_command(&command).unwrap();
        let decoded_command = matching_domain::decode_wal_command(&wal_payload).unwrap();
        let result = execute_market_management(
            MarketManagementContext {
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
        live_image.snapshot_seq = seq.last_applied();
        live_image.markets = books;
        live_image.registry = registry;
        live_image.managed_markets = managed;
        live_image.dedup = Some(window);
        live_image.place = place;
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
                (command, &mut None, false),
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
        assert!(!group_should_flush(1, 8, start, deadline));
        assert!(group_should_flush(1, 8, deadline, deadline));
        assert!(group_should_flush(8, 8, start, deadline));
    }
    #[test]
    fn single_sync_covers_batch_and_failure_keeps_real_state() {
        for fail in [false, true] {
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
                        Message::Formal(command(2), second_tx),
                        IngressClass::Mutation
                    )
                    .is_ok()
            );
            let mut epochs = UserControlEpochs::new(4).unwrap();
            let before = epochs.clone();
            let mut dedup = DedupWindow::new(IdempotencyConfig::default()).unwrap();
            let mut seq = Sequencer::new();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_fence_group(
                    (command(1), first_tx),
                    &mut consumer,
                    &mut gate,
                    (&mut epochs, &mut dedup, &mut seq),
                    (ShardId(1), ClusterId(1), LogicalTime::new(0)),
                )
            }));
            assert_eq!(syncs.load(Ordering::SeqCst), 1);
            if fail {
                assert!(result.is_err());
                assert_eq!(epochs, before);
                assert_eq!(seq.last_applied(), 0);
                assert!(dedup.lookup(&command(1)).unwrap().is_none());
                assert!(first_rx.recv().is_err());
                assert!(second_rx.recv().is_err());
            } else {
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
                    Message::Formal(command(2), second_tx),
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
                (command(1), first_tx),
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

/// 从可信配置构造空市场基线；市场策略尚不可从恢复配置单独推导。
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
    if trusted.routes.routes().any(|route| {
        route.shard_id == trusted.shard
            && route.state == RouteState::Active
            && !markets.contains_key(&route.market_id)
    }) {
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
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut image = candidate.image;
        let mut seq = Sequencer::from_last_applied(image.snapshot_seq)
            .map_err(|_| std::io::Error::other("重放序号耗尽"))?;
        let clock = LogicalClock::new(image.now);
        let mut durable: Option<LocalDurableCommit> = None;
        let mut executions = Vec::new();
        executions
            .try_reserve(records.len())
            .map_err(|_| std::io::Error::other("重放结果容量不足"))?;
        let mut event_hasher = crate::EventHasher::new();
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
        let mut previous_epoch = if image.genesis_baseline && lease_epoch.is_some() {
            None
        } else {
            Some(image.primary_epoch)
        };
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
            let identity = FormalIdentity {
                cluster: image.cluster,
                shard: image.shard,
                event_capacity: u64::from(u32::MAX) + 1,
            };
            let command = record.command;
            let result = match &command.payload {
                Command::ShardKillSwitch(_) => {
                    let window = image.dedup.as_mut().ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_shard_kill(ShardKillContext { identity, epoch: image.primary_epoch, seq: &mut seq, clock: &clock, markets: &mut image.markets, registry: &mut image.registry, place: &mut image.place, dedup: window, require_lease: false }, command)
                }
                Command::UpdateFeeSchedule(_) => {
                    let window = image.dedup.as_mut().ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_fee_schedule_update(identity, &image.routes, &mut seq, &clock, window,
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
                            identity,
                            routes: &image.routes,
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
                    image
                        .routes
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
                Command::MassCancelShard(_) => {
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_formal_guarded(
                        &image.routes,
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
            for event in &result.events {
                event_hasher
                    .update(event)
                    .map_err(|_| std::io::Error::other("重放事件摘要编码失败"))?;
            }
            image.snapshot_seq = result.seq;
            executions.push(result);
        }
        if seq.last_applied() != candidate.committed_through {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "重放未覆盖持久前缀",
            ));
        }
        if let Some(epoch) = lease_epoch {
            image.primary_epoch = epoch;
        }
        image.start_allowed = true;
        image.genesis_baseline = false;
        Ok(crate::SnapshotReplayResult {
            image,
            executions,
            event_hash: event_hasher.finish(),
        })
    }));
    result.map_err(|_| std::io::Error::other("重放触发内部不变量失败"))?
}
