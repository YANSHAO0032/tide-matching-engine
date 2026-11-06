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
        if !image.start_allowed {
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
            Some(PlaceRuntimeState { config: place, ids })
        } else {
            None
        };
        let mut initial_sequence = Sequencer::new();
        if let Some(image) = restored {
            initial_sequence = Sequencer::from_last_applied(image.snapshot_seq)
                .map_err(|_| ShardError::InvalidConfig)?;
            markets = image.markets;
            registry = image.registry;
            user_epochs = image.user_epochs;
            dedup = image.dedup;
            place_state = image.place;
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
                            if durable.as_ref().is_some_and(|gate| gate.group.is_some()) =>
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
                                        (command, &mut durable),
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
                            let _ = reply.send(
                                markets
                                    .get(&market)
                                    .map(|book| book.order(id).cloned())
                                    .ok_or(RejectReason::MarketNotFound),
                            );
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
                                            ids: state
                                                .ids
                                                .try_clone_preallocated()
                                                .map_err(|()| SnapshotError::Capacity)?,
                                        })
                                    })
                                    .transpose()?;
                                Ok(SnapshotImage {
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
}

/// 正式限价命令所需的单写者状态。
struct PlaceExecutionContext<'a> {
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
        if matches!(command.payload, Command::ApplyUserControlFenceShard(_)) {
            return self.fence.stage(command);
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
            return execute_place(
                PlaceExecutionContext {
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
        }
        execute_formal_idempotent(
            routes,
            FormalIdentity {
                shard: self.fence.shard,
                cluster: self.fence.cluster,
                event_capacity: u64::from(u32::MAX) + 1,
            },
            &mut self.fence.seq,
            &self.fence.clock,
            &mut self.markets,
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
    let (limit, delay) = gate.group.expect("仅批量模式调用");
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
    *context.registry = candidate.registry;
    *context.place = candidate.place;
    for (reply, result) in replies {
        let _ = reply.send(result);
    }
    deferred
}

/// 完成预检后执行限价候选，事件容量有效时才一次安装全部后态。
fn execute_place(
    context: PlaceExecutionContext<'_>,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let PlaceExecutionContext {
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
    let planned = place_candidate(
        PlacePlanningContext {
            policy,
            keys: &state.config.keys,
            command_id: command.command_id,
            session: command.session_id,
            now: clock.now(),
        },
        place,
        book,
        registry,
        ids,
        &cursor,
    );
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
        (command.clone(), durable),
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
    (command, durable): (CommandEnvelope, &mut Option<LocalDurableCommit>),
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
            let (result, events) = candidate
                .apply_mass_cancel(&mut candidate_registry, plan)
                .expect("新生成的批量撤单计划发生状态漂移");
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
        FormalOutcome::MassCancel(_) | FormalOutcome::UserFence(_) | FormalOutcome::Placed(_) => {
            (CommandStatus::Succeeded, None)
        }
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
                (command, &mut None),
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
        let mut state = PlaceRuntimeState { config, ids };
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
        for record in records {
            if seq.last_applied().checked_add(1) != Some(record.shard_seq)
                || record.shard_seq == u64::MAX
                || record.epoch != image.primary_epoch
                || record.engine_time != clock.now().get()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "重放序号、纪元或逻辑时间不一致",
                ));
            }
            let identity = FormalIdentity {
                cluster: image.cluster,
                shard: image.shard,
                event_capacity: u64::from(u32::MAX) + 1,
            };
            let command = record.command;
            let result = match &command.payload {
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
                    execute_place(
                        PlaceExecutionContext {
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
                    )
                }
                Command::MassCancelShard(_) => {
                    let window = image
                        .dedup
                        .as_mut()
                        .ok_or_else(|| std::io::Error::other("缺少重放去重窗口"))?;
                    execute_formal_idempotent(
                        &image.routes,
                        identity,
                        &mut seq,
                        &clock,
                        &mut image.markets,
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
        image.start_allowed = true;
        Ok(crate::SnapshotReplayResult {
            image,
            executions,
            event_hash: event_hasher.finish(),
        })
    }));
    result.map_err(|_| std::io::Error::other("重放触发内部不变量失败"))?
}
