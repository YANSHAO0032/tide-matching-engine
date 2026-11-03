//! 分片工作线程及进程内消息。
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
            (now, IdempotencyConfig::default(), 0, 4096, None),
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
            (now, idempotency, 0, 4096, None),
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
            (now, idempotency, primary_epoch, user_capacity, None),
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
            (now, idempotency, primary_epoch, user_capacity, Some(place)),
        )
    }

    /// 校验分片初始状态后建立唯一工作线程和有界入口。
    fn start_inner(
        shard: ShardId,
        cluster: ClusterId,
        routes: RouteTable,
        books: Vec<(MarketId, ProductionOrderBook)>,
        config: ShardConfig,
        mut registry: Option<ReservationRegistry>,
        (now, idempotency, primary_epoch, user_capacity, place_config): (
            LogicalTime,
            IdempotencyConfig,
            u64,
            usize,
            Option<PlaceSupportConfig>,
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
        if routes.routes().any(|route| {
            route.shard_id == shard
                && route.state == RouteState::Active
                && !markets.contains_key(&route.market_id)
        }) {
            return Err(ShardError::InvalidConfig);
        }
        let ownership =
            crate::ownership::MarketOwnership::acquire(cluster, shard, markets.keys().copied())
                .ok_or(ShardError::InvalidConfig)?;
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: config.capacity,
            mutation_reserved_slots: config.mutation_reserved_slots,
            max_query_inflight: config.max_query_inflight,
            max_control_inflight: config.max_control_inflight,
        })
        .map_err(|_| ShardError::InvalidConfig)?;
        let route_snapshot = Arc::new(routes);
        let worker_routes = Arc::clone(&route_snapshot);
        let close = producer.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let drop_requested = Arc::new(AtomicBool::new(false));
        let worker_drop = Arc::clone(&drop_requested);
        let join = thread::spawn(move || {
            // 所有权与工作线程同寿命，排空后才释放。
            let _ownership = ownership;
            let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut seq = Sequencer::new();
                let clock = LogicalClock::new(now);
                let mut waiter = Waiter::new(config.wait_strategy);
                loop {
                    let Some(msg) = consumer.pop() else {
                        if worker_drop.load(Ordering::Acquire) && consumer.is_closed_and_empty() {
                            break;
                        }
                        waiter.idle();
                        continue;
                    };
                    waiter.reset();
                    match msg {
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
                                        (command, window, &mut place_state),
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
                                        command,
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
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
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
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
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
    (command, dedup, place_state): (
        CommandEnvelope,
        &mut DedupWindow,
        &mut Option<PlaceRuntimeState>,
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
        command.clone(),
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
    command: CommandEnvelope,
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
    let n = seq.allocate().expect("分片序号耗尽，停止处理");
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
            (command.clone(), &mut window, &mut None),
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
            (command, &mut window, &mut None),
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
                command,
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
