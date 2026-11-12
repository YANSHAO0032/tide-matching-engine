//! 单市场订单簿状态、构造与基础查询；各订单策略在独立子模块中实现。

use crate::{
    OrderArena, OrderIndex, OrderNode, PriceLevel, QueuePriorityAllocator, QueuePriorityPolicy,
    StopBook, StopPriorityAllocator,
};
use matching_domain::{OrderId, Price, PriceReferenceState, QueuePriority, RejectReason, Side};
use matching_risk::ReservationState;
use std::collections::{BTreeMap, HashMap};

pub(crate) mod amend_candidate;
mod amend_iceberg;
mod amend_stop;
mod cancel_replace;
mod cancel_replace_view;
mod expiry;
mod iceberg;
mod market_binding;
/// 可信市场绑定失败。
pub use market_binding::MarketBindingError;
mod limit_execution;
mod market_by_base;
mod market_by_quote;
mod mass_cancel;
mod matching;
mod price_band;
mod reservation_place;
mod resting;
mod snapshot;
mod stop_orders;
mod stop_trigger;

pub use amend_candidate::ProductionAmendRegistryPlan;
pub use cancel_replace::{ProductionCancelReplaceContext, ProductionCancelReplacePlan};
pub use cancel_replace_view::ProductionBookWithout;
use expiry::RegularExpiryIndex;
pub use mass_cancel::ProductionMassCancelPlan;
pub use reservation_place::{ProductionMarketPlan, ProductionPlaceOrder, ProductionPlacePlan};
pub use stop_trigger::ProductionTriggerRegistryPlan;

/// 指明已预检的余量将追加到既有档位还是新建档位。
///
/// 该枚举只保存提交前后必须保持一致的值，不保留对容器的借用；
/// 因而预检完成后仍可执行挂单状态变更，再通过失败即停止的断言检查
/// 提交时的所有者状态没有发生意外漂移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestTargetPlan {
    /// 将余量追加到提交前已存在的价档。
    Existing {
        /// 提交前既有价档的先进先出尾节点。
        tail: OrderIndex,
        /// 提交前既有价档中的节点数量。
        count_before: u32,
        /// 提交前既有价档的可见数量聚合值。
        total_before: u64,
        /// 提交后既有价档应具有的节点数量。
        count_after: u32,
        /// 提交后既有价档应具有的可见数量聚合值。
        total_after: u64,
    },
    /// 将余量写入提交前尚不存在的新价档。
    New {
        /// 提交后新建价档应具有的节点数量。
        count_after: u32,
        /// 提交后新建价档应具有的可见数量聚合值。
        total_after: u64,
    },
}

/// `preflight_rest_common` 产生的只读挂单提交计划。
///
/// 它记录身份、计划分配的优先级及目标档位的聚合快照。
/// 公开 `rest` 另行施加严格不交叉；完整限价单 GTC 则在
/// 挂单已被预检后复用本计划，保证余量的可预期拒绝发生在首笔成交前。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestPlan {
    /// 即将挂入订单簿的订单标识符。
    order_id: OrderId,
    /// 挂单所在的买卖方向。
    side: Side,
    /// 挂单所属的价格档位价格。
    price: Price,
    /// 已预留给本次实际挂单的队列优先级。
    priority: QueuePriority,
    /// 既有或新建价格档位的预检聚合计划。
    target: RestTargetPlan,
}

/// 连续成交的只读预测结果。
///
/// `trade_count` 决定调用方至少要提供多少 TradeId，`remaining` 是
/// 本次连续撮合结束后吃单方的预测余量。它不拥有节点或可变引用，
/// 也不改变新到订单、订单簿或优先级分配器。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FillPlan {
    /// 预期产生的成交笔数。
    trade_count: usize,
    /// 连续撮合结束后吃单方的基础资产余量。
    remaining: u64,
}

/// 单市场生产订单簿的查询、挂单、取消和撮合入口。
///
/// 价格顺序由双侧有序索引保存，同价先进先出由 PriceLevel 链表维护；
/// OrderId 索引只承担定位，不决定业务顺序。当前支持已确定应挂单的
/// 限价单 GTC 余量、已知标识符的取消、完整限价单 GTC、市价订单，以及
/// 限价单 IOC/FOK/PostOnly 与冰山。实际挂单及冰山补量时分配优先级；不包含完整资金和命令协议。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionOrderBook {
    /// 由分片配置绑定的可信市场；旧独立订单簿保持未绑定。
    market_id: Option<matching_domain::MarketId>,
    /// 买侧价格档位；合法非空条目中的节点必须具有相同价格和买入方向。
    bids: BTreeMap<Price, PriceLevel>,
    /// 卖侧价格档位；合法非空条目中的节点必须具有相同价格和卖出方向。
    asks: BTreeMap<Price, PriceLevel>,
    /// 当前活动 OrderId 到 Arena 槽位的定位索引，必须与节点身份一致。
    orders: HashMap<OrderId, OrderIndex>,
    /// 活动普通与未触发止损订单的本地预留快照，仅供身份查询。
    reservations: HashMap<OrderId, ReservationState>,
    /// 节点的唯一存储；价格档位和标识符索引通过 OrderIndex 引用它。
    arena: OrderArena,
    /// 本市场独占的优先级状态；只读查询不消耗序号。
    queue_priority_allocator: QueuePriorityAllocator,
    /// 未触发止损订单及其位置、到期索引。
    stop_book: StopBook,
    /// 未触发止损订单专用的市场本地优先级状态。
    stop_priority_allocator: StopPriorityAllocator,
    /// 普通 DAY/GTD 挂单的二级到期索引。
    regular_expiry: RegularExpiryIndex,
    /// 各参考来源的最近已接受价格和序号。
    reference_prices: PriceReferenceState,
    /// 启用价格带时固定的市场配置与数值规则。
    price_band: Option<(
        matching_domain::PriceBandConfig,
        matching_domain::MarketMath,
    )>,
}

impl ProductionOrderBook {
    /// 返回当前市场普通及 Stop 优先级分配器共用的策略。
    pub fn priority_policy(&self) -> QueuePriorityPolicy {
        *self.queue_priority_allocator.policy()
    }
    /// 原子更新两个优先级分配器的保护策略，保持各自计数器不变。
    pub fn update_priority_policy(
        &mut self,
        policy: QueuePriorityPolicy,
    ) -> Result<(), RejectReason> {
        let queue = QueuePriorityAllocator::try_restore(policy, self.next_queue_priority())
            .map_err(|_| RejectReason::PrioritySpaceExhaustion)?;
        let stop = StopPriorityAllocator::try_restore(policy, self.next_stop_priority())
            .map_err(|_| RejectReason::PrioritySpaceExhaustion)?;
        self.queue_priority_allocator = queue;
        self.stop_priority_allocator = stop;
        Ok(())
    }
    /// 使用已验证的策略创建空簿，市场内优先级从零开始。
    pub fn new(policy: QueuePriorityPolicy) -> Self {
        Self {
            market_id: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            reservations: HashMap::new(),
            arena: OrderArena::new(),
            queue_priority_allocator: QueuePriorityAllocator::new(policy),
            stop_book: StopBook::new(),
            stop_priority_allocator: StopPriorityAllocator::new(policy),
            regular_expiry: RegularExpiryIndex::default(),
            reference_prices: PriceReferenceState::default(),
            price_band: None,
        }
    }

    /// 校验恢复镜像中的价档、链表、索引、到期与优先级约束。
    ///
    /// 只验证当前市场内状态，不验证外部分片的预留登记表或路由。
    pub fn validate_snapshot_invariants(&self) -> Result<(), &'static str> {
        use std::collections::BTreeSet;
        if self.queue_priority_allocator.policy() != self.stop_priority_allocator.policy() {
            return Err("普通与止损优先级策略不一致");
        }
        self.arena
            .validate_snapshot_free()
            .map_err(|_| "Arena 空闲栈不一致")?;
        self.stop_book.validate()?;
        let mut slots = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut queue_priorities = BTreeSet::new();
        let mut expiry = RegularExpiryIndex::default();
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price, level) in levels {
                level
                    .validate_snapshot_shape()
                    .map_err(|_| "价档端点无效")?;
                if level.count() == 0 {
                    return Err("价格索引包含空档");
                }
                let mut current = level.head();
                let mut previous = None;
                let mut previous_priority = None;
                let mut count = 0_u32;
                let mut visible = 0_u64;
                while let Some(index) = current {
                    if !slots.insert(index) {
                        return Err("价档链存在重复节点或环");
                    }
                    let node = self.arena.get(index).ok_or("价档引用空槽")?;
                    node.validate_snapshot_fields()
                        .map_err(|_| "节点数量或有效期无效")?;
                    if node.prev() != previous
                        || node.original_order().side != side
                        || node.original_order().price != *price
                    {
                        return Err("价档链接或节点位置不一致");
                    }
                    if previous_priority.is_some_and(|p| p >= node.priority())
                        || !queue_priorities.insert(node.priority())
                        || node.priority().get() >= self.next_queue_priority()
                    {
                        return Err("普通订单优先级无效");
                    }
                    let id = node.original_order().order_id;
                    if !ids.insert(id) || self.orders.get(&id) != Some(&index) {
                        return Err("普通订单身份索引无效");
                    }
                    count = count.checked_add(1).ok_or("价档数量溢出")?;
                    visible = visible
                        .checked_add(node.remaining())
                        .ok_or("价档聚合溢出")?;
                    expiry.insert(id, node.time_in_force());
                    previous = Some(index);
                    previous_priority = Some(node.priority());
                    current = node.next();
                }
                if previous != level.tail()
                    || count != level.count()
                    || visible != level.total_visible_qty()
                {
                    return Err("价档端点或聚合不一致");
                }
            }
        }
        if self.orders.len() != slots.len() || expiry != self.regular_expiry {
            return Err("订单或到期索引不一致");
        }
        let (arena_slots, _) = self.arena.state_snapshot();
        for (position, node) in arena_slots.iter().enumerate() {
            if node.is_some()
                && !slots
                    .contains(&OrderIndex::try_from_usize(position).map_err(|_| "Arena 下标溢出")?)
            {
                return Err("Arena 活动节点不可达");
            }
        }
        let mut stop_priorities = BTreeSet::new();
        for side in [Side::Buy, Side::Sell] {
            for (order, priority) in self.stop_book.ordered(side) {
                if !ids.insert(order.order_id())
                    || !stop_priorities.insert(priority)
                    || priority.get() >= self.next_stop_priority()
                {
                    return Err("止损订单身份或优先级无效");
                }
            }
        }
        for (id, state) in &self.reservations {
            let regular_user = self
                .orders
                .get(id)
                .and_then(|index| self.arena.get(*index))
                .map(|node| node.original_order().user_id);
            let stop = self.stop_book.get(*id).map(|(order, _)| order);
            let stop_user = stop.map(|order| match order {
                matching_domain::StopOrder::Limit(order) => order.user_id,
                matching_domain::StopOrder::Market(order) => order.user_id,
            });
            if state.order_id != *id
                || !ids.contains(id)
                || state.status != matching_risk::ReservationStatus::Active
                || self
                    .market_id
                    .is_some_and(|market| market != state.market_id)
                || regular_user.or(stop_user) != Some(state.user_id)
                || stop.is_some_and(|order| order.reservation_id() != state.reservation_id)
            {
                return Err("本地预留身份不一致");
            }
        }
        if matches!((self.best_price(Side::Buy),self.best_price(Side::Sell)),(Some(bid),Some(ask)) if bid>=ask)
        {
            return Err("订单簿交叉");
        }
        Ok(())
    }

    /// 查询订单簿绑定的可信市场。
    pub fn market_id(&self) -> Option<matching_domain::MarketId> {
        self.market_id
    }

    /// 查询指定簿侧的最优价格，空侧返回 None；方向不是新到订单方向。
    ///
    /// 契约要求买入取最高买价、卖出取最低卖价，不分配优先级或缓存价格。
    pub fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.asks.first_key_value().map(|(price, _)| *price),
        }
    }

    /// 经单键标识符查找借用 Arena 节点；未知标识符返回 None。
    ///
    /// # 不变量断言
    ///
    /// 索引命中但槽位不存在或节点身份不匹配时，按内部不变量损坏停止，
    /// 不将损坏静默解释成订单不存在。此查询不遍历或校验整条价档链。
    pub fn order(&self, order_id: OrderId) -> Option<&OrderNode> {
        let index = *self.orders.get(&order_id)?;

        let node = self.arena.get(index).unwrap_or_else(|| {
            panic!(
                "ProductionOrderBook invariant violation: \
                 order index references missing arena slot: \
                 order_id={order_id:?}, index={index:?}"
            )
        });
        assert_eq!(
            node.original_order().order_id,
            order_id,
            "ProductionOrderBook invariant violation: \
             order index points to different order: \
             requested={order_id:?}, actual={:?}, index={index:?}",
            node.original_order().order_id,
        );
        Some(node)
    }

    /// 按订单身份只读查询已登记的本地预留快照。
    pub fn reservation_state(&self, order_id: OrderId) -> Option<&ReservationState> {
        self.reservations.get(&order_id)
    }

    /// 读取下一次分配将使用的优先级，不推进分配器。
    pub fn next_queue_priority(&self) -> u128 {
        self.queue_priority_allocator.next()
    }

    /// 读取下一次未触发止损订单将使用的 StopPriority。
    pub fn next_stop_priority(&self) -> u128 {
        self.stop_priority_allocator.next()
    }

    /// 判断标识是否被常规订单或未触发止损订单占用。
    pub(crate) fn contains_active_order(&self, order_id: OrderId) -> bool {
        self.orders.contains_key(&order_id) || self.stop_book.contains(order_id)
    }

    /// 在任何订单簿写入前核对候选预留身份及既有影子状态。
    fn validate_new_reservation(
        &self,
        order_id: OrderId,
        user_id: matching_domain::UserId,
        state: &ReservationState,
    ) -> Result<(), RejectReason> {
        if self.contains_active_order(order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self
            .market_id
            .is_some_and(|market| market != state.market_id)
        {
            return Err(RejectReason::InvalidReservation);
        }
        if state.status != matching_risk::ReservationStatus::Active
            || state.order_id != order_id
            || state.user_id != user_id
            || self.reservations.contains_key(&order_id)
        {
            return Err(RejectReason::InvalidReservation);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
