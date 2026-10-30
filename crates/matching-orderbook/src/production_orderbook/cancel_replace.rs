//! 生产撤单替换的只读组合计划与无业务拒绝提交。
//!
//! 预检使用排除旧身份的生产视图；提交消费冻结的片计划或 STP 候选后态及资金后态。

use super::{ProductionOrderBook, RestPlan, expiry::RegularExpiryIndex, iceberg::IcebergPlan};
use crate::{
    OrderIndex, OrderNode, PriceLevel, QueuePriorityAllocator, StopBook, StopPriorityAllocator,
};
use matching_domain::{
    CancelReason, CancelReplaceOrder, CancelReplaceRejection, EngineEvent, IcebergOrder,
    LimitOrder, LogicalTime, MarketMath, OrderId, OrderRemaining, Price, PriceReferenceState, Qty,
    RejectReason, ReplacementOrderKind, StopOrder, StopPriority, TimeInForce, TradeId,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationApplyError, ReservationRegistry, ReservationState,
    ReservationStatus, RiskKeySet,
};
use std::collections::{BTreeMap, HashMap};

/// 调用方固定的真实市场数值、逻辑时间、时段和成交身份。
///
/// 资金需求与费率由独立的 `ExpectedReplacementReservation` 提供，不能从令牌猜测。
/// 成交身份沿用正常生产撮合的调用方契约：容量不足在首次写入前停止处理。
pub struct ProductionCancelReplaceContext<'a> {
    /// 实际市场价格、数量和本金规则。
    pub math: &'a MarketMath,
    /// 持久化的逻辑时间输入，不读取墙上时钟。
    pub now: LogicalTime,
    /// 当前实际交易时段身份。
    pub current_session_id: u64,
    /// 调用方已分配的原序成交身份；本入口不分配全局身份。
    pub trade_ids: &'a [TradeId],
}

/// 提交前完整物理业务快照，不是持久化快照格式。
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProductionSnapshot {
    /// 买方价格档及其 intrusive FIFO 链的完整快照。
    bids: BTreeMap<Price, PriceLevel>,
    /// 卖方价格档及其 intrusive FIFO 链的完整快照。
    asks: BTreeMap<Price, PriceLevel>,
    /// 订单身份到 Arena 槽位的索引快照。
    orders: HashMap<OrderId, OrderIndex>,
    /// 活动订单身份到金融预留影子的映射快照。
    reservations: HashMap<OrderId, ReservationState>,
    /// Arena 槽位和空闲槽位链的完整快照。
    arena: (Vec<Option<OrderNode>>, Vec<OrderIndex>),
    /// 普通挂单优先级分配器快照。
    queue_priority_allocator: QueuePriorityAllocator,
    /// 未触发止损订单及其索引的完整快照。
    stop_book: StopBook,
    /// 止损优先级分配器快照。
    stop_priority_allocator: StopPriorityAllocator,
    /// 普通订单到期索引快照。
    regular_expiry: RegularExpiryIndex,
    /// 价格保护参考价状态快照。
    reference_prices: PriceReferenceState,
    /// 价格带配置和市场数值规则的冻结快照。
    price_band: Option<(matching_domain::PriceBandConfig, MarketMath)>,
}

impl ProductionSnapshot {
    fn capture(book: &ProductionOrderBook) -> Self {
        Self {
            bids: book.bids.clone(),
            asks: book.asks.clone(),
            orders: book.orders.clone(),
            reservations: book.reservations.clone(),
            arena: book.arena.state_snapshot(),
            queue_priority_allocator: book.queue_priority_allocator.clone(),
            stop_book: book.stop_book.clone(),
            stop_priority_allocator: book.stop_priority_allocator.clone(),
            regular_expiry: book.regular_expiry.clone(),
            reference_prices: book.reference_prices.clone(),
            price_band: book.price_band,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OldLocation {
    /// 旧单位于普通订单簿的 Arena 槽位。
    Regular(OrderIndex),
    /// 旧单位于未触发止损簿的稳定止损优先级位置。
    Stop(StopPriority),
}

/// 不可伪造的生产撤单替换计划；固定完整前态和全部确定性提交输入。
///
/// 普通片成交由真实生产价格索引、先进先出链与独立补量扫描生成，
/// 不保存参考簿或调用其计划/撮合。资金后态包含旧单、终结挂单和新单的归属。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionCancelReplacePlan {
    /// 计划生成时的完整物理订单簿快照，用于提交前漂移检测。
    before: ProductionSnapshot,
    /// 计划生成时的完整金融登记表快照，用于提交前漂移检测。
    before_registry: ReservationRegistry,
    /// 预先验证的提交后金融登记表状态。
    after_registry: ReservationRegistry,
    /// 按订单策略冻结的独立生产提交计划。
    commit: ProductionReplaceCommit,
}

/// 普通撮合沿用物理片计划；STP 预演使用完整生产簿后态。
#[derive(Clone, Debug, PartialEq, Eq)]
enum ProductionReplaceCommit {
    /// 已冻结的常规片成交与剩余挂单计划。
    Standard(Box<ProductionStandardReplace>),
    /// 已完成所有 STP 动作及结构校验的候选后态。
    Stp {
        /// 排除旧单并执行替换单后的完整生产簿。
        after_book: Box<ProductionOrderBook>,
        /// 旧单取消及替换执行的原序事件。
        events: Vec<EngineEvent>,
    },
}

/// 非 STP 替换的既有生产提交输入。
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProductionStandardReplace {
    /// 待移除旧订单的身份。
    old_id: OrderId,
    /// 旧单在普通簿或止损簿中的实际位置。
    old_location: OldLocation,
    /// 取消事件使用的旧单总剩余基础数量。
    old_remaining: Qty,
    /// 已通过结构和数值验证的新普通限价载荷。
    accepted: LimitOrder,
    /// 新单为冰山时已验证的单片峰值；普通限价时为空。
    peak: Option<Qty>,
    /// 新单已验证的有效期。
    tif: TimeInForce,
    /// 真实生产索引与 FIFO 扫描冻结的冰山成交片计划。
    fills: IcebergPlan,
    /// 与冻结成交片一一对应的调用方成交身份。
    trade_ids: Vec<TradeId>,
    /// 新单尚有余量时冻结的实际挂单计划。
    residual: Option<RestPlan>,
    /// 新单余量挂单预先保留的 Arena 槽位。
    residual_slot: Option<OrderIndex>,
    /// 提交后仍有活动新订单时绑定给它的金融预留快照。
    active_reservation: ReservationState,
    /// 逐笔实际成交后已预检的参考价状态。
    reference_after: PriceReferenceState,
}

impl ProductionOrderBook {
    /// 先验证旧单实际活动位置和身份，再通过同一个逻辑删除视图完成全部下单预检。
    ///
    /// 资金过渡晚于结构、时间、数值、片成交及余量挂单预检；所有失败只返回拒绝描述，
    /// 不登记新预留、不解除旧链接、不改变优先级、到期引用或参考价格。
    pub fn plan_cancel_replace(
        &self,
        request: &CancelReplaceOrder,
        expected: &ExpectedReplacementReservation,
        registry: &ReservationRegistry,
        keys: &RiskKeySet,
        context: ProductionCancelReplaceContext<'_>,
    ) -> Result<ProductionCancelReplacePlan, CancelReplaceRejection> {
        self.plan_cancel_replace_inner(request, expected, registry, keys, context)
            .map_err(|reason| request.rejection(reason))
    }

    /// 完整核对真实簿和登记表前态后，提交已冻结的旧单删除、资金后态、片成交及余量。
    ///
    /// 漂移在首次写入前返回不变量错误；首次写入后仅有断言保护的确定性提交，
    /// 不运行正常业务拒绝、资金验签、重新撮合或回滚，不发出提前资金释放。
    pub fn apply_cancel_replace(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ProductionCancelReplacePlan,
    ) -> Result<Vec<EngineEvent>, ReservationApplyError> {
        if ProductionSnapshot::capture(self) != plan.before || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        let ProductionCancelReplacePlan {
            before: _,
            before_registry: _,
            after_registry,
            commit,
        } = plan;
        let plan = match commit {
            ProductionReplaceCommit::Stp { after_book, events } => {
                *self = *after_book;
                *registry = after_registry;
                return Ok(events);
            }
            ProductionReplaceCommit::Standard(plan) => *plan,
        };
        self.reference_prices = plan.reference_after.clone();
        match plan.old_location {
            OldLocation::Regular(index) => {
                assert_eq!(self.orders.get(&plan.old_id), Some(&index));
                self.cancel(plan.old_id)
                    .expect("preflighted production old regular identity");
            }
            OldLocation::Stop(priority) => {
                let (_, actual) = self
                    .cancel_stop(plan.old_id)
                    .expect("preflighted production old Stop identity");
                assert_eq!(actual, priority);
            }
        }
        *registry = after_registry;
        let mut events = vec![EngineEvent::OrderCanceled {
            order_id: plan.old_id,
            remaining: OrderRemaining::Base(plan.old_remaining),
            reason: CancelReason::CancelReplace,
        }];
        events.extend(self.commit_iceberg_plan(
            &plan.fills,
            plan.accepted.side,
            plan.accepted.order_id,
            &plan.trade_ids,
        ));
        if plan.peak.is_some() && !plan.fills.fills.is_empty() {
            events.push(if plan.fills.base_remaining == 0 {
                EngineEvent::OrderFilled {
                    order_id: plan.accepted.order_id,
                }
            } else {
                EngineEvent::OrderPartiallyFilled {
                    order_id: plan.accepted.order_id,
                    remaining: Qty::try_new(plan.fills.base_remaining)
                        .expect("positive frozen iceberg residual"),
                }
            });
        }
        if plan.tif == TimeInForce::Ioc && plan.fills.base_remaining > 0 {
            events.push(EngineEvent::OrderCanceled {
                order_id: plan.accepted.order_id,
                remaining: OrderRemaining::Base(
                    Qty::try_new(plan.fills.base_remaining).expect("positive frozen IOC residual"),
                ),
                reason: CancelReason::IocRemainder,
            });
        }
        if let Some(rest) = plan.residual {
            self.assert_rest_strictly_non_crossing(&rest);
            if let Some(peak) = plan.peak {
                self.commit_iceberg_rest(
                    IcebergOrder::try_new(plan.accepted.clone(), peak)
                        .expect("validated production iceberg"),
                    plan.fills.base_remaining,
                    rest,
                );
            } else {
                let mut residual = plan.accepted.clone();
                residual.qty = Qty::try_new(plan.fills.base_remaining)
                    .expect("positive frozen Limit residual");
                self.commit_rest_timed(residual, plan.tif, rest);
                self.arena
                    .get_mut(self.orders[&plan.accepted.order_id])
                    .expect("committed residual node")
                    .amend_regular_in_place(
                        plan.accepted.clone(),
                        Qty::try_new(plan.fills.base_remaining).expect("positive residual"),
                        plan.tif,
                    );
            }
            assert!(
                self.reservations
                    .insert(plan.accepted.order_id, plan.active_reservation)
                    .is_none()
            );
            assert_eq!(
                self.orders.get(&plan.accepted.order_id).copied(),
                plan.residual_slot
            );
        }
        Ok(events)
    }

    fn plan_cancel_replace_inner(
        &self,
        request: &CancelReplaceOrder,
        expected: &ExpectedReplacementReservation,
        registry: &ReservationRegistry,
        keys: &RiskKeySet,
        context: ProductionCancelReplaceContext<'_>,
    ) -> Result<ProductionCancelReplacePlan, RejectReason> {
        if let Some((_, configured)) = self.price_band_config() {
            assert_eq!(*context.math, configured, "市场数值配置不一致");
        }
        let (old_location, old_user, old_remaining) = if let Some(old) =
            self.order(request.old_order_id)
        {
            (
                OldLocation::Regular(self.orders[&request.old_order_id]),
                old.original_order().user_id,
                old.total_remaining(),
            )
        } else if let Some((old, priority)) = self.stop_book.get(request.old_order_id) {
            match old {
                StopOrder::Limit(old) => (OldLocation::Stop(priority), old.user_id, old.qty.get()),
                StopOrder::Market(old) => {
                    if old.user_id != request.user_id || request.market_id != expected.market_id {
                        return Err(RejectReason::OrderNotFound);
                    }
                    return Err(RejectReason::InvalidOrderCombination);
                }
            }
        } else {
            return Err(RejectReason::OrderNotFound);
        };
        if old_user != request.user_id || request.market_id != expected.market_id {
            return Err(RejectReason::OrderNotFound);
        }
        request.validate_structure()?;
        if self.contains_active_order(request.replacement.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let old_state = self
            .reservations
            .get(&request.old_order_id)
            .ok_or(RejectReason::InvalidReservation)?;
        if old_state.status != ReservationStatus::Active
            || old_state.order_id != request.old_order_id
            || old_state.user_id != old_user
            || old_state.market_id != expected.market_id
            || old_state.reservation_id != expected.old_reservation_id
            || old_state.version != expected.expected_version
            || expected.old_order_id != request.old_order_id
            || expected.new_order_id != request.replacement.order_id
            || expected.user_id != old_user
        {
            return Err(RejectReason::InvalidReservation);
        }
        if let Some((StopOrder::Limit(old), _)) = self.stop_book.get(request.old_order_id)
            && (old.reservation_id != old_state.reservation_id
                || old.fee_schedule_version != old_state.fee_schedule_version)
        {
            return Err(RejectReason::InvalidReservation);
        }
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;

        let replacement = &request.replacement;
        let accepted = LimitOrder {
            order_id: replacement.order_id,
            user_id: replacement.user_id,
            stp_key: replacement.stp_key,
            stp_mode: replacement.stp_mode,
            side: replacement.side,
            price: replacement.price,
            qty: replacement.qty,
        };
        self.validate_band_price(accepted.price)?;
        let (tif, flags, peak) = match replacement.kind {
            ReplacementOrderKind::Limit { tif, flags } => (tif, flags, None),
            ReplacementOrderKind::Iceberg {
                tif,
                flags,
                peak_size,
            } => {
                IcebergOrder::try_new(accepted.clone(), peak_size)?;
                (tif, flags, Some(peak_size))
            }
        };
        if accepted.price.get() % context.math.tick_size() != 0 {
            return Err(RejectReason::InvalidTick);
        }
        if !accepted.qty.get().is_multiple_of(context.math.lot_size())
            || peak.is_some_and(|p| !p.get().is_multiple_of(context.math.lot_size()))
        {
            return Err(RejectReason::InvalidLot);
        }
        if !matches!(tif, TimeInForce::Ioc | TimeInForce::Fok) {
            Self::validate_resting_tif(tif, context.now, context.current_session_id)?;
        }
        context
            .math
            .trade_notional(accepted.price, accepted.qty)
            .map_err(Self::reject_quote_math_error)?;
        if let Some(peak) = peak {
            context
                .math
                .trade_notional(accepted.price, peak)
                .map_err(Self::reject_quote_math_error)?;
        }

        let view = self.book_without(request.old_order_id)?;
        if accepted.stp_mode != matching_domain::StpMode::None {
            if flags.post_only {
                view.preflight_post_only(&accepted)?;
            }
            let mut after_book = self.clone();
            match old_location {
                OldLocation::Regular(_) => {
                    after_book
                        .cancel(request.old_order_id)
                        .expect("planned old order");
                }
                OldLocation::Stop(_) => {
                    after_book
                        .cancel_stop(request.old_order_id)
                        .expect("planned old stop");
                }
            }
            let matching_events = if let Some(peak) = peak {
                let iceberg = IcebergOrder::try_new(accepted.clone(), peak)?;
                if flags.post_only {
                    after_book.place_iceberg_post_only(iceberg, context.math)?;
                    Vec::new()
                } else {
                    after_book.place_iceberg(iceberg, context.math, context.trade_ids)?
                }
            } else if flags.post_only {
                after_book.place_limit_post_only(accepted.clone())?;
                Vec::new()
            } else {
                match tif {
                    TimeInForce::Ioc => after_book
                        .place_limit_ioc(accepted.clone(), context.math, context.trade_ids)?
                        .events()
                        .to_vec(),
                    TimeInForce::Fok => after_book
                        .place_limit_fok(accepted.clone(), context.math, context.trade_ids)?
                        .events()
                        .to_vec(),
                    _ => after_book.place_limit_timed_with_events(
                        accepted.clone(),
                        tif,
                        context.now,
                        context.current_session_id,
                        context.trade_ids,
                    )?,
                }
            };
            let mut total = matching_domain::QuoteAmount::new(0);
            after_book.reference_prices =
                self.reference_prices
                    .plan_trade_prices(matching_events.iter().filter_map(|event| match event {
                        EngineEvent::Trade(trade) => Some(trade.price),
                        _ => None,
                    }))?;
            for event in &matching_events {
                if let EngineEvent::Trade(trade) = event {
                    total = total.checked_add(
                        context
                            .math
                            .trade_notional(trade.price, trade.qty)
                            .map_err(Self::reject_quote_math_error)?,
                    )?;
                }
            }
            let mut terminal_ids = self
                .orders
                .keys()
                .copied()
                .filter(|id| *id != request.old_order_id && !after_book.orders.contains_key(id))
                .collect::<Vec<_>>();
            terminal_ids.sort_unstable();
            let mut terminal = Vec::new();
            for id in &terminal_ids {
                terminal.push(
                    self.reservations
                        .get(id)
                        .ok_or(RejectReason::InvalidReservation)?,
                );
            }
            let transition = registry
                .plan_transition(keys, expected, &request.reservation_transition)
                .map_err(|rejection| rejection.reason)?;
            let active = transition.active_state().clone();
            let mut after_registry = registry.clone();
            after_registry
                .apply_transition(transition)
                .expect("fresh replacement transition");
            if let Some(index) = after_book.orders.get(&accepted.order_id).copied() {
                if peak.is_none() {
                    let node = after_book
                        .arena
                        .get_mut(index)
                        .expect("replacement residual");
                    let remaining = Qty::try_new(node.total_remaining())?;
                    node.amend_regular_in_place(accepted.clone(), remaining, node.time_in_force());
                }
                assert!(
                    after_book
                        .reservations
                        .insert(accepted.order_id, active.clone())
                        .is_none()
                );
            } else {
                terminal.push(&active);
            }
            let bindings = after_registry.plan_binding_transitions(terminal, [])?;
            after_registry
                .apply_binding_transition(bindings)
                .expect("fresh replacement bindings");
            after_registry.validate_order_bindings(
                after_book
                    .reservations
                    .iter()
                    .map(|(id, state)| (*id, state)),
            )?;
            let mut events = vec![EngineEvent::OrderCanceled {
                order_id: request.old_order_id,
                remaining: OrderRemaining::Base(Qty::try_new(old_remaining)?),
                reason: CancelReason::CancelReplace,
            }];
            events.extend(matching_events);
            return Ok(ProductionCancelReplacePlan {
                before: ProductionSnapshot::capture(self),
                before_registry: registry.clone(),
                after_registry,
                commit: ProductionReplaceCommit::Stp {
                    after_book: Box::new(after_book),
                    events,
                },
            });
        }
        let fills = view.plan_limit_fills(&accepted, context.math, flags.post_only)?;
        if tif == TimeInForce::Fok && fills.base_remaining != 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        assert!(
            context.trade_ids.len() >= fills.fills.len(),
            "insufficient TradeIds before CancelReplace commit"
        );
        let residual =
            if fills.base_remaining > 0 && !matches!(tif, TimeInForce::Ioc | TimeInForce::Fok) {
                let mut visible = accepted.clone();
                visible.qty = Qty::try_new(
                    peak.map_or(fills.base_remaining, |p| p.get().min(fills.base_remaining)),
                )?;
                Some(
                    view.plan_rest(
                        &visible,
                        fills.next_priority,
                        fills
                            .fills
                            .iter()
                            .any(|fill| fill.terminal_maker().is_some()),
                    )?,
                )
            } else {
                None
            };
        // 普通旧单与全成挂单按真实提交次序释放槽位；补量始终立即复用自己的槽位。
        // 余量只消费释放栈最后一个位置，否则追加到现有槽位数组尾部。
        let residual_slot = if residual.is_some() {
            let (slots, mut free) = self.arena.state_snapshot();
            if let OldLocation::Regular(index) = old_location {
                free.push(index);
            }
            for fill in &fills.fills {
                if let Some(id) = fill.terminal_maker() {
                    free.push(self.orders[&id]);
                }
            }
            Some(match free.last() {
                Some(index) => *index,
                None => OrderIndex::try_from_usize(slots.len())?,
            })
        } else {
            None
        };

        // 按片成交顺序收集终结集合；每张全成挂单只在最后一片出现一次。
        // 挂单影子属于完整簿预检，必须在新单资金过渡校验之前确认。
        let mut terminal = Vec::new();
        let mut terminal_ids = Vec::new();
        for fill in &fills.fills {
            if let Some(id) = fill.terminal_maker() {
                terminal.push(
                    self.reservations
                        .get(&id)
                        .ok_or(RejectReason::InvalidReservation)?,
                );
                terminal_ids.push(id);
            }
        }
        let transition = registry
            .plan_transition(keys, expected, &request.reservation_transition)
            .map_err(|r| r.reason)?;
        let active_reservation = transition.active_state().clone();
        let mut after_registry = registry.clone();
        after_registry
            .apply_transition(transition)
            .expect("fresh production reservation transition");
        if residual.is_none() {
            terminal.push(&active_reservation);
        }
        let bindings = after_registry.plan_binding_transitions(terminal, [])?;
        after_registry
            .apply_binding_transition(bindings)
            .expect("fresh production terminal binding batch");
        after_registry.validate_order_bindings(
            self.reservations
                .iter()
                .filter(|(id, _)| **id != request.old_order_id && !terminal_ids.contains(id))
                .map(|(id, state)| (*id, state))
                .chain(residual.map(|_| (accepted.order_id, &active_reservation))),
        )?;
        let trade_ids = context.trade_ids[..fills.fills.len()].to_vec();
        let reference_after = self
            .reference_prices
            .plan_trade_prices(fills.fills.iter().map(|fill| fill.price))?;
        Ok(ProductionCancelReplacePlan {
            before: ProductionSnapshot::capture(self),
            before_registry: registry.clone(),
            after_registry,
            commit: ProductionReplaceCommit::Standard(Box::new(ProductionStandardReplace {
                old_id: request.old_order_id,
                old_location,
                old_remaining: Qty::try_new(old_remaining)?,
                accepted,
                peak,
                tif,
                fills,
                trade_ids,
                residual,
                residual_slot,
                active_reservation,
                reference_after,
            })),
        })
    }
}
