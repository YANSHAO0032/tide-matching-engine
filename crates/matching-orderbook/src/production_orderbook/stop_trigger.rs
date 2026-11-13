//! 生产订单簿的参考价跨越、Trigger-R1 与止损 child 级联。

use super::ProductionOrderBook;
use matching_domain::{
    EngineEvent, LogicalTime, MarketByBase, MarketByQuote, MarketMath, PriceReference,
    PriceReferenceKind, RejectReason, ReservationReleaseReason, Side, StopMarketQuantity,
    StopOrder, StopPriority, TimeInForce, TradeId,
};
use matching_risk::{ReservationApplyError, ReservationRegistry, ReservationState};
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};

/// 已移出触发簿的订单及其待接续预留；只有实际挂入普通簿才重新关联预留。
pub(crate) type TriggeredStop = (
    StopOrder,
    StopPriority,
    EngineEvent,
    Option<ReservationState>,
);

/// Stop 级联及终结预留归属的不可伪造生产计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionTriggerRegistryPlan {
    /// 预演前的完整订单簿。
    before_book: ProductionOrderBook,
    /// 预演前的完整预留登记表。
    before_registry: ReservationRegistry,
    /// 已完成触发级联的生产簿后态。
    after_book: ProductionOrderBook,
    /// 已处理订单终结归属的登记表后态。
    after_registry: ReservationRegistry,
    /// 原序触发、成交与终结事件；真实 R2 候选还包含结算和释放。
    events: Vec<EngineEvent>,
}

/// 将结算预检错误映射到既有稳定拒绝原因。
fn stop_settlement_error(error: matching_risk::SettlementPlanError) -> RejectReason {
    use matching_risk::SettlementPlanError;
    match error {
        SettlementPlanError::Reservation(reason) | SettlementPlanError::FeeSchedule(reason) => {
            reason
        }
        SettlementPlanError::Overflow
        | SettlementPlanError::AssetScale(matching_risk::AssetScaleError::Overflow) => {
            RejectReason::ArithmeticOverflow
        }
        SettlementPlanError::Math(error) => error
            .to_reject_reason()
            .unwrap_or(RejectReason::InvalidReservation),
        _ => RejectReason::InvalidReservation,
    }
}

/// 只识别能证明订单终结的事件及对应的稳定释放原因。
fn stop_terminal_event(
    event: &EngineEvent,
) -> Result<Option<(matching_domain::OrderId, ReservationReleaseReason)>, RejectReason> {
    use matching_domain::CancelReason;
    Ok(match event {
        EngineEvent::TriggeredOrderRejected { order_id, .. } => {
            Some((*order_id, ReservationReleaseReason::TriggerRejected))
        }
        EngineEvent::OrderCanceled {
            order_id, reason, ..
        } => Some((
            *order_id,
            match reason {
                CancelReason::SelfTradePreventionMaker
                | CancelReason::SelfTradePreventionTaker
                | CancelReason::SelfTradePreventionBoth => {
                    ReservationReleaseReason::SelfTradePrevention
                }
                CancelReason::IocRemainder => ReservationReleaseReason::IocRemainder,
                _ => return Err(RejectReason::InvalidOrderCombination),
            },
        )),
        EngineEvent::MarketProtectionTriggered { order_id, .. } => {
            Some((*order_id, ReservationReleaseReason::MarketProtection))
        }
        EngineEvent::OrderFilled { order_id } => {
            Some((*order_id, ReservationReleaseReason::FilledRemainder))
        }
        _ => None,
    })
}

/// 对已经从簿中移除且等待 R2 的单个预留，原子修改候选并输出真实剩余。
fn release_pending_stop(
    plan: &mut ProductionTriggerRegistryPlan,
    identities: &BTreeMap<matching_domain::OrderId, matching_domain::ReservationId>,
    id: matching_domain::OrderId,
    reason: ReservationReleaseReason,
    output: &mut Vec<EngineEvent>,
) -> Result<(), RejectReason> {
    let reservation = *identities
        .get(&id)
        .ok_or(RejectReason::InvalidReservation)?;
    if !plan.after_registry.is_pending_r2(reservation) {
        return Ok(());
    }
    let r2 = plan.after_registry.plan_r2(&[], &[reservation])?;
    let book = plan.after_book.plan_r2_shadow_update(&r2)?;
    let releases = r2.releases().to_vec();
    plan.after_registry
        .apply_r2(r2)
        .map_err(|_| RejectReason::InvalidReservation)?;
    plan.after_book = book;
    output.extend(releases.into_iter().map(|release| release.to_event(reason)));
    Ok(())
}

/// 市价 child 无成交且未生成其他终结事件时，在下一次触发前释放其剩余。
fn flush_untraded_stops(
    plan: &mut ProductionTriggerRegistryPlan,
    identities: &BTreeMap<matching_domain::OrderId, matching_domain::ReservationId>,
    last_trade: &BTreeMap<matching_domain::OrderId, usize>,
    terminal_event: &BTreeMap<matching_domain::OrderId, (usize, ReservationReleaseReason)>,
    triggered: &mut BTreeSet<matching_domain::OrderId>,
    output: &mut Vec<EngineEvent>,
) -> Result<(), RejectReason> {
    let ids: Vec<_> = triggered.iter().copied().collect();
    for id in ids {
        if last_trade.contains_key(&id) || terminal_event.contains_key(&id) {
            continue;
        }
        let reservation = *identities
            .get(&id)
            .ok_or(RejectReason::InvalidReservation)?;
        if plan.after_registry.is_pending_r2(reservation) {
            if !matches!(
                plan.before_book.stop_order(id),
                Some((StopOrder::Market(_), _))
            ) {
                return Err(RejectReason::InvalidReservation);
            }
            release_pending_stop(
                plan,
                identities,
                id,
                ReservationReleaseReason::IocRemainder,
                output,
            )?;
        }
        triggered.remove(&id);
    }
    Ok(())
}

impl ProductionOrderBook {
    /// 在同一隔离候选中预演止损级联、成交结算和终结预留释放。
    /// 调用方仍须提供完整 Trigger-R1、预分配且全局唯一的 `trade_ids`，
    /// 并在持久提交后才发布事件。
    pub fn plan_trigger_stops_with_r2(
        &self,
        reference: PriceReference,
        context: crate::TriggerRegistryContext<'_>,
        settlement: matching_risk::TradeSettlementContext<'_>,
        registry: &ReservationRegistry,
        trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<ProductionTriggerRegistryPlan, RejectReason> {
        if self.market_id() != Some(settlement.market_id) || *context.math != settlement.math {
            return Err(RejectReason::InvalidReservation);
        }
        let mut plan =
            self.plan_trigger_stops_with_registry(reference, context, registry, trigger_r1)?;
        let identities: BTreeMap<_, _> = self
            .reservation_snapshots()
            .into_iter()
            .map(|(order, state)| (order, state.reservation_id))
            .collect();
        let mut last_trade = BTreeMap::new();
        let mut terminal_event = BTreeMap::new();
        let mut seen_trades = BTreeSet::new();
        for (index, event) in plan.events.iter().enumerate() {
            if let EngineEvent::Trade(trade) = event {
                if !seen_trades.insert(trade.trade_id) {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                last_trade.insert(trade.maker_order_id, index);
                last_trade.insert(trade.taker_order_id, index);
            }
            if let Some((id, reason)) = stop_terminal_event(event)? {
                terminal_event.entry(id).or_insert((index, reason));
            }
        }
        let mut deferred: BTreeMap<_, Vec<EngineEvent>> = BTreeMap::new();
        let mut triggered = BTreeSet::new();
        let mut output = Vec::new();
        for (index, event) in std::mem::take(&mut plan.events).into_iter().enumerate() {
            if let EngineEvent::StopTriggered { order_id, .. } = &event {
                flush_untraded_stops(
                    &mut plan,
                    &identities,
                    &last_trade,
                    &terminal_event,
                    &mut triggered,
                    &mut output,
                )?;
                triggered.insert(*order_id);
            }
            if let EngineEvent::Trade(trade) = &event {
                let maker = *identities
                    .get(&trade.maker_order_id)
                    .ok_or(RejectReason::InvalidReservation)?;
                let taker = *identities
                    .get(&trade.taker_order_id)
                    .ok_or(RejectReason::InvalidReservation)?;
                let mut terminal = Vec::new();
                for (id, reservation) in
                    [(trade.maker_order_id, maker), (trade.taker_order_id, taker)]
                {
                    if last_trade.get(&id) == Some(&index)
                        && plan.after_registry.is_pending_r2(reservation)
                    {
                        terminal.push(reservation);
                    }
                }
                let trade_context = matching_risk::TradeSettlementContext {
                    market_id: settlement.market_id,
                    math: settlement.math,
                    base: settlement.base,
                    quote: settlement.quote,
                    fees: settlement.fees,
                };
                let candidate = plan
                    .after_book
                    .plan_trade_r2_candidate(
                        &plan.after_registry,
                        trade_context,
                        trade,
                        maker,
                        taker,
                        &terminal,
                    )
                    .map_err(stop_settlement_error)?;
                let (delta, releases) = plan
                    .after_book
                    .apply_trade_r2_candidate(&mut plan.after_registry, candidate)
                    .map_err(|_| RejectReason::InvalidReservation)?;
                output.push(event.clone());
                output.push(EngineEvent::SettlementDelta(delta.to_event_payload()));
                for release in releases {
                    let (later, reason) = terminal_event
                        .get(&release.order_id)
                        .copied()
                        .filter(|(position, _)| *position > index)
                        .map_or(
                            (false, ReservationReleaseReason::FilledRemainder),
                            |(_, reason)| (true, reason),
                        );
                    let id = release.order_id;
                    let event = release.to_event(reason);
                    if later {
                        deferred.entry(id).or_default().push(event);
                    } else {
                        output.push(event);
                    }
                }
                continue;
            }
            let terminal = stop_terminal_event(&event)?;
            output.push(event);
            if let Some((id, reason)) = terminal {
                release_pending_stop(&mut plan, &identities, id, reason, &mut output)?;
                if let Some(events) = deferred.remove(&id) {
                    output.extend(events);
                }
                triggered.remove(&id);
            }
        }
        flush_untraded_stops(
            &mut plan,
            &identities,
            &last_trade,
            &terminal_event,
            &mut triggered,
            &mut output,
        )?;
        if !deferred.is_empty() {
            return Err(RejectReason::InvalidReservation);
        }
        for (id, reservation) in identities {
            if registry.is_pending_r2(reservation) {
                continue;
            }
            if plan.after_registry.is_pending_r2(reservation)
                && plan.after_book.reservation_state(id).is_none()
            {
                return Err(RejectReason::InvalidReservation);
            }
        }
        plan.events = output;
        Ok(plan)
    }
    /// 在副本上完成 Stop 级联并预检所有终结订单的 PendingR2 归属。
    pub fn plan_trigger_stops_with_registry(
        &self,
        reference: PriceReference,
        context: crate::TriggerRegistryContext<'_>,
        registry: &ReservationRegistry,
        trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<ProductionTriggerRegistryPlan, RejectReason> {
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )?;
        let mut after_book = self.clone();
        let mut events = after_book.trigger_stops(
            reference,
            context.now,
            context.current_session_id,
            context.math,
            context.trade_ids,
            trigger_r1,
        )?;
        // 登记表边界只迁移归属，不执行真实 R2；旧窄接口的释放投影不能当成金融事实。
        events.retain(|event| !matches!(event, EngineEvent::ReservationReleased { .. }));
        let mut terminal = self
            .reservations
            .iter()
            .filter(|(id, _)| !after_book.reservations.contains_key(id))
            .collect::<Vec<_>>();
        terminal.sort_unstable_by_key(|(id, _)| **id);
        let transition =
            registry.plan_binding_transitions(terminal.iter().map(|(_, state)| *state), [])?;
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh trigger binding plan");
        self.validate_registry_bindings(
            &after_registry,
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ProductionTriggerRegistryPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }

    /// 校验完整前态后一次安装触发级联及其预留归属。
    pub fn apply_trigger_stops_with_registry(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ProductionTriggerRegistryPlan,
    ) -> Result<Vec<EngineEvent>, ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok(plan.events)
    }
    /// 收集一次价格跨越触发的止损订单，先终结其 StopBook 身份。
    ///
    /// 首个有效参考值只建立基线；价格上涨只按低到高收集买方 Stop，价格下跌只按
    /// 高到低收集卖方 Stop。静态开市参考不能驱动 Stop，乱序来源序号也不会改簿。
    pub(crate) fn collect_triggered_stops(
        &mut self,
        reference: PriceReference,
    ) -> Result<Vec<TriggeredStop>, RejectReason> {
        if reference.kind == PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        let Some(previous) = self.reference_prices.advance(reference)? else {
            return Ok(Vec::new());
        };
        let side = if reference.price > previous {
            Side::Buy
        } else if reference.price < previous {
            Side::Sell
        } else {
            return Ok(Vec::new());
        };
        let triggered =
            self.stop_book
                .take_triggered_crossing(reference.kind, side, previous, reference.price);
        Ok(triggered
            .into_iter()
            .map(|(order, priority)| {
                let event = EngineEvent::StopTriggered {
                    order_id: order.order_id(),
                    child_kind: order.child_kind(),
                };
                let reservation = self.reservations.remove(&order.order_id());
                (order, priority, event, reservation)
            })
            .collect())
    }

    /// 依次执行一条参考更新及其成交引起的 Stop child 级联。
    ///
    /// 调用方提供的 Trigger-R1 在任何 child 撮合前运行，并只能依赖本地、可重放
    /// 的预留、费率和保护状态；拒绝时 stop 已终结，随后仅产生拒绝与预留释放事件。
    /// 逻辑时间和当前时段必须由调用方显式传入，不从来源序号或审计时间推导。
    /// 本入口的 root 为参考更新，保护使用该次新参考；整条级联的后代都使用同一上下文。
    pub fn trigger_stops(
        &mut self,
        reference: PriceReference,
        now: LogicalTime,
        current_session_id: u64,
        math: &MarketMath,
        trade_ids: &[TradeId],
        mut trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if let Some((_, configured_math)) = self.price_band {
            assert_eq!(&configured_math, math, "市场数值配置不一致");
        }
        self.preflight_last_trade_sequence_capacity(reference, trade_ids.len())?;
        let band = self
            .price_band
            .map(|(config, configured_math)| {
                let price = if config.reference_kind == reference.kind {
                    Some(reference.price)
                } else {
                    self.reference_prices
                        .latest(config.reference_kind)
                        .map(|r| r.price)
                }
                .ok_or(RejectReason::ReferencePriceUnavailable)?;
                config.bounds(price, configured_math.tick_size())
            })
            .transpose();
        // 参考更新是本次 root 的显式输入：使用新值一次冻结整个级联的保护上下文。
        let protection_context = matching_domain::market_order::CommandStartProtectionContext {
            reference_price_at_command_start: reference.price,
            market_band_worst: None,
        };
        let mut pending = VecDeque::from(self.collect_triggered_stops(reference)?);
        let mut events = Vec::new();
        let mut used_trade_ids = 0usize;

        while let Some((order, _priority, triggered, reservation)) = pending.pop_front() {
            events.push(triggered);
            let validation = self
                .validate_triggered_child_tif(&order, now, current_session_id)
                .and_then(|()| band.as_ref().map(|_| ()).map_err(|reason| *reason))
                .and_then(|()| match (&order, &band) {
                    (StopOrder::Limit(stop), Ok(Some((lower, upper))))
                        if stop.price < *lower || stop.price > *upper =>
                    {
                        Err(RejectReason::PriceBandExceeded)
                    }
                    _ => Ok(()),
                })
                .and_then(|()| trigger_r1(&order));
            if let Err(reason) = validation {
                Self::append_trigger_rejection(&mut events, &order, reason);
                continue;
            }

            let mut child_context = protection_context.clone();
            child_context.market_band_worst = band.as_ref().ok().and_then(|bounds| {
                bounds.map(|(lower, upper)| match &order {
                    StopOrder::Limit(stop) => {
                        if stop.side == matching_domain::Side::Buy {
                            upper
                        } else {
                            lower
                        }
                    }
                    StopOrder::Market(stop) => {
                        if stop.side == matching_domain::Side::Buy {
                            upper
                        } else {
                            lower
                        }
                    }
                })
            });
            let child_events = self.execute_triggered_child(
                &order,
                math,
                &trade_ids[used_trade_ids..],
                now,
                current_session_id,
                (&child_context, band.as_ref().ok().copied().flatten()),
            );
            let mut child_events = match child_events {
                Ok(events) => events,
                Err(reason) => {
                    Self::append_trigger_rejection(&mut events, &order, reason);
                    continue;
                }
            };
            // 触发身份已终结；只有实际挂入普通簿的 child 接续同一预留和已提交版本。
            if self.order(order.order_id()).is_some() {
                if let StopOrder::Limit(stop) = &order {
                    let index = self.orders[&stop.order_id];
                    let node = self.arena.get_mut(index).expect("resting child slot");
                    let mut original = node.original_order().clone();
                    original.qty = stop.qty;
                    // 保留 child 已接受的绝对总量，避免后续改单把成交量误计为零。
                    node.amend_regular_in_place(
                        original,
                        matching_domain::Qty::try_new(node.total_remaining())
                            .expect("positive child residual"),
                        node.time_in_force(),
                    );
                }
                if let Some(reservation) = reservation {
                    assert!(
                        self.reservations
                            .insert(order.order_id(), reservation)
                            .is_none()
                    );
                }
            }
            used_trade_ids = used_trade_ids
                .checked_add(
                    child_events
                        .iter()
                        .filter(|event| matches!(event, EngineEvent::Trade(_)))
                        .count(),
                )
                .ok_or(RejectReason::ArithmeticOverflow)?;

            for trade in child_events.iter().filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade),
                _ => None,
            }) {
                let last_trade = self.reference_prices.next_last_trade(trade.price)?;
                // 成交仍更新 LastTrade 并驱动来源匹配与跨越，不能重写 root 保护参考。
                pending.extend(self.collect_triggered_stops(last_trade)?);
            }
            events.append(&mut child_events);
        }
        Ok(events)
    }

    /// 先在参考快照上应用 root，确保后代成交序号在首次真实写入前已有容量。
    fn preflight_last_trade_sequence_capacity(
        &self,
        reference: PriceReference,
        trade_id_capacity: usize,
    ) -> Result<(), RejectReason> {
        if reference.kind == PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        let mut planned_references = self.reference_prices.clone();
        planned_references.advance(reference)?;
        let capacity =
            u64::try_from(trade_id_capacity).map_err(|_| RejectReason::ArithmeticOverflow)?;
        if let Some(last_trade) = planned_references.latest(PriceReferenceKind::LastTrade) {
            last_trade
                .source_seq
                .checked_add(capacity)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        Ok(())
    }

    /// 根据调用方显式逻辑时间和时段重校验 child 有效期。
    fn validate_triggered_child_tif(
        &self,
        order: &StopOrder,
        now: LogicalTime,
        session: u64,
    ) -> Result<(), RejectReason> {
        match order {
            StopOrder::Limit(child) => match child.child_tif {
                TimeInForce::Gtc => Ok(()),
                TimeInForce::Gtd { expire_at } if expire_at > now => Ok(()),
                TimeInForce::Gtd { .. } => Err(RejectReason::InvalidExpireTime),
                TimeInForce::Day { session_id } if session_id == session => Ok(()),
                TimeInForce::Day { .. } => Err(RejectReason::InvalidSession),
                _ => Err(RejectReason::InvalidOrderCombination),
            },
            StopOrder::Market(child) if child.child_tif == TimeInForce::Ioc => Ok(()),
            _ => Err(RejectReason::InvalidOrderCombination),
        }
    }

    /// 执行已通过 Trigger-R1 的 child，并保留底层产生的原序事件。
    fn execute_triggered_child(
        &mut self,
        order: &StopOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
        now: LogicalTime,
        session: u64,
        protection: (
            &matching_domain::market_order::CommandStartProtectionContext,
            Option<(matching_domain::Price, matching_domain::Price)>,
        ),
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let (context, frozen_band) = protection;
        match order {
            StopOrder::Limit(stop) => self.place_limit_timed_frozen(
                matching_domain::LimitOrder {
                    order_id: stop.order_id,
                    user_id: stop.user_id,
                    stp_key: stop.stp_key,
                    stp_mode: stop.stp_mode,
                    side: stop.side,
                    price: stop.price,
                    qty: stop.qty,
                },
                stop.child_tif,
                now,
                session,
                trade_ids,
                Some(frozen_band),
            ),
            StopOrder::Market(stop) => match stop.quantity {
                StopMarketQuantity::Base(qty) => {
                    let incoming = MarketByBase {
                        order_id: stop.order_id,
                        user_id: stop.user_id,
                        stp_key: stop.stp_key,
                        stp_mode: stop.stp_mode,
                        side: stop.side,
                        qty,
                        market_protection: stop.market_protection.clone(),
                    };
                    if stop.market_protection.is_some()
                        || context.market_band_worst.is_some()
                        || stop.stp_mode != matching_domain::StpMode::None
                    {
                        let unprotected =
                            matching_domain::market_order::CommandStartProtectionContext {
                                reference_price_at_command_start: context
                                    .reference_price_at_command_start,
                                market_band_worst: None,
                            };
                        let context = if stop.market_protection.is_some()
                            || context.market_band_worst.is_some()
                        {
                            context
                        } else {
                            &unprotected
                        };
                        self.execute_market_by_base_with_frozen_protection(
                            incoming, context, math, trade_ids,
                        )
                    } else {
                        self.execute_market_by_base_inner(incoming, trade_ids, math)
                            .map(|trades| trades.into_iter().map(EngineEvent::Trade).collect())
                    }
                }
                StopMarketQuantity::Quote(quote_budget) => {
                    let incoming = MarketByQuote {
                        order_id: stop.order_id,
                        user_id: stop.user_id,
                        stp_key: stop.stp_key,
                        stp_mode: stop.stp_mode,
                        side: stop.side,
                        quote_budget,
                        market_protection: stop.market_protection.clone(),
                    };
                    if stop.market_protection.is_some()
                        || context.market_band_worst.is_some()
                        || stop.stp_mode != matching_domain::StpMode::None
                    {
                        let unprotected =
                            matching_domain::market_order::CommandStartProtectionContext {
                                reference_price_at_command_start: context
                                    .reference_price_at_command_start,
                                market_band_worst: None,
                            };
                        let context = if stop.market_protection.is_some()
                            || context.market_band_worst.is_some()
                        {
                            context
                        } else {
                            &unprotected
                        };
                        self.execute_market_by_quote_with_frozen_protection(
                            incoming, context, math, trade_ids,
                        )
                        .map(|execution| execution.events().to_vec())
                    } else {
                        self.execute_market_by_quote_inner(incoming, math, trade_ids)
                            .map(|execution| {
                                execution
                                    .trades()
                                    .iter()
                                    .cloned()
                                    .map(EngineEvent::Trade)
                                    .collect()
                            })
                    }
                }
            },
        }
    }

    /// 将 Trigger-R1 失败转换为不可再交易的固定事件链。
    fn append_trigger_rejection(
        events: &mut Vec<EngineEvent>,
        order: &StopOrder,
        reason: RejectReason,
    ) {
        events.push(EngineEvent::TriggeredOrderRejected {
            order_id: order.order_id(),
            reason,
        });
        events.push(EngineEvent::ReservationReleased {
            order_id: order.order_id(),
            reservation_id: order.reservation_id(),
            reason: ReservationReleaseReason::TriggerRejected,
            released: None,
        });
    }

    /// 在根命令候选状态中执行全部止损后代；任何游标溢出均丢弃候选。
    pub fn trigger_stops_with_cursor(
        &mut self,
        reference: PriceReference,
        now: LogicalTime,
        current_session_id: u64,
        math: &MarketMath,
        cursor: &mut matching_domain::TradeIdCursor,
        trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let mut candidate = self.clone();
        let mut source = crate::trade_id_source::TradeIdSource::cursor(cursor.clone());
        let events = candidate.trigger_stops_cursor_inner(
            reference,
            now,
            current_session_id,
            math,
            &mut source,
            trigger_r1,
        )?;
        *cursor = source.into_cursor().expect("命令游标来源必须存在");
        *self = candidate;
        Ok(events)
    }

    /// 以根命令共享来源执行已校验的触发子单。
    fn execute_triggered_child_cursor(
        &mut self,
        order: &StopOrder,
        math: &MarketMath,
        source: &mut crate::trade_id_source::TradeIdSource<'_>,
        now: LogicalTime,
        session: u64,
        protection: (
            &matching_domain::market_order::CommandStartProtectionContext,
            Option<(matching_domain::Price, matching_domain::Price)>,
        ),
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let (context, frozen_band) = protection;
        match order {
            StopOrder::Limit(stop) => {
                let order = matching_domain::LimitOrder {
                    order_id: stop.order_id,
                    user_id: stop.user_id,
                    stp_key: stop.stp_key,
                    stp_mode: stop.stp_mode,
                    side: stop.side,
                    price: stop.price,
                    qty: stop.qty,
                };
                order.validate_stp_mode()?;
                if self.contains_active_order(order.order_id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                Self::validate_resting_tif(stop.child_tif, now, session)?;
                if let Some((low, high)) = frozen_band
                    && (order.price < low || order.price > high)
                {
                    return Err(RejectReason::PriceBandExceeded);
                }
                let id = order.order_id;
                let (events, _) =
                    self.execute_limit_stp_with_source(order, source, None, None, false)?;
                if let Some(index) = self.orders.get(&id).copied() {
                    self.arena
                        .get_mut(index)
                        .expect("定时余量节点必须存在")
                        .replace_initial_gtc_with_tif(stop.child_tif);
                    self.regular_expiry.insert(id, stop.child_tif);
                }
                Ok(events)
            }
            StopOrder::Market(stop) => {
                let boundary = if stop.market_protection.is_some()
                    || context.market_band_worst.is_some()
                {
                    matching_domain::market_order::effective_protection_at_command_start_with_source(stop.market_protection.as_ref(), context, stop.side, math)?
                } else {
                    None
                };
                let trades_only = stop.market_protection.is_none()
                    && context.market_band_worst.is_none()
                    && stop.stp_mode == matching_domain::StpMode::None;
                let events = match stop.quantity {
                    StopMarketQuantity::Base(qty) => {
                        let incoming = MarketByBase {
                            order_id: stop.order_id,
                            user_id: stop.user_id,
                            stp_key: stop.stp_key,
                            stp_mode: stop.stp_mode,
                            side: stop.side,
                            qty,
                            market_protection: stop.market_protection.clone(),
                        };
                        incoming.validate_stp_mode()?;
                        self.execute_market_base_with_boundary_source(
                            incoming,
                            Some(math),
                            boundary,
                            source,
                        )
                    }
                    StopMarketQuantity::Quote(quote_budget) => {
                        let incoming = MarketByQuote {
                            order_id: stop.order_id,
                            user_id: stop.user_id,
                            stp_key: stop.stp_key,
                            stp_mode: stop.stp_mode,
                            side: stop.side,
                            quote_budget,
                            market_protection: stop.market_protection.clone(),
                        };
                        incoming.validate_stp_mode()?;
                        self.execute_market_quote_with_boundary_source(
                            incoming, math, boundary, source,
                        )
                        .map(|execution| execution.events().to_vec())
                    }
                }?;
                if trades_only {
                    Ok(events
                        .into_iter()
                        .filter(|event| matches!(event, EngineEvent::Trade(_)))
                        .collect())
                } else {
                    Ok(events)
                }
            }
        }
    }

    fn trigger_stops_cursor_inner(
        &mut self,
        reference: PriceReference,
        now: LogicalTime,
        current_session_id: u64,
        math: &MarketMath,
        source: &mut crate::trade_id_source::TradeIdSource<'_>,
        mut trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if let Some((_, configured_math)) = self.price_band {
            assert_eq!(&configured_math, math, "市场数值配置不一致");
        }
        if reference.kind == PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        let band = self
            .price_band
            .map(|(config, configured_math)| {
                let price = if config.reference_kind == reference.kind {
                    Some(reference.price)
                } else {
                    self.reference_prices
                        .latest(config.reference_kind)
                        .map(|r| r.price)
                }
                .ok_or(RejectReason::ReferencePriceUnavailable)?;
                config.bounds(price, configured_math.tick_size())
            })
            .transpose();
        // 参考更新是本次 root 的显式输入：使用新值一次冻结整个级联的保护上下文。
        let protection_context = matching_domain::market_order::CommandStartProtectionContext {
            reference_price_at_command_start: reference.price,
            market_band_worst: None,
        };
        let mut pending = VecDeque::from(self.collect_triggered_stops(reference)?);
        let mut events = Vec::new();

        while let Some((order, _priority, triggered, reservation)) = pending.pop_front() {
            events.push(triggered);
            let validation = self
                .validate_triggered_child_tif(&order, now, current_session_id)
                .and_then(|()| band.as_ref().map(|_| ()).map_err(|reason| *reason))
                .and_then(|()| match (&order, &band) {
                    (StopOrder::Limit(stop), Ok(Some((lower, upper))))
                        if stop.price < *lower || stop.price > *upper =>
                    {
                        Err(RejectReason::PriceBandExceeded)
                    }
                    _ => Ok(()),
                })
                .and_then(|()| trigger_r1(&order));
            if let Err(reason) = validation {
                Self::append_trigger_rejection(&mut events, &order, reason);
                continue;
            }

            let mut child_context = protection_context.clone();
            child_context.market_band_worst = band.as_ref().ok().and_then(|bounds| {
                bounds.map(|(lower, upper)| match &order {
                    StopOrder::Limit(stop) => {
                        if stop.side == matching_domain::Side::Buy {
                            upper
                        } else {
                            lower
                        }
                    }
                    StopOrder::Market(stop) => {
                        if stop.side == matching_domain::Side::Buy {
                            upper
                        } else {
                            lower
                        }
                    }
                })
            });
            let mut child_source = source.clone();
            let child_events = self.execute_triggered_child_cursor(
                &order,
                math,
                &mut child_source,
                now,
                current_session_id,
                (&child_context, band.as_ref().ok().copied().flatten()),
            );
            let mut child_events = match child_events {
                Ok(events) => {
                    *source = child_source;
                    events
                }
                Err(reason) => {
                    if child_source.cursor_exhausted() {
                        return Err(reason);
                    }
                    Self::append_trigger_rejection(&mut events, &order, reason);
                    continue;
                }
            };
            // 触发身份已终结；只有实际挂入普通簿的 child 接续同一预留和已提交版本。
            if self.order(order.order_id()).is_some() {
                if let StopOrder::Limit(stop) = &order {
                    let index = self.orders[&stop.order_id];
                    let node = self.arena.get_mut(index).expect("resting child slot");
                    let mut original = node.original_order().clone();
                    original.qty = stop.qty;
                    // 保留 child 已接受的绝对总量，避免后续改单把成交量误计为零。
                    node.amend_regular_in_place(
                        original,
                        matching_domain::Qty::try_new(node.total_remaining())
                            .expect("positive child residual"),
                        node.time_in_force(),
                    );
                }
                if let Some(reservation) = reservation {
                    assert!(
                        self.reservations
                            .insert(order.order_id(), reservation)
                            .is_none()
                    );
                }
            }
            for trade in child_events.iter().filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade),
                _ => None,
            }) {
                let last_trade = self.reference_prices.next_last_trade(trade.price)?;
                // 成交仍更新 LastTrade 并驱动来源匹配与跨越，不能重写 root 保护参考。
                pending.extend(self.collect_triggered_stops(last_trade)?);
            }
            events.append(&mut child_events);
        }
        Ok(events)
    }

    /// 在候选订单簿、登记表及游标中预演完整根级联，仅成功后提交。
    pub fn trigger_stops_with_registry_and_cursor(
        &mut self,
        reference: PriceReference,
        context: crate::TriggerRegistryContext<'_>,
        registry: &mut ReservationRegistry,
        cursor: &mut matching_domain::TradeIdCursor,
        trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )?;
        let mut candidate_book = self.clone();
        let mut candidate_cursor = cursor.clone();
        let mut events = candidate_book.trigger_stops_with_cursor(
            reference,
            context.now,
            context.current_session_id,
            context.math,
            &mut candidate_cursor,
            trigger_r1,
        )?;
        events.retain(|event| !matches!(event, EngineEvent::ReservationReleased { .. }));
        let mut terminal = self
            .reservations
            .iter()
            .filter(|(id, _)| !candidate_book.reservations.contains_key(id))
            .collect::<Vec<_>>();
        terminal.sort_unstable_by_key(|(id, _)| **id);
        let transition =
            registry.plan_binding_transitions(terminal.iter().map(|(_, state)| *state), [])?;
        let mut candidate_registry = registry.clone();
        candidate_registry
            .apply_binding_transition(transition)
            .expect("fresh trigger binding plan");
        self.validate_registry_bindings(
            &candidate_registry,
            candidate_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        *self = candidate_book;
        *registry = candidate_registry;
        *cursor = candidate_cursor;
        Ok(events)
    }
}
