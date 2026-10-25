//! 独立 Vec 参考簿的价格跨越、Trigger-R1 与止损 child 级联。

use super::ReferenceOrderBook;
use matching_domain::{
    EngineEvent, LogicalTime, MarketByBase, MarketByQuote, MarketMath, PriceReference,
    PriceReferenceKind, RejectReason, ReservationReleaseReason, Side, StopMarketQuantity,
    StopOrder, StopPriority, TimeInForce, TradeId,
};
use matching_risk::ReservationState;
use std::collections::VecDeque;

/// 已移出触发簿的订单及其待接续预留；只有实际挂入普通簿才重新关联预留。
pub(crate) type TriggeredStop = (
    StopOrder,
    StopPriority,
    EngineEvent,
    Option<ReservationState>,
);

impl ReferenceOrderBook {
    /// 按一次真实参考价跨越移除止损订单，并产生确定性触发事件。
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
        let ids = self
            .stop_orders(side)
            .into_iter()
            .filter(|(order, _)| order.trigger_reference() == reference.kind)
            .filter(|(order, _)| match side {
                Side::Buy => previous < order.stop_price() && order.stop_price() <= reference.price,
                Side::Sell => {
                    reference.price <= order.stop_price() && order.stop_price() < previous
                }
            })
            .map(|(order, _)| order.order_id())
            .collect::<Vec<_>>();
        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let reservation = self.reservations.get(&id).cloned();
                self.cancel_stop(id)
                    .map(|(order, priority)| (order, priority, reservation))
            })
            .map(|(order, priority, reservation)| {
                let event = EngineEvent::StopTriggered {
                    order_id: order.order_id(),
                    child_kind: order.child_kind(),
                };
                (order, priority, event, reservation)
            })
            .collect())
    }

    /// 以独立 Vec 参考模型执行一条参考更新及其 Stop child 级联。
    ///
    /// 触发后的每个 child 都先调用本地 Trigger-R1；失败不会调用任一撮合入口，
    /// 且只写入 Stop 终结所需的拒绝与预留释放事件。
    /// 逻辑时间和当前时段由调用方显式提供，不根据来源序号或审计时间猜测。
    /// 本入口的 root 为参考更新，一次冻结更新后参考，全部后代沿用同一保护上下文。
    pub fn trigger_stops(
        &mut self,
        reference: PriceReference,
        now: LogicalTime,
        current_session_id: u64,
        math: &MarketMath,
        trade_ids: &[TradeId],
        mut trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.preflight_last_trade_sequence_capacity(reference, trade_ids.len())?;
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
                .and_then(|()| trigger_r1(&order));
            if let Err(reason) = validation {
                Self::append_trigger_rejection(&mut events, &order, reason);
                continue;
            }

            let mut child_events = match self.execute_triggered_child(
                &order,
                math,
                &trade_ids[used_trade_ids..],
                now,
                current_session_id,
                &protection_context,
            ) {
                Ok(events) => events,
                Err(reason) => {
                    Self::append_trigger_rejection(&mut events, &order, reason);
                    continue;
                }
            };
            // 独立检查参考 Vec 的真实挂单结果，不为拒绝或已终结 child 保留预留。
            if self
                .orders
                .iter()
                .any(|entry| entry.original_order().order_id == order.order_id())
            {
                if let StopOrder::Limit(stop) = &order {
                    let entry = self
                        .orders
                        .iter_mut()
                        .find(|entry| entry.original_order().order_id == stop.order_id)
                        .expect("resting reference child");
                    let mut original = entry.original_order().clone();
                    original.qty = stop.qty;
                    // 接续绝对目标，而不把已成交量混入新剩余量。
                    entry.amend_regular_in_place(
                        original,
                        matching_domain::Qty::try_new(entry.total_remaining())
                            .expect("positive child residual"),
                        entry.time_in_force(),
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

    /// 独立使用显式逻辑时间和时段重校验 child 有效期。
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

    /// 执行已通过 Trigger-R1 的 child，保留参考模型原序事件。
    fn execute_triggered_child(
        &mut self,
        order: &StopOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
        now: LogicalTime,
        session: u64,
        context: &matching_domain::market_order::CommandStartProtectionContext,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        match order {
            StopOrder::Limit(stop) => self.place_limit_timed_with_events(
                matching_domain::LimitGtcOrder {
                    order_id: stop.order_id,
                    user_id: stop.user_id,
                    side: stop.side,
                    price: stop.price,
                    qty: stop.qty,
                },
                stop.child_tif,
                now,
                session,
                trade_ids,
            ),
            StopOrder::Market(stop) => match stop.quantity {
                StopMarketQuantity::Base(qty) => {
                    let incoming = MarketByBase {
                        order_id: stop.order_id,
                        user_id: stop.user_id,
                        side: stop.side,
                        qty,
                        market_protection: stop.market_protection.clone(),
                    };
                    if stop.market_protection.is_some() {
                        self.execute_market_by_base_with_protection(
                            incoming, context, math, trade_ids,
                        )
                    } else {
                        self.execute_market_by_base(incoming, trade_ids)
                            .map(|trades| trades.into_iter().map(EngineEvent::Trade).collect())
                    }
                }
                StopMarketQuantity::Quote(quote_budget) => {
                    let incoming = MarketByQuote {
                        order_id: stop.order_id,
                        user_id: stop.user_id,
                        side: stop.side,
                        quote_budget,
                        market_protection: stop.market_protection.clone(),
                    };
                    if stop.market_protection.is_some() {
                        self.execute_market_by_quote_with_protection(
                            incoming, context, math, trade_ids,
                        )
                        .map(|execution| execution.events().to_vec())
                    } else {
                        self.execute_market_by_quote(incoming, math, trade_ids)
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
        });
    }
}
