//! 生产订单簿的参考价跨越、Trigger-R1 与止损 child 级联。

use super::ProductionOrderBook;
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

impl ProductionOrderBook {
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

            let child_events = self.execute_triggered_child(
                &order,
                math,
                &trade_ids[used_trade_ids..],
                now,
                current_session_id,
                &protection_context,
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
        context: &matching_domain::market_order::CommandStartProtectionContext,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        match order {
            StopOrder::Limit(stop) => self.place_limit_timed_with_events(
                matching_domain::LimitOrder {
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
