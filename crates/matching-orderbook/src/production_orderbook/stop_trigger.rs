//! 生产订单簿的参考价跨越、Trigger-R1 与止损 child 级联。

use super::ProductionOrderBook;
use matching_domain::{
    EngineEvent, MarketByBase, MarketByQuote, MarketMath, PriceReference, PriceReferenceKind,
    RejectReason, ReservationReleaseReason, Side, StopMarketQuantity, StopOrder, StopPriority,
    TimeInForce, TradeId,
};
use std::collections::VecDeque;

impl ProductionOrderBook {
    /// 收集一次价格跨越触发的止损订单，先终结其 StopBook 身份。
    ///
    /// 首个有效参考值只建立基线；价格上涨只按低到高收集买方 Stop，价格下跌只按
    /// 高到低收集卖方 Stop。静态开市参考不能驱动 Stop，乱序来源序号也不会改簿。
    pub(crate) fn collect_triggered_stops(
        &mut self,
        reference: PriceReference,
    ) -> Result<Vec<(StopOrder, StopPriority, EngineEvent)>, RejectReason> {
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
        Ok(self
            .stop_book
            .take_triggered_crossing(side, previous, reference.price)
            .into_iter()
            .map(|(order, priority)| {
                let event = EngineEvent::StopTriggered {
                    order_id: order.order_id(),
                    child_kind: order.child_kind(),
                };
                (order, priority, event)
            })
            .collect())
    }

    /// 依次执行一条参考更新及其成交引起的 Stop child 级联。
    ///
    /// 调用方提供的 Trigger-R1 在任何 child 撮合前运行，并只能依赖本地、可重放
    /// 的预留、费率和保护状态；拒绝时 stop 已终结，随后仅产生拒绝与预留释放事件。
    pub fn trigger_stops(
        &mut self,
        reference: PriceReference,
        math: &MarketMath,
        trade_ids: &[TradeId],
        mut trigger_r1: impl FnMut(&StopOrder) -> Result<(), RejectReason>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.preflight_last_trade_sequence_capacity(trade_ids.len())?;
        let mut pending = VecDeque::from(self.collect_triggered_stops(reference)?);
        let mut events = Vec::new();
        let mut used_trade_ids = 0usize;

        while let Some((order, _priority, triggered)) = pending.pop_front() {
            events.push(triggered);
            let validation = self
                .validate_triggered_child_tif(&order)
                .and_then(|()| trigger_r1(&order));
            if let Err(reason) = validation {
                Self::append_trigger_rejection(&mut events, &order, reason);
                continue;
            }

            let child_events =
                self.execute_triggered_child(&order, math, &trade_ids[used_trade_ids..]);
            let mut child_events = match child_events {
                Ok(events) => events,
                Err(reason) => {
                    Self::append_trigger_rejection(&mut events, &order, reason);
                    continue;
                }
            };
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
                pending.extend(self.collect_triggered_stops(last_trade)?);
            }
            events.append(&mut child_events);
        }
        Ok(events)
    }

    /// 确保 child 内部成交的最近成交价序号可在首笔成交前安全递增。
    fn preflight_last_trade_sequence_capacity(
        &self,
        trade_id_capacity: usize,
    ) -> Result<(), RejectReason> {
        let capacity =
            u64::try_from(trade_id_capacity).map_err(|_| RejectReason::ArithmeticOverflow)?;
        if let Some(last_trade) = self.reference_prices.latest(PriceReferenceKind::LastTrade) {
            last_trade
                .source_seq
                .checked_add(capacity)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        Ok(())
    }

    /// 校验当前已实现 child 执行入口覆盖的有效期组合。
    fn validate_triggered_child_tif(&self, order: &StopOrder) -> Result<(), RejectReason> {
        match order {
            StopOrder::Limit(order) if matches!(order.child_tif, TimeInForce::Gtc) => Ok(()),
            StopOrder::Market(order) if matches!(order.child_tif, TimeInForce::Ioc) => Ok(()),
            StopOrder::Limit(_) | StopOrder::Market(_) => {
                Err(RejectReason::InvalidOrderCombination)
            }
        }
    }

    /// 执行已通过 Trigger-R1 的 child，并保留底层产生的原序事件。
    fn execute_triggered_child(
        &mut self,
        order: &StopOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        match order {
            StopOrder::Limit(stop) => self.place_limit_gtc_with_events(
                matching_domain::LimitGtcOrder {
                    order_id: stop.order_id,
                    user_id: stop.user_id,
                    side: stop.side,
                    price: stop.price,
                    qty: stop.qty,
                },
                trade_ids,
            ),
            StopOrder::Market(stop) => match stop.quantity {
                StopMarketQuantity::Base(qty) => self
                    .execute_market_by_base(
                        MarketByBase {
                            order_id: stop.order_id,
                            user_id: stop.user_id,
                            side: stop.side,
                            qty,
                            market_protection: None,
                        },
                        trade_ids,
                    )
                    .map(|trades| trades.into_iter().map(EngineEvent::Trade).collect()),
                StopMarketQuantity::Quote(quote_budget) => self
                    .execute_market_by_quote(
                        MarketByQuote {
                            order_id: stop.order_id,
                            user_id: stop.user_id,
                            side: stop.side,
                            quote_budget,
                            market_protection: None,
                        },
                        math,
                        trade_ids,
                    )
                    .map(|execution| {
                        execution
                            .trades()
                            .iter()
                            .cloned()
                            .map(EngineEvent::Trade)
                            .collect()
                    }),
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
