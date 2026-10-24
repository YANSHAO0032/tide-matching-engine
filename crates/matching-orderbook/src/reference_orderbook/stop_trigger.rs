//! 独立 Vec 参考簿的价格跨越、Trigger-R1 与止损 child 级联。

use super::ReferenceOrderBook;
use matching_domain::{
    EngineEvent, MarketByBase, MarketByQuote, MarketMath, PriceReference, PriceReferenceKind,
    RejectReason, ReservationReleaseReason, Side, StopMarketQuantity, StopOrder, StopPriority,
    TimeInForce, TradeId,
};
use std::collections::VecDeque;

impl ReferenceOrderBook {
    /// 按一次真实参考价跨越移除止损订单，并产生确定性触发事件。
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
        let ids = self
            .stop_orders(side)
            .into_iter()
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
            .filter_map(|id| self.cancel_stop(id))
            .map(|(order, priority)| {
                let event = EngineEvent::StopTriggered {
                    order_id: order.order_id(),
                    child_kind: order.child_kind(),
                };
                (order, priority, event)
            })
            .collect())
    }

    /// 以独立 Vec 参考模型执行一条参考更新及其 Stop child 级联。
    ///
    /// 触发后的每个 child 都先调用本地 Trigger-R1；失败不会调用任一撮合入口，
    /// 且只写入 Stop 终结所需的拒绝与预留释放事件。
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

            let mut child_events =
                match self.execute_triggered_child(&order, math, &trade_ids[used_trade_ids..]) {
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

    /// 校验当前参考模型已有的 child 有效期组合。
    fn validate_triggered_child_tif(&self, order: &StopOrder) -> Result<(), RejectReason> {
        match order {
            StopOrder::Limit(order) if matches!(order.child_tif, TimeInForce::Gtc) => Ok(()),
            StopOrder::Market(order) if matches!(order.child_tif, TimeInForce::Ioc) => Ok(()),
            StopOrder::Limit(_) | StopOrder::Market(_) => {
                Err(RejectReason::InvalidOrderCombination)
            }
        }
    }

    /// 执行已通过 Trigger-R1 的 child，保留参考模型原序事件。
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
