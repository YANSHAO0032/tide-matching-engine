//! 参考簿以 Vec 扫描和显式排序实现普通订单到期。

use super::{PRIORITY_GUARD, ReferenceOrderBook};
use crate::ReferenceOrder;
use matching_domain::{
    EngineEvent, ExpireReason, ExpiredOrder, LimitOrder, LogicalTime, QueuePriority, RejectReason,
    TimeInForce, TradeId, TriggerExpiry,
};
use matching_risk::ReservationState;

impl ReferenceOrderBook {
    /// 将时效挂单和调用方预留快照作为一次独占操作提交。
    pub fn rest_timed_with_reservation(
        &mut self,
        order: LimitOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
        state: ReservationState,
    ) -> Result<(), RejectReason> {
        self.validate_new_reservation(order.order_id, order.user_id, &state)?;
        let order_id = order.order_id;
        self.rest_timed(order, tif, now, current_session_id)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(())
    }

    /// 独立执行带 DAY/GTD/GTC 存活期的普通限价单，并仅标记实际挂单余量。
    pub fn place_limit_timed_with_events(
        &mut self,
        order: LimitOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        match tif {
            TimeInForce::Gtc => {}
            TimeInForce::Gtd { expire_at } if expire_at > now => {}
            TimeInForce::Gtd { .. } => return Err(RejectReason::InvalidExpireTime),
            TimeInForce::Day { session_id } if session_id == current_session_id => {}
            TimeInForce::Day { .. } => return Err(RejectReason::InvalidSession),
            TimeInForce::Ioc | TimeInForce::Fok => {
                return Err(RejectReason::InvalidOrderCombination);
            }
        }
        let order_id = order.order_id;
        let events = self.place_limit_gtc_with_events(order, trade_ids)?;
        if let Some(resting) = self
            .orders
            .iter_mut()
            .find(|entry| entry.original_order().order_id == order_id)
        {
            resting.replace_initial_gtc_with_tif(tif);
        }
        Ok(events)
    }

    /// 以独立扫描方式挂入普通 DAY/GTD/GTC 订单。
    pub fn rest_timed(
        &mut self,
        order: LimitOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<(), RejectReason> {
        match tif {
            TimeInForce::Gtc => {}
            TimeInForce::Gtd { expire_at } if expire_at > now => {}
            TimeInForce::Gtd { .. } => return Err(RejectReason::InvalidExpireTime),
            TimeInForce::Day { session_id } if session_id == current_session_id => {}
            TimeInForce::Day { .. } => return Err(RejectReason::InvalidSession),
            TimeInForce::Ioc | TimeInForce::Fok => {
                return Err(RejectReason::InvalidOrderCombination);
            }
        }
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.next_priority >= u128::MAX - PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let next = self
            .next_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;
        self.orders.push(ReferenceOrder::new_timed(
            order,
            QueuePriority::new(self.next_priority),
            tif,
        ));
        self.next_priority = next;
        Ok(())
    }

    /// 扫描普通 GTD 订单，并按到期时间和订单标识排序后终结。
    pub fn expire_gtd_through(&mut self, now: LogicalTime) -> Vec<ExpiredOrder> {
        let mut matches = self
            .orders
            .iter()
            .filter_map(|order| match order.time_in_force() {
                TimeInForce::Gtd { expire_at } if expire_at <= now => {
                    Some((expire_at, order.original_order().order_id))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        matches.sort_unstable();
        let mut expired = matches
            .into_iter()
            .map(|(expire_at, order_id)| {
                self.cancel(order_id).unwrap_or_else(|reason| {
                    panic!("ReferenceOrderBook invariant violation: scanned GTD order missing: {reason:?}")
                });
                (
                    expire_at,
                    ExpiredOrder {
                        order_id,
                        reason: ExpireReason::GtdReached,
                    },
                )
            })
            .collect::<Vec<_>>();
        expired.extend(self.expire_stop_gtd_through(now).into_iter().map(
            |(order, _)| match order.trigger_expiry() {
                TriggerExpiry::Gtd { expire_at } => (
                    expire_at,
                    ExpiredOrder {
                        order_id: order.order_id(),
                        reason: ExpireReason::StopTriggerGtdReached,
                    },
                ),
                TriggerExpiry::Gtc | TriggerExpiry::Day { .. } => panic!(
                    "ReferenceOrderBook invariant violation: non-GTD stop returned by GTD expiry"
                ),
            },
        ));
        expired.sort_unstable_by_key(|(expire_at, entry)| (*expire_at, entry.order_id));
        expired.into_iter().map(|(_, entry)| entry).collect()
    }

    /// 扫描并终结当前交易时段内的普通 DAY 订单。
    pub fn end_session(
        &mut self,
        session_id: u64,
        current_session_id: u64,
    ) -> Result<Vec<ExpiredOrder>, RejectReason> {
        if session_id != current_session_id {
            return Err(RejectReason::InvalidSession);
        }
        let mut ids = self
            .orders
            .iter()
            .filter_map(|order| match order.time_in_force() {
                TimeInForce::Day { session_id: value } if value == session_id => {
                    Some(order.original_order().order_id)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        let mut expired = ids
            .into_iter()
            .map(|order_id| {
                self.cancel(order_id).unwrap_or_else(|reason| {
                    panic!("ReferenceOrderBook invariant violation: scanned DAY order missing: {reason:?}")
                });
                ExpiredOrder {
                    order_id,
                    reason: ExpireReason::SessionEnded,
                }
            })
            .collect::<Vec<_>>();
        expired.extend(
            self.expire_stop_day_session(session_id)
                .into_iter()
                .map(|(order, _)| ExpiredOrder {
                    order_id: order.order_id(),
                    reason: ExpireReason::StopTriggerSessionEnded,
                }),
        );
        expired.sort_unstable_by_key(|entry| entry.order_id);
        Ok(expired)
    }
}
