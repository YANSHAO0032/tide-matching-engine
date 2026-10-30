//! 独立 Vec 参考簿的未触发止损容器、取消与到期逻辑。

use super::{PRIORITY_GUARD, ReferenceOrderBook};
use matching_domain::{
    LogicalTime, OrderId, RejectReason, Side, StopMarketQuantity, StopOrder, StopPriority,
    TimeInForce, TriggerExpiry,
};
use matching_risk::ReservationState;

/// 参考模型中未触发止损订单的最小运行态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ReferenceStopOrder {
    /// 原始止损订单。
    pub(super) order: StopOrder,
    /// 仅用于同触发价先进先出的独立优先级。
    pub(super) priority: StopPriority,
}

impl ReferenceOrderBook {
    /// 在一次参考模型提交中登记未触发 Stop 与其本地预留快照。
    pub fn place_stop_with_reservation(
        &mut self,
        order: StopOrder,
        now: LogicalTime,
        current_session_id: u64,
        state: ReservationState,
    ) -> Result<StopPriority, RejectReason> {
        let order_id = order.order_id();
        let user_id = match &order {
            StopOrder::Limit(stop) => stop.user_id,
            StopOrder::Market(stop) => stop.user_id,
        };
        self.validate_new_reservation(order_id, user_id, &state)?;
        if order.reservation_id() != state.reservation_id {
            return Err(RejectReason::InvalidReservation);
        }
        let priority = self.place_stop(order, now, current_session_id)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(priority)
    }

    /// 登记未触发止损订单；参考实现使用 Vec 扫描，独立于生产 StopBook。
    pub fn place_stop(
        &mut self,
        order: StopOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<StopPriority, RejectReason> {
        order.validate_stp_mode()?;
        if self.contains_active_order(order.order_id()) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if order.trigger_reference() == matching_domain::PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        if order
            .market_protection()
            .and_then(|p| p.max_slippage_bps)
            .is_some_and(|bps| bps >= 10_000)
        {
            return Err(RejectReason::InvalidSlippageBps);
        }
        match &order {
            StopOrder::Limit(order)
                if matches!(
                    order.child_tif,
                    TimeInForce::Gtc | TimeInForce::Day { .. } | TimeInForce::Gtd { .. }
                ) => {}
            StopOrder::Market(order)
                if matches!(
                    (&order.quantity, order.child_tif),
                    (
                        StopMarketQuantity::Base(_) | StopMarketQuantity::Quote(_),
                        TimeInForce::Ioc
                    )
                ) => {}
            StopOrder::Limit(_) | StopOrder::Market(_) => {
                return Err(RejectReason::InvalidOrderCombination);
            }
        }
        match order.trigger_expiry() {
            TriggerExpiry::Gtc => {}
            TriggerExpiry::Day { session_id } if session_id == current_session_id => {}
            TriggerExpiry::Day { .. } => return Err(RejectReason::InvalidExpireTime),
            TriggerExpiry::Gtd { expire_at } if expire_at > now => {}
            TriggerExpiry::Gtd { .. } => return Err(RejectReason::InvalidExpireTime),
        }
        if self.next_stop_priority >= u128::MAX - PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let next = self
            .next_stop_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;
        let priority = StopPriority::new(self.next_stop_priority);
        self.stop_orders
            .push(ReferenceStopOrder { order, priority });
        self.next_stop_priority = next;
        Ok(priority)
    }

    /// 判断标识是否被常规订单或未触发止损订单占用。
    pub(crate) fn contains_active_order(&self, order_id: OrderId) -> bool {
        self.orders
            .iter()
            .any(|order| order.original_order().order_id == order_id)
            || self
                .stop_orders
                .iter()
                .any(|entry| entry.order.order_id() == order_id)
    }

    /// 读取下一次独立 StopPriority。
    pub fn next_stop_priority(&self) -> u128 {
        self.next_stop_priority
    }

    /// 查询一张未触发止损订单。
    pub fn stop_order(&self, order_id: OrderId) -> Option<(&StopOrder, StopPriority)> {
        self.stop_orders
            .iter()
            .find(|entry| entry.order.order_id() == order_id)
            .map(|entry| (&entry.order, entry.priority))
    }

    /// 返回一侧按触发顺序排列的未触发止损订单快照。
    pub fn stop_orders(&self, side: Side) -> Vec<(&StopOrder, StopPriority)> {
        let mut entries = self
            .stop_orders
            .iter()
            .filter(|entry| entry.order.side() == side)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            let price_order = match side {
                Side::Buy => left.order.stop_price().cmp(&right.order.stop_price()),
                Side::Sell => right.order.stop_price().cmp(&left.order.stop_price()),
            };
            let source_order = match side {
                Side::Buy => left
                    .order
                    .trigger_reference()
                    .cmp(&right.order.trigger_reference()),
                Side::Sell => right
                    .order
                    .trigger_reference()
                    .cmp(&left.order.trigger_reference()),
            };
            price_order
                .then(source_order)
                .then(left.priority.cmp(&right.priority))
        });
        entries
            .into_iter()
            .map(|entry| (&entry.order, entry.priority))
            .collect()
    }

    /// 取消一张未触发止损订单。
    pub fn cancel_stop(&mut self, order_id: OrderId) -> Option<(StopOrder, StopPriority)> {
        let index = self
            .stop_orders
            .iter()
            .position(|entry| entry.order.order_id() == order_id)?;
        let entry = self.stop_orders.remove(index);
        self.reservations.remove(&order_id);
        Some((entry.order, entry.priority))
    }

    /// 扫描并删除不晚于当前逻辑时间的 GTD 止损订单。
    pub fn expire_stop_gtd_through(&mut self, now: LogicalTime) -> Vec<(StopOrder, StopPriority)> {
        self.expire_stops(
            |expiry| matches!(expiry, TriggerExpiry::Gtd { expire_at } if expire_at <= now),
        )
    }

    /// 扫描并删除指定交易时段的 DAY 止损订单。
    pub fn expire_stop_day_session(&mut self, session_id: u64) -> Vec<(StopOrder, StopPriority)> {
        self.expire_stops(|expiry| matches!(expiry, TriggerExpiry::Day { session_id: value } if value == session_id))
    }

    /// 使用稳定 Vec 顺序实现参考模型的到期移除。
    fn expire_stops(
        &mut self,
        predicate: impl Fn(TriggerExpiry) -> bool,
    ) -> Vec<(StopOrder, StopPriority)> {
        let mut expired = Vec::new();
        let mut retained = Vec::with_capacity(self.stop_orders.len());
        for entry in self.stop_orders.drain(..) {
            if predicate(entry.order.trigger_expiry()) {
                self.reservations.remove(&entry.order.order_id());
                expired.push((entry.order, entry.priority));
            } else {
                retained.push(entry);
            }
        }
        self.stop_orders = retained;
        expired
    }
}
