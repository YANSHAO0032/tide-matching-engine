//! 冰山改单的独立参考 Vec 规划与一次提交。
use super::amend_candidate::IcebergAmendPlan;
use super::iceberg::SliceRequest;
use super::{PRIORITY_GUARD, ReferenceOrderBook};
use crate::ReferenceOrder;
use matching_domain::{
    AmendOrder, AmendedQuantity, EngineEvent, Qty, QueuePriority, RejectReason, TradeId,
};

impl ReferenceOrderBook {
    /// 参考模型只在虚拟 Vec 中规划；成功前不改真实订单或分配器。
    pub(super) fn commit_iceberg_amend(
        &mut self,
        plan: IcebergAmendPlan,
        amend: &AmendOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if amend.reservation_adjustment.is_some() {
            return Err(RejectReason::InvalidReservation);
        }
        let p = plan.regular;
        let position = self
            .orders
            .iter()
            .position(|e| e.original_order().order_id == p.order_id)
            .expect("planned iceberg");
        let original = Self::amended_payload(self.orders[position].original_order(), p)?;
        let retained = p.old_price == p.new_price
            && p.new_remaining <= p.old_remaining
            && plan.new_peak <= plan.old_peak;
        let event = EngineEvent::OrderAmended {
            order_id: p.order_id,
            old_price: Some(p.old_price),
            new_price: Some(p.new_price),
            old_quantity: AmendedQuantity::BaseRemaining(p.old_remaining),
            new_quantity: AmendedQuantity::BaseRemaining(p.new_remaining),
            priority_retained: retained,
        };
        if retained {
            let visible = Qty::try_new(
                plan.old_visible_remaining
                    .get()
                    .min(plan.new_peak.get())
                    .min(p.new_remaining.get()),
            )?;
            self.orders[position] = ReferenceOrder::amended_iceberg(
                original,
                p.new_remaining,
                visible,
                plan.new_peak,
                p.priority,
                p.target_tif,
            );
            return Ok(vec![event]);
        }
        let mut slice = self.plan_slices(SliceRequest {
            id: p.order_id,
            side: p.side,
            remaining: p.new_remaining.get(),
            limit: Some(p.new_price),
            budget: None,
            math: None,
            protection: None,
            one_fill: false,
        })?;
        let count = slice
            .events
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count();
        assert!(
            trade_ids.len() >= count,
            "insufficient TradeIds for reference iceberg Amend"
        );
        let position = slice
            .orders
            .iter()
            .position(|e| e.original_order().order_id == p.order_id)
            .expect("virtual iceberg");
        slice.orders.remove(position);
        if slice.remaining > 0 {
            if slice.next_priority >= u128::MAX - PRIORITY_GUARD {
                return Err(RejectReason::PrioritySpaceExhaustion);
            }
            let visible = Qty::try_new(slice.remaining.min(plan.new_peak.get()))?;
            Self::preflight_amend_level(&slice.orders, p, visible)?;
            let priority = QueuePriority::new(slice.next_priority);
            slice.next_priority = slice
                .next_priority
                .checked_add(1)
                .ok_or(RejectReason::PrioritySpaceExhaustion)?;
            slice.orders.push(ReferenceOrder::amended_iceberg(
                original,
                Qty::try_new(slice.remaining)?,
                visible,
                plan.new_peak,
                priority,
                p.target_tif,
            ));
        }
        let mut events = vec![event];
        let mut order_events = Vec::new();
        let mut ids = trade_ids.iter();
        for mut event in slice.events {
            if let EngineEvent::Trade(trade) = &mut event {
                trade.trade_id = *ids.next().expect("planned TradeId");
                events.push(event);
            } else {
                order_events.push(event);
            }
        }
        if count > 0 {
            order_events.push(if slice.remaining == 0 {
                EngineEvent::OrderFilled {
                    order_id: p.order_id,
                }
            } else {
                EngineEvent::OrderPartiallyFilled {
                    order_id: p.order_id,
                    remaining: Qty::try_new(slice.remaining).expect("planned residual"),
                }
            });
        }
        // 写屏障：真实 Vec、预留清理和分配器同时推进至已规划状态。
        self.orders = slice.orders;
        self.next_priority = slice.next_priority;
        self.retain_active_reservations();
        events.extend(order_events);
        Ok(events)
    }
}
