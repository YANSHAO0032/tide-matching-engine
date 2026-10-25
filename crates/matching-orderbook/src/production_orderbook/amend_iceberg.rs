//! 冰山改单独立规划显示片、成交和结构提交。
use super::amend_candidate::IcebergAmendPlan;
use super::iceberg::Budget;
use super::resting::commit_rest_target;
use super::{ProductionOrderBook, RestPlan};
use crate::OrderNode;
use matching_domain::{
    AmendOrder, AmendedQuantity, EngineEvent, IcebergOrder, Qty, RejectReason, Side, TradeId,
};

impl ProductionOrderBook {
    /// 所有数量、成交、目标档和优先级失败均在删除原单之前完成。
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
        let index = self.orders[&p.order_id];
        let mut original = self
            .arena
            .get(index)
            .expect("planned iceberg")
            .original_order()
            .clone();
        original.price = p.new_price;
        original.qty = Qty::try_new(
            p.already_filled
                .checked_add(p.new_remaining.get())
                .ok_or(RejectReason::ArithmeticOverflow)?,
        )?;
        let iceberg = IcebergOrder::try_new(original, plan.new_peak)?;
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
            let visible = plan
                .old_visible_remaining
                .get()
                .min(plan.new_peak.get())
                .min(p.new_remaining.get());
            let mut replacement =
                OrderNode::new_iceberg(iceberg, p.new_remaining.get(), p.priority);
            replacement.amend_iceberg_in_place(
                replacement.original_order().clone(),
                p.new_remaining,
                Qty::try_new(visible)?,
                plan.new_peak,
                p.target_tif,
            );
            let levels = match p.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            // 写屏障：没有公开增量，不需要新槽位或优先级。
            levels
                .get_mut(&p.old_price)
                .expect("planned level")
                .amend_iceberg_in_place(&mut self.arena, index, replacement);
            self.regular_expiry.remove(p.order_id, p.old_tif);
            self.regular_expiry.insert(p.order_id, p.target_tif);
            return Ok(vec![event]);
        }
        let fills = if p.old_price != p.new_price {
            Some(self.preflight_iceberg_plan(
                p.side,
                Some(p.new_price),
                Budget::Base(p.new_remaining.get()),
                None,
                None,
                None,
            )?)
        } else {
            None
        };
        assert!(
            trade_ids.len() >= fills.as_ref().map_or(0, |f| f.fills.len()),
            "insufficient TradeIds for iceberg Amend"
        );
        let remaining = fills
            .as_ref()
            .map_or(p.new_remaining.get(), |f| f.base_remaining);
        let residual = if remaining > 0 {
            let visible = Qty::try_new(remaining.min(plan.new_peak.get()))?;
            let mut visible_plan = p;
            visible_plan.old_remaining = plan.old_visible_remaining;
            Some(
                self.preflight_amend_rest(
                    visible_plan,
                    visible,
                    fills
                        .as_ref()
                        .map_or(self.next_queue_priority(), |f| f.next_priority),
                )?,
            )
        } else {
            None
        };
        // 写屏障：全部片成交、补量分配及目标价档聚合均已冻结。
        let shadow = self
            .reservations
            .remove(&p.order_id)
            .expect("planned shadow");
        self.cancel(p.order_id).expect("planned iceberg");
        let mut events = vec![event];
        let mut order_events = Vec::new();
        if let Some(fills) = &fills {
            for event in self.commit_iceberg_plan(fills, p.side, p.order_id, trade_ids) {
                if matches!(event, EngineEvent::Trade(_)) {
                    events.push(event);
                } else {
                    order_events.push(event);
                }
            }
            if !fills.fills.is_empty() {
                order_events.push(if remaining == 0 {
                    EngineEvent::OrderFilled {
                        order_id: p.order_id,
                    }
                } else {
                    EngineEvent::OrderPartiallyFilled {
                        order_id: p.order_id,
                        remaining: Qty::try_new(remaining).expect("planned residual"),
                    }
                });
            }
        }
        if let Some(rest) = residual {
            self.commit_amended_iceberg_rest(iceberg, remaining, p.target_tif, rest);
            self.reservations.insert(p.order_id, shadow);
        }
        events.extend(order_events);
        Ok(events)
    }

    /// 消费已检查聚合与分配器的冰山挂单计划。
    fn commit_amended_iceberg_rest(
        &mut self,
        iceberg: IcebergOrder,
        remaining: u64,
        tif: matching_domain::TimeInForce,
        rest: RestPlan,
    ) {
        let mut node = OrderNode::new_iceberg(iceberg, remaining, rest.priority);
        node.replace_initial_gtc_with_tif(tif);
        let slot = match rest.side {
            Side::Buy => commit_rest_target(&mut self.bids, &mut self.arena, node, &rest),
            Side::Sell => commit_rest_target(&mut self.asks, &mut self.arena, node, &rest),
        };
        assert!(self.orders.insert(rest.order_id, slot).is_none());
        self.regular_expiry.insert(rest.order_id, tif);
        assert_eq!(
            self.queue_priority_allocator
                .allocate()
                .expect("planned iceberg priority"),
            rest.priority
        );
    }
}
