//! 参考簿未触发止损改单；独立扫描源 Vec 并一次提交新载荷。

use super::{PRIORITY_GUARD, ReferenceOrderBook, amend_candidate::ReferenceAmendPlan};
use matching_domain::{
    AmendOrder, AmendedQuantity, EngineEvent, LogicalTime, QuoteBudget, RejectReason,
    StopMarketQuantity, StopOrder, StopPriority, TimeInForce, TriggerExpiry,
};

/// 独立 Vec 扫描生成的完整提交动作，不借用或改变原簿。
struct ReferenceStopAction {
    /// 源 Vec 中原订单的位置。
    position: usize,
    /// 校验后的全部候选字段。
    order: StopOrder,
    /// 提交后生效的触发优先级。
    priority: StopPriority,
    /// 提交后分配器值。
    next: u128,
    /// 是否保留源位置。
    retained: bool,
    /// 预先构造的有序输出。
    events: Vec<EngineEvent>,
}

impl ReferenceOrderBook {
    /// 独立预检并提交未触发 Stop；不触碰普通挂单和 QueuePriority。
    ///
    /// child 模板和到期变化保留触发优先级，改触发价或来源才重新排队。
    /// 触发源和市价保护随候选原子保存。本低层簿接口要求调用方已经核对资金覆盖；
    /// 缺失预留状态或带未验证令牌均拒绝。需要核验资金的命令使用 `amend_with_context`，
    /// 本接口成功时保留预留内容及版本。
    pub fn amend_stop_with_events(
        &mut self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let plan = self.plan_stop_amend(amend, now, current_session_id)?;
        // 写屏障：到期继续扫描源载荷；不触碰预留映射或普通优先级。
        if plan.retained {
            self.stop_orders[plan.position].order = plan.order;
        } else {
            self.stop_orders.remove(plan.position);
            self.stop_orders.push(super::ReferenceStopOrder {
                order: plan.order,
                priority: plan.priority,
            });
        }
        self.next_stop_priority = plan.next;
        Ok(plan.events)
    }

    /// 基于不可变 Vec 快照完成所有业务拒绝、时效与分配器预检。
    fn plan_stop_amend(
        &self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<ReferenceStopAction, RejectReason> {
        let candidate = match self.build_amend_plan(amend, now, current_session_id)? {
            ReferenceAmendPlan::Stop(plan) => plan,
            _ => return Err(RejectReason::InvalidOrderCombination),
        };
        if amend.reservation_adjustment.is_some() {
            return Err(RejectReason::InvalidReservation);
        }
        let position = self
            .stop_orders
            .iter()
            .position(|entry| entry.order.order_id() == amend.order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        let mut order = self.stop_orders[position].order.clone();
        let old_price;
        let old_quantity;
        match &mut order {
            StopOrder::Limit(child) => {
                old_price = Some(child.price);
                old_quantity = AmendedQuantity::BaseRemaining(child.qty);
                if let Some(price) = amend.new_price {
                    child.price = price;
                }
                if let Some(qty) = amend.new_qty {
                    child.qty = qty;
                }
                if let Some(tif) = amend.new_tif {
                    child.child_tif = tif;
                }
                Self::validate_stop_child_amend_tif(child.child_tif, now, current_session_id)?;
                child.stop_price = candidate.new_stop_price;
                child.trigger_reference = candidate.new_reference;
                child.trigger_expiry = candidate.target_expiry;
            }
            StopOrder::Market(child) => {
                if amend.new_price.is_some() {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                old_price = None;
                match &mut child.quantity {
                    StopMarketQuantity::Base(qty) => {
                        old_quantity = AmendedQuantity::BaseRemaining(*qty);
                        if let Some(target) = amend.new_qty {
                            *qty = target;
                        }
                    }
                    StopMarketQuantity::Quote(budget) => {
                        old_quantity = AmendedQuantity::QuoteBudget(
                            matching_domain::QuoteAmount::new(budget.get()),
                        );
                        if let Some(target) = amend.new_quote_budget {
                            *budget = QuoteBudget::try_new(target.get())?;
                        }
                    }
                }
                if let Some(tif) = amend.new_tif {
                    child.child_tif = tif;
                }
                if child.child_tif != TimeInForce::Ioc {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                child.market_protection = candidate.new_protection.clone();
                child.stop_price = candidate.new_stop_price;
                child.trigger_reference = candidate.new_reference;
                child.trigger_expiry = candidate.target_expiry;
            }
        }
        match order.trigger_expiry() {
            TriggerExpiry::Gtc => {}
            TriggerExpiry::Gtd { expire_at } if expire_at > now => {}
            TriggerExpiry::Day { session_id } if session_id == current_session_id => {}
            _ => return Err(RejectReason::InvalidExpireTime),
        }
        let retained = candidate.old_stop_price == candidate.new_stop_price
            && candidate.old_reference == candidate.new_reference;
        let (priority, next) = if retained {
            (candidate.priority, self.next_stop_priority)
        } else {
            if self.next_stop_priority >= u128::MAX - PRIORITY_GUARD {
                return Err(RejectReason::PrioritySpaceExhaustion);
            }
            (
                StopPriority::new(self.next_stop_priority),
                self.next_stop_priority
                    .checked_add(1)
                    .ok_or(RejectReason::PrioritySpaceExhaustion)?,
            )
        };
        let (new_price, new_quantity) = match &order {
            StopOrder::Limit(child) => {
                (Some(child.price), AmendedQuantity::BaseRemaining(child.qty))
            }
            StopOrder::Market(child) => (
                None,
                match child.quantity {
                    StopMarketQuantity::Base(qty) => AmendedQuantity::BaseRemaining(qty),
                    StopMarketQuantity::Quote(ref budget) => AmendedQuantity::QuoteBudget(
                        matching_domain::QuoteAmount::new(budget.get()),
                    ),
                },
            ),
        };
        let events = vec![EngineEvent::OrderAmended {
            order_id: amend.order_id,
            old_price,
            new_price,
            old_quantity,
            new_quantity,
            priority_retained: retained,
        }];
        Ok(ReferenceStopAction {
            position,
            order,
            priority,
            next,
            retained,
            events,
        })
    }

    /// 独立检查限价 child 可接受的时效和当前逻辑时间边界。
    fn validate_stop_child_amend_tif(
        tif: TimeInForce,
        now: LogicalTime,
        session: u64,
    ) -> Result<(), RejectReason> {
        match tif {
            TimeInForce::Gtc => Ok(()),
            TimeInForce::Gtd { expire_at } if expire_at > now => Ok(()),
            TimeInForce::Gtd { .. } => Err(RejectReason::InvalidExpireTime),
            TimeInForce::Day { session_id } if session_id == session => Ok(()),
            TimeInForce::Day { .. } => Err(RejectReason::InvalidSession),
            _ => Err(RejectReason::InvalidOrderCombination),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{
        AmendOrder, MarketId, OrderId, Price, Qty, ReservationId, StopLimitOrder,
        TriggerExpiryPatch, UserId,
    };
    use matching_risk::ReservationState;

    #[test]
    fn amend_stop_reference_plan_is_pure_and_high_water_blocks_only_changed_trigger() {
        let mut book = ReferenceOrderBook::new();
        let id = OrderId::new(1);
        book.place_stop_with_reservation(
            StopOrder::Limit(StopLimitOrder {
                trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
                order_id: id,
                user_id: UserId::new(4),
                reservation_id: ReservationId::new(1),
                fee_schedule_version: 2,
                side: matching_domain::Side::Buy,
                stop_price: Price::try_new(100).unwrap(),
                price: Price::try_new(101).unwrap(),
                qty: Qty::try_new(8).unwrap(),
                child_tif: TimeInForce::Gtc,
                trigger_expiry: TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(20),
                },
            }),
            LogicalTime::new(1),
            9,
            ReservationState {
                reservation_id: ReservationId::new(1),
                order_id: id,
                user_id: UserId::new(4),
                market_id: MarketId::new(7),
                version: 11,
                reserves: Vec::new(),
            },
        )
        .unwrap();
        let mut amend = AmendOrder {
            order_id: id,
            user_id: UserId::new(4),
            market_id: MarketId::new(7),
            new_qty: Some(Qty::try_new(12).unwrap()),
            new_price: None,
            new_quote_budget: None,
            new_visible_qty: None,
            new_stop_price: Some(Price::try_new(110).unwrap()),
            new_trigger_reference: None,
            new_market_protection: None,
            new_tif: None,
            new_trigger_expiry: Some(TriggerExpiryPatch::ClearToGtc),
            reservation_adjustment: None,
        };
        let before = book.test_deterministic_state();
        let plan = book
            .plan_stop_amend(&amend, LogicalTime::new(10), 9)
            .unwrap();
        assert_eq!(plan.priority.get(), 1);
        assert_eq!(plan.next, 2);
        assert!(!plan.retained);
        assert_eq!(book.test_deterministic_state(), before);
        book.next_stop_priority = u128::MAX - PRIORITY_GUARD;
        let before = book.test_deterministic_state();
        assert_eq!(
            book.amend_stop_with_events(&amend, LogicalTime::new(10), 9),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(book.test_deterministic_state(), before);
        amend.new_stop_price = Some(Price::try_new(100).unwrap());
        amend.new_trigger_reference = Some(matching_domain::PriceReferenceKind::MarkPrice);
        assert_eq!(
            book.amend_stop_with_events(&amend, LogicalTime::new(10), 9),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(book.test_deterministic_state(), before);
        amend.new_trigger_reference = None;
        amend.new_stop_price = Some(Price::try_new(100).unwrap());
        book.amend_stop_with_events(&amend, LogicalTime::new(10), 9)
            .unwrap();
        assert_eq!(book.next_stop_priority, u128::MAX - PRIORITY_GUARD);
        assert_eq!(book.stop_order(id).unwrap().1.get(), 0);
        assert_eq!(book.next_priority, 0);
        assert!(
            book.expire_stop_gtd_through(LogicalTime::new(20))
                .is_empty()
        );
    }
}
