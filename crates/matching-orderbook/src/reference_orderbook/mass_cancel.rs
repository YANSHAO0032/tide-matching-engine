//! 参考簿独立构造批量撤单计划，并在单一提交点安装后态。

use super::ReferenceOrderBook;
use matching_domain::{
    CancelReason, EngineEvent, OrderId, OrderRemaining, Qty, QuoteAmount, ShardCancelScope,
    ShardMassCancelResult, StopMarketQuantity, StopOrder, matches_shard_cancel_scope,
};
use matching_risk::{ReservationApplyError, ReservationRegistry, ReservationState};

#[derive(Clone, Debug, PartialEq, Eq)]
/// 参考簿只读预检得到的不可变批量撤单提交快照。
pub struct ReferenceMassCancelPlan {
    /// 计划建立时的完整参考簿快照，用于提交前检测状态漂移。
    before_book: ReferenceOrderBook,
    /// 计划建立时的金融预留登记表快照，用于提交前检测归属漂移。
    before_registry: ReservationRegistry,
    /// 已在独立参考副本中完成删除与到期清理的目标订单簿后态。
    after_book: ReferenceOrderBook,
    /// 已把本批活动绑定迁移到 PendingR2 的登记表后态。
    after_registry: ReservationRegistry,
    /// 按 OrderId 升序冻结、等待与后态一起发布的撤单事件。
    events: Vec<EngineEvent>,
}

impl ReferenceOrderBook {
    /// 在独立参考 Vec 中按价格和同价原序预演开市前价格带重校验。
    /// 本计划只迁移预留归属，真实释放与参考价更新由调用方共同提交。
    pub fn plan_preopen_revalidation(
        &self,
        lower: matching_domain::Price,
        upper: matching_domain::Price,
        registry: &ReservationRegistry,
    ) -> Result<ReferenceMassCancelPlan, matching_domain::RejectReason> {
        if lower > upper {
            return Err(matching_domain::RejectReason::InvalidPrice);
        }
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;
        let mut targets = Vec::new();
        for order in self
            .orders(matching_domain::Side::Sell)
            .into_iter()
            .chain(self.orders(matching_domain::Side::Buy))
        {
            let original = order.original_order();
            if original.price >= lower && original.price <= upper {
                continue;
            }
            let state = self
                .reservations
                .get(&original.order_id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            targets.push((
                original.order_id,
                OrderRemaining::Base(Qty::try_new(order.total_remaining())?),
                state,
            ));
        }
        let transition =
            registry.plan_binding_transitions(targets.iter().map(|(_, _, state)| *state), [])?;
        let mut after_book = self.clone();
        for (id, _, _) in &targets {
            after_book.cancel(*id).expect("参考计划中的订单必须存在");
        }
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("新建的预留计划不能漂移");
        let events = targets
            .into_iter()
            .map(|(order_id, remaining, _)| EngineEvent::OrderCanceled {
                order_id,
                remaining,
                reason: CancelReason::PriceBandRevalidation,
            })
            .collect();
        Ok(ReferenceMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }
    /// 在独立参考簿候选中终结指定时段的 DAY 订单并预检预留迁移。
    pub fn plan_end_session(
        &self,
        session_id: u64,
        current_session_id: u64,
        registry: &ReservationRegistry,
    ) -> Result<ReferenceMassCancelPlan, matching_domain::RejectReason> {
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;
        let mut after_book = self.clone();
        let expired = after_book.end_session(session_id, current_session_id)?;
        let mut bindings = Vec::with_capacity(expired.len());
        for order in &expired {
            bindings.push(
                self.reservations
                    .get(&order.order_id)
                    .ok_or(matching_domain::RejectReason::InvalidReservation)?,
            );
        }
        let transition = registry.plan_binding_transitions(bindings, [])?;
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("新建的时段终结计划不能漂移");
        let events = expired
            .into_iter()
            .map(|entry| EngineEvent::OrderExpired {
                order_id: entry.order_id,
                reason: entry.reason,
            })
            .collect();
        Ok(ReferenceMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }
    /// 在独立参考簿候选中计划关市；DAY 到期，其他活动订单取消。
    pub fn plan_market_close(
        &self,
        session_id: u64,
        registry: &ReservationRegistry,
    ) -> Result<ReferenceMassCancelPlan, matching_domain::RejectReason> {
        use matching_domain::{ExpireReason, TimeInForce, TriggerExpiry};
        let mut plan = self.plan_mass_cancel(ShardCancelScope::All, registry)?;
        for event in &mut plan.events {
            let EngineEvent::OrderCanceled {
                order_id, reason, ..
            } = event
            else {
                unreachable!("批量终结计划只产生撤单事件");
            };
            let id = *order_id;
            let expiry = if let Some(order) = self
                .orders
                .iter()
                .find(|order| order.original_order().order_id == id)
            {
                match order.time_in_force() {
                    TimeInForce::Day { session_id: day } if day == session_id => {
                        Some(ExpireReason::SessionEnded)
                    }
                    TimeInForce::Day { .. } => {
                        return Err(matching_domain::RejectReason::InvalidSession);
                    }
                    _ => None,
                }
            } else {
                let (order, _) = self.stop_order(id).expect("关市计划中的 Stop 订单必须存在");
                match order.trigger_expiry() {
                    TriggerExpiry::Day { session_id: day } if day == session_id => {
                        Some(ExpireReason::StopTriggerSessionEnded)
                    }
                    TriggerExpiry::Day { .. } => {
                        return Err(matching_domain::RejectReason::InvalidSession);
                    }
                    _ => None,
                }
            };
            if let Some(reason) = expiry {
                *event = EngineEvent::OrderExpired {
                    order_id: id,
                    reason,
                };
            } else {
                *reason = CancelReason::MarketClosed;
            }
        }
        Ok(plan)
    }
    /// 只读枚举、排序并预检范围内的活动订单，不改变订单簿或预留登记表。
    pub fn plan_mass_cancel(
        &self,
        scope: ShardCancelScope,
        registry: &ReservationRegistry,
    ) -> Result<ReferenceMassCancelPlan, matching_domain::RejectReason> {
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;
        let mut targets = self
            .active_remainders()?
            .into_iter()
            .filter(|(_, _, state)| matches_scope(scope, state))
            .collect::<Vec<_>>();
        targets.sort_unstable_by_key(|(id, _, _)| *id);
        let transition =
            registry.plan_binding_transitions(targets.iter().map(|(_, _, state)| *state), [])?;
        let mut after_book = self.clone();
        for (id, _, _) in &targets {
            if after_book
                .orders
                .iter()
                .any(|order| order.original_order().order_id == *id)
            {
                after_book
                    .cancel(*id)
                    .expect("planned reference regular order missing");
            } else {
                after_book
                    .cancel_stop(*id)
                    .expect("planned reference stop order missing");
            }
        }
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh reference binding plan drifted");
        let events = targets
            .into_iter()
            .map(|(order_id, remaining, _)| EngineEvent::OrderCanceled {
                order_id,
                remaining,
                reason: CancelReason::MassCancel,
            })
            .collect();
        Ok(ReferenceMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }

    /// 验证计划前态后一次安装订单簿和预留登记表的后态。
    pub fn apply_mass_cancel(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ReferenceMassCancelPlan,
    ) -> Result<(ShardMassCancelResult, Vec<EngineEvent>), ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        let count = u64::try_from(plan.events.len()).expect("event count exceeds u64");
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok((
            ShardMassCancelResult {
                canceled_orders: count,
            },
            plan.events,
        ))
    }

    fn active_remainders(
        &self,
    ) -> Result<Vec<(OrderId, OrderRemaining, &ReservationState)>, matching_domain::RejectReason>
    {
        let mut result = Vec::new();
        for order in &self.orders {
            let id = order.original_order().order_id;
            let state = self
                .reservations
                .get(&id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            result.push((
                id,
                OrderRemaining::Base(Qty::try_new(order.total_remaining())?),
                state,
            ));
        }
        for entry in &self.stop_orders {
            let id = entry.order.order_id();
            let state = self
                .reservations
                .get(&id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            let remaining = match &entry.order {
                StopOrder::Limit(order) => OrderRemaining::Base(order.qty),
                StopOrder::Market(order) => match order.quantity {
                    StopMarketQuantity::Base(qty) => OrderRemaining::Base(qty),
                    StopMarketQuantity::Quote(budget) => {
                        OrderRemaining::Quote(QuoteAmount::new(budget.get()))
                    }
                },
            };
            result.push((id, remaining, state));
        }
        Ok(result)
    }
}

fn matches_scope(scope: ShardCancelScope, state: &ReservationState) -> bool {
    matches_shard_cancel_scope(
        scope,
        state.user_id,
        state.market_id,
        state.origin_session_id,
    )
}
