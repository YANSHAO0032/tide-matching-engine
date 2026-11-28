//! 生产簿在独立候选上预演单笔、批量和市场管理撤单，核对前态后安装订单簿及预留绑定。

use super::ProductionOrderBook;
use matching_domain::{
    CancelReason, EngineEvent, OrderId, OrderRemaining, Qty, QuoteAmount, ShardCancelScope,
    ShardMassCancelResult, StopMarketQuantity, StopOrder, matches_shard_cancel_scope,
};
use matching_risk::{ReservationApplyError, ReservationRegistry, ReservationState};

#[derive(Clone, Debug, PartialEq, Eq)]
/// 生产簿撤单或市场清理计划，包含提交前后的订单簿、预留绑定和有序事件。
pub struct ProductionMassCancelPlan {
    /// 计划建立时的完整生产簿快照，用于提交前检测状态漂移。
    before_book: ProductionOrderBook,
    /// 计划建立时的金融预留登记表快照，用于提交前检测归属漂移。
    before_registry: ReservationRegistry,
    /// 已在独立生产副本中完成索引、Arena、到期及 Stop 清理的后态。
    after_book: ProductionOrderBook,
    /// 已把本批活动绑定迁移到 PendingR2 的登记表后态。
    after_registry: ReservationRegistry,
    /// 按各计划规定的顺序生成、等待与后态一起提交的订单事件。
    events: Vec<EngineEvent>,
}

impl ProductionOrderBook {
    /// 按订单 ID 核对普通挂单或未触发止损单的用户及预留绑定。普通挂单按总余量、止损单按原计价模式报告取消。
    /// 候选先将绑定移入 PendingR2，正式分片在同一命令中结清实际剩余。
    pub fn plan_user_cancel(
        &self,
        input: &matching_domain::CancelOrder,
        registry: &ReservationRegistry,
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
        use matching_domain::RejectReason;
        if self.market_id() != Some(input.market_id) {
            return Err(RejectReason::OrderNotFound);
        }
        let id = input.order_id;
        let (owner, remaining, is_stop) = if let Some(order) = self.order(id) {
            (
                order.original_order().user_id,
                OrderRemaining::Base(Qty::try_new(order.total_remaining())?),
                false,
            )
        } else if let Some((stop, _)) = self.stop_order(id) {
            let (user, remaining) = match stop {
                StopOrder::Limit(s) => (s.user_id, OrderRemaining::Base(s.qty)),
                StopOrder::Market(s) => (
                    s.user_id,
                    match s.quantity {
                        StopMarketQuantity::Base(q) => OrderRemaining::Base(q),
                        StopMarketQuantity::Quote(q) => {
                            OrderRemaining::Quote(QuoteAmount::new(q.get()))
                        }
                    },
                ),
            };
            (user, remaining, true)
        } else {
            return Err(RejectReason::OrderNotFound);
        };
        if owner != input.user_id {
            return Err(RejectReason::OrderNotFound);
        }
        let shadow = self
            .reservation_state(id)
            .ok_or(RejectReason::InvalidReservation)?;
        if shadow.user_id != owner || shadow.order_id != id || shadow.market_id != input.market_id {
            return Err(RejectReason::InvalidReservation);
        }
        let transition = registry.plan_binding_transitions([shadow], [])?;
        let mut after_book = self.clone();
        if is_stop {
            after_book
                .cancel_stop(id)
                .expect("已定位的止损订单必须存在");
        } else {
            after_book.cancel(id).expect("已定位的普通订单必须存在");
        }
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("已预检的单笔撤单归属不能漂移");
        Ok(ProductionMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events: vec![EngineEvent::OrderCanceled {
                order_id: id,
                remaining,
                reason: CancelReason::UserRequested,
            }],
        })
    }

    /// 预演熔断开关按策略终结市场内所有活动订单。
    pub fn plan_kill_switch_cancel(
        &self,
        registry: &ReservationRegistry,
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
        let mut plan = self.plan_mass_cancel(ShardCancelScope::All, registry)?;
        for event in &mut plan.events {
            if let EngineEvent::OrderCanceled { reason, .. } = event {
                *reason = CancelReason::KillSwitch;
            }
        }
        Ok(plan)
    }
    /// 按卖盘升价、买盘降价及价档链表顺序预演开市前普通挂单重校验。
    ///
    /// 边界须来自同一参考更新候选；本计划只迁移预留归属至 PendingR2，
    /// 调用方仍须在同一持久候选中完成真实释放及参考价更新。
    pub fn plan_preopen_revalidation(
        &self,
        lower: matching_domain::Price,
        upper: matching_domain::Price,
        registry: &ReservationRegistry,
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
        if lower > upper {
            return Err(matching_domain::RejectReason::InvalidPrice);
        }
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )?;
        let mut targets = Vec::new();
        for (price, level) in self.asks.iter().chain(self.bids.iter().rev()) {
            if *price >= lower && *price <= upper {
                continue;
            }
            let mut current = level.head();
            while let Some(index) = current {
                let node = self.arena.get(index).expect("价档引用的订单必须存在");
                let id = node.original_order().order_id;
                let state = self
                    .reservations
                    .get(&id)
                    .ok_or(matching_domain::RejectReason::InvalidReservation)?;
                targets.push((
                    id,
                    OrderRemaining::Base(Qty::try_new(node.total_remaining())?),
                    state,
                ));
                current = node.next();
            }
        }
        let transition =
            registry.plan_binding_transitions(targets.iter().map(|(_, _, state)| *state), [])?;
        let mut after_book = self.clone();
        for (id, _, _) in &targets {
            after_book.cancel(*id).expect("计划中的普通订单必须存在");
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
        Ok(ProductionMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }
    /// 在候选簿中终结指定交易时段的 DAY 订单，并预检预留归属迁移。
    pub fn plan_end_session(
        &self,
        session_id: u64,
        current_session_id: u64,
        registry: &ReservationRegistry,
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )?;
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
        Ok(ProductionMassCancelPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }
    /// 在副本中计划关市的全部活动订单终结，DAY 到期且其他订单按关市取消。
    ///
    /// 预留绑定迁入 PendingR2；不会声称资金已结算或已释放。
    pub fn plan_market_close(
        &self,
        session_id: u64,
        registry: &ReservationRegistry,
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
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
            let expiry = if let Some(order) = self.order(id) {
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
    ) -> Result<ProductionMassCancelPlan, matching_domain::RejectReason> {
        self.validate_registry_bindings(
            registry,
            self.reservations.iter().map(|(id, state)| (*id, state)),
        )?;
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
            if after_book.orders.contains_key(id) {
                after_book
                    .cancel(*id)
                    .expect("planned production regular order missing");
            } else {
                after_book
                    .cancel_stop(*id)
                    .expect("planned production stop order missing");
            }
        }
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh production binding plan drifted");
        let events = targets
            .into_iter()
            .map(|(order_id, remaining, _)| EngineEvent::OrderCanceled {
                order_id,
                remaining,
                reason: CancelReason::MassCancel,
            })
            .collect();
        Ok(ProductionMassCancelPlan {
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
        plan: ProductionMassCancelPlan,
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
        for (id, index) in &self.orders {
            let node = self
                .arena
                .get(*index)
                .expect("production order index references missing arena slot");
            let state = self
                .reservations
                .get(id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            result.push((
                *id,
                OrderRemaining::Base(Qty::try_new(node.total_remaining())?),
                state,
            ));
        }
        for (order, _) in self
            .stop_book
            .ordered(matching_domain::Side::Buy)
            .into_iter()
            .chain(self.stop_book.ordered(matching_domain::Side::Sell))
        {
            let id = order.order_id();
            let state = self
                .reservations
                .get(&id)
                .ok_or(matching_domain::RejectReason::InvalidReservation)?;
            let remaining = match order {
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
