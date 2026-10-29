//! 普通下单的独立参考计划与预留归属提交边界。
//!
//! 只在可丢弃参考副本上调用既有撮合入口，不复制撮合算法；
//! 真实订单簿与真实预留登记表在完整预演通过后才一起提交。

use super::ReferenceOrderBook;
use matching_domain::{
    EngineEvent, IcebergOrder, LimitOrder, LogicalTime, MarketMath, Qty, RejectReason, TimeInForce,
    TradeId,
};
use matching_risk::{
    PlannedBindingTransition, ReservationApplyError, ReservationRegistry, ReservationState,
};

/// 既有普通下单能力的输入；不包含撤单替换身份或资金过渡。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferencePlaceOrder {
    /// 普通限价载荷及其执行策略；只挂单标志仅允许搭配 GTC。
    Limit {
        /// 独立的完整下单载荷。
        order: LimitOrder,
        /// 普通限价支持 GTC、IOC、FOK、DAY 与 GTD。
        time_in_force: TimeInForce,
        /// 是否要求只挂单。
        post_only: bool,
    },
    /// 已验证峰值的冰山载荷；当前仅支持 GTC。
    Iceberg {
        /// 冰山完整下单载荷。
        order: IcebergOrder,
        /// 是否要求只挂单。
        post_only: bool,
    },
}

/// 调用方已有的可信数值、逻辑时间、时段和成交身份输入。
///
/// 不推导资产、费率、保护价或资金需求；预留金融状态必须已由真实登记表持有。
/// 成交身份的容量与唯一性仍遵守既有参考撮合入口的调用方契约。
pub struct ReferencePlaceContext<'a> {
    /// 当前市场的数值规则。
    pub math: &'a MarketMath,
    /// 显式传入的逻辑时间，不读取墙上时钟。
    pub now: LogicalTime,
    /// 当前交易时段身份。
    pub current_session_id: u64,
    /// 足以覆盖完整预演的、由调用方分配的成交身份。
    pub trade_ids: &'a [TradeId],
}

/// 不可伪造的完整下单计划；同时固定簿前态、已验证后态与整批预留归属。
///
/// 金融状态、版本、费率和覆盖量不会因下单终结而改写；终结只转入 PendingR2。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferencePlacePlan {
    /// 预演开始时的完整参考簿，用于提交前漂移检测。
    before: ReferenceOrderBook,
    /// 已冻结的预演后参考簿状态。
    after: ReferenceOrderBook,
    /// 已验证的活动订单与 PendingR2 归属变更。
    bindings: PlannedBindingTransition,
    /// 必须原顺序发布的正常下单事件。
    events: Vec<EngineEvent>,
}

/// 完整冻结市价执行、报价余量及预留归属的独立参考计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceMarketPlan {
    /// 预演前参考簿。
    before_book: ReferenceOrderBook,
    /// 预演前金融登记表。
    before_registry: ReservationRegistry,
    /// 执行后的参考簿。
    after_book: ReferenceOrderBook,
    /// 终结归属迁移后的登记表。
    after_registry: ReservationRegistry,
    /// 原序业务事件。
    events: Vec<EngineEvent>,
    /// 报价模式使用金额；基础模式为空。
    quote_amounts: Option<(matching_domain::QuoteAmount, matching_domain::QuoteAmount)>,
}

/// 与新到订单金融接管无关的完整只读下单预演，供普通下单与撤单替换共同使用。
///
/// 已固定结构、时间、市场数值、撮合、余量挂单和终结挂单快照；不分配真实优先级。
pub(super) struct ReferencePlacePreflight {
    /// 预演开始时的参考簿快照。
    before: ReferenceOrderBook,
    /// 不含金融归属提交的撮合后参考簿快照。
    after: ReferenceOrderBook,
    /// 已通过结构与数值验证的普通限价订单载荷。
    accepted: LimitOrder,
    /// 在本次预演中成交或自成交防护终结、必须转入 PendingR2 的 maker 金融快照。
    terminal_makers: Vec<ReservationState>,
    /// 正常下单链产生的原序事件。
    events: Vec<EngineEvent>,
}

impl ReferenceOrderBook {
    /// 在独立参考副本中预演市价执行及全部终结预留归属。
    pub fn plan_market_with_registry(
        &self,
        request: crate::MarketRegistryOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: crate::MarketRegistryContext<'_>,
    ) -> Result<ReferenceMarketPlan, RejectReason> {
        let (id, user) = match &request {
            crate::MarketRegistryOrder::Base(order) => (order.order_id, order.user_id),
            crate::MarketRegistryOrder::Quote(order) => (order.order_id, order.user_id),
        };
        self.validate_new_reservation(id, user, &state)?;
        registry.validate_order_bindings(
            self.reservations
                .iter()
                .map(|(id, state)| (*id, state))
                .chain(std::iter::once((id, &state))),
        )?;
        let mut after_book = self.clone();
        let (events, quote_amounts) = match request {
            crate::MarketRegistryOrder::Base(order) => {
                let events = if let Some(protection) = context.protection {
                    after_book.execute_market_by_base_with_protection(
                        order,
                        protection,
                        context.math,
                        context.trade_ids,
                    )?
                } else {
                    after_book
                        .execute_market_by_base(order, context.trade_ids)?
                        .events()
                        .to_vec()
                };
                (events, None)
            }
            crate::MarketRegistryOrder::Quote(order) => {
                if let Some(protection) = context.protection {
                    let execution = after_book.execute_market_by_quote_with_protection(
                        order,
                        protection,
                        context.math,
                        context.trade_ids,
                    )?;
                    (
                        execution.events().to_vec(),
                        Some((execution.spent_quote(), execution.unused_quote())),
                    )
                } else {
                    let execution = after_book.execute_market_by_quote(
                        order,
                        context.math,
                        context.trade_ids,
                    )?;
                    (
                        execution.events().to_vec(),
                        Some((execution.spent_quote(), execution.unused_quote())),
                    )
                }
            }
        };
        let mut total = matching_domain::QuoteAmount::new(0);
        for event in &events {
            if let EngineEvent::Trade(trade) = event {
                total = total.checked_add(
                    context
                        .math
                        .trade_notional(trade.price, trade.qty)
                        .map_err(Self::reject_quote_math_error)?,
                )?;
            }
        }
        let mut terminal = vec![&state];
        for maker in &self.orders {
            let id = maker.original_order().order_id;
            if !after_book.contains_active_order(id) {
                terminal.push(
                    self.reservations
                        .get(&id)
                        .ok_or(RejectReason::InvalidReservation)?,
                );
            }
        }
        let transition = registry.plan_binding_transitions(terminal, [])?;
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh market binding plan");
        after_registry.validate_order_bindings(
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ReferenceMarketPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
            quote_amounts,
        })
    }

    /// 校验前态并提交市价结果与预留归属。
    pub fn apply_market_with_registry(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ReferenceMarketPlan,
    ) -> Result<crate::MarketRegistryResult, ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok((plan.events, plan.quote_amounts))
    }
    /// 在零真实写入的普通下单预演中收集所有终结归属并验证整批计划。
    ///
    /// 新到订单的真实金融状态须已登记为其 ActiveOrder；任何终结挂单缺少
    /// 匹配影子状态或活动归属均拒绝，禁止猜测预留。实际挂单余量保持活动归属。
    pub fn plan_place_with_registry(
        &self,
        request: ReferencePlaceOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: ReferencePlaceContext<'_>,
    ) -> Result<ReferencePlacePlan, RejectReason> {
        let accepted = match &request {
            ReferencePlaceOrder::Limit { order, .. } => order.clone(),
            ReferencePlaceOrder::Iceberg { order, .. } => order.order().clone(),
        };
        self.validate_new_reservation(accepted.order_id, accepted.user_id, &state)?;
        // 登记表是金融状态权威；完整核对当前影子及已接管但尚未执行的新到订单。
        registry.validate_order_bindings(
            self.reservations
                .iter()
                .map(|(id, state)| (*id, state))
                .chain(std::iter::once((accepted.order_id, &state))),
        )?;
        let preflight = self.preflight_place(request, context)?;
        self.bind_place_preflight(preflight, state, registry)
    }

    /// 在接管或过渡新到预留之前，完成全部结构、时间、数值和簿依赖校验。
    ///
    /// 只调用既有参考撮合入口；挂单、补量和优先级变化全部留在可丢弃副本内。
    pub(super) fn preflight_place(
        &self,
        request: ReferencePlaceOrder,
        context: ReferencePlaceContext<'_>,
    ) -> Result<ReferencePlacePreflight, RejectReason> {
        let accepted = match &request {
            ReferencePlaceOrder::Limit { order, .. } => order.clone(),
            ReferencePlaceOrder::Iceberg { order, .. } => order.order().clone(),
        };
        self.check_duplicate(accepted.order_id)?;
        if self.reservations.contains_key(&accepted.order_id) {
            return Err(RejectReason::InvalidReservation);
        }
        if accepted.price.get() % context.math.tick_size() != 0 {
            return Err(RejectReason::InvalidTick);
        }
        if !accepted.qty.get().is_multiple_of(context.math.lot_size()) {
            return Err(RejectReason::InvalidLot);
        }
        match &request {
            ReferencePlaceOrder::Limit {
                time_in_force,
                post_only,
                ..
            } => {
                if *post_only && *time_in_force != TimeInForce::Gtc {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                match time_in_force {
                    TimeInForce::Gtd { expire_at } if *expire_at <= context.now => {
                        return Err(RejectReason::InvalidExpireTime);
                    }
                    TimeInForce::Day { session_id }
                        if *session_id != context.current_session_id =>
                    {
                        return Err(RejectReason::InvalidSession);
                    }
                    _ => {}
                }
            }
            ReferencePlaceOrder::Iceberg { order, .. } => {
                if !order
                    .peak_size()
                    .get()
                    .is_multiple_of(context.math.lot_size())
                {
                    return Err(RejectReason::InvalidLot);
                }
            }
        }
        context
            .math
            .trade_notional(accepted.price, accepted.qty)
            .map_err(Self::reject_quote_math_error)?;

        let mut after = self.clone();
        let events = match request {
            ReferencePlaceOrder::Limit {
                order,
                time_in_force,
                post_only,
            } => {
                if post_only {
                    if time_in_force != TimeInForce::Gtc {
                        return Err(RejectReason::InvalidOrderCombination);
                    }
                    after.place_limit_post_only(order)?;
                    Vec::new()
                } else {
                    match time_in_force {
                        TimeInForce::Ioc => after
                            .place_limit_ioc(order, context.math, context.trade_ids)?
                            .events()
                            .to_vec(),
                        TimeInForce::Fok => after
                            .place_limit_fok(order, context.math, context.trade_ids)?
                            .events()
                            .to_vec(),
                        TimeInForce::Gtc | TimeInForce::Day { .. } | TimeInForce::Gtd { .. } => {
                            after.place_limit_timed_with_events(
                                order,
                                time_in_force,
                                context.now,
                                context.current_session_id,
                                context.trade_ids,
                            )?
                        }
                    }
                }
            }
            ReferencePlaceOrder::Iceberg { order, post_only } => {
                if post_only {
                    after.place_iceberg_post_only(order, context.math)?;
                    Vec::new()
                } else {
                    after.place_iceberg(order, context.math, context.trade_ids)?
                }
            }
        };

        // 既有普通 GTC 窄入口不接收数值规则；本边界在真实提交前统一核对
        // 预演得到的逐笔本金及整单累计本金，不复写其撮合选择或补量过程。
        events.iter().try_fold(0u128, |total, event| {
            if let EngineEvent::Trade(trade) = event {
                let notional = context
                    .math
                    .trade_notional(trade.price, trade.qty)
                    .map_err(Self::reject_quote_math_error)?;
                total
                    .checked_add(notional.get())
                    .ok_or(RejectReason::ArithmeticOverflow)
            } else {
                Ok(total)
            }
        })?;

        let mut terminal_makers = Vec::new();
        // 源 Vec 的顺序确定，不按 HashMap 的迭代顺序生成业务计划。
        for maker in &self.orders {
            let id = maker.original_order().order_id;
            if !after.contains_active_order(id) {
                terminal_makers.push(
                    self.reservations
                        .get(&id)
                        .ok_or(RejectReason::InvalidReservation)?
                        .clone(),
                );
            }
        }
        if let Some(resting) = after
            .orders
            .iter_mut()
            .find(|order| order.original_order().order_id == accepted.order_id)
            && resting.peak_size().is_none()
        {
            // 保留接收绝对总量，避免把入簿前的成交量丢给后续改单。
            resting.amend_regular_in_place(
                accepted.clone(),
                Qty::try_new(resting.total_remaining())?,
                resting.time_in_force(),
            );
        }
        Ok(ReferencePlacePreflight {
            before: self.clone(),
            after,
            accepted,
            terminal_makers,
            events,
        })
    }

    /// 将已完成的下单预演与真实登记表中的新到金融快照组合，不再撮合或校验数值。
    ///
    /// 归属校验只核对资金过渡得到的快照及已预检终结集合，不重跑结构或簿预检。
    pub(super) fn bind_place_preflight(
        &self,
        preflight: ReferencePlacePreflight,
        state: ReservationState,
        registry: &ReservationRegistry,
    ) -> Result<ReferencePlacePlan, RejectReason> {
        assert_eq!(
            *self, preflight.before,
            "reference preflight source must remain unchanged"
        );
        let ReferencePlacePreflight {
            before,
            mut after,
            accepted,
            terminal_makers,
            events,
        } = preflight;
        self.validate_new_reservation(accepted.order_id, accepted.user_id, &state)?;
        registry.validate_order_bindings(
            self.reservations
                .iter()
                .map(|(id, state)| (*id, state))
                .chain(std::iter::once((accepted.order_id, &state))),
        )?;
        let mut terminal = terminal_makers.iter().collect::<Vec<_>>();
        if after.contains_active_order(accepted.order_id) {
            assert!(
                after
                    .reservations
                    .insert(accepted.order_id, state.clone())
                    .is_none()
            );
        } else {
            terminal.push(&state);
        }
        let bindings = registry.plan_binding_transitions(terminal, [])?;
        let mut checked_registry = registry.clone();
        checked_registry
            .apply_binding_transition(bindings.clone())
            .expect("fresh reference binding plan must match its registry");
        checked_registry
            .validate_order_bindings(after.reservations.iter().map(|(id, state)| (*id, state)))?;
        Ok(ReferencePlacePlan {
            before,
            after,
            bindings,
            events,
        })
    }

    /// 完整前态匹配后，在同一个单写者边界提交预留归属和已验证参考簿。
    ///
    /// 漂移返回必须停止处理的不变量错误；首次真实写入后没有业务拒绝或回滚。
    /// 保留既有撮合事件原序，不生成资金释放事件或完整接受事件封装。
    pub fn apply_place_with_registry(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ReferencePlacePlan,
    ) -> Result<Vec<EngineEvent>, ReservationApplyError> {
        if *self != plan.before {
            return Err(ReservationApplyError::StateChanged);
        }
        registry.apply_binding_transition(plan.bindings)?;
        *self = plan.after;
        Ok(plan.events)
    }
}
