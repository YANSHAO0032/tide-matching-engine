//! 普通下单与真实预留归属的独立生产预演和提交。

use super::{ProductionCancelReplaceContext, ProductionOrderBook};
use matching_domain::{EngineEvent, IcebergOrder, LimitOrder, Qty, RejectReason, TimeInForce};
use matching_risk::{ReservationApplyError, ReservationRegistry, ReservationState};

/// 具有明确执行策略的普通下单输入。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProductionPlaceOrder {
    /// 普通限价单及有效期。
    Limit {
        /// 新到订单的完整载荷。
        order: LimitOrder,
        /// 订单有效期。
        time_in_force: TimeInForce,
        /// 是否只挂单。
        post_only: bool,
    },
    /// 冰山限价单及只挂单选项。
    Iceberg {
        /// 已验证峰值的冰山载荷。
        order: IcebergOrder,
        /// 是否只挂单。
        post_only: bool,
    },
}

/// 同时冻结生产簿与真实预留登记表的下单计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionPlacePlan {
    /// 计划建立时的完整生产簿。
    before_book: ProductionOrderBook,
    /// 计划建立时的金融登记表。
    before_registry: ReservationRegistry,
    /// 已完成生产撮合的订单簿后态。
    after_book: ProductionOrderBook,
    /// 终结归属已转入 PendingR2 的金融后态。
    after_registry: ReservationRegistry,
    /// 原序成交、取消与冰山事件。
    events: Vec<EngineEvent>,
}

/// 完整冻结市价执行、报价余量及预留归属的生产计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionMarketPlan {
    /// 预演前生产簿。
    before_book: ProductionOrderBook,
    /// 预演前金融登记表。
    before_registry: ReservationRegistry,
    /// 执行后的生产簿。
    after_book: ProductionOrderBook,
    /// 终结归属迁移后的登记表。
    after_registry: ReservationRegistry,
    /// 原序业务事件。
    events: Vec<EngineEvent>,
    /// 报价模式使用金额；基础模式为空。
    quote_amounts: Option<(matching_domain::QuoteAmount, matching_domain::QuoteAmount)>,
}

impl ProductionOrderBook {
    /// 预演市价执行并把所有终结预留统一计划到 PendingR2。
    pub fn plan_market_with_registry(
        &self,
        request: crate::MarketRegistryOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: crate::MarketRegistryContext<'_>,
    ) -> Result<ProductionMarketPlan, RejectReason> {
        self.plan_market_with_registry_inner(request, state, registry, context, None)
    }

    fn plan_market_with_registry_inner(
        &self,
        request: crate::MarketRegistryOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: crate::MarketRegistryContext<'_>,
        cursor: Option<&mut matching_domain::TradeIdCursor>,
    ) -> Result<ProductionMarketPlan, RejectReason> {
        let (id, user) = match &request {
            crate::MarketRegistryOrder::Base(order) => (order.order_id, order.user_id),
            crate::MarketRegistryOrder::Quote(order) => (order.order_id, order.user_id),
        };
        self.validate_new_reservation(id, user, &state)?;
        self.validate_registry_bindings(
            registry,
            self.reservations
                .iter()
                .map(|(id, state)| (*id, state))
                .chain(std::iter::once((id, &state))),
        )?;
        let mut after_book = self.clone();
        let (events, quote_amounts) = if let Some(cursor) = cursor {
            match request {
                crate::MarketRegistryOrder::Base(order) => {
                    let events = if let Some(protection) = context.protection {
                        order.validate_stp_mode()?;
                        if let Some((_, configured)) = after_book.price_band_config() {
                            assert_eq!(*context.math, configured, "市场数值配置不一致");
                        }
                        let mut frozen = protection.clone();
                        if let Some((lower, upper)) = after_book.price_band_bounds()? {
                            frozen.market_band_worst =
                                Some(if order.side == matching_domain::Side::Buy {
                                    upper
                                } else {
                                    lower
                                });
                        }
                        let boundary = matching_domain::market_order::effective_protection_at_command_start_with_source(order.market_protection.as_ref(), &frozen, order.side, context.math)?;
                        let mut source =
                            crate::trade_id_source::TradeIdSource::cursor(cursor.clone());
                        let events = after_book.execute_market_base_with_boundary_source(
                            order,
                            Some(context.math),
                            boundary,
                            &mut source,
                        )?;
                        *cursor = source.into_cursor().expect("游标来源");
                        events
                    } else {
                        after_book
                            .execute_market_by_base_with_cursor(order, context.math, cursor)?
                            .events()
                            .to_vec()
                    };
                    (events, None)
                }
                crate::MarketRegistryOrder::Quote(order) => {
                    if let Some(protection) = context.protection {
                        order.validate_stp_mode()?;
                        if let Some((_, configured)) = after_book.price_band_config() {
                            assert_eq!(*context.math, configured, "市场数值配置不一致");
                        }
                        let mut frozen = protection.clone();
                        if let Some((lower, upper)) = after_book.price_band_bounds()? {
                            frozen.market_band_worst =
                                Some(if order.side == matching_domain::Side::Buy {
                                    upper
                                } else {
                                    lower
                                });
                        }
                        let boundary = matching_domain::market_order::effective_protection_at_command_start_with_source(order.market_protection.as_ref(), &frozen, order.side, context.math)?;
                        let mut source =
                            crate::trade_id_source::TradeIdSource::cursor(cursor.clone());
                        let execution = after_book.execute_market_quote_with_boundary_source(
                            order,
                            context.math,
                            boundary,
                            &mut source,
                        )?;
                        *cursor = source.into_cursor().expect("游标来源");
                        (
                            execution.events().to_vec(),
                            Some((execution.spent_quote(), execution.unused_quote())),
                        )
                    } else {
                        let execution = after_book.execute_market_by_quote_with_cursor(
                            order,
                            context.math,
                            cursor,
                        )?;
                        (
                            execution.events().to_vec(),
                            Some((execution.spent_quote(), execution.unused_quote())),
                        )
                    }
                }
            }
        } else {
            match request {
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
                            .execute_market_by_base(order, context.trade_ids, context.math)?
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
        after_book.reference_prices =
            self.reference_prices
                .plan_trade_prices(events.iter().filter_map(|event| match event {
                    EngineEvent::Trade(trade) => Some(trade.price),
                    _ => None,
                }))?;
        let mut ids = self
            .orders
            .keys()
            .copied()
            .filter(|id| !after_book.orders.contains_key(id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        let mut terminal = vec![&state];
        for id in ids {
            terminal.push(
                self.reservations
                    .get(&id)
                    .ok_or(RejectReason::InvalidReservation)?,
            );
        }
        let transition = registry.plan_binding_transitions(terminal, [])?;
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh market binding plan");
        self.validate_registry_bindings(
            &after_registry,
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ProductionMarketPlan {
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
        plan: ProductionMarketPlan,
    ) -> Result<crate::MarketRegistryResult, ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok((plan.events, plan.quote_amounts))
    }
    /// 在生产副本上预演下单、数值及预留终结，不修改真实状态。
    pub fn plan_place_with_registry(
        &self,
        request: ProductionPlaceOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: ProductionCancelReplaceContext<'_>,
    ) -> Result<ProductionPlacePlan, RejectReason> {
        self.plan_place_with_registry_inner(request, state, registry, context, None)
    }

    fn plan_place_with_registry_inner(
        &self,
        request: ProductionPlaceOrder,
        state: ReservationState,
        registry: &ReservationRegistry,
        context: ProductionCancelReplaceContext<'_>,
        mut cursor: Option<&mut matching_domain::TradeIdCursor>,
    ) -> Result<ProductionPlacePlan, RejectReason> {
        if let Some((_, configured)) = self.price_band_config() {
            assert_eq!(*context.math, configured, "市场数值配置不一致");
        }
        let accepted = match &request {
            ProductionPlaceOrder::Limit { order, .. } => order.clone(),
            ProductionPlaceOrder::Iceberg { order, .. } => order.order().clone(),
        };
        self.validate_new_reservation(accepted.order_id, accepted.user_id, &state)?;
        self.validate_registry_bindings(
            registry,
            self.reservations
                .iter()
                .map(|(id, state)| (*id, state))
                .chain(std::iter::once((accepted.order_id, &state))),
        )?;
        accepted.validate_stp_mode()?;
        self.validate_band_price(accepted.price)?;
        if accepted.price.get() % context.math.tick_size() != 0 {
            return Err(RejectReason::InvalidTick);
        }
        if !accepted.qty.get().is_multiple_of(context.math.lot_size()) {
            return Err(RejectReason::InvalidLot);
        }
        context
            .math
            .trade_notional(accepted.price, accepted.qty)
            .map_err(Self::reject_quote_math_error)?;
        let mut after_book = self.clone();
        let events = match request {
            ProductionPlaceOrder::Limit {
                order,
                time_in_force,
                post_only,
            } => {
                if post_only && time_in_force != TimeInForce::Gtc {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                if post_only {
                    after_book.place_limit_post_only(order)?;
                    Vec::new()
                } else {
                    if let Some(cursor) = cursor.as_deref_mut() {
                        match time_in_force {
                            TimeInForce::Ioc | TimeInForce::Fok => after_book
                                .place_limit_immediate_with_cursor(
                                    order,
                                    context.math,
                                    time_in_force == TimeInForce::Fok,
                                    cursor,
                                )?
                                .events()
                                .to_vec(),
                            _ => {
                                Self::validate_resting_tif(
                                    time_in_force,
                                    context.now,
                                    context.current_session_id,
                                )?;
                                let id = order.order_id;
                                let events =
                                    after_book.place_limit_gtc_with_cursor(order, cursor)?;
                                if let Some(index) = after_book.orders.get(&id).copied() {
                                    after_book
                                        .arena
                                        .get_mut(index)
                                        .expect("planned residual")
                                        .replace_initial_gtc_with_tif(time_in_force);
                                    after_book.regular_expiry.insert(id, time_in_force);
                                }
                                events
                            }
                        }
                    } else {
                        match time_in_force {
                            TimeInForce::Ioc => after_book
                                .place_limit_ioc(order, context.math, context.trade_ids)?
                                .events()
                                .to_vec(),
                            TimeInForce::Fok => after_book
                                .place_limit_fok(order, context.math, context.trade_ids)?
                                .events()
                                .to_vec(),
                            _ => after_book.place_limit_timed_with_events(
                                order,
                                time_in_force,
                                context.now,
                                context.current_session_id,
                                context.trade_ids,
                            )?,
                        }
                    }
                }
            }
            ProductionPlaceOrder::Iceberg { order, post_only } => {
                if !order
                    .peak_size()
                    .get()
                    .is_multiple_of(context.math.lot_size())
                {
                    return Err(RejectReason::InvalidLot);
                }
                if post_only {
                    after_book.place_iceberg_post_only(order, context.math)?;
                    Vec::new()
                } else {
                    if let Some(cursor) = cursor {
                        after_book.place_iceberg_with_cursor(order, context.math, cursor)?
                    } else {
                        after_book.place_iceberg(order, context.math, context.trade_ids)?
                    }
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
        after_book.reference_prices =
            self.reference_prices
                .plan_trade_prices(events.iter().filter_map(|event| match event {
                    EngineEvent::Trade(trade) => Some(trade.price),
                    _ => None,
                }))?;
        let mut ids = self
            .orders
            .keys()
            .copied()
            .filter(|id| !after_book.orders.contains_key(id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        let mut terminal = Vec::new();
        for id in ids {
            terminal.push(
                self.reservations
                    .get(&id)
                    .ok_or(RejectReason::InvalidReservation)?,
            );
        }
        if let Some(index) = after_book.orders.get(&accepted.order_id).copied() {
            let node = after_book.arena.get_mut(index).expect("planned residual");
            if node.peak_size().is_none() {
                let remaining = Qty::try_new(node.total_remaining())?;
                node.amend_regular_in_place(accepted.clone(), remaining, node.time_in_force());
            }
            assert!(
                after_book
                    .reservations
                    .insert(accepted.order_id, state.clone())
                    .is_none()
            );
        } else {
            terminal.push(&state);
        }
        let transition = registry.plan_binding_transitions(terminal, [])?;
        let mut after_registry = registry.clone();
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh placement binding plan");
        self.validate_registry_bindings(
            &after_registry,
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ProductionPlacePlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }

    /// 校验计划前态并一次安装订单簿与预留归属后态。
    pub fn apply_place_with_registry(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ProductionPlacePlan,
    ) -> Result<Vec<EngineEvent>, ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok(plan.events)
    }

    /// 在候选簿、登记表和游标中预演市价，成功时原子提交。
    pub fn market_with_registry_and_cursor(
        &mut self,
        request: crate::MarketRegistryOrder,
        state: ReservationState,
        registry: &mut ReservationRegistry,
        context: crate::MarketRegistryContext<'_>,
        cursor: &mut matching_domain::TradeIdCursor,
    ) -> Result<crate::MarketRegistryResult, RejectReason> {
        let mut candidate = cursor.clone();
        let plan = self.plan_market_with_registry_inner(
            request,
            state,
            registry,
            context,
            Some(&mut candidate),
        )?;
        let result = self
            .apply_market_with_registry(registry, plan)
            .expect("fresh market plan");
        *cursor = candidate;
        Ok(result)
    }

    /// 在候选簿、登记表与游标上预演下单，全部成功后一次安装。
    pub fn place_with_registry_and_cursor(
        &mut self,
        request: ProductionPlaceOrder,
        state: ReservationState,
        registry: &mut ReservationRegistry,
        context: ProductionCancelReplaceContext<'_>,
        cursor: &mut matching_domain::TradeIdCursor,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let mut candidate = cursor.clone();
        let plan = self.plan_place_with_registry_inner(
            request,
            state,
            registry,
            context,
            Some(&mut candidate),
        )?;
        let events = self
            .apply_place_with_registry(registry, plan)
            .expect("fresh placement plan");
        *cursor = candidate;
        Ok(events)
    }
}
