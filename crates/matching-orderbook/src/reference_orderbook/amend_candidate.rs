//! 参考订单簿的只读改单候选与提交计划。

use super::iceberg::SliceRequest;
use super::{ReferenceOrderBook, ReferenceStopOrder};
use crate::ReferenceOrder;
use matching_domain::{
    AmendOrder, AmendQuantityMode, AmendedQuantity, EngineEvent, LimitOrder, LogicalTime, Price,
    Qty, QueuePriority, RegularAmendKind, RejectReason, StopMarketQuantity, StopPriority,
    TimeInForce, TradeId, TriggerExpiry, TriggerExpiryPatch, classify_regular_amend,
    remaining_after_target_total, validate_amend_quantity_mode,
};
use matching_risk::ReservationState;

/// 后续无失败提交所需的预留身份版本快照。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReservationSnapshot {
    /// 本地预留的稳定身份。
    pub(crate) reservation_id: matching_domain::ReservationId,
    /// 成功应用调整前必须保持的乐观版本。
    pub(crate) version: u64,
}

/// 参考簿常规限价订单的只读提交计划。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RegularAmendPlan {
    /// 保持不变的订单身份。
    pub(crate) order_id: matching_domain::OrderId,
    /// 原订单方向。
    pub(crate) side: matching_domain::Side,
    /// 修改前价格。
    pub(crate) old_price: Price,
    /// 修改后的候选价格。
    pub(crate) new_price: Price,
    /// 原始总基础数量。
    pub(crate) original_total: Qty,
    /// 已完成成交的基础数量。
    pub(crate) already_filled: u64,
    /// 修改前总余量。
    pub(crate) old_remaining: Qty,
    /// 重新撮合前的候选总余量。
    pub(crate) new_remaining: Qty,
    /// 共享领域语义确定的优先级动作。
    pub(crate) kind: RegularAmendKind,
    /// 当前或目标普通订单有效期。
    pub(crate) target_tif: TimeInForce,
    /// 当前市场本地排队优先级。
    pub(crate) priority: QueuePriority,
    /// 关联预留的不可变身份版本。
    pub(crate) reservation: ReservationSnapshot,
}

/// 参考簿冰山订单的只读提交计划。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IcebergAmendPlan {
    /// 通用限价候选。
    pub(crate) regular: RegularAmendPlan,
    /// 修改前可见余量。
    pub(crate) old_visible_remaining: Qty,
    /// 修改前隐藏余量。
    pub(crate) old_hidden_remaining: u64,
    /// 修改前峰值。
    pub(crate) old_peak: Qty,
    /// 修改后的显示峰值。
    pub(crate) new_peak: Qty,
}

/// 未触发 Stop 的只读候选；当前只保留可由既有运行态表达的字段。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StopAmendPlan {
    /// 原触发源与原市价保护属于不可变候选快照。
    pub(crate) old_reference: matching_domain::PriceReferenceKind,
    /// 候选触发来源。
    pub(crate) new_reference: matching_domain::PriceReferenceKind,
    /// 原市价保护模板。
    pub(crate) old_protection: Option<matching_domain::MarketProtection>,
    /// 候选市价保护模板。
    pub(crate) new_protection: Option<matching_domain::MarketProtection>,
    /// 订单身份。
    pub(crate) order_id: matching_domain::OrderId,
    /// 当前 StopPriority。
    pub(crate) priority: StopPriority,
    /// 修改前触发价。
    pub(crate) old_stop_price: Price,
    /// 修改后的触发价。
    pub(crate) new_stop_price: Price,
    /// 当前或补丁后的触发前有效期。
    pub(crate) target_expiry: TriggerExpiry,
    /// 关联预留身份版本。
    pub(crate) reservation: ReservationSnapshot,
}

/// 两类订单容器各自生成的只读改单计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReferenceAmendPlan {
    /// 普通限价订单候选。
    Regular(RegularAmendPlan),
    /// 冰山订单候选。
    Iceberg(IcebergAmendPlan),
    /// 未触发 Stop 候选。
    Stop(StopAmendPlan),
}

/// 独立参考改单与资金归属的一次性完整后态计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceAmendRegistryPlan {
    /// 预演前参考簿。
    before_book: ReferenceOrderBook,
    /// 预演前真实金融登记表。
    before_registry: matching_risk::ReservationRegistry,
    /// 改单后的参考簿。
    after_book: ReferenceOrderBook,
    /// 验签调整和终结归属后的登记表。
    after_registry: matching_risk::ReservationRegistry,
    /// 原序改单、成交和取消事件。
    events: Vec<EngineEvent>,
}

impl ReferenceOrderBook {
    /// 在独立参考副本中预演改单、签名调整和终结预留归属。
    pub fn plan_amend_with_registry(
        &self,
        amend: &AmendOrder,
        registry: &matching_risk::ReservationRegistry,
        context: crate::AmendRegistryContext<'_>,
    ) -> Result<ReferenceAmendRegistryPlan, RejectReason> {
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;
        let mut after_book = self.clone();
        let events = after_book.amend_with_context(
            amend,
            context.now,
            context.current_session_id,
            context.trade_ids,
            context.execution,
        )?;
        let mut after_registry = registry.clone();
        if let Some(token) = &amend.reservation_adjustment {
            let state = self
                .reservations
                .get(&amend.order_id)
                .ok_or(RejectReason::InvalidReservation)?;
            let expected = matching_risk::ExpectedAmendReservation {
                command_id: context.execution.command_id,
                reservation_id: state.reservation_id,
                order_id: amend.order_id,
                user_id: amend.user_id,
                market_id: amend.market_id,
                fee_schedule_version: context.execution.fee_schedule_version,
                fee_tier_id: context.execution.fee_tier_id,
                required_reserves: context.execution.required_reserves.clone(),
            };
            let adjustment = after_registry.plan_amend_adjustment(
                context.execution.risk_keys,
                &expected,
                token,
            )?;
            after_registry
                .apply_amend_adjustment(adjustment)
                .expect("fresh verified amend adjustment");
        }
        let mut ids = self
            .reservations
            .iter()
            .filter(|(id, _)| !after_book.reservations.contains_key(id))
            .map(|(id, state)| (*id, state.reservation_id))
            .collect::<Vec<_>>();
        ids.sort_unstable_by_key(|(id, _)| *id);
        let transition = after_registry.plan_binding_transitions(
            ids.iter().map(|(_, id)| {
                after_registry
                    .get(*id)
                    .expect("verified terminal reservation")
            }),
            [],
        )?;
        after_registry
            .apply_binding_transition(transition)
            .expect("fresh amend bindings");
        after_registry.validate_order_bindings(
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ReferenceAmendRegistryPlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }

    /// 校验完整前态并原子安装参考改单和资金归属后态。
    pub fn apply_amend_with_registry(
        &mut self,
        registry: &mut matching_risk::ReservationRegistry,
        plan: ReferenceAmendRegistryPlan,
    ) -> Result<Vec<EngineEvent>, matching_risk::ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(matching_risk::ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok(plan.events)
    }
    /// 使用实际市场数值规则、候选资金需求和已加载公钥执行普通、冰山或未触发止损改单。
    ///
    /// 预留调整先在副本中验签和检查版本容量；任何后续簿预检失败均丢弃副本。
    /// 成功后仅对仍活动的同一订单替换预留；全成订单保持终结清理结果。
    pub fn amend_with_context(
        &mut self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
        trade_ids: &[TradeId],
        context: &crate::AmendExecutionContext<'_>,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let candidate = self.build_amend_plan(amend, now, current_session_id)?;
        self.preflight_amend_numeric(amend, &candidate, context)?;
        let planned = context.prepare(
            amend,
            self.reservations
                .get(&amend.order_id)
                .ok_or(RejectReason::InvalidReservation)?,
        )?;
        let mut verified_amend = amend.clone();
        verified_amend.reservation_adjustment = None;
        let events = if self
            .stop_orders
            .iter()
            .any(|entry| entry.order.order_id() == amend.order_id)
        {
            self.amend_stop_with_events(&verified_amend, now, current_session_id)?
        } else {
            self.amend_regular_with_events(&verified_amend, now, current_session_id, trade_ids)?
        };
        if let Some(state) = self.reservations.get_mut(&amend.order_id) {
            *state = planned;
        }
        Ok(events)
    }

    /// 参考模型从自身候选和源 Vec 独立规划数值失败，不读取生产计划。
    fn preflight_amend_numeric(
        &self,
        amend: &AmendOrder,
        candidate: &ReferenceAmendPlan,
        context: &crate::AmendExecutionContext<'_>,
    ) -> Result<(), RejectReason> {
        let regular = match candidate {
            ReferenceAmendPlan::Regular(plan) => Some(plan),
            ReferenceAmendPlan::Iceberg(plan) => {
                context.validate_qty(plan.new_peak)?;
                Some(&plan.regular)
            }
            ReferenceAmendPlan::Stop(plan) => {
                context.validate_price(plan.new_stop_price)?;
                if let Some(price) = plan.new_protection.as_ref().and_then(|p| p.worst_price) {
                    context.validate_price(price)?;
                }
                let stop = &self
                    .stop_orders
                    .iter()
                    .find(|entry| entry.order.order_id() == plan.order_id)
                    .expect("planned reference Stop")
                    .order;
                match stop {
                    matching_domain::StopOrder::Limit(order) => {
                        if context.fee_schedule_version != order.fee_schedule_version {
                            return Err(RejectReason::InvalidReservation);
                        }
                        let price = amend.new_price.unwrap_or(order.price);
                        let qty = amend.new_qty.unwrap_or(order.qty);
                        context
                            .market_math
                            .trade_notional(price, qty)
                            .map_err(Self::reject_quote_math_error)?;
                    }
                    matching_domain::StopOrder::Market(order) => {
                        if context.fee_schedule_version != order.fee_schedule_version {
                            return Err(RejectReason::InvalidReservation);
                        }
                        if let StopMarketQuantity::Base(qty) = order.quantity {
                            context.validate_qty(amend.new_qty.unwrap_or(qty))?;
                        }
                    }
                }
                None
            }
        };
        if let Some(plan) = regular {
            context
                .market_math
                .trade_notional(plan.new_price, plan.new_remaining)
                .map_err(Self::reject_quote_math_error)?;
            if plan.new_price != plan.old_price {
                let original = self
                    .orders
                    .iter()
                    .find(|maker| maker.original_order().order_id == plan.order_id)
                    .expect("validated reference Amend order must remain active")
                    .original_order();
                self.plan_slices(SliceRequest {
                    id: plan.order_id,
                    user_id: original.user_id,
                    stp_key: original.stp_key,
                    stp_mode: original.stp_mode,
                    side: plan.side,
                    remaining: plan.new_remaining.get(),
                    limit: Some(plan.new_price),
                    budget: None,
                    math: Some(&context.market_math),
                    protection: None,
                    one_fill: false,
                })?;
            }
        }
        Ok(())
    }

    /// 以参考 Vec 路径提交一张常规限价订单的改单。
    ///
    /// 目标数量为包含已成交量的绝对总量，必须严格大于已成交量。同价减量
    /// 保留队列位置；增量或改价失去优先级，只有实际挂单才分配新优先级。
    /// 所有成交事件位于 OrderAmended 之后、订单生命周期事件之前。
    ///
    /// 接受关联本地预留快照的普通限价单及冰山；未触发止损使用独立入口。
    /// 这是由调用方完成市场数值和资金覆盖校验后的低层簿接口，缺少核验上下文时显式拒绝令牌。
    /// 需要市场步长、本金、资金覆盖或验签的命令必须调用 `amend_with_context`；本接口成功时保留活动
    /// 订单的预留内容和版本，完全成交终结时删除预留。
    ///
    /// # 不变量断言
    ///
    /// 调用方提供的 TradeId 不足时，在首次结构写入前停止处理；多余标识符不使用。
    pub fn amend_regular_with_events(
        &mut self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let plan = match self.build_amend_plan(amend, now, current_session_id)? {
            ReferenceAmendPlan::Regular(plan) => plan,
            ReferenceAmendPlan::Iceberg(plan) => {
                return self.commit_iceberg_amend(plan, amend, trade_ids);
            }
            ReferenceAmendPlan::Stop(_) => {
                return Err(RejectReason::InvalidOrderCombination);
            }
        };
        // 当前阶段不能静默忽略已签名的预留调整授权。
        if amend.reservation_adjustment.is_some() {
            return Err(RejectReason::InvalidReservation);
        }
        let reservation = self
            .reservations
            .get(&plan.order_id)
            .cloned()
            .ok_or(RejectReason::InvalidReservation)?;
        let index = self
            .orders
            .iter()
            .position(|order| order.original_order().order_id == plan.order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        let original = self.orders[index].original_order().clone();
        let amended = Self::amended_payload(&original, plan)?;
        let amended_event = Self::amended_event(plan);

        match plan.kind {
            RegularAmendKind::InPlaceReduce => {
                let order = &mut self.orders[index];
                order.amend_regular_in_place(amended, plan.new_remaining, plan.target_tif);
                Ok(vec![amended_event])
            }
            RegularAmendKind::RequeueSamePrice => {
                // 与既有挂单预检顺序一致，先只读验证优先级，再检查目标档聚合。
                if self.next_priority >= u128::MAX - super::PRIORITY_GUARD {
                    return Err(RejectReason::PrioritySpaceExhaustion);
                }
                Self::preflight_amend_level(&self.orders, plan, plan.new_remaining)?;
                let priority = self.allocate_amend_priority()?;
                let replacement = ReferenceOrder::amended_regular(
                    amended,
                    plan.new_remaining,
                    priority,
                    plan.target_tif,
                );
                self.orders.remove(index);
                self.orders.push(replacement);
                Ok(vec![amended_event])
            }
            RegularAmendKind::RepriceAndMatch => {
                self.commit_reprice_plan(plan, amended, reservation, amended_event, trade_ids)
            }
        }
    }

    /// 独立扫描删除旧单后的目标档，保证新余量与档位数量均可表示。
    pub(super) fn preflight_amend_level(
        orders: &[ReferenceOrder],
        plan: RegularAmendPlan,
        remaining: Qty,
    ) -> Result<(), RejectReason> {
        let mut total = remaining.get();
        let mut count = 1u32;
        for entry in orders.iter().filter(|entry| {
            let original = entry.original_order();
            original.order_id != plan.order_id
                && original.side == plan.side
                && original.price == plan.new_price
        }) {
            total = total
                .checked_add(entry.visible_remaining())
                .ok_or(RejectReason::ArithmeticOverflow)?;
            count = count
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        Ok(())
    }

    /// 构造新绝对总量的运行态订单载荷；已成交量继续由总量减当前余量推导。
    pub(super) fn amended_payload(
        original: &LimitOrder,
        plan: RegularAmendPlan,
    ) -> Result<LimitOrder, RejectReason> {
        let target_total = plan
            .already_filled
            .checked_add(plan.new_remaining.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let mut amended = original.clone();
        amended.price = plan.new_price;
        amended.qty = Qty::try_new(target_total)?;
        Ok(amended)
    }

    /// 生成固定在所有成交前的改单事件。
    pub(super) fn amended_event(plan: RegularAmendPlan) -> EngineEvent {
        EngineEvent::OrderAmended {
            order_id: plan.order_id,
            old_price: Some(plan.old_price),
            new_price: Some(plan.new_price),
            old_quantity: AmendedQuantity::BaseRemaining(plan.old_remaining),
            new_quantity: AmendedQuantity::BaseRemaining(plan.new_remaining),
            priority_retained: matches!(plan.kind, RegularAmendKind::InPlaceReduce),
        }
    }

    /// 在写入前预检并取得一次新的本地队列优先级。
    fn allocate_amend_priority(&mut self) -> Result<QueuePriority, RejectReason> {
        if self.next_priority >= u128::MAX - super::PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let priority = QueuePriority::new(self.next_priority);
        self.next_priority = self
            .next_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;
        Ok(priority)
    }

    /// 以独立的参考 Vec 虚拟成交计划提交改价订单。
    fn commit_reprice_plan(
        &mut self,
        plan: RegularAmendPlan,
        amended: LimitOrder,
        reservation: ReservationState,
        amended_event: EngineEvent,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let mut slice = self.plan_slices(SliceRequest {
            id: plan.order_id,
            user_id: amended.user_id,
            stp_key: amended.stp_key,
            stp_mode: amended.stp_mode,
            side: plan.side,
            remaining: plan.new_remaining.get(),
            limit: Some(plan.new_price),
            budget: None,
            math: None,
            protection: None,
            one_fill: false,
        })?;
        let required_trade_ids = slice
            .events
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count();
        assert!(
            trade_ids.len() >= required_trade_ids,
            "insufficient TradeIds for preflighted reference Amend reprice"
        );

        let residual = if slice.remaining == 0 || slice.taker_terminal {
            None
        } else {
            Some(Qty::try_new(slice.remaining)?)
        };
        let residual_priority = if residual.is_none() {
            None
        } else {
            if slice.next_priority >= u128::MAX - super::PRIORITY_GUARD {
                return Err(RejectReason::PrioritySpaceExhaustion);
            }
            let priority = QueuePriority::new(slice.next_priority);
            slice.next_priority = slice
                .next_priority
                .checked_add(1)
                .ok_or(RejectReason::PrioritySpaceExhaustion)?;
            Some(priority)
        };
        if let Some(remaining) = residual {
            Self::preflight_amend_level(&slice.orders, plan, remaining)?;
        }

        let mut ids = trade_ids.iter().copied();
        let preserve_order = amended.stp_mode != matching_domain::StpMode::None;
        let mut trades = Vec::with_capacity(required_trade_ids);
        let mut order_events = Vec::new();
        for mut event in slice.events {
            if let EngineEvent::Trade(trade) = &mut event {
                trade.trade_id = ids.next().expect("preflighted TradeId");
                if preserve_order {
                    order_events.push(event);
                } else {
                    trades.push(event);
                }
            } else {
                order_events.push(event);
            }
        }
        let amended_index = slice
            .orders
            .iter()
            .position(|order| order.original_order().order_id == plan.order_id)
            .expect("reference Amend plan retains its original order until replacement");
        slice.orders.remove(amended_index);
        if let (Some(priority), Some(residual)) = (residual_priority, residual) {
            slice.orders.push(ReferenceOrder::amended_regular(
                amended,
                residual,
                priority,
                plan.target_tif,
            ));
        }

        if !slice.taker_terminal && required_trade_ids > 0 {
            order_events.push(if slice.remaining == 0 {
                EngineEvent::OrderFilled {
                    order_id: plan.order_id,
                }
            } else {
                EngineEvent::OrderPartiallyFilled {
                    order_id: plan.order_id,
                    remaining: residual.expect("positive reprice residual has Qty"),
                }
            });
        }

        // 写屏障：到此所有可预期拒绝、算术、优先级和 TradeId 容量均已完成。
        self.orders = slice.orders;
        self.next_priority = slice.next_priority;
        self.retain_active_reservations();
        // 余量仍在簿中才保留同一 shadow state；订单终结沿用既有终结清理规则。
        if self
            .orders
            .iter()
            .any(|order| order.original_order().order_id == plan.order_id)
        {
            // 低层簿提交保留活动预留；带上下文的入口在所有簿预检成功后替换已验证目标。
            self.reservations.insert(plan.order_id, reservation);
        }

        let mut events = Vec::with_capacity(1 + trades.len() + order_events.len());
        events.push(amended_event);
        events.extend(trades);
        events.extend(order_events);
        Ok(events)
    }

    /// 从参考簿不可变快照构造改单计划，不触碰任何运行态或分配器。
    pub(crate) fn build_amend_plan(
        &self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<ReferenceAmendPlan, RejectReason> {
        if let Some(order) = self
            .orders
            .iter()
            .find(|entry| entry.original_order().order_id == amend.order_id)
        {
            let reservation = self.reservation_snapshot(amend)?;
            return Self::build_regular_plan(order, amend, now, current_session_id, reservation);
        }
        if let Some(stop) = self
            .stop_orders
            .iter()
            .find(|entry| entry.order.order_id() == amend.order_id)
        {
            let reservation = self.reservation_snapshot(amend)?;
            return Self::build_stop_plan(stop, amend, reservation);
        }
        Err(RejectReason::OrderNotFound)
    }

    /// 核对本地 shadow state 与命令身份，不复制 reserve 容器。
    fn reservation_snapshot(
        &self,
        amend: &AmendOrder,
    ) -> Result<ReservationSnapshot, RejectReason> {
        let state = self
            .reservations
            .get(&amend.order_id)
            .ok_or(RejectReason::InvalidReservation)?;
        if state.order_id != amend.order_id
            || state.user_id != amend.user_id
            || state.market_id != amend.market_id
        {
            return Err(RejectReason::InvalidReservation);
        }
        Ok(ReservationSnapshot {
            reservation_id: state.reservation_id,
            version: state.version,
        })
    }

    /// 以独立 Vec 运行态计算普通或冰山数量候选。
    fn build_regular_plan(
        order: &crate::ReferenceOrder,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
        reservation: ReservationSnapshot,
    ) -> Result<ReferenceAmendPlan, RejectReason> {
        if order.original_order().user_id != amend.user_id
            || amend.new_stop_price.is_some()
            || amend.new_trigger_reference.is_some()
            || amend.new_trigger_expiry.is_some()
            || amend.new_market_protection.is_some()
        {
            return Err(RejectReason::InvalidOrderCombination);
        }
        validate_amend_quantity_mode(
            AmendQuantityMode::BaseQty,
            amend.new_qty,
            amend.new_quote_budget,
        )?;
        let old_remaining = Qty::try_new(order.total_remaining())?;
        let new_remaining =
            remaining_after_target_total(order.original_order().qty, old_remaining, amend.new_qty)?;
        let target_tif = amend.new_tif.unwrap_or(order.time_in_force());
        Self::validate_regular_tif(target_tif, now, current_session_id)?;
        let new_price = amend.new_price.unwrap_or(order.original_order().price);
        let already_filled = order
            .original_order()
            .qty
            .get()
            .checked_sub(old_remaining.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let regular = RegularAmendPlan {
            order_id: amend.order_id,
            side: order.original_order().side,
            old_price: order.original_order().price,
            new_price,
            original_total: order.original_order().qty,
            already_filled,
            old_remaining,
            new_remaining,
            kind: classify_regular_amend(
                order.original_order().price,
                new_price,
                old_remaining,
                new_remaining,
            ),
            target_tif,
            priority: order.priority(),
            reservation,
        };
        let Some(old_peak) = order.peak_size() else {
            if amend.new_visible_qty.is_some() {
                return Err(RejectReason::InvalidIceberg);
            }
            return Ok(ReferenceAmendPlan::Regular(regular));
        };
        let new_peak = amend.new_visible_qty.unwrap_or(old_peak);
        if new_peak >= amend.new_qty.unwrap_or(regular.original_total) {
            return Err(RejectReason::InvalidIceberg);
        }
        Ok(ReferenceAmendPlan::Iceberg(IcebergAmendPlan {
            regular,
            old_visible_remaining: Qty::try_new(order.visible_remaining())?,
            old_hidden_remaining: order.hidden_remaining(),
            old_peak,
            new_peak,
        }))
    }

    /// 构造未触发 Stop 的纯快照，保留原始及候选触发源与市价保护。
    fn build_stop_plan(
        stop: &ReferenceStopOrder,
        amend: &AmendOrder,
        reservation: ReservationSnapshot,
    ) -> Result<ReferenceAmendPlan, RejectReason> {
        if amend.new_visible_qty.is_some() {
            return Err(RejectReason::InvalidOrderCombination);
        }
        match &stop.order {
            matching_domain::StopOrder::Limit(order) => {
                if order.user_id != amend.user_id {
                    return Err(RejectReason::OrderNotFound);
                }
                validate_amend_quantity_mode(
                    AmendQuantityMode::BaseQty,
                    amend.new_qty,
                    amend.new_quote_budget,
                )?;
            }
            matching_domain::StopOrder::Market(order) => {
                if order.user_id != amend.user_id {
                    return Err(RejectReason::OrderNotFound);
                }
                let mode = match order.quantity {
                    StopMarketQuantity::Base(_) => AmendQuantityMode::BaseQty,
                    StopMarketQuantity::Quote(_) => AmendQuantityMode::UntriggeredStopMarketByQuote,
                };
                validate_amend_quantity_mode(mode, amend.new_qty, amend.new_quote_budget)?;
            }
        }
        let new_reference = amend
            .new_trigger_reference
            .unwrap_or(stop.order.trigger_reference());
        if new_reference == matching_domain::PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        let old_protection = stop.order.market_protection().cloned();
        let new_protection = match &amend.new_market_protection {
            None | Some(matching_domain::MarketProtectionPatch::Keep) => old_protection.clone(),
            Some(matching_domain::MarketProtectionPatch::Clear) => None,
            Some(matching_domain::MarketProtectionPatch::Set(protection)) => {
                if matches!(stop.order, matching_domain::StopOrder::Limit(_)) {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                Some(protection.clone())
            }
        };
        if new_protection
            .as_ref()
            .and_then(|p| p.max_slippage_bps)
            .is_some_and(|bps| bps >= 10_000)
        {
            return Err(RejectReason::InvalidSlippageBps);
        }
        let target_expiry = match amend.new_trigger_expiry {
            None | Some(TriggerExpiryPatch::Keep) => stop.order.trigger_expiry(),
            Some(TriggerExpiryPatch::ClearToGtc) => TriggerExpiry::Gtc,
            Some(TriggerExpiryPatch::Set(expiry)) => expiry,
        };
        Ok(ReferenceAmendPlan::Stop(StopAmendPlan {
            old_reference: stop.order.trigger_reference(),
            new_reference,
            old_protection,
            new_protection,
            order_id: amend.order_id,
            priority: stop.priority,
            old_stop_price: stop.order.stop_price(),
            new_stop_price: amend.new_stop_price.unwrap_or(stop.order.stop_price()),
            target_expiry,
            reservation,
        }))
    }

    /// 只读校验普通挂单迁移后的有效期边界。
    fn validate_regular_tif(
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<(), RejectReason> {
        match tif {
            TimeInForce::Gtc => Ok(()),
            TimeInForce::Gtd { expire_at } if expire_at > now => Ok(()),
            TimeInForce::Gtd { .. } => Err(RejectReason::InvalidExpireTime),
            TimeInForce::Day { session_id } if session_id == current_session_id => Ok(()),
            TimeInForce::Day { .. } => Err(RejectReason::InvalidSession),
            TimeInForce::Ioc | TimeInForce::Fok => Err(RejectReason::InvalidOrderCombination),
        }
    }
}
