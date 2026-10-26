//! 生产订单簿的只读改单候选与提交计划。

use super::iceberg::{Budget, IcebergPlan};
use super::resting::commit_rest_target;
use super::{ProductionOrderBook, RestPlan, RestTargetPlan};
use crate::{OrderIndex, OrderNode};
use matching_domain::{
    AmendOrder, AmendQuantityMode, AmendedQuantity, EngineEvent, LimitGtcOrder, LogicalTime, Price,
    Qty, QueuePriority, RegularAmendKind, RejectReason, StopMarketQuantity, StopPriority,
    TimeInForce, TradeId, TriggerExpiry, TriggerExpiryPatch, classify_regular_amend,
    remaining_after_target_total, validate_amend_quantity_mode,
};

/// 后续无失败提交所需的预留身份版本快照。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReservationSnapshot {
    /// 本地预留的稳定身份。
    pub(crate) reservation_id: matching_domain::ReservationId,
    /// 成功应用调整前必须保持的乐观版本。
    pub(crate) version: u64,
}

/// 生产簿常规限价订单的只读提交计划。
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
    /// 修改前普通订单有效期，用于迁移二级到期索引。
    pub(crate) old_tif: TimeInForce,
    /// 当前市场本地排队优先级。
    pub(crate) priority: QueuePriority,
    /// 关联预留的不可变身份版本。
    pub(crate) reservation: ReservationSnapshot,
}

/// 生产簿冰山订单的只读提交计划。
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

/// 生产数据结构生成的只读改单计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProductionAmendPlan {
    /// 普通限价订单候选。
    Regular(RegularAmendPlan),
    /// 冰山订单候选。
    Iceberg(IcebergAmendPlan),
    /// 未触发 Stop 候选。
    Stop(StopAmendPlan),
}

/// 改价写屏障之前冻结的全部生产成交及余量提交输入。
struct RegularRepriceCommitPlan {
    /// 不可变候选身份与时效。
    candidate: RegularAmendPlan,
    /// 保存目标绝对总量的订单载荷。
    amended: LimitGtcOrder,
    /// 生产价档和先进先出链生成的全部片成交。
    fills: IcebergPlan,
    /// 只有正余量才有挂单与新优先级计划。
    residual: Option<(Qty, RestPlan)>,
}

impl ProductionOrderBook {
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
        let events = if self.stop_book.get(amend.order_id).is_some() {
            self.amend_stop_with_events(&verified_amend, now, current_session_id)?
        } else {
            self.amend_regular_with_events(&verified_amend, now, current_session_id, trade_ids)?
        };
        if let Some(state) = self.reservations.get_mut(&amend.order_id) {
            *state = planned;
        }
        Ok(events)
    }

    /// 用生产候选和真实价档独立核对全部数值，早于资金副本与任何结构写入。
    fn preflight_amend_numeric(
        &self,
        amend: &AmendOrder,
        candidate: &ProductionAmendPlan,
        context: &crate::AmendExecutionContext<'_>,
    ) -> Result<(), RejectReason> {
        let regular = match candidate {
            ProductionAmendPlan::Regular(plan) => Some(plan),
            ProductionAmendPlan::Iceberg(plan) => {
                context.validate_qty(plan.new_peak)?;
                Some(&plan.regular)
            }
            ProductionAmendPlan::Stop(plan) => {
                context.validate_price(plan.new_stop_price)?;
                if let Some(price) = plan.new_protection.as_ref().and_then(|p| p.worst_price) {
                    context.validate_price(price)?;
                }
                let stop = self.stop_book.get(plan.order_id).expect("planned Stop").0;
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
                self.preflight_iceberg_plan(
                    plan.side,
                    Some(plan.new_price),
                    Budget::Base(plan.new_remaining.get()),
                    Some(&context.market_math),
                    None,
                    None,
                )?;
            }
        }
        Ok(())
    }

    /// 以生产 Arena/价档计划执行常规改单；所有业务拒绝先于首次结构写入。
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
            ProductionAmendPlan::Regular(plan) => plan,
            ProductionAmendPlan::Iceberg(plan) => {
                return self.commit_iceberg_amend(plan, amend, trade_ids);
            }
            ProductionAmendPlan::Stop(_) => {
                return Err(RejectReason::InvalidOrderCombination);
            }
        };
        if amend.reservation_adjustment.is_some() {
            return Err(RejectReason::InvalidReservation);
        }
        let index = self.orders[&plan.order_id];
        let mut amended = self
            .arena
            .get(index)
            .expect("candidate slot exists")
            .original_order()
            .clone();
        amended.price = plan.new_price;
        amended.qty = Qty::try_new(
            plan.already_filled
                .checked_add(plan.new_remaining.get())
                .ok_or(RejectReason::ArithmeticOverflow)?,
        )?;
        let event = EngineEvent::OrderAmended {
            order_id: plan.order_id,
            old_price: Some(plan.old_price),
            new_price: Some(plan.new_price),
            old_quantity: AmendedQuantity::BaseRemaining(plan.old_remaining),
            new_quantity: AmendedQuantity::BaseRemaining(plan.new_remaining),
            priority_retained: matches!(plan.kind, RegularAmendKind::InPlaceReduce),
        };
        match plan.kind {
            RegularAmendKind::InPlaceReduce => {
                // 同价减量不会增加聚合，也不分配优先级；候选已校验正余量与时效。
                self.commit_amend_reduce(index, amended, plan);
                Ok(vec![event])
            }
            RegularAmendKind::RequeueSamePrice => {
                let rest = self.preflight_amend_rest(
                    plan,
                    plan.new_remaining,
                    self.next_queue_priority(),
                )?;
                // 写屏障：绝对数量、时效、优先级和删除旧单后的价档容量已全部预检。
                let shadow = self
                    .reservations
                    .remove(&plan.order_id)
                    .expect("candidate shadow exists");
                self.cancel(plan.order_id).expect("candidate order exists");
                self.commit_amend_rest(amended, plan.new_remaining, plan.target_tif, rest);
                self.reservations.insert(plan.order_id, shadow);
                Ok(vec![event])
            }
            RegularAmendKind::RepriceAndMatch => {
                let commit = self.preflight_amend_reprice(plan, amended, trade_ids)?;
                Ok(self.commit_amend_reprice(commit, event, trade_ids))
            }
        }
    }

    /// 同价减量只改载荷、余量和到期引用，保持槽位、链接及优先级。
    fn commit_amend_reduce(
        &mut self,
        index: OrderIndex,
        amended: LimitGtcOrder,
        plan: RegularAmendPlan,
    ) {
        let levels = match plan.side {
            matching_domain::Side::Buy => &mut self.bids,
            matching_domain::Side::Sell => &mut self.asks,
        };
        levels
            .get_mut(&plan.old_price)
            .expect("candidate price level exists")
            .amend_regular_in_place(
                &mut self.arena,
                index,
                amended,
                plan.new_remaining,
                plan.target_tif,
            );
        self.regular_expiry.remove(plan.order_id, plan.old_tif);
        self.regular_expiry.insert(plan.order_id, plan.target_tif);
    }

    /// 在只读状态中构造删除旧单后的目标档位快照。
    ///
    /// 旧单删除保证至少有一个可复用槽位；对手成交不会增加本侧槽位需求。
    /// 因此不要求删除前的 Arena 仍能额外插入一个新节点。
    pub(super) fn preflight_amend_rest(
        &self,
        plan: RegularAmendPlan,
        remaining: Qty,
        next: u128,
    ) -> Result<RestPlan, RejectReason> {
        if next >= self.queue_priority_allocator.policy().effective_ceiling() {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let priority = QueuePriority::new(next);
        let levels = match plan.side {
            matching_domain::Side::Buy => &self.bids,
            matching_domain::Side::Sell => &self.asks,
        };
        let target = if let Some(level) = levels.get(&plan.new_price) {
            let same_level = plan.old_price == plan.new_price;
            let count_before = level
                .count()
                .checked_sub(u32::from(same_level))
                .expect("candidate order belongs to level");
            let total_before = level
                .total_visible_qty()
                .checked_sub(if same_level {
                    plan.old_remaining.get()
                } else {
                    0
                })
                .expect("candidate quantity included in aggregate");
            if count_before == 0 {
                assert_eq!(total_before, 0);
                RestTargetPlan::New {
                    count_after: 1,
                    total_after: remaining.get(),
                }
            } else {
                let old_index = self.orders[&plan.order_id];
                let tail = if same_level && level.tail() == Some(old_index) {
                    self.arena
                        .get(old_index)
                        .expect("candidate slot exists")
                        .prev()
                } else {
                    level.tail()
                }
                .expect("nonempty target has tail");
                let tail_node = self.arena.get(tail).expect("target tail exists");
                assert_eq!(tail_node.original_order().side, plan.side);
                assert_eq!(tail_node.original_order().price, plan.new_price);
                assert!(priority > tail_node.priority());
                RestTargetPlan::Existing {
                    tail,
                    count_before,
                    total_before,
                    count_after: count_before
                        .checked_add(1)
                        .ok_or(RejectReason::ArithmeticOverflow)?,
                    total_after: total_before
                        .checked_add(remaining.get())
                        .ok_or(RejectReason::ArithmeticOverflow)?,
                }
            }
        } else {
            RestTargetPlan::New {
                count_after: 1,
                total_after: remaining.get(),
            }
        };
        Ok(RestPlan {
            order_id: plan.order_id,
            side: plan.side,
            price: plan.new_price,
            priority,
            target,
        })
    }

    /// 提交事先计算好的余量节点；不在写屏障后重新校验时效或构造正数量。
    fn commit_amend_rest(
        &mut self,
        amended: LimitGtcOrder,
        remaining: Qty,
        tif: TimeInForce,
        rest: RestPlan,
    ) {
        assert!(!self.contains_active_order(rest.order_id));
        assert_eq!(self.next_queue_priority(), rest.priority.get());
        let mut node = OrderNode::new_timed(amended.clone(), rest.priority, tif);
        node.amend_regular_in_place(amended, remaining, tif);
        let slot = match rest.side {
            matching_domain::Side::Buy => {
                commit_rest_target(&mut self.bids, &mut self.arena, node, &rest)
            }
            matching_domain::Side::Sell => {
                commit_rest_target(&mut self.asks, &mut self.arena, node, &rest)
            }
        };
        assert!(self.orders.insert(rest.order_id, slot).is_none());
        self.regular_expiry.insert(rest.order_id, tif);
        assert_eq!(
            self.queue_priority_allocator
                .allocate()
                .expect("preflighted amend priority"),
            rest.priority
        );
    }

    /// 生产自己的片成交计划覆盖普通及冰山 maker，并在删除旧单前冻结全部成交。
    fn preflight_amend_reprice(
        &self,
        plan: RegularAmendPlan,
        amended: LimitGtcOrder,
        trade_ids: &[TradeId],
    ) -> Result<RegularRepriceCommitPlan, RejectReason> {
        let fills = self.preflight_iceberg_plan(
            plan.side,
            Some(plan.new_price),
            Budget::Base(plan.new_remaining.get()),
            None,
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= fills.fills.len(),
            "insufficient TradeIds for preflighted production Amend reprice"
        );
        let residual = if fills.base_remaining == 0 {
            None
        } else {
            let remaining = Qty::try_new(fills.base_remaining)?;
            Some((
                remaining,
                self.preflight_amend_rest(plan, remaining, fills.next_priority)?,
            ))
        };

        Ok(RegularRepriceCommitPlan {
            candidate: plan,
            amended,
            fills,
            residual,
        })
    }

    /// 只消费已冻结计划；写屏障后没有可返回的业务错误。
    fn commit_amend_reprice(
        &mut self,
        commit: RegularRepriceCommitPlan,
        event: EngineEvent,
        trade_ids: &[TradeId],
    ) -> Vec<EngineEvent> {
        let RegularRepriceCommitPlan {
            candidate: plan,
            amended,
            fills,
            residual,
        } = commit;
        // 写屏障：所有片成交、补量优先级、标识符、余量、价档容量和时效均已确定。
        let shadow = self
            .reservations
            .remove(&plan.order_id)
            .expect("candidate shadow exists");
        self.cancel(plan.order_id).expect("candidate order exists");
        let maker_events = self.commit_iceberg_plan(&fills, plan.side, plan.order_id, trade_ids);
        if let Some((remaining, rest)) = residual {
            self.commit_amend_rest(amended, remaining, plan.target_tif, rest);
            self.reservations.insert(plan.order_id, shadow);
        }
        let mut events = vec![event];
        let mut order_events = Vec::new();
        for event in maker_events {
            if matches!(event, EngineEvent::Trade(_)) {
                events.push(event);
            } else {
                order_events.push(event);
            }
        }
        if !fills.fills.is_empty() {
            order_events.push(match residual {
                Some((remaining, _)) => EngineEvent::OrderPartiallyFilled {
                    order_id: plan.order_id,
                    remaining,
                },
                None => EngineEvent::OrderFilled {
                    order_id: plan.order_id,
                },
            });
        }
        events.extend(order_events);
        events
    }

    /// 从 Arena、索引与 StopBook 的不可变快照构造改单计划。
    pub(crate) fn build_amend_plan(
        &self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<ProductionAmendPlan, RejectReason> {
        if let Some(node) = self.order(amend.order_id) {
            let reservation = self.reservation_snapshot(amend)?;
            return Self::build_regular_plan(node, amend, now, current_session_id, reservation);
        }
        if let Some((stop, priority)) = self.stop_book.get(amend.order_id) {
            let reservation = self.reservation_snapshot(amend)?;
            return Self::build_stop_plan(stop, priority, amend, reservation);
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

    /// 以 Arena 节点快照计算普通或冰山数量候选。
    fn build_regular_plan(
        node: &crate::OrderNode,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
        reservation: ReservationSnapshot,
    ) -> Result<ProductionAmendPlan, RejectReason> {
        if node.original_order().user_id != amend.user_id
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
        let old_remaining = Qty::try_new(node.total_remaining())?;
        let new_remaining =
            remaining_after_target_total(node.original_order().qty, old_remaining, amend.new_qty)?;
        let target_tif = amend.new_tif.unwrap_or(node.time_in_force());
        Self::validate_resting_tif(target_tif, now, current_session_id)?;
        let new_price = amend.new_price.unwrap_or(node.original_order().price);
        let already_filled = node
            .original_order()
            .qty
            .get()
            .checked_sub(old_remaining.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let regular = RegularAmendPlan {
            order_id: amend.order_id,
            side: node.original_order().side,
            old_price: node.original_order().price,
            new_price,
            original_total: node.original_order().qty,
            already_filled,
            old_remaining,
            new_remaining,
            kind: classify_regular_amend(
                node.original_order().price,
                new_price,
                old_remaining,
                new_remaining,
            ),
            target_tif,
            old_tif: node.time_in_force(),
            priority: node.priority(),
            reservation,
        };
        let Some(old_peak) = node.peak_size() else {
            if amend.new_visible_qty.is_some() {
                return Err(RejectReason::InvalidIceberg);
            }
            return Ok(ProductionAmendPlan::Regular(regular));
        };
        let new_peak = amend.new_visible_qty.unwrap_or(old_peak);
        if new_peak >= amend.new_qty.unwrap_or(regular.original_total) {
            return Err(RejectReason::InvalidIceberg);
        }
        Ok(ProductionAmendPlan::Iceberg(IcebergAmendPlan {
            regular,
            old_visible_remaining: Qty::try_new(node.visible_remaining())?,
            old_hidden_remaining: node.hidden_remaining(),
            old_peak,
            new_peak,
        }))
    }

    /// 构造未触发 Stop 的纯快照，保留原始及候选触发源与市价保护。
    fn build_stop_plan(
        stop: &matching_domain::StopOrder,
        priority: StopPriority,
        amend: &AmendOrder,
        reservation: ReservationSnapshot,
    ) -> Result<ProductionAmendPlan, RejectReason> {
        if amend.new_visible_qty.is_some() {
            return Err(RejectReason::InvalidOrderCombination);
        }
        match stop {
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
            .unwrap_or(stop.trigger_reference());
        if new_reference == matching_domain::PriceReferenceKind::StaticOpening {
            return Err(RejectReason::InvalidReferencePriceSource);
        }
        let old_protection = stop.market_protection().cloned();
        let new_protection = match &amend.new_market_protection {
            None | Some(matching_domain::MarketProtectionPatch::Keep) => old_protection.clone(),
            Some(matching_domain::MarketProtectionPatch::Clear) => None,
            Some(matching_domain::MarketProtectionPatch::Set(protection)) => {
                if matches!(stop, matching_domain::StopOrder::Limit(_)) {
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
            None | Some(TriggerExpiryPatch::Keep) => stop.trigger_expiry(),
            Some(TriggerExpiryPatch::ClearToGtc) => TriggerExpiry::Gtc,
            Some(TriggerExpiryPatch::Set(expiry)) => expiry,
        };
        Ok(ProductionAmendPlan::Stop(StopAmendPlan {
            old_reference: stop.trigger_reference(),
            new_reference,
            old_protection,
            new_protection,
            order_id: amend.order_id,
            priority,
            old_stop_price: stop.stop_price(),
            new_stop_price: amend.new_stop_price.unwrap_or(stop.stop_price()),
            target_expiry,
            reservation,
        }))
    }
}
