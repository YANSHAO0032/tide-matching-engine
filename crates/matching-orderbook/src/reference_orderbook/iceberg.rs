//! 基于独立动态 Vec 模型规划挂单的可见量、隐藏量生命周期。
//! 整条命令和调用方提供的 `TradeId` 全部通过预检前，
//! 所有变更只作用于可丢弃的虚拟状态，不对真实订单簿执行回滚。
use super::ReferenceOrderBook;
use crate::{IncomingOrder, ReferenceOrder};
use matching_domain::market_order::{
    CommandStartProtectionContext, effective_protection_at_command_start_with_source,
    maker_price_within_protection,
};
use matching_domain::{
    CancelReason, EngineEvent, IcebergOrder, LimitImmediateExecution, LimitOrder, MarketByBase,
    MarketByQuote, MarketMath, OrderId, OrderRemaining, Price, ProtectionSource, Qty,
    QueuePriority, QuoteAmount, RejectReason, Side, StpDecision, StpKey, StpMode, TradeEvent,
    TradeId, UserId, decide_stp,
};
use matching_risk::ReservationState;

pub(super) struct SliceRequest<'a> {
    /// 正在执行的订单标识符。
    pub id: OrderId,
    /// 当前吃单订单的持久用户身份。
    pub user_id: UserId,
    /// 当前吃单订单的持久自成交防护分组身份。
    pub stp_key: StpKey,
    /// 只由当前吃单订单决定的自成交防护策略。
    pub stp_mode: StpMode,
    /// 正在执行的买卖方向。
    pub side: Side,
    /// 尚待执行的基础资产数量。
    pub remaining: u64,
    /// 限价订单的可选最差可成交价格。
    pub limit: Option<Price>,
    /// 报价预算订单的可选剩余报价金额。
    pub budget: Option<QuoteAmount>,
    /// 报价预算计算所需的可选数值规则。
    pub math: Option<&'a MarketMath>,
    /// 命令开始已计算的可选有效保护价格及其来源。
    pub protection: Option<(Price, ProtectionSource)>,
    /// 是否在首笔可执行成交后立即停止。
    pub one_fill: bool,
}

pub(super) struct SlicePlan {
    /// 提交后按参考模型顺序保存的全部挂单。
    pub(super) orders: Vec<ReferenceOrder>,
    /// 提交后预期分配器的下一队列优先级数值。
    pub(super) next_priority: u128,
    /// 按撮合顺序产生的生命周期事件。
    pub(super) events: Vec<EngineEvent>,
    /// 计划结束后尚待执行的基础资产数量。
    pub remaining: u64,
    /// 计划使用的报价资产本金。
    pub spent: QuoteAmount,
    /// 计划结束后未使用的报价资产金额。
    pub unused: QuoteAmount,
    /// 吃单是否已由 STP 终结，后续入口不得把同一余量挂回订单簿或再次取消。
    pub taker_terminal: bool,
}

impl ReferenceOrderBook {
    /// 独立规划限价单成交，并仅为实际入簿余量保存预留状态。
    pub fn place_limit_gtc_with_reservation(
        &mut self,
        order: LimitOrder,
        state: ReservationState,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.validate_new_reservation(order.order_id, order.user_id, &state)?;
        let order_id = order.order_id;
        let accepted = order.clone();
        let events = self.place_limit_gtc_with_events(order, trade_ids)?;
        if let Some(maker) = self
            .orders
            .iter_mut()
            .find(|maker| maker.original_order().order_id == order_id)
        {
            // 独立保留接收绝对总量，入簿前成交仍计入后续改单的已成交量。
            maker.amend_regular_in_place(
                accepted,
                Qty::try_new(maker.total_remaining()).expect("positive reference residual"),
                maker.time_in_force(),
            );
            assert!(self.reservations.insert(order_id, state).is_none());
        }
        Ok(events)
    }

    /// 在一次参考模型提交中挂入冰山订单并保存预留状态。
    pub fn place_iceberg_post_only_with_reservation(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
        state: ReservationState,
    ) -> Result<(), RejectReason> {
        let order_id = iceberg.order().order_id;
        self.validate_new_reservation(order_id, iceberg.order().user_id, &state)?;
        self.place_iceberg_post_only(iceberg, math)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(())
    }

    pub(super) fn has_iceberg_maker(&self, incoming_side: Side) -> bool {
        self.orders.iter().any(|maker| {
            maker.original_order().side != incoming_side && maker.peak_size().is_some()
        })
    }

    pub(super) fn check_duplicate(&self, id: OrderId) -> Result<(), RejectReason> {
        if self.contains_active_order(id) {
            Err(RejectReason::DuplicateOrderId)
        } else {
            Ok(())
        }
    }

    /// 每片模拟成交后，重新按价格和 Vec 中的稳定顺序选取挂单。
    /// 补量时将订单追加到虚拟 Vec 尾部，独立模拟先进先出顺序。
    pub(super) fn plan_slices(&self, request: SliceRequest<'_>) -> Result<SlicePlan, RejectReason> {
        let mut virtual_book = Self {
            orders: self.orders.clone(),
            reservations: self.reservations.clone(),
            next_priority: self.next_priority,
            stop_orders: self.stop_orders.clone(),
            next_stop_priority: self.next_stop_priority,
            reference_prices: self.reference_prices.clone(),
        };
        let mut remaining = request.remaining;
        let mut unused = request.budget.unwrap_or(QuoteAmount::new(0));
        let mut spent = QuoteAmount::new(0);
        let mut events = Vec::new();
        let mut fill_count = 0usize;
        let mut taker_terminal = false;
        loop {
            if (request.budget.is_none() && remaining == 0)
                || (request.budget.is_some() && unused.get() == 0)
            {
                break;
            }
            let mut candidates: Vec<_> = virtual_book
                .orders
                .iter()
                .enumerate()
                .filter(|(_, maker)| {
                    maker.original_order().side != request.side && maker.remaining() != 0
                })
                .map(|(index, _)| index)
                .collect();
            candidates.sort_by(|&a, &b| {
                let a = virtual_book.orders[a].original_order().price;
                let b = virtual_book.orders[b].original_order().price;
                match request.side {
                    Side::Buy => a.cmp(&b),
                    Side::Sell => b.cmp(&a),
                }
            });
            let Some(index) = candidates.first().copied() else {
                break;
            };
            let maker = &virtual_book.orders[index];
            let price = maker.original_order().price;
            if !maker_price_within_protection(request.side, price, request.limit) {
                break;
            }
            if let Some((boundary, source)) = request.protection
                && !maker_price_within_protection(request.side, price, Some(boundary))
            {
                events.push(EngineEvent::MarketProtectionTriggered {
                    order_id: request.id,
                    next_price: price,
                    effective_worst_price: boundary,
                    source,
                });
                break;
            }
            let available = if request.budget.is_some() {
                let math = request.math.expect("quote requests supply math");
                // 预算不能支付一手时，本档不可执行，不能触发自成交防护。
                let Some(qty) = math
                    .max_qty_for_quote(unused, price)
                    .map_err(Self::reject_quote_math_error)?
                else {
                    break;
                };
                qty.get()
            } else {
                remaining
            };
            let maker_order = maker.original_order();
            match decide_stp(
                request.user_id,
                request.stp_key,
                request.stp_mode,
                maker_order.user_id,
                maker_order.stp_key,
            ) {
                StpDecision::NoConflict => {}
                StpDecision::CancelTaker => {
                    events.push(EngineEvent::OrderCanceled {
                        order_id: request.id,
                        remaining: if request.budget.is_some() {
                            OrderRemaining::Quote(unused)
                        } else {
                            OrderRemaining::Base(
                                Qty::try_new(remaining)
                                    .expect("active base taker has positive remaining"),
                            )
                        },
                        reason: CancelReason::SelfTradePreventionTaker,
                    });
                    taker_terminal = true;
                    break;
                }
                StpDecision::CancelMaker => {
                    let canceled = virtual_book.orders.remove(index);
                    events.push(EngineEvent::OrderCanceled {
                        order_id: canceled.original_order().order_id,
                        remaining: OrderRemaining::Base(
                            Qty::try_new(canceled.total_remaining())
                                .expect("active maker has positive total remaining"),
                        ),
                        reason: CancelReason::SelfTradePreventionMaker,
                    });
                    continue;
                }
                StpDecision::CancelBoth => {
                    let canceled = virtual_book.orders.remove(index);
                    events.push(EngineEvent::OrderCanceled {
                        order_id: canceled.original_order().order_id,
                        remaining: OrderRemaining::Base(
                            Qty::try_new(canceled.total_remaining())
                                .expect("active maker has positive total remaining"),
                        ),
                        reason: CancelReason::SelfTradePreventionMaker,
                    });
                    events.push(EngineEvent::OrderCanceled {
                        order_id: request.id,
                        remaining: if request.budget.is_some() {
                            OrderRemaining::Quote(unused)
                        } else {
                            OrderRemaining::Base(
                                Qty::try_new(remaining)
                                    .expect("active base taker has positive remaining"),
                            )
                        },
                        reason: CancelReason::SelfTradePreventionBoth,
                    });
                    taker_terminal = true;
                    break;
                }
            }
            let qty = Qty::try_new(available.min(maker.remaining()))?;
            if let Some(math) = request.math {
                let value = math
                    .trade_notional(price, qty)
                    .map_err(Self::reject_quote_math_error)?;
                spent = spent.checked_add(value)?;
                if request.budget.is_some() {
                    unused = unused.checked_sub(value)?;
                }
            }
            if request.budget.is_none() {
                remaining = remaining
                    .checked_sub(qty.get())
                    .ok_or(RejectReason::ArithmeticOverflow)?;
            }
            events.push(EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(0),
                maker_order_id: maker.original_order().order_id,
                taker_order_id: request.id,
                price,
                qty,
                maker_side: maker.original_order().side,
            }));
            let iceberg = maker.peak_size().is_some();
            let id = maker.original_order().order_id;
            virtual_book.orders[index].apply_fill(qty)?;
            let total = virtual_book.orders[index].total_remaining();
            if iceberg {
                events.push(if total == 0 {
                    EngineEvent::OrderFilled { order_id: id }
                } else {
                    EngineEvent::OrderPartiallyFilled {
                        order_id: id,
                        remaining: Qty::try_new(total)?,
                    }
                });
            }
            if virtual_book.orders[index].remaining() == 0 {
                let mut exhausted = virtual_book.orders.remove(index);
                if exhausted.hidden_remaining() != 0 {
                    let priority = virtual_book.allocate_slice_priority()?;
                    let visible = exhausted.refresh(priority);
                    virtual_book
                        .check_visible_aggregate(exhausted.original_order(), visible.get())?;
                    virtual_book.orders.push(exhausted);
                    events.push(EngineEvent::IcebergRefreshed {
                        order_id: id,
                        visible,
                    });
                }
            }
            fill_count = fill_count
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?;
            if request.one_fill && fill_count == 1 {
                break;
            }
        }
        Ok(SlicePlan {
            orders: virtual_book.orders,
            next_priority: virtual_book.next_priority,
            events,
            remaining,
            spent,
            unused,
            taker_terminal,
        })
    }

    fn allocate_slice_priority(&mut self) -> Result<QueuePriority, RejectReason> {
        if self.next_priority >= u128::MAX - super::PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let value = self.next_priority;
        self.next_priority = value
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;
        Ok(QueuePriority::new(value))
    }

    fn check_visible_aggregate(
        &self,
        order: &LimitOrder,
        visible: u64,
    ) -> Result<(), RejectReason> {
        self.orders
            .iter()
            .filter(|maker| {
                maker.original_order().side == order.side
                    && maker.original_order().price == order.price
            })
            .try_fold(visible, |total, maker| total.checked_add(maker.remaining()))
            .ok_or(RejectReason::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn commit_slices(
        &mut self,
        mut plan: SlicePlan,
        trade_ids: &[TradeId],
    ) -> Vec<EngineEvent> {
        let required = plan
            .events
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count();
        assert!(
            trade_ids.len() >= required,
            "insufficient TradeIds for reference slice plan"
        );
        let mut ids = trade_ids.iter().copied();
        for event in &mut plan.events {
            if let EngineEvent::Trade(trade) = event {
                trade.trade_id = ids.next().expect("preflighted TradeId");
            }
        }
        self.orders = plan.orders;
        self.retain_active_reservations();
        self.next_priority = plan.next_priority;
        plan.events
    }

    fn plan_limit_slices(
        &self,
        order: &LimitOrder,
        math: Option<&MarketMath>,
        remaining: u64,
        one_fill: bool,
    ) -> Result<SlicePlan, RejectReason> {
        self.plan_slices(SliceRequest {
            id: order.order_id,
            user_id: order.user_id,
            stp_key: order.stp_key,
            stp_mode: order.stp_mode,
            side: order.side,
            remaining,
            limit: Some(order.price),
            budget: None,
            math,
            protection: None,
            one_fill,
        })
    }

    pub(super) fn iceberg_immediate(
        &mut self,
        order: LimitOrder,
        math: &MarketMath,
        ids: &[TradeId],
        fok: bool,
    ) -> Result<LimitImmediateExecution, RejectReason> {
        self.check_duplicate(order.order_id)?;
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        let mut plan = self.plan_limit_slices(&order, Some(math), order.qty.get(), false)?;
        if fok && plan.taker_terminal {
            let terminal = plan
                .events
                .last()
                .cloned()
                .expect("terminal plan has cancellation");
            let maker_cancel = if matches!(
                terminal,
                EngineEvent::OrderCanceled {
                    reason: CancelReason::SelfTradePreventionBoth,
                    ..
                }
            ) {
                plan.events
                    .iter()
                    .rev()
                    .find(|event| {
                        matches!(
                            event,
                            EngineEvent::OrderCanceled {
                                reason: CancelReason::SelfTradePreventionMaker,
                                ..
                            }
                        )
                    })
                    .cloned()
            } else {
                None
            };
            plan.orders = self.orders.clone();
            plan.next_priority = self.next_priority;
            plan.events.clear();
            if let Some(EngineEvent::OrderCanceled { order_id, .. }) = maker_cancel {
                let index = plan
                    .orders
                    .iter()
                    .position(|maker| maker.original_order().order_id == order_id)
                    .expect("planned STP maker remains active");
                let maker = plan.orders.remove(index);
                plan.events.push(EngineEvent::OrderCanceled {
                    order_id,
                    remaining: OrderRemaining::Base(Qty::try_new(maker.total_remaining())?),
                    reason: CancelReason::SelfTradePreventionMaker,
                });
            }
            let reason = match terminal {
                EngineEvent::OrderCanceled { reason, .. } => reason,
                _ => unreachable!("terminal cancellation"),
            };
            plan.events.push(EngineEvent::OrderCanceled {
                order_id: order.order_id,
                remaining: OrderRemaining::Base(order.qty),
                reason,
            });
            plan.remaining = order.qty.get();
        }
        if fok && plan.remaining != 0 && !plan.taker_terminal {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        let canceled = if plan.remaining == 0 || plan.taker_terminal {
            None
        } else {
            Some(Qty::try_new(plan.remaining)?)
        };
        let events = self.commit_slices(plan, ids);
        Ok(LimitImmediateExecution::from_events(
            order.order_id,
            events,
            canceled,
        ))
    }

    /// 撮合普通 GTC 并保留冰山挂单的完整生命周期事件，最后挂入余量。
    pub fn place_limit_gtc_with_events(
        &mut self,
        order: LimitOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.place_sliced_gtc(order, None, None, trade_ids)
    }

    /// 以提交总量主动尝试成交，余量按可见片挂入订单簿。
    ///
    /// 发生主动成交后，按总余量报告新到订单的部分成交或全部成交状态。
    /// 纯挂单输入不生成成交生命周期事件；本接口不包含接受或开单事件封装。
    pub fn place_iceberg(
        &mut self,
        order: IcebergOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.place_sliced_gtc(
            order.order().clone(),
            Some(order.peak_size()),
            Some(math),
            trade_ids,
        )
    }

    fn place_sliced_gtc(
        &mut self,
        order: LimitOrder,
        peak: Option<Qty>,
        math: Option<&MarketMath>,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        order.validate_stp_mode()?;
        self.check_duplicate(order.order_id)?;
        if let Some(math) = math {
            math.trade_notional(order.price, order.qty)
                .map_err(Self::reject_quote_math_error)?;
            if let Some(peak) = peak {
                math.trade_notional(order.price, peak)
                    .map_err(Self::reject_quote_math_error)?;
            }
        }
        let mut plan = self.plan_limit_slices(&order, math, order.qty.get(), false)?;
        let mut virtual_book = Self {
            orders: plan.orders,
            reservations: self.reservations.clone(),
            next_priority: plan.next_priority,
            stop_orders: self.stop_orders.clone(),
            next_stop_priority: self.next_stop_priority,
            reference_prices: self.reference_prices.clone(),
        };
        if plan.remaining != 0 && !plan.taker_terminal {
            let priority = virtual_book.allocate_slice_priority()?;
            let mut payload = order.clone();
            payload.qty = Qty::try_new(plan.remaining)?;
            let resting = if let Some(peak) = peak {
                ReferenceOrder::iceberg(order.clone(), plan.remaining, peak, priority)
            } else {
                ReferenceOrder::new(payload, priority)
            };
            virtual_book.check_visible_aggregate(&order, resting.remaining())?;
            virtual_book.orders.push(resting);
        }
        plan.orders = virtual_book.orders;
        plan.next_priority = virtual_book.next_priority;
        if peak.is_some() && !plan.taker_terminal && plan.remaining != order.qty.get() {
            plan.events.push(if plan.remaining == 0 {
                EngineEvent::OrderFilled {
                    order_id: order.order_id,
                }
            } else {
                EngineEvent::OrderPartiallyFilled {
                    order_id: order.order_id,
                    remaining: Qty::try_new(plan.remaining)?,
                }
            });
        }
        Ok(self.commit_slices(plan, trade_ids))
    }

    /// 包括等价的交叉在改簿前拒绝；只有实际挂单才分配优先级。
    pub fn place_iceberg_post_only(
        &mut self,
        order: IcebergOrder,
        math: &MarketMath,
    ) -> Result<(), RejectReason> {
        self.preflight_post_only(order.order())?;
        math.trade_notional(order.order().price, order.order().qty)
            .map_err(Self::reject_quote_math_error)?;
        math.trade_notional(order.order().price, order.peak_size())
            .map_err(Self::reject_quote_math_error)?;
        self.place_iceberg(order, math, &[]).map(|_| ())
    }

    pub(super) fn execute_slice_fills(
        &mut self,
        incoming: &mut IncomingOrder,
        one_fill: bool,
    ) -> Result<Vec<(OrderId, Price, Qty)>, RejectReason> {
        let plan = self.plan_limit_slices(
            incoming.original_order(),
            None,
            incoming.remaining(),
            one_fill,
        )?;
        let executed = incoming
            .remaining()
            .checked_sub(plan.remaining)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let fills = plan
            .events
            .iter()
            .filter_map(|event| {
                if let EngineEvent::Trade(t) = event {
                    Some((t.maker_order_id, t.price, t.qty))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // 既有接口仅返回成交投影；富事件接口保留补量和生命周期事件。
        let ids = vec![TradeId::new(0); fills.len()];
        let incoming_fill = if executed == 0 {
            None
        } else {
            Some(Qty::try_new(executed)?)
        };
        let _projected_events = self.commit_slices(plan, &ids);
        if let Some(qty) = incoming_fill {
            incoming
                .apply_fill(qty)
                .expect("preflighted incoming deduction");
        }
        Ok(fills)
    }

    pub(super) fn slice_market_base(
        &mut self,
        incoming: MarketByBase,
        protection: Option<(&CommandStartProtectionContext, &MarketMath)>,
        ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.check_duplicate(incoming.order_id)?;
        let (math, boundary) = if let Some((context, math)) = protection {
            let boundary = effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?;
            (Some(math), boundary)
        } else {
            (None, None)
        };
        let plan = self.plan_slices(SliceRequest {
            id: incoming.order_id,
            user_id: incoming.user_id,
            stp_key: incoming.stp_key,
            stp_mode: incoming.stp_mode,
            side: incoming.side,
            remaining: incoming.qty.get(),
            limit: None,
            budget: None,
            math,
            protection: boundary,
            one_fill: false,
        })?;
        Ok(self.commit_slices(plan, ids))
    }

    pub(super) fn slice_market_quote(
        &mut self,
        incoming: MarketByQuote,
        context: Option<&CommandStartProtectionContext>,
        math: &MarketMath,
        ids: &[TradeId],
    ) -> Result<(Vec<EngineEvent>, QuoteAmount, QuoteAmount), RejectReason> {
        self.check_duplicate(incoming.order_id)?;
        let boundary = if let Some(context) = context {
            effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?
        } else {
            None
        };
        let plan = self.plan_slices(SliceRequest {
            id: incoming.order_id,
            user_id: incoming.user_id,
            stp_key: incoming.stp_key,
            stp_mode: incoming.stp_mode,
            side: incoming.side,
            remaining: 0,
            limit: None,
            budget: Some(QuoteAmount::new(incoming.quote_budget.get())),
            math: Some(math),
            protection: boundary,
            one_fill: false,
        })?;
        let spent = plan.spent;
        let unused = plan.unused;
        Ok((self.commit_slices(plan, ids), spent, unused))
    }
}
