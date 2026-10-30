//! 报价预算市价单及价格保护的只读计划和执行。

use super::ProductionOrderBook;
use crate::PriceLevel;
use crate::market_plan::{
    MarketByQuotePlan, ProtectedMarketByQuotePlan, ProtectionBoundaryStop, QuoteFillPlan,
};
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
    effective_protection_at_command_start_with_source, maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, MarketByQuote, MarketByQuoteExecution, MarketMath, MathError, Price, Qty,
    QuoteAmount, RejectReason, Side, TradeEvent, TradeId,
};

/// MarketByQuote 只读预检过程中的临时累计状态。
///
/// 仅修改本对象，不修改 OrderBook / Arena / PriceLevel。
#[derive(Debug)]
struct QuotePreflightState {
    /// 尚可用于后续成交的报价资产预算。
    remaining_quote: QuoteAmount,
    /// 已由计划成交使用的报价资产本金。
    spent_quote: QuoteAmount,
    /// 按未来事件顺序记录的挂单成交计划。
    fills: Vec<QuoteFillPlan>,
}

impl ProductionOrderBook {
    /// 市价按报价资产预算立即成交。
    ///
    /// 全部可预期错误发生在首笔状态变更前。
    /// 不挂入吃单方，也不为吃单方分配 QueuePriority；冰山挂单补量取新优先级。
    pub fn execute_market_by_quote(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<MarketByQuoteExecution, RejectReason> {
        let mut candidate = self.clone();
        let execution = candidate.execute_market_by_quote_inner(incoming, math, trade_ids)?;
        candidate.reference_prices =
            self.reference_prices
                .plan_trade_prices(execution.events().iter().filter_map(|event| match event {
                    EngineEvent::Trade(trade) => Some(trade.price),
                    _ => None,
                }))?;
        *self = candidate;
        Ok(execution)
    }

    /// 按命令开始时固定的保护边界执行 MarketByQuote。
    ///
    /// 预检、TradeId 足量检查和金额计划校验均先于首笔状态变更。
    /// 提交只使用 QuoteFillPlan，不重新计算保护边界、本金或预算。
    /// 保护事件不消耗 TradeId，市价吃单方不挂簿。
    pub fn execute_market_by_quote_with_protection(
        &mut self,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        let mut candidate = self.clone();
        let execution = candidate
            .execute_market_by_quote_with_protection_inner(incoming, context, math, trade_ids)?;
        candidate.reference_prices =
            self.reference_prices
                .plan_trade_prices(execution.events().iter().filter_map(|event| match event {
                    EngineEvent::Trade(trade) => Some(trade.price),
                    _ => None,
                }))?;
        *self = candidate;
        Ok(execution)
    }

    pub(super) fn reject_quote_math_error(error: MathError) -> RejectReason {
        error.to_reject_reason().unwrap_or_else(|| {
            panic!(
                "ProductionOrderBook invariant violation: \
             non-rejectable MarketByQuote math error: {error:?}"
            )
        })
    }

    fn preflight_quote_level(
        &self,
        level: &PriceLevel,
        maker_side: Side,
        price: Price,
        math: &MarketMath,
        state: &mut QuotePreflightState,
    ) -> Result<bool, RejectReason> {
        assert!(level.count() > 0, "matching level is empty");

        let mut current = level.head();
        let mut previous = None;
        let mut previous_priority = None;
        let mut observed_count = 0u32;
        let mut observed_total = 0u64;
        let mut stop = false;

        // 即使本单在档位中途耗尽，也扫描剩余链表完成
        // 本档结构检查，确保提交依赖的先进先出链正确。
        while let Some(index) = current {
            let node = self
                .arena
                .get(index)
                .expect("MarketByQuote preflight missing Arena node");

            assert_eq!(node.prev(), previous);
            assert_eq!(node.original_order().side, maker_side);
            assert_eq!(node.original_order().price, price);
            assert!(node.remaining() > 0);

            if let Some(priority) = previous_priority {
                assert!(priority < node.priority());
            }

            assert_eq!(
                self.orders.get(&node.original_order().order_id),
                Some(&index),
            );

            observed_count = observed_count
                .checked_add(1)
                .expect("MarketByQuote level count overflow");

            observed_total = observed_total
                .checked_add(node.remaining())
                .expect("MarketByQuote level quantity overflow");

            if !stop && state.remaining_quote.get() > 0 {
                let affordable = math
                    .max_qty_for_quote(state.remaining_quote, price)
                    .map_err(Self::reject_quote_math_error)?;

                match affordable {
                    None => {
                        stop = true;
                    }
                    Some(max_qty) => {
                        let lots = max_qty.get().min(node.remaining());

                        let qty = Qty::try_new(lots).expect("MarketByQuote planned zero fill");

                        let notional = math
                            .trade_notional(price, qty)
                            .map_err(Self::reject_quote_math_error)?;

                        assert!(notional.get() > 0, "MarketByQuote planned zero notional");

                        state.remaining_quote = state.remaining_quote.checked_sub(notional)?;
                        state.spent_quote = state.spent_quote.checked_add(notional)?;

                        state.fills.push(QuoteFillPlan {
                            maker_order_id: node.original_order().order_id,
                            maker_price: price,
                            qty,
                            notional,
                        });

                        if state.remaining_quote.get() == 0 {
                            stop = true;
                        }
                    }
                }
            }

            previous_priority = Some(node.priority());
            previous = Some(index);
            current = node.next();

            // 防止损坏链表形成环导致无限循环。
            assert!(
                observed_count <= level.count(),
                "MarketByQuote FIFO cycle or incorrect count"
            );
        }

        assert_eq!(observed_count, level.count());
        assert_eq!(observed_total, level.total_visible_qty());
        assert_eq!(previous, level.tail());

        Ok(stop)
    }

    pub(super) fn preflight_market_by_quote(
        &self,
        incoming: &MarketByQuote,
        math: &MarketMath,
    ) -> Result<MarketByQuotePlan, RejectReason> {
        // 重复标识符必须先于盘口和 MarketMath 检查。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let budget = incoming.quote_budget.get();

        let mut state = QuotePreflightState {
            remaining_quote: QuoteAmount::new(budget),
            spent_quote: QuoteAmount::new(0),
            fills: Vec::new(),
        };

        match maker_side {
            Side::Sell => {
                // 买入吃单方：`asks` 从低到高。
                for (price, level) in &self.asks {
                    if state.remaining_quote.get() == 0 {
                        break;
                    }

                    if self.preflight_quote_level(level, maker_side, *price, math, &mut state)? {
                        break;
                    }
                }
            }
            Side::Buy => {
                // 卖出吃单方：`bids` 从高到低。
                for (price, level) in self.bids.iter().rev() {
                    if state.remaining_quote.get() == 0 {
                        break;
                    }

                    if self.preflight_quote_level(level, maker_side, *price, math, &mut state)? {
                        break;
                    }
                }
            }
        }

        let accounted = state.spent_quote.checked_add(state.remaining_quote)?;

        assert_eq!(
            accounted.get(),
            budget,
            "MarketByQuote budget conservation mismatch"
        );

        Ok(MarketByQuotePlan {
            required_trade_ids: state.fills.len(),
            fills: state.fills,
            spent_quote: state.spent_quote,
            remaining_quote: state.remaining_quote,
        })
    }

    pub(super) fn preflight_market_by_quote_with_protection(
        &self,
        incoming: &MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByQuotePlan, RejectReason> {
        // 1. 重复标识符必须优先于保护及数学错误。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 命令开始时固定有效保护价格和来源，只调用一次。
        let effective = effective_protection_at_command_start_with_source(
            incoming.market_protection.as_ref(),
            context,
            incoming.side,
            math,
        )?;

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let budget = incoming.quote_budget.get();

        let mut state = QuotePreflightState {
            remaining_quote: QuoteAmount::new(budget),
            spent_quote: QuoteAmount::new(0),
            fills: Vec::new(),
        };

        let mut stop = None;

        // true：继续下一价档；false：终止本次预检。
        let mut scan_level = |price: Price, level: &PriceLevel| -> Result<bool, RejectReason> {
            if state.remaining_quote.get() == 0 {
                return Ok(false);
            }

            // 3. 保护先于不足一手的预算/MarketMath。
            // 整档同价，因此在访问先进先出节点前检查一次即可。
            if !maker_price_within_protection(
                incoming.side,
                price,
                effective.map(|(worst, _)| worst),
            ) {
                let (effective_worst_price, source) =
                    effective.expect("out-of-bound price requires protection");

                stop = Some(ProtectionBoundaryStop {
                    next_price: price,
                    effective_worst_price,
                    source,
                });

                // 首个越界价立即终止，不扫描更远价格。
                return Ok(false);
            }

            assert!(level.count() > 0);
            assert!(level.head().is_some());
            assert!(level.tail().is_some());

            let mut current = level.head();
            let mut previous = None;
            let mut previous_priority = None;
            let mut observed_count = 0u32;
            let mut observed_total = 0u64;
            let mut halted = false;

            // 4. 同价先进先出：头节点 -> `next` -> 尾节点。
            while let Some(index) = current {
                let node = self
                    .arena
                    .get(index)
                    .expect("protected Quote preflight missing Arena node");

                assert_eq!(node.prev(), previous);
                assert_eq!(node.original_order().side, maker_side);
                assert_eq!(node.original_order().price, price);
                assert!(node.remaining() > 0);

                assert_eq!(
                    self.orders.get(&node.original_order().order_id),
                    Some(&index)
                );

                if let Some(priority) = previous_priority {
                    assert!(
                        priority < node.priority(),
                        "protected Quote FIFO priority regression"
                    );
                }

                observed_count = observed_count
                    .checked_add(1)
                    .expect("protected Quote level count overflow");

                assert!(
                    observed_count <= level.count(),
                    "protected Quote FIFO cycle or count mismatch"
                );

                observed_total = observed_total
                    .checked_add(node.remaining())
                    .expect("protected Quote aggregate overflow");

                // 停止扫描后仅完成本档的结构检查，
                // 不再做预算计算或规划新成交。
                if !halted && state.remaining_quote.get() > 0 {
                    let affordable = math
                        .max_qty_for_quote(state.remaining_quote, price)
                        .map_err(Self::reject_quote_math_error)?;

                    match affordable {
                        None => {
                            // 在保护边界内发生不足一手的预算。
                            halted = true;
                        }
                        Some(max_qty) => {
                            let lots = max_qty.get().min(node.remaining());

                            let qty =
                                Qty::try_new(lots).expect("protected Quote planned zero fill");

                            let notional = math
                                .trade_notional(price, qty)
                                .map_err(Self::reject_quote_math_error)?;

                            assert!(notional.get() > 0, "protected Quote planned zero notional");

                            state.remaining_quote = state.remaining_quote.checked_sub(notional)?;

                            state.spent_quote = state.spent_quote.checked_add(notional)?;

                            state.fills.push(QuoteFillPlan {
                                maker_order_id: node.original_order().order_id,
                                maker_price: price,
                                qty,
                                notional,
                            });

                            if state.remaining_quote.get() == 0 {
                                halted = true;
                            }
                        }
                    }
                }

                previous_priority = Some(node.priority());
                previous = Some(index);
                current = node.next();
            }

            // 5. 验证整个已访问档位的聚合与链表。
            assert_eq!(observed_count, level.count());
            assert_eq!(observed_total, level.total_visible_qty());
            assert_eq!(previous, level.tail());

            Ok(!halted)
        };

        // 6. 直接使用生产簿 BTreeMap 的业务价格顺序。
        match incoming.side {
            Side::Buy => {
                for (price, level) in &self.asks {
                    if !scan_level(*price, level)? {
                        break;
                    }
                }
            }
            Side::Sell => {
                for (price, level) in self.bids.iter().rev() {
                    if !scan_level(*price, level)? {
                        break;
                    }
                }
            }
        }

        // 7. 本地金额守恒验证，不修改任何订单簿状态。
        let accounted = state.spent_quote.checked_add(state.remaining_quote)?;

        assert_eq!(
            accounted.get(),
            budget,
            "protected MarketByQuote budget conservation mismatch"
        );

        Ok(ProtectedMarketByQuotePlan {
            fills: state.fills,
            spent_quote: state.spent_quote,
            unused_quote: state.remaining_quote,
            stop,
        })
    }

    pub(super) fn execute_market_by_quote_inner(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<MarketByQuoteExecution, RejectReason> {
        incoming.validate_stp_mode()?;
        if incoming
            .market_protection
            .as_ref()
            .and_then(|p| p.max_slippage_bps)
            .is_some_and(|bps| bps >= 10_000)
        {
            return Err(RejectReason::InvalidSlippageBps);
        }
        if let Some((config, band_math)) = self.price_band_config() {
            assert_eq!(*math, band_math, "市场数值配置不一致");
            let reference = self
                .reference_prices
                .latest(config.reference_kind)
                .ok_or(RejectReason::ReferencePriceUnavailable)?
                .price;
            let (lower, upper) = config.bounds(reference, band_math.tick_size())?;
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: reference,
                market_band_worst: Some(if incoming.side == Side::Buy {
                    upper
                } else {
                    lower
                }),
            };
            let execution =
                self.execute_market_by_quote_with_protection(incoming, &context, math, trade_ids)?;
            return Ok(MarketByQuoteExecution::from_events(
                execution.events().to_vec(),
                execution.spent_quote(),
                execution.unused_quote(),
            ));
        }
        if incoming
            .market_protection
            .as_ref()
            .is_some_and(|p| p.max_slippage_bps.is_some())
        {
            return Err(RejectReason::ReferencePriceUnavailable);
        }
        if let Some(worst) = incoming
            .market_protection
            .as_ref()
            .and_then(|p| p.worst_price)
        {
            if worst.get() % math.tick_size() != 0 {
                return Err(RejectReason::InvalidTick);
            }
            let execution = self.execute_market_quote_with_boundary(
                incoming,
                math,
                Some((worst, matching_domain::ProtectionSource::User)),
                trade_ids,
            )?;
            return Ok(MarketByQuoteExecution::from_events(
                execution.events().to_vec(),
                execution.spent_quote(),
                execution.unused_quote(),
            ));
        }
        if incoming.stp_mode != matching_domain::StpMode::None {
            let execution = self.execute_market_quote_stp(incoming, None, math, trade_ids)?;
            return Ok(MarketByQuoteExecution::from_events(
                execution.events().to_vec(),
                execution.spent_quote(),
                execution.unused_quote(),
            ));
        }
        if self.has_iceberg_maker(incoming.side) {
            let execution = self.execute_market_quote_iceberg(incoming, math, None, trade_ids)?;
            return Ok(MarketByQuoteExecution::from_events(
                execution.events().to_vec(),
                execution.spent_quote(),
                execution.unused_quote(),
            ));
        }
        let plan = self.preflight_market_by_quote(&incoming, math)?;

        assert!(
            trade_ids.len() >= plan.required_trade_ids,
            "ProductionOrderBook invariant violation: \
         insufficient TradeIds for MarketByQuote: required={}, provided={}",
            plan.required_trade_ids,
            trade_ids.len(),
        );

        assert_eq!(plan.fills.len(), plan.required_trade_ids);

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let priority_before = self.next_queue_priority();
        let mut trades = Vec::with_capacity(plan.required_trade_ids);

        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            let index = *self
                .orders
                .get(&fill.maker_order_id)
                .expect("preflighted MarketByQuote maker ID disappeared");

            let level = match maker_side {
                Side::Buy => self.bids.get(&fill.maker_price),
                Side::Sell => self.asks.get(&fill.maker_price),
            }
            .expect("preflighted MarketByQuote level disappeared");

            // 必须仍然是当前最优对手价的先进先出头节点。
            assert_eq!(level.head(), Some(index));

            // 报价预算可能不足以再成交当前最优价档的一整手，但仍足以成交更低价
            // 的后续档位。此处只能复核预检锁定的档位与 FIFO 头，不能要求该档位
            // 仍是整个对手侧的最优价格。

            let maker = self
                .arena
                .get(index)
                .expect("preflighted MarketByQuote maker node disappeared");

            assert_eq!(maker.original_order().order_id, fill.maker_order_id);
            assert_eq!(maker.original_order().side, maker_side);
            assert_eq!(maker.original_order().price, fill.maker_price);

            let maker_remaining = maker.remaining();
            let fill_lots = fill.qty.get();

            assert!(fill_lots > 0 && fill_lots <= maker_remaining);

            let maker_after = maker_remaining
                .checked_sub(fill_lots)
                .expect("preflighted MarketByQuote maker underflow");

            // 原序事件使用计划中的价、量。
            // 不调用 MarketMath，也不重算本金。
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.maker_order_id,
                taker_order_id: incoming.order_id,
                price: fill.maker_price,
                qty: fill.qty,
                maker_side,
            };

            if maker_after > 0 {
                let level = match maker_side {
                    Side::Buy => self.bids.get_mut(&fill.maker_price),
                    Side::Sell => self.asks.get_mut(&fill.maker_price),
                }
                .expect("preflighted MarketByQuote partial level missing");

                level.apply_head_partial_fill(&mut self.arena, fill.qty);
            } else {
                let (level_became_empty, maker_tif) = {
                    let level = match maker_side {
                        Side::Buy => self.bids.get_mut(&fill.maker_price),
                        Side::Sell => self.asks.get_mut(&fill.maker_price),
                    }
                    .expect("preflighted MarketByQuote full level missing");

                    let removed = level
                        .pop_front(&mut self.arena)
                        .expect("preflighted MarketByQuote head missing");

                    assert_eq!(removed.original_order().order_id, fill.maker_order_id);

                    (level.is_empty(), removed.time_in_force())
                };

                // pop_front 已释放 Arena 槽位，不能再 `remove` 一次。
                assert_eq!(self.orders.remove(&fill.maker_order_id), Some(index));
                self.reservations.remove(&fill.maker_order_id);
                self.regular_expiry.remove(fill.maker_order_id, maker_tif);

                if level_became_empty {
                    let removed_level = match maker_side {
                        Side::Buy => self.bids.remove(&fill.maker_price),
                        Side::Sell => self.asks.remove(&fill.maker_price),
                    }
                    .expect("preflighted MarketByQuote empty level missing");

                    assert!(removed_level.is_empty());
                }
            }

            trades.push(trade);
        }

        assert_eq!(trades.len(), plan.required_trade_ids);
        assert_eq!(self.next_queue_priority(), priority_before);
        assert!(!self.orders.contains_key(&incoming.order_id));

        Ok(MarketByQuoteExecution::new(
            trades,
            plan.spent_quote,
            plan.remaining_quote,
        ))
    }

    fn execute_market_by_quote_with_protection_inner(
        &mut self,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        incoming.validate_stp_mode()?;
        if let Some((_, configured_math)) = self.price_band_config() {
            assert_eq!(*math, configured_math, "市场数值配置不一致");
        }
        let mut effective_context = context.clone();
        if let Some((lower, upper)) = self.price_band_bounds()? {
            effective_context.market_band_worst = Some(if incoming.side == Side::Buy {
                upper
            } else {
                lower
            });
        }
        let context = &effective_context;
        self.execute_market_by_quote_with_frozen_protection(incoming, context, math, trade_ids)
    }

    /// 使用根命令已冻结的保护上下文，不重新读取市场参考价。
    pub(super) fn execute_market_by_quote_with_frozen_protection(
        &mut self,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        incoming.validate_stp_mode()?;
        if incoming.stp_mode != matching_domain::StpMode::None {
            return self.execute_market_quote_stp(incoming, Some(context), math, trade_ids);
        }
        if self.has_iceberg_maker(incoming.side) {
            return self.execute_market_quote_iceberg(incoming, math, Some(context), trade_ids);
        }
        // 1. 完整只读预检。
        let plan = self.preflight_market_by_quote_with_protection(&incoming, context, math)?;

        // 2. 所有 TradeId 检查必须先于首次状态变更。
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for protected MarketByQuote: required={}, provided={}",
            plan.fills.len(),
            trade_ids.len(),
        );

        // 3. 只核对已规划的本金，不重新调用 MarketMath。
        let mut planned_spent = QuoteAmount::new(0);

        for fill in &plan.fills {
            assert!(fill.notional.get() > 0);

            planned_spent = planned_spent
                .checked_add(fill.notional)
                .expect("preflighted protected Quote notional overflow");
        }

        assert_eq!(planned_spent, plan.spent_quote);

        let accounted = plan
            .spent_quote
            .checked_add(plan.unused_quote)
            .expect("preflighted protected Quote budget overflow");

        assert_eq!(accounted.get(), incoming.quote_budget.get());

        let trade_count = plan.fills.len();
        let event_count = trade_count
            .checked_add(usize::from(plan.stop.is_some()))
            .expect("protected MarketByQuote event count overflow");

        // 首笔挂单状态变更前完成事件容量预分配。
        let mut events = Vec::with_capacity(event_count);

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let priority_before = self.next_queue_priority();

        // 4. 严格依照预检成交顺序提交。
        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            // 必须仍然是当前最优对手价。
            let best_price = self
                .best_price(maker_side)
                .expect("preflighted protected Quote best price missing");

            assert_eq!(best_price, fill.maker_price);

            let index = *self
                .orders
                .get(&fill.maker_order_id)
                .expect("preflighted protected Quote maker ID missing");

            // 挂单必须位于预检价格档的先进先出头节点。
            let level = match maker_side {
                Side::Buy => self.bids.get(&fill.maker_price),
                Side::Sell => self.asks.get(&fill.maker_price),
            }
            .expect("preflighted protected Quote level missing");

            assert_eq!(level.head(), Some(index));

            let maker = self
                .arena
                .get(index)
                .expect("preflighted protected Quote maker node missing");

            assert_eq!(maker.original_order().order_id, fill.maker_order_id);
            assert_eq!(maker.original_order().side, maker_side);
            assert_eq!(maker.original_order().price, fill.maker_price);

            let maker_remaining = maker.remaining();
            let maker_priority = maker.priority();
            let fill_lots = fill.qty.get();

            assert!(fill_lots > 0 && fill_lots <= maker_remaining);

            let maker_after = maker_remaining
                .checked_sub(fill_lots)
                .expect("preflighted protected Quote maker underflow");

            // 事件只读取计划中的挂单、价格和数量。
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.maker_order_id,
                taker_order_id: incoming.order_id,
                price: fill.maker_price,
                qty: fill.qty,
                maker_side,
            };

            // 5. 首次挂单状态变更。
            if maker_after > 0 {
                // 部分成交：保留先进先出槽位和 QueuePriority。
                let level = match maker_side {
                    Side::Buy => self.bids.get_mut(&fill.maker_price),
                    Side::Sell => self.asks.get_mut(&fill.maker_price),
                }
                .expect("preflighted protected Quote partial level missing");

                level.apply_head_partial_fill(&mut self.arena, fill.qty);

                let updated = self
                    .arena
                    .get(index)
                    .expect("partial maker must retain Arena slot");

                assert_eq!(updated.remaining(), maker_after);
                assert_eq!(updated.priority(), maker_priority);
                assert_eq!(self.orders.get(&fill.maker_order_id), Some(&index));
            } else {
                // 完全成交：先进先出 pop_front 释放 Arena 槽位。
                let (level_became_empty, maker_tif) = {
                    let level = match maker_side {
                        Side::Buy => self.bids.get_mut(&fill.maker_price),
                        Side::Sell => self.asks.get_mut(&fill.maker_price),
                    }
                    .expect("preflighted protected Quote full level missing");

                    let removed = level
                        .pop_front(&mut self.arena)
                        .expect("preflighted protected Quote FIFO head missing");

                    assert_eq!(removed.original_order().order_id, fill.maker_order_id,);
                    assert_eq!(removed.remaining(), maker_remaining);

                    (level.is_empty(), removed.time_in_force())
                };

                // pop_front 已释放槽位，不得二次 `arena.remove`。
                assert_eq!(self.orders.remove(&fill.maker_order_id), Some(index),);
                self.reservations.remove(&fill.maker_order_id);
                self.regular_expiry.remove(fill.maker_order_id, maker_tif);

                if level_became_empty {
                    let removed_level = match maker_side {
                        Side::Buy => self.bids.remove(&fill.maker_price),
                        Side::Sell => self.asks.remove(&fill.maker_price),
                    }
                    .expect("preflighted protected Quote empty level missing");

                    assert!(removed_level.is_empty());
                }
            }

            events.push(EngineEvent::Trade(trade));
        }

        assert_eq!(events.len(), trade_count);

        // 6. 所有 Trade 之后最多追加一个保护终结事件。
        // 不使用 TradeId，也不读取下一张挂单。
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }

        assert_eq!(events.len(), event_count);

        // 此普通挂单路径不消耗 QueuePriority，也不挂入市价吃单方。
        assert_eq!(self.next_queue_priority(), priority_before);
        assert!(!self.orders.contains_key(&incoming.order_id));

        Ok(ProtectedMarketByQuoteExecution::new(
            events,
            plan.spent_quote,
            plan.unused_quote,
        ))
    }
}
