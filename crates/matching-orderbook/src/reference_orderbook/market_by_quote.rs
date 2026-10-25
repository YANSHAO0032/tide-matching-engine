//! 报价币预算市价订单的金额规划、保护终止和成交提交。

use super::ReferenceOrderBook;
use crate::market_plan::{
    MarketByQuotePlan, ProtectedMarketByQuotePlan, ProtectionBoundaryStop, QuoteFillPlan,
};
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
    effective_protection_at_command_start_with_source, maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, MarketByQuote, MarketByQuoteExecution, MarketMath, Qty, QuoteAmount, RejectReason,
    Side, TradeEvent, TradeId,
};

impl ReferenceOrderBook {
    /// 只读规划 MarketByQuote。
    ///
    /// - 重复活动 OrderId 优先拒绝；
    /// - 挂单按价格优先、同价先进先出；
    /// - 不足一手时立即不足一手的预算停止；
    /// - 不修改订单簿、不消耗 QueuePriority；
    /// - 不生成 TradeEvent 或 MarketByQuoteExecution。
    pub(super) fn preflight_market_by_quote(
        &self,
        incoming: &MarketByQuote,
        math: &MarketMath,
    ) -> Result<MarketByQuotePlan, RejectReason> {
        // 1. 重复标识符必须先于盘口读取。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let original_budget = incoming.quote_budget.get();

        let mut remaining_quote = QuoteAmount::new(original_budget);
        let mut spent_quote = QuoteAmount::new(0);
        let mut fills = Vec::new();
        let mut required_trade_ids = 0usize;

        // 2. `orders()` 返回已经按价格优先、同价先进先出
        // 排列的引用视图，不修改原始 Vec。
        for maker in self.orders(maker_side) {
            if remaining_quote.get() == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            if maker_remaining == 0 {
                continue;
            }

            let maker_price = maker.original_order().price;

            // 3. 从剩余报价预算计算可购买数量。
            let affordable = math
                .max_qty_for_quote(remaining_quote, maker_price)
                .map_err(Self::reject_quote_math_error)?;

            // 余额不足一手，整张市价订单停止。
            let Some(max_qty) = affordable else {
                break;
            };

            // 4. 挂单当前剩余量限制实际成交量。
            let fill_lots = max_qty.get().min(maker_remaining);

            let fill_qty =
                Qty::try_new(fill_lots).expect("preflighted MarketByQuote fill must be positive");

            // 5. 使用唯一的 MarketMath 计算成交名义额。
            let notional = math
                .trade_notional(maker_price, fill_qty)
                .map_err(Self::reject_quote_math_error)?;

            assert!(
                notional.get() > 0,
                "MarketByQuote planned notional must be positive"
            );

            // 6. 以受检算术更新金额，仍然只修改局部变量。
            remaining_quote = remaining_quote.checked_sub(notional)?;

            spent_quote = spent_quote.checked_add(notional)?;

            required_trade_ids = required_trade_ids
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            fills.push(QuoteFillPlan {
                maker_order_id: maker.original_order().order_id,
                maker_price,
                qty: fill_qty,
                notional,
            });
        }

        // 7. 预算守恒。
        let accounted = spent_quote.checked_add(remaining_quote)?;

        assert_eq!(
            accounted.get(),
            original_budget,
            "MarketByQuote budget conservation mismatch"
        );

        assert_eq!(fills.len(), required_trade_ids);

        Ok(MarketByQuotePlan {
            fills,
            required_trade_ids,
            spent_quote,
            remaining_quote,
        })
    }

    /// 受保护 MarketByQuote 执行入口使用的只读计划。
    ///
    /// 顺序：重复标识符 → 命令开始时保护 → 挂单边界
    ///      → 报价资产预算反推 → 本金计算 → 本地金额累计。
    ///
    /// 不生成事件、不消耗 TradeId、不修改 ReferenceOrderBook。
    pub(super) fn preflight_market_by_quote_with_protection(
        &self,
        incoming: &MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByQuotePlan, RejectReason> {
        // 1. 重复标识符永远先于保护及数学计算。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 命令开始有效边界只计算一次，并保留来源。
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

        let original_budget = incoming.quote_budget.get();

        let mut remaining_quote = QuoteAmount::new(original_budget);
        let mut spent_quote = QuoteAmount::new(0);
        let mut fills = Vec::new();
        let mut stop = None;

        // `orders(maker_side)` 已经是价格优先、同价先进先出的只读视图。
        for maker in self.orders(maker_side) {
            if remaining_quote.get() == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            if maker_remaining == 0 {
                continue;
            }

            let maker_price = maker.original_order().price;

            // 3. 必须先判断保护边界。
            //    越界时不能再执行 max_qty_for_quote / trade_notional。
            if !maker_price_within_protection(
                incoming.side,
                maker_price,
                effective.map(|(price, _)| price),
            ) {
                let (effective_worst_price, source) =
                    effective.expect("out-of-bound maker requires protection");

                stop = Some(ProtectionBoundaryStop {
                    next_price: maker_price,
                    effective_worst_price,
                    source,
                });

                // 首个越界挂单立即停止。
                break;
            }

            // 4. 只有边界允许后，才进行预算反推。
            let affordable = math
                .max_qty_for_quote(remaining_quote, maker_price)
                .map_err(Self::reject_quote_math_error)?;

            // 预算不足一手：不足一个合法手，整单停止；不生成保护终止。
            let Some(max_qty) = affordable else {
                break;
            };

            let fill_lots = max_qty.get().min(maker_remaining);

            let fill_qty =
                Qty::try_new(fill_lots).expect("protected MarketByQuote fill must be positive");

            // 5. 唯一 MarketMath 入口计算成交本金。
            let notional = math
                .trade_notional(maker_price, fill_qty)
                .map_err(Self::reject_quote_math_error)?;

            assert!(
                notional.get() > 0,
                "protected MarketByQuote planned notional must be positive"
            );

            // 6. 仅更新局部金额，不修改订单簿。
            remaining_quote = remaining_quote.checked_sub(notional)?;
            spent_quote = spent_quote.checked_add(notional)?;

            fills.push(QuoteFillPlan {
                maker_order_id: maker.original_order().order_id,
                maker_price,
                qty: fill_qty,
                notional,
            });
        }

        // 保持原有 MarketByQuote 预检的金额守恒断言。
        let accounted = spent_quote.checked_add(remaining_quote)?;

        assert_eq!(
            accounted.get(),
            original_budget,
            "protected MarketByQuote budget conservation mismatch"
        );

        Ok(ProtectedMarketByQuotePlan {
            fills,
            spent_quote,
            unused_quote: remaining_quote,
            stop,
        })
    }

    /// 按报价币预算执行市价订单。
    ///
    /// 所有可预期失败都发生在首笔状态变更前：
    /// 1. 只读规划整个挂单成交序列；
    /// 2. 检查调用方 TradeId 数量；
    /// 3. 按计划逐笔提交，不重新计算本金。
    ///
    /// 市价吃单方不挂簿；冰山挂单补量仍分配 QueuePriority。
    pub fn execute_market_by_quote(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<MarketByQuoteExecution, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            let (events, spent, unused) =
                self.slice_market_quote(incoming, None, math, trade_ids)?;
            let trades = events
                .into_iter()
                .filter_map(|event| {
                    if let EngineEvent::Trade(t) = event {
                        Some(t)
                    } else {
                        None
                    }
                })
                .collect();
            return Ok(MarketByQuoteExecution::new(trades, spent, unused));
        }
        // 1. 全量只读预检。
        let plan = self.preflight_market_by_quote(&incoming, math)?;

        // 2. TradeId 必须在任何挂单状态变更前充足。
        assert!(
            trade_ids.len() >= plan.required_trade_ids,
            "ReferenceOrderBook invariant violation: \
         insufficient TradeIds for MarketByQuote: \
         required={}, provided={}",
            plan.required_trade_ids,
            trade_ids.len(),
        );

        assert_eq!(
            plan.fills.len(),
            plan.required_trade_ids,
            "MarketByQuote plan count mismatch",
        );

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut trades = Vec::with_capacity(plan.required_trade_ids);

        // 3. 严格按照只读计划的原序提交。
        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            // 使用计划中的挂单标识符定位，而不是重新选择最优挂单。
            let maker_index = self
                .orders
                .iter()
                .position(|maker| maker.original_order().order_id == fill.maker_order_id)
                .expect("preflighted MarketByQuote maker disappeared");

            let maker = &self.orders[maker_index];

            // 预检与提交处于同一写入者生命周期，
            // 计划中的身份、方向、价格和数量必须仍然有效。
            assert_eq!(maker.original_order().order_id, fill.maker_order_id,);
            assert_eq!(maker.original_order().side, maker_side,);
            assert_eq!(maker.original_order().price, fill.maker_price,);

            let maker_remaining = maker.remaining();
            let fill_lots = fill.qty.get();

            assert!(
                fill_lots > 0 && fill_lots <= maker_remaining,
                "preflighted MarketByQuote quantity no longer valid",
            );

            let maker_after = maker_remaining
                .checked_sub(fill_lots)
                .expect("preflighted MarketByQuote maker underflow");

            // 先构造事件，再修改挂单。
            // 成交价、数量来自计划；不调用 MarketMath。
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.maker_order_id,
                taker_order_id: incoming.order_id,
                price: fill.maker_price,
                qty: fill.qty,
                maker_side,
            };

            self.orders[maker_index]
                .apply_fill(fill.qty)
                .unwrap_or_else(|reason| {
                    panic!(
                        "preflighted MarketByQuote maker mutation failed: \
                     {reason:?}"
                    )
                });

            // 完全成交挂单从参考簿 Vec 中物理移除。
            if maker_after == 0 {
                let removed = self.orders.remove(maker_index);
                self.reservations.remove(&fill.maker_order_id);

                assert_eq!(removed.original_order().order_id, fill.maker_order_id,);
                assert_eq!(removed.remaining(), 0);
            }

            trades.push(trade);
        }

        assert_eq!(
            trades.len(),
            plan.required_trade_ids,
            "MarketByQuote committed trade count differs from plan",
        );

        // 4. 使用只读计划的金额结果，绝不重新计算本金。
        Ok(MarketByQuoteExecution::new(
            trades,
            plan.spent_quote,
            plan.remaining_quote,
        ))
    }

    /// 参考簿受保护 MarketByQuote 执行。
    ///
    /// 完整只读预检、TradeId 数量校验及事件容量准备
    /// 必须先于任何订单簿状态变更。
    ///
    /// 使用预检计划中的挂单、价格、数量和本金；
    /// 不重新计算保护边界或成交名义额。
    ///
    /// 保护终结事件不消耗 TradeId。
    /// 市价吃单方的未使用报价预算不重新挂簿。
    pub fn execute_market_by_quote_with_protection(
        &mut self,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            let (events, spent, unused) =
                self.slice_market_quote(incoming, Some(context), math, trade_ids)?;
            return Ok(ProtectedMarketByQuoteExecution::new(events, spent, unused));
        }
        // ========================================================
        // 第一阶段：完整只读预检
        // ========================================================

        let plan = self.preflight_market_by_quote_with_protection(&incoming, context, math)?;

        // TradeId 只对应计划中的 Trade，不对应保护终结事件。
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for protected Reference MarketByQuote: \
         required={}, provided={}",
            plan.fills.len(),
            trade_ids.len(),
        );

        // 只聚合预检已经确定的本金，
        // 不重新调用 MarketMath 或重新计算价格 * 数量。
        // 在第一次状态变更前确认金额计划内部一致。
        let mut planned_spent = QuoteAmount::new(0);

        for fill in &plan.fills {
            assert!(
                fill.notional.get() > 0,
                "protected MarketByQuote plan contains zero notional"
            );

            planned_spent = planned_spent
                .checked_add(fill.notional)
                .expect("preflighted protected Quote notional overflow");
        }

        assert_eq!(planned_spent, plan.spent_quote);

        assert_eq!(
            plan.spent_quote
                .checked_add(plan.unused_quote)
                .expect("preflighted protected Quote budget overflow")
                .get(),
            incoming.quote_budget.get(),
        );

        let event_count = plan
            .fills
            .len()
            .checked_add(usize::from(plan.stop.is_some()))
            .expect("protected MarketByQuote event count overflow");

        // 在修改挂单前完成完整事件容量分配。
        let mut events = Vec::with_capacity(event_count);

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let priority_before = self.next_priority;

        // ========================================================
        // 第二阶段：严格按已通过预检的成交计划提交
        // ========================================================

        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            // 参考簿重新读取当前最佳挂单，
            // 只用于确认提交顺序未偏离计划，不重新计算保护。
            let best_maker_id = self
                .orders(maker_side)
                .into_iter()
                .find(|maker| maker.remaining() > 0)
                .map(|maker| maker.original_order().order_id)
                .expect("preflighted protected Quote maker disappeared");

            assert_eq!(
                best_maker_id, fill.maker_order_id,
                "protected Quote maker diverged from price/FIFO plan",
            );

            // 按计划挂单标识符定位源 Vec 中的运行态。
            let maker_index = self
                .orders
                .iter()
                .position(|maker| maker.original_order().order_id == fill.maker_order_id)
                .expect("preflighted protected Quote maker ID missing");

            let maker = &self.orders[maker_index];

            assert_eq!(maker.original_order().order_id, fill.maker_order_id);
            assert_eq!(maker.original_order().side, maker_side);
            assert_eq!(maker.original_order().price, fill.maker_price);

            let maker_remaining = maker.remaining();
            let maker_priority = maker.priority();
            let fill_lots = fill.qty.get();

            assert!(
                fill_lots > 0 && fill_lots <= maker_remaining,
                "preflighted protected Quote fill quantity invalid"
            );

            let maker_after = maker_remaining
                .checked_sub(fill_lots)
                .expect("preflighted protected Quote maker underflow");

            // 事件价、量直接来自计划。
            // 本金已在第一阶段确认并计入 `plan.spent_quote`。
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.maker_order_id,
                taker_order_id: incoming.order_id,
                price: fill.maker_price,
                qty: fill.qty,
                maker_side,
            };

            // 从这里开始首次修改挂单。
            self.orders[maker_index]
                .apply_fill(fill.qty)
                .expect("preflighted protected Quote fill must succeed");

            if maker_after == 0 {
                // 完全成交：物理移除挂单。
                let removed = self.orders.remove(maker_index);
                self.reservations.remove(&fill.maker_order_id);

                assert_eq!(removed.original_order().order_id, fill.maker_order_id);
                assert_eq!(removed.remaining(), 0);
            } else {
                // 部分成交：保留源位置和 QueuePriority。
                assert_eq!(self.orders[maker_index].remaining(), maker_after);
                assert_eq!(self.orders[maker_index].priority(), maker_priority);
            }

            events.push(EngineEvent::Trade(trade));
        }

        // ========================================================
        // 第三阶段：追加最多一个保护终结事件
        // ========================================================

        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }

        assert_eq!(events.len(), event_count);

        // 此普通挂单路径不消耗优先级，也不挂入市价吃单方。
        assert_eq!(self.next_priority, priority_before);

        assert!(
            !self
                .orders
                .iter()
                .any(|maker| { maker.original_order().order_id == incoming.order_id }),
            "protected MarketByQuote taker must not rest"
        );

        // 金额来自只读计划，不在提交阶段重新计算。
        Ok(ProtectedMarketByQuoteExecution::new(
            events,
            plan.spent_quote,
            plan.unused_quote,
        ))
    }
}
