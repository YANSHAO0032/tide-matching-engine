use crate::{
    IncomingOrder, ReferenceOrder,
    market_plan::{
        MarketByQuotePlan, ProtectedMarketByQuotePlan, ProtectionBoundaryStop, QuoteFillPlan,
    },
};

use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
    effective_protection_at_command_start_with_source, maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, LimitGtcOrder, MarketByBase, MarketByQuote, MarketByQuoteExecution, MarketMath,
    MathError, OrderId, Price, Qty, QueuePriority, QuoteAmount, RejectReason, Side, TradeEvent,
    TradeId,
};

#[cfg(test)]
use matching_domain::ProtectionSource;

mod limit_execution;

/// 为 priority 计数器保留不可分配的高位保护空间。
///
/// 到达该边界前必须拒绝新 resting order，避免计数器 wrap 后破坏 FIFO 语义。
const PRIORITY_GUARD: u128 = 1_024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProtectedMarketByBasePlan {
    // 保留此前已验收的 maker ID + Qty fill plan。
    fills: Vec<(OrderId, Qty)>,

    // 至多一个终止点，即价格优先顺序中的首个越界 maker。
    stop: Option<ProtectionBoundaryStop>,
}
/// 单市场参考订单簿
///
/// orders 的原始顺序表示已知挂单先后。
#[derive(Debug, Default)]
pub struct ReferenceOrderBook {
    /// 挂单的权威存储顺序；同价 FIFO 的参考基线由该顺序加稳定排序得出。
    orders: Vec<ReferenceOrder>,
    /// 下一张成功 rest 的订单将取得的 market-local priority 值。
    next_priority: u128,
}

impl ReferenceOrderBook {
    /// 创建不含挂单且 priority 从零开始的单市场参考订单簿。
    pub fn new() -> Self {
        Self::default()
    }

    /// 将订单加入当前市场的参考订单簿。
    ///
    /// 校验顺序：
    /// 1. 拒绝重复订单 ID；
    /// 2. 检查 priority 高水位；
    /// 3. checked increment；
    /// 4. 创建运行态订单并追加到源 Vec。
    ///
    /// 失败时不修改现有订单、剩余量或 next_priority。
    pub fn rest(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        // 必须先检查重复 ID，即使计数器已经耗尽也是如此。
        if self
            .orders
            .iter()
            .any(|existing| existing.original_order().order_id == order.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }

        if self.next_priority >= u128::MAX - PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }

        // 所有可能失败的检查均在修改状态之前执行。
        let next = self
            .next_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;

        let priority = QueuePriority::new(self.next_priority);
        let resting = ReferenceOrder::new(order, priority);

        self.orders.push(resting);
        self.next_priority = next;

        Ok(())
    }

    /// 返回指定侧按价格优先、同价 FIFO 排列的只读视图
    ///
    /// Buy: 价格降序
    /// Sell: 价格升序
    ///
    /// 使用稳定排序保留同价订单在源 Vec 中的顺序
    /// 排序仅作用于引用列表，不修改底层订单
    pub fn orders(&self, side: Side) -> Vec<&ReferenceOrder> {
        let mut result: Vec<_> = self
            .orders
            .iter()
            .filter(|order| order.original_order().side == side)
            .collect();
        result.sort_by(|a, b| {
            let a_price = a.original_order().price;
            let b_price = b.original_order().price;
            match side {
                Side::Buy => b_price.cmp(&a_price),
                Side::Sell => a_price.cmp(&b_price),
            }
        });
        result
    }

    /// 返回指定侧价格/FIFO 优先级最高的订单。
    ///
    pub fn best(&self, side: Side) -> Option<&ReferenceOrder> {
        self.orders(side).into_iter().next()
    }

    /// 返回对 `incoming` 价格可成交的最优 resting maker。
    ///
    /// 选择遵循对手方价格优先和同价 FIFO，并忽略已经耗尽的运行态订单。
    pub fn best_crossing_maker(&self, incoming: &LimitGtcOrder) -> Option<&ReferenceOrder> {
        self.best_crossing_maker_index(incoming)
            .map(|index| &self.orders[index])
    }

    /// PostOnly 候选的只读预检。
    ///
    /// 输入代表 Limit GTC + PostOnly 候选。
    ///
    /// 检查顺序：
    /// 1. 全簿重复 OrderId
    /// 2. 是否与对手盘交叉
    ///
    /// Ok(()) 仅表示通过当前两项预检，
    /// 不表示订单已经接受或成功挂簿。
    ///
    /// 不修改订单簿，不检查或分配 QueuePriority。
    fn preflight_post_only(&self, order: &LimitGtcOrder) -> Result<(), RejectReason> {
        if self
            .orders
            .iter()
            .any(|existing| existing.original_order().order_id == order.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }

        if self.best_crossing_maker(order).is_some() {
            return Err(RejectReason::PostOnlyWouldCross);
        }

        Ok(())
    }

    /// 查找最佳可成交 maker 在源 `Vec` 中的索引。
    ///
    /// 该索引只在当前不可变借用期间有效，供后续规划和一次性执行定位同一 maker。
    pub(crate) fn best_crossing_maker_index(&self, incoming: &LimitGtcOrder) -> Option<usize> {
        let opposite_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let maker = self.orders(opposite_side).into_iter().find(|maker| {
            if maker.remaining() == 0 {
                return false;
            }
            let maker_price = maker.original_order().price;

            match incoming.side {
                Side::Buy => incoming.price >= maker_price,
                Side::Sell => incoming.price <= maker_price,
            }
        })?;
        self.orders
            .iter()
            .position(|source| std::ptr::eq(source, maker))
    }

    /// 规划下一笔成交，返回源 Vec 索引和成交数量。
    ///
    /// 不修改订单簿，索引仅供内部使用。
    fn plan_next_fill(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(usize, Qty)>, RejectReason> {
        if incoming.remaining() == 0 {
            return Ok(None);
        }
        let Some(maker_index) = self.best_crossing_maker_index(incoming.original_order()) else {
            return Ok(None);
        };
        // 计算双方当前最多能够成交的数量
        let maker_remaining = self.orders[maker_index].remaining();
        let fill_lots = incoming.remaining().min(maker_remaining);

        let fill_qty = Qty::try_new(fill_lots)?;
        Ok(Some((maker_index, fill_qty)))
    }

    /// 返回下一笔成交的 maker 与数量，但不修改双方运行态。
    ///
    /// 与内部 `plan_next_fill` 共用价格/FIFO 选择逻辑进行只读预览；
    /// 返回的引用在订单簿发生可变操作前有效。
    pub fn next_fill_plan(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(&ReferenceOrder, Qty)>, RejectReason> {
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        Ok(Some((&self.orders[maker_index], fill_qty)))
    }

    /// 执行至多一笔 next-fill，不生成事件，也不将 incoming 剩余量挂簿。
    ///
    /// 先规划并预检双方扣减，再修改运行态。
    /// 返回 maker 身份、maker 价格和成交量。
    pub fn execute_next_fill(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Option<(OrderId, Price, Qty)>, RejectReason> {
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        let lots = fill_qty.get();
        let maker = &self.orders[maker_index];

        maker
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        incoming
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        let maker_id = maker.original_order().order_id;
        let maker_price = maker.original_order().price;

        self.orders[maker_index].apply_fill(fill_qty)?;
        incoming.apply_fill(fill_qty)?;

        if self.orders[maker_index].remaining() == 0 {
            self.orders.remove(maker_index);
        }

        Ok(Some((maker_id, maker_price, fill_qty)))
    }

    /// 持续执行单笔成交，直到 incoming 耗尽或不存在可成交 maker。
    ///
    /// 结果按实际成交顺序排列，价格取各 resting maker 的价格。
    /// 每笔成交沿用 execute_next_fill 的双侧预检；不生成事件，也不 rest 剩余 incoming。
    pub fn execute_fills(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Vec<(OrderId, Price, Qty)>, RejectReason> {
        let mut fills = Vec::new();
        while let Some(fill) = self.execute_next_fill(incoming)? {
            fills.push(fill)
        }
        Ok(fills)
    }

    /// 按 OrderId 取消一张 resting order。
    ///
    /// 订单不存在时返回 OrderNotFound，不修改订单簿。
    /// 成功时返回被移除的订单，不消耗或重排 priority。
    pub fn cancel(&mut self, order_id: OrderId) -> Result<ReferenceOrder, RejectReason> {
        let index = self
            .orders
            .iter()
            .position(|order| order.original_order().order_id == order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        Ok(self.orders.remove(index))
    }

    /// 执行至多一笔真实成交，并转换为 TradeEvent。
    ///
    /// TradeId 由调用方提供；无可成交 maker 时返回 None。
    /// 不生成 EngineEvent，不写 WAL，不自动 rest。
    pub fn execute_next_fill_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        let maker_side = match incoming.original_order().side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let taker_order_id = incoming.original_order().order_id;

        let Some((maker_order_id, price, qty)) = self.execute_next_fill(incoming)? else {
            return Ok(None);
        };

        Ok(Some(TradeEvent {
            trade_id,
            maker_order_id,
            taker_order_id,
            price,
            qty,
            maker_side,
        }))
    }

    fn reject_quote_math_error(error: MathError) -> RejectReason {
        error.to_reject_reason().unwrap_or_else(|| {
            panic!(
                "ReferenceOrderBook invariant violation: \
             non-rejectable MarketByQuote math error: {error:?}"
            )
        })
    }

    /// 只读预检 MarketByBase。
    ///
    /// 返回完成本次撮合需要的 TradeId 数量。
    /// 不修改订单、remaining 或 next_priority。
    fn preflight_market_by_base(&self, incoming: &MarketByBase) -> Result<usize, RejectReason> {
        // 1. 活动订单 ID 重复必须优先拒绝。
        if self
            .orders
            .iter()
            .any(|maker| maker.original_order().order_id == incoming.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 市价单只读取对手盘。
        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut remaining = incoming.qty.get();
        let mut required_trade_ids = 0usize;

        // orders(side) 已保证：
        // Sell: price ASC + same-price FIFO
        // Buy: price DESC + same-price FIFO
        for maker in self.orders(maker_side) {
            if remaining == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            // 防御测试 fixture 中可能残留的耗尽订单。
            if maker_remaining == 0 {
                continue;
            }

            let fill = remaining.min(maker_remaining);

            // fill 必须严格大于零。
            remaining = remaining
                .checked_sub(fill)
                .expect("preflight MarketByBase remaining underflow");

            required_trade_ids = required_trade_ids
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }

        Ok(required_trade_ids)
    }

    /// 只读规划带保护的 MarketByBase。
    ///
    /// 返回按价格优先、同价 FIFO 排列的 maker ID 与计划数量。
    /// 遇到首个保护价之外的 maker 立即停止。
    ///
    /// 不修改盘口、不消耗 TradeId、不分配 QueuePriority。
    #[cfg(test)]
    fn preflight_market_by_base_with_protection(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<Vec<(OrderId, Qty)>, RejectReason> {
        Ok(self
            .preflight_protected_market_by_base_plan(incoming, context, math)?
            .fills)
    }

    /// 只读规划 MarketByQuote。
    ///
    /// - 重复活动 OrderId 优先拒绝；
    /// - maker 按价格优先、同价 FIFO；
    /// - 不足一 lot 时立即 dust 停止；
    /// - 不修改订单簿、不消耗 QueuePriority；
    /// - 不生成 TradeEvent 或 MarketByQuoteExecution。
    fn preflight_market_by_quote(
        &self,
        incoming: &MarketByQuote,
        math: &MarketMath,
    ) -> Result<MarketByQuotePlan, RejectReason> {
        // 1. 重复 ID 必须先于盘口读取。
        if self
            .orders
            .iter()
            .any(|maker| maker.original_order().order_id == incoming.order_id)
        {
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

        // 2. orders() 返回已经按价格优先、同价 FIFO
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

            // 余额不足一 lot，整张市价订单停止。
            let Some(max_qty) = affordable else {
                break;
            };

            // 4. Maker 当前剩余量限制实际成交量。
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

            // 6. Checked 金额更新，仍然只修改局部变量。
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
    /// 顺序：Duplicate → command-start protection → maker 边界
    ///      → quote 预算反推 → 本金计算 → 本地金额累计。
    ///
    /// 不生成事件、不消耗 TradeId、不修改 ReferenceOrderBook。
    fn preflight_market_by_quote_with_protection(
        &self,
        incoming: &MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByQuotePlan, RejectReason> {
        // 1. Duplicate 永远先于保护及数学计算。
        if self
            .orders
            .iter()
            .any(|maker| maker.original_order().order_id == incoming.order_id)
        {
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

        // orders(maker_side) 已经是价格优先、同价 FIFO 的只读视图。
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

                // 首个越界 maker 立即停止。
                break;
            }

            // 4. 只有边界允许后，才进行预算反推。
            let affordable = math
                .max_qty_for_quote(remaining_quote, maker_price)
                .map_err(Self::reject_quote_math_error)?;

            // Dust：不足一个合法 lot，整单停止；不生成保护 stop。
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

    /// 市价按基础币数量立即成交。
    ///
    /// - 活动重复 ID 在任何 mutation 前拒绝。
    /// - TradeId 不足在任何 mutation 前 panic（测试/Reference fail-closed）。
    /// - maker 按价格优先、同价 FIFO 选择。
    /// - 市价 taker 剩余量不挂簿。
    /// - 不分配 QueuePriority。
    pub fn execute_market_by_base(
        &mut self,
        incoming: MarketByBase,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        // 1. 只读预检。
        let required = self.preflight_market_by_base(&incoming)?;

        // 2. 必须先验证 TradeId 足够，再允许任何 mutation。
        // 与现有 differential Reference adapter 的 fail-closed 断言约定一致。
        assert!(
            trade_ids.len() >= required,
            "insufficient TradeIds for Reference MarketByBase: \
         required={required}, provided={}",
            trade_ids.len()
        );

        // 3. 开始提交。
        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut remaining = incoming.qty.get();
        let mut trades = Vec::with_capacity(required);

        for trade_id in trade_ids.iter().copied().take(required) {
            // 按当前盘口重新取得最佳 maker。
            // 使用原有只读排序语义，不依赖 OrderId 数值排序。
            let maker_index = {
                let maker = self
                    .orders(maker_side)
                    .into_iter()
                    .find(|maker| maker.remaining() > 0)
                    .expect("preflighted MarketByBase maker disappeared");
                let maker_order_id = maker.original_order().order_id;
                self.orders
                    .iter()
                    .position(|source| source.original_order().order_id == maker_order_id)
                    .expect("preflighted MarketByBase maker index missing")
            };

            let maker = &self.orders[maker_index];
            let maker_order_id = maker.original_order().order_id;
            let maker_price = maker.original_order().price;

            // 双方剩余量取最小值，保证严格正成交。
            let fill_lots = remaining.min(maker.remaining());

            let fill_qty =
                Qty::try_new(fill_lots).expect("preflighted MarketByBase fill must be positive");

            // 4. 扣减 maker。
            self.orders[maker_index]
                .apply_fill(fill_qty)
                .expect("preflighted MarketByBase maker fill must succeed");

            remaining = remaining
                .checked_sub(fill_lots)
                .expect("preflighted MarketByBase remaining underflow");

            // 5. 耗尽 maker 直接删除，部分 maker 保留。
            if self.orders[maker_index].remaining() == 0 {
                self.orders.remove(maker_index);
            }

            // 6. TradeEvent 必须使用真实 maker 成交价格。
            trades.push(TradeEvent {
                trade_id,
                maker_order_id,
                taker_order_id: incoming.order_id,
                price: maker_price,
                qty: fill_qty,
                maker_side,
            });
        }

        debug_assert_eq!(trades.len(), required);

        // remaining 即使大于 0，也直接终结，不执行 rest。
        Ok(trades)
    }

    /// Reference-only：按命令开始时固定的保护边界执行 MarketByBase。
    ///
    /// 先完整预检，再提交成交；保护终止不消耗 TradeId。
    /// 不自动 rest taker，不修改 next_priority。
    pub fn execute_market_by_base_with_protection(
        &mut self,
        incoming: MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        // 阶段一：完整只读计划。
        let plan = self.preflight_protected_market_by_base_plan(&incoming, context, math)?;

        // TradeId 只为 Trade 分配，不为 ProtectionTriggered 分配。
        // 与现有 Reference MarketByBase 保持一致：
        // 数量不足时，在任何 mutation 前 fail-closed panic。
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for protected Reference MarketByBase"
        );

        // 阶段二：提交。
        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let event_count = plan
            .fills
            .len()
            .checked_add(usize::from(plan.stop.is_some()))
            .expect("protected MarketByBase event count overflow");

        let mut events = Vec::with_capacity(event_count);

        for ((maker_order_id, qty), trade_id) in
            plan.fills.into_iter().zip(trade_ids.iter().copied())
        {
            // 按预检时记录的 maker ID 定位。
            // 不重新选择最佳 maker，也不重新判断保护边界。
            let maker_index = self
                .orders
                .iter()
                .position(|maker| maker.original_order().order_id == maker_order_id)
                .expect("preflighted maker disappeared");

            let maker = &mut self.orders[maker_index];

            let maker_price = maker.original_order().price;

            // 预检已验证计划数量不超过 maker.remaining。
            maker
                .apply_fill(qty)
                .expect("preflighted protected fill must succeed");

            let exhausted = maker.remaining() == 0;

            if exhausted {
                self.orders.remove(maker_index);
            }

            events.push(EngineEvent::Trade(TradeEvent {
                trade_id,
                maker_order_id,
                taker_order_id: incoming.order_id,
                price: maker_price,
                qty,
                maker_side,
            }));
        }

        // 所有 Trade 后，紧接一个保护终结事件。
        // 没有越界则不产生该事件。
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }

        // 不将未成交的 taker 余量挂入 Reference。
        Ok(events)
    }

    /// 按报价币预算执行市价订单。
    ///
    /// 所有可预期失败都发生在首笔 mutation 前：
    /// 1. 只读规划整个 maker 成交序列；
    /// 2. 检查调用方 TradeId 数量；
    /// 3. 按计划逐笔提交，不重新计算 notional。
    ///
    /// 市价 taker 不挂簿，不消耗 QueuePriority。
    pub fn execute_market_by_quote(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<MarketByQuoteExecution, RejectReason> {
        // 1. 全量只读预检。
        let plan = self.preflight_market_by_quote(&incoming, math)?;

        // 2. TradeId 必须在任何 maker mutation 前充足。
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
            // 使用计划中的 maker ID 定位，而不是重新选择最优 maker。
            let maker_index = self
                .orders
                .iter()
                .position(|maker| maker.original_order().order_id == fill.maker_order_id)
                .expect("preflighted MarketByQuote maker disappeared");

            let maker = &self.orders[maker_index];

            // 预检与提交处于同一 writer 生命周期，
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

            // 先构造事件，再修改 maker。
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

            // Fully filled maker 从 Reference Vec 中物理移除。
            if maker_after == 0 {
                let removed = self.orders.remove(maker_index);

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

    /// Reference 受保护 MarketByQuote 执行。
    ///
    /// 完整只读预检、TradeId 数量校验及事件容量准备
    /// 必须先于任何订单簿 mutation。
    ///
    /// 使用预检计划中的 maker、价格、数量和 notional；
    /// 不重新计算保护边界或成交名义额。
    ///
    /// 保护终结事件不消耗 TradeId。
    /// 市价 taker 的未使用报价预算不重新挂簿。
    pub fn execute_market_by_quote_with_protection(
        &mut self,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
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

        // 只聚合预检已经确定的 notional，
        // 不重新调用 MarketMath 或重新计算 price * qty。
        // 在第一次 mutation 前确认金额计划内部一致。
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

        // 在修改 maker 前完成完整事件容量分配。
        let mut events = Vec::with_capacity(event_count);

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let priority_before = self.next_priority;

        // ========================================================
        // 第二阶段：严格按已通过预检的 fill plan 提交
        // ========================================================

        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            // Reference 重新读取当前最佳 maker，
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

            // 按计划 maker ID 定位源 Vec 中的运行态订单。
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

            // 事件价、量直接来自 plan。
            // notional 已在第一阶段确认并计入 plan.spent_quote。
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.maker_order_id,
                taker_order_id: incoming.order_id,
                price: fill.maker_price,
                qty: fill.qty,
                maker_side,
            };

            // 从这里开始首次修改 maker。
            self.orders[maker_index]
                .apply_fill(fill.qty)
                .expect("preflighted protected Quote fill must succeed");

            if maker_after == 0 {
                // 完全成交：物理移除 maker。
                let removed = self.orders.remove(maker_index);

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

        // 市价成交不消耗 priority，也不 rest taker。
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

    fn preflight_protected_market_by_base_plan(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByBasePlan, RejectReason> {
        // 1. Duplicate 优先于保护计算。
        if self
            .orders
            .iter()
            .any(|maker| maker.original_order().order_id == incoming.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 使用带来源的命令开始保护 helper。
        //    只计算一次，错误原样传播。
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

        let mut remaining = incoming.qty.get();
        let mut fills = Vec::new();
        let mut stop = None;

        // 3. 只读扫描，复用 Reference 的价格/FIFO 排序。
        for maker in self.orders(maker_side) {
            if remaining == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            if maker_remaining == 0 {
                continue;
            }

            let maker_price = maker.original_order().price;

            // 4. 判断当前 maker 是否位于有效保护边界内。
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

                // 首个越界 maker 不计入成交计划。
                break;
            }

            let fill_lots = remaining.min(maker_remaining);

            let qty = Qty::try_new(fill_lots).expect("positive remaining and maker quantity");

            remaining = remaining
                .checked_sub(fill_lots)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            fills.push((maker.original_order().order_id, qty));
        }

        Ok(ProtectedMarketByBasePlan { fills, stop })
    }
}

#[cfg(test)]
mod tests {
    //! 覆盖 T02 参考模型的排序、成交、取消和 priority 边界。
    use super::*;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, UserId};

    /// 构造带指定业务字段的 resting order fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> ReferenceOrder {
        ReferenceOrder::new(
            LimitGtcOrder {
                order_id: OrderId::new(id),
                user_id: UserId::new(id as u64),
                side,
                price: Price::try_new(price).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            },
            QueuePriority::new(0),
        )
    }

    /// 直接构造非空参考簿，仅用于表达需要特定源顺序的测试前置条件。
    fn book(orders: Vec<ReferenceOrder>) -> ReferenceOrderBook {
        ReferenceOrderBook {
            orders,
            next_priority: 0,
        }
    }

    /// 提取只读视图中的 order ID，便于断言显示顺序。
    fn ids(orders: &[&ReferenceOrder]) -> Vec<OrderId> {
        orders
            .iter()
            .map(|order| order.original_order().order_id)
            .collect()
    }

    #[test]
    fn empty_book_and_empty_side() {
        let empty = ReferenceOrderBook::new();

        assert!(empty.orders(Side::Buy).is_empty());
        assert!(empty.orders(Side::Sell).is_empty());
        assert!(empty.best(Side::Buy).is_none());
        assert!(empty.best(Side::Sell).is_none());

        let buy_only = book(vec![order(1, Side::Buy, 90, 10)]);
        assert!(buy_only.orders(Side::Sell).is_empty());
        assert!(buy_only.best(Side::Sell).is_none());

        let sell_only = book(vec![order(2, Side::Sell, 110, 10)]);
        assert!(sell_only.orders(Side::Buy).is_empty());
        assert!(sell_only.best(Side::Buy).is_none());
    }

    #[test]
    fn buy_and_sell_views_are_isolated_and_price_sorted() {
        // 最高 bid=95，最低 ask=101，不构成交叉盘口。
        let book = book(vec![
            order(1, Side::Buy, 90, 10),
            order(2, Side::Sell, 105, 10),
            order(3, Side::Buy, 95, 10),
            order(4, Side::Sell, 101, 10),
            order(5, Side::Buy, 92, 10),
            order(6, Side::Sell, 103, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(3), OrderId::new(5), OrderId::new(1)]
        );
        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(6), OrderId::new(2)]
        );
    }

    #[test]
    fn buy_same_price_preserves_fifo_not_id_order() {
        let book = book(vec![
            order(30, Side::Buy, 90, 10),
            order(10, Side::Buy, 90, 10),
            order(20, Side::Buy, 90, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );
    }

    #[test]
    fn sell_same_price_preserves_fifo_not_id_order() {
        let book = book(vec![
            order(30, Side::Sell, 110, 10),
            order(10, Side::Sell, 110, 10),
            order(20, Side::Sell, 110, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );
    }

    #[test]
    fn later_better_price_precedes_earlier_worse_price() {
        let book = book(vec![
            order(1, Side::Buy, 90, 10),
            order(2, Side::Sell, 110, 10),
            order(3, Side::Buy, 95, 10),
            order(4, Side::Sell, 105, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(3), OrderId::new(1)]
        );
        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(2)]
        );
    }

    #[test]
    fn price_boundaries_sort_correctly() {
        // 分别测试两个单侧簿，避免构造交叉盘口。
        let buys = book(vec![
            order(1, Side::Buy, 1, 10),
            order(2, Side::Buy, i64::MAX, 10),
        ]);
        assert_eq!(
            ids(&buys.orders(Side::Buy)),
            vec![OrderId::new(2), OrderId::new(1)]
        );

        let sells = book(vec![
            order(3, Side::Sell, i64::MAX, 10),
            order(4, Side::Sell, 1, 10),
        ]);
        assert_eq!(
            ids(&sells.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(3)]
        );
    }

    #[test]
    fn repeated_queries_do_not_mutate_source_or_remaining() {
        let mut first = order(30, Side::Buy, 90, 10);
        first.apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let book = book(vec![
            first,
            order(10, Side::Sell, 110, 20),
            order(20, Side::Buy, 95, 30),
            order(40, Side::Sell, 105, 40),
        ]);

        // 记录源 Vec 的顺序、完整原始订单及运行态剩余量。
        let snapshot: Vec<_> = book
            .orders
            .iter()
            .map(|order| (order.original_order().clone(), order.remaining()))
            .collect();

        let first_buy = ids(&book.orders(Side::Buy));
        let first_sell = ids(&book.orders(Side::Sell));

        for _ in 0..3 {
            assert_eq!(ids(&book.orders(Side::Buy)), first_buy);
            assert_eq!(ids(&book.orders(Side::Sell)), first_sell);

            assert!(book.best(Side::Buy).is_some());
            assert!(book.best(Side::Sell).is_some());
        }

        let after: Vec<_> = book
            .orders
            .iter()
            .map(|order| (order.original_order().clone(), order.remaining()))
            .collect();

        assert_eq!(snapshot, after);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }

    /// 构造经公开 `rest` 路径写入参考簿的合法订单 fixture。
    fn candidate(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 保存完整可观察 resting 状态，用于失败原子性断言。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|order| {
                (
                    order.original_order().clone(),
                    order.remaining(),
                    order.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn rest_assigns_monotonically_increasing_priorities() {
        let mut book = ReferenceOrderBook::new();

        for id in [30, 10, 20] {
            book.rest(candidate(id, Side::Buy, 90, 10)).unwrap();
        }

        let view = book.orders(Side::Buy);

        let actual: Vec<_> = view
            .iter()
            .map(|order| (order.original_order().order_id, order.priority().get()))
            .collect();

        assert_eq!(
            actual,
            vec![
                (OrderId::new(30), 0),
                (OrderId::new(10), 1),
                (OrderId::new(20), 2),
            ]
        );

        assert_eq!(book.next_priority, 3);
    }

    #[test]
    fn real_rest_preserves_price_priority_and_both_sides_fifo() {
        let mut book = ReferenceOrderBook::new();

        // 双侧盘口不交叉：最高 bid=95，最低 ask=105。
        for order in [
            candidate(30, Side::Buy, 90, 10),
            candidate(40, Side::Sell, 110, 10),
            candidate(10, Side::Buy, 90, 10),
            candidate(50, Side::Sell, 110, 10),
            candidate(20, Side::Buy, 90, 10),
            candidate(60, Side::Sell, 110, 10),
            // 后到的更优价格必须排在前面。
            candidate(70, Side::Buy, 95, 10),
            candidate(80, Side::Sell, 105, 10),
        ] {
            book.rest(order).unwrap();
        }

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![
                OrderId::new(70),
                OrderId::new(30),
                OrderId::new(10),
                OrderId::new(20),
            ]
        );

        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![
                OrderId::new(80),
                OrderId::new(40),
                OrderId::new(50),
                OrderId::new(60),
            ]
        );

        assert_eq!(book.best(Side::Buy).unwrap().priority().get(), 6);
        assert_eq!(book.best(Side::Sell).unwrap().priority().get(), 7);
    }

    #[test]
    fn duplicate_order_id_does_not_mutate_book() {
        let mut book = ReferenceOrderBook::new();

        let original = candidate(1, Side::Buy, 90, 10);
        book.rest(original.clone()).unwrap();

        let before = snapshot(&book);
        let next_before = book.next_priority;

        // 同一订单 ID，即使方向、价格、数量不同也必须拒绝。
        let result = book.rest(candidate(1, Side::Sell, 110, 20));

        assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders[0].original_order(), &original);
        assert_eq!(book.orders[0].remaining(), 10);

        // 失败不消耗 priority，下一张合法订单获得紧接着的值。
        book.rest(candidate(2, Side::Buy, 90, 10)).unwrap();

        assert_eq!(book.orders[1].priority().get(), 1);
        assert_eq!(book.next_priority, 2);
    }

    #[test]
    fn priority_guard_allows_last_safe_rest_then_rejects() {
        let threshold = u128::MAX - PRIORITY_GUARD;

        // 只在测试模块中初始化高水位计数器。
        let mut book = ReferenceOrderBook {
            orders: Vec::new(),
            next_priority: threshold - 1,
        };

        // 临界点前的最后一次成功挂单。
        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        assert_eq!(book.orders[0].priority().get(), threshold - 1);
        assert_eq!(book.next_priority, threshold);

        let before = snapshot(&book);

        // 达到阈值后，不能再消耗 priority。
        assert_eq!(
            book.rest(candidate(2, Side::Buy, 90, 10)),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, threshold);
    }

    #[test]
    fn duplicate_id_takes_precedence_over_priority_exhaustion() {
        let threshold = u128::MAX - PRIORITY_GUARD;

        let mut book = ReferenceOrderBook {
            orders: Vec::new(),
            next_priority: threshold - 1,
        };

        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        let before = snapshot(&book);

        // 同时满足重复 ID 和高水位耗尽时，必须先返回 2008。
        assert_eq!(
            book.rest(candidate(1, Side::Buy, 90, 10)),
            Err(RejectReason::DuplicateOrderId)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, threshold);
    }

    #[test]
    fn failed_rest_preserves_existing_partial_fill() {
        let mut book = ReferenceOrderBook::new();

        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let before = snapshot(&book);
        let next_before = book.next_priority;

        assert_eq!(
            book.rest(candidate(1, Side::Buy, 90, 10)),
            Err(RejectReason::DuplicateOrderId)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }
    #[test]
    fn cancel_existing_order_preserves_other_orders() {
        let mut book = ReferenceOrderBook::new();

        for id in [30, 10, 20] {
            book.rest(candidate(id, Side::Buy, 95, 10)).unwrap();
        }

        // 预先部分成交待撤订单。
        book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        let canceled = book.cancel(OrderId::new(10)).unwrap();

        assert_eq!(canceled.original_order().order_id, OrderId::new(10));
        assert_eq!(canceled.remaining(), 7);
        assert_eq!(canceled.priority(), before[1].2);

        // 其余订单保持原有 FIFO 顺序及运行态。
        assert_eq!(snapshot(&book), vec![before[0].clone(), before[2].clone()]);
        assert_eq!(book.next_priority, priority_before);

        // 撤销后允许相同 OrderId 再次挂入。
        book.rest(candidate(10, Side::Buy, 95, 10)).unwrap();

        assert_eq!(book.orders[2].priority().get(), priority_before);
        assert_eq!(book.next_priority, priority_before + 1);
    }

    #[test]
    fn cancel_missing_order_is_failure_atomic() {
        let mut book = ReferenceOrderBook::new();

        book.rest(candidate(1, Side::Sell, 105, 10)).unwrap();
        book.rest(candidate(2, Side::Sell, 105, 10)).unwrap();

        // 包含部分成交 maker，防止只验证原始订单。
        book.orders[0].apply_fill(Qty::try_new(4).unwrap()).unwrap();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        assert!(matches!(
            book.cancel(OrderId::new(999)),
            Err(RejectReason::OrderNotFound)
        ));

        // 完整原始订单、remaining、priority、源顺序不变。
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);

        // 失败后仍能按原有 FIFO 选择 maker。
        let best = book.best(Side::Sell).unwrap();
        assert_eq!(best.original_order().order_id, OrderId::new(1));
        assert_eq!(best.remaining(), 6);
    }

    #[test]
    fn cancel_empty_book_is_failure_atomic() {
        let mut book = ReferenceOrderBook::new();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        assert!(matches!(
            book.cancel(OrderId::new(1)),
            Err(RejectReason::OrderNotFound)
        ));

        assert_eq!(snapshot(&book), before);
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, priority_before);
    }
    #[test]
    fn cancel_preserves_fifo_and_remaining_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            // 分别取消同价 FIFO 队列的首项、中间项和尾项。
            for cancel_index in 0..3 {
                let mut book = ReferenceOrderBook::new();

                // ID 顺序故意不同于挂单顺序。
                for id in [30, 10, 20] {
                    book.rest(candidate(id, side, 100, 10)).unwrap();
                }

                // 制造不同的运行态剩余量：
                // ID=30 -> 9
                // ID=10 -> 7
                // ID=20 -> 5
                for (index, filled) in [1, 3, 5].into_iter().enumerate() {
                    book.orders[index]
                        .apply_fill(Qty::try_new(filled).unwrap())
                        .unwrap();
                }

                let before = snapshot(&book);
                let priority_before = book.next_priority;

                let canceled_id = book.orders[cancel_index].original_order().order_id;

                let expected_canceled = before[cancel_index].clone();

                let expected_remaining: Vec<_> = before
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != cancel_index)
                    .map(|(_, state)| state.clone())
                    .collect();

                let expected_fifo: Vec<_> = expected_remaining
                    .iter()
                    .map(|(original, _, _)| original.order_id)
                    .collect();

                // 执行取消。
                let canceled = book.cancel(canceled_id).unwrap();

                // 返回值必须保留完整原始订单、剩余量和优先级。
                assert_eq!(canceled.original_order(), &expected_canceled.0);
                assert_eq!(canceled.remaining(), expected_canceled.1);
                assert_eq!(canceled.priority(), expected_canceled.2);

                // 原始委托数量不能被之前的部分成交修改。
                assert_eq!(canceled.original_order().qty.get(), 10);

                // source Vec 的相对顺序、完整原始订单、
                // remaining 和 QueuePriority 均保持不变。
                assert_eq!(snapshot(&book), expected_remaining);

                // 同价订单仍保持原来的 FIFO 相对顺序。
                assert_eq!(ids(&book.orders(side)), expected_fifo);

                // 取消不分配新的 QueuePriority。
                assert_eq!(book.next_priority, priority_before);

                // 只移除目标订单。
                assert_eq!(book.orders.len(), 2);
                assert!(
                    book.orders
                        .iter()
                        .all(|order| { order.original_order().order_id != canceled_id })
                );
            }
        }
    }
}

#[cfg(test)]
mod maker_query_tests {
    //! 验证 maker 查询的价格交叉、FIFO 与只读性质。
    use super::*;
    use matching_domain::{OrderId, Price, Qty, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将 maker 查询结果投影为订单身份，便于断言选择结果。
    fn maker_id(maker: Option<&ReferenceOrder>) -> Option<OrderId> {
        maker.map(|maker| maker.original_order().order_id)
    }

    /// 构造不交叉的双侧盘口，其中后挂入订单拥有更优价格。
    fn two_sided_book() -> ReferenceOrderBook {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(11, Side::Sell, 110, 10),
            order(21, Side::Buy, 90, 10),
            order(12, Side::Sell, 105, 10),
            order(22, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        book
    }

    #[test]
    fn buy_crosses_best_ask_and_sell_crosses_best_bid() {
        let book = two_sided_book();

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 108, 10))),
            Some(OrderId::new(12))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(101, Side::Sell, 92, 10))),
            Some(OrderId::new(22))
        );
    }

    #[test]
    fn equal_price_is_crossing_on_both_sides() {
        let book = two_sided_book();

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 10))),
            Some(OrderId::new(12))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(101, Side::Sell, 95, 10))),
            Some(OrderId::new(22))
        );
    }

    #[test]
    fn non_crossing_orders_return_none_on_both_sides() {
        let book = two_sided_book();

        // 买入价格低于最优卖价。
        assert!(
            book.best_crossing_maker(&order(100, Side::Buy, 104, 10))
                .is_none()
        );

        // 卖出价格高于最优买价。
        assert!(
            book.best_crossing_maker(&order(101, Side::Sell, 96, 10))
                .is_none()
        );
    }

    #[test]
    fn empty_opposite_side_returns_none() {
        let empty = ReferenceOrderBook::new();

        assert!(
            empty
                .best_crossing_maker(&order(100, Side::Buy, 100, 10))
                .is_none()
        );

        assert!(
            empty
                .best_crossing_maker(&order(101, Side::Sell, 100, 10))
                .is_none()
        );

        let mut buy_only = ReferenceOrderBook::new();
        buy_only.rest(order(1, Side::Buy, 90, 10)).unwrap();

        assert!(
            buy_only
                .best_crossing_maker(&order(100, Side::Buy, 100, 10))
                .is_none()
        );

        let mut sell_only = ReferenceOrderBook::new();
        sell_only.rest(order(2, Side::Sell, 110, 10)).unwrap();

        assert!(
            sell_only
                .best_crossing_maker(&order(101, Side::Sell, 100, 10))
                .is_none()
        );
    }

    #[test]
    fn same_price_makers_preserve_fifo_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(130, Side::Buy, 95, 10),
            order(10, Side::Sell, 105, 10),
            order(110, Side::Buy, 95, 10),
            order(20, Side::Sell, 105, 10),
            order(120, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        // ID 的数值顺序故意不同于挂单顺序。
        assert_eq!(
            maker_id(book.best_crossing_maker(&order(200, Side::Buy, 105, 10))),
            Some(OrderId::new(30))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(201, Side::Sell, 95, 10))),
            Some(OrderId::new(130))
        );
    }

    #[test]
    fn exhausted_maker_does_not_block_next_maker() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(30, Side::Sell, 105, 1)).unwrap();
        book.rest(order(10, Side::Sell, 105, 1)).unwrap();

        // 测试模块内部模拟已经完整成交的 resting 订单。
        book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 1))),
            Some(OrderId::new(10))
        );
    }

    #[test]
    fn repeated_queries_preserve_all_book_state() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(20, Side::Buy, 95, 10),
            order(10, Side::Sell, 105, 10),
        ] {
            book.rest(resting).unwrap();
        }

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        // 包括源顺序、全部原始字段、remaining 和 priority。
        let snapshot = |book: &ReferenceOrderBook| {
            book.orders
                .iter()
                .map(|resting| {
                    (
                        resting.original_order().clone(),
                        resting.remaining(),
                        resting.priority(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let before = snapshot(&book);
        let next_before = book.next_priority;

        let incoming_buy = order(100, Side::Buy, 105, 10);
        let incoming_sell = order(101, Side::Sell, 95, 10);

        for _ in 0..5 {
            assert_eq!(
                maker_id(book.best_crossing_maker(&incoming_buy)),
                Some(OrderId::new(30))
            );

            assert_eq!(
                maker_id(book.best_crossing_maker(&incoming_sell)),
                Some(OrderId::new(20))
            );
        }

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders.len(), 3);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }

    fn assert_index_matches_reference(
        book: &ReferenceOrderBook,
        incoming: &LimitGtcOrder,
        expected: Option<usize>,
    ) {
        let index = book.best_crossing_maker_index(incoming);
        let reference = book.best_crossing_maker(incoming);

        assert_eq!(index, expected);

        match (index, reference) {
            (Some(index), Some(maker)) => {
                assert!(std::ptr::eq(maker, &book.orders[index]));
            }
            (None, None) => {}
            _ => panic!("maker index and public reference disagree"),
        }
    }

    #[test]
    fn index_skips_exhausted_makers_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 1),
            order(130, Side::Buy, 95, 1),
            order(10, Side::Sell, 105, 1),
            order(110, Side::Buy, 95, 1),
        ] {
            book.rest(resting).unwrap();
        }

        // 仅测试 fixture 修改运行态，查询自身不负责扣减。
        book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);
        assert_eq!(book.orders[1].remaining(), 0);

        // 跳过耗尽订单，选择下一张同价 maker。
        assert_index_matches_reference(&book, &order(200, Side::Buy, 105, 1), Some(2));

        assert_index_matches_reference(&book, &order(201, Side::Sell, 95, 1), Some(3));
    }
}

#[cfg(test)]
mod post_only_preflight_tests {
    use super::*;
    use matching_domain::UserId;

    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitGtcOrder, u64, QueuePriority)>, u128) {
        (
            book.orders
                .iter()
                .map(|resting| {
                    (
                        resting.original_order().clone(),
                        resting.remaining(),
                        resting.priority(),
                    )
                })
                .collect(),
            book.next_priority,
        )
    }

    fn assert_read_only(
        book: &ReferenceOrderBook,
        candidate: LimitGtcOrder,
        expected: Result<(), RejectReason>,
    ) {
        let before = snapshot(book);
        let candidate_before = candidate.clone();

        for _ in 0..3 {
            assert_eq!(book.preflight_post_only(&candidate), expected);

            assert_eq!(snapshot(book), before);
            assert_eq!(candidate, candidate_before);
        }
    }
    #[test]
    fn post_only_crossing_boundaries_on_both_sides() {
        for (side, maker_side, maker_price, crossing, equal, non_crossing) in [
            (Side::Buy, Side::Sell, 105, 106, 105, 104),
            (Side::Sell, Side::Buy, 95, 94, 95, 96),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            for (i, (price, expected)) in [
                (crossing, Err(RejectReason::PostOnlyWouldCross)),
                (equal, Err(RejectReason::PostOnlyWouldCross)),
                (non_crossing, Ok(())),
            ]
            .into_iter()
            .enumerate()
            {
                assert_read_only(&book, order(100 + i as u128, side, price, 5), expected);
            }
        }
    }
    #[test]
    fn post_only_accepts_empty_or_same_side_book() {
        for side in [Side::Buy, Side::Sell] {
            let mut book = ReferenceOrderBook::new();

            assert_read_only(&book, order(100, side, 100, 1), Ok(()));

            book.rest(order(1, side, 100, 10)).unwrap();

            assert_read_only(&book, order(101, side, 100, 1), Ok(()));

            assert_read_only(&book, order(102, side, i64::MAX, 1), Ok(()));
        }
    }
    #[test]
    fn duplicate_id_precedes_post_only_crossing() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(7, Side::Buy, 90, 10)).unwrap();

        book.rest(order(8, Side::Sell, 105, 10)).unwrap();

        // 与本方现存 ID 重复，同时交叉。
        assert_read_only(
            &book,
            order(7, Side::Buy, 105, 1),
            Err(RejectReason::DuplicateOrderId),
        );

        assert_read_only(
            &book,
            order(8, Side::Sell, 90, 1),
            Err(RejectReason::DuplicateOrderId),
        );

        // 与对手方 ID 重复，同时交叉。
        assert_read_only(
            &book,
            order(8, Side::Buy, 105, 1),
            Err(RejectReason::DuplicateOrderId),
        );

        assert_read_only(
            &book,
            order(7, Side::Sell, 90, 1),
            Err(RejectReason::DuplicateOrderId),
        );

        // 即使不交叉，重复 ID 仍然拒绝。
        assert_read_only(
            &book,
            order(7, Side::Buy, 89, 1),
            Err(RejectReason::DuplicateOrderId),
        );
    }
    #[test]
    fn multi_level_fifo_and_remaining_are_preserved() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(90, Side::Sell, 110, 10),
            order(30, Side::Sell, 105, 10),
            order(10, Side::Sell, 105, 10),
            order(290, Side::Buy, 90, 10),
            order(230, Side::Buy, 95, 10),
            order(210, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        // 模拟一张已经部分成交的 maker。
        book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        // 验证现有 maker 查询选择的 FIFO 首单。
        assert_eq!(
            book.best_crossing_maker(&order(300, Side::Buy, 105, 1))
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(30),
        );

        assert_eq!(
            book.best_crossing_maker(&order(301, Side::Sell, 95, 1))
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(230),
        );

        for (id, side, price, expected) in [
            (300, Side::Buy, 105, Err(RejectReason::PostOnlyWouldCross)),
            (302, Side::Buy, 104, Ok(())),
            (301, Side::Sell, 95, Err(RejectReason::PostOnlyWouldCross)),
            (303, Side::Sell, 96, Ok(())),
        ] {
            assert_read_only(&book, order(id, side, price, 1), expected);
        }

        assert_eq!(book.orders[1].remaining(), 7);
    }
    #[test]
    fn post_only_handles_price_extremes() {
        let mut sell_book = ReferenceOrderBook::new();

        sell_book.rest(order(1, Side::Sell, i64::MAX, 1)).unwrap();

        assert_read_only(
            &sell_book,
            order(2, Side::Buy, i64::MAX, 1),
            Err(RejectReason::PostOnlyWouldCross),
        );

        assert_read_only(&sell_book, order(3, Side::Buy, i64::MAX - 1, 1), Ok(()));

        let mut buy_book = ReferenceOrderBook::new();

        buy_book.rest(order(4, Side::Buy, 1, 1)).unwrap();

        assert_read_only(
            &buy_book,
            order(5, Side::Sell, 1, 1),
            Err(RejectReason::PostOnlyWouldCross),
        );

        assert_read_only(&buy_book, order(6, Side::Sell, 2, 1), Ok(()));

        let mut extreme_book = ReferenceOrderBook::new();

        extreme_book.rest(order(7, Side::Buy, i64::MAX, 1)).unwrap();

        assert_read_only(
            &extreme_book,
            order(8, Side::Sell, 1, 1),
            Err(RejectReason::PostOnlyWouldCross),
        );
    }
    #[test]
    fn preflight_does_not_check_or_consume_priority() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        // 测试专用：模拟已达到 priority 极值。
        book.next_priority = u128::MAX;

        assert_read_only(&book, order(2, Side::Buy, 104, 1), Ok(()));

        assert_read_only(
            &book,
            order(3, Side::Buy, 105, 1),
            Err(RejectReason::PostOnlyWouldCross),
        );

        assert_read_only(
            &book,
            order(1, Side::Buy, 105, 1),
            Err(RejectReason::DuplicateOrderId),
        );
    }
}

#[cfg(test)]
mod next_fill_planner_tests {
    //! 验证只读 next-fill 规划的 maker、数量和状态保持性质。
    use super::ReferenceOrderBook;
    use crate::IncomingOrder;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    /// 构造最高 bid 为 95、最低 ask 为 105 的不交叉盘口。
    fn two_sided_book() -> ReferenceOrderBook {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();
        book.rest(order(2, Side::Buy, 95, 10)).unwrap();

        book
    }

    /// 验证公开规划结果、maker 对象身份与成交数量。
    fn assert_plan(
        book: &ReferenceOrderBook,
        incoming: &IncomingOrder,
        expected: Option<(usize, u64)>,
    ) {
        let actual = book.next_fill_plan(incoming).unwrap();

        match (actual, expected) {
            (None, None) => {
                assert!(
                    book.best_crossing_maker(incoming.original_order())
                        .is_none()
                );
            }

            (Some((maker, qty)), Some((index, lots))) => {
                // 必须是源 Vec 中的同一个 ReferenceOrder。
                assert!(std::ptr::eq(maker, &book.orders[index]));

                // 必须与已有的 maker 选择逻辑保持一致。
                let selected = book.best_crossing_maker(incoming.original_order()).unwrap();

                assert!(std::ptr::eq(maker, selected));

                // 计划成交量始终是合法的正 Qty。
                assert_eq!(qty.get(), lots);
                assert!(qty.get() > 0);
            }

            (actual, expected) => {
                panic!(
                    "unexpected plan: actual={actual:?}, \
                     expected={expected:?}"
                );
            }
        }
    }

    /// 保存完整订单、运行态剩余量和 FIFO priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|resting| {
                (
                    resting.original_order().clone(),
                    resting.remaining(),
                    resting.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn buy_and_sell_exact_fill() {
        let book = two_sided_book();

        // Buy @105 = Ask @105。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 10)));

        // Sell @95 = Bid @95。
        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 10)));

        // 查询不会执行成交。
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[1].remaining(), 10);
    }

    #[test]
    fn buy_and_sell_cross_at_better_prices() {
        let book = two_sided_book();

        assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((0, 10)));

        assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((1, 10)));
    }

    #[test]
    fn maker_partial_fill_on_both_sides() {
        let book = two_sided_book();

        // incoming 比 maker 小，maker 只会部分成交。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 4), Some((0, 4)));

        assert_plan(&book, &incoming(101, Side::Sell, 95, 6), Some((1, 6)));

        // 这里只生成计划，没有实际扣减。
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[1].remaining(), 10);
    }

    #[test]
    fn taker_partial_fill_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Buy, 95, 4)).unwrap();

        // incoming 比 maker 大，只能成交 maker 的剩余量。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 3)));

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 4)));

        assert_eq!(book.orders[0].remaining(), 3);
        assert_eq!(book.orders[1].remaining(), 4);
    }

    #[test]
    fn empty_book_returns_no_plan() {
        let book = ReferenceOrderBook::new();

        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
    }

    #[test]
    fn non_crossing_prices_return_no_plan() {
        let book = two_sided_book();

        // Buy @104 < Ask @105。
        assert_plan(&book, &incoming(100, Side::Buy, 104, 10), None);

        // Sell @96 > Bid @95。
        assert_plan(&book, &incoming(101, Side::Sell, 96, 10), None);
    }

    #[test]
    fn empty_opposite_side_returns_no_plan() {
        let mut buy_only = ReferenceOrderBook::new();

        buy_only.rest(order(1, Side::Buy, 95, 10)).unwrap();

        // incoming 也是 Buy，没有卖盘。
        assert_plan(&buy_only, &incoming(100, Side::Buy, 200, 10), None);

        let mut sell_only = ReferenceOrderBook::new();

        sell_only.rest(order(2, Side::Sell, 105, 10)).unwrap();

        // incoming 也是 Sell，没有买盘。
        assert_plan(&sell_only, &incoming(101, Side::Sell, 1, 10), None);
    }

    #[test]
    fn best_price_takes_precedence_over_earlier_worse_price() {
        let mut book = ReferenceOrderBook::new();

        // 更优价格的 maker 后挂入。
        for resting in [
            order(11, Side::Sell, 110, 10),
            order(21, Side::Buy, 90, 10),
            order(12, Side::Sell, 105, 10),
            order(22, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((2, 10)));

        assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((3, 10)));
    }

    #[test]
    fn same_price_preserves_fifo_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        // 同价 ID 顺序故意与数值顺序不同。
        for resting in [
            order(30, Side::Sell, 105, 3),
            order(130, Side::Buy, 95, 3),
            order(10, Side::Sell, 105, 7),
            order(110, Side::Buy, 95, 8),
            order(20, Side::Sell, 105, 9),
            order(120, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        let buy = incoming(200, Side::Buy, 105, 10);
        let sell = incoming(201, Side::Sell, 95, 10);

        // 最早挂入的同价 maker 获得优先级。
        assert_plan(&book, &buy, Some((0, 3)));
        assert_plan(&book, &sell, Some((1, 3)));

        assert_eq!(
            book.best_crossing_maker(buy.original_order())
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(30),
        );

        assert_eq!(
            book.best_crossing_maker(sell.original_order())
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(130),
        );
    }

    #[test]
    fn exhausted_makers_are_skipped_and_fifo_is_preserved() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 3),
            order(130, Side::Buy, 95, 3),
            order(10, Side::Sell, 105, 7),
            order(110, Side::Buy, 95, 8),
        ] {
            book.rest(resting).unwrap();
        }

        let buy = incoming(200, Side::Buy, 105, 10);
        let sell = incoming(201, Side::Sell, 95, 10);

        // 耗尽之前，先选择最早挂入的订单。
        assert_plan(&book, &buy, Some((0, 3)));
        assert_plan(&book, &sell, Some((1, 3)));

        // 模拟已有成交，耗尽最早的两张 maker。
        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);
        assert_eq!(book.orders[1].remaining(), 0);

        // 跳过耗尽订单，选择下一张同价 maker。
        assert_plan(&book, &buy, Some((2, 7)));
        assert_plan(&book, &sell, Some((3, 8)));
    }

    #[test]
    fn all_exhausted_makers_produce_no_plan() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Buy, 95, 4)).unwrap();

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(4).unwrap()).unwrap();

        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
    }

    #[test]
    fn maximum_quantity_is_preserved_in_plan() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, u64::MAX)).unwrap();

        assert_plan(
            &book,
            &incoming(100, Side::Buy, 105, u64::MAX),
            Some((0, u64::MAX)),
        );

        assert_eq!(book.orders[0].remaining(), u64::MAX);
    }

    #[test]
    fn repeated_planning_does_not_mutate_book_or_incoming() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(20, Side::Buy, 95, 8),
            order(10, Side::Sell, 105, 12),
        ] {
            book.rest(resting).unwrap();
        }

        // 预先制造部分成交的 resting maker。
        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let buy = incoming(100, Side::Buy, 105, 9);
        let sell = incoming(101, Side::Sell, 95, 3);

        let book_before = snapshot(&book);
        let next_priority_before = book.next_priority;

        let buy_original = buy.original_order().clone();
        let sell_original = sell.original_order().clone();

        let buy_remaining = buy.remaining();
        let sell_remaining = sell.remaining();

        for _ in 0..10 {
            assert_plan(&book, &buy, Some((0, 7)));
            assert_plan(&book, &sell, Some((1, 3)));
        }

        // 订单簿的全部运行态保持不变。
        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, next_priority_before);
        assert_eq!(book.orders.len(), 3);

        // incoming 的原始订单和运行态也保持不变。
        assert_eq!(buy.original_order(), &buy_original);
        assert_eq!(sell.original_order(), &sell_original);

        assert_eq!(buy.remaining(), buy_remaining);
        assert_eq!(sell.remaining(), sell_remaining);

        // 原始订单数量不受规划影响。
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }
    #[test]
    fn partially_filled_buy_uses_current_remaining() {
        let mut book = two_sided_book();

        // Sell maker: original=10, remaining=8。
        book.orders[0].apply_fill(Qty::try_new(2).unwrap()).unwrap();

        // Buy incoming: original=10, remaining=3。
        let mut buy = incoming(100, Side::Buy, 105, 10);
        buy.apply_fill(Qty::try_new(7).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = buy.original_order().clone();
        let remaining_before = buy.remaining();

        // min(current remaining=3, maker remaining=8) = 3。
        for _ in 0..3 {
            assert_plan(&book, &buy, Some((0, 3)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[0].remaining(), 8);
        assert_eq!(book.orders[0].priority(), book_before[0].2);

        assert_eq!(buy.original_order(), &original_before);
        assert_eq!(buy.original_order().qty.get(), 10);
        assert_eq!(buy.remaining(), remaining_before);
        assert_eq!(buy.remaining(), 3);
    }

    #[test]
    fn partially_filled_sell_uses_current_remaining() {
        let mut book = two_sided_book();

        // Buy maker: original=10, remaining=4。
        book.orders[1].apply_fill(Qty::try_new(6).unwrap()).unwrap();

        // Sell incoming: original=10, remaining=7。
        let mut sell = incoming(101, Side::Sell, 95, 10);
        sell.apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = sell.original_order().clone();
        let remaining_before = sell.remaining();

        // min(current remaining=7, maker remaining=4) = 4。
        for _ in 0..3 {
            assert_plan(&book, &sell, Some((1, 4)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[1].remaining(), 4);
        assert_eq!(book.orders[1].priority(), book_before[1].2);

        assert_eq!(sell.original_order(), &original_before);
        assert_eq!(sell.original_order().qty.get(), 10);
        assert_eq!(sell.remaining(), remaining_before);
        assert_eq!(sell.remaining(), 7);
    }

    #[test]
    fn exhausted_buy_returns_none_with_crossing_maker() {
        let book = two_sided_book();

        let mut buy = incoming(100, Side::Buy, 105, 10);
        buy.apply_fill(Qty::try_new(10).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = buy.original_order().clone();
        let remaining_before = buy.remaining();

        assert_eq!(buy.remaining(), 0);

        // 确认存在 crossing maker，避免测试因无对手盘而通过。
        assert!(book.best_crossing_maker(buy.original_order()).is_some());

        for _ in 0..3 {
            assert!(matches!(book.next_fill_plan(&buy), Ok(None)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[0].priority(), book_before[0].2);

        assert_eq!(buy.original_order(), &original_before);
        assert_eq!(buy.original_order().qty.get(), 10);
        assert_eq!(buy.remaining(), remaining_before);
        assert_eq!(buy.remaining(), 0);
    }

    #[test]
    fn exhausted_sell_returns_none_with_crossing_maker() {
        let book = two_sided_book();

        let mut sell = incoming(101, Side::Sell, 95, 10);
        sell.apply_fill(Qty::try_new(10).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = sell.original_order().clone();
        let remaining_before = sell.remaining();

        assert_eq!(sell.remaining(), 0);

        // 确认存在 crossing maker。
        assert!(book.best_crossing_maker(sell.original_order()).is_some());

        for _ in 0..3 {
            assert!(matches!(book.next_fill_plan(&sell), Ok(None)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[1].remaining(), 10);
        assert_eq!(book.orders[1].priority(), book_before[1].2);

        assert_eq!(sell.original_order(), &original_before);
        assert_eq!(sell.original_order().qty.get(), 10);
        assert_eq!(sell.remaining(), remaining_before);
        assert_eq!(sell.remaining(), 0);
    }
}

#[cfg(test)]
mod next_fill_execution_tests {
    //! 验证 next-fill 执行、连续成交、TradeEvent 与固定命令带确定性。
    use super::*;
    use matching_domain::{OrderId, Price, Qty, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    /// 保存源 `Vec` 顺序、原始订单、remaining 和 priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn cross_price_uses_maker_identity_and_price() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 3);
            let original = taker.original_order().clone();
            let priority = book.orders[0].priority();

            let result = book.execute_next_fill(&mut taker);

            assert_eq!(
                result,
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(3).unwrap(),
                )))
            );

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn exact_fill_removes_maker_and_exhausts_incoming() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);
            let original = taker.original_order().clone();
            let next_priority = book.next_priority;

            assert_eq!(
                book.execute_next_fill(&mut taker),
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(10).unwrap(),
                )))
            );

            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, next_priority);

            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn partial_maker_keeps_priority() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let priority = book.orders[0].priority();
            let next_priority = book.next_priority;

            let mut taker = incoming(2, side, taker_price, 6);

            assert!(matches!(
                book.execute_next_fill(&mut taker),
                Ok(Some((_, _, qty))) if qty.get() == 6
            ));

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 4);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(book.next_priority, next_priority);
            assert_eq!(taker.remaining(), 0);
        }
    }

    #[test]
    fn filled_maker_preserves_incoming_remainder() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 4)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);
            let original = taker.original_order().clone();

            assert!(matches!(
                book.execute_next_fill(&mut taker),
                Ok(Some((_, _, qty))) if qty.get() == 4
            ));

            assert!(book.orders.is_empty());

            // 剩余量保留在 incoming，不自动 rest。
            assert_eq!(taker.remaining(), 6);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn execution_uses_current_incoming_remaining() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 8)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);

            taker.apply_fill(Qty::try_new(7).unwrap()).unwrap();

            let original = taker.original_order().clone();
            let priority = book.orders[0].priority();

            assert_eq!(
                book.execute_next_fill(&mut taker),
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(3).unwrap(),
                )))
            );

            assert_eq!(book.orders[0].remaining(), 5);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn no_plan_preserves_all_state() {
        for (side, incoming_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, Side::Sell, 105, 10)).unwrap();

            book.rest(order(2, Side::Buy, 95, 10)).unwrap();

            let mut taker = incoming(3, side, incoming_price, 10);

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let original = taker.original_order().clone();
            let remaining = taker.remaining();

            assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(taker.remaining(), remaining);
        }
    }

    #[test]
    fn exhausted_incoming_returns_none() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);

            taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();

            assert!(book.best_crossing_maker(taker.original_order()).is_some());

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let original = taker.original_order().clone();

            assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(taker.remaining(), 0);
        }
    }
    #[test]
    fn multi_fill_respects_price_then_fifo_and_maker_prices_on_both_sides() {
        for (side, maker_side, taker_price, better, worse) in [
            (Side::Buy, Side::Sell, 120, 105, 110),
            (Side::Sell, Side::Buy, 80, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            // 故意让更差价格先挂入；同价订单的 ID 与 FIFO 顺序相反。
            book.rest(order(90, maker_side, worse, 3)).unwrap();
            book.rest(order(30, maker_side, better, 2)).unwrap();
            book.rest(order(10, maker_side, better, 4)).unwrap();
            let next_before = book.next_priority;
            let mut taker = incoming(200, side, taker_price, 8);
            let original = taker.original_order().clone();

            let fills = book.execute_fills(&mut taker).unwrap();
            assert_eq!(
                fills,
                vec![
                    (
                        OrderId::new(30),
                        Price::try_new(better).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                    (
                        OrderId::new(10),
                        Price::try_new(better).unwrap(),
                        Qty::try_new(4).unwrap()
                    ),
                    (
                        OrderId::new(90),
                        Price::try_new(worse).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                ]
            );
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(90));
            assert_eq!(book.orders[0].remaining(), 1);
            assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
            assert_eq!(book.next_priority, next_before);
        }
    }

    #[test]
    fn multi_fill_keeps_unmatched_incoming_and_does_not_rest_it() {
        for (side, maker_side, taker_price, maker_price) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, maker_price, 2)).unwrap();
            book.rest(order(2, maker_side, maker_price, 3)).unwrap();
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 9);
            let original = taker.original_order().clone();

            assert_eq!(
                book.execute_fills(&mut taker).unwrap(),
                vec![
                    (
                        OrderId::new(1),
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                    (
                        OrderId::new(2),
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(3).unwrap()
                    ),
                ]
            );
            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, next_before);
            assert_eq!(taker.remaining(), 4);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn multi_fill_stops_at_non_crossing_price_and_preserves_resting_order() {
        for (side, maker_side, taker_price, crossing, non_crossing) in [
            (Side::Buy, Side::Sell, 107, 105, 110),
            (Side::Sell, Side::Buy, 93, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, non_crossing, 6)).unwrap();
            book.rest(order(2, maker_side, crossing, 3)).unwrap();
            let kept_original = book.orders[0].original_order().clone();
            let kept_priority = book.orders[0].priority();
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 8);
            let original = taker.original_order().clone();

            assert_eq!(
                book.execute_fills(&mut taker).unwrap(),
                vec![(
                    OrderId::new(2),
                    Price::try_new(crossing).unwrap(),
                    Qty::try_new(3).unwrap()
                )]
            );
            assert_eq!(taker.remaining(), 5);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order(), &kept_original);
            assert_eq!(book.orders[0].remaining(), 6);
            assert_eq!(book.orders[0].priority(), kept_priority);
            assert_eq!(book.next_priority, next_before);
        }
    }

    #[test]
    fn multi_fill_with_no_plan_or_exhausted_incoming_is_read_only() {
        for (side, taker_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, Side::Sell, 105, 10)).unwrap();
            book.rest(order(2, Side::Buy, 95, 10)).unwrap();
            let before = snapshot(&book);
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 10);
            let original = taker.original_order().clone();

            assert!(book.execute_fills(&mut taker).unwrap().is_empty());
            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, next_before);
            assert_eq!(taker.remaining(), 10);
            assert_eq!(taker.original_order(), &original);
        }

        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, Side::Sell, 105, 10)).unwrap();
        let before = snapshot(&book);
        let next_before = book.next_priority;
        let mut taker = incoming(100, Side::Buy, 110, 10);
        taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();
        let original = taker.original_order().clone();

        assert!(book.execute_fills(&mut taker).unwrap().is_empty());
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
    }

    #[test]
    fn empty_book_execute_fills_preserves_all_state() {
        let mut book = ReferenceOrderBook::new();
        let mut taker = incoming(100, Side::Buy, 105, 10);

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = taker.original_order().clone();
        let remaining_before = taker.remaining();

        let fills = book.execute_fills(&mut taker).unwrap();

        // 空簿不产生任何成交。
        assert!(fills.is_empty());

        // 订单簿及优先级计数器保持不变。
        assert_eq!(snapshot(&book), book_before);
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, priority_before);

        // IncomingOrder 的原始订单及剩余量保持不变。
        assert_eq!(taker.original_order(), &original_before);
        assert_eq!(taker.remaining(), remaining_before);
    }
    #[test]
    fn trade_event_uses_supplied_id_and_maker_price() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 3);
            let trade_id = TradeId::new(100);

            let trade = book
                .execute_next_fill_trade(&mut taker, trade_id)
                .unwrap()
                .unwrap();

            assert_eq!(trade.trade_id, trade_id);
            assert_eq!(trade.maker_order_id, OrderId::new(1));
            assert_eq!(trade.taker_order_id, OrderId::new(2));
            assert_eq!(trade.price, Price::try_new(maker_price).unwrap());
            assert_eq!(trade.qty.get(), 3);
            assert_eq!(trade.maker_side, maker_side);

            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(taker.remaining(), 0);
        }
    }
    #[test]
    fn trade_event_preserves_execution_semantics() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 4)).unwrap();

        let mut taker = incoming(2, Side::Buy, 110, 10);

        let trade = book
            .execute_next_fill_trade(&mut taker, TradeId::new(101))
            .unwrap()
            .unwrap();

        assert_eq!(trade.qty.get(), 4);
        assert_eq!(trade.price.get(), 105);
        assert_eq!(trade.maker_side, Side::Sell);

        assert!(book.orders.is_empty());
        assert_eq!(taker.remaining(), 6);
        assert_eq!(taker.original_order().qty.get(), 10);
    }
    #[test]
    fn no_fill_produces_no_trade_event() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        let mut taker = incoming(2, Side::Buy, 100, 10);

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = taker.original_order().clone();
        let remaining_before = taker.remaining();

        let result = book
            .execute_next_fill_trade(&mut taker, TradeId::new(102))
            .unwrap();

        assert!(result.is_none());
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(taker.original_order(), &original_before);
        assert_eq!(taker.remaining(), remaining_before);
    }
    /// 在独立空簿上执行固定命令带，供确定性回归比较。
    fn run_fixed_command_tape() -> (
        Vec<TradeEvent>,
        Vec<(LimitGtcOrder, u64, QueuePriority)>,
        u128,
    ) {
        // 每次独立从空簿开始。
        let mut book = ReferenceOrderBook::new();
        let mut trades = Vec::new();

        // 固定挂单顺序，priority 分别为 0、1、2、3。
        book.rest(order(30, Side::Sell, 105, 3)).unwrap();
        book.rest(order(10, Side::Sell, 105, 7)).unwrap();
        book.rest(order(20, Side::Buy, 95, 5)).unwrap();
        book.rest(order(40, Side::Sell, 110, 6)).unwrap();

        // Buy @105，初始数量 8。
        let mut taker = incoming(100, Side::Buy, 105, 8);

        // 第一笔：maker 30 完全成交 3。
        trades.push(
            book.execute_next_fill_trade(&mut taker, TradeId::new(501))
                .unwrap()
                .expect("first trade"),
        );

        // 第二笔：maker 10 成交 5，剩余 2。
        trades.push(
            book.execute_next_fill_trade(&mut taker, TradeId::new(502))
                .unwrap()
                .expect("second trade"),
        );

        // Incoming 已耗尽，不应继续成交。
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order().qty.get(), 8);

        // 取消部分成交后的 maker 10。
        let canceled = book.cancel(OrderId::new(10)).unwrap();

        assert_eq!(canceled.original_order().order_id, OrderId::new(10));
        assert_eq!(canceled.original_order().qty.get(), 7);
        assert_eq!(canceled.remaining(), 2);
        assert_eq!(canceled.priority(), QueuePriority::new(1));

        let final_snapshot = snapshot(&book);
        let next_priority = book.next_priority;

        (trades, final_snapshot, next_priority)
    }
    #[test]
    fn fixed_command_tape_is_deterministic() {
        let first = run_fixed_command_tape();
        let second = run_fixed_command_tape();

        // 两次完全独立运行，结果必须相同。
        assert_eq!(first, second);

        let (trades, final_book, next_priority) = first;

        // 手工定义预期事件顺序。
        let expected_trades = vec![
            TradeEvent {
                trade_id: TradeId::new(501),
                maker_order_id: OrderId::new(30),
                taker_order_id: OrderId::new(100),
                price: Price::try_new(105).unwrap(),
                qty: Qty::try_new(3).unwrap(),
                maker_side: Side::Sell,
            },
            TradeEvent {
                trade_id: TradeId::new(502),
                maker_order_id: OrderId::new(10),
                taker_order_id: OrderId::new(100),
                price: Price::try_new(105).unwrap(),
                qty: Qty::try_new(5).unwrap(),
                maker_side: Side::Sell,
            },
        ];

        assert_eq!(trades, expected_trades);

        // maker 30 因完全成交被移除。
        // maker 10 因 Cancel 被移除。
        // 剩余订单保持原 source 顺序与 priority。
        let expected_book = vec![
            (order(20, Side::Buy, 95, 5), 5, QueuePriority::new(2)),
            (order(40, Side::Sell, 110, 6), 6, QueuePriority::new(3)),
        ];

        assert_eq!(final_book, expected_book);

        // 成交、移除和取消均不消耗新的 priority。
        assert_eq!(next_priority, 4);
    }
}

#[cfg(test)]
mod property_tests {
    //! 用生成式输入覆盖 FIFO、数量守恒、取消和失败原子性。
    use super::*;
    use matching_domain::UserId;
    use proptest::prelude::*;

    /// 构造生成式测试使用的合法订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 保存源 `Vec` 顺序、原始订单、remaining 和 priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect()
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    proptest! {
        // 性质一：成交守恒、正成交量、同价 FIFO 前缀。
        #[test]
        fn prop_same_price_fifo_and_quantity_conservation(
            maker_qtys in prop::collection::vec(1u64..=32, 1..=8),
            taker_qty in 1u64..=256,
            maker_is_sell in any::<bool>(),
        ) {
            let maker_side = if maker_is_sell {
                Side::Sell
            } else {
                Side::Buy
            };
            let taker_side = if maker_is_sell {
                Side::Buy
            } else {
                Side::Sell
            };
            let maker_price = if maker_is_sell { 105 } else { 95 };
            let taker_price = if maker_is_sell { 110 } else { 90 };

            let mut book = ReferenceOrderBook::new();

            for (i, &qty) in maker_qtys.iter().enumerate() {
                book.rest(order(
                    i as u128 + 1,
                    maker_side,
                    maker_price,
                    qty,
                )).unwrap();
            }

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let total_maker: u64 = maker_qtys.iter().sum();

            let mut taker = incoming(
                100,
                taker_side,
                taker_price,
                taker_qty,
            );

            let fills = book.execute_fills(&mut taker).unwrap();

            // 期望成交量由生成的数据直接计算，
            // 不调用 planner 或其他撮合算法。
            let expected_total = taker_qty.min(total_maker);
            let actual_total: u64 =
                fills.iter().map(|(_, _, qty)| qty.get()).sum();

            prop_assert_eq!(actual_total, expected_total);
            prop_assert_eq!(
                actual_total + taker.remaining(),
                taker_qty
            );
            prop_assert_eq!(
                taker.original_order().qty.get(),
                taker_qty
            );

            // 手工构建 FIFO 前缀及每张 maker 的期望成交量。
            let mut unfilled = taker_qty;
            let mut expected_fills = Vec::new();
            let mut expected_book = Vec::new();

            for (i, &(ref original, remaining, priority))
                in before.iter().enumerate()
            {
                let filled = remaining.min(unfilled);

                if filled > 0 {
                    expected_fills.push((
                        original.order_id,
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(filled).unwrap(),
                    ));
                    unfilled -= filled;
                }

                let maker_remaining = remaining - filled;
                if maker_remaining > 0 {
                    expected_book.push((
                        original.clone(),
                        maker_remaining,
                        priority,
                    ));
                }

                prop_assert_eq!(
                    original.order_id,
                    OrderId::new(i as u128 + 1)
                );
            }

            // 同时验证 FIFO 前缀、每笔数量及 maker-price。
            prop_assert_eq!(fills, expected_fills);

            // 完全成交的 maker 被移除；
            // 未耗尽的 maker 保持 source 顺序及 priority。
            prop_assert_eq!(snapshot(&book), expected_book);
            prop_assert_eq!(book.next_priority, priority_before);
        }

        // 性质二：任意有效位置取消后的完整状态。
        #[test]
        fn prop_cancel_valid_position_preserves_remaining_book(
            quantities in prop::collection::vec(1u64..=100, 1..=8),
            index_seed in any::<usize>(),
            is_sell in any::<bool>(),
        ) {
            let side = if is_sell {
                Side::Sell
            } else {
                Side::Buy
            };

            let mut book = ReferenceOrderBook::new();

            for (i, &qty) in quantities.iter().enumerate() {
                book.rest(order(
                    i as u128 + 1,
                    side,
                    100,
                    qty,
                )).unwrap();

                // 预先制造不同的 maker remaining，
                // 同时保留 Qty 严格正值约束。
                let filled = qty / 2;
                if filled > 0 {
                    book.orders[i]
                        .apply_fill(Qty::try_new(filled).unwrap())
                        .unwrap();
                }
            }

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let target_index = index_seed % quantities.len();
            let target_id = before[target_index].0.order_id;

            // 期望状态仅通过过滤取消前的快照生成。
            let expected_remaining: Vec<_> = before
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != target_index)
                .map(|(_, state)| state.clone())
                .collect();

            let expected_canceled = before[target_index].clone();

            let canceled = book.cancel(target_id).unwrap();

            prop_assert_eq!(
                canceled.original_order(),
                &expected_canceled.0
            );
            prop_assert_eq!(
                canceled.remaining(),
                expected_canceled.1
            );
            prop_assert_eq!(
                canceled.priority(),
                expected_canceled.2
            );

            // 完整快照同时验证 source/FIFO 顺序、
            // 原始订单、remaining 和 QueuePriority。
            prop_assert_eq!(snapshot(&book), expected_remaining);
                let target_removed = book
            .orders
            .iter()
            .all(|resting| resting.original_order().order_id != target_id);

            prop_assert!(
                target_removed,
                "canceled order must not remain in the book"
            );

            prop_assert_eq!(
                book.orders.len(),
                quantities.len() - 1
            );
            prop_assert_eq!(
                book.next_priority,
                priority_before
            );
        }
    }
}

#[cfg(test)]
mod market_by_base_tests {
    use super::*;
    use matching_domain::{MarketByBase, MarketProtection, UserId};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn market(id: u128, side: Side, qty: u64) -> MarketByBase {
        MarketByBase {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            qty: Qty::try_new(qty).unwrap(),
            market_protection: None,
        }
    }

    fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitGtcOrder, u64, QueuePriority)>, u128) {
        (
            book.orders
                .iter()
                .map(|maker| {
                    (
                        maker.original_order().clone(),
                        maker.remaining(),
                        maker.priority(),
                    )
                })
                .collect(),
            book.next_priority,
        )
    }
    #[test]
    fn market_by_base_empty_book_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut book = ReferenceOrderBook::new();
            let before = snapshot(&book);

            let trades = book
                .execute_market_by_base(market(100, side, 10), &[])
                .unwrap();

            assert!(trades.is_empty());
            assert_eq!(snapshot(&book), before);
        }
    }
    #[test]
    fn market_by_base_partially_fills_maker_both_sides() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 105, 10)).unwrap();

            let priority_before = book.next_priority;

            let trades = book
                .execute_market_by_base(market(100, taker_side, 3), &[TradeId::new(500)])
                .unwrap();

            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].trade_id, TradeId::new(500));
            assert_eq!(trades[0].maker_order_id, OrderId::new(1));
            assert_eq!(trades[0].taker_order_id, OrderId::new(100));
            assert_eq!(trades[0].price.get(), 105);
            assert_eq!(trades[0].qty.get(), 3);
            assert_eq!(trades[0].maker_side, maker_side);

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.next_priority, priority_before);
        }
    }
    #[test]
    fn market_by_base_respects_price_priority_and_fifo() {
        for (taker_side, maker_side, worse, better) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();

            // 源 Vec 顺序故意不同于价格优先顺序和 OrderId 顺序。
            book.rest(limit(20, maker_side, worse, 4)).unwrap();
            book.rest(limit(30, maker_side, better, 3)).unwrap();
            book.rest(limit(10, maker_side, better, 5)).unwrap();

            let priority_before = book.next_priority;

            let trades = book
                .execute_market_by_base(
                    market(100, taker_side, 10),
                    &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
                )
                .unwrap();

            assert_eq!(trades.len(), 3);

            assert_eq!(
                trades.iter().map(|t| t.maker_order_id).collect::<Vec<_>>(),
                vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
            );

            assert_eq!(
                trades.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
                vec![better, better, worse]
            );

            assert_eq!(
                trades.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
                vec![3, 5, 2]
            );

            assert_eq!(
                trades.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
                vec![TradeId::new(501), TradeId::new(502), TradeId::new(503)]
            );

            // 最后一张 maker 部分成交，保留原始 payload 和 priority。
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(20));
            assert_eq!(book.orders[0].remaining(), 2);
            assert_eq!(book.next_priority, priority_before);
        }
    }
    #[test]
    fn market_by_base_discards_unfilled_taker_remainder() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 3)).unwrap();

            let priority_before = book.next_priority;

            let trades = book
                .execute_market_by_base(market(100, taker_side, 10), &[TradeId::new(600)])
                .unwrap();

            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].qty.get(), 3);

            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, priority_before);
        }
    }
    #[test]
    fn market_by_base_duplicate_id_is_failure_atomic() {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, Side::Sell, 100, 10)).unwrap();

        let before = snapshot(&book);

        let result = book.execute_market_by_base(market(1, Side::Buy, 5), &[]);

        assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        assert_eq!(snapshot(&book), before);
    }
    #[test]
    fn market_by_base_insufficient_trade_ids_preserves_state() {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(30, Side::Sell, 105, 3)).unwrap();
        book.rest(limit(10, Side::Sell, 110, 7)).unwrap();

        let before = snapshot(&book);

        // 需要两笔成交，却只提供一个 TradeId。
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.execute_market_by_base(market(100, Side::Buy, 10), &[TradeId::new(700)]);
        }));

        assert!(result.is_err());
        assert_eq!(snapshot(&book), before);
    }
    #[test]
    fn protected_market_by_base_preflight_respects_fifo_and_boundary() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (taker_side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let mut book = ReferenceOrderBook::new();

            // 故意打乱价格与 ID 的顺序。
            // ID=40 先于 ID=10 挂在边界价 100。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: Price::try_new(100_003).unwrap(),
                market_band_worst: Some(Price::try_new(100).unwrap()),
            };

            let before = snapshot(&book);
            let priority_before = book.next_priority;

            // 数量足够扫过全部 maker。
            // 只能规划 better + 两张边界价订单；
            // 首张 outside maker 必须终止扫描。
            let plan = book
                .preflight_market_by_base_with_protection(
                    &market(100, taker_side, 20),
                    &context,
                    &math,
                )
                .unwrap();

            assert_eq!(
                plan,
                vec![
                    (OrderId::new(30), Qty::try_new(2).unwrap()),
                    (OrderId::new(40), Qty::try_new(4).unwrap()),
                    (OrderId::new(10), Qty::try_new(3).unwrap()),
                ]
            );

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);

            // 数量不足：第二张订单仅部分计划成交。
            // 同价 FIFO 必须先选择 ID=40，而不是 ID=10。
            let plan = book
                .preflight_market_by_base_with_protection(
                    &market(101, taker_side, 5),
                    &context,
                    &math,
                )
                .unwrap();

            assert_eq!(
                plan,
                vec![
                    (OrderId::new(30), Qty::try_new(2).unwrap()),
                    (OrderId::new(40), Qty::try_new(3).unwrap()),
                ]
            );

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
        }
    }

    #[test]
    fn protected_market_by_base_preflight_without_boundary_scans_all_fifo_levels() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (taker_side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let mut book = ReferenceOrderBook::new();

            // 与已有保护边界测试使用相同的多档、同价 FIFO 数据。
            // 源 Vec 顺序故意不同于实际撮合顺序。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            // 没有用户保护，也没有市场 band。
            let incoming = market(100, taker_side, 20);
            assert_eq!(incoming.market_protection, None);

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: Price::try_new(100_003).unwrap(),
                market_band_worst: None,
            };

            let before = snapshot(&book);
            let priority_before = book.next_priority;

            let plan = book
                .preflight_market_by_base_with_protection(&incoming, &context, &math)
                .unwrap();

            // 无有效边界时，不能在 100 价位终止。
            // 必须继续纳入此前因保护边界而被排除的更远 maker。
            //
            // Buy:  90 -> 100 -> 100 -> 110 -> 120
            // Sell: 110 -> 100 -> 100 -> 90 -> 80
            assert_eq!(
                plan,
                vec![
                    (OrderId::new(30), Qty::try_new(2).unwrap()),
                    (OrderId::new(40), Qty::try_new(4).unwrap()),
                    (OrderId::new(10), Qty::try_new(3).unwrap()),
                    (OrderId::new(50), Qty::try_new(1).unwrap()),
                    (OrderId::new(90), Qty::try_new(2).unwrap()),
                ]
            );

            // 预检不得修改订单内容、remaining、FIFO 或优先级。
            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
        }
    }

    #[test]
    fn protected_market_by_base_preflight_propagates_errors_atomically() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: Price::try_new(100_003).unwrap(),
                market_band_worst: Some(Price::try_new(100_000).unwrap()),
            };

            let before = snapshot(&book);
            let priority_before = book.next_priority;

            // 1. 非法 bps：即使 band 有效，仍必须返回 3007。
            let invalid_bps = MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            };

            let expected = invalid_bps
                .user_protection_price(
                    context.reference_price_at_command_start,
                    taker_side,
                    math.tick_size(),
                )
                .unwrap_err();

            assert_eq!(expected as u16, 3007);

            let mut incoming = market(100, taker_side, 5);
            incoming.market_protection = Some(invalid_bps);

            assert_eq!(
                book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
                Err(expected)
            );

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);

            // 2. 显式 worst_price 未按 tick=10 对齐。
            let invalid_price = MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(Price::try_new(100_003).unwrap()),
            };

            let expected = invalid_price
                .user_protection_price(
                    context.reference_price_at_command_start,
                    taker_side,
                    math.tick_size(),
                )
                .unwrap_err();

            let mut incoming = market(100, taker_side, 5);
            incoming.market_protection = Some(invalid_price);

            assert_eq!(
                book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
                Err(expected)
            );

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
        }

        // 3. Buy 的滑点边界超过 i64 正价格值域：
        //    既有 Domain checked 算术错误必须原样传播。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(i64::MAX).unwrap(),
            market_band_worst: Some(Price::try_new(100_000).unwrap()),
        };

        let protection = MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        };

        let expected = protection
            .user_protection_price(
                context.reference_price_at_command_start,
                Side::Buy,
                math.tick_size(),
            )
            .unwrap_err();

        assert_eq!(expected, RejectReason::ArithmeticOverflow);

        let mut incoming = market(100, Side::Buy, 5);
        incoming.market_protection = Some(protection);

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        assert_eq!(
            book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
            Err(expected)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
    }
    #[test]
    fn protected_market_by_base_failures_preserve_book() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, Side::Sell, 90, 2)).unwrap();
        book.rest(limit(2, Side::Sell, 100, 4)).unwrap();
        book.rest(limit(3, Side::Sell, 110, 3)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: Some(p(100)),
        };

        let before = snapshot(&book);

        // 1. Duplicate 必须优先于非法 protection。
        let mut duplicate = market(1, Side::Buy, 10);
        duplicate.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        });

        assert_eq!(
            book.execute_market_by_base_with_protection(duplicate, &context, &math, &[],),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(snapshot(&book), before);

        // 2. 非法 bps 必须原样传播。
        let mut invalid = market(100, Side::Buy, 10);
        invalid.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        });

        let expected = invalid
            .market_protection
            .as_ref()
            .unwrap()
            .user_protection_price(
                context.reference_price_at_command_start,
                Side::Buy,
                math.tick_size(),
            )
            .unwrap_err();

        assert_eq!(expected as u16, 3007);

        assert_eq!(
            book.execute_market_by_base_with_protection(invalid, &context, &math, &[],),
            Err(expected)
        );
        assert_eq!(snapshot(&book), before);

        // 3. TradeId 不足：需要两个成交 ID，却只提供一个。
        let mut incoming = market(100, Side::Buy, 10);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(100)),
        });

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.execute_market_by_base_with_protection(
                incoming,
                &context,
                &math,
                &[TradeId::new(700)],
            );
        }));

        assert!(result.is_err());

        // 包括 maker payload、remaining、FIFO 和 next_priority。
        assert_eq!(snapshot(&book), before);
    }
    #[test]
    fn protected_entry_without_effective_boundary_matches_all_available() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, first, second) in [
            (Side::Buy, Side::Sell, 90, 110),
            (Side::Sell, Side::Buy, 110, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, second, 2)).unwrap();
            book.rest(limit(2, maker_side, first, 3)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: None,
            };

            let events = book
                .execute_market_by_base_with_protection(
                    market(100, side, 10),
                    &context,
                    &math,
                    &[TradeId::new(701), TradeId::new(702)],
                )
                .unwrap();

            assert_eq!(events.len(), 2);
            assert!(matches!(
                &events[0],
                EngineEvent::Trade(t)
                    if t.maker_order_id == OrderId::new(2)
            ));
            assert!(matches!(
                &events[1],
                EngineEvent::Trade(t)
                    if t.maker_order_id == OrderId::new(1)
            ));

            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, 2);
        }
    }

    #[test]
    fn protected_market_by_base_emits_trades_before_protection_stop() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better, outside, farther, user, band, source) in [
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                110,
                ProtectionSource::User,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                90,
                100,
                ProtectionSource::MarketBand,
            ),
        ] {
            let mut book = ReferenceOrderBook::new();

            // 故意打乱源顺序，验证价格优先与同价 FIFO。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let mut incoming = market(100, side, 20);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            });

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(band)),
            };

            let priority_before = book.next_priority;

            let events = book
                .execute_market_by_base_with_protection(
                    incoming,
                    &context,
                    &math,
                    &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
                )
                .unwrap();

            let expected_trade = |trade_id, maker_id, price, qty| {
                EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(trade_id),
                    maker_order_id: OrderId::new(maker_id),
                    taker_order_id: OrderId::new(100),
                    price: p(price),
                    qty: Qty::try_new(qty).unwrap(),
                    maker_side,
                })
            };

            assert_eq!(
                events,
                vec![
                    expected_trade(701, 30, better, 2),
                    expected_trade(702, 40, 100, 4),
                    expected_trade(703, 10, 100, 3),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source,
                    },
                ]
            );

            // 已成交的三个 maker 被移除。
            // 越界 maker 及更远 maker 完整保留。
            assert_eq!(
                snapshot(&book),
                (
                    vec![
                        (limit(90, maker_side, farther, 2), 2, QueuePriority::new(0),),
                        (limit(50, maker_side, outside, 1), 1, QueuePriority::new(4),),
                    ],
                    priority_before,
                )
            );

            // 市价单剩余 11 lots 不得挂簿。
            assert!(
                book.orders
                    .iter()
                    .all(|maker| maker.original_order().order_id != OrderId::new(100))
            );
        }
    }

    #[test]
    fn protected_market_by_base_boundary_equal_is_tradable() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)]
        {
            for (incoming_qty, should_stop) in [(2, false), (3, true)] {
                let mut book = ReferenceOrderBook::new();
                book.rest(limit(1, maker_side, 100, 2)).unwrap();
                book.rest(limit(2, maker_side, outside, 4)).unwrap();

                let mut incoming = market(100, side, incoming_qty);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100)),
                });

                let context = CommandStartProtectionContext {
                    reference_price_at_command_start: p(100_000),
                    market_band_worst: Some(p(100)),
                };

                let events = book
                    .execute_market_by_base_with_protection(
                        incoming,
                        &context,
                        &math,
                        &[TradeId::new(700)],
                    )
                    .unwrap();

                assert_eq!(
                    events[0],
                    EngineEvent::Trade(TradeEvent {
                        trade_id: TradeId::new(700),
                        maker_order_id: OrderId::new(1),
                        taker_order_id: OrderId::new(100),
                        price: p(100),
                        qty: Qty::try_new(2).unwrap(),
                        maker_side,
                    })
                );

                if should_stop {
                    assert_eq!(
                        events[1],
                        EngineEvent::MarketProtectionTriggered {
                            order_id: OrderId::new(100),
                            next_price: p(outside),
                            effective_worst_price: p(100),
                            source: ProtectionSource::Both,
                        }
                    );
                    assert_eq!(events.len(), 2);
                } else {
                    // Taker 恰好成交完，不能继续探测越界 maker。
                    assert_eq!(events.len(), 1);
                }

                assert_eq!(
                    snapshot(&book),
                    (
                        vec![(limit(2, maker_side, outside, 4), 4, QueuePriority::new(1),)],
                        2,
                    )
                );
            }
        }
    }
    #[test]
    fn protected_market_by_base_empty_and_immediate_stop() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)]
        {
            let mut book = ReferenceOrderBook::new();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(100)),
            };

            let before = snapshot(&book);

            assert_eq!(
                book.execute_market_by_base_with_protection(
                    market(100, side, 10),
                    &context,
                    &math,
                    &[],
                ),
                Ok(vec![])
            );

            assert_eq!(snapshot(&book), before);

            book.rest(limit(1, maker_side, outside, 5)).unwrap();

            let before = snapshot(&book);

            // 首个 maker 就越界，不需要任何 TradeId。
            let events = book
                .execute_market_by_base_with_protection(market(100, side, 10), &context, &math, &[])
                .unwrap();

            assert_eq!(
                events,
                vec![EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::MarketBand,
                }]
            );

            assert_eq!(snapshot(&book), before);
        }
    }
}

#[cfg(test)]
mod market_by_quote_preflight_tests {
    use super::*;
    use matching_domain::{MarketProtection, QuoteBudget, UserId};

    fn math(tick_size: i64, lot_size: u64) -> MarketMath {
        MarketMath::try_new(0, 0, 0, tick_size, lot_size).unwrap()
    }

    fn market(id: u128, side: Side, budget: u128) -> MarketByQuote {
        MarketByQuote {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            quote_budget: QuoteBudget::try_new(budget).unwrap(),
            market_protection: None,
        }
    }

    fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitGtcOrder, u64, QueuePriority)>, u128) {
        (
            book.orders
                .iter()
                .map(|maker| {
                    (
                        maker.original_order().clone(),
                        maker.remaining(),
                        maker.priority(),
                    )
                })
                .collect(),
            book.next_priority,
        )
    }

    #[test]
    fn market_by_quote_preflight_empty_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let book = ReferenceOrderBook::new();
            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote(&market(100, side, 1000), &math(1, 1))
                .unwrap();

            assert!(plan.fills.is_empty());
            assert_eq!(plan.required_trade_ids, 0);
            assert_eq!(plan.spent_quote.get(), 0);
            assert_eq!(plan.remaining_quote.get(), 1000);
            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn market_by_quote_preflight_price_fifo_both_sides() {
        for (taker_side, maker_side, worse, better) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(20, maker_side, worse, 4)).unwrap();
            book.rest(limit(30, maker_side, better, 3)).unwrap();
            book.rest(limit(10, maker_side, better, 5)).unwrap();

            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote(&market(100, taker_side, 2000), &math(1, 1))
                .unwrap();

            assert_eq!(plan.required_trade_ids, 3);

            assert_eq!(
                plan.fills
                    .iter()
                    .map(|fill| fill.maker_order_id)
                    .collect::<Vec<_>>(),
                vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
            );

            assert_eq!(
                plan.fills
                    .iter()
                    .map(|fill| fill.maker_price.get())
                    .collect::<Vec<_>>(),
                vec![better, better, worse]
            );

            assert_eq!(
                plan.fills
                    .iter()
                    .map(|fill| fill.qty.get())
                    .collect::<Vec<_>>(),
                vec![3, 5, 4]
            );

            let expected_spent = 8 * better as u128 + 4 * worse as u128;

            assert_eq!(plan.spent_quote.get(), expected_spent);
            assert_eq!(plan.remaining_quote.get(), 2000 - expected_spent);

            assert_eq!(
                plan.fills
                    .iter()
                    .map(|fill| fill.notional.get())
                    .sum::<u128>(),
                expected_spent
            );

            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn market_by_quote_preflight_partial_budget() {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, Side::Sell, 100, 10)).unwrap();

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote(&market(100, Side::Buy, 350), &math(1, 1))
            .unwrap();

        assert_eq!(plan.required_trade_ids, 1);
        assert_eq!(plan.fills[0].qty.get(), 3);
        assert_eq!(plan.fills[0].notional.get(), 300);
        assert_eq!(plan.spent_quote.get(), 300);
        assert_eq!(plan.remaining_quote.get(), 50);

        assert_eq!(snapshot(&book), before);
    }

    #[test]
    fn market_by_quote_preflight_dust_stops_both_sides() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 10)).unwrap();

            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote(&market(100, taker_side, 99), &math(1, 1))
                .unwrap();

            assert!(plan.fills.is_empty());
            assert_eq!(plan.required_trade_ids, 0);
            assert_eq!(plan.spent_quote.get(), 0);
            assert_eq!(plan.remaining_quote.get(), 99);

            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn market_by_quote_preflight_duplicate_first() {
        let mut book = ReferenceOrderBook::new();

        // 价格 6 不满足 tick_size=5。
        book.rest(limit(1, Side::Sell, 6, 10)).unwrap();

        let before = snapshot(&book);

        let result = book.preflight_market_by_quote(&market(1, Side::Buy, 1000), &math(5, 1));

        assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        assert_eq!(snapshot(&book), before);
    }

    #[test]
    fn market_by_quote_preflight_invalid_tick_is_atomic() {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, Side::Sell, 6, 10)).unwrap();

        let before = snapshot(&book);

        let result = book.preflight_market_by_quote(&market(100, Side::Buy, 1000), &math(5, 1));

        assert_eq!(result, Err(RejectReason::InvalidTick));
        assert_eq!(snapshot(&book), before);
    }

    #[test]
    fn market_by_quote_preflight_invalid_lot_is_atomic() {
        let mut book = ReferenceOrderBook::new();

        // maker remaining=11，不满足 lot_size=10。
        book.rest(limit(1, Side::Sell, 5, 11)).unwrap();

        let before = snapshot(&book);

        let result = book.preflight_market_by_quote(&market(100, Side::Buy, 1000), &math(1, 10));

        assert_eq!(result, Err(RejectReason::InvalidLot));
        assert_eq!(snapshot(&book), before);
    }

    #[test]
    fn protected_quote_preflight_multilevel_fifo_and_sources() {
        let p = |value| Price::try_new(value).unwrap();
        let math = math(10, 1);

        let cases = [
            // side, maker side, better, outside, farther, user, band,
            // spent, source
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                110,
                880,
                ProtectionSource::User,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                110,
                100,
                880,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                100,
                880,
                ProtectionSource::Both,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                90,
                920,
                ProtectionSource::User,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                90,
                100,
                920,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                100,
                920,
                ProtectionSource::Both,
            ),
        ];

        for (side, maker_side, better, outside, farther, user, band, spent, source) in cases {
            let mut book = ReferenceOrderBook::new();

            // 源 Vec 顺序刻意不等于业务撮合顺序。
            // 价位 100 上 ID=40 先于 ID=10。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let mut incoming = market(100, side, 2000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            });

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(band)),
            };

            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(&incoming, &context, &math)
                .unwrap();

            assert_eq!(
                plan.fills,
                vec![
                    QuoteFillPlan {
                        maker_order_id: OrderId::new(30),
                        maker_price: p(better),
                        qty: Qty::try_new(2).unwrap(),
                        notional: QuoteAmount::new((2 * better) as u128),
                    },
                    QuoteFillPlan {
                        maker_order_id: OrderId::new(40),
                        maker_price: p(100),
                        qty: Qty::try_new(4).unwrap(),
                        notional: QuoteAmount::new(400),
                    },
                    QuoteFillPlan {
                        maker_order_id: OrderId::new(10),
                        maker_price: p(100),
                        qty: Qty::try_new(3).unwrap(),
                        notional: QuoteAmount::new(300),
                    },
                ]
            );

            assert_eq!(plan.spent_quote, QuoteAmount::new(spent));
            assert_eq!(plan.unused_quote, QuoteAmount::new(2000 - spent));

            assert_eq!(
                plan.stop,
                Some(ProtectionBoundaryStop {
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source,
                })
            );

            // 包含 Vec 原顺序、订单内容、remaining、priority allocator。
            assert_eq!(snapshot(&book), before);
        }
    }
    #[test]
    fn protected_quote_preflight_empty_dust_and_unbounded() {
        let p = |value| Price::try_new(value).unwrap();
        let math = math(10, 1);

        for (side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100)),
            };

            // 1. 空簿。
            let book = ReferenceOrderBook::new();
            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(
                    &market(100, side, 1000),
                    &context,
                    &math,
                )
                .unwrap();

            assert!(plan.fills.is_empty());
            assert_eq!(plan.spent_quote, QuoteAmount::new(0));
            assert_eq!(plan.unused_quote, QuoteAmount::new(1000));
            assert_eq!(plan.stop, None);
            assert_eq!(snapshot(&book), before);

            // 2. 首档越界：即使预算 1 不够一 lot，也必须先 stop。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, outside, 5)).unwrap();
            book.rest(limit(2, maker_side, farther, 5)).unwrap();

            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(&market(100, side, 1), &context, &math)
                .unwrap();

            assert!(plan.fills.is_empty());
            assert_eq!(plan.spent_quote.get(), 0);
            assert_eq!(plan.unused_quote.get(), 1);
            assert_eq!(
                plan.stop,
                Some(ProtectionBoundaryStop {
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::MarketBand,
                })
            );
            assert_eq!(snapshot(&book), before);

            // 3. 第一张 maker 在保护范围内，但预算不足一 lot。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 5)).unwrap();
            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(&market(100, side, 99), &context, &math)
                .unwrap();

            assert!(plan.fills.is_empty());
            assert_eq!(plan.spent_quote.get(), 0);
            assert_eq!(plan.unused_quote.get(), 99);
            assert_eq!(plan.stop, None);
            assert_eq!(snapshot(&book), before);

            // 4. 已经成交一张 maker，下一档在保护边界内，但发生 dust。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, better, 1)).unwrap();
            book.rest(limit(2, maker_side, 100, 5)).unwrap();

            let budget = better as u128 + 5;
            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(
                    &market(100, side, budget),
                    &context,
                    &math,
                )
                .unwrap();

            assert_eq!(plan.fills.len(), 1);
            assert_eq!(plan.fills[0].maker_order_id, OrderId::new(1));
            assert_eq!(plan.fills[0].qty.get(), 1);
            assert_eq!(plan.spent_quote.get(), better as u128);
            assert_eq!(plan.unused_quote.get(), 5);
            assert_eq!(plan.stop, None);
            assert_eq!(snapshot(&book), before);

            // 5. 已成交一张 maker、预算只剩 5，
            // 但下一张 maker 越界，应当先记录保护 stop，而非 dust。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, better, 1)).unwrap();
            book.rest(limit(2, maker_side, outside, 5)).unwrap();
            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(
                    &market(100, side, budget),
                    &context,
                    &math,
                )
                .unwrap();

            assert_eq!(plan.fills.len(), 1);
            assert_eq!(plan.spent_quote.get(), better as u128);
            assert_eq!(plan.unused_quote.get(), 5);
            assert_eq!(
                plan.stop,
                Some(ProtectionBoundaryStop {
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::MarketBand,
                })
            );
            assert_eq!(snapshot(&book), before);

            // 6. 无用户保护、无 band：所有 maker 可按预算进入计划。
            let mut book = ReferenceOrderBook::new();

            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let no_boundary = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: None,
            };

            let before = snapshot(&book);

            let plan = book
                .preflight_market_by_quote_with_protection(
                    &market(100, side, 2000),
                    &no_boundary,
                    &math,
                )
                .unwrap();

            assert_eq!(
                plan.fills
                    .iter()
                    .map(|f| f.maker_order_id)
                    .collect::<Vec<_>>(),
                vec![
                    OrderId::new(30),
                    OrderId::new(40),
                    OrderId::new(10),
                    OrderId::new(50),
                    OrderId::new(90),
                ]
            );
            assert_eq!(plan.fills.len(), 5);
            assert_eq!(
                plan.spent_quote.get(),
                match side {
                    Side::Buy => 1230,
                    Side::Sell => 1170,
                }
            );
            assert_eq!(
                plan.unused_quote.get(),
                match side {
                    Side::Buy => 770,
                    Side::Sell => 830,
                }
            );
            assert_eq!(plan.stop, None);
            assert_eq!(snapshot(&book), before);
        }
    }
    #[test]
    fn protected_quote_preflight_rejections_are_atomic() {
        let p = |value| Price::try_new(value).unwrap();
        let math = math(10, 1);

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100_000)),
            };

            let invalid_bps = MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            };

            let before = snapshot(&book);

            // Duplicate 优先于非法 bps。
            let mut duplicate = market(1, side, 1000);
            duplicate.market_protection = Some(invalid_bps.clone());

            assert_eq!(
                book.preflight_market_by_quote_with_protection(&duplicate, &context, &math,),
                Err(RejectReason::DuplicateOrderId)
            );
            assert_eq!(snapshot(&book), before);

            // 不重复的订单：非法 bps 必须返回稳定 wire 3007。
            let mut invalid = market(100, side, 1000);
            invalid.market_protection = Some(invalid_bps);

            let result = book.preflight_market_by_quote_with_protection(&invalid, &context, &math);

            assert_eq!(result, Err(RejectReason::InvalidSlippageBps));
            assert_eq!(RejectReason::InvalidSlippageBps as u16, 3007);
            assert_eq!(snapshot(&book), before);

            // 显式 worst 未按 tick=10 对齐。
            let mut invalid = market(100, side, 1000);
            invalid.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100_003)),
            });

            assert_eq!(
                book.preflight_market_by_quote_with_protection(&invalid, &context, &math,),
                Err(RejectReason::InvalidTick)
            );
            assert_eq!(snapshot(&book), before);
        }

        // Buy bps 计算溢出：即使 band 有效也必须拒绝。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };

        let mut incoming = market(100, Side::Buy, 1000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });

        let before = snapshot(&book);

        assert_eq!(
            book.preflight_market_by_quote_with_protection(&incoming, &context, &math,),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(snapshot(&book), before);

        // 更重要的原子性：即使已只读规划一笔 fill，
        // 后续 maker 的数学校验失败，也绝不能改簿。
        for (side, maker_side, better, invalid_price) in [
            (Side::Buy, Side::Sell, 90, 101),
            (Side::Sell, Side::Buy, 110, 101),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, better, 1)).unwrap();
            book.rest(limit(2, maker_side, invalid_price, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: None,
            };

            let before = snapshot(&book);

            assert_eq!(
                book.preflight_market_by_quote_with_protection(
                    &market(100, side, 1000),
                    &context,
                    &math,
                ),
                Err(RejectReason::InvalidTick)
            );

            assert_eq!(snapshot(&book), before);
        }
    }
}

#[cfg(test)]
mod market_by_quote_execution_tests {
    use super::*;
    use matching_domain::market_order::ProtectedMarketByQuoteExecution;
    use matching_domain::{MarketProtection, QuoteBudget, UserId};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn math() -> MarketMath {
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
    }

    fn market(id: u128, side: Side, budget: u128) -> MarketByQuote {
        MarketByQuote {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            quote_budget: QuoteBudget::try_new(budget).unwrap(),
            market_protection: None,
        }
    }

    fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitGtcOrder, u64, QueuePriority)>, u128) {
        (
            book.orders
                .iter()
                .map(|maker| {
                    (
                        maker.original_order().clone(),
                        maker.remaining(),
                        maker.priority(),
                    )
                })
                .collect(),
            book.next_priority,
        )
    }

    #[test]
    fn market_by_quote_execution_empty_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut book = ReferenceOrderBook::new();
            let before = snapshot(&book);

            let result = book
                .execute_market_by_quote(market(100, side, 1000), &math(), &[])
                .unwrap();

            assert!(result.trades().is_empty());
            assert_eq!(result.spent_quote().get(), 0);
            assert_eq!(result.unused_quote().get(), 1000);
            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn market_by_quote_execution_partial_both_sides() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, 100, 10)).unwrap();
            book.rest(limit(2, maker_side, 100, 5)).unwrap();

            let next_before = book.next_priority;
            let priority_before = book.orders[0].priority();

            let result = book
                .execute_market_by_quote(
                    market(100, taker_side, 350),
                    &math(),
                    &[TradeId::new(500)],
                )
                .unwrap();

            assert_eq!(result.trades().len(), 1);

            let trade = &result.trades()[0];

            assert_eq!(trade.trade_id, TradeId::new(500));
            assert_eq!(trade.maker_order_id, OrderId::new(1));
            assert_eq!(trade.taker_order_id, OrderId::new(100));
            assert_eq!(trade.maker_side, maker_side);
            assert_eq!(trade.price.get(), 100);
            assert_eq!(trade.qty.get(), 3);

            assert_eq!(result.spent_quote().get(), 300);
            assert_eq!(result.unused_quote().get(), 50);

            // Partial maker 不删除、不移动。
            assert_eq!(book.orders.len(), 2);
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.orders[0].priority(), priority_before);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(1),);

            assert_eq!(book.orders[1].remaining(), 5);
            assert_eq!(book.next_priority, next_before);

            assert!(
                !book
                    .orders
                    .iter()
                    .any(|maker| { maker.original_order().order_id == OrderId::new(100) })
            );
        }
    }

    #[test]
    fn market_by_quote_execution_multi_level_fifo_both_sides() {
        for (taker_side, maker_side, worse, better) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();

            // ID 数值顺序与实际 FIFO 顺序不同。
            book.rest(limit(20, maker_side, worse, 4)).unwrap();
            book.rest(limit(30, maker_side, better, 3)).unwrap();
            book.rest(limit(10, maker_side, better, 5)).unwrap();

            let next_before = book.next_priority;

            let expected_spent = 8 * better as u128 + 4 * worse as u128;

            let result = book
                .execute_market_by_quote(
                    market(100, taker_side, 2000),
                    &math(),
                    &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
                )
                .unwrap();

            let trades = result.trades();

            assert_eq!(trades.len(), 3);

            assert_eq!(
                trades
                    .iter()
                    .map(|trade| trade.maker_order_id)
                    .collect::<Vec<_>>(),
                vec![OrderId::new(30), OrderId::new(10), OrderId::new(20),],
            );

            assert_eq!(
                trades
                    .iter()
                    .map(|trade| trade.price.get())
                    .collect::<Vec<_>>(),
                vec![better, better, worse],
            );

            assert_eq!(
                trades
                    .iter()
                    .map(|trade| trade.qty.get())
                    .collect::<Vec<_>>(),
                vec![3, 5, 4],
            );

            assert_eq!(
                trades
                    .iter()
                    .map(|trade| trade.trade_id)
                    .collect::<Vec<_>>(),
                vec![TradeId::new(501), TradeId::new(502), TradeId::new(503),],
            );

            for trade in trades {
                assert_eq!(trade.maker_side, maker_side);
                assert_eq!(trade.taker_order_id, OrderId::new(100));
            }

            assert_eq!(result.spent_quote().get(), expected_spent);
            assert_eq!(result.unused_quote().get(), 2000 - expected_spent,);

            // 所有 maker 已耗尽，市价 taker 不挂簿。
            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, next_before);
        }
    }

    #[test]
    fn market_by_quote_execution_dust_and_no_rest() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, 100, 10)).unwrap();

            let before = snapshot(&book);

            // 99 quote 不足成交 1 lot。
            let result = book
                .execute_market_by_quote(market(100, taker_side, 99), &math(), &[])
                .unwrap();

            assert!(result.trades().is_empty());
            assert_eq!(result.spent_quote().get(), 0);
            assert_eq!(result.unused_quote().get(), 99);
            assert_eq!(snapshot(&book), before);

            // 能先成交、之后产生 dust 的情况。
            let result = book
                .execute_market_by_quote(
                    market(101, taker_side, 350),
                    &math(),
                    &[TradeId::new(600)],
                )
                .unwrap();

            assert_eq!(result.trades().len(), 1);
            assert_eq!(result.trades()[0].qty.get(), 3);
            assert_eq!(result.spent_quote().get(), 300);
            assert_eq!(result.unused_quote().get(), 50);

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.next_priority, before.1);

            assert!(
                !book
                    .orders
                    .iter()
                    .any(|maker| { matches!(maker.original_order().order_id.get(), 100 | 101) })
            );
        }
    }

    #[test]
    fn market_by_quote_execution_duplicate_is_atomic() {
        for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, 100, 10)).unwrap();

            let before = snapshot(&book);

            // TradeId 为空，但 duplicate 必须优先于足量检查。
            let result = book.execute_market_by_quote(market(1, taker_side, 1000), &math(), &[]);

            assert_eq!(result, Err(RejectReason::DuplicateOrderId));
            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn market_by_quote_execution_insufficient_trade_ids_is_atomic() {
        for (taker_side, maker_side, worse, better) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(30, maker_side, better, 3)).unwrap();
            book.rest(limit(10, maker_side, worse, 5)).unwrap();

            let before = snapshot(&book);

            // 两张 maker 均可成交，需要两个 TradeId。
            // 仅提供一个，必须在第一笔 mutation 前 panic。
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = book.execute_market_by_quote(
                    market(100, taker_side, 2000),
                    &math(),
                    &[TradeId::new(700)],
                );
            }));

            assert!(result.is_err());

            // 包含原始订单、剩余量、priority、源 Vec 顺序及计数器。
            assert_eq!(snapshot(&book), before);
        }
    }

    #[test]
    fn protected_quote_execution_multilevel_sources() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let cases = [
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                110,
                ProtectionSource::User,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                110,
                100,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                100,
                ProtectionSource::Both,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                90,
                ProtectionSource::User,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                90,
                100,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                100,
                ProtectionSource::Both,
            ),
        ];

        for (side, maker_side, better, outside, farther, user, band, source) in cases {
            let mut book = ReferenceOrderBook::new();

            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let mut incoming = market(100, side, 2000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            });

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(band)),
            };

            let priority_before = book.next_priority;

            let actual = book
                .execute_market_by_quote_with_protection(
                    incoming,
                    &context,
                    &math,
                    &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
                )
                .unwrap();

            let trade = |tid, maker_id, price, qty| {
                EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(tid),
                    maker_order_id: OrderId::new(maker_id),
                    taker_order_id: OrderId::new(100),
                    price: p(price),
                    qty: Qty::try_new(qty).unwrap(),
                    maker_side,
                })
            };

            let expected_spent = if side == Side::Buy { 880 } else { 920 };

            let expected = ProtectedMarketByQuoteExecution::new(
                vec![
                    trade(701, 30, better, 2),
                    trade(702, 40, 100, 4),
                    trade(703, 10, 100, 3),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source,
                    },
                ],
                QuoteAmount::new(expected_spent),
                QuoteAmount::new(2000 - expected_spent),
            );

            assert_eq!(actual, expected);

            // 已成交 maker 删除，越界与更远 maker 完整保留。
            assert_eq!(
                snapshot(&book),
                (
                    vec![
                        (limit(90, maker_side, farther, 2), 2, QueuePriority::new(0)),
                        (limit(50, maker_side, outside, 1), 1, QueuePriority::new(4)),
                    ],
                    priority_before,
                )
            );

            assert!(
                !book
                    .orders
                    .iter()
                    .any(|maker| maker.original_order().order_id == OrderId::new(100))
            );
        }
    }
    #[test]
    fn protected_quote_execution_boundary_dust_and_immediate_stop() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)]
        {
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100)),
            };

            let execute = |book: &mut ReferenceOrderBook, budget, ids: &[TradeId]| {
                book.execute_market_by_quote_with_protection(
                    market(100, side, budget),
                    &context,
                    &math,
                    ids,
                )
                .unwrap()
            };

            // 1. 空簿：事件为空、全部预算 unused。
            let mut book = ReferenceOrderBook::new();
            let before = snapshot(&book);
            let actual = execute(&mut book, 1000, &[]);

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![],
                    QuoteAmount::new(0),
                    QuoteAmount::new(1000),
                )
            );
            assert_eq!(snapshot(&book), before);

            // 2. 首档越界，且预算不足一 lot：
            // 必须先触发保护，而不是按 dust 停止。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, outside, 5)).unwrap();
            let before = snapshot(&book);

            let actual = execute(&mut book, 1, &[]);

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source: ProtectionSource::MarketBand,
                    }],
                    QuoteAmount::new(0),
                    QuoteAmount::new(1),
                )
            );
            assert_eq!(snapshot(&book), before);

            // 3. maker 正好在边界上；预算完全耗尽，
            // 后面即使越界，也不能产生 trigger。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 2)).unwrap();
            book.rest(limit(2, maker_side, outside, 4)).unwrap();

            let actual = execute(&mut book, 200, &[TradeId::new(700)]);

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![EngineEvent::Trade(TradeEvent {
                        trade_id: TradeId::new(700),
                        maker_order_id: OrderId::new(1),
                        taker_order_id: OrderId::new(100),
                        price: p(100),
                        qty: Qty::try_new(2).unwrap(),
                        maker_side,
                    })],
                    QuoteAmount::new(200),
                    QuoteAmount::new(0),
                )
            );

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(2));

            // 4. 部分 maker 成交：remaining=7，
            // 原 priority 保留；剩余 50 形成 dust。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 10)).unwrap();
            let priority_before = book.orders[0].priority();

            let actual = execute(&mut book, 350, &[TradeId::new(701)]);

            assert_eq!(actual.events().len(), 1);
            assert!(matches!(
                &actual.events()[0],
                EngineEvent::Trade(trade)
                    if trade.maker_order_id == OrderId::new(1)
                        && trade.qty == Qty::try_new(3).unwrap()
            ));
            assert_eq!(actual.spent_quote(), QuoteAmount::new(300));
            assert_eq!(actual.unused_quote(), QuoteAmount::new(50));
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.orders[0].priority(), priority_before);
            assert_eq!(book.next_priority, 1);

            // 5. 首档在边界内，但初始预算不足一 lot。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 5)).unwrap();
            let before = snapshot(&book);

            let actual = execute(&mut book, 99, &[]);

            assert_eq!(actual.events(), &[]);
            assert_eq!(actual.spent_quote(), QuoteAmount::new(0));
            assert_eq!(actual.unused_quote(), QuoteAmount::new(99));
            assert_eq!(snapshot(&book), before);

            // 6. 成交价 100 用掉 200，还剩 5。
            // 下一 maker 越界：优先触发保护，而不是 dust。
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 2)).unwrap();
            book.rest(limit(2, maker_side, outside, 4)).unwrap();

            let actual = execute(&mut book, 205, &[TradeId::new(702)]);

            assert_eq!(actual.events().len(), 2);
            assert!(matches!(actual.events()[0], EngineEvent::Trade(_)));
            assert_eq!(
                actual.events()[1],
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::MarketBand,
                }
            );
            assert_eq!(actual.spent_quote(), QuoteAmount::new(200));
            assert_eq!(actual.unused_quote(), QuoteAmount::new(5));
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(2));
        }
    }
    #[test]
    fn protected_quote_execution_without_boundary_scans_all() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better, outside, farther, spent) in [
            (Side::Buy, Side::Sell, 90, 110, 120, 1230),
            (Side::Sell, Side::Buy, 110, 90, 80, 1170),
        ] {
            let mut book = ReferenceOrderBook::new();

            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(limit(id, maker_side, price, qty)).unwrap();
            }

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: None,
            };

            let before_priority = book.next_priority;

            let trade_ids = [
                TradeId::new(701),
                TradeId::new(702),
                TradeId::new(703),
                TradeId::new(704),
                TradeId::new(705),
            ];

            let actual = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 2000),
                    &context,
                    &math,
                    &trade_ids,
                )
                .unwrap();

            let expected_makers = [
                (30, better, 2),
                (40, 100, 4),
                (10, 100, 3),
                (50, outside, 1),
                (90, farther, 2),
            ];

            let expected_events = expected_makers
                .iter()
                .enumerate()
                .map(|(index, &(maker_id, price, qty))| {
                    EngineEvent::Trade(TradeEvent {
                        trade_id: trade_ids[index],
                        maker_order_id: OrderId::new(maker_id),
                        taker_order_id: OrderId::new(100),
                        price: p(price),
                        qty: Qty::try_new(qty).unwrap(),
                        maker_side,
                    })
                })
                .collect();

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    expected_events,
                    QuoteAmount::new(spent),
                    QuoteAmount::new(2000 - spent),
                )
            );

            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, before_priority);
        }
    }
    fn assert_protected_quote_rejected_atomically(
        book: &mut ReferenceOrderBook,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        expected: RejectReason,
    ) {
        let before = snapshot(book);

        assert_eq!(
            book.execute_market_by_quote_with_protection(incoming, context, math, &[],),
            Err(expected)
        );

        assert_eq!(snapshot(book), before);
    }
    #[test]
    fn protected_quote_execution_rejections_are_atomic() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100_000)),
            };

            let invalid_bps = MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            };

            // Duplicate 优先于非法 bps。
            let mut incoming = market(1, side, 1000);
            incoming.market_protection = Some(invalid_bps.clone());

            assert_protected_quote_rejected_atomically(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::DuplicateOrderId,
            );

            // 非重复订单：3007。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(invalid_bps);

            assert_protected_quote_rejected_atomically(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::InvalidSlippageBps,
            );

            // 显式 worst_price 未按 tick=10 对齐。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100_003)),
            });

            assert_protected_quote_rejected_atomically(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::InvalidTick,
            );
        }

        // 命令开始时计算 Buy 滑点边界溢出。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };

        let mut incoming = market(100, Side::Buy, 1000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });

        assert_protected_quote_rejected_atomically(
            &mut book,
            incoming,
            &context,
            &math,
            RejectReason::ArithmeticOverflow,
        );

        // 数学错误发生在已经规划出前序 fill 之后：
        // 仍然不得提交任何 maker mutation。
        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, better, 1)).unwrap();
            book.rest(limit(2, maker_side, 101, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: None,
            };

            assert_protected_quote_rejected_atomically(
                &mut book,
                market(100, side, 1000),
                &context,
                &math,
                RejectReason::InvalidTick,
            );
        }
    }

    #[test]
    fn protected_quote_execution_invalid_lot_is_atomic() {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, Side::Sell, 5, 11)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100).unwrap(),
            market_band_worst: None,
        };

        let math = MarketMath::try_new(0, 0, 0, 1, 10).unwrap();

        assert_protected_quote_rejected_atomically(
            &mut book,
            market(100, Side::Buy, 1000),
            &context,
            &math,
            RejectReason::InvalidLot,
        );
    }

    #[test]
    fn protected_quote_execution_insufficient_ids_is_atomic() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ReferenceOrderBook::new();

            book.rest(limit(1, maker_side, better, 2)).unwrap();
            book.rest(limit(2, maker_side, 100, 3)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100)),
            };

            let before = snapshot(&book);

            // 需要两个 TradeId，只提供一个。
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = book.execute_market_by_quote_with_protection(
                    market(100, side, 1000),
                    &context,
                    &math,
                    &[TradeId::new(700)],
                );
            }));

            assert!(result.is_err());
            assert_eq!(snapshot(&book), before);
        }
    }
}
