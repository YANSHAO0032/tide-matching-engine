//! 基础币数量市价订单的只读预检、保护终止和成交提交。

use super::ReferenceOrderBook;
use crate::market_plan::ProtectionBoundaryStop;
use matching_domain::market_order::{
    CommandStartProtectionContext, effective_protection_at_command_start_with_source,
    maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, MarketByBase, MarketMath, OrderId, Qty, RejectReason, Side, TradeEvent, TradeId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProtectedMarketByBasePlan {
    // 保留此前已验收的挂单标识符 + Qty 成交计划。
    fills: Vec<(OrderId, Qty)>,

    // 至多一个终止点，即价格优先顺序中的首个越界挂单。
    stop: Option<ProtectionBoundaryStop>,
}

impl ReferenceOrderBook {
    /// 只读预检 MarketByBase。
    ///
    /// 返回完成本次撮合需要的 TradeId 数量。
    /// 不修改订单、剩余量或 next_priority。
    fn preflight_market_by_base(&self, incoming: &MarketByBase) -> Result<usize, RejectReason> {
        // 1. 活动订单标识符重复必须优先拒绝。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 市价单只读取对手盘。
        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut remaining = incoming.qty.get();
        let mut required_trade_ids = 0usize;

        // `orders(side)` 已保证：
        // 卖出: 价格升序 + 同价先进先出
        // 买入: 价格降序 + 同价先进先出
        for maker in self.orders(maker_side) {
            if remaining == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            // 防御测试数据中可能残留的耗尽订单。
            if maker_remaining == 0 {
                continue;
            }

            let fill = remaining.min(maker_remaining);

            // 成交必须严格大于零。
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
    /// 返回按价格优先、同价先进先出排列的挂单标识符与计划数量。
    /// 遇到首个保护价之外的挂单立即停止。
    ///
    /// 不修改盘口、不消耗 TradeId、不分配 QueuePriority。
    #[cfg(test)]
    pub(super) fn preflight_market_by_base_with_protection(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<Vec<(OrderId, Qty)>, RejectReason> {
        Ok(self
            .preflight_protected_market_by_base_plan(incoming, context, math)?
            .fills)
    }

    /// 市价按基础币数量立即成交。
    ///
    /// - 活动重复标识符在任何状态变更前拒绝。
    /// - TradeId 不足在任何状态变更前触发不变量断言（测试/参考簿故障时停止处理）。
    /// - 挂单按价格优先、同价先进先出选择。
    /// - 市价吃单方剩余量不挂簿。
    /// - 不分配 QueuePriority。
    pub fn execute_market_by_base(
        &mut self,
        incoming: MarketByBase,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            return self
                .slice_market_base(incoming, None, trade_ids)
                .map(|events| {
                    events
                        .into_iter()
                        .filter_map(|event| {
                            if let EngineEvent::Trade(t) = event {
                                Some(t)
                            } else {
                                None
                            }
                        })
                        .collect()
                });
        }
        // 1. 只读预检。
        let required = self.preflight_market_by_base(&incoming)?;

        // 2. 必须先验证 TradeId 足够，再允许任何状态变更。
        // 与现有差分参考簿适配器的失败即停止的断言约定一致。
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
            // 按当前盘口重新取得最佳挂单。
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

            // 4. 扣减挂单。
            self.orders[maker_index]
                .apply_fill(fill_qty)
                .expect("preflighted MarketByBase maker fill must succeed");

            remaining = remaining
                .checked_sub(fill_lots)
                .expect("preflighted MarketByBase remaining underflow");

            // 5. 耗尽挂单直接删除，部分挂单保留。
            if self.orders[maker_index].remaining() == 0 {
                self.orders.remove(maker_index);
            }

            // 6. TradeEvent 必须使用真实挂单成交价格。
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

        // 剩余量即使大于 0，也直接终结，不执行挂单。
        Ok(trades)
    }

    /// 参考簿专用：按命令开始时固定的保护边界执行 MarketByBase。
    ///
    /// 先完整预检，再提交成交；保护终止不消耗 TradeId。
    /// 不自动挂单吃单方；冰山挂单补量分配新优先级。
    pub fn execute_market_by_base_with_protection(
        &mut self,
        incoming: MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            return self.slice_market_base(incoming, Some((context, math)), trade_ids);
        }
        // 阶段一：完整只读计划。
        let plan = self.preflight_protected_market_by_base_plan(&incoming, context, math)?;

        // TradeId 只为 Trade 分配，不为 ProtectionTriggered 分配。
        // 与现有参考簿 MarketByBase 保持一致：
        // 数量不足时，在任何状态变更前触发不变量断言并停止处理。
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
            // 按预检时记录的挂单标识符定位。
            // 不重新选择最佳挂单，也不重新判断保护边界。
            let maker_index = self
                .orders
                .iter()
                .position(|maker| maker.original_order().order_id == maker_order_id)
                .expect("preflighted maker disappeared");

            let maker = &mut self.orders[maker_index];

            let maker_price = maker.original_order().price;

            // 预检已验证计划数量不超过 `maker.remaining`。
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

        // 不将未成交的吃单方余量挂入参考簿。
        Ok(events)
    }

    fn preflight_protected_market_by_base_plan(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByBasePlan, RejectReason> {
        // 1. 重复标识符优先于保护计算。
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // 2. 使用带来源的命令开始保护辅助函数。
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

        // 3. 只读扫描，复用参考簿的价格/先进先出排序。
        for maker in self.orders(maker_side) {
            if remaining == 0 {
                break;
            }

            let maker_remaining = maker.remaining();

            if maker_remaining == 0 {
                continue;
            }

            let maker_price = maker.original_order().price;

            // 4. 判断当前挂单是否位于有效保护边界内。
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

                // 首个越界挂单不计入成交计划。
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
