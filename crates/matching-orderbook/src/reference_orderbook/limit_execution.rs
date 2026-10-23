//! 基于独立 Vec 模型实现限价订单的执行策略。

use super::ReferenceOrderBook;
use matching_domain::{
    LimitGtcOrder, LimitImmediateExecution, MarketMath, MathError, Qty, RejectReason, Side,
    TradeEvent, TradeId,
};

/// 全部计划挂单扣减完成前，原始 Vec 中的位置保持稳定。
struct LimitFillPlan {
    /// 原始 Vec 中待扣减订单的位置及其成交数量。
    fills: Vec<(usize, Qty)>,
    /// 计划结束后吃单方的基础资产余量。
    remaining: u64,
}

fn reject_limit_math_error(error: MathError) -> RejectReason {
    error.to_reject_reason().unwrap_or_else(|| {
        panic!("ReferenceOrderBook invariant violation: non-rejectable limit math error: {error:?}")
    })
}

impl ReferenceOrderBook {
    /// PostOnly 候选的只读预检。
    ///
    /// 输入代表限价单 GTC + PostOnly 候选。
    ///
    /// 检查顺序：
    /// 1. 全簿重复 OrderId
    /// 2. 是否与对手盘交叉
    ///
    /// Ok(()) 仅表示通过当前两项预检，
    /// 不表示订单已经接受或成功挂簿。
    ///
    /// 不修改订单簿，不检查或分配 QueuePriority。
    pub(super) fn preflight_post_only(&self, order: &LimitGtcOrder) -> Result<(), RejectReason> {
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

    /// 通过重复标识符与包含等价的交叉检查后，挂入限价 GTC PostOnly 订单。
    ///
    /// 不尝试主动成交；只有成功挂单才分配队列优先级；
    /// 拒绝时保持完整订单簿和分配器状态不变。
    /// 完成重复与交叉检查后，在挂单前检查优先级及当前价档数量。
    /// 与既有挂单接口相同，调用方负责校验市场价格步长和数量步长对齐。
    pub fn place_limit_post_only(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        self.preflight_post_only(&order)?;
        if self.next_priority >= u128::MAX - super::PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        // Vec 模型不缓存价档总量；挂单前根据当前剩余量，
        // 独立检查相同的 u64 聚合边界契约。
        self.orders
            .iter()
            .filter(|maker| {
                maker.original_order().side == order.side
                    && maker.original_order().price == order.price
            })
            .try_fold(order.qty.get(), |total, maker| {
                total.checked_add(maker.remaining())
            })
            .ok_or(RejectReason::ArithmeticOverflow)?;
        self.rest(order)
    }

    /// 执行限价 IOC；正余量直接取消，不进入订单簿。
    ///
    /// 既有 `LimitGtcOrder` 值提供限价字段；本入口选择 IOC 策略。
    /// 首笔改簿前完成所有可执行成交及其本金的检查，
    /// 调用方提供的成交标识符必须覆盖计划中的全部成交笔数；
    /// 不足时与市价接口一样，在改簿前触发调用方契约断言。
    pub fn place_limit_ioc(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        if self.has_iceberg_maker(order.side) {
            return self.iceberg_immediate(order, math, trade_ids, false);
        }
        let plan = self.preflight_limit_immediate(&order, math)?;
        Ok(self.commit_limit_immediate(order, plan, trade_ids))
    }

    /// 仅当只读计划能成交全部数量时，执行限价 FOK。
    ///
    /// 既有限价载荷在此不决定 GTC 策略；可执行流动性不足时，
    /// 在检查成交标识符或改簿前直接拒绝。
    /// 新到 FOK 订单不分配优先级；冰山挂单成功补量会取得新优先级。
    /// 拒绝时保持整个分配器状态不变。
    ///
    /// # 不变量断言
    ///
    /// 确认流动性足够后，若 `TradeId` 不足，则在改簿前触发断言。
    /// 成交标识符的全局唯一性仍由调用方保证。
    pub fn place_limit_fok(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        if self.has_iceberg_maker(order.side) {
            return self.iceberg_immediate(order, math, trade_ids, true);
        }
        let plan = self.preflight_limit_immediate(&order, math)?;
        if plan.remaining != 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        Ok(self.commit_limit_immediate(order, plan, trade_ids))
    }

    /// 独立扫描按价格稳定排序的 Vec 位置，不使用生产簿的计划或
    /// 执行过程来生成参考结果。
    fn preflight_limit_immediate(
        &self,
        order: &LimitGtcOrder,
        math: &MarketMath,
    ) -> Result<LimitFillPlan, RejectReason> {
        if self
            .orders
            .iter()
            .any(|maker| maker.original_order().order_id == order.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }
        // 即使订单簿为空或流动性不足，也校验完整提交订单。
        math.trade_notional(order.price, order.qty)
            .map_err(reject_limit_math_error)?;

        let mut candidates: Vec<_> = self
            .orders
            .iter()
            .enumerate()
            .filter(|(_, maker)| {
                maker.original_order().side != order.side && maker.remaining() != 0
            })
            .map(|(index, _)| index)
            .collect();
        // 稳定排序保留同价订单在原 Vec 中的先进先出顺序。
        candidates.sort_by(|&left, &right| {
            let left = self.orders[left].original_order().price;
            let right = self.orders[right].original_order().price;
            match order.side {
                Side::Buy => left.cmp(&right),
                Side::Sell => right.cmp(&left),
            }
        });

        let mut remaining = order.qty.get();
        let mut notional = 0u128;
        let mut fills = Vec::new();
        for index in candidates {
            if remaining == 0 {
                break;
            }
            let maker = &self.orders[index];
            let price = maker.original_order().price;
            let crosses = match order.side {
                Side::Buy => price <= order.price,
                Side::Sell => price >= order.price,
            };
            if !crosses {
                break;
            }
            let qty = Qty::try_new(remaining.min(maker.remaining()))?;
            let fill_notional = math
                .trade_notional(price, qty)
                .map_err(reject_limit_math_error)?;
            notional = notional
                .checked_add(fill_notional.get())
                .ok_or(RejectReason::ArithmeticOverflow)?;
            maker
                .remaining()
                .checked_sub(qty.get())
                .ok_or(RejectReason::ArithmeticOverflow)?;
            remaining = remaining
                .checked_sub(qty.get())
                .ok_or(RejectReason::ArithmeticOverflow)?;
            fills.push((index, qty));
        }
        Ok(LimitFillPlan { fills, remaining })
    }

    /// 提交完整且已预检的计划；全部计划成交应用完毕前，
    /// 不移除原始位置。首笔写入后不再返回可恢复错误，
    /// 新到订单的余量也不会进入挂单 Vec。
    fn commit_limit_immediate(
        &mut self,
        order: LimitGtcOrder,
        plan: LimitFillPlan,
        trade_ids: &[TradeId],
    ) -> LimitImmediateExecution {
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for preflighted limit fills"
        );
        let canceled_remaining = if plan.remaining == 0 {
            None
        } else {
            Some(Qty::try_new(plan.remaining).expect("preflighted positive limit remainder"))
        };
        let mut trades = Vec::with_capacity(plan.fills.len());
        for ((index, qty), trade_id) in plan.fills.into_iter().zip(trade_ids.iter().copied()) {
            let maker = &mut self.orders[index];
            trades.push(TradeEvent {
                trade_id,
                maker_order_id: maker.original_order().order_id,
                taker_order_id: order.order_id,
                price: maker.original_order().price,
                qty,
                maker_side: maker.original_order().side,
            });
            maker
                .apply_fill(qty)
                .expect("preflighted limit maker deduction must succeed");
        }
        self.orders.retain(|maker| maker.remaining() != 0);
        LimitImmediateExecution::new(order.order_id, trades, canceled_remaining)
    }
}
