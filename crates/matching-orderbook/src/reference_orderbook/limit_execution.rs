//! Independent Vec-based execution policies for limit orders.

use super::ReferenceOrderBook;
use matching_domain::{
    LimitGtcOrder, LimitImmediateExecution, MarketMath, MathError, Qty, RejectReason, Side,
    TradeEvent, TradeId,
};

/// Source Vec positions remain stable until all planned maker deductions finish.
struct LimitFillPlan {
    fills: Vec<(usize, Qty)>,
    remaining: u64,
}

fn reject_limit_math_error(error: MathError) -> RejectReason {
    error.to_reject_reason().unwrap_or_else(|| {
        panic!("ReferenceOrderBook invariant violation: non-rejectable limit math error: {error:?}")
    })
}

impl ReferenceOrderBook {
    /// Rest a Limit GTC PostOnly candidate after duplicate and inclusive crossing checks.
    ///
    /// No taker fill is attempted. Only a successful rest allocates queue priority;
    /// a rejection preserves the complete book and allocator state.
    /// After duplicate/crossing, check priority and live level quantity before rest.
    /// The caller must validate market tick/lot alignment, as for the existing rest API.
    pub fn place_limit_post_only(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        self.preflight_post_only(&order)?;
        if self.next_priority >= u128::MAX - super::PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        // The Vec model has no cached PriceLevel total. Independently check the
        // same u64 aggregate contract using live remaining quantities before rest.
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

    /// Execute a limit IOC, canceling any positive remainder without resting it.
    ///
    /// The legacy LimitGtcOrder value supplies the limit fields; this entry point
    /// selects IOC. All executable fills and their checked notionals are validated
    /// before the first mutation. Trade IDs must cover the full planned fill count;
    /// shortage is a caller invariant panic before mutation, as in the Market APIs.
    pub fn place_limit_ioc(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        let plan = self.preflight_limit_immediate(&order, math)?;
        Ok(self.commit_limit_immediate(order, plan, trade_ids))
    }

    /// Execute a limit FOK only when the read-only plan fills the entire quantity.
    ///
    /// The legacy limit payload does not select GTC here. Insufficient executable
    /// liquidity rejects before inspecting supplied Trade IDs or changing the book.
    /// Both successful execution and rejection leave queue priority unchanged.
    ///
    /// # Panics
    ///
    /// Insufficient TradeIds panic before mutation once full liquidity is confirmed.
    /// Their global uniqueness remains the caller's responsibility.
    pub fn place_limit_fok(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<LimitImmediateExecution, RejectReason> {
        let plan = self.preflight_limit_immediate(&order, math)?;
        if plan.remaining != 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        Ok(self.commit_limit_immediate(order, plan, trade_ids))
    }

    /// Independently scan stable price-sorted Vec positions; no production planner
    /// or execution routine is used to derive the reference result.
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
        // Validate the full submitted order, even with an empty or shallow book.
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
        // Stable sorting preserves source Vec FIFO among equal prices.
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

    /// Commit the complete prevalidated plan without removing source positions
    /// until every planned fill has been applied. No recoverable error follows
    /// the first write, and incoming remainder never enters the resting Vec.
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

#[cfg(test)]
mod tests;
