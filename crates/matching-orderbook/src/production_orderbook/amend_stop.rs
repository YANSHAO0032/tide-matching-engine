//! 生产簿未触发止损改单的候选载荷与预检提交边界。

use super::{ProductionOrderBook, amend_candidate::ProductionAmendPlan};
use matching_domain::{
    AmendOrder, AmendedQuantity, EngineEvent, LogicalTime, QuoteBudget, RejectReason,
    StopMarketQuantity, StopOrder,
};

impl ProductionOrderBook {
    /// 原子修改未触发 Stop 的 child、触发价和触发前到期条件。
    ///
    /// child 字段修改保留 StopPriority；改变触发价或来源才迁移队尾并分配新值。
    /// 本入口不分配 QueuePriority、不成交。触发源决定所属来源档位，市价保护随 child 模板保存。
    /// 本低层簿接口要求调用方已经核对资金覆盖；缺少预留状态或携带未验证令牌均拒绝。
    /// 资金核验命令使用 `amend_with_context`，只有簿提交成功后才安装已验证的预留目标。
    pub fn amend_stop_with_events(
        &mut self,
        amend: &AmendOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let candidate = match self.build_amend_plan(amend, now, current_session_id)? {
            ProductionAmendPlan::Stop(plan) => plan,
            _ => return Err(RejectReason::InvalidOrderCombination),
        };
        if amend.reservation_adjustment.is_some() {
            return Err(RejectReason::InvalidReservation);
        }
        let (old, _) = self
            .stop_book
            .get(candidate.order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        let mut replacement = old.clone();
        let (old_price, new_price, old_quantity, new_quantity) = match &mut replacement {
            StopOrder::Limit(child) => {
                let old_price = child.price;
                let old_qty = child.qty;
                child.price = amend.new_price.unwrap_or(child.price);
                child.qty = amend.new_qty.unwrap_or(child.qty);
                child.stop_price = candidate.new_stop_price;
                child.trigger_reference = candidate.new_reference;
                child.child_tif = amend.new_tif.unwrap_or(child.child_tif);
                child.trigger_expiry = candidate.target_expiry;
                (
                    Some(old_price),
                    Some(child.price),
                    AmendedQuantity::BaseRemaining(old_qty),
                    AmendedQuantity::BaseRemaining(child.qty),
                )
            }
            StopOrder::Market(child) => {
                if amend.new_price.is_some() {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                let (old_qty, new_qty) = match &mut child.quantity {
                    StopMarketQuantity::Base(qty) => {
                        let before = *qty;
                        *qty = amend.new_qty.unwrap_or(*qty);
                        (
                            AmendedQuantity::BaseRemaining(before),
                            AmendedQuantity::BaseRemaining(*qty),
                        )
                    }
                    StopMarketQuantity::Quote(budget) => {
                        let before = matching_domain::QuoteAmount::new(budget.get());
                        if let Some(amount) = amend.new_quote_budget {
                            *budget = QuoteBudget::try_new(amount.get())?;
                        }
                        (
                            AmendedQuantity::QuoteBudget(before),
                            AmendedQuantity::QuoteBudget(matching_domain::QuoteAmount::new(
                                budget.get(),
                            )),
                        )
                    }
                };
                child.market_protection = candidate.new_protection.clone();
                child.stop_price = candidate.new_stop_price;
                child.trigger_reference = candidate.new_reference;
                child.child_tif = amend.new_tif.unwrap_or(child.child_tif);
                child.trigger_expiry = candidate.target_expiry;
                (None, None, old_qty, new_qty)
            }
        };
        let retained = candidate.old_stop_price == candidate.new_stop_price
            && candidate.old_reference == candidate.new_reference;
        let plan = self.stop_book.plan_replacement(
            replacement,
            now,
            current_session_id,
            &self.stop_priority_allocator,
        )?;
        let events = vec![EngineEvent::OrderAmended {
            order_id: candidate.order_id,
            old_price,
            new_price,
            old_quantity,
            new_quantity,
            priority_retained: retained,
        }];
        // 写屏障：业务拒绝、时效和优先级校验均已完成。
        self.stop_book
            .commit_replacement(plan, &mut self.stop_priority_allocator);
        Ok(events)
    }
}
