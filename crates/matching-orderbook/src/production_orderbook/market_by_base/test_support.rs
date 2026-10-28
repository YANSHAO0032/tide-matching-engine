//! 生产市价保护预检的测试投影。
//!
//! 辅助入口只暴露测试所需的成交计划，不参与真实市价单执行。

use super::super::ProductionOrderBook;
use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{MarketByBase, MarketMath, OrderId, Qty, RejectReason};

impl ProductionOrderBook {
    /// 测试读取受保护预检结果，保留生产计划中的价格优先与先进先出顺序。
    pub(crate) fn preflight_market_by_base_with_protection(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<Vec<(OrderId, Qty)>, RejectReason> {
        let plan = self.preflight_protected_market_by_base_plan(incoming, context, math)?;

        Ok(plan
            .fills
            .into_iter()
            .map(|fill| (fill.maker_order_id, fill.qty))
            .collect())
    }
}
