//! 参考市价保护预检的测试投影。
//!
//! 辅助入口读取同一份只读计划，以便与生产实现逐项比较。

use super::super::ReferenceOrderBook;
use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{MarketByBase, MarketMath, OrderId, Qty, RejectReason};

impl ReferenceOrderBook {
    /// 测试读取受保护预检结果，保留参考模型的成交计划原序。
    pub(crate) fn preflight_market_by_base_with_protection(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<Vec<(OrderId, Qty)>, RejectReason> {
        Ok(self
            .preflight_protected_market_by_base_plan(incoming, context, math)?
            .fills)
    }
}
