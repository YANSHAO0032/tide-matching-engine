//! 参考簿与生产簿各自预检后共用的只读市价计划数据。
//!
//! 这些类型只保存已验证的计划结果；它们不访问订单簿容器、不决定挂单
//! 扫描顺序，也不提交状态变更。两种订单簿仍独立执行预检和提交，以保留
//! 差分验证所需的实现独立性。

use matching_domain::{OrderId, Price, ProtectionSource, Qty, QuoteAmount};

/// 单笔按报价资产预算执行的挂单成交计划，不代表已经提交的成交。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuoteFillPlan {
    pub(crate) maker_order_id: OrderId,
    pub(crate) maker_price: Price,
    pub(crate) qty: Qty,
    pub(crate) notional: QuoteAmount,
}

/// 完整的无保护 MarketByQuote 只读计划。
///
/// `fills` 的顺序就是未来成交事件的顺序；`required_trade_ids` 是提交前
/// TradeId 容量预检的显式契约，即使目前它与成交数量相同也不省略。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarketByQuotePlan {
    pub(crate) fills: Vec<QuoteFillPlan>,
    pub(crate) required_trade_ids: usize,
    pub(crate) spent_quote: QuoteAmount,
    pub(crate) remaining_quote: QuoteAmount,
}

/// 保护 MarketByQuote 的只读计划。
///
/// `stop` 只表示按保护边界终止，不表示报价资产不足一手的预算或订单簿耗尽。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtectedMarketByQuotePlan {
    pub(crate) fills: Vec<QuoteFillPlan>,
    pub(crate) spent_quote: QuoteAmount,
    pub(crate) unused_quote: QuoteAmount,
    pub(crate) stop: Option<ProtectionBoundaryStop>,
}

/// 价格优先顺序中首个因保护边界不可成交的挂单。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProtectionBoundaryStop {
    pub(crate) next_price: Price,
    pub(crate) effective_worst_price: Price,
    pub(crate) source: ProtectionSource,
}
