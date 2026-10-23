//! 参考簿与生产簿各自预检后共用的只读市价计划数据。
//!
//! 这些类型只保存已验证的计划结果；它们不访问订单簿容器、不决定挂单
//! 扫描顺序，也不提交状态变更。两种订单簿仍独立执行预检和提交，以保留
//! 差分验证所需的实现独立性。

use matching_domain::{OrderId, Price, ProtectionSource, Qty, QuoteAmount};

/// 单笔按报价资产预算执行的挂单成交计划，不代表已经提交的成交。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuoteFillPlan {
    /// 将被成交的挂单订单标识符。
    pub(crate) maker_order_id: OrderId,
    /// 该挂单的成交价格。
    pub(crate) maker_price: Price,
    /// 本笔计划成交的基础资产数量。
    pub(crate) qty: Qty,
    /// 本笔计划成交对应的报价资产本金。
    pub(crate) notional: QuoteAmount,
}

/// 完整的无保护 MarketByQuote 只读计划。
///
/// `fills` 的顺序就是未来成交事件的顺序；`required_trade_ids` 是提交前
/// TradeId 容量预检的显式契约，即使目前它与成交数量相同也不省略。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarketByQuotePlan {
    /// 按未来事件顺序排列的挂单成交计划。
    pub(crate) fills: Vec<QuoteFillPlan>,
    /// 提交前必须提供的 TradeId 数量。
    pub(crate) required_trade_ids: usize,
    /// 计划使用的报价资产本金。
    pub(crate) spent_quote: QuoteAmount,
    /// 计划结束后未使用的报价资产金额。
    pub(crate) remaining_quote: QuoteAmount,
}

/// 保护 MarketByQuote 的只读计划。
///
/// `stop` 只表示按保护边界终止，不表示报价资产不足一手的预算或订单簿耗尽。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtectedMarketByQuotePlan {
    /// 按未来事件顺序排列的挂单成交计划。
    pub(crate) fills: Vec<QuoteFillPlan>,
    /// 计划使用的报价资产本金。
    pub(crate) spent_quote: QuoteAmount,
    /// 计划结束后未使用的报价资产金额。
    pub(crate) unused_quote: QuoteAmount,
    /// 因保护边界停止时首个不可成交的价格信息。
    pub(crate) stop: Option<ProtectionBoundaryStop>,
}

/// 价格优先顺序中首个因保护边界不可成交的挂单。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProtectionBoundaryStop {
    /// 首个越过保护边界的候选挂单价格。
    pub(crate) next_price: Price,
    /// 合并所有保护来源后的最差可成交价格。
    pub(crate) effective_worst_price: Price,
    /// 形成有效最差价格的保护来源。
    pub(crate) source: ProtectionSource,
}
