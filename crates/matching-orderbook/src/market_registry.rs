//! 市价订单与预留归属计划的共享输入；不含撮合算法。

use matching_domain::{
    MarketByBase, MarketByQuote, MarketMath, TradeId, market_order::CommandStartProtectionContext,
};

/// 市价业务事件及可选的已用、未用报价资产金额。
pub type MarketRegistryResult = (
    Vec<matching_domain::EngineEvent>,
    Option<(matching_domain::QuoteAmount, matching_domain::QuoteAmount)>,
);

/// 市价订单的原始计量模式。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketRegistryOrder {
    /// 基础资产数量模式。
    Base(MarketByBase),
    /// 报价资产预算模式。
    Quote(MarketByQuote),
}

/// 市价计划所使用的确定性数值、保护和成交身份输入。
pub struct MarketRegistryContext<'a> {
    /// 市场数值配置。
    pub math: &'a MarketMath,
    /// 调用方已分配的成交身份。
    pub trade_ids: &'a [TradeId],
    /// 命令开始冻结的可选保护上下文。
    pub protection: Option<&'a CommandStartProtectionContext>,
}
