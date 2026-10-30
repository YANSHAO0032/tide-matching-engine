//! 参考簿测试入口；各组保留独立前置数据与既有断言。

use super::{PRIORITY_GUARD, ReferenceOrderBook};
use crate::{IncomingOrder, ReferenceOrder};
use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{
    EngineEvent, LimitOrder, MarketByQuote, MarketMath, OrderId, Price, ProtectionSource, Qty,
    QueuePriority, QuoteAmount, RejectReason, Side, TradeEvent, TradeId,
};
mod amend_candidate;
mod amend_regular;
mod basic;
mod cancel_replace;
mod cancel_replace_view;
mod expiry;
mod iceberg;
mod limit_execution;
mod maker_query;
mod market_by_base;
mod market_by_quote_execution;
mod market_by_quote_preflight;
mod next_fill_execution;
mod next_fill_planner;
mod post_only_preflight;
mod price_band;
mod property;
mod protected_market_by_base;
mod protected_market_by_quote_execution;
mod protected_market_by_quote_preflight;
mod reservation;
mod reservation_place;
mod stp;
