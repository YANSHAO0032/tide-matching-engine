//! ProductionOrderBook 的查询、挂单、取消、状态保持与损坏故障时停止处理验证。

use super::ProductionOrderBook;
use crate::market_plan::{ProtectedMarketByQuotePlan, ProtectionBoundaryStop, QuoteFillPlan};
use crate::{IncomingOrder, OrderIndex, OrderNode, PriceLevel, QueuePriorityPolicy};
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
};
use matching_domain::{
    EngineEvent, LimitGtcOrder, MarketByBase, MarketByQuote, MarketMath, OrderId, Price,
    ProtectionSource, Qty, QueuePriority, QuoteAmount, RejectReason, Side, TradeEvent, TradeId,
    UserId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use support::*;

mod support;

mod cancel;
mod differential;
mod iceberg;
mod immediate_constraints;
mod invariants;
mod limit_gtc;
mod market_by_base;
mod market_by_quote;
mod matching;
mod query;
mod resting;

/// 一个价格档位的只读测试投影，不是持久化快照格式。
#[derive(Debug, Clone, PartialEq, Eq)]
struct LevelSnapshotEntry {
    /// 有序价格索引中的键。
    price: Price,
    /// 此档声明的先进先出首尾槽位。
    head: Option<OrderIndex>,
    /// 此档声明的先进先出尾槽位。
    tail: Option<OrderIndex>,
    /// 此档声明的节点数和可见数量合计。
    count: u32,
    /// 此档声明的可见基础数量聚合值。
    total_visible_qty: u64,
}
