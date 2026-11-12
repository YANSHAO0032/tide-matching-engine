//! ProductionOrderBook 的查询、挂单、取消、状态保持与损坏故障时停止处理验证。

use super::ProductionOrderBook;
use crate::market_plan::{ProtectedMarketByQuotePlan, ProtectionBoundaryStop, QuoteFillPlan};
use crate::{IncomingOrder, OrderIndex, OrderNode, PriceLevel, QueuePriorityPolicy};
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
};
use matching_domain::{
    EngineEvent, LimitOrder, MarketByBase, MarketByQuote, MarketMath, OrderId, Price,
    ProtectionSource, Qty, QueuePriority, QuoteAmount, RejectReason, Side, TradeEvent, TradeId,
    UserId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use support::*;

mod support;

mod amend_atomicity;
mod amend_candidate;
mod amend_iceberg;
mod amend_regular;
mod amend_reservation;
mod amend_stop;
mod broad_review;
mod cancel;
mod cancel_replace;
mod cancel_replace_view;
mod differential;
mod expiry;
mod iceberg;
mod immediate_constraints;
mod invariants;
mod limit_gtc;
mod market_by_base;
mod market_by_quote;
mod matching;
mod price_band;
mod query;
mod reservation;
mod resting;
mod stop_trigger;
mod stp;

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
mod trade_cursor;

#[test]
fn priority_policy_update_preserves_counters_and_is_atomic_at_both_boundaries() {
    let old = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(10)).unwrap();
    let mut book = ProductionOrderBook::new(old);
    book.queue_priority_allocator = crate::QueuePriorityAllocator::try_restore(old, 3).unwrap();
    book.stop_priority_allocator = crate::StopPriorityAllocator::try_restore(old, 7).unwrap();
    let before = book.clone();
    let too_small = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(6)).unwrap();
    assert_eq!(
        book.update_priority_policy(too_small),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(book, before);
    book.queue_priority_allocator = crate::QueuePriorityAllocator::try_restore(old, 7).unwrap();
    book.stop_priority_allocator = crate::StopPriorityAllocator::try_restore(old, 3).unwrap();
    let before = book.clone();
    assert_eq!(
        book.update_priority_policy(too_small),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(book, before);
    book.queue_priority_allocator = crate::QueuePriorityAllocator::try_restore(old, 3).unwrap();
    book.stop_priority_allocator = crate::StopPriorityAllocator::try_restore(old, 7).unwrap();
    let exact = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(7)).unwrap();
    book.update_priority_policy(exact).unwrap();
    assert_eq!(book.next_queue_priority(), 3);
    assert_eq!(book.next_stop_priority(), 7);
    assert_eq!(book.priority_policy(), exact);
    assert_eq!(book.stop_priority_allocator.policy(), &exact);
    assert_eq!(
        book.stop_priority_allocator.preflight_allocate(),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
}
