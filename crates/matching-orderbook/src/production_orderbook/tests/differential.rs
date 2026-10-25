//! 差分测试入口与共享观察类型；各场景独立调用参考簿和生产簿。
mod amend_full;
mod amend_regular;
mod execution_constraints;
mod expiry;
mod iceberg;
mod limit_gtc;
mod market_by_base;
mod market_by_quote;
#[cfg(test)]
mod protected_market_by_quote;
mod protected_quote_scenarios;
mod support;
mod tape;

use super::{LevelSnapshotEntry, level_snapshot, order, policy, validate_book};
use std::collections::BTreeSet;

use crate::production_orderbook::ProductionOrderBook;
use crate::reference_orderbook::ReferenceOrderBook;
use crate::{IncomingOrder, OrderNode, PriceLevel, ReferenceOrder};

use proptest::prelude::*;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{
    EngineEvent, LimitGtcOrder, MarketByBase, MarketByQuote, MarketByQuoteExecution, MarketMath,
    MarketProtection, OrderId, Price, ProtectionSource, Qty, QueuePriority, QuoteBudget,
    RejectReason, Side, TradeEvent, TradeId, UserId,
};

use support::{
    assert_production_failure_state_unchanged, assert_regular_book_invariants, observe_production,
    observe_production_order, observe_reference, observe_reference_order, opposite_side,
    reference_contains_order,
};
use tape::{
    MAX_TAPE_LENGTH, TAPE_GENERATOR_VERSION, generate_tape, place_reference, run_tape,
    run_tape_streaming, validate_tape_domain,
};

/// 基础差分命令：限价单 GTC、取消与两类市价单。
///
/// 固定命令序列覆盖各分支；此文件的固定种子随机生成器只生成 GTC/取消。
/// 保护市价与 IOC/FOK/PostOnly 使用各自的命令结构和独立执行适配。
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Place(LimitGtcOrder),
    Cancel(OrderId),
    MarketByBase(MarketByBase, Vec<TradeId>),
    MarketByQuote(MarketByQuote, Vec<TradeId>),
}

/// 跨参考簿 / 生产簿的统一挂单-订单观察模型。
///
/// 不包含 Arena 槽位 / 前驱 / `next`：
///
/// - 参考簿没有这些物理结构；
/// - 差分比较只比较业务语义；
/// - 生产簿的物理结构正确性由 validate_book 单独验证。
#[derive(Debug, PartialEq, Eq)]
struct ObservedOrder {
    /// 不随成交扣减改变的原始限价订单载荷。
    original: LimitGtcOrder,
    /// 观察时该挂单尚未成交的基础资产数量。
    remaining: u64,
    /// 挂单当前持有的市场本地队列优先级。
    priority: QueuePriority,
}

/// 单一价格档的业务观察。
#[derive(Debug, PartialEq, Eq)]
struct ObservedDepth {
    /// 被观察价格档的价格。
    price: Price,
    /// 该价格档中挂单节点的数量。
    order_count: u32,
    /// 该价格档的可见基础资产数量聚合值。
    total_qty: u64,
}

/// 单侧订单簿观察。
///
/// 订单集合必须保持业务顺序：
///
/// 买入:
///     价格降序
///     同价先进先出
///
/// 卖出:
///     价格升序
///     同价先进先出
#[derive(Debug, PartialEq, Eq)]
struct ObservedSide {
    /// 按该侧业务价格与先进先出顺序排列的订单投影。
    orders: Vec<ObservedOrder>,
    /// 按该侧业务价格顺序排列的深度投影。
    depth: Vec<ObservedDepth>,
    /// 该侧最优价格；空侧为 None。
    best_price: Option<Price>,
}

/// 整簿业务观察。
#[derive(Debug, PartialEq, Eq)]
struct ObservedBook {
    /// 买侧的业务观察。
    bids: ObservedSide,
    /// 卖侧的业务观察。
    asks: ObservedSide,
}

/// 将两套实现的不同返回类型收敛到同一个命令执行记录。
#[derive(Debug, PartialEq, Eq)]
enum CommandOutcome {
    Place(Result<Vec<TradeEvent>, RejectReason>),
    Cancel(Result<ObservedOrder, RejectReason>),
    MarketByBase(Result<Vec<TradeEvent>, RejectReason>),
    MarketByQuote(Result<MarketByQuoteExecution, RejectReason>),
}

/// 每执行一条命令都保存一次可观察结果。
///
/// production_next_priority 只记录生产簿：
/// 此基础测试适配器沿用逐笔优先级观察，不直接调用参考簿的计数器读取接口，因此不声称
/// “直接比较了两套实现的内部优先级分配器”。
///
/// 参考簿的优先级正确性通过每张有效订单的优先级比较；
/// 生产簿 `next` 则与“成功挂单次数”比较。
#[derive(Debug, PartialEq, Eq)]
struct StepRecord {
    /// 当前命令的统一执行结果。
    outcome: CommandOutcome,
    /// 当前命令提交后的整簿业务观察。
    book: ObservedBook,
    /// 生产簿下一次将分配的队列优先级数值。
    production_next_priority: u128,
}

/// 参考簿下单适配器的成功结果。
///
/// ReferenceOrderBook 的低层成交入口不会自动挂单吃单方余量，
/// 而 `ProductionOrderBook::place_limit_gtc` 会。
///
/// 因此差分适配器必须显式补齐：
///
/// 成交 -> 实际余量 -> 挂单
///
/// 已挂单表示本条下单最终是否实际消耗了一次 QueuePriority。
#[derive(Debug)]
struct ReferencePlaceResult {
    /// 参考簿撮合路径产生的原序成交事件。
    trades: Vec<TradeEvent>,
    /// 此次下单是否实际将余量挂入参考簿。
    rested: bool,
}

/// 测试自己的 TradeId 分配器。
///
/// 成交标识符 `TradeId`：
///
/// - 在首笔状态变更前准备；
/// - 全命令序列唯一；
/// - 不依赖两套 OrderBook 自己分配；
/// - 同一命令序列重跑时完全确定。
#[derive(Debug)]
struct TradeIdSource {
    /// 下一笔将分配给测试成交的原始标识符数值。
    next: u128,
}

impl TradeIdSource {
    fn new() -> Self {
        Self { next: 1 }
    }

    fn take(&mut self, count: usize) -> Vec<TradeId> {
        (0..count)
            .map(|_| {
                let raw = self.next;

                self.next = self
                    .next
                    .checked_add(1)
                    .expect("differential TradeId counter overflow");

                TradeId::new(raw)
            })
            .collect()
    }
}

/// 与 `OrderArena::test_snapshot` 一致的完整节点和空闲链表快照。
type ArenaSnapshot = (Vec<Option<OrderNode>>, Vec<crate::OrderIndex>);
