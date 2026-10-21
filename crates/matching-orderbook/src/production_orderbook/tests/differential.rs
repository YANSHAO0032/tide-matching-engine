use super::{level_snapshot, order, policy, validate_book};
mod execution_constraints;
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

/// 基础 differential 命令：Limit GTC、Cancel 与两类 Market。
///
/// 固定 tape 覆盖各分支；此文件的固定 seed 随机生成器只生成 GTC/Cancel。
/// 保护市价与 IOC/FOK/PostOnly 使用各自的命令结构和独立执行适配。
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Place(LimitGtcOrder),
    Cancel(OrderId),
    MarketByBase(MarketByBase, Vec<TradeId>),
    MarketByQuote(MarketByQuote, Vec<TradeId>),
}

/// 基础 tape 的测试域约束：在创建订单簿之前检查完整 tape。
///
/// Reference adapter 采用 fills -> residual rest，尚未实现一般情况下的
/// 失败原子事务；因此极值输入不能进入逐命令执行阶段。
/// DuplicateOrderId / OrderNotFound 不属于输入域错误。
const MAX_TAPE_LENGTH: usize = 100_000;

fn validate_tape_domain(commands: &[Command]) -> Result<(), String> {
    if commands.len() > MAX_TAPE_LENGTH {
        return Err(format!(
            "tape length={} outside allowed range 0..=100000",
            commands.len()
        ));
    }

    for (index, command) in commands.iter().enumerate() {
        // T05.2a 诊断优先级：OrderId 必须先于 Place price/qty。
        let id = match command {
            Command::Place(order) => order.order_id.get(),
            Command::Cancel(order_id) => order_id.get(),
            Command::MarketByBase(order, _) => order.order_id.get(),
            Command::MarketByQuote(order, _) => order.order_id.get(),
        };

        if !(1..=128).contains(&id) {
            return Err(format!(
                "command[{index}] OrderId={id} outside allowed range 1..=128"
            ));
        }

        if let Command::Place(order) = command {
            let price = order.price.get();
            if !(80..=120).contains(&price) {
                return Err(format!(
                    "command[{index}] Place price={price} outside allowed range 80..=120"
                ));
            }

            let qty = order.qty.get();
            if !(1..=100).contains(&qty) {
                return Err(format!(
                    "command[{index}] Place qty={qty} outside allowed range 1..=100"
                ));
            }
        }

        if let Command::MarketByBase(order, _) = command {
            let qty = order.qty.get();
            if !(1..=100).contains(&qty) {
                return Err(format!(
                    "command[{index}] MarketByBase qty={qty} outside allowed range 1..=100"
                ));
            }
        }

        if let Command::MarketByQuote(order, _) = command {
            let budget = order.quote_budget.get();

            // 固定短 tape 的受控预算范围。
            // 不修改随机生成器的抽样范围。
            if !(1..=20_000).contains(&budget) {
                return Err(format!(
                    "command[{index}] MarketByQuote budget={budget} \
             outside allowed range 1..=20000"
                ));
            }
        }
    }

    Ok(())
}

/// T05.2b 固定 seed 随机命令生成器。测试输入独立于订单簿状态。
const TAPE_GENERATOR_VERSION: &str = "T05.2b-v1";

fn generate_tape(seed: [u8; 32], length: usize) -> Vec<Command> {
    assert!(
        length <= MAX_TAPE_LENGTH,
        "generate_tape length={length} outside allowed range 0..=100000"
    );

    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    // kind 0..=2: Place; kind 3: Cancel; Buy/Sell 概率各半。
    let strategy = (0u8..4, 1u128..=128, 80i64..=120, 1u64..=100, any::<bool>());
    let mut commands = Vec::with_capacity(length);

    for index in 0..length {
        let tree = strategy.new_tree(&mut runner).unwrap_or_else(|error| {
            panic!(
                "tape sampling failed: seed={seed:02x?}, algorithm=ChaCha, \
                 index={index}, error={error:?}"
            )
        });
        let (kind, id, price, qty, is_buy) = tree.current();
        let command = if kind < 3 {
            let side = if is_buy { Side::Buy } else { Side::Sell };
            Command::Place(order(id, side, price, qty))
        } else {
            Command::Cancel(OrderId::new(id))
        };
        commands.push(command);
    }

    validate_tape_domain(&commands)
        .unwrap_or_else(|reason| panic!("generated invalid tape: {reason}"));
    commands
}

/// 跨 Reference / Production 的统一 resting-order 观察模型。
///
/// 不包含 Arena slot / prev / next：
///
/// - Reference 没有这些物理结构；
/// - 差分比较只比较业务语义；
/// - Production 的物理结构正确性由 validate_book 单独验证。
#[derive(Debug, PartialEq, Eq)]
struct ObservedOrder {
    original: LimitGtcOrder,
    remaining: u64,
    priority: QueuePriority,
}

/// 单一价格档的业务观察。
#[derive(Debug, PartialEq, Eq)]
struct ObservedDepth {
    price: Price,
    order_count: u32,
    total_qty: u64,
}

/// 单侧订单簿观察。
///
/// orders 必须保持业务顺序：
///
/// Buy:
///     price DESC
///     same-price FIFO
///
/// Sell:
///     price ASC
///     same-price FIFO
#[derive(Debug, PartialEq, Eq)]
struct ObservedSide {
    orders: Vec<ObservedOrder>,
    depth: Vec<ObservedDepth>,
    best_price: Option<Price>,
}

/// 整簿业务观察。
#[derive(Debug, PartialEq, Eq)]
struct ObservedBook {
    bids: ObservedSide,
    asks: ObservedSide,
}

/// 将两套实现的不同返回类型收敛到同一个 command transcript。
#[derive(Debug, PartialEq, Eq)]
enum CommandOutcome {
    Place(Result<Vec<TradeEvent>, RejectReason>),
    Cancel(Result<ObservedOrder, RejectReason>),
    MarketByBase(Result<Vec<TradeEvent>, RejectReason>),
    MarketByQuote(Result<MarketByQuoteExecution, RejectReason>),
}

/// 每执行一条命令都保存一次可观察结果。
///
/// production_next_priority 只记录 Production：
/// Reference 当前没有公开 next-priority getter，因此本测试不假称
/// “直接比较了两套实现的内部 priority allocator”。
///
/// Reference 的 priority 正确性通过每张 live order 的 priority 比较；
/// Production next 则与“成功 rest 次数”比较。
#[derive(Debug, PartialEq, Eq)]
struct StepRecord {
    outcome: CommandOutcome,
    book: ObservedBook,
    production_next_priority: u128,
}

/// Reference Place adapter 的成功结果。
///
/// ReferenceOrderBook 的低层成交入口不会自动 rest taker remainder，
/// 而 ProductionOrderBook::place_limit_gtc 会。
///
/// 因此 differential adapter 必须显式补齐：
///
/// fills -> actual remainder -> rest
///
/// rested 表示本条 Place 最终是否实际消耗了一次 QueuePriority。
#[derive(Debug)]
struct ReferencePlaceResult {
    trades: Vec<TradeEvent>,
    rested: bool,
}

/// 测试自己的 TradeId allocator。
///
/// TradeId：
///
/// - 在首笔 mutation 前准备；
/// - 全 tape 唯一；
/// - 不依赖两套 OrderBook 自己分配；
/// - 同一 tape 重跑时完全确定。
#[derive(Debug)]
struct TradeIdSource {
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

/// 判断 Reference 当前 active book 是否已经存在指定 OrderId。
///
/// 必须通过公开有序视图观察，不能访问 Reference 内部 Vec。
fn reference_contains_order(book: &ReferenceOrderBook, order_id: OrderId) -> bool {
    [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| book.orders(side))
        .any(|resting| resting.original_order().order_id == order_id)
}

/// 返回 opposite side。
fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// 投影 Reference 返回的 removed order。
fn observe_reference_order(order: &ReferenceOrder) -> ObservedOrder {
    ObservedOrder {
        original: order.original_order().clone(),
        remaining: order.remaining(),
        priority: order.priority(),
    }
}

/// 投影 Production 返回的 removed/live node。
fn observe_production_order(node: &OrderNode) -> ObservedOrder {
    ObservedOrder {
        original: node.original_order().clone(),
        remaining: node.remaining(),
        priority: node.priority(),
    }
}

/// Reference 一侧业务观察。
///
/// 严格使用 ReferenceOrderBook::orders(side)；
/// 不重新按 ID / priority 排序。
fn observe_reference_side(book: &ReferenceOrderBook, side: Side) -> ObservedSide {
    let ordered = book.orders(side);

    let mut orders = Vec::with_capacity(ordered.len());
    let mut depth: Vec<ObservedDepth> = Vec::new();

    for resting in ordered {
        let original = resting.original_order().clone();
        let remaining = resting.remaining();

        assert!(
            remaining > 0,
            "differential Reference observation found exhausted live order: \
             order_id={:?}",
            original.order_id
        );

        orders.push(ObservedOrder {
            original: original.clone(),
            remaining,
            priority: resting.priority(),
        });

        match depth.last_mut() {
            Some(level) if level.price == original.price => {
                level.order_count = level
                    .order_count
                    .checked_add(1)
                    .expect("differential Reference depth count overflow");

                level.total_qty = level
                    .total_qty
                    .checked_add(remaining)
                    .expect("differential Reference depth quantity overflow");
            }

            _ => {
                depth.push(ObservedDepth {
                    price: original.price,
                    order_count: 1,
                    total_qty: remaining,
                });
            }
        }
    }

    let best_price = orders.first().map(|entry| entry.original.price);

    ObservedSide {
        orders,
        depth,
        best_price,
    }
}

/// Reference 整簿业务观察。
fn observe_reference(book: &ReferenceOrderBook) -> ObservedBook {
    ObservedBook {
        bids: observe_reference_side(book, Side::Buy),
        asks: observe_reference_side(book, Side::Sell),
    }
}

/// 从 Production 的一个 PriceLevel 按真实 intrusive FIFO：
///
/// head -> next
///
/// 生成订单观察。
///
/// 禁止：
///
/// - 根据 OrderId 排序；
/// - 根据 priority 重新排序；
/// - 遍历 HashMap<OrderId, OrderIndex> 推导业务顺序。
fn append_production_level(
    book: &ProductionOrderBook,
    expected_side: Side,
    price: Price,
    level: &PriceLevel,
    orders: &mut Vec<ObservedOrder>,
    depth: &mut Vec<ObservedDepth>,
) {
    assert!(
        level.count() > 0,
        "differential Production observation found empty price level: \
         side={expected_side:?}, price={price:?}"
    );

    let mut current = level.head();
    let mut observed_count = 0u32;
    let mut observed_qty = 0u64;

    while let Some(index) = current {
        let node = book
            .arena
            .get(index)
            .expect("differential Production observation found missing Arena node");

        assert_eq!(
            node.original_order().side,
            expected_side,
            "differential Production observation found side mismatch"
        );

        assert_eq!(
            node.original_order().price,
            price,
            "differential Production observation found price mismatch"
        );

        assert!(
            node.remaining() > 0,
            "differential Production observation found exhausted live node"
        );

        orders.push(observe_production_order(node));

        observed_count = observed_count
            .checked_add(1)
            .expect("differential Production observed count overflow");

        observed_qty = observed_qty
            .checked_add(node.remaining())
            .expect("differential Production observed quantity overflow");

        current = node.next();
    }

    // 观察逻辑自身也锁定一次聚合值。
    //
    // observe_production 已在任何链表遍历前完成整簿验证。
    assert_eq!(
        observed_count,
        level.count(),
        "differential Production level count mismatch"
    );

    assert_eq!(
        observed_qty,
        level.total_visible_qty(),
        "differential Production level quantity mismatch"
    );

    depth.push(ObservedDepth {
        price,
        order_count: level.count(),
        total_qty: level.total_visible_qty(),
    });
}

/// Production 一侧业务观察。
///
/// Buy:
///     BTreeMap reverse => 高价 -> 低价
///
/// Sell:
///     BTreeMap forward => 低价 -> 高价
///
/// 同价格：
///     PriceLevel head -> next
fn observe_production_side(book: &ProductionOrderBook, side: Side) -> ObservedSide {
    let mut orders = Vec::new();
    let mut depth = Vec::new();

    match side {
        Side::Buy => {
            for (price, level) in book.bids.iter().rev() {
                append_production_level(book, Side::Buy, *price, level, &mut orders, &mut depth);
            }
        }

        Side::Sell => {
            for (price, level) in &book.asks {
                append_production_level(book, Side::Sell, *price, level, &mut orders, &mut depth);
            }
        }
    }

    ObservedSide {
        orders,
        depth,
        best_price: book.best_price(side),
    }
}

/// Production 整簿业务观察。
///
/// 成功和拒绝路径都必须先通过整簿校验，避免在损坏链表上无限遍历。
fn observe_production(book: &ProductionOrderBook) -> ObservedBook {
    validate_book(book).unwrap_or_else(|reason| {
        panic!("Production validator failed before differential observation: {reason}")
    });

    ObservedBook {
        bids: observe_production_side(book, Side::Buy),
        asks: observe_production_side(book, Side::Sell),
    }
}

/// T05.5 当前 regular-book 的只读不变量检查。
///
/// 适用范围（以当前 regular Limit GTC / Cancel 测试域为准）：
/// §54: 1–5、7–11、26、49；16 仅成交正量与单 tape 内 TradeId 唯一；
/// 28–30 仅 regular QueuePriority 的单调分配、同价 FIFO 和不复用部分。
/// Stop、expiry、跨 shard 唯一性、WAL、digest、crash 均不适用，
/// 不将本测试解释为覆盖这些尚未接入的系统性质。
fn assert_regular_book_invariants(book: &ObservedBook, next_priority: u128, step: usize) {
    let mut live_ids = BTreeSet::new();
    let mut live_priorities = BTreeSet::new();

    for (side, observed) in [(Side::Buy, &book.bids), (Side::Sell, &book.asks)] {
        assert_eq!(
            observed.best_price,
            observed.depth.first().map(|level| level.price),
            "regular best price mismatch: step={step}, side={side:?}"
        );

        let mut order_cursor = 0usize;
        for (level_index, level) in observed.depth.iter().enumerate() {
            assert!(
                level.order_count > 0,
                "empty regular depth level: step={step}"
            );
            assert!(level.total_qty > 0, "zero regular depth qty: step={step}");

            if level_index > 0 {
                let previous = observed.depth[level_index - 1].price.get();
                let current = level.price.get();
                match side {
                    Side::Buy => assert!(previous > current, "bid price order: step={step}"),
                    Side::Sell => assert!(previous < current, "ask price order: step={step}"),
                }
            }

            let mut level_qty = 0u64;
            let mut previous_priority: Option<u128> = None;
            for _ in 0..level.order_count {
                let entry = observed.orders.get(order_cursor).unwrap_or_else(|| {
                    panic!("missing regular order for depth: step={step}, side={side:?}")
                });
                assert_eq!(
                    entry.original.side, side,
                    "regular side mismatch: step={step}"
                );
                assert_eq!(
                    entry.original.price, level.price,
                    "regular level mismatch: step={step}"
                );
                assert!(entry.remaining > 0, "regular live zero qty: step={step}");
                assert!(
                    entry.remaining <= entry.original.qty.get(),
                    "regular remaining exceeds original: step={step}"
                );
                level_qty = level_qty
                    .checked_add(entry.remaining)
                    .expect("regular depth qty overflow");

                let id = entry.original.order_id.get();
                assert!(
                    live_ids.insert(id),
                    "duplicate regular live OrderId={id}: step={step}"
                );
                let priority = entry.priority.get();
                assert!(
                    priority < next_priority,
                    "regular priority not yet issued: step={step}"
                );
                assert!(
                    live_priorities.insert(priority),
                    "duplicate regular priority: step={step}"
                );
                if let Some(previous) = previous_priority {
                    assert!(
                        previous < priority,
                        "regular same-price FIFO priority: step={step}"
                    );
                }
                previous_priority = Some(priority);
                order_cursor += 1;
            }
            assert_eq!(
                level_qty, level.total_qty,
                "regular depth qty mismatch: step={step}"
            );
        }
        assert_eq!(
            order_cursor,
            observed.orders.len(),
            "unaccounted regular live orders: step={step}"
        );
    }

    if let (Some(bid), Some(ask)) = (book.bids.best_price, book.asks.best_price) {
        assert!(bid.get() < ask.get(), "crossed regular book: step={step}");
    }
}

/// Reference 完整 Limit GTC 差分适配。
///
/// Production:
///
///     place_limit_gtc
///         -> preflight
///         -> fills
///         -> residual rest
///
/// Reference 当前是低层组合：
///
///     duplicate check
///         -> execute_next_fill_trade*
///         -> actual remainder
///         -> rest(actual remainder)
///
/// 此适配使用小数量、远离 priority 上限的受控输入，因此这里只允许：
///
/// - DuplicateOrderId 作为受控 Place 错误；
/// - 正常成交 / rest。
///
/// ArithmeticOverflow / PrioritySpaceExhaustion 等其它错误一律立即 panic，
/// 不把这个测试 adapter 描述成通用失败原子事务。
fn place_reference(
    book: &mut ReferenceOrderBook,
    order: LimitGtcOrder,
    trade_ids: &[TradeId],
) -> Result<ReferencePlaceResult, RejectReason> {
    // ------------------------------------------------------------
    // 1. active duplicate 必须先于任何 mutation。
    // ------------------------------------------------------------

    if reference_contains_order(book, order.order_id) {
        return Err(RejectReason::DuplicateOrderId);
    }

    // ------------------------------------------------------------
    // 2. 首笔 mutation 前保证 TradeId 足量。
    //
    // 当前每个 resting maker 对一个 incoming 最多贡献一笔成交。
    // opposite side 当前 live order 总数因此是安全上界。
    //
    // 可以多给，不能少给。
    // ------------------------------------------------------------

    let maximum_fill_count = book.orders(opposite_side(order.side)).len();

    assert!(
        trade_ids.len() >= maximum_fill_count,
        "differential Reference adapter received insufficient TradeIds: \
         maximum_fill_count={maximum_fill_count}, provided={}",
        trade_ids.len()
    );

    let mut incoming = IncomingOrder::new(order.clone());
    let mut trades = Vec::new();

    // ------------------------------------------------------------
    // 3. 逐笔成交。
    // ------------------------------------------------------------

    for trade_id in trade_ids.iter().copied() {
        match book.execute_next_fill_trade(&mut incoming, trade_id) {
            Ok(Some(trade)) => {
                trades.push(trade);
            }

            Ok(None) => {
                break;
            }

            Err(reason) => {
                panic!(
                    "unexpected Reference fill error in T05.1 fixed differential tape: \
                     order_id={:?}, reason={reason:?}",
                    order.order_id
                );
            }
        }
    }

    // ------------------------------------------------------------
    // 4. Reference 不自动 rest remainder。
    //
    // 必须使用“实际剩余量”，不能把原始 qty 直接重新挂入。
    // ------------------------------------------------------------

    let rested = incoming.remaining() > 0;

    if rested {
        let residual_qty = Qty::try_new(incoming.remaining())
            .expect("positive Reference differential remainder must form Qty");

        let mut residual_order = order;

        residual_order.qty = residual_qty;

        book.rest(residual_order).unwrap_or_else(|reason| {
            panic!(
                "unexpected Reference residual-rest error in T05.1 fixed differential tape: \
                 reason={reason:?}"
            )
        });
    }

    Ok(ReferencePlaceResult { trades, rested })
}

/// 与 OrderArena::test_snapshot 一致的完整节点和 free-list 快照。
type ArenaSnapshot = (Vec<Option<OrderNode>>, Vec<crate::OrderIndex>);

/// 在受控业务错误后验证 Production 的完整内部状态不变。
///
/// 这里比 semantic observation 更严格，同时覆盖：
///
/// - bid / ask level metadata
/// - OrderId index
/// - Arena occupied slots / free-list
/// - next QueuePriority
/// - allocator policy
fn assert_production_failure_state_unchanged(
    book: &ProductionOrderBook,
    bids_before: &[super::LevelSnapshotEntry],
    asks_before: &[super::LevelSnapshotEntry],
    orders_before: &std::collections::HashMap<OrderId, crate::OrderIndex>,
    arena_before: &ArenaSnapshot,
    next_before: u128,
    policy_before: crate::QueuePriorityPolicy,
) {
    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "failed differential command mutated Production bids"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "failed differential command mutated Production asks"
    );

    assert_eq!(
        &book.orders, orders_before,
        "failed differential command mutated Production ID index"
    );

    assert_eq!(
        &book.arena.test_snapshot(),
        arena_before,
        "failed differential command mutated Production Arena"
    );

    assert_eq!(
        book.next_queue_priority(),
        next_before,
        "failed differential command consumed Production QueuePriority"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "failed differential command mutated Production allocator policy"
    );
}

/// 收集模式下执行一条受控 command tape。
///
/// 每条 command 后都完成：
///
/// 1. Reference / Production command outcome 对照；
/// 2. TradeEvent 原顺序对照；
/// 3. 双侧完整 resting order 对照；
/// 4. remaining / priority 对照；
/// 5. depth 对照；
/// 6. best price 对照；
/// 7. Production validate_book；
/// 8. Production next priority == 成功 rest 次数。
fn run_tape(commands: &[Command]) -> Vec<StepRecord> {
    let mut transcript = Vec::with_capacity(commands.len());
    execute_tape(commands, |record| transcript.push(record));
    transcript
}

/// T05.3: 不存储完整 transcript；每步验证完成后仅增加计数。
fn run_tape_streaming(commands: &[Command]) -> usize {
    let mut processed = 0usize;
    execute_tape(commands, |_record| {
        processed += 1;
    });
    processed
}

/// 唯一的差分执行核心。
///
/// 状态顺序固定为：完整输入域预检、空簿初始化、命令前观察与失败快照、
/// 两套实现独立执行、结果和原序事件比较、命令后观察与不变量比较，最后才
/// 向回调交付 `StepRecord`。因此流式模式只丢弃已经验证的记录，不能退化为
/// 最终状态比较；任何失败都保留对应的 step 和 command 上下文。
fn execute_tape(commands: &[Command], mut on_record: impl FnMut(StepRecord)) {
    validate_tape_domain(commands)
        .unwrap_or_else(|reason| panic!("invalid differential tape domain: {reason}"));

    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let quote_math = MarketMath::try_new(0, 0, 0, 1, 1)
        .expect("fixed MarketByQuote math configuration must be valid");

    let mut trade_ids = TradeIdSource::new();

    // Reference 没有公开 next-priority getter。
    //
    // 因此这里独立记录“成功 rest 次数”。
    // Production allocator 从 0 开始，每次成功 rest 恰好推进一次。
    let mut expected_production_next_priority = 0u128;
    // 测试专用：全 tape 成交 ID 唯一性，不随单步快照丢弃。
    let mut seen_trade_ids = BTreeSet::new();

    assert_eq!(validate_book(&production), Ok(()));

    for (step_index, command) in commands.iter().enumerate() {
        // ------------------------------------------------------------
        // command 前，两套簿必须已经一致。
        // ------------------------------------------------------------

        let reference_before = observe_reference(&reference);
        let production_before = observe_production(&production);

        assert_eq!(
            reference_before, production_before,
            "books already diverged before differential command: \
             step={step_index}, command={command:?}"
        );

        // Production 完整失败原子性快照。
        let production_bids_before = level_snapshot(&production.bids);
        let production_asks_before = level_snapshot(&production.asks);
        let production_orders_before = production.orders.clone();
        let production_arena_before = production.arena.test_snapshot();
        let production_next_before = production.next_queue_priority();
        let production_policy_before = *production.queue_priority_allocator.policy();

        let outcome = match command {
            // ========================================================
            // Place
            // ========================================================
            Command::Place(candidate) => {
                // duplicate 必须先决定。
                //
                // duplicate command 不需要为撮合准备 TradeId，
                // 也不应该推进测试 TradeId source。
                let duplicate = reference_contains_order(&reference, candidate.order_id);

                let supplied_trade_ids = if duplicate {
                    Vec::new()
                } else {
                    // 每个 opposite live order 至多需要一个 TradeId。
                    //
                    // 这是 mutation 前确定的安全上界。
                    let maximum_fill_count = reference.orders(opposite_side(candidate.side)).len();

                    trade_ids.take(maximum_fill_count)
                };

                let reference_result =
                    place_reference(&mut reference, candidate.clone(), &supplied_trade_ids);

                let production_result =
                    production.place_limit_gtc(candidate.clone(), &supplied_trade_ids);

                match (reference_result, production_result) {
                    // ------------------------------------------------
                    // 两边成功。
                    // ------------------------------------------------
                    (Ok(reference_place), Ok(production_trades)) => {
                        // TradeEvent 必须保持实际成交顺序。
                        assert_eq!(
                            reference_place.trades, production_trades,
                            "TradeEvent tape diverged: \
                             step={step_index}, command={command:?}"
                        );

                        // 两边是否最终挂入 incoming remainder 必须一致。
                        let production_rested = production.order(candidate.order_id).is_some();

                        let reference_rested =
                            reference_contains_order(&reference, candidate.order_id);

                        assert_eq!(
                            reference_place.rested, reference_rested,
                            "Reference adapter rest decision does not match \
                             Reference live book: \
                             step={step_index}, command={command:?}"
                        );

                        assert_eq!(
                            reference_place.rested, production_rested,
                            "Reference / Production residual-rest decision \
                             diverged: \
                             step={step_index}, command={command:?}"
                        );

                        // 只有最终存在 actual remainder 并成功 rest，
                        // 才允许消耗 priority。
                        //
                        // exact fill 不推进。
                        if reference_place.rested {
                            expected_production_next_priority = expected_production_next_priority
                                .checked_add(1)
                                .expect("differential expected priority overflow");
                        }

                        CommandOutcome::Place(Ok(production_trades))
                    }

                    // ------------------------------------------------
                    // 此受控 GTC 输入域唯一允许的 Place error：
                    // active duplicate。
                    // ------------------------------------------------
                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "DuplicateOrderId mutated Reference semantic state: \
                             step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "DuplicateOrderId mutated Production semantic state: \
                             step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
                    }

                    // ------------------------------------------------
                    // 本步不接受其它错误。
                    // ------------------------------------------------
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected Place differential result: \
                             step={step_index}, \
                             command={command:?}, \
                             reference={reference_result:?}, \
                             production={production_result:?}"
                        );
                    }
                }
            }

            // ========================================================
            // MarketByBase: 两套公开入口各自独立执行。
            // TradeId 由固定 tape 明确提供，不使用 Reference 推导结果。
            // ========================================================
            Command::MarketByBase(candidate, supplied_trade_ids) => {
                let reference_result =
                    reference.execute_market_by_base(candidate.clone(), supplied_trade_ids);
                let production_result =
                    production.execute_market_by_base(candidate.clone(), supplied_trade_ids);

                match (reference_result, production_result) {
                    (Ok(reference_trades), Ok(production_trades)) => {
                        // Vec 原序比较，不允许按 ID、价格或 TradeId 排序。
                        assert_eq!(
                            reference_trades, production_trades,
                            "MarketByBase TradeEvent order diverged: \
                             step={step_index}, command={command:?}"
                        );
                        assert!(
                            !reference_contains_order(&reference, candidate.order_id),
                            "MarketByBase taker unexpectedly rested in Reference: step={step_index}"
                        );
                        assert!(
                            production.order(candidate.order_id).is_none(),
                            "MarketByBase taker unexpectedly rested in Production: step={step_index}"
                        );
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "MarketByBase consumed QueuePriority: step={step_index}"
                        );
                        CommandOutcome::MarketByBase(Ok(production_trades))
                    }
                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        assert_eq!(observe_reference(&reference), reference_before);
                        assert_eq!(observe_production(&production), production_before);
                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );
                        CommandOutcome::MarketByBase(Err(RejectReason::DuplicateOrderId))
                    }
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected MarketByBase differential outcome: \
                             step={step_index}, command={command:?}, \
                             reference={reference_result:?}, production={production_result:?}"
                        );
                    }
                }
            }

            Command::MarketByQuote(candidate, supplied_trade_ids) => {
                // 两套模型独立调用各自公开入口。
                // 不能以 Reference 的成交结果驱动 Production。
                let reference_result = reference.execute_market_by_quote(
                    candidate.clone(),
                    &quote_math,
                    supplied_trade_ids,
                );

                let production_result = production.execute_market_by_quote(
                    candidate.clone(),
                    &quote_math,
                    supplied_trade_ids,
                );

                match (reference_result, production_result) {
                    (Ok(reference_execution), Ok(production_execution)) => {
                        // 比较完整结果值对象：
                        // - 原序 Vec<TradeEvent>
                        // - spent_quote
                        // - unused_quote
                        // 不排序、不投影、不重算 notional。
                        assert_eq!(
                            reference_execution, production_execution,
                            "MarketByQuote execution diverged: \
                 step={step_index}, command={command:?}"
                        );

                        // 成功市价 taker 不能成为 active resting order。
                        assert!(
                            !reference_contains_order(&reference, candidate.order_id),
                            "MarketByQuote taker rested in Reference: \
                 step={step_index}"
                        );

                        assert!(
                            production.order(candidate.order_id).is_none(),
                            "MarketByQuote taker rested in Production: \
                 step={step_index}"
                        );

                        // Quote 市价单不消费 QueuePriority。
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "MarketByQuote consumed QueuePriority: \
                 step={step_index}"
                        );

                        CommandOutcome::MarketByQuote(Ok(production_execution))
                    }

                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        // Duplicate 必须在 maker mutation 前拒绝。
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "MarketByQuote duplicate mutated Reference: \
                 step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "MarketByQuote duplicate mutated Production: \
                 step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::MarketByQuote(Err(RejectReason::DuplicateOrderId))
                    }

                    (reference_result, production_result) => {
                        panic!(
                            "unexpected MarketByQuote differential outcome: \
                 step={step_index}, command={command:?}, \
                 reference={reference_result:?}, \
                 production={production_result:?}"
                        );
                    }
                }
            }

            // ========================================================
            // Cancel
            // ========================================================
            Command::Cancel(order_id) => {
                let reference_result = reference.cancel(*order_id);
                let production_result = production.cancel(*order_id);

                match (reference_result, production_result) {
                    // ------------------------------------------------
                    // 两边成功。
                    // ------------------------------------------------
                    (Ok(reference_removed), Ok(production_removed)) => {
                        let reference_removed = observe_reference_order(&reference_removed);

                        let production_removed = observe_production_order(&production_removed);

                        assert_eq!(
                            reference_removed, production_removed,
                            "Cancel removed payload diverged: \
                             step={step_index}, order_id={order_id:?}"
                        );

                        // Cancel 不消费 QueuePriority。
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "successful Cancel consumed Production \
                             QueuePriority: \
                             step={step_index}, order_id={order_id:?}"
                        );

                        CommandOutcome::Cancel(Ok(production_removed))
                    }

                    // ------------------------------------------------
                    // 此受控输入域唯一允许的 Cancel error。
                    // ------------------------------------------------
                    (Err(RejectReason::OrderNotFound), Err(RejectReason::OrderNotFound)) => {
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "OrderNotFound mutated Reference semantic state: \
                             step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "OrderNotFound mutated Production semantic state: \
                             step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
                    }

                    // ------------------------------------------------
                    // 本步不接受其它错误。
                    // ------------------------------------------------
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected Cancel differential result: \
                             step={step_index}, \
                             order_id={order_id:?}, \
                             reference={reference_result:?}, \
                             production={production_result:?}"
                        );
                    }
                }
            }
        };

        // ------------------------------------------------------------
        // 每条 command 后 Production 都必须通过整簿 validator。
        // ------------------------------------------------------------

        assert_eq!(
            validate_book(&production),
            Ok(()),
            "Production validator failed after differential command: \
             step={step_index}, command={command:?}"
        );

        // ------------------------------------------------------------
        // 每条 command 后重新独立观察两套簿。
        // ------------------------------------------------------------

        let reference_after = observe_reference(&reference);
        let production_after = observe_production(&production);

        assert_eq!(
            reference_after, production_after,
            "Reference / Production semantic state diverged after command: \
             step={step_index}, command={command:?}"
        );

        // ------------------------------------------------------------
        // Reference 无公开 next getter。
        //
        // Production next 必须等于成功 rest 次数。
        // ------------------------------------------------------------

        assert_eq!(
            production.next_queue_priority(),
            expected_production_next_priority,
            "Production next QueuePriority diverged from successful-rest \
             count: \
             step={step_index}, command={command:?}"
        );

        // 所有原有比较、validator、失败原子性及 priority 检查通过之后，
        // 才核查当前 regular-book 不变量及本 tape 的成交 ID。
        assert_regular_book_invariants(
            &production_after,
            production.next_queue_priority(),
            step_index,
        );
        let successful_trades: Option<&[TradeEvent]> = match &outcome {
            CommandOutcome::Place(Ok(trades)) | CommandOutcome::MarketByBase(Ok(trades)) => {
                Some(trades)
            }

            CommandOutcome::MarketByQuote(Ok(execution)) => Some(execution.trades()),

            _ => None,
        };
        if let Some(trades) = successful_trades {
            for trade in trades {
                assert!(
                    trade.qty.get() > 0,
                    "zero TradeEvent qty: step={step_index}"
                );
                let id = trade.trade_id.get();
                assert!(
                    seen_trade_ids.insert(id),
                    "duplicate TradeId={id} in tape: step={step_index}"
                );
            }
        }

        on_record(StepRecord {
            outcome,
            book: production_after,
            production_next_priority: production.next_queue_priority(),
        });
    }
}

/// 主方向固定 tape。
///
/// 预期：
///
/// Sell 30 @105 x3
/// Sell 10 @105 x7
///
/// Buy 100 @110 x8
///   -> maker 30 fill 3
///   -> maker 10 fill 5
///   -> exact fill，不 rest，不消耗 priority
///
/// Buy 101 @110 x5
///   -> maker 10 fill 2
///   -> residual Buy 101 @110 x3
///
/// Place duplicate 101：
///   当前 active 101 是 Buy@110；
///   故意发送 Sell101@100，它本应 crossing，
///   用来证明 duplicate check 在 matching 前完成。
///
/// Cancel101
/// Cancel101 again -> OrderNotFound
///
/// 再用 101 挂 Sell@120 x4，证明 Cancel 后 ID 可复用。
fn buy_taker_tape() -> Vec<Command> {
    vec![
        Command::Place(order(30, Side::Sell, 105, 3)),
        Command::Place(order(10, Side::Sell, 105, 7)),
        Command::Place(order(100, Side::Buy, 110, 8)),
        Command::Place(order(101, Side::Buy, 110, 5)),
        Command::Place(order(101, Side::Sell, 100, 1)),
        Command::Cancel(OrderId::new(101)),
        Command::Cancel(OrderId::new(101)),
        Command::Place(order(101, Side::Sell, 120, 4)),
    ]
}

/// 即使价树为空，也不能把含有孤立节点的损坏簿观察成合法空簿。
#[test]
#[should_panic(expected = "arena/orphan")]
fn production_observation_rejects_orphan_arena_node() {
    let mut book = ProductionOrderBook::new(policy());
    book.arena
        .insert(OrderNode::new(
            order(99, Side::Buy, 100, 5),
            QueuePriority::new(0),
        ))
        .unwrap();

    let _ = observe_production(&book);
}

/// 在生成 FIFO 投影前由 validator 拒绝可达环，而非继续沿 next 遍历。
#[test]
#[should_panic(expected = "link/cycle-or-duplicate")]
fn production_observation_rejects_cycle_before_traversal() {
    let mut book = ProductionOrderBook::new(policy());
    let first = super::rest_fixture(&mut book, order(1, Side::Buy, 100, 5));
    let second = super::rest_fixture(&mut book, order(2, Side::Buy, 100, 7));
    book.arena.get_mut(second).unwrap().set_next(Some(first));

    let _ = observe_production(&book);
}

/// Sell taker 的方向镜像。
///
/// Buy 30 @95 x3
/// Buy 10 @95 x7
///
/// Sell 100 @90 x8
///   -> maker 30 fill 3
///   -> maker 10 fill 5
///
/// Sell 101 @90 x5
///   -> maker 10 fill 2
///   -> residual Sell 101 @90 x3
///
/// active duplicate 时故意发送 Buy101@100，
/// 它本应 crossing Sell101@90，用于验证 duplicate-first。
fn sell_taker_tape() -> Vec<Command> {
    vec![
        Command::Place(order(30, Side::Buy, 95, 3)),
        Command::Place(order(10, Side::Buy, 95, 7)),
        Command::Place(order(100, Side::Sell, 90, 8)),
        Command::Place(order(101, Side::Sell, 90, 5)),
        Command::Place(order(101, Side::Buy, 100, 1)),
        Command::Cancel(OrderId::new(101)),
        Command::Cancel(OrderId::new(101)),
        Command::Place(order(101, Side::Buy, 80, 4)),
    ]
}

/// 固定 MarketByBase 输入只包含正 Qty；TradeId 序列直接记录在 tape 中。
fn market_command(id: u128, side: Side, qty: u64, ids: &[u128]) -> Command {
    Command::MarketByBase(
        MarketByBase {
            order_id: OrderId::new(id),
            user_id: matching_domain::UserId::new(id as u64),
            side,
            qty: Qty::try_new(qty).unwrap(),
            market_protection: None,
        },
        ids.iter().copied().map(TradeId::new).collect(),
    )
}

fn quote_market_command(id: u128, side: Side, budget: u128, ids: &[u128]) -> Command {
    Command::MarketByQuote(
        MarketByQuote {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            quote_budget: QuoteBudget::try_new(budget).unwrap(),
            market_protection: None,
        },
        ids.iter().copied().map(TradeId::new).collect(),
    )
}

fn fixed_market_by_base_tape(taker_side: Side) -> Vec<Command> {
    let maker_side = opposite_side(taker_side);
    let (better, worse) = match taker_side {
        Side::Buy => (105, 110),
        Side::Sell => (95, 90),
    };
    vec![
        // empty opposite side，MarketByBase 不得挂簿。
        market_command(90, taker_side, 5, &[]),
        // 单档 partial：maker30 剩余 4。
        Command::Place(order(30, maker_side, better, 7)),
        market_command(91, taker_side, 3, &[9001]),
        // 第二笔买/卖入场：原有 maker30 优先，随后同价 maker10，再到 worse maker20。
        Command::Place(order(20, maker_side, worse, 4)),
        Command::Place(order(10, maker_side, better, 5)),
        market_command(92, taker_side, 11, &[9002, 9003, 9004]),
        // 此时只余 worse maker20 的 2 lots；输入大于流动性，余量终结。
        market_command(93, taker_side, 9, &[9005]),
        // 当前 opposite side 已空；再次 empty。
        market_command(94, taker_side, 1, &[]),
        // 活动重复 ID 优先返回 DuplicateOrderId，提供足量 TradeId（空即可）。
        Command::Place(order(50, maker_side, worse, 2)),
        market_command(50, taker_side, 1, &[]),
    ]
}

fn fixed_market_by_quote_tape(taker_side: Side) -> Vec<Command> {
    let maker_side = opposite_side(taker_side);

    let (better, worse) = match taker_side {
        Side::Buy => (105i64, 110i64),
        Side::Sell => (95i64, 90i64),
    };

    let partial_budget = (3 * better + 1) as u128;
    let multi_budget = (9 * better + 2 * worse) as u128;

    vec![
        // 0. 空对手盘，不挂单，不消耗 priority。
        quote_market_command(90, taker_side, 1000, &[]),
        // 1. 建立第一张 maker。
        Command::Place(order(30, maker_side, better, 7)),
        // 2. 小于当前最优价格，直接 dust。
        quote_market_command(91, taker_side, (better - 1) as u128, &[]),
        // 3. 单 maker partial：成交 3，maker30 剩余 4。
        //    unused=1。
        quote_market_command(92, taker_side, partial_budget, &[9101]),
        // 4. 先挂较差档。
        Command::Place(order(20, maker_side, worse, 4)),
        // 5. 后挂同价 maker。
        Command::Place(order(10, maker_side, better, 5)),
        // 6. 价格优先 + 同价 FIFO：
        // maker30: 4
        // maker10: 5
        // maker20: 2
        // maker20 留下 2。
        quote_market_command(93, taker_side, multi_budget, &[9102, 9103, 9104]),
        // 7. 耗尽最后 2 lots，报价预算余量终结。
        quote_market_command(94, taker_side, 1000, &[9105]),
        // 8. 再次空簿。
        quote_market_command(95, taker_side, 100, &[]),
        // 9. 为重复 ID 准备 active maker。
        Command::Place(order(50, maker_side, worse, 2)),
        // 10. duplicate-first；没有 TradeId 也必须先返回业务拒绝。
        quote_market_command(50, taker_side, 1000, &[]),
    ]
}

#[test]
fn fixed_market_by_base_buy_and_sell_tapes_are_differential_and_reproducible() {
    for side in [Side::Buy, Side::Sell] {
        let tape = fixed_market_by_base_tape(side);
        let first = run_tape(&tape);
        let second = run_tape(&tape);
        assert_eq!(
            first, second,
            "MarketByBase fixed tape not reproducible: {side:?}"
        );
        assert_eq!(first.len(), 10);

        for &index in &[0usize, 2, 5, 6, 7] {
            assert!(matches!(
                &first[index].outcome,
                CommandOutcome::MarketByBase(Ok(_))
            ));
            let taker_id = match &tape[index] {
                Command::MarketByBase(order, _) => order.order_id,
                _ => unreachable!(),
            };
            assert!(
                first[index]
                    .book
                    .bids
                    .orders
                    .iter()
                    .chain(&first[index].book.asks.orders)
                    .all(|entry| entry.original.order_id != taker_id)
            );
            let previous_priority = if index == 0 {
                0
            } else {
                first[index - 1].production_next_priority
            };
            assert_eq!(first[index].production_next_priority, previous_priority);
        }
        let CommandOutcome::MarketByBase(Ok(partial)) = &first[2].outcome else {
            panic!("partial")
        };
        assert_eq!(partial.len(), 1);
        assert_eq!(partial[0].qty.get(), 3);
        assert_eq!(partial[0].trade_id, TradeId::new(9001));

        let CommandOutcome::MarketByBase(Ok(multi)) = &first[5].outcome else {
            panic!("multi")
        };
        assert_eq!(
            multi
                .iter()
                .map(|t| t.maker_order_id.get())
                .collect::<Vec<_>>(),
            vec![30, 10, 20]
        );
        assert_eq!(
            multi.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
            vec![4, 5, 2]
        );
        assert_eq!(
            multi.iter().map(|t| t.trade_id.get()).collect::<Vec<_>>(),
            vec![9002, 9003, 9004]
        );
        let (better, worse) = match side {
            Side::Buy => (105, 110),
            Side::Sell => (95, 90),
        };
        assert_eq!(
            multi.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
            vec![better, better, worse]
        );
        assert!(
            multi.iter().all(
                |t| t.taker_order_id == OrderId::new(92) && t.maker_side == opposite_side(side)
            )
        );

        // NOTE: maker20 原本 4，multi 吃掉 2，余 2；下一笔全部吃掉。
        let CommandOutcome::MarketByBase(Ok(exhaust)) = &first[6].outcome else {
            panic!("remainder")
        };
        assert_eq!(exhaust.len(), 1);
        assert_eq!(exhaust[0].qty.get(), 2);
        assert!(first[6].book.bids.orders.is_empty() && first[6].book.asks.orders.is_empty());
        assert!(
            matches!(&first[7].outcome, CommandOutcome::MarketByBase(Ok(trades)) if trades.is_empty())
        );
        assert_eq!(
            first[9].outcome,
            CommandOutcome::MarketByBase(Err(RejectReason::DuplicateOrderId))
        );
        assert_eq!(first[9].book, first[8].book);
        assert_eq!(
            first[9].production_next_priority,
            first[8].production_next_priority
        );
    }
}

fn t0632_market(
    id: u128,
    side: Side,
    qty: u64,
    market_protection: Option<MarketProtection>,
) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection,
    }
}

fn t0632_context(reference: i64, band: Option<i64>) -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(reference).unwrap(),
        market_band_worst: band.map(|v| Price::try_new(v).unwrap()),
    }
}

fn t0632_trade(
    trade_id: u128,
    maker_id: u128,
    taker_id: u128,
    price: i64,
    qty: u64,
    maker_side: Side,
) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(trade_id),
        maker_order_id: OrderId::new(maker_id),
        taker_order_id: OrderId::new(taker_id),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        maker_side,
    })
}

fn t0632_assert_protected_case(
    makers: &[(u128, Side, i64, u64)],
    incoming: MarketByBase,
    context: CommandStartProtectionContext,
    trade_ids: &[TradeId],
    expected: Result<Vec<EngineEvent>, RejectReason>,
) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    // 两套订单簿从同一组挂单独立初始化。
    for &(id, side, price, qty) in makers {
        let maker = order(id, side, price, qty);
        reference.rest(maker.clone()).unwrap();
        production.rest(maker).unwrap();
    }

    let reference_before = observe_reference(&reference);
    let production_before = observe_production(&production);
    assert_eq!(reference_before, production_before);

    // 复用已有完整 Production 失败快照体系。
    let bids_before = level_snapshot(&production.bids);
    let asks_before = level_snapshot(&production.asks);
    let orders_before = production.orders.clone();
    let arena_before = production.arena.test_snapshot();
    let priority_before = production.next_queue_priority();
    let policy_before = *production.queue_priority_allocator.policy();

    // 必须独立调用两个公开入口。
    let reference_result = reference.execute_market_by_base_with_protection(
        incoming.clone(),
        &context,
        &math,
        trade_ids,
    );

    let production_result = production.execute_market_by_base_with_protection(
        incoming.clone(),
        &context,
        &math,
        trade_ids,
    );

    // 完整 Result 比较：包含原序 Vec<EngineEvent> 和精确 RejectReason。
    assert_eq!(reference_result, production_result);

    // 不能只依靠两套实现互相对照。
    assert_eq!(reference_result, expected);

    // 对返回的事件再次检查排序和成交 ID。
    if let Ok(events) = &reference_result {
        let mut triggered = false;
        let mut seen_trade_ids = BTreeSet::new();

        for event in events {
            match event {
                EngineEvent::Trade(trade) => {
                    assert!(!triggered, "Trade appeared after protection event");
                    assert!(trade.qty.get() > 0);
                    assert!(seen_trade_ids.insert(trade.trade_id.get()));
                }
                EngineEvent::MarketProtectionTriggered { .. } => {
                    assert!(!triggered, "more than one protection event");
                    triggered = true;
                }
                other => panic!("unexpected protected event: {other:?}"),
            }
        }
    }

    // 事件比较之后，才观察并比较执行后订单簿。
    let reference_after = observe_reference(&reference);
    let production_after = observe_production(&production);

    assert_eq!(reference_after, production_after);

    assert_regular_book_invariants(&production_after, production.next_queue_priority(), 0);

    if expected.is_err() {
        // Reference：完整业务状态必须不变。
        assert_eq!(reference_after, reference_before);

        // Production：包括 BTreeMap、Arena、链表和 allocator。
        assert_eq!(production_after, production_before);

        assert_production_failure_state_unchanged(
            &production,
            &bids_before,
            &asks_before,
            &orders_before,
            &arena_before,
            priority_before,
            policy_before,
        );
    } else {
        // 市价 taker 不得挂簿。
        assert!(!reference_contains_order(&reference, incoming.order_id));
        assert!(production.order(incoming.order_id).is_none());
    }

    // 成功/拒绝均不得分配 QueuePriority。
    assert_eq!(production.next_queue_priority(), priority_before);
    assert_eq!(validate_book(&production), Ok(()));
}

#[test]
fn t0632_protected_multilevel_sources_are_differential() {
    let p = |v| Price::try_new(v).unwrap();

    let cases = [
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            110,
            ProtectionSource::User,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            110,
            100,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            100,
            ProtectionSource::Both,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            90,
            ProtectionSource::User,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            90,
            100,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            100,
            ProtectionSource::Both,
        ),
    ];

    for (side, maker_side, better, outside, farther, user, band, source) in cases {
        let makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        let incoming = t0632_market(
            100,
            side,
            20,
            Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            }),
        );

        let expected = vec![
            t0632_trade(701, 30, 100, better, 2, maker_side),
            t0632_trade(702, 40, 100, 100, 4, maker_side),
            t0632_trade(703, 10, 100, 100, 3, maker_side),
            EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source,
            },
        ];

        t0632_assert_protected_case(
            &makers,
            incoming,
            t0632_context(100_000, Some(band)),
            &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
            Ok(expected),
        );
    }
}

#[test]
fn t0632_protected_execution_boundaries_are_differential() {
    let p = |v| Price::try_new(v).unwrap();

    for (side, maker_side, better, outside, farther) in [
        (Side::Buy, Side::Sell, 90, 110, 120),
        (Side::Sell, Side::Buy, 110, 90, 80),
    ] {
        let bounded = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(100)),
        });

        let boundary_makers = [(1, maker_side, 100, 2), (2, maker_side, outside, 4)];

        // 1. 边界相等可成交；taker 恰好耗尽，不得触发保护。
        t0632_assert_protected_case(
            &boundary_makers,
            t0632_market(100, side, 2, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![t0632_trade(700, 1, 100, 100, 2, maker_side)]),
        );

        // 2. 边界成交后仍有余量，遇到下一档才触发保护。
        t0632_assert_protected_case(
            &boundary_makers,
            t0632_market(100, side, 3, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![
                t0632_trade(700, 1, 100, 100, 2, maker_side),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::Both,
                },
            ]),
        );

        // 3. 单 maker 部分成交，taker 耗尽，无 trigger。
        t0632_assert_protected_case(
            &[(1, maker_side, 100, 5)],
            t0632_market(100, side, 2, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![t0632_trade(700, 1, 100, 100, 2, maker_side)]),
        );

        // 4. 首档越界：零笔 Trade，零 TradeId。
        t0632_assert_protected_case(
            &[(1, maker_side, outside, 5)],
            t0632_market(100, side, 10, None),
            t0632_context(100_000, Some(100)),
            &[],
            Ok(vec![EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            }]),
        );

        // 5. 空簿：无 Trade，也无触发事件。
        t0632_assert_protected_case(
            &[],
            t0632_market(100, side, 10, bounded),
            t0632_context(100_000, Some(100)),
            &[],
            Ok(vec![]),
        );

        // 6. 无有效保护：扫描全部多档，包括边界以外的 maker。
        let all_makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        t0632_assert_protected_case(
            &all_makers,
            t0632_market(100, side, 20, None),
            t0632_context(100_000, None),
            &[
                TradeId::new(701),
                TradeId::new(702),
                TradeId::new(703),
                TradeId::new(704),
                TradeId::new(705),
            ],
            Ok(vec![
                t0632_trade(701, 30, 100, better, 2, maker_side),
                t0632_trade(702, 40, 100, 100, 4, maker_side),
                t0632_trade(703, 10, 100, 100, 3, maker_side),
                t0632_trade(704, 50, 100, outside, 1, maker_side),
                t0632_trade(705, 90, 100, farther, 2, maker_side),
            ]),
        );
    }
}

#[test]
fn t0632_protected_rejections_are_differential_and_atomic() {
    let p = |v| Price::try_new(v).unwrap();

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let makers = [(1, maker_side, 100_000, 5)];

        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        // Duplicate 优先于非法 bps。
        t0632_assert_protected_case(
            &makers,
            t0632_market(1, side, 5, Some(invalid_bps.clone())),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::DuplicateOrderId),
        );

        // 3007：即使 band 可用，非法 bps 也必须拒绝。
        t0632_assert_protected_case(
            &makers,
            t0632_market(100, side, 5, Some(invalid_bps)),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::InvalidSlippageBps),
        );

        // 显式 worst price 未按 tick=10 对齐。
        t0632_assert_protected_case(
            &makers,
            t0632_market(
                100,
                side,
                5,
                Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100_003)),
                }),
            ),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::InvalidTick),
        );
    }

    // Buy reference=i64::MAX，100bps 向上扩张，算术溢出。
    t0632_assert_protected_case(
        &[(1, Side::Sell, 100_000, 5)],
        t0632_market(
            100,
            Side::Buy,
            5,
            Some(MarketProtection {
                max_slippage_bps: Some(100),
                worst_price: None,
            }),
        ),
        t0632_context(i64::MAX, Some(100_000)),
        &[],
        Err(RejectReason::ArithmeticOverflow),
    );
}

#[test]
fn fixed_market_by_quote_buy_and_sell_tapes_are_differential_and_reproducible() {
    for side in [Side::Buy, Side::Sell] {
        let tape = fixed_market_by_quote_tape(side);

        // 每次 run_tape 都重新构造独立的两个空订单簿。
        let first = run_tape(&tape);
        let second = run_tape(&tape);

        // 比较完整 transcript，包括 outcome、整簿和 priority。
        assert_eq!(
            first, second,
            "MarketByQuote fixed tape not reproducible: {side:?}"
        );

        assert_eq!(first.len(), 11);

        // 所有成功的 Quote 命令均不得 rest taker，
        // 也不得消耗 priority。
        for &index in &[0usize, 2, 3, 6, 7, 8] {
            let CommandOutcome::MarketByQuote(Ok(_)) = &first[index].outcome else {
                panic!(
                    "MarketByQuote expected success: \
                     step={index}, side={side:?}"
                );
            };

            let taker_id = match &tape[index] {
                Command::MarketByQuote(order, _) => order.order_id,
                _ => unreachable!(),
            };

            assert!(
                first[index]
                    .book
                    .bids
                    .orders
                    .iter()
                    .chain(&first[index].book.asks.orders)
                    .all(|entry| entry.original.order_id != taker_id),
            );

            let priority_before = if index == 0 {
                0
            } else {
                first[index - 1].production_next_priority
            };

            assert_eq!(first[index].production_next_priority, priority_before,);
        }

        let (better, worse) = match side {
            Side::Buy => (105i64, 110i64),
            Side::Sell => (95i64, 90i64),
        };

        // Empty + initial dust。
        for (index, budget) in [(0usize, 1000u128), (2usize, (better - 1) as u128)] {
            let CommandOutcome::MarketByQuote(Ok(execution)) = &first[index].outcome else {
                panic!("expected empty/dust quote result");
            };

            assert!(execution.trades().is_empty());
            assert_eq!(execution.spent_quote().get(), 0);
            assert_eq!(execution.unused_quote().get(), budget);
        }

        // Partial maker。
        let CommandOutcome::MarketByQuote(Ok(partial)) = &first[3].outcome else {
            panic!("MarketByQuote partial expected");
        };

        assert_eq!(partial.trades().len(), 1);
        assert_eq!(partial.trades()[0].maker_order_id, OrderId::new(30));
        assert_eq!(partial.trades()[0].trade_id, TradeId::new(9101));
        assert_eq!(partial.trades()[0].qty.get(), 3);
        assert_eq!(partial.trades()[0].price.get(), better);
        assert_eq!(partial.spent_quote().get(), (3 * better) as u128);
        assert_eq!(partial.unused_quote().get(), 1);

        let remaining_side = match side {
            Side::Buy => &first[3].book.asks,
            Side::Sell => &first[3].book.bids,
        };

        assert_eq!(remaining_side.orders.len(), 1);
        assert_eq!(remaining_side.orders[0].remaining, 4);

        // 同价 FIFO + 多档成交。
        let CommandOutcome::MarketByQuote(Ok(multi)) = &first[6].outcome else {
            panic!("MarketByQuote multi-level expected");
        };

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.maker_order_id.get())
                .collect::<Vec<_>>(),
            vec![30, 10, 20],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.qty.get())
                .collect::<Vec<_>>(),
            vec![4, 5, 2],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.price.get())
                .collect::<Vec<_>>(),
            vec![better, better, worse],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.trade_id.get())
                .collect::<Vec<_>>(),
            vec![9102, 9103, 9104],
        );

        assert!(multi.trades().iter().all(|trade| {
            trade.taker_order_id == OrderId::new(93) && trade.maker_side == opposite_side(side)
        }));

        let expected_multi_spent = (9 * better + 2 * worse) as u128;

        assert_eq!(multi.spent_quote().get(), expected_multi_spent);
        assert_eq!(multi.unused_quote().get(), 0);

        let remaining_side = match side {
            Side::Buy => &first[6].book.asks,
            Side::Sell => &first[6].book.bids,
        };

        assert_eq!(remaining_side.orders.len(), 1);
        assert_eq!(remaining_side.orders[0].original.order_id, OrderId::new(20),);
        assert_eq!(remaining_side.orders[0].remaining, 2);

        // 最后两个 lots 全部成交，额外预算终结。
        let CommandOutcome::MarketByQuote(Ok(exhaust)) = &first[7].outcome else {
            panic!("MarketByQuote liquidity exhaustion expected");
        };

        assert_eq!(exhaust.trades().len(), 1);
        assert_eq!(exhaust.trades()[0].maker_order_id, OrderId::new(20));
        assert_eq!(exhaust.trades()[0].qty.get(), 2);
        assert_eq!(exhaust.trades()[0].trade_id, TradeId::new(9105));
        assert_eq!(exhaust.spent_quote().get(), (2 * worse) as u128);
        assert_eq!(exhaust.unused_quote().get(), 1000 - (2 * worse) as u128,);

        assert!(first[7].book.bids.orders.is_empty());
        assert!(first[7].book.asks.orders.is_empty());

        // 流动性耗尽后再次发单：空成交。
        let CommandOutcome::MarketByQuote(Ok(empty)) = &first[8].outcome else {
            panic!("MarketByQuote empty after exhaustion expected");
        };

        assert!(empty.trades().is_empty());
        assert_eq!(empty.spent_quote().get(), 0);
        assert_eq!(empty.unused_quote().get(), 100);

        // Duplicate 拒绝且不修改语义簿和 priority。
        assert_eq!(
            first[10].outcome,
            CommandOutcome::MarketByQuote(Err(RejectReason::DuplicateOrderId)),
        );

        assert_eq!(first[10].book, first[9].book);
        assert_eq!(
            first[10].production_next_priority,
            first[9].production_next_priority,
        );
    }
}

// MarketByQuote 跨 tick/lot/price/qty/budget 的预算守恒差分。
//
// 仅生成成功路径：
// - 合法且对齐的 maker price/qty；
// - 正 QuoteBudget；
// - 无重复 OrderId；
// - 三个预分配 TradeId；
// - Reference / Production 独立构造与执行。
//
// 不依赖固定 tape 的逐笔预期表。
proptest! {
    #[test]
    fn market_by_quote_budget_conservation_property(
        is_buy in any::<bool>(),
        tick_size in 1i64..=5,
        lot_size in 1u64..=10,
        price_unit in 2i64..=100,
        maker30_lots in 2u64..=10,
        maker10_lots in 1u64..=10,
        maker20_lots in 1u64..=10,
        budget_case in 0u8..6,
        random_budget in 1u128..=100_000,
    ) {
        // 1. 构造对齐的价格和数量。
        let taker_side = if is_buy {
            Side::Buy
        } else {
            Side::Sell
        };
        let maker_side = opposite_side(taker_side);

        let (better_units, worse_units) = match taker_side {
            Side::Buy => (price_unit, price_unit + 1),
            Side::Sell => (price_unit + 1, price_unit),
        };

        let better_price = better_units
            .checked_mul(tick_size)
            .expect("better price overflow");
        let worse_price = worse_units
            .checked_mul(tick_size)
            .expect("worse price overflow");

        let qty30 = maker30_lots
            .checked_mul(lot_size)
            .expect("maker30 qty overflow");
        let qty10 = maker10_lots
            .checked_mul(lot_size)
            .expect("maker10 qty overflow");
        let qty20 = maker20_lots
            .checked_mul(lot_size)
            .expect("maker20 qty overflow");

        // 2. 用对齐的价格和 lot 推导预算边界。
        // 本 property 固定零 scale，使用整数边界；
        // 不复制 MarketMath 的通用 quote-to-qty 实现。
        let best_one_lot = (better_price as u128)
            .checked_mul(u128::from(lot_size))
            .expect("best one-lot notional overflow");

        let worse_one_lot = (worse_price as u128)
            .checked_mul(u128::from(lot_size))
            .expect("worse one-lot notional overflow");

        let better_total_lots = maker30_lots
            .checked_add(maker10_lots)
            .expect("better total lots overflow");

        let better_total = best_one_lot
            .checked_mul(u128::from(better_total_lots))
            .expect("better total notional overflow");

        let worse_total = worse_one_lot
            .checked_mul(u128::from(maker20_lots))
            .expect("worse total notional overflow");

        let total_notional = better_total
            .checked_add(worse_total)
            .expect("total notional overflow");

        let budget = match budget_case {
            0 => best_one_lot - 1,
            1 => best_one_lot,
            2 => best_one_lot
                .checked_mul(u128::from(maker30_lots - 1))
                .and_then(|value| value.checked_add(1))
                .expect("partial budget overflow"),
            3 => better_total
                .checked_add(worse_one_lot)
                .expect("multi-level budget overflow"),
            4 => total_notional
                .checked_add(1)
                .expect("exhaustion budget overflow"),
            _ => random_budget,
        };

        prop_assert!(budget > 0);

        // 3. 独立创建两套订单簿。
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());

        // 挂单顺序：
        // maker20：较差档
        // maker30：较优档 FIFO head
        // maker10：较优档 FIFO tail
        let makers = [
            order(20, maker_side, worse_price, qty20),
            order(30, maker_side, better_price, qty30),
            order(10, maker_side, better_price, qty10),
        ];

        for maker in makers {
            reference
                .rest(maker.clone())
                .expect("valid Reference maker rest");

            production
                .rest(maker)
                .expect("valid Production maker rest");
        }

        // 4. 构造相同的 MarketMath 和 MarketByQuote。
        let math = MarketMath::try_new(
            0,
            0,
            0,
            tick_size,
            lot_size,
        )
        .expect("generated MarketMath must be valid");

        let quote_budget = QuoteBudget::try_new(budget)
            .expect("generated budget must be positive");

        let taker = MarketByQuote {
            order_id: OrderId::new(100),
            user_id: UserId::new(100),
            side: taker_side,
            quote_budget,
            market_protection:None
        };

        // 每个 maker 最多产生一笔成交。
        let trade_ids = [
            TradeId::new(9001),
            TradeId::new(9002),
            TradeId::new(9003),
        ];

        // 5. 执行前检查两套簿一致，并记录 priority。
        prop_assert_eq!(
            observe_reference(&reference),
            observe_production(&production),
        );

        let priority_before = production.next_queue_priority();

        // 6. 两套公开入口独立执行。
        let reference_execution = reference
            .execute_market_by_quote(
                taker.clone(),
                &math,
                &trade_ids,
            )
            .expect("valid Reference MarketByQuote must succeed");

        let production_execution = production
            .execute_market_by_quote(
                taker.clone(),
                &math,
                &trade_ids,
            )
            .expect("valid Production MarketByQuote must succeed");

        // 7. 完整 execution 对比。
        // 包含原序 trades、spent_quote、unused_quote。
        prop_assert_eq!(
            &reference_execution,
            &production_execution,
        );

        // 8. 预算守恒。
        let spent = production_execution.spent_quote().get();
        let unused = production_execution.unused_quote().get();

        prop_assert!(spent <= budget);

        let recovered_budget = spent
            .checked_add(unused)
            .expect("spent + unused must not overflow");

        prop_assert_eq!(recovered_budget, budget);

        // 9. 成交必须严格正量。
        for trade in production_execution.trades() {
            prop_assert!(trade.qty.get() > 0);
        }

        // 10. 无成交时预算全部返还。
        if production_execution.trades().is_empty() {
            prop_assert_eq!(spent, 0);
            prop_assert_eq!(unused, budget);
        }

        // 11. Reference / Production 整簿有序观察一致。
        // 同时比较 orders、remaining、FIFO、depth、best_price。
        prop_assert_eq!(
            observe_reference(&reference),
            observe_production(&production),
        );

        // 12. 市价 taker 不得挂簿。
        prop_assert!(
            !reference_contains_order(&reference, taker.order_id)
        );

        prop_assert!(
            production.order(taker.order_id).is_none()
        );

        // 13. Quote 市价成交不得分配 QueuePriority。
        prop_assert_eq!(
            production.next_queue_priority(),
            priority_before,
        );

        // 14. Production 内部结构完整性。
        prop_assert_eq!(validate_book(&production), Ok(()));
    }
}

#[test]
fn fixed_buy_taker_tape_is_differential_and_reproducible() {
    let tape = buy_taker_tape();

    // ------------------------------------------------------------
    // 同一固定 tape 必须在独立空簿上重复得到完全相同 transcript。
    // ------------------------------------------------------------

    let first = run_tape(&tape);
    let second = run_tape(&tape);

    assert_eq!(
        first, second,
        "same fixed Buy-taker tape must be fully reproducible"
    );

    assert_eq!(first.len(), 8);

    // ============================================================
    // Step 0:
    // Sell30@105 x3
    //
    // 无成交但存在 remainder，因此必须 rest。
    // ============================================================

    assert_eq!(first[0].production_next_priority, 1);

    assert_eq!(first[0].book.asks.orders.len(), 1);

    assert_eq!(
        first[0].book.asks.orders[0].original.order_id,
        OrderId::new(30)
    );

    assert_eq!(first[0].book.asks.orders[0].remaining, 3);

    assert_eq!(first[0].book.asks.orders[0].priority, QueuePriority::new(0));

    // ============================================================
    // Step 1:
    // Sell10@105 x7
    //
    // 同价 FIFO：
    // 30 -> 10
    // ============================================================

    assert_eq!(first[1].production_next_priority, 2);

    assert_eq!(
        first[1]
            .book
            .asks
            .orders
            .iter()
            .map(|entry| entry.original.order_id)
            .collect::<Vec<_>>(),
        vec![OrderId::new(30), OrderId::new(10)]
    );

    assert_eq!(
        first[1].book.asks.depth,
        vec![ObservedDepth {
            price: Price::try_new(105).unwrap(),
            order_count: 2,
            total_qty: 10,
        }]
    );

    assert_eq!(
        first[1].book.asks.best_price,
        Some(Price::try_new(105).unwrap())
    );

    // ============================================================
    // Step 2:
    // Buy100@110 x8
    //
    // 30 -> 10
    // fill 3 -> 5
    //
    // incoming exact fill，所以：
    //
    // - Buy100 不挂入；
    // - priority 仍然是 2。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[2].outcome else {
        panic!("Buy100 must succeed");
    };

    assert_eq!(trades.len(), 2);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(1),
            OrderId::new(30),
            OrderId::new(100),
            Price::try_new(105).unwrap(),
            Qty::try_new(3).unwrap(),
            Side::Sell,
        )
    );

    assert_eq!(
        (
            trades[1].trade_id,
            trades[1].maker_order_id,
            trades[1].taker_order_id,
            trades[1].price,
            trades[1].qty,
            trades[1].maker_side,
        ),
        (
            TradeId::new(2),
            OrderId::new(10),
            OrderId::new(100),
            Price::try_new(105).unwrap(),
            Qty::try_new(5).unwrap(),
            Side::Sell,
        )
    );

    // maker10 原 qty=7，当前 remaining=2。
    assert_eq!(first[2].book.asks.orders.len(), 1);

    assert_eq!(
        first[2].book.asks.orders[0].original.order_id,
        OrderId::new(10)
    );

    assert_eq!(
        first[2].book.asks.orders[0].original.qty,
        Qty::try_new(7).unwrap()
    );

    assert_eq!(first[2].book.asks.orders[0].remaining, 2);

    assert_eq!(first[2].book.asks.orders[0].priority, QueuePriority::new(1));

    assert!(first[2].book.bids.orders.is_empty());

    // exact fill 不允许消耗新 priority。
    assert_eq!(first[2].production_next_priority, 2);

    // ============================================================
    // Step 3:
    // Buy101@110 x5
    //
    // maker10 剩余2：
    //   fill 2
    //
    // taker 剩余3：
    //   residual Buy101@110 x3
    //
    // residual payload 的 qty 必须是真实余量3，而不是原始5。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[3].outcome else {
        panic!("Buy101 must succeed");
    };

    assert_eq!(trades.len(), 1);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(3),
            OrderId::new(10),
            OrderId::new(101),
            Price::try_new(105).unwrap(),
            Qty::try_new(2).unwrap(),
            Side::Sell,
        )
    );

    assert!(first[3].book.asks.orders.is_empty());

    assert_eq!(first[3].book.bids.orders.len(), 1);

    let resting_101 = &first[3].book.bids.orders[0];

    assert_eq!(resting_101.original.order_id, OrderId::new(101));

    assert_eq!(resting_101.original.side, Side::Buy);

    assert_eq!(resting_101.original.price, Price::try_new(110).unwrap());

    // 关键：payload 本身就是 residual qty。
    assert_eq!(resting_101.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(resting_101.remaining, 3);

    assert_eq!(resting_101.priority, QueuePriority::new(2));

    assert_eq!(
        first[3].book.bids.depth,
        vec![ObservedDepth {
            price: Price::try_new(110).unwrap(),
            order_count: 1,
            total_qty: 3,
        }]
    );

    assert_eq!(
        first[3].book.bids.best_price,
        Some(Price::try_new(110).unwrap())
    );

    assert_eq!(first[3].book.asks.best_price, None);

    assert_eq!(first[3].production_next_priority, 3);

    // ============================================================
    // Step 4:
    // duplicate 101。
    //
    // 候选本身 Sell@100，本来会与 Buy101@110 crossing。
    //
    // 必须优先 DuplicateOrderId，且完整状态不变。
    // ============================================================

    assert_eq!(
        first[4].outcome,
        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
    );

    assert_eq!(first[4].book, first[3].book);

    assert_eq!(first[4].production_next_priority, 3);

    // ============================================================
    // Step 5:
    // Cancel101 成功。
    // ============================================================

    let CommandOutcome::Cancel(Ok(removed)) = &first[5].outcome else {
        panic!("Cancel101 must succeed");
    };

    assert_eq!(removed.original.order_id, OrderId::new(101));

    assert_eq!(removed.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(removed.remaining, 3);

    assert_eq!(removed.priority, QueuePriority::new(2));

    assert!(first[5].book.bids.orders.is_empty());
    assert!(first[5].book.asks.orders.is_empty());

    // Cancel 不消耗 priority。
    assert_eq!(first[5].production_next_priority, 3);

    // ============================================================
    // Step 6:
    // repeated Cancel -> OrderNotFound，状态不变。
    // ============================================================

    assert_eq!(
        first[6].outcome,
        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
    );

    assert_eq!(first[6].book, first[5].book);

    assert_eq!(first[6].production_next_priority, 3);

    // ============================================================
    // Step 7:
    // Cancel 后重新使用 OrderId=101。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[7].outcome else {
        panic!("reused OrderId 101 Place must succeed");
    };

    assert!(trades.is_empty());

    assert!(first[7].book.bids.orders.is_empty());

    assert_eq!(first[7].book.asks.orders.len(), 1);

    let reused_101 = &first[7].book.asks.orders[0];

    assert_eq!(reused_101.original.order_id, OrderId::new(101));

    assert_eq!(reused_101.original.side, Side::Sell);

    assert_eq!(reused_101.original.price, Price::try_new(120).unwrap());

    assert_eq!(reused_101.original.qty, Qty::try_new(4).unwrap());

    assert_eq!(reused_101.remaining, 4);

    // 原 101 priority=2 已被取消；
    // ID 可以复用，但 priority 不能复用。
    assert_eq!(reused_101.priority, QueuePriority::new(3));

    assert_eq!(
        first[7].book.asks.best_price,
        Some(Price::try_new(120).unwrap())
    );

    assert_eq!(first[7].production_next_priority, 4);
}

#[test]
fn fixed_sell_taker_mirror_tape_is_differential_and_reproducible() {
    let tape = sell_taker_tape();

    let first = run_tape(&tape);
    let second = run_tape(&tape);

    assert_eq!(
        first, second,
        "same fixed Sell-taker tape must be fully reproducible"
    );

    assert_eq!(first.len(), 8);

    // ============================================================
    // 初始两个 Buy maker 都正常 rest。
    // ============================================================

    assert_eq!(first[0].production_next_priority, 1);
    assert_eq!(first[1].production_next_priority, 2);

    assert_eq!(
        first[1]
            .book
            .bids
            .orders
            .iter()
            .map(|entry| entry.original.order_id)
            .collect::<Vec<_>>(),
        vec![OrderId::new(30), OrderId::new(10)]
    );

    assert_eq!(
        first[1].book.bids.depth,
        vec![ObservedDepth {
            price: Price::try_new(95).unwrap(),
            order_count: 2,
            total_qty: 10,
        }]
    );

    assert_eq!(
        first[1].book.bids.best_price,
        Some(Price::try_new(95).unwrap())
    );

    // ============================================================
    // Step 2:
    // Sell100@90 x8
    //
    // Buy30 -> Buy10
    // fill 3 -> 5
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[2].outcome else {
        panic!("Sell100 must succeed");
    };

    assert_eq!(trades.len(), 2);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(1),
            OrderId::new(30),
            OrderId::new(100),
            Price::try_new(95).unwrap(),
            Qty::try_new(3).unwrap(),
            Side::Buy,
        )
    );

    assert_eq!(
        (
            trades[1].trade_id,
            trades[1].maker_order_id,
            trades[1].taker_order_id,
            trades[1].price,
            trades[1].qty,
            trades[1].maker_side,
        ),
        (
            TradeId::new(2),
            OrderId::new(10),
            OrderId::new(100),
            Price::try_new(95).unwrap(),
            Qty::try_new(5).unwrap(),
            Side::Buy,
        )
    );

    assert_eq!(first[2].book.bids.orders.len(), 1);

    assert_eq!(
        first[2].book.bids.orders[0].original.order_id,
        OrderId::new(10)
    );

    assert_eq!(first[2].book.bids.orders[0].remaining, 2);

    // exact fill 不消耗 priority。
    assert_eq!(first[2].production_next_priority, 2);

    // ============================================================
    // Step 3:
    // Sell101@90 x5
    //
    // maker10 fill2
    // residual Sell101@90 x3
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[3].outcome else {
        panic!("Sell101 must succeed");
    };

    assert_eq!(trades.len(), 1);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].taker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(3),
            OrderId::new(10),
            OrderId::new(101),
            Price::try_new(95).unwrap(),
            Qty::try_new(2).unwrap(),
            Side::Buy,
        )
    );

    assert!(first[3].book.bids.orders.is_empty());

    assert_eq!(first[3].book.asks.orders.len(), 1);

    let resting_101 = &first[3].book.asks.orders[0];

    assert_eq!(resting_101.original.order_id, OrderId::new(101));

    assert_eq!(resting_101.original.side, Side::Sell);

    assert_eq!(resting_101.original.price, Price::try_new(90).unwrap());

    assert_eq!(resting_101.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(resting_101.remaining, 3);

    assert_eq!(resting_101.priority, QueuePriority::new(2));

    assert_eq!(
        first[3].book.asks.depth,
        vec![ObservedDepth {
            price: Price::try_new(90).unwrap(),
            order_count: 1,
            total_qty: 3,
        }]
    );

    assert_eq!(
        first[3].book.asks.best_price,
        Some(Price::try_new(90).unwrap())
    );

    assert_eq!(first[3].production_next_priority, 3);

    // ============================================================
    // Step 4:
    // duplicate Sell101 已 active。
    //
    // 候选 Buy101@100 本来会 crossing，
    // 仍必须先返回 DuplicateOrderId。
    // ============================================================

    assert_eq!(
        first[4].outcome,
        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
    );

    assert_eq!(first[4].book, first[3].book);

    assert_eq!(first[4].production_next_priority, 3);

    // ============================================================
    // Step 5 Cancel101。
    // ============================================================

    let CommandOutcome::Cancel(Ok(removed)) = &first[5].outcome else {
        panic!("mirror Cancel101 must succeed");
    };

    assert_eq!(removed.original.order_id, OrderId::new(101));

    assert_eq!(removed.original.qty, Qty::try_new(3).unwrap());

    assert_eq!(removed.remaining, 3);

    assert_eq!(removed.priority, QueuePriority::new(2));

    assert!(first[5].book.bids.orders.is_empty());
    assert!(first[5].book.asks.orders.is_empty());

    assert_eq!(first[5].production_next_priority, 3);

    // ============================================================
    // Step 6 repeated cancel。
    // ============================================================

    assert_eq!(
        first[6].outcome,
        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
    );

    assert_eq!(first[6].book, first[5].book);

    assert_eq!(first[6].production_next_priority, 3);

    // ============================================================
    // Step 7 ID reuse。
    // ============================================================

    let CommandOutcome::Place(Ok(trades)) = &first[7].outcome else {
        panic!("mirror reused OrderId 101 Place must succeed");
    };

    assert!(trades.is_empty());

    assert_eq!(first[7].book.bids.orders.len(), 1);

    assert!(first[7].book.asks.orders.is_empty());

    let reused_101 = &first[7].book.bids.orders[0];

    assert_eq!(reused_101.original.order_id, OrderId::new(101));

    assert_eq!(reused_101.original.side, Side::Buy);

    assert_eq!(reused_101.original.price, Price::try_new(80).unwrap());

    assert_eq!(reused_101.original.qty, Qty::try_new(4).unwrap());

    assert_eq!(reused_101.remaining, 4);

    assert_eq!(reused_101.priority, QueuePriority::new(3));

    assert_eq!(
        first[7].book.bids.best_price,
        Some(Price::try_new(80).unwrap())
    );

    assert_eq!(first[7].production_next_priority, 4);
}

/// 空 tape 和原有 1k smoke 长度都属于允许域；重复取消不存在 ID 是合法业务输入。
#[test]
fn tape_domain_accepts_empty_and_1000_smoke_length() {
    assert_eq!(validate_tape_domain(&[]), Ok(()));
    assert!(run_tape(&[]).is_empty());

    let tape: Vec<_> = (0..1000)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

#[test]
fn tape_domain_accepts_all_field_boundaries() {
    let tape = vec![
        Command::Place(order(1, Side::Buy, 80, 1)),
        Command::Place(order(128, Side::Sell, 120, 100)),
        Command::Cancel(OrderId::new(1)),
        Command::Cancel(OrderId::new(128)),
    ];
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

#[test]
fn tape_domain_rejects_overlong_tape() {
    let tape: Vec<_> = (0..100_001)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    let error = validate_tape_domain(&tape).unwrap_err();
    assert!(error.contains("length=100001"), "{error}");
    assert!(error.contains("0..=100000"), "{error}");
}

#[test]
fn tape_domain_rejects_out_of_range_fields_with_command_index() {
    let cases = [
        (Command::Cancel(OrderId::new(0)), "OrderId=0", "1..=128"),
        (Command::Cancel(OrderId::new(129)), "OrderId=129", "1..=128"),
        (
            Command::Place(order(0, Side::Buy, 100, 1)),
            "OrderId=0",
            "1..=128",
        ),
        (
            Command::Place(order(129, Side::Buy, 100, 1)),
            "OrderId=129",
            "1..=128",
        ),
        (
            Command::Place(order(1, Side::Buy, 79, 1)),
            "price=79",
            "80..=120",
        ),
        (
            Command::Place(order(1, Side::Buy, 121, 1)),
            "price=121",
            "80..=120",
        ),
        (
            Command::Place(order(1, Side::Buy, 100, 101)),
            "qty=101",
            "1..=100",
        ),
    ];

    for (invalid, field, range) in cases {
        let tape = [Command::Cancel(OrderId::new(1)), invalid];
        let error = validate_tape_domain(&tape).unwrap_err();
        assert!(error.contains("command[1]"), "{error}");
        assert!(error.contains(field), "{error}");
        assert!(error.contains(range), "{error}");
    }
}

/// 同一条 Place 多字段非法时，只报告最高优先级的 OrderId 错误。
#[test]
fn tape_domain_prioritizes_order_id_over_price_and_qty() {
    let tape = [Command::Place(order(0, Side::Buy, 121, 101))];
    let error = validate_tape_domain(&tape).unwrap_err();

    assert!(error.contains("command[0]"), "{error}");
    assert!(error.contains("OrderId=0"), "{error}");
    assert!(error.contains("1..=128"), "{error}");
    assert!(!error.contains("price="), "{error}");
    assert!(!error.contains("qty="), "{error}");
}

#[test]
#[should_panic(expected = "invalid differential tape domain: command[0] Place price=121")]
fn run_tape_rejects_out_of_domain_input_before_execution() {
    let _ = run_tape(&[Command::Place(order(1, Side::Buy, 121, 1))]);
}

/// 两种包装器必须共用执行循环，且各自处理所有短 tape 命令。
#[test]
fn collected_and_streamed_short_tape_process_every_step() {
    let tape = buy_taker_tape();
    assert_eq!(run_tape(&tape).len(), tape.len());
    assert_eq!(run_tape_streaming(&tape), tape.len());
}

#[test]
fn streamed_fixed_seed_1000_tape_processes_every_step() {
    const SEED: [u8; 32] = [0x05; 32];
    let seed_hex = SEED
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "generator={}, algorithm=ChaCha, seed=0x{}, length=1000",
        TAPE_GENERATOR_VERSION, seed_hex
    );
    let tape = generate_tape(SEED, 1000);
    assert_eq!(run_tape_streaming(&tape), 1000);
}

/// 空 regular book 也必须独立通过只读 invariant helper。
#[test]
fn empty_regular_book_passes_invariant_helper() {
    let production = ProductionOrderBook::new(policy());
    let observed = observe_production(&production);

    assert_regular_book_invariants(&observed, production.next_queue_priority(), 0);
}

#[test]
fn empty_tape_streaming_returns_zero() {
    assert_eq!(run_tape_streaming(&[]), 0);
    assert!(run_tape(&[]).is_empty());
}

#[test]
fn fixed_seed_generator_is_reproducible_and_bounded() {
    let seed = [0x05; 32];
    assert!(generate_tape(seed, 0).is_empty());
    let first = generate_tape(seed, 1000);
    let second = generate_tape(seed, 1000);
    assert_eq!(first, second);
    assert_eq!(validate_tape_domain(&first), Ok(()));
}

#[test]
#[should_panic(expected = "generate_tape length=100001 outside allowed range 0..=100000")]
fn generated_tape_rejects_overlong_length() {
    let _ = generate_tape([0x05; 32], 100_001);
}

/// T05.4：新的最大长度在输入域中合法；仍只验证输入，不回放该 tape。
#[test]
fn tape_domain_accepts_100k_boundary() {
    let tape: Vec<_> = (0..MAX_TAPE_LENGTH)
        .map(|_| Command::Cancel(OrderId::new(1)))
        .collect();
    assert_eq!(tape.len(), 100_000);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

/// T05.4：生成器必须先通过长度门禁，之后才允许初始化 RNG / 分配 Vec。
#[test]
fn generated_tape_accepts_100k_boundary() {
    let tape = generate_tape([0x05; 32], MAX_TAPE_LENGTH);
    assert_eq!(tape.len(), 100_000);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
}

/// T05.4：固定 seed 的 100k 单条 tape，逐命令检查但不保留 StepRecord。
#[test]
fn streamed_fixed_seed_100k_tape_processes_every_step() {
    const SEED: [u8; 32] = [0x05; 32];
    const LENGTH: usize = 100_000;

    let seed_hex = SEED
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "generator={}, algorithm=ChaCha, seed=0x{}, length={}",
        TAPE_GENERATOR_VERSION, seed_hex, LENGTH
    );

    let tape = generate_tape(SEED, LENGTH);
    assert_eq!(validate_tape_domain(&tape), Ok(()));
    assert_eq!(run_tape_streaming(&tape), LENGTH);
}

/// T05.5 两个具名固定 seed；每个独立生成两次并完整比较输入。
/// 所有 §54 声明的覆盖范围见 assert_regular_book_invariants 注释。
/// 这不是 Stop / expiry / WAL / digest / crash / 跨 shard 断言。
fn assert_reproducible_streamed_10k(name: &str, seed: [u8; 32]) {
    const LENGTH: usize = 10_000;
    let seed_hex = seed
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "name={name}, generator={}, algorithm=ChaCha, seed=0x{}, length={LENGTH}",
        TAPE_GENERATOR_VERSION, seed_hex
    );
    let first = generate_tape(seed, LENGTH);
    let second = generate_tape(seed, LENGTH);
    assert_eq!(first, second, "generated tape differs for seed {name}");
    assert_eq!(validate_tape_domain(&first), Ok(()));
    assert_eq!(validate_tape_domain(&second), Ok(()));
    assert_eq!(run_tape_streaming(&first), LENGTH);
    assert_eq!(run_tape_streaming(&second), LENGTH);
}

#[test]
fn streamed_fixed_seed_10k_alpha_is_reproducible() {
    assert_reproducible_streamed_10k("alpha", [0x0a; 32]);
}

#[test]
fn streamed_fixed_seed_10k_beta_is_reproducible() {
    assert_reproducible_streamed_10k("beta", [0x0b; 32]);
}

#[cfg(test)]
mod t0638_protected_quote_tests {
    use super::*;
    use matching_domain::market_order::{
        CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
    };
    use matching_domain::{EngineEvent, MarketProtection, ProtectionSource, QuoteAmount};
    fn quote(
        id: u128,
        side: Side,
        budget: u128,
        protection: Option<MarketProtection>,
    ) -> MarketByQuote {
        MarketByQuote {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            quote_budget: QuoteBudget::try_new(budget).unwrap(),
            market_protection: protection,
        }
    }

    fn context(band: Option<i64>) -> CommandStartProtectionContext {
        CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100_003).unwrap(),
            market_band_worst: band.map(|p| Price::try_new(p).unwrap()),
        }
    }

    fn trade(tid: u128, maker_id: u128, price: i64, qty: u64, maker_side: Side) -> EngineEvent {
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId::new(tid),
            maker_order_id: OrderId::new(maker_id),
            taker_order_id: OrderId::new(100),
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
            maker_side,
        })
    }
    fn run_case(
        makers: &[(u128, Side, i64, u64)],
        incoming: &MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> (
        Result<ProtectedMarketByQuoteExecution, RejectReason>,
        ObservedBook,
    ) {
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());

        // ============================================================
        // 1. 分别构建 Reference / Production 订单簿
        // ============================================================

        for &(id, side, price, qty) in makers {
            let maker = order(id, side, price, qty);

            assert_eq!(reference.rest(maker.clone()), Ok(()));
            assert_eq!(production.rest(maker), Ok(()));
        }

        // ============================================================
        // 2. 执行前的完整业务状态
        // ============================================================

        let reference_before = observe_reference(&reference);
        let production_before = observe_production(&production);

        assert_eq!(
            reference_before, production_before,
            "initial protected Quote book state diverged"
        );

        assert_eq!(validate_book(&production), Ok(()));

        // ============================================================
        // 3. 保存 Production 完整物理状态
        // ============================================================

        let bids_before = level_snapshot(&production.bids);
        let asks_before = level_snapshot(&production.asks);

        let orders_before = production.orders.clone();
        let arena_before = production.arena.test_snapshot();

        let next_before = production.next_queue_priority();

        let policy_before = *production.queue_priority_allocator.policy();

        // ============================================================
        // 4. 两套引擎独立执行公开受保护 Quote 入口
        // ============================================================

        let reference_result = reference.execute_market_by_quote_with_protection(
            incoming.clone(),
            context,
            math,
            trade_ids,
        );

        let production_result = production.execute_market_by_quote_with_protection(
            incoming.clone(),
            context,
            math,
            trade_ids,
        );

        // 完整比较：
        // - Result / RejectReason
        // - Vec<EngineEvent> 原始事件顺序
        // - spent_quote
        // - unused_quote
        assert_eq!(
            reference_result, production_result,
            "protected MarketByQuote result diverged"
        );

        // ============================================================
        // 5. 执行后完整业务观察
        // ============================================================

        let reference_after = observe_reference(&reference);
        let production_after = observe_production(&production);

        // 订单、remaining、FIFO、depth、best price、priority
        // 必须完全一致。
        assert_eq!(
            reference_after, production_after,
            "protected MarketByQuote book state diverged"
        );

        // 市价订单不能分配新的 QueuePriority。
        assert_eq!(
            production.next_queue_priority(),
            next_before,
            "protected MarketByQuote consumed QueuePriority"
        );

        assert_regular_book_invariants(&production_after, production.next_queue_priority(), 0);

        // ============================================================
        // 6. 按 Reference 的真实执行结果分支
        // ============================================================

        match reference_result.as_ref() {
            // --------------------------------------------------------
            // Err：两套订单簿必须保持失败原子性
            // --------------------------------------------------------
            Err(_) => {
                // Reference 完整业务状态不变。
                assert_eq!(
                    reference_after, reference_before,
                    "failed protected Quote mutated Reference"
                );

                // Production 完整业务状态不变。
                assert_eq!(
                    production_after, production_before,
                    "failed protected Quote mutated Production"
                );

                // Production 物理状态不变：
                // - bids / asks BTreeMap
                // - PriceLevel 链表和聚合
                // - OrderId index
                // - Arena slots / free-list
                // - QueuePriority next
                // - allocator policy
                assert_production_failure_state_unchanged(
                    &production,
                    &bids_before,
                    &asks_before,
                    &orders_before,
                    &arena_before,
                    next_before,
                    policy_before,
                );
            }

            // --------------------------------------------------------
            // Ok：直接使用模式匹配绑定的 execution
            // --------------------------------------------------------
            Ok(execution) => {
                // 市价 taker 不允许挂簿。
                assert!(
                    !reference_contains_order(&reference, incoming.order_id,),
                    "protected Quote taker rested in Reference"
                );

                assert!(
                    production.order(incoming.order_id).is_none(),
                    "protected Quote taker rested in Production"
                );

                // 验证 TradeId 唯一性及保护终结事件顺序。
                let mut seen_trade_ids = BTreeSet::new();
                let mut seen_trigger = false;

                for (index, event) in execution.events().iter().enumerate() {
                    match event {
                        EngineEvent::Trade(fill) => {
                            // trigger 出现后不允许继续产生成交。
                            assert!(!seen_trigger, "Trade emitted after protection trigger");

                            // 成交数量必须严格正值。
                            assert!(
                                fill.qty.get() > 0,
                                "protected Quote emitted zero-quantity Trade"
                            );

                            // TradeId 在本次执行中不得重复。
                            assert!(
                                seen_trade_ids.insert(fill.trade_id.get()),
                                "protected Quote emitted duplicate TradeId"
                            );
                        }

                        EngineEvent::MarketProtectionTriggered { .. } => {
                            // 同一命令最多只能有一个 trigger。
                            assert!(!seen_trigger, "protected Quote emitted multiple triggers");

                            // trigger 必须是最后一个事件。
                            assert_eq!(
                                index + 1,
                                execution.events().len(),
                                "protection trigger must be terminal"
                            );

                            seen_trigger = true;
                        }

                        other => {
                            panic!("unexpected protected Quote event: {other:?}");
                        }
                    }
                }
            }
        }

        // ============================================================
        // 7. 最终 Production 整簿验证
        // ============================================================

        assert_eq!(
            validate_book(&production),
            Ok(()),
            "Production validator failed after protected Quote"
        );

        // Result 在前面的 assert_eq! 和 match 中均通过引用访问，
        // 因此仍然可以将 production_result 原样返回。
        (production_result, production_after)
    }
    #[test]
    fn t0638_multilevel_fifo_and_protection_sources() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        let cases = [
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                110,
                ProtectionSource::User,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                110,
                100,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Buy,
                Side::Sell,
                90,
                110,
                120,
                100,
                100,
                ProtectionSource::Both,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                90,
                ProtectionSource::User,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                90,
                100,
                ProtectionSource::MarketBand,
            ),
            (
                Side::Sell,
                Side::Buy,
                110,
                90,
                80,
                100,
                100,
                ProtectionSource::Both,
            ),
        ];

        for (side, maker_side, better, outside, farther, user, band, source) in cases {
            let makers = [
                (90, maker_side, farther, 2),
                (40, maker_side, 100, 4),
                (30, maker_side, better, 2),
                (10, maker_side, 100, 3),
                (50, maker_side, outside, 1),
            ];

            let incoming = quote(
                100,
                side,
                2000,
                Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(user)),
                }),
            );

            let context = context(Some(band));
            let ids = [TradeId::new(701), TradeId::new(702), TradeId::new(703)];

            let spent = if side == Side::Buy { 880 } else { 920 };

            let expected = Ok(ProtectedMarketByQuoteExecution::new(
                vec![
                    trade(701, 30, better, 2, maker_side),
                    trade(702, 40, 100, 4, maker_side),
                    trade(703, 10, 100, 3, maker_side),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source,
                    },
                ],
                QuoteAmount::new(spent),
                QuoteAmount::new(2000 - spent),
            ));

            let first = run_case(&makers, &incoming, &context, &math, &ids);
            let second = run_case(&makers, &incoming, &context, &math, &ids);

            // 完整结果和最终盘口必须可重复。
            assert_eq!(first, second);
            assert_eq!(first.0, expected);

            let maker_book = if maker_side == Side::Buy {
                &first.1.bids
            } else {
                &first.1.asks
            };

            // 越界 maker 和更远 maker 保留在盘口，
            // 其 FIFO priority 也不发生重新分配。
            assert_eq!(
                maker_book
                    .orders
                    .iter()
                    .map(|o| (o.original.order_id, o.remaining, o.priority,))
                    .collect::<Vec<_>>(),
                vec![
                    (OrderId::new(50), 1, QueuePriority::new(4)),
                    (OrderId::new(90), 2, QueuePriority::new(0)),
                ],
            );

            assert_eq!(maker_book.best_price, Some(p(outside)));
        }
    }
    fn assert_expected(
        makers: &[(u128, Side, i64, u64)],
        incoming: &MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        ids: &[TradeId],
        expected: &Result<ProtectedMarketByQuoteExecution, RejectReason>,
    ) -> ObservedBook {
        let first = run_case(makers, incoming, context, math, ids);
        let second = run_case(makers, incoming, context, math, ids);

        assert_eq!(first, second, "protected Quote not reproducible");
        assert_eq!(&first.0, expected);

        first.1
    }
    #[test]
    fn t0638_protection_precedes_quote_dust() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better, outside) in [
            (Side::Buy, Side::Sell, 90, 110),
            (Side::Sell, Side::Buy, 110, 90),
        ] {
            let makers = [(1, maker_side, better, 1), (2, maker_side, outside, 5)];

            let incoming = quote(100, side, better as u128 + 5, None);

            let expected = Ok(ProtectedMarketByQuoteExecution::new(
                vec![
                    trade(701, 1, better, 1, maker_side),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: Price::try_new(outside).unwrap(),
                        effective_worst_price: Price::try_new(100).unwrap(),
                        source: ProtectionSource::MarketBand,
                    },
                ],
                QuoteAmount::new(better as u128),
                QuoteAmount::new(5),
            ));

            let after = assert_expected(
                &makers,
                &incoming,
                &context(Some(100)),
                &math,
                &[TradeId::new(701)],
                &expected,
            );

            let side_view = if maker_side == Side::Buy {
                &after.bids
            } else {
                &after.asks
            };

            assert_eq!(side_view.orders.len(), 1);
            assert_eq!(side_view.orders[0].original.order_id, OrderId::new(2),);
            assert_eq!(side_view.orders[0].remaining, 5);
            assert_eq!(side_view.orders[0].priority, QueuePriority::new(1));
        }
    }
    #[test]
    fn t0638_duplicate_and_invalid_bps_are_atomic() {
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for side in [Side::Buy, Side::Sell] {
            let maker_side = opposite_side(side);
            let makers = [(1, maker_side, 100_000, 5)];

            let invalid_bps = Some(MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            });

            let ctx = context(Some(100_000));

            let duplicate = quote(1, side, 1000, invalid_bps.clone());

            assert_expected(
                &makers,
                &duplicate,
                &ctx,
                &math,
                &[],
                &Err(RejectReason::DuplicateOrderId),
            );

            let nonduplicate = quote(100, side, 1000, invalid_bps);

            assert_expected(
                &makers,
                &nonduplicate,
                &ctx,
                &math,
                &[],
                &Err(RejectReason::InvalidSlippageBps),
            );
        }
    }

    // Protected MarketByQuote 固定差分场景补充。
    //
    // 将整个模块添加到现有 t0638_protected_quote_tests 内部。
    //
    // 验证：
    // 1. Reference / Production 独立执行公开入口；
    // 2. 完整比较原序事件、spent、unused；
    // 3. 比较双侧订单、FIFO、depth、best price、priority；
    // 4. 失败时验证 Reference 业务状态与 Production 完整物理状态不变；
    // 5. TradeId 不足保留 fail-closed panic 契约。

    mod t0638_fixed_scenarios {
        use super::super::{
            ObservedBook, ProductionOrderBook, ReferenceOrderBook,
            assert_production_failure_state_unchanged, assert_regular_book_invariants,
            level_snapshot, observe_production, observe_reference, order, policy,
            reference_contains_order, validate_book,
        };

        use matching_domain::market_order::{
            CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
        };

        use matching_domain::{
            EngineEvent, MarketByQuote, MarketMath, MarketProtection, OrderId, Price, Qty,
            QuoteAmount, QuoteBudget, RejectReason, Side, TradeEvent, TradeId, UserId,
        };

        use std::panic::{AssertUnwindSafe, catch_unwind};

        type Expected = Result<ProtectedMarketByQuoteExecution, RejectReason>;

        type Maker = (u128, Side, i64, u64);

        // ============================================================
        // Fixtures
        // ============================================================

        fn math(tick: i64, lot: u64) -> MarketMath {
            MarketMath::try_new(0, 0, 0, tick, lot).unwrap()
        }

        fn quote(
            id: u128,
            side: Side,
            budget: u128,
            protection: Option<MarketProtection>,
        ) -> MarketByQuote {
            MarketByQuote {
                order_id: OrderId::new(id),
                user_id: UserId::new(id as u64),
                side,
                quote_budget: QuoteBudget::try_new(budget).unwrap(),
                market_protection: protection,
            }
        }

        fn ctx(reference: i64, band: Option<i64>) -> CommandStartProtectionContext {
            CommandStartProtectionContext {
                reference_price_at_command_start: Price::try_new(reference).unwrap(),

                market_band_worst: band.map(|value| Price::try_new(value).unwrap()),
            }
        }

        fn trade(id: u128, maker: u128, price: i64, qty: u64, maker_side: Side) -> EngineEvent {
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(id),
                maker_order_id: OrderId::new(maker),
                taker_order_id: OrderId::new(100),
                price: Price::try_new(price).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
                maker_side,
            })
        }

        fn expected(events: Vec<EngineEvent>, spent: u128, unused: u128) -> Expected {
            Ok(ProtectedMarketByQuoteExecution::new(
                events,
                QuoteAmount::new(spent),
                QuoteAmount::new(unused),
            ))
        }

        // ============================================================
        // 双模型构建
        // ============================================================

        fn build(makers: &[Maker]) -> (ReferenceOrderBook, ProductionOrderBook) {
            let mut reference = ReferenceOrderBook::new();
            let mut production = ProductionOrderBook::new(policy());

            for &(id, side, price, qty) in makers {
                let resting = order(id, side, price, qty);

                reference.rest(resting.clone()).unwrap();
                production.rest(resting).unwrap();
            }

            (reference, production)
        }

        // ============================================================
        // 差分执行与观察
        // ============================================================

        /// 两套引擎分别调用自己的公开受保护 Quote 执行入口。
        ///
        /// wanted 由测试手写固定预期构成，而不是从任意一侧结果推导。
        ///
        /// stable_maker 用于验证 partial maker 的 Arena slot
        /// 和 QueuePriority 在成交前后不变。
        fn execute_once(
            makers: &[Maker],
            incoming: &MarketByQuote,
            context: &CommandStartProtectionContext,
            math: &MarketMath,
            trade_ids: &[TradeId],
            wanted: &Expected,
            stable_maker: Option<u128>,
        ) -> ObservedBook {
            let (mut reference, mut production) = build(makers);

            assert_eq!(validate_book(&production), Ok(()));

            // 执行前业务状态。
            let reference_before = observe_reference(&reference);
            let production_before = observe_production(&production);

            assert_eq!(reference_before, production_before);

            // Production 执行前物理状态。
            let bids_before = level_snapshot(&production.bids);
            let asks_before = level_snapshot(&production.asks);

            let orders_before = production.orders.clone();
            let arena_before = production.arena.test_snapshot();

            let next_before = production.next_queue_priority();

            let policy_before = *production.queue_priority_allocator.policy();

            // partial maker 的原始 slot 和 priority。
            let stable_before = stable_maker.map(|id| {
                let id = OrderId::new(id);

                let slot = *production.orders.get(&id).unwrap();
                let priority = production.order(id).unwrap().priority();

                (id, slot, priority)
            });

            // --------------------------------------------------------
            // 两套模型独立执行
            // --------------------------------------------------------

            let reference_result = reference.execute_market_by_quote_with_protection(
                incoming.clone(),
                context,
                math,
                trade_ids,
            );

            let production_result = production.execute_market_by_quote_with_protection(
                incoming.clone(),
                context,
                math,
                trade_ids,
            );

            // 双方必须分别符合独立固定预期。
            assert_eq!(
                &reference_result, wanted,
                "Reference differs from independent fixture"
            );

            assert_eq!(
                &production_result, wanted,
                "Production differs from independent fixture"
            );

            // Result 包含：
            // Vec<EngineEvent> 完整原序、spent、unused 或 RejectReason。
            assert_eq!(
                reference_result, production_result,
                "protected Quote models differ"
            );

            // --------------------------------------------------------
            // 执行后完整业务观察
            // --------------------------------------------------------

            let reference_after = observe_reference(&reference);
            let production_after = observe_production(&production);

            assert_eq!(reference_after, production_after);

            // 市价单不能消耗 QueuePriority。
            assert_eq!(production.next_queue_priority(), next_before);

            assert_eq!(validate_book(&production), Ok(()));

            assert_regular_book_invariants(&production_after, next_before, 0);

            // --------------------------------------------------------
            // 拒绝原子性
            // --------------------------------------------------------

            if wanted.is_err() {
                // Reference 业务状态不变。
                assert_eq!(reference_after, reference_before);

                // Production 业务状态不变。
                assert_eq!(production_after, production_before);

                // Production 物理状态不变。
                assert_production_failure_state_unchanged(
                    &production,
                    &bids_before,
                    &asks_before,
                    &orders_before,
                    &arena_before,
                    next_before,
                    policy_before,
                );
            } else {
                // ----------------------------------------------------
                // 成功路径检查
                // ----------------------------------------------------

                // Taker 不应挂簿。
                assert!(!reference_contains_order(&reference, incoming.order_id));

                assert!(production.order(incoming.order_id).is_none());

                let execution = wanted.as_ref().unwrap();

                // Quote 本金守恒。
                assert_eq!(
                    execution
                        .spent_quote()
                        .checked_add(execution.unused_quote())
                        .unwrap()
                        .get(),
                    incoming.quote_budget.get(),
                );

                // Trade* -> 最多一个终结保护事件。
                let mut terminal_seen = false;

                for (index, event) in execution.events().iter().enumerate() {
                    match event {
                        EngineEvent::Trade(fill) => {
                            assert!(!terminal_seen, "Trade emitted after protection trigger");

                            assert!(fill.qty.get() > 0);
                        }

                        EngineEvent::MarketProtectionTriggered { .. } => {
                            assert!(!terminal_seen);

                            assert_eq!(index + 1, execution.events().len());

                            terminal_seen = true;
                        }

                        other => {
                            panic!("unexpected protected Quote event: {other:?}");
                        }
                    }
                }

                // 如果指定 partial maker，必须保留 slot/priority。
                if let Some((id, slot, priority)) = stable_before {
                    assert_eq!(production.orders.get(&id), Some(&slot));

                    assert_eq!(production.order(id).unwrap().priority(), priority);
                }
            }

            production_after
        }

        /// 同一固定场景完整重复两次，保证确定性。
        fn assert_case(
            makers: &[Maker],
            incoming: &MarketByQuote,
            context: &CommandStartProtectionContext,
            math: &MarketMath,
            ids: &[TradeId],
            wanted: Expected,
            stable_maker: Option<u128>,
        ) -> ObservedBook {
            let first = execute_once(makers, incoming, context, math, ids, &wanted, stable_maker);

            let second = execute_once(makers, incoming, context, math, ids, &wanted, stable_maker);

            assert_eq!(
                first, second,
                "fixed protected Quote scenario is non-deterministic"
            );

            first
        }

        // ============================================================
        // 测试一：空簿、边界相等、预算耗尽、两类 dust
        // ============================================================

        #[test]
        fn t0638_empty_equal_budget_initial_and_after_dust() {
            let math = math(10, 1);

            for (side, maker_side, better, outside) in [
                (Side::Buy, Side::Sell, 90, 110),
                (Side::Sell, Side::Buy, 110, 90),
            ] {
                let bounded = ctx(100_003, Some(100));

                // 1. 空簿
                assert_case(
                    &[],
                    &quote(100, side, 1000, None),
                    &bounded,
                    &math,
                    &[],
                    expected(vec![], 0, 1000),
                    None,
                );

                // 2. 边界相等、预算恰好耗尽。
                // 不允许继续读取下一越界 maker。
                let after = assert_case(
                    &[(1, maker_side, 100, 2), (2, maker_side, outside, 4)],
                    &quote(100, side, 200, None),
                    &bounded,
                    &math,
                    &[TradeId::new(701)],
                    expected(vec![trade(701, 1, 100, 2, maker_side)], 200, 0),
                    None,
                );

                let view = if maker_side == Side::Buy {
                    &after.bids
                } else {
                    &after.asks
                };

                assert_eq!(view.orders.len(), 1);

                assert_eq!(view.orders[0].original.order_id, OrderId::new(2));

                assert_eq!(view.depth[0].price, Price::try_new(outside).unwrap());

                assert_eq!(view.best_price, Some(Price::try_new(outside).unwrap()));

                // 3. 初始 dust。
                // 99 quote 不能支付 100 * 1 lot。
                assert_case(
                    &[(1, maker_side, 100, 5)],
                    &quote(100, side, 99, None),
                    &bounded,
                    &math,
                    &[],
                    expected(vec![], 0, 99),
                    None,
                );

                // 4. 成交后 dust。
                // better 档成交 1 lot 后只剩 5 quote。
                // 后续档位价格 100 在保护范围内，
                // 不能因为 dust 产生 trigger。
                let budget = better as u128 + 5;

                let after = assert_case(
                    &[(1, maker_side, better, 1), (2, maker_side, 100, 5)],
                    &quote(100, side, budget, None),
                    &bounded,
                    &math,
                    &[TradeId::new(702)],
                    expected(
                        vec![trade(702, 1, better, 1, maker_side)],
                        better as u128,
                        5,
                    ),
                    None,
                );

                let view = if maker_side == Side::Buy {
                    &after.bids
                } else {
                    &after.asks
                };

                assert_eq!(view.orders.len(), 1);

                assert_eq!(view.orders[0].original.order_id, OrderId::new(2));

                assert_eq!(view.orders[0].remaining, 5);
                assert_eq!(view.depth[0].total_qty, 5);
            }
        }

        // ============================================================
        // 测试二：无边界时扫描全部多档和 FIFO
        // ============================================================

        #[test]
        fn t0638_no_boundary_sweeps_all_price_levels_and_fifo() {
            let math = math(10, 1);

            for (side, maker_side, better, outside, farther, spent) in [
                (Side::Buy, Side::Sell, 90, 110, 120, 1230),
                (Side::Sell, Side::Buy, 110, 90, 80, 1170),
            ] {
                // ID 顺序刻意不等于价格优先/FIFO 顺序。
                let makers = [
                    (90, maker_side, farther, 2),
                    (40, maker_side, 100, 4),
                    (30, maker_side, better, 2),
                    (10, maker_side, 100, 3),
                    (50, maker_side, outside, 1),
                ];

                let ids = [
                    TradeId::new(701),
                    TradeId::new(702),
                    TradeId::new(703),
                    TradeId::new(704),
                    TradeId::new(705),
                ];

                let after = assert_case(
                    &makers,
                    &quote(100, side, 2000, None),
                    &ctx(100_003, None),
                    &math,
                    &ids,
                    expected(
                        vec![
                            trade(701, 30, better, 2, maker_side),
                            trade(702, 40, 100, 4, maker_side),
                            trade(703, 10, 100, 3, maker_side),
                            trade(704, 50, outside, 1, maker_side),
                            trade(705, 90, farther, 2, maker_side),
                        ],
                        spent,
                        2000 - spent,
                    ),
                    None,
                );

                // 五张 maker 均已完全成交。
                assert!(after.bids.orders.is_empty());
                assert!(after.asks.orders.is_empty());

                assert!(after.bids.depth.is_empty());
                assert!(after.asks.depth.is_empty());

                assert_eq!(after.bids.best_price, None);
                assert_eq!(after.asks.best_price, None);
            }
        }

        // ============================================================
        // 测试三：partial 保留 slot/priority，full 删除订单和档位
        // ============================================================

        #[test]
        fn t0638_partial_preserves_slot_fifo_priority_and_full_removes_levels() {
            let math = math(10, 1);

            for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
                let bounded = ctx(100_003, Some(100));

                // ----------------------------------------------------
                // partial maker
                // ----------------------------------------------------

                let partial = assert_case(
                    &[(1, maker_side, 100, 10), (2, maker_side, 100, 5)],
                    &quote(100, side, 350, None),
                    &bounded,
                    &math,
                    &[TradeId::new(701)],
                    expected(vec![trade(701, 1, 100, 3, maker_side)], 300, 50),
                    Some(1),
                );

                let view = if maker_side == Side::Buy {
                    &partial.bids
                } else {
                    &partial.asks
                };

                assert_eq!(view.orders.len(), 2);

                // FIFO 第一张 maker 部分成交。
                assert_eq!(view.orders[0].original.order_id, OrderId::new(1));

                assert_eq!(view.orders[0].remaining, 7);
                assert_eq!(view.orders[0].priority.get(), 0);

                // FIFO 第二张 maker 不变。
                assert_eq!(view.orders[1].original.order_id, OrderId::new(2));

                assert_eq!(view.orders[1].remaining, 5);
                assert_eq!(view.orders[1].priority.get(), 1);

                // 档位聚合：7 + 5 = 12。
                assert_eq!(view.depth[0].order_count, 2);
                assert_eq!(view.depth[0].total_qty, 12);

                assert_eq!(view.best_price, Some(Price::try_new(100).unwrap()));

                // ----------------------------------------------------
                // full maker
                // ----------------------------------------------------

                let full = assert_case(
                    &[(1, maker_side, 100, 2)],
                    &quote(100, side, 200, None),
                    &bounded,
                    &math,
                    &[TradeId::new(702)],
                    expected(vec![trade(702, 1, 100, 2, maker_side)], 200, 0),
                    None,
                );

                assert!(full.bids.orders.is_empty());
                assert!(full.asks.orders.is_empty());

                assert!(full.bids.depth.is_empty());
                assert!(full.asks.depth.is_empty());

                assert_eq!(full.bids.best_price, None);
                assert_eq!(full.asks.best_price, None);
            }
        }

        // ============================================================
        // 测试四：InvalidTick / ArithmeticOverflow / InvalidLot
        // ============================================================

        #[test]
        fn t0638_invalid_tick_overflow_and_invalid_lot_are_atomic() {
            let fixed_math = math(10, 1);

            for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
                let makers = [(1, maker_side, 100_000, 5)];

                // ----------------------------------------------------
                // InvalidTick：用户显式 worst_price 不满足 tick
                // ----------------------------------------------------

                assert_case(
                    &makers,
                    &quote(
                        100,
                        side,
                        1000,
                        Some(MarketProtection {
                            max_slippage_bps: None,
                            worst_price: Some(Price::try_new(100_003).unwrap()),
                        }),
                    ),
                    &ctx(100_003, Some(100_000)),
                    &fixed_math,
                    &[],
                    Err(RejectReason::InvalidTick),
                    None,
                );

                // ----------------------------------------------------
                // InvalidLot：Qty=11 合法，但 lot_size=10
                // ----------------------------------------------------

                assert_case(
                    &[(1, maker_side, 100, 11)],
                    &quote(100, side, 2000, None),
                    &ctx(100_003, None),
                    &math(1, 10),
                    &[],
                    Err(RejectReason::InvalidLot),
                    None,
                );
            }

            // --------------------------------------------------------
            // ArithmeticOverflow：Buy bps 边界计算溢出
            // --------------------------------------------------------

            assert_case(
                &[(1, Side::Sell, 100_000, 5)],
                &quote(
                    100,
                    Side::Buy,
                    1000,
                    Some(MarketProtection {
                        max_slippage_bps: Some(100),
                        worst_price: None,
                    }),
                ),
                &ctx(i64::MAX, Some(100_000)),
                &fixed_math,
                &[],
                Err(RejectReason::ArithmeticOverflow),
                None,
            );

            // 已有 Duplicate-before-bps 和 3007 测试保留不变。
        }

        // ============================================================
        // 测试五：TradeId 不足，独立 panic + 完整原子性
        // ============================================================

        #[test]
        fn t0638_insufficient_trade_ids_panic_atomically_on_both_books() {
            let math = math(10, 1);

            for (side, maker_side, better) in
                [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
            {
                // 两个可成交 maker，需要两个 TradeId。
                let makers = [(1, maker_side, better, 2), (2, maker_side, 100, 3)];

                let (mut reference, mut production) = build(&makers);

                let incoming = quote(100, side, 2000, None);

                let context = ctx(100_003, Some(100));

                // 故意只提供一个。
                let ids = [TradeId::new(701)];

                // ----------------------------------------------------
                // 1. 执行前业务观察
                // ----------------------------------------------------

                let reference_before = observe_reference(&reference);
                let production_before = observe_production(&production);

                assert_eq!(reference_before, production_before);
                assert_eq!(validate_book(&production), Ok(()));

                // ----------------------------------------------------
                // 2. Production 完整物理快照
                // ----------------------------------------------------

                let bids_before = level_snapshot(&production.bids);
                let asks_before = level_snapshot(&production.asks);

                let orders_before = production.orders.clone();
                let arena_before = production.arena.test_snapshot();

                let next_before = production.next_queue_priority();

                let policy_before = *production.queue_priority_allocator.policy();

                // ----------------------------------------------------
                // 3. Reference 独立 catch_unwind
                // ----------------------------------------------------

                let reference_panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = reference.execute_market_by_quote_with_protection(
                        incoming.clone(),
                        &context,
                        &math,
                        &ids,
                    );
                }));

                // ----------------------------------------------------
                // 4. Production 独立 catch_unwind
                // ----------------------------------------------------

                let production_panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = production.execute_market_by_quote_with_protection(
                        incoming.clone(),
                        &context,
                        &math,
                        &ids,
                    );
                }));

                assert!(
                    reference_panic.is_err(),
                    "Reference must panic before first mutation"
                );

                assert!(
                    production_panic.is_err(),
                    "Production must panic before first mutation"
                );

                // ----------------------------------------------------
                // 5. Reference 业务状态不得改变
                // ----------------------------------------------------

                assert_eq!(observe_reference(&reference), reference_before);

                // ----------------------------------------------------
                // 6. Production 业务状态不得改变
                // ----------------------------------------------------

                assert_eq!(observe_production(&production), production_before);

                // ----------------------------------------------------
                // 7. Production 完整物理状态不得改变
                // ----------------------------------------------------

                assert_production_failure_state_unchanged(
                    &production,
                    &bids_before,
                    &asks_before,
                    &orders_before,
                    &arena_before,
                    next_before,
                    policy_before,
                );

                assert_eq!(validate_book(&production), Ok(()));

                assert_regular_book_invariants(&observe_production(&production), next_before, 0);
            }
        }
    }
    #[test]
    fn t0639_protected_quote_bps_boundary_execution_differential() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        const BUDGET: u128 = 2_000_000;

        // 固定 reference=200_010，tick=10。
        //
        // 0 bps:
        //   Buy / Sell = 200_010
        //
        // 9_999 bps:
        //   Buy = floor_to_tick(399_999.999) = 399_990
        //   Sell = ceil_to_tick(20.001)       = 30
        //
        // 10_000 bps:
        //   InvalidSlippageBps (3007)
        //
        // 最后一列为手写的三笔预期成交本金之和。
        let cases: [(Side, u32, i64, i64, i64, u128); 6] = [
            (Side::Buy, 0, 200_000, 200_010, 200_020, 600_020),
            (Side::Sell, 0, 200_020, 200_010, 200_000, 600_040),
            (Side::Buy, 9_999, 399_980, 399_990, 400_000, 1_199_960),
            (Side::Sell, 9_999, 40, 30, 20, 100),
            (Side::Buy, 10_000, 200_000, 200_010, 200_020, 0),
            (Side::Sell, 10_000, 200_020, 200_010, 200_000, 0),
        ];

        for (side, bps, better, boundary, outside, spent) in cases {
            let maker_side = opposite_side(side);

            // OrderId、插入顺序与最终撮合顺序故意不同。
            //
            // priority:
            //   maker 40 -> 0，越界 maker
            //   maker 20 -> 1，边界相等
            //   maker 30 -> 2，内侧价格
            //   maker 10 -> 3，边界相等、FIFO 第二张
            //
            // 正确成交原序：
            //   maker 30 -> maker 20 -> maker 10
            //
            // maker 40 不允许成交。
            let makers = [
                (40, maker_side, outside, 2),
                (20, maker_side, boundary, 1),
                (30, maker_side, better, 1),
                (10, maker_side, boundary, 1),
            ];

            let incoming = quote(
                100,
                side,
                BUDGET,
                Some(MarketProtection {
                    max_slippage_bps: Some(bps),
                    worst_price: None,
                }),
            );

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(200_010),
                market_band_worst: None,
            };

            // 只提供三笔 TradeId。
            // 保护 trigger 不能消耗 TradeId。
            let trade_ids = [TradeId::new(701), TradeId::new(702), TradeId::new(703)];

            // 预期由测试数据手写，不读取任何簿的私有计划。
            let expected = if bps == 10_000 {
                Err(RejectReason::InvalidSlippageBps)
            } else {
                Ok(ProtectedMarketByQuoteExecution::new(
                    vec![
                        trade(701, 30, better, 1, maker_side),
                        trade(702, 20, boundary, 1, maker_side),
                        trade(703, 10, boundary, 1, maker_side),
                        EngineEvent::MarketProtectionTriggered {
                            order_id: OrderId::new(100),
                            next_price: p(outside),
                            effective_worst_price: p(boundary),
                            source: ProtectionSource::User,
                        },
                    ],
                    QuoteAmount::new(spent),
                    QuoteAmount::new(BUDGET - spent),
                ))
            };

            // 两套模型各自独立执行。
            //
            // 现有 run_case 应保持以下完整检查：
            // - Reference / Production Result 完全相等
            // - 原序 EngineEvent
            // - spent_quote / unused_quote
            // - 订单和 FIFO 投影
            // - depth / best price / priority
            // - taker 不 rest
            // - Trade qty 严格正
            // - Production validate_book
            // - Err 时两模型失败原子性
            // - Err 时 Production 完整物理快照不变
            let first = run_case(&makers, &incoming, &context, &math, &trade_ids);

            let second = run_case(&makers, &incoming, &context, &math, &trade_ids);

            // 固定 tape 运行两次，结果及最终可观察状态一致。
            assert_eq!(
                first, second,
                "T06.39 nondeterministic: side={side:?}, bps={bps}"
            );

            // 不仅比较双模型，也比较测试独立手写的固定预期。
            assert_eq!(
                first.0, expected,
                "T06.39 unexpected execution: side={side:?}, bps={bps}"
            );

            if bps == 10_000 {
                // run_case 的 Err 分支已经验证完整失败原子性。
                // 非法 bps 必须在首笔 mutation 前被拒绝。
                continue;
            }

            // --------------------------------------------------------
            // 成功场景：越界 maker 不成交、不删除、不改变 priority。
            // --------------------------------------------------------

            let after = &first.1;

            let maker_book = match maker_side {
                Side::Buy => &after.bids,
                Side::Sell => &after.asks,
            };

            // 只有越界 maker 40 仍在簿中。
            // 内侧 maker 和两个边界 maker 已完全成交。
            assert_eq!(
                maker_book.orders.len(),
                1,
                "unexpected resting maker count: side={side:?}, bps={bps}"
            );

            let remaining_maker = &maker_book.orders[0];

            assert_eq!(remaining_maker.original.order_id, OrderId::new(40),);

            assert_eq!(remaining_maker.original.price, p(outside),);

            // 未成交 maker 原始数量完整保留。
            assert_eq!(remaining_maker.remaining, 2,);

            // maker 40 最先插入，priority=0。
            // 市价成交和保护触发均不能重新分配 priority。
            assert_eq!(remaining_maker.priority, QueuePriority::new(0),);

            // 越界价格成为对手侧最新 best price。
            assert_eq!(maker_book.best_price, Some(p(outside)),);

            // 原有成交档位已经移除，只剩一个越界价档。
            assert_eq!(maker_book.depth.len(), 1);

            assert_eq!(maker_book.depth[0].price, p(outside),);

            assert_eq!(maker_book.depth[0].order_count, 1,);

            assert_eq!(maker_book.depth[0].total_qty, 2,);

            // Taker 不 rest；当前另一侧必须为空。
            let taker_book = match side {
                Side::Buy => &after.bids,
                Side::Sell => &after.asks,
            };

            assert!(taker_book.orders.is_empty());
            assert!(taker_book.depth.is_empty());
            assert_eq!(taker_book.best_price, None);
        }
    }

    #[test]
    fn t0639_protected_quote_unaligned_reference_directional_rounding_execution() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for side in [Side::Buy, Side::Sell] {
            let maker_side = opposite_side(side);

            // maker 价格合法，且按 tick=10 对齐。
            // Buy：200_010 位于有效保护边界上，允许成交。
            // Sell：200_010 低于有效保护边界，必须停止。
            let makers = [(40, maker_side, 200_010, 2)];

            let incoming = quote(
                100,
                side,
                2_000_000,
                Some(MarketProtection {
                    max_slippage_bps: Some(0),
                    worst_price: None,
                }),
            );

            // command-start reference 故意不按 tick 对齐。
            //
            // reference = 200_013
            // tick      = 10
            // bps       = 0
            //
            // 方向性取整规则：
            // Buy  -> floor_to_tick(200_013) = 200_010
            // Sell -> ceil_to_tick(200_013)  = 200_020
            //
            // 必须将原始 reference 交给 Domain 计算，
            // 测试中不能提前进行对齐。
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(200_013),
                market_band_worst: None,
            };

            // 固定预期独立手写，不读取任何一侧的私有计划。
            let (trade_ids, expected) = match side {
                Side::Buy => {
                    // Buy 有效边界：200_010。
                    // maker@200_010 恰好等于边界，允许成交。
                    //
                    // qty = 2
                    // spent = 200_010 * 2 = 400_020
                    // unused = 2_000_000 - 400_020 = 1_599_980
                    //
                    // 必须提供一枚 TradeId。
                    (
                        vec![TradeId::new(701)],
                        Ok(ProtectedMarketByQuoteExecution::new(
                            vec![trade(701, 40, 200_010, 2, maker_side)],
                            QuoteAmount::new(400_020),
                            QuoteAmount::new(1_599_980),
                        )),
                    )
                }

                Side::Sell => {
                    // Sell 有效边界：200_020。
                    // maker@200_010 低于边界，首档即越界。
                    //
                    // 不发生任何成交，不消耗 TradeId。
                    // 返回一个 terminal protection trigger。
                    //
                    // spent = 0
                    // unused = 2_000_000
                    (
                        vec![],
                        Ok(ProtectedMarketByQuoteExecution::new(
                            vec![EngineEvent::MarketProtectionTriggered {
                                order_id: OrderId::new(100),
                                next_price: p(200_010),
                                effective_worst_price: p(200_020),
                                source: ProtectionSource::User,
                            }],
                            QuoteAmount::new(0),
                            QuoteAmount::new(2_000_000),
                        )),
                    )
                }
            };

            // ========================================================
            // 1. Reference / Production 独立执行第一次
            // ========================================================

            let first = run_case(&makers, &incoming, &context, &math, &trade_ids);

            // ========================================================
            // 2. Reference / Production 独立执行第二次
            // ========================================================

            let second = run_case(&makers, &incoming, &context, &math, &trade_ids);

            // 两次完整结果与最终盘口必须一致。
            //
            // run_case 已包含：
            // - Reference / Production 完整结果比较
            // - 原序 EngineEvent
            // - spent / unused
            // - 订单、remaining、FIFO、depth、best price、priority
            // - taker 不 rest
            // - Trade qty 严格正
            // - Production validate_book
            assert_eq!(
                first, second,
                "T06.39 directional rounding is nondeterministic: \
             side={side:?}"
            );

            // 两模型结果还必须符合独立手写的固定预期。
            assert_eq!(
                first.0, expected,
                "T06.39 unexpected directional rounding result: \
             side={side:?}"
            );

            // ========================================================
            // 3. 精确检查最终盘口
            // ========================================================

            let after = &first.1;

            let maker_book = match maker_side {
                Side::Buy => &after.bids,
                Side::Sell => &after.asks,
            };

            match side {
                Side::Buy => {
                    // maker@200_010 完全成交，订单和价档均移除。
                    assert!(maker_book.orders.is_empty());
                    assert!(maker_book.depth.is_empty());
                    assert_eq!(maker_book.best_price, None);
                }

                Side::Sell => {
                    // 首档越界，maker 不成交、不删除。
                    assert_eq!(maker_book.orders.len(), 1);

                    let remaining_maker = &maker_book.orders[0];

                    assert_eq!(remaining_maker.original.order_id, OrderId::new(40),);

                    assert_eq!(remaining_maker.original.price, p(200_010),);

                    assert_eq!(remaining_maker.remaining, 2);

                    // 原始 priority 保持不变。
                    assert_eq!(remaining_maker.priority, QueuePriority::new(0),);

                    // 价档保持完整。
                    assert_eq!(maker_book.depth.len(), 1);

                    assert_eq!(maker_book.depth[0].price, p(200_010),);

                    assert_eq!(maker_book.depth[0].order_count, 1);
                    assert_eq!(maker_book.depth[0].total_qty, 2);

                    assert_eq!(maker_book.best_price, Some(p(200_010)),);
                }
            }

            // 两种方向均不允许 taker 挂簿。
            let taker_book = match side {
                Side::Buy => &after.bids,
                Side::Sell => &after.asks,
            };

            assert!(taker_book.orders.is_empty());
            assert!(taker_book.depth.is_empty());
            assert_eq!(taker_book.best_price, None);
        }
    }
}
