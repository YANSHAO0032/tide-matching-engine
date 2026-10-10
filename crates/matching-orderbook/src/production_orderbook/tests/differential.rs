use super::{level_snapshot, order, policy, validate_book};
use std::collections::BTreeSet;

use crate::production_orderbook::ProductionOrderBook;
use crate::reference_orderbook::ReferenceOrderBook;
use crate::{IncomingOrder, OrderNode, PriceLevel, ReferenceOrder};

use proptest::prelude::*;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

use matching_domain::{
    LimitGtcOrder, OrderId, Price, Qty, QueuePriority, RejectReason, Side, TradeEvent, TradeId,
};

/// T05.1 只覆盖固定短 tape：
///
/// - Place Limit GTC
/// - Cancel
///
/// 随机 tape 和 100k 流式回放由后续 T05.2b–T05.4 测试覆盖。
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Place(LimitGtcOrder),
    Cancel(OrderId),
}

/// T05.2 的测试域约束：在创建订单簿之前检查完整 tape。
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
/// T05.1 使用小数量、远离 priority 上限的固定输入，因此这里只允许：
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
                    // T05.1 唯一受控 Place error：
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
                    // T05.1 唯一受控 Cancel error。
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
        if let CommandOutcome::Place(Ok(trades)) = &outcome {
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
