//! 共享业务观察与结构不变量检查，以及观察器自身的损坏状态回归。
use super::*;

/// 判断参考簿当前活动订单簿是否已经存在指定 OrderId。
///
/// 必须通过公开有序视图观察，不能访问参考簿内部 Vec。
pub(super) fn reference_contains_order(book: &ReferenceOrderBook, order_id: OrderId) -> bool {
    [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| book.orders(side))
        .any(|resting| resting.original_order().order_id == order_id)
}

/// 返回对手侧。
pub(super) fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// 投影参考簿返回的已移除订单。
pub(super) fn observe_reference_order(order: &ReferenceOrder) -> ObservedOrder {
    ObservedOrder {
        original: order.original_order().clone(),
        remaining: order.remaining(),
        priority: order.priority(),
    }
}

/// 投影生产簿返回的已移除/有效 `node`。
pub(super) fn observe_production_order(node: &OrderNode) -> ObservedOrder {
    ObservedOrder {
        original: node.original_order().clone(),
        remaining: node.remaining(),
        priority: node.priority(),
    }
}

/// 参考簿一侧业务观察。
///
/// 严格使用 `ReferenceOrderBook::orders`(方向)；
/// 不重新按标识符 / 优先级排序。
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

/// 参考簿整簿业务观察。
pub(super) fn observe_reference(book: &ReferenceOrderBook) -> ObservedBook {
    ObservedBook {
        bids: observe_reference_side(book, Side::Buy),
        asks: observe_reference_side(book, Side::Sell),
    }
}

/// 从生产簿的一个 PriceLevel 按真实侵入式先进先出：
///
/// 头节点 -> `next`
///
/// 生成订单观察。
///
/// 禁止：
///
/// - 根据 OrderId 排序；
/// - 根据优先级重新排序；
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

/// 生产簿一侧业务观察。
///
/// 买入:
///     BTreeMap 反向遍历 => 高价 -> 低价
///
/// 卖出:
///     BTreeMap 正向遍历 => 低价 -> 高价
///
/// 同价格：
///     PriceLevel 头节点 -> `next`
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

/// 生产簿整簿业务观察。
///
/// 成功和拒绝路径都必须先通过整簿校验，避免在损坏链表上无限遍历。
pub(super) fn observe_production(book: &ProductionOrderBook) -> ObservedBook {
    validate_book(book).unwrap_or_else(|reason| {
        panic!("Production validator failed before differential observation: {reason}")
    });

    ObservedBook {
        bids: observe_production_side(book, Side::Buy),
        asks: observe_production_side(book, Side::Sell),
    }
}

/// 当前普通订单簿的只读不变量检查。
///
/// 适用范围（以当前普通订单限价单 GTC / 取消测试域为准）：
/// §54: 1–5、7–11、26、49；16 仅成交正量与单命令序列内 TradeId 唯一；
/// 28–30 仅普通订单 QueuePriority 的单调分配、同价先进先出和不复用部分。
/// 止损订单、到期索引、跨分片唯一性、WAL、摘要、崩溃均不适用，
/// 不将本测试解释为覆盖这些尚未接入的系统性质。
pub(super) fn assert_regular_book_invariants(
    book: &ObservedBook,
    next_priority: u128,
    step: usize,
) {
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

/// 在受控业务错误后验证生产簿的完整内部状态不变。
///
/// 这里比语义观察更严格，同时覆盖：
///
/// - 买价 / 卖价价档元数据
/// - OrderId 索引
/// - Arena 已占用槽位 / 空闲链表
/// - 下一队列优先级 `next`（QueuePriority）
/// - 分配器策略
pub(super) fn assert_production_failure_state_unchanged(
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

/// 在生成先进先出投影前由校验器拒绝可达环，而非继续沿 `next` 遍历。
#[test]
#[should_panic(expected = "link/cycle-or-duplicate")]
fn production_observation_rejects_cycle_before_traversal() {
    let mut book = ProductionOrderBook::new(policy());
    let first = super::super::rest_fixture(&mut book, order(1, Side::Buy, 100, 5));
    let second = super::super::rest_fixture(&mut book, order(2, Side::Buy, 100, 7));
    book.arena.get_mut(second).unwrap().set_next(Some(first));

    let _ = observe_production(&book);
}

/// 空普通订单簿也必须独立通过只读不变量辅助函数。
#[test]
fn empty_regular_book_passes_invariant_helper() {
    let production = ProductionOrderBook::new(policy());
    let observed = observe_production(&production);

    assert_regular_book_invariants(&observed, production.next_queue_priority(), 0);
}
