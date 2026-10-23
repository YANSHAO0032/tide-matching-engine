use super::*;

#[test]
fn validate_book_accepts_empty_book() {
    let book = ProductionOrderBook::new(policy());

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_single_side_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 90, 4));

    rest_fixture(&mut book, order(2, Side::Buy, 100, 5));

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_multi_level_two_sided_book() {
    let mut book = ProductionOrderBook::new(policy());

    for original in [
        order(1, Side::Buy, 90, 4),
        order(2, Side::Buy, 95, 5),
        order(3, Side::Sell, 105, 6),
        order(4, Side::Sell, 110, 7),
    ] {
        rest_fixture(&mut book, original);
    }

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_same_price_fifo_chain() {
    let mut book = ProductionOrderBook::new(policy());

    for original in [
        order(30, Side::Buy, 100, 4),
        order(10, Side::Buy, 100, 5),
        order(20, Side::Buy, 100, 6),
    ] {
        rest_fixture(&mut book, original);
    }

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_does_not_mutate_state() {
    let mut book = ProductionOrderBook::new(policy());

    let ids = [
        OrderId::new(1),
        OrderId::new(2),
        OrderId::new(3),
        OrderId::new(4),
    ];

    for original in [
        order(1, Side::Buy, 90, 4),
        order(2, Side::Buy, 95, 5),
        order(3, Side::Sell, 105, 6),
        order(4, Side::Sell, 110, 7),
    ] {
        rest_fixture(&mut book, original);
    }

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let nodes_before = node_snapshot(&book, &ids);
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();
    let policy_before = *book.queue_priority_allocator.policy();

    assert_eq!(validate_book(&book), Ok(()));

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);
    assert_eq!(book.orders, orders_before);
    assert_eq!(node_snapshot(&book, &ids), nodes_before);
    assert_eq!(book.arena.test_snapshot(), arena_before);

    assert_eq!(book.queue_priority_allocator.next(), next_before);

    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);
}

fn assert_book_error(book: &ProductionOrderBook, expected_category: &str) {
    // 校验器调用前记录全部可观察业务状态。
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);

    let orders_before = book.orders.clone();

    // 完整覆盖 Arena 槽位 + 空闲链表，
    // 同时也包含所有已占用 OrderNode 状态。
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();

    let policy_before = *book.queue_priority_allocator.policy();

    // ---------- 校验 ----------

    let error = validate_book(book).expect_err("corrupted book must fail validation");

    assert!(
        error.starts_with(expected_category),
        "expected category={expected_category:?}, actual={error:?}"
    );

    // ---------- 必须完全只读 ----------

    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "validator mutated bid levels: error={error:?}"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "validator mutated ask levels: error={error:?}"
    );

    assert_eq!(
        book.orders, orders_before,
        "validator mutated ID index: error={error:?}"
    );

    assert_eq!(
        book.arena.test_snapshot(),
        arena_before,
        "validator mutated Arena slots/free-list: error={error:?}"
    );

    assert_eq!(
        book.queue_priority_allocator.next(),
        next_before,
        "validator consumed queue priority: error={error:?}"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "validator mutated allocator policy: error={error:?}"
    );
}

#[test]
fn validate_book_rejects_missing_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    book.orders.remove(&OrderId::new(1));

    assert_book_error(&book, "id-index/missing");
}

#[test]
fn validate_book_rejects_wrong_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    assert_ne!(first, second);

    book.orders.insert(OrderId::new(1), second);

    assert_book_error(&book, "id-index/wrong-slot");
}

#[test]
fn validate_book_rejects_extra_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    book.orders.insert(OrderId::new(999), slot);

    assert_book_error(&book, "id-index/node-id-mismatch");
}

#[test]
fn validate_book_rejects_wrong_node_price() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let removed = book.arena.remove(slot).unwrap();

    let replacement = OrderNode::new(order(1, Side::Buy, 101, 5), removed.priority());

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    assert_book_error(&book, "node/price-mismatch");
}

#[test]
fn validate_book_rejects_wrong_node_side() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let removed = book.arena.remove(slot).unwrap();

    let replacement = OrderNode::new(order(1, Side::Sell, 100, 5), removed.priority());

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    assert_book_error(&book, "node/side-mismatch");
}

#[test]
fn validate_book_rejects_empty_price_level() {
    let mut book = ProductionOrderBook::new(policy());

    book.bids
        .insert(Price::try_new(100).unwrap(), PriceLevel::new());

    assert_book_error(&book, "level/empty");
}

#[test]
fn validate_book_rejects_orphan_arena_node() {
    let mut book = ProductionOrderBook::new(policy());

    book.arena
        .insert(OrderNode::new(
            order(99, Side::Buy, 100, 5),
            QueuePriority::new(0),
        ))
        .unwrap();

    assert_book_error(&book, "arena/orphan");
}

#[test]
fn validate_book_rejects_allocator_next_behind_active_priority() {
    let mut book = ProductionOrderBook::new(policy());

    let original = order(1, Side::Buy, 100, 5);
    let order_id = original.order_id;
    let price = original.price;

    // `allocator.next()` 仍然是 0，
    // 但主动构造一个优先级=10 的活动 `node`。
    let priority = QueuePriority::new(10);

    let mut level = PriceLevel::new();

    let slot = level
        .push_back(&mut book.arena, OrderNode::new(original, priority))
        .unwrap();

    book.bids.insert(price, level);
    book.orders.insert(order_id, slot);

    assert_eq!(book.next_queue_priority(), 0);

    assert_book_error(&book, "allocator/next-not-ahead");
}

#[test]
fn validate_book_rejects_crossed_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 105, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

    assert_book_error(&book, "book/crossed-or-locked");
}

#[test]
fn validate_book_rejects_cycle_without_looping_forever() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    book.arena.get_mut(second).unwrap().set_next(Some(first));

    assert_book_error(&book, "link/cycle-or-duplicate");
}

#[test]
fn validate_book_accepts_free_arena_slot_outside_active_book() {
    let mut book = ProductionOrderBook::new(policy());

    // 先建立真实合法价格档位。
    rest_fixture(&mut book, order(1, Side::Buy, 95, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 105, 7));

    assert_eq!(validate_book(&book), Ok(()));

    // 再直接向 Arena 插入一个与订单簿无关的临时节点。
    // 它从未进入 PriceLevel，也没有进入订单集合映射。
    let temporary = book
        .arena
        .insert(OrderNode::new(
            order(999, Side::Buy, 90, 1),
            QueuePriority::new(999),
        ))
        .unwrap();

    assert!(book.arena.get(temporary).is_some());

    // 真实 `remove`，留下空闲链表槽位。
    let removed = book.arena.remove(temporary).unwrap();

    assert_eq!(removed.original_order().order_id, OrderId::new(999));

    assert!(book.arena.get(temporary).is_none());

    // 空闲槽位不属于已占用 `node`，因此不是孤立节点。
    // 活动盘口仍然完全合法。
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_rejects_locked_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

    assert_book_error(&book, "book/crossed-or-locked");
}

#[test]
fn validate_book_rejects_non_increasing_level_priority() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    let second_original = book.arena.get(second).unwrap().original_order().clone();

    let _ = book.arena.remove(second).unwrap();

    let mut replacement =
        OrderNode::new(second_original, book.arena.get(first).unwrap().priority());

    replacement.set_prev(Some(first));

    assert_eq!(book.arena.insert(replacement).unwrap(), second);

    assert_book_error(&book, "priority/non-increasing");

    let _ = book.arena.remove(second).unwrap();
}
