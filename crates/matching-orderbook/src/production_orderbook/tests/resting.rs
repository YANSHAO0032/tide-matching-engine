use super::*;

#[test]
fn rest_accepts_first_and_two_sided_non_crossing_orders() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(95).unwrap())
    );

    assert_eq!(book.best_price(Side::Sell), None);

    assert_eq!(book.next_queue_priority(), 1);
    assert_eq!(validate_book(&book), Ok(()));

    book.rest(order(2, Side::Sell, 105, 7)).unwrap();

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(95).unwrap())
    );

    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(105).unwrap())
    );

    assert_eq!(book.next_queue_priority(), 2);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn rest_preserves_same_price_fifo() {
    let mut book = ProductionOrderBook::new(policy());

    for id in [30, 10, 20] {
        book.rest(order(id, Side::Buy, 100, 5)).unwrap();

        assert_eq!(validate_book(&book), Ok(()));
    }

    let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

    assert_eq!(level.count(), 3);
    assert_eq!(level.total_visible_qty(), 15);

    let first = level.head().unwrap();
    let second = book.arena.get(first).unwrap().next().unwrap();
    let third = book.arena.get(second).unwrap().next().unwrap();

    assert_eq!(
        book.arena.get(first).unwrap().original_order().order_id,
        OrderId::new(30)
    );

    assert_eq!(
        book.arena.get(second).unwrap().original_order().order_id,
        OrderId::new(10)
    );

    assert_eq!(
        book.arena.get(third).unwrap().original_order().order_id,
        OrderId::new(20)
    );

    assert_eq!(
        book.arena.get(first).unwrap().priority(),
        QueuePriority::new(0)
    );

    assert_eq!(
        book.arena.get(second).unwrap().priority(),
        QueuePriority::new(1)
    );

    assert_eq!(
        book.arena.get(third).unwrap().priority(),
        QueuePriority::new(2)
    );

    assert_eq!(level.tail(), Some(third));
    assert_eq!(book.next_queue_priority(), 3);
}

#[test]
fn rest_updates_best_price_from_tree_endpoints() {
    let mut book = ProductionOrderBook::new(policy());

    for candidate in [
        order(1, Side::Buy, 90, 1),
        order(2, Side::Buy, 95, 1),
        order(3, Side::Buy, 92, 1),
        order(4, Side::Sell, 110, 1),
        order(5, Side::Sell, 105, 1),
        order(6, Side::Sell, 108, 1),
    ] {
        book.rest(candidate).unwrap();
        assert_eq!(validate_book(&book), Ok(()));
    }

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(95).unwrap())
    );

    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(105).unwrap())
    );
}

#[test]
fn rest_rejects_duplicate_id_without_mutation() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    assert_rest_error_preserves_state(
        &mut book,
        // 故意修改方向 / 价格 / 数量；
        // 标识符相同仍必须首先命中 DuplicateOrderId。
        order(1, Side::Sell, 105, 999),
        RejectReason::DuplicateOrderId,
    );

    assert_eq!(book.next_queue_priority(), 1);
}

#[test]
fn rest_rejects_priority_exhaustion_without_mutation() {
    let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

    // `next`=0，可以完成最后一次合法分配。
    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    assert_eq!(book.next_queue_priority(), 1);
    assert!(book.queue_priority_allocator.is_exhausted());
    assert_eq!(validate_book(&book), Ok(()));

    assert_rest_error_preserves_state(
        &mut book,
        order(2, Side::Buy, 90, 5),
        RejectReason::PrioritySpaceExhaustion,
    );
}

#[test]
fn duplicate_id_precedes_priority_exhaustion() {
    let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    assert!(book.queue_priority_allocator.is_exhausted());

    assert_rest_error_preserves_state(
        &mut book,
        order(1, Side::Sell, 105, 7),
        RejectReason::DuplicateOrderId,
    );
}

#[test]
fn rest_same_level_qty_overflow_is_atomic() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 100, u64::MAX)).unwrap();

    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.next_queue_priority(), 1);

    assert_rest_error_preserves_state(
        &mut book,
        order(2, Side::Buy, 100, 1),
        RejectReason::ArithmeticOverflow,
    );

    // 失败不能建立第二张订单。
    assert_eq!(book.order(OrderId::new(2)), None);

    // 分配器同样没有被消耗。
    assert_eq!(book.next_queue_priority(), 1);

    let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

    assert_eq!(level.count(), 1);
    assert_eq!(level.total_visible_qty(), u64::MAX);
}

#[test]
fn rest_panics_before_mutation_if_candidate_would_cross_or_lock() {
    let cases = [
        // 买入 == 最优卖价：价格相等。
        (order(1, Side::Sell, 105, 5), order(2, Side::Buy, 105, 1)),
        // 买入 > 最优卖价：价格交叉。
        (order(1, Side::Sell, 105, 5), order(2, Side::Buy, 110, 1)),
        // 卖出 == 最优买价：价格相等。
        (order(1, Side::Buy, 95, 5), order(2, Side::Sell, 95, 1)),
        // 卖出 < 最优买价：价格交叉。
        (order(1, Side::Buy, 95, 5), order(2, Side::Sell, 90, 1)),
    ];

    for (resting, candidate) in cases {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(resting).unwrap();

        assert_eq!(validate_book(&book), Ok(()));

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.rest(candidate);
        }));

        assert!(result.is_err(), "crossed/locked rest must fail closed");

        assert_eq!(
            level_snapshot(&book.bids),
            bids_before,
            "panic path mutated bids"
        );

        assert_eq!(
            level_snapshot(&book.asks),
            asks_before,
            "panic path mutated asks"
        );

        assert_eq!(book.orders, orders_before, "panic path mutated ID index");

        assert_eq!(
            book.arena.test_snapshot(),
            arena_before,
            "panic path mutated Arena slots/free-list"
        );

        assert_eq!(
            book.queue_priority_allocator.next(),
            next_before,
            "panic path consumed QueuePriority"
        );

        assert_eq!(
            book.queue_priority_allocator.policy(),
            &policy_before,
            "panic path mutated QueuePriority policy"
        );

        assert_eq!(
            validate_book(&book),
            Ok(()),
            "book must remain valid after fail-closed panic"
        );
    }
}
