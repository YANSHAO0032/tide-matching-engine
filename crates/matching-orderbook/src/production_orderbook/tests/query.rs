use super::*;

#[test]
fn empty_book_and_single_side_queries() {
    let mut book = ProductionOrderBook::new(policy());

    assert_eq!(book.best_price(Side::Buy), None);
    assert_eq!(book.best_price(Side::Sell), None);
    assert_eq!(book.order(OrderId::new(1)), None);
    assert_eq!(book.next_queue_priority(), 0);

    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(100).unwrap())
    );
    assert_eq!(book.best_price(Side::Sell), None);

    assert_eq!(
        book.order(OrderId::new(1))
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(1)
    );

    assert_eq!(book.order(OrderId::new(999)), None);
    assert_eq!(book.next_queue_priority(), 1);
}

#[test]
fn best_price_uses_tree_endpoints_and_keeps_sides_isolated() {
    let mut book = ProductionOrderBook::new(policy());

    // 故意不按价格顺序插入。
    for original in [
        order(1, Side::Buy, 90, 1),
        order(2, Side::Buy, 105, 1),
        order(3, Side::Buy, 95, 1),
        order(4, Side::Sell, 130, 1),
        order(5, Side::Sell, 110, 1),
        order(6, Side::Sell, 120, 1),
    ] {
        rest_fixture(&mut book, original);
    }

    // 买入取最高买价。
    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(105).unwrap())
    );

    // 卖出取最低卖价。
    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(110).unwrap())
    );

    // 两侧树完全隔离。
    assert!(book.bids.contains_key(&Price::try_new(105).unwrap()));
    assert!(!book.asks.contains_key(&Price::try_new(105).unwrap()));

    assert!(book.asks.contains_key(&Price::try_new(110).unwrap()));
    assert!(!book.bids.contains_key(&Price::try_new(110).unwrap()));
}

#[test]
fn best_price_handles_price_boundaries_without_crossing_fixture() {
    let min_price = 1;
    let max_price = i64::MAX;

    // ---------- 只有买单订单簿 ----------

    let mut bids = ProductionOrderBook::new(policy());

    rest_fixture(&mut bids, order(1, Side::Buy, min_price, 1));

    rest_fixture(&mut bids, order(2, Side::Buy, max_price, 1));

    assert_eq!(
        bids.best_price(Side::Buy),
        Some(Price::try_new(max_price).unwrap())
    );

    // 单侧测试数据，另一侧必须保持为空。
    assert_eq!(bids.best_price(Side::Sell), None);
    assert!(bids.asks.is_empty());

    // 两个边界价格都真实存在于买价树。
    assert!(bids.bids.contains_key(&Price::try_new(min_price).unwrap()));

    assert!(bids.bids.contains_key(&Price::try_new(max_price).unwrap()));

    // ---------- 只有卖单订单簿 ----------

    let mut asks = ProductionOrderBook::new(policy());

    rest_fixture(&mut asks, order(3, Side::Sell, max_price, 1));

    rest_fixture(&mut asks, order(4, Side::Sell, min_price, 1));

    assert_eq!(
        asks.best_price(Side::Sell),
        Some(Price::try_new(min_price).unwrap())
    );

    // 单侧测试数据，另一侧必须保持为空。
    assert_eq!(asks.best_price(Side::Buy), None);
    assert!(asks.bids.is_empty());

    assert!(asks.asks.contains_key(&Price::try_new(min_price).unwrap()));

    assert!(asks.asks.contains_key(&Price::try_new(max_price).unwrap()));
}

#[test]
fn same_price_orders_are_individually_addressable() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(30, Side::Buy, 100, 4));

    let second = rest_fixture(&mut book, order(10, Side::Buy, 100, 5));

    let third = rest_fixture(&mut book, order(20, Side::Buy, 100, 6));

    let price = Price::try_new(100).unwrap();
    let level = book.bids.get(&price).unwrap();

    assert_eq!(level.count(), 3);
    assert_eq!(level.head(), Some(first));
    assert_eq!(level.tail(), Some(third));

    assert_eq!(book.arena.get(first).unwrap().next(), Some(second));
    assert_eq!(book.arena.get(second).unwrap().next(), Some(third));

    // 查询按 OrderId 定位，与数值标识符排序无关。
    for id in [30, 10, 20] {
        assert_eq!(
            book.order(OrderId::new(id))
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(id)
        );
    }

    // 三次测试数据分配。
    assert_eq!(book.next_queue_priority(), 3);
}

#[test]
fn repeated_queries_preserve_state() {
    let mut book = ProductionOrderBook::new(policy());

    for original in [
        order(30, Side::Buy, 95, 4),
        order(10, Side::Buy, 95, 5),
        order(40, Side::Sell, 105, 6),
        order(20, Side::Sell, 110, 7),
    ] {
        rest_fixture(&mut book, original);
    }

    let ids = [
        OrderId::new(30),
        OrderId::new(10),
        OrderId::new(40),
        OrderId::new(20),
    ];

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let nodes_before = node_snapshot(&book, &ids);
    let next_before = book.next_queue_priority();
    let arena_before = book.arena.test_snapshot();

    let allocator_policy_before = *book.queue_priority_allocator.policy();
    for _ in 0..32 {
        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(95).unwrap())
        );

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(105).unwrap())
        );

        for id in ids {
            assert_eq!(book.order(id).unwrap().original_order().order_id, id);
        }

        assert_eq!(book.order(OrderId::new(999)), None);

        assert_eq!(book.next_queue_priority(), next_before);
    }

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);

    assert_eq!(book.orders, orders_before);

    assert_eq!(node_snapshot(&book, &ids), nodes_before);

    // Arena 的槽位 + 空闲链表必须完整不变。
    assert_eq!(book.arena.test_snapshot(), arena_before);

    // 查询不得消耗 QueuePriority。
    assert_eq!(book.next_queue_priority(), next_before);

    // 查询同样不能修改分配器策略。
    assert_eq!(
        book.queue_priority_allocator.policy(),
        &allocator_policy_before
    );
}

#[test]
fn order_panics_when_index_references_missing_slot() {
    let mut book = ProductionOrderBook::new(policy());

    let order_id = OrderId::new(1);

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    assert_eq!(book.orders.get(&order_id), Some(&slot));

    // 只破坏 Arena，订单集合索引仍存在。
    let removed = book.arena.remove(slot).unwrap();
    assert_eq!(removed.original_order().order_id, order_id);

    assert!(book.arena.get(slot).is_none());
    assert!(book.orders.contains_key(&order_id));

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.order(order_id);
    }));

    assert!(
        result.is_err(),
        "missing indexed arena slot must fail closed"
    );
}

#[test]
fn order_panics_when_index_points_to_different_order() {
    let mut book = ProductionOrderBook::new(policy());

    let first_id = OrderId::new(1);
    let second_id = OrderId::new(2);

    let first_slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second_slot = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    assert_ne!(first_slot, second_slot);

    // PriceLevel / Arena 都仍是真实合法链。
    // 仅破坏 OrderId -> OrderIndex 映射。
    book.orders.insert(first_id, second_slot);

    assert_eq!(
        book.arena
            .get(second_slot)
            .unwrap()
            .original_order()
            .order_id,
        second_id
    );

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.order(first_id);
    }));

    assert!(
        result.is_err(),
        "index pointing to another order must fail closed"
    );

    // 被错误指向的第二张订单自身仍可正常查询。
    assert_eq!(
        book.order(second_id).unwrap().original_order().order_id,
        second_id
    );
}
