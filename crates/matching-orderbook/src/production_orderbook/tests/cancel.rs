use super::*;

fn assert_cancel_error_preserves_state(
    book: &mut ProductionOrderBook,
    order_id: OrderId,
    expected: RejectReason,
) {
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();

    let policy_before = *book.queue_priority_allocator.policy();

    assert_eq!(book.cancel(order_id), Err(expected));

    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "failed cancel mutated bids"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "failed cancel mutated asks"
    );

    assert_eq!(book.orders, orders_before, "failed cancel mutated ID index");

    assert_eq!(
        book.arena.test_snapshot(),
        arena_before,
        "failed cancel mutated Arena slots/free-list"
    );

    assert_eq!(
        book.queue_priority_allocator.next(),
        next_before,
        "failed cancel consumed QueuePriority"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "failed cancel mutated allocator policy"
    );

    assert_eq!(validate_book(book), Ok(()));
}

#[test]
fn cancel_single_order_removes_level_on_both_sides() {
    for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
        let mut book = ProductionOrderBook::new(policy());

        let original = order(1, side, price, 7);

        book.rest(original.clone()).unwrap();

        let next_before = book.next_queue_priority();
        let policy_before = *book.queue_priority_allocator.policy();

        let removed = book.cancel(OrderId::new(1)).unwrap();

        assert_eq!(removed.original_order(), &original);
        assert_eq!(removed.remaining(), 7);
        assert_eq!(removed.priority(), QueuePriority::new(0));

        // 低层删除必须清空侵入式链接。
        assert_eq!(removed.prev(), None);
        assert_eq!(removed.next(), None);

        assert_eq!(book.order(OrderId::new(1)), None);

        match side {
            Side::Buy => {
                assert!(book.bids.is_empty());
                assert_eq!(book.best_price(Side::Buy), None);
            }

            Side::Sell => {
                assert!(book.asks.is_empty());
                assert_eq!(book.best_price(Side::Sell), None);
            }
        }

        // 取消不消费分配器。
        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn cancel_same_price_head_middle_and_tail_preserves_fifo() {
    for target in [1u128, 2, 3] {
        let mut book = ProductionOrderBook::new(policy());

        for id in [1u128, 2, 3] {
            book.rest(order(id, Side::Buy, 100, id as u64)).unwrap();
        }

        let next_before = book.next_queue_priority();

        let removed = book.cancel(OrderId::new(target)).unwrap();

        assert_eq!(removed.original_order().order_id, OrderId::new(target));

        assert_eq!(removed.prev(), None);
        assert_eq!(removed.next(), None);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.order(OrderId::new(target)), None);

        let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

        assert_eq!(level.count(), 2);

        let head = level.head().unwrap();
        let tail = level.tail().unwrap();

        let head_node = book.arena.get(head).unwrap();
        let tail_node = book.arena.get(tail).unwrap();

        let expected = match target {
            1 => [2u128, 3],
            2 => [1u128, 3],
            3 => [1u128, 2],
            _ => unreachable!(),
        };

        assert_eq!(
            head_node.original_order().order_id,
            OrderId::new(expected[0])
        );

        assert_eq!(
            tail_node.original_order().order_id,
            OrderId::new(expected[1])
        );

        assert_eq!(head_node.next(), Some(tail));

        assert_eq!(tail_node.prev(), Some(head));

        // 剩余节点优先级不得重排。
        assert_eq!(head_node.priority(), QueuePriority::new(expected[0] - 1));

        assert_eq!(tail_node.priority(), QueuePriority::new(expected[1] - 1));

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn cancel_best_level_exposes_next_best_price() {
    let mut book = ProductionOrderBook::new(policy());

    for candidate in [
        order(1, Side::Buy, 90, 1),
        order(2, Side::Buy, 95, 1),
        order(3, Side::Buy, 100, 1),
        order(4, Side::Sell, 105, 1),
        order(5, Side::Sell, 110, 1),
        order(6, Side::Sell, 115, 1),
    ] {
        book.rest(candidate).unwrap();
    }

    let next_before = book.next_queue_priority();

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(100).unwrap())
    );

    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(105).unwrap())
    );

    book.cancel(OrderId::new(3)).unwrap();

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(95).unwrap())
    );

    // 原最优价档已经空，因此必须从树中消失。
    assert!(!book.bids.contains_key(&Price::try_new(100).unwrap()));

    assert_eq!(validate_book(&book), Ok(()));

    book.cancel(OrderId::new(4)).unwrap();

    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(110).unwrap())
    );

    assert!(!book.asks.contains_key(&Price::try_new(105).unwrap()));

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn cancel_unknown_id_is_atomic() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    book.rest(order(2, Side::Sell, 105, 7)).unwrap();

    assert_cancel_error_preserves_state(&mut book, OrderId::new(999), RejectReason::OrderNotFound);
}

#[test]
fn repeated_cancel_returns_order_not_found_without_mutation() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 95, 5)).unwrap();

    let removed = book.cancel(OrderId::new(1)).unwrap();

    assert_eq!(removed.original_order().order_id, OrderId::new(1));

    assert_eq!(validate_book(&book), Ok(()));

    assert_cancel_error_preserves_state(&mut book, OrderId::new(1), RejectReason::OrderNotFound);
}

#[test]
fn canceled_slot_can_be_reused_without_reviving_old_order_id() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(10, Side::Buy, 95, 5)).unwrap();

    let old_slot = *book.orders.get(&OrderId::new(10)).unwrap();

    let removed = book.cancel(OrderId::new(10)).unwrap();

    assert_eq!(removed.priority(), QueuePriority::new(0));

    assert_eq!(book.order(OrderId::new(10)), None);

    assert_eq!(book.next_queue_priority(), 1);

    // Arena 空闲链表应复用刚释放的槽位。
    book.rest(order(20, Side::Buy, 95, 7)).unwrap();

    let new_slot = *book.orders.get(&OrderId::new(20)).unwrap();

    assert_eq!(new_slot, old_slot, "canceled Arena slot should be reused");

    // 相同物理槽位已属于全新的 OrderId。
    assert_eq!(
        book.arena.get(new_slot).unwrap().original_order().order_id,
        OrderId::new(20)
    );

    assert_eq!(
        book.arena.get(new_slot).unwrap().priority(),
        QueuePriority::new(1)
    );

    // 旧标识符映射已彻底解除，不能因为槽位复用命中新订单。
    assert_eq!(book.order(OrderId::new(10)), None);

    assert_eq!(
        book.order(OrderId::new(20))
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(20)
    );

    assert_eq!(book.next_queue_priority(), 2);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn cancel_same_price_sell_head_middle_and_tail_preserves_fifo() {
    for target in [1u128, 2, 3] {
        let mut book = ProductionOrderBook::new(policy());

        for (id, qty) in [(1u128, 4u64), (2u128, 5u64), (3u128, 6u64)] {
            book.rest(order(id, Side::Sell, 100, qty)).unwrap();
        }

        assert_eq!(validate_book(&book), Ok(()));

        let next_before = book.next_queue_priority();
        assert_eq!(next_before, 3);

        let removed = book.cancel(OrderId::new(target)).unwrap();

        assert_eq!(removed.original_order().order_id, OrderId::new(target));
        assert_eq!(removed.priority(), QueuePriority::new(target - 1));
        assert_eq!(removed.prev(), None);
        assert_eq!(removed.next(), None);

        // 取消不能消耗 / 重排 QueuePriority。
        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.order(OrderId::new(target)), None);

        let level = book.asks.get(&Price::try_new(100).unwrap()).unwrap();

        assert_eq!(level.count(), 2);

        let expected_qty = match target {
            1 => 11, // 5 + 6
            2 => 10, // 4 + 6
            3 => 9,  // 4 + 5
            _ => unreachable!(),
        };

        assert_eq!(level.total_visible_qty(), expected_qty);

        let expected_ids = match target {
            1 => [2u128, 3],
            2 => [1u128, 3],
            3 => [1u128, 2],
            _ => unreachable!(),
        };

        let head = level.head().unwrap();
        let tail = level.tail().unwrap();

        assert_ne!(head, tail);

        let head_node = book.arena.get(head).unwrap();
        let tail_node = book.arena.get(tail).unwrap();

        // 先进先出剩余节点顺序。
        assert_eq!(
            head_node.original_order().order_id,
            OrderId::new(expected_ids[0])
        );
        assert_eq!(
            tail_node.original_order().order_id,
            OrderId::new(expected_ids[1])
        );

        // 两节点链必须完整互反。
        assert_eq!(head_node.prev(), None);
        assert_eq!(head_node.next(), Some(tail));

        assert_eq!(tail_node.prev(), Some(head));
        assert_eq!(tail_node.next(), None);

        // 剩余节点优先级保持原值，不能因为取消压缩。
        assert_eq!(
            head_node.priority(),
            QueuePriority::new(expected_ids[0] - 1)
        );
        assert_eq!(
            tail_node.priority(),
            QueuePriority::new(expected_ids[1] - 1)
        );

        assert_eq!(book.next_queue_priority(), 3);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

fn assert_cancel_panics_without_mutation(book: &mut ProductionOrderBook, order_id: OrderId) {
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();

    // 完整包含已占用槽位 + 空闲链表。
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();

    let policy_before = *book.queue_priority_allocator.policy();

    // 这里必须直接调用取消。
    //
    // 不允许先调用 `order()` 或 `validate_book()`，
    // 因为当前订单簿本身就是有意构造的损坏状态。
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.cancel(order_id);
    }));

    assert!(result.is_err(), "corrupted cancel path must fail closed");

    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "cancel panic path mutated bids"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "cancel panic path mutated asks"
    );

    assert_eq!(
        book.orders, orders_before,
        "cancel panic path mutated ID index"
    );

    assert_eq!(
        book.arena.test_snapshot(),
        arena_before,
        "cancel panic path mutated Arena slots/free-list"
    );

    assert_eq!(
        book.queue_priority_allocator.next(),
        next_before,
        "cancel panic path changed next priority"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "cancel panic path changed allocator policy"
    );
}

#[test]
fn cancel_panics_without_mutation_when_index_points_to_freed_slot() {
    let mut book = ProductionOrderBook::new(policy());

    let order_id = OrderId::new(1);

    book.rest(order(1, Side::Buy, 100, 5)).unwrap();

    let slot = *book.orders.get(&order_id).unwrap();

    let removed = book.arena.remove(slot).unwrap();

    assert_eq!(removed.original_order().order_id, order_id);

    // 订单集合与 PriceLevel 故意保留，只有 Arena 槽位被释放。
    assert!(book.orders.contains_key(&order_id));
    assert!(book.arena.get(slot).is_none());

    assert_cancel_panics_without_mutation(&mut book, order_id);
}

#[test]
fn cancel_panics_without_mutation_when_index_points_to_other_live_order() {
    let mut book = ProductionOrderBook::new(policy());

    let first_id = OrderId::new(1);
    let second_id = OrderId::new(2);

    book.rest(order(1, Side::Buy, 100, 5)).unwrap();

    book.rest(order(2, Side::Buy, 100, 7)).unwrap();

    let second_slot = *book.orders.get(&second_id).unwrap();

    // 故意让 first_id 指向第二张有效订单。
    book.orders.insert(first_id, second_slot);

    assert_cancel_panics_without_mutation(&mut book, first_id);
}

#[test]
fn cancel_panics_without_mutation_when_node_side_points_to_missing_level() {
    let mut book = ProductionOrderBook::new(policy());

    let order_id = OrderId::new(1);

    book.rest(order(1, Side::Buy, 100, 5)).unwrap();

    let slot = *book.orders.get(&order_id).unwrap();

    let removed = book.arena.remove(slot).unwrap();
    let priority = removed.priority();

    // 原节点真实位于 `bids`[100]，
    // 但替换后的节点自述为卖出，因此取消会寻找 `asks`[100]。
    let replacement = OrderNode::new(order(1, Side::Sell, 100, 5), priority);

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    // `asks`[100] 不存在。
    assert!(!book.asks.contains_key(&Price::try_new(100).unwrap()));

    assert_cancel_panics_without_mutation(&mut book, order_id);
}

#[test]
fn cancel_panics_without_mutation_when_node_price_points_to_missing_level() {
    let mut book = ProductionOrderBook::new(policy());

    let order_id = OrderId::new(1);

    book.rest(order(1, Side::Buy, 100, 5)).unwrap();

    let slot = *book.orders.get(&order_id).unwrap();

    let removed = book.arena.remove(slot).unwrap();
    let priority = removed.priority();

    // PriceLevel 仍然是真实的 `bids`[100]，
    // 但节点现在自述价格=101。
    let replacement = OrderNode::new(order(1, Side::Buy, 101, 5), priority);

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    assert!(!book.bids.contains_key(&Price::try_new(101).unwrap()));

    assert_cancel_panics_without_mutation(&mut book, order_id);
}

#[test]
fn cancel_panics_without_mutation_when_exact_price_level_is_missing() {
    for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
        let mut book = ProductionOrderBook::new(policy());

        let order_id = OrderId::new(1);
        let price = Price::try_new(price).unwrap();

        book.rest(LimitOrder {
            order_id,
            user_id: UserId::new(1),
            side,
            price,
            qty: Qty::try_new(5).unwrap(),
        })
        .unwrap();

        // Arena 与标识符映射都仍然存在；
        // 只删除节点应该所属的准确价格档。
        let removed_level = match side {
            Side::Buy => book.bids.remove(&price),
            Side::Sell => book.asks.remove(&price),
        };

        assert!(removed_level.is_some());

        assert_cancel_panics_without_mutation(&mut book, order_id);
    }
}
