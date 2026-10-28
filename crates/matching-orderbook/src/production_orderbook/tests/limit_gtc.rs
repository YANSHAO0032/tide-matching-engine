use super::*;

#[test]
fn common_rest_preflight_accepts_currently_crossing_order_without_mutation() {
    for (maker_side, maker_price, taker_side, taker_price) in [
        (Side::Sell, 105, Side::Buy, 110),
        (Side::Buy, 95, Side::Sell, 90),
    ] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, maker_side, maker_price, 5)).unwrap();

        let candidate = order(2, taker_side, taker_price, 7);

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        let plan = book.preflight_rest_common(&candidate).unwrap();

        assert_eq!(plan.order_id, candidate.order_id);
        assert_eq!(plan.side, candidate.side);
        assert_eq!(plan.price, candidate.price);

        // 公共预检必须完全只读。
        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);
        assert_eq!(book.orders, orders_before);
        assert_eq!(book.arena.test_snapshot(), arena_before);
        assert_eq!(book.queue_priority_allocator.next(), next_before);
        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn duplicate_rest_precedes_crossing_check() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 5)).unwrap();

    let candidate = order(1, Side::Buy, 110, 5);

    assert_rest_error_preserves_state(&mut book, candidate, RejectReason::DuplicateOrderId);
}

#[test]
fn place_limit_gtc_buy_matches_multi_price_fifo_and_rests_remainder() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(30, Side::Sell, 105, 2)).unwrap();

    book.rest(order(10, Side::Sell, 105, 3)).unwrap();

    book.rest(order(40, Side::Sell, 110, 2)).unwrap();

    book.rest(order(90, Side::Sell, 115, 9)).unwrap();

    let next_before = book.next_queue_priority();

    let trades = book
        .place_limit_gtc(
            order(200, Side::Buy, 110, 10),
            &[TradeId::new(100), TradeId::new(101), TradeId::new(102)],
        )
        .unwrap();

    assert_eq!(trades.len(), 3);

    assert_eq!(
        (
            trades[0].trade_id,
            trades[0].maker_order_id,
            trades[0].price,
            trades[0].qty,
            trades[0].maker_side,
        ),
        (
            TradeId::new(100),
            OrderId::new(30),
            Price::try_new(105).unwrap(),
            Qty::try_new(2).unwrap(),
            Side::Sell,
        )
    );

    assert_eq!(
        (
            trades[1].trade_id,
            trades[1].maker_order_id,
            trades[1].price,
            trades[1].qty,
        ),
        (
            TradeId::new(101),
            OrderId::new(10),
            Price::try_new(105).unwrap(),
            Qty::try_new(3).unwrap(),
        )
    );

    assert_eq!(
        (
            trades[2].trade_id,
            trades[2].maker_order_id,
            trades[2].price,
            trades[2].qty,
        ),
        (
            TradeId::new(102),
            OrderId::new(40),
            Price::try_new(110).unwrap(),
            Qty::try_new(2).unwrap(),
        )
    );

    for trade in &trades {
        assert_eq!(trade.taker_order_id, OrderId::new(200));
    }

    assert_eq!(book.order(OrderId::new(30)), None);
    assert_eq!(book.order(OrderId::new(10)), None);
    assert_eq!(book.order(OrderId::new(40)), None);

    // 115 不可成交，完全没动。
    assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 9);

    // GTC 余量 = 3。
    let resting = book.order(OrderId::new(200)).unwrap();

    assert_eq!(resting.remaining(), 3);
    assert_eq!(resting.original_order().side, Side::Buy);
    assert_eq!(resting.original_order().price, Price::try_new(110).unwrap());
    assert_eq!(resting.original_order().qty, Qty::try_new(3).unwrap());

    // 只有余量挂单消耗一个优先级。
    assert_eq!(book.next_queue_priority(), next_before + 1);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn place_limit_gtc_exact_fill_does_not_allocate_queue_priority() {
    for (maker_side, maker_price, taker_side, taker_price) in [
        (Side::Sell, 105, Side::Buy, 110),
        (Side::Buy, 95, Side::Sell, 90),
    ] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, maker_side, maker_price, 5)).unwrap();

        let next_before = book.next_queue_priority();

        let trades = book
            .place_limit_gtc(order(2, taker_side, taker_price, 5), &[TradeId::new(100)])
            .unwrap();

        assert_eq!(trades.len(), 1);

        assert_eq!(book.order(OrderId::new(1)), None);

        // 吃单方全部成交，不应该进入订单集合。
        assert_eq!(book.order(OrderId::new(2)), None);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn place_limit_gtc_exact_fill_succeeds_when_priority_allocator_is_exhausted() {
    let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

    // 唯一一次合法分配：
    // 挂单优先级=0，之后 `allocator.next`=1 且已耗尽。
    book.rest(order(1, Side::Sell, 105, 5)).unwrap();

    assert_eq!(book.next_queue_priority(), 1);
    assert!(book.queue_priority_allocator.is_exhausted());
    assert_eq!(validate_book(&book), Ok(()));

    let trades = book
        .place_limit_gtc(order(2, Side::Buy, 110, 5), &[TradeId::new(100)])
        .unwrap();

    assert_eq!(trades.len(), 1);

    assert_eq!(trades[0].trade_id, TradeId::new(100));
    assert_eq!(trades[0].maker_order_id, OrderId::new(1));
    assert_eq!(trades[0].taker_order_id, OrderId::new(2));
    assert_eq!(trades[0].price, Price::try_new(105).unwrap());
    assert_eq!(trades[0].qty, Qty::try_new(5).unwrap());
    assert_eq!(trades[0].maker_side, Side::Sell);

    // 挂单全部成交后移除。
    assert_eq!(book.order(OrderId::new(1)), None);

    // 吃单方也全部成交，不允许挂单。
    assert_eq!(book.order(OrderId::new(2)), None);

    assert!(book.asks.is_empty());
    assert!(book.bids.is_empty());

    // 全部成交必须完全绕过挂单优先级预检，
    // 分配器已耗尽也不能影响成交。
    assert_eq!(book.next_queue_priority(), 1);
    assert!(book.queue_priority_allocator.is_exhausted());

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn place_limit_gtc_remainder_priority_exhaustion_is_atomic_before_first_fill() {
    let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

    // 与全部成交测试完全相同的初始状态。
    //
    // 挂单优先级=0；
    // 挂单完成后 `next`=1，并命中测试用上限。
    book.rest(order(1, Side::Sell, 105, 5)).unwrap();

    assert_eq!(book.next_queue_priority(), 1);
    assert!(book.queue_priority_allocator.is_exhausted());
    assert_eq!(validate_book(&book), Ok(()));

    // 买入数量=8：
    //
    // 计划成交挂单数量=5；
    // 余量数量=3；
    // 实际只需要一笔成交，所以一个 TradeId 是足量的。
    //
    // 必须在执行这第一笔成交之前，由余量挂单预检
    // 返回 PrioritySpaceExhaustion。
    assert_place_error_preserves_state(
        &mut book,
        order(2, Side::Buy, 110, 8),
        &[TradeId::new(100)],
        RejectReason::PrioritySpaceExhaustion,
    );

    // 辅助函数已比较完整状态，这里再直接锁定最关键的业务证据：
    // 挂单根本没被成交。
    let maker = book.order(OrderId::new(1)).unwrap();

    assert_eq!(maker.remaining(), 5);
    assert_eq!(maker.original_order().side, Side::Sell);
    assert_eq!(maker.original_order().price, Price::try_new(105).unwrap());

    // 已拒绝吃单方未进入标识符索引 / 订单簿。
    assert_eq!(book.order(OrderId::new(2)), None);

    assert_eq!(book.next_queue_priority(), 1);
    assert!(book.queue_priority_allocator.is_exhausted());

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn place_limit_gtc_non_crossing_order_rests_directly() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 7)).unwrap();

    let next_before = book.next_queue_priority();

    let trades = book
        .place_limit_gtc(order(2, Side::Buy, 100, 5), &[])
        .unwrap();

    assert!(trades.is_empty());

    let resting = book.order(OrderId::new(2)).unwrap();

    assert_eq!(resting.remaining(), 5);

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(100).unwrap())
    );

    assert_eq!(book.next_queue_priority(), next_before + 1);

    assert_eq!(validate_book(&book), Ok(()));
}

fn assert_place_error_preserves_state(
    book: &mut ProductionOrderBook,
    candidate: LimitOrder,
    trade_ids: &[TradeId],
    expected: RejectReason,
) {
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.next_queue_priority();

    let policy_before = *book.queue_priority_allocator.policy();

    assert_eq!(book.place_limit_gtc(candidate, trade_ids,), Err(expected));

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);
    assert_eq!(book.orders, orders_before);
    assert_eq!(book.arena.test_snapshot(), arena_before);
    assert_eq!(book.next_queue_priority(), next_before);
    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

    assert_eq!(validate_book(book), Ok(()));
}

#[test]
fn place_limit_gtc_duplicate_is_rejected_before_matching() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 5)).unwrap();

    // 同标识符，同时也是可以成交的买入。
    assert_place_error_preserves_state(
        &mut book,
        order(1, Side::Buy, 110, 10),
        &[TradeId::new(1)],
        RejectReason::DuplicateOrderId,
    );
}

#[test]
fn place_limit_gtc_insufficient_trade_ids_panics_before_any_fill() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 2)).unwrap();

    book.rest(order(2, Side::Sell, 105, 3)).unwrap();

    book.rest(order(3, Side::Sell, 110, 4)).unwrap();

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.next_queue_priority();

    let policy_before = *book.queue_priority_allocator.policy();

    // 数量=8 需要 3 笔，只给 2 个 TradeId。
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.place_limit_gtc(
            order(100, Side::Buy, 110, 8),
            &[TradeId::new(1), TradeId::new(2)],
        );
    }));

    assert!(result.is_err());

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);
    assert_eq!(book.orders, orders_before);
    assert_eq!(book.arena.test_snapshot(), arena_before);
    assert_eq!(book.next_queue_priority(), next_before);
    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn place_limit_gtc_same_side_aggregate_overflow_fails_before_any_write() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Buy, 100, u64::MAX)).unwrap();

    assert_place_error_preserves_state(
        &mut book,
        order(2, Side::Buy, 100, 1),
        &[],
        RejectReason::ArithmeticOverflow,
    );
}

#[test]
fn place_limit_gtc_sell_matches_multi_price_fifo_and_rests_remainder() {
    let mut book = ProductionOrderBook::new(policy());

    // 更差价格故意先挂，证明匹配不是按插入全局顺序。
    book.rest(order(90, Side::Buy, 85, 9)).unwrap();

    // 最优买价，同价先进先出。
    book.rest(order(30, Side::Buy, 95, 2)).unwrap();

    book.rest(order(10, Side::Buy, 95, 3)).unwrap();

    // 第二价格档。
    book.rest(order(40, Side::Buy, 90, 2)).unwrap();

    let next_before = book.next_queue_priority();

    let trades = book
        .place_limit_gtc(
            order(200, Side::Sell, 90, 10),
            &[TradeId::new(200), TradeId::new(201), TradeId::new(202)],
        )
        .unwrap();

    assert_eq!(trades.len(), 3);

    // 95 的先进先出头节点。
    assert_eq!(trades[0].trade_id, TradeId::new(200));
    assert_eq!(trades[0].maker_order_id, OrderId::new(30));
    assert_eq!(trades[0].taker_order_id, OrderId::new(200));
    assert_eq!(trades[0].price, Price::try_new(95).unwrap());
    assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
    assert_eq!(trades[0].maker_side, Side::Buy);

    // 95 的第二个先进先出挂单。
    assert_eq!(trades[1].trade_id, TradeId::new(201));
    assert_eq!(trades[1].maker_order_id, OrderId::new(10));
    assert_eq!(trades[1].taker_order_id, OrderId::new(200));
    assert_eq!(trades[1].price, Price::try_new(95).unwrap());
    assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
    assert_eq!(trades[1].maker_side, Side::Buy);

    // 然后才是 90。
    assert_eq!(trades[2].trade_id, TradeId::new(202));
    assert_eq!(trades[2].maker_order_id, OrderId::new(40));
    assert_eq!(trades[2].taker_order_id, OrderId::new(200));
    assert_eq!(trades[2].price, Price::try_new(90).unwrap());
    assert_eq!(trades[2].qty, Qty::try_new(2).unwrap());
    assert_eq!(trades[2].maker_side, Side::Buy);

    // 总成交 7，GTC 余量=3。
    let resting = book.order(OrderId::new(200)).unwrap();

    assert_eq!(resting.remaining(), 3);
    assert_eq!(resting.original_order().side, Side::Sell);
    assert_eq!(resting.original_order().price, Price::try_new(90).unwrap());
    assert_eq!(resting.original_order().qty, Qty::try_new(3).unwrap());

    // 余量挂单获得撮合前分配器的下一优先级。
    assert_eq!(resting.priority(), QueuePriority::new(next_before));

    assert_eq!(book.next_queue_priority(), next_before + 1);

    // 被完整成交的挂单已消失。
    assert_eq!(book.order(OrderId::new(30)), None);
    assert_eq!(book.order(OrderId::new(10)), None);
    assert_eq!(book.order(OrderId::new(40)), None);

    // 85 不可成交，完全保留。
    assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 9);

    // 成交完成后：
    // 最优买价 = 85
    // 余量卖出成为最优卖价 = 90
    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(85).unwrap())
    );

    assert_eq!(
        book.best_price(Side::Sell),
        Some(Price::try_new(90).unwrap())
    );

    let level = book.asks.get(&Price::try_new(90).unwrap()).unwrap();

    assert_eq!(level.count(), 1);

    let taker_slot = *book.orders.get(&OrderId::new(200)).unwrap();

    assert_eq!(level.head(), Some(taker_slot));
    assert_eq!(level.tail(), Some(taker_slot));
    assert_eq!(level.total_visible_qty(), 3);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn place_limit_gtc_remainder_reuses_fully_filled_maker_slot() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 2)).unwrap();

    let maker_id = OrderId::new(1);

    let taker_id = OrderId::new(100);

    let maker_slot = *book.orders.get(&maker_id).unwrap();

    let maker_priority = book.order(maker_id).unwrap().priority();

    let next_before = book.next_queue_priority();

    // 买入数量=5:
    // - 全部成交挂单 2
    // - 余量 3 挂单到买价@110
    let trades = book
        .place_limit_gtc(order(100, Side::Buy, 110, 5), &[TradeId::new(10)])
        .unwrap();

    assert_eq!(trades.len(), 1);

    assert_eq!(trades[0].maker_order_id, maker_id);
    assert_eq!(trades[0].taker_order_id, taker_id);
    assert_eq!(trades[0].price, Price::try_new(105).unwrap());
    assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
    assert_eq!(trades[0].maker_side, Side::Sell);

    // 旧挂单已从标识符索引消失。
    assert_eq!(book.order(maker_id), None);

    assert!(!book.orders.contains_key(&maker_id));

    // Arena 空闲链表是后进先出。
    // 挂单 pop_front 释放的槽位应立即被余量挂单复用。
    assert_eq!(book.orders.get(&taker_id), Some(&maker_slot));

    let taker = book.order(taker_id).unwrap();

    assert_eq!(taker.remaining(), 3);

    assert_eq!(taker.original_order().order_id, taker_id);

    assert_eq!(taker.original_order().price, Price::try_new(110).unwrap());

    // 复用槽位 ≠ 复用优先级。
    assert_ne!(taker.priority(), maker_priority);

    assert_eq!(taker.priority(), QueuePriority::new(next_before));

    assert_eq!(book.next_queue_priority(), next_before + 1);

    // 槽位内现在必须是新吃单方，而非失效挂单。
    assert_eq!(
        book.arena
            .get(maker_slot)
            .unwrap()
            .original_order()
            .order_id,
        taker_id
    );

    assert_eq!(book.best_price(Side::Sell), None);

    assert_eq!(
        book.best_price(Side::Buy),
        Some(Price::try_new(110).unwrap())
    );

    assert_eq!(validate_book(&book), Ok(()));
}
