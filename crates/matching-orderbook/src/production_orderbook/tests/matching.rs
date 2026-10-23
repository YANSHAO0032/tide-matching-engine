use super::*;

#[test]
fn head_partial_fill_keeps_production_book_valid_on_both_sides() {
    for (side, raw_price) in [(Side::Buy, 95), (Side::Sell, 105)] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, side, raw_price, 10)).unwrap();
        book.rest(order(2, side, raw_price, 7)).unwrap();

        let price = Price::try_new(raw_price).unwrap();

        let orders_before = book.orders.clone();
        let next_before = book.next_queue_priority();
        let policy_before = *book.queue_priority_allocator.policy();

        match side {
            Side::Buy => {
                book.bids
                    .get_mut(&price)
                    .unwrap()
                    .apply_head_partial_fill(&mut book.arena, Qty::try_new(3).unwrap());
            }

            Side::Sell => {
                book.asks
                    .get_mut(&price)
                    .unwrap()
                    .apply_head_partial_fill(&mut book.arena, Qty::try_new(3).unwrap());
            }
        }

        assert_eq!(book.orders, orders_before);
        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn next_fill_trade_partially_fills_maker_on_both_sides() {
    for (taker_side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let maker_priority = book.order(OrderId::new(1)).unwrap().priority();

        let next_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(2, taker_side, taker_price, 6));

        let trade_id = TradeId::new(100);

        let trade = book
            .execute_next_fill_trade(&mut incoming, trade_id)
            .unwrap()
            .unwrap();

        assert_eq!(trade.trade_id, trade_id);
        assert_eq!(trade.maker_order_id, OrderId::new(1));
        assert_eq!(trade.taker_order_id, OrderId::new(2));
        assert_eq!(trade.price, Price::try_new(maker_price).unwrap());
        assert_eq!(trade.qty, Qty::try_new(6).unwrap());
        assert_eq!(trade.maker_side, maker_side);

        let maker = book.order(OrderId::new(1)).unwrap();

        assert_eq!(maker.remaining(), 4);
        assert_eq!(maker.priority(), maker_priority);

        assert_eq!(incoming.remaining(), 0);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn next_fill_trade_exact_fill_removes_maker_on_both_sides() {
    for (taker_side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let next_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(2, taker_side, taker_price, 10));

        let trade = book
            .execute_next_fill_trade(&mut incoming, TradeId::new(1))
            .unwrap()
            .unwrap();

        assert_eq!(trade.maker_order_id, OrderId::new(1));

        assert_eq!(trade.qty, Qty::try_new(10).unwrap());

        assert_eq!(book.order(OrderId::new(1)), None);

        assert_eq!(book.best_price(maker_side), None);

        assert_eq!(incoming.remaining(), 0);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn next_fill_trade_removes_full_maker_but_keeps_taker_remainder_unrested() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 4)).unwrap();

    let next_before = book.next_queue_priority();

    let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 10));

    let trade = book
        .execute_next_fill_trade(&mut incoming, TradeId::new(1))
        .unwrap()
        .unwrap();

    assert_eq!(trade.qty, Qty::try_new(4).unwrap());

    assert_eq!(incoming.remaining(), 6);

    assert_eq!(book.order(OrderId::new(1)), None);

    // 吃单方没有被偷偷挂进生产簿。
    assert_eq!(book.order(OrderId::new(2)), None);

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn next_fill_trade_selects_best_price_then_fifo_head() {
    let mut book = ProductionOrderBook::new(policy());

    // 故意先挂更差卖价。
    book.rest(order(90, Side::Sell, 110, 5)).unwrap();

    // 最优卖价同价两张，标识符顺序故意与先进先出无关。
    book.rest(order(30, Side::Sell, 105, 5)).unwrap();

    book.rest(order(10, Side::Sell, 105, 5)).unwrap();

    let mut incoming = IncomingOrder::new(order(200, Side::Buy, 120, 2));

    let trade = book
        .execute_next_fill_trade(&mut incoming, TradeId::new(1))
        .unwrap()
        .unwrap();

    assert_eq!(trade.maker_order_id, OrderId::new(30));

    assert_eq!(trade.price, Price::try_new(105).unwrap());

    assert_eq!(book.order(OrderId::new(30)).unwrap().remaining(), 3);

    // 同价第二张完全没动。
    assert_eq!(book.order(OrderId::new(10)).unwrap().remaining(), 5);

    // 更差价格也没动。
    assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 5);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn next_fill_trade_stops_after_one_full_maker() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 3)).unwrap();

    book.rest(order(2, Side::Sell, 105, 4)).unwrap();

    let second_priority = book.order(OrderId::new(2)).unwrap().priority();

    let mut incoming = IncomingOrder::new(order(10, Side::Buy, 110, 10));

    let trade = book
        .execute_next_fill_trade(&mut incoming, TradeId::new(1))
        .unwrap()
        .unwrap();

    assert_eq!(trade.maker_order_id, OrderId::new(1));

    // 第一挂单被完整吃掉。
    assert_eq!(book.order(OrderId::new(1)), None);

    // 本次不能继续吃第二挂单。
    let second = book.order(OrderId::new(2)).unwrap();

    assert_eq!(second.remaining(), 4);
    assert_eq!(second.priority(), second_priority);

    // 新到订单只扣这一笔。
    assert_eq!(incoming.remaining(), 7);

    let level = book.asks.get(&Price::try_new(105).unwrap()).unwrap();

    assert_eq!(level.count(), 1);
    assert_eq!(level.total_visible_qty(), 4);

    assert_eq!(validate_book(&book), Ok(()));
}

fn assert_no_fill_preserves_state(book: &mut ProductionOrderBook, incoming: &mut IncomingOrder) {
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.next_queue_priority();
    let policy_before = *book.queue_priority_allocator.policy();

    let incoming_original = incoming.original_order().clone();

    let incoming_remaining = incoming.remaining();

    let result = book
        .execute_next_fill_trade(incoming, TradeId::new(999))
        .unwrap();

    assert_eq!(result, None);

    assert_eq!(level_snapshot(&book.bids), bids_before);

    assert_eq!(level_snapshot(&book.asks), asks_before);

    assert_eq!(book.orders, orders_before);

    assert_eq!(book.arena.test_snapshot(), arena_before);

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

    assert_eq!(incoming.original_order(), &incoming_original);

    assert_eq!(incoming.remaining(), incoming_remaining);

    assert_eq!(validate_book(book), Ok(()));
}

#[test]
fn next_fill_trade_no_cross_is_pure_noop() {
    for (side, price) in [(Side::Buy, 104), (Side::Sell, 96)] {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        book.rest(order(2, Side::Buy, 95, 10)).unwrap();

        let mut incoming = IncomingOrder::new(order(3, side, price, 5));

        assert_no_fill_preserves_state(&mut book, &mut incoming);
    }
}

#[test]
fn next_fill_trade_exhausted_incoming_is_pure_noop() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 10)).unwrap();

    let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 5));

    incoming.apply_fill(Qty::try_new(5).unwrap()).unwrap();

    assert_eq!(incoming.remaining(), 0);

    assert_no_fill_preserves_state(&mut book, &mut incoming);
}

#[test]
fn next_fill_trade_buy_with_empty_asks_is_pure_noop() {
    let mut book = ProductionOrderBook::new(policy());

    // 只建立同侧 `bids`，明确 `asks` 为空。
    book.rest(order(1, Side::Buy, 95, 10)).unwrap();

    assert!(book.asks.is_empty());
    assert!(!book.bids.is_empty());
    assert_eq!(validate_book(&book), Ok(()));

    let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 6));

    assert_no_fill_preserves_state(&mut book, &mut incoming);
}

#[test]
fn next_fill_trade_sell_with_empty_bids_is_pure_noop() {
    let mut book = ProductionOrderBook::new(policy());

    // 只建立同侧 `asks`，明确 `bids` 为空。
    book.rest(order(1, Side::Sell, 105, 10)).unwrap();

    assert!(book.bids.is_empty());
    assert!(!book.asks.is_empty());
    assert_eq!(validate_book(&book), Ok(()));

    let mut incoming = IncomingOrder::new(order(2, Side::Sell, 90, 6));

    assert_no_fill_preserves_state(&mut book, &mut incoming);
}

#[test]
fn execute_fill_trades_buy_consumes_asks_by_price_then_fifo() {
    let mut book = ProductionOrderBook::new(policy());

    // 故意先放更差价格。
    book.rest(order(90, Side::Sell, 115, 7)).unwrap();

    book.rest(order(30, Side::Sell, 105, 2)).unwrap();

    book.rest(order(10, Side::Sell, 105, 3)).unwrap();

    book.rest(order(40, Side::Sell, 110, 5)).unwrap();

    let maker_40_priority = book.order(OrderId::new(40)).unwrap().priority();

    let next_before = book.next_queue_priority();

    let mut incoming = IncomingOrder::new(order(200, Side::Buy, 110, 8));

    let trade_ids = [TradeId::new(100), TradeId::new(101), TradeId::new(102)];

    let trades = book.execute_fill_trades(&mut incoming, &trade_ids).unwrap();

    assert_eq!(trades.len(), 3);

    assert_eq!(trades[0].trade_id, TradeId::new(100));
    assert_eq!(trades[0].maker_order_id, OrderId::new(30));
    assert_eq!(trades[0].taker_order_id, OrderId::new(200));
    assert_eq!(trades[0].price, Price::try_new(105).unwrap());
    assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
    assert_eq!(trades[0].maker_side, Side::Sell);

    assert_eq!(trades[1].trade_id, TradeId::new(101));
    assert_eq!(trades[1].maker_order_id, OrderId::new(10));
    assert_eq!(trades[1].taker_order_id, OrderId::new(200));
    assert_eq!(trades[1].price, Price::try_new(105).unwrap());
    assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
    assert_eq!(trades[1].maker_side, Side::Sell);

    assert_eq!(trades[2].trade_id, TradeId::new(102));
    assert_eq!(trades[2].maker_order_id, OrderId::new(40));
    assert_eq!(trades[2].taker_order_id, OrderId::new(200));
    assert_eq!(trades[2].price, Price::try_new(110).unwrap());
    assert_eq!(trades[2].qty, Qty::try_new(3).unwrap());
    assert_eq!(trades[2].maker_side, Side::Sell);

    assert_eq!(incoming.remaining(), 0);

    assert_eq!(book.order(OrderId::new(30)), None);
    assert_eq!(book.order(OrderId::new(10)), None);

    let maker_40 = book.order(OrderId::new(40)).unwrap();

    assert_eq!(maker_40.remaining(), 2);
    assert_eq!(maker_40.priority(), maker_40_priority);

    // 115 完全没碰。
    assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 7);

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn execute_fill_trades_sell_consumes_bids_by_price_then_fifo() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(90, Side::Buy, 85, 7)).unwrap();

    book.rest(order(30, Side::Buy, 95, 2)).unwrap();

    book.rest(order(10, Side::Buy, 95, 3)).unwrap();

    book.rest(order(40, Side::Buy, 90, 5)).unwrap();

    let maker_40_priority = book.order(OrderId::new(40)).unwrap().priority();

    let next_before = book.next_queue_priority();

    let mut incoming = IncomingOrder::new(order(200, Side::Sell, 90, 8));

    let trades = book
        .execute_fill_trades(
            &mut incoming,
            &[TradeId::new(200), TradeId::new(201), TradeId::new(202)],
        )
        .unwrap();

    assert_eq!(trades.len(), 3);

    assert_eq!(trades[0].trade_id, TradeId::new(200));
    assert_eq!(trades[0].maker_order_id, OrderId::new(30));
    assert_eq!(trades[0].price, Price::try_new(95).unwrap());
    assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
    assert_eq!(trades[0].maker_side, Side::Buy);

    assert_eq!(trades[1].trade_id, TradeId::new(201));
    assert_eq!(trades[1].maker_order_id, OrderId::new(10));
    assert_eq!(trades[1].price, Price::try_new(95).unwrap());
    assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
    assert_eq!(trades[1].maker_side, Side::Buy);

    assert_eq!(trades[2].trade_id, TradeId::new(202));
    assert_eq!(trades[2].maker_order_id, OrderId::new(40));
    assert_eq!(trades[2].price, Price::try_new(90).unwrap());
    assert_eq!(trades[2].qty, Qty::try_new(3).unwrap());
    assert_eq!(trades[2].maker_side, Side::Buy);

    assert_eq!(incoming.remaining(), 0);

    assert_eq!(book.order(OrderId::new(30)), None);
    assert_eq!(book.order(OrderId::new(10)), None);

    let maker_40 = book.order(OrderId::new(40)).unwrap();

    assert_eq!(maker_40.remaining(), 2);
    assert_eq!(maker_40.priority(), maker_40_priority);

    assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 7);

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn execute_fill_trades_stops_exactly_when_taker_is_exhausted() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 2)).unwrap();

    book.rest(order(2, Side::Sell, 105, 4)).unwrap();

    book.rest(order(3, Side::Sell, 105, 6)).unwrap();

    let third_before = book.order(OrderId::new(3)).unwrap().clone();

    let mut incoming = IncomingOrder::new(order(100, Side::Buy, 105, 5));

    let trades = book
        .execute_fill_trades(
            &mut incoming,
            &[
                TradeId::new(1),
                TradeId::new(2),
                // 多余标识符必须被忽略。
                TradeId::new(3),
            ],
        )
        .unwrap();

    assert_eq!(trades.len(), 2);

    assert_eq!(trades[0].maker_order_id, OrderId::new(1));
    assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());

    assert_eq!(trades[1].maker_order_id, OrderId::new(2));
    assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());

    assert_eq!(incoming.remaining(), 0);

    // 挂单 2 部分成交。
    assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 1);

    // 后续仍可成交挂单完全未触碰。
    assert_eq!(book.order(OrderId::new(3)).unwrap(), &third_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn execute_fill_trades_stops_before_non_crossing_next_price() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 3)).unwrap();

    book.rest(order(2, Side::Sell, 110, 7)).unwrap();

    let next_level_before = book.order(OrderId::new(2)).unwrap().clone();

    let next_priority_before = book.next_queue_priority();

    let mut incoming = IncomingOrder::new(order(100, Side::Buy, 105, 10));

    let trades = book
        .execute_fill_trades(&mut incoming, &[TradeId::new(1)])
        .unwrap();

    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].maker_order_id, OrderId::new(1));

    assert_eq!(incoming.remaining(), 7);

    // 110 > 吃单方限价 105。
    assert_eq!(book.order(OrderId::new(2)).unwrap(), &next_level_before);

    // 新到订单余量没有自动挂单。
    assert_eq!(book.order(OrderId::new(100)), None);

    assert_eq!(book.next_queue_priority(), next_priority_before);

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn execute_fill_trades_empty_or_non_crossing_returns_empty_without_mutation() {
    // 空簿。
    {
        let mut book = ProductionOrderBook::new(policy());

        let mut incoming = IncomingOrder::new(order(100, Side::Buy, 110, 5));

        let arena_before = book.arena.test_snapshot();
        let next_before = book.next_queue_priority();

        let trades = book.execute_fill_trades(&mut incoming, &[]).unwrap();

        assert!(trades.is_empty());
        assert_eq!(incoming.remaining(), 5);
        assert_eq!(book.arena.test_snapshot(), arena_before);
        assert_eq!(book.next_queue_priority(), next_before);
        assert_eq!(validate_book(&book), Ok(()));
    }

    // 有对手盘，但价格不交叉。
    {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 5)).unwrap();

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();
        let next_before = book.next_queue_priority();
        let policy_before = *book.queue_priority_allocator.policy();

        let mut incoming = IncomingOrder::new(order(100, Side::Buy, 104, 5));

        let original_before = incoming.original_order().clone();
        let remaining_before = incoming.remaining();

        let trades = book.execute_fill_trades(&mut incoming, &[]).unwrap();

        assert!(trades.is_empty());

        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);
        assert_eq!(book.orders, orders_before);
        assert_eq!(book.arena.test_snapshot(), arena_before);

        assert_eq!(book.next_queue_priority(), next_before);
        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(incoming.original_order(), &original_before);
        assert_eq!(incoming.remaining(), remaining_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn execute_fill_trades_insufficient_trade_ids_panics_before_any_mutation() {
    let mut book = ProductionOrderBook::new(policy());

    book.rest(order(1, Side::Sell, 105, 2)).unwrap();

    book.rest(order(2, Side::Sell, 105, 3)).unwrap();

    book.rest(order(3, Side::Sell, 110, 5)).unwrap();

    // 数量=8，需要：
    // 订单 1 的成交量为 2
    // 订单 2 的成交量为 3
    // 订单 3 的成交量为 3
    // 共 3 个 TradeId。
    let mut incoming = IncomingOrder::new(order(100, Side::Buy, 110, 8));

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.next_queue_priority();

    let policy_before = *book.queue_priority_allocator.policy();

    let incoming_original_before = incoming.original_order().clone();

    let incoming_remaining_before = incoming.remaining();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.execute_fill_trades(&mut incoming, &[TradeId::new(1), TradeId::new(2)]);
    }));

    assert!(
        result.is_err(),
        "insufficient TradeIds must fail closed before first fill"
    );

    assert_eq!(level_snapshot(&book.bids), bids_before);

    assert_eq!(level_snapshot(&book.asks), asks_before);

    assert_eq!(book.orders, orders_before);

    assert_eq!(book.arena.test_snapshot(), arena_before);

    assert_eq!(book.next_queue_priority(), next_before);

    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

    assert_eq!(incoming.original_order(), &incoming_original_before);

    assert_eq!(incoming.remaining(), incoming_remaining_before);

    assert_eq!(validate_book(&book), Ok(()));
}
