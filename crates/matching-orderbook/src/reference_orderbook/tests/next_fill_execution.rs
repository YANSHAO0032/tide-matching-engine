//! 验证 `next`-成交执行、连续成交、TradeEvent 与固定命令带确定性。
use super::*;
use matching_domain::{OrderId, Price, Qty, UserId};

/// 构造指定方向、价格和数量的订单测试数据。
fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 将指定订单包装为尚未成交的新到订单运行态。
fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
    IncomingOrder::new(order(id, side, price, qty))
}

/// 保存源 `Vec` 顺序、原始订单、剩余量和优先级。
fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
    book.orders
        .iter()
        .map(|maker| {
            (
                maker.original_order().clone(),
                maker.remaining(),
                maker.priority(),
            )
        })
        .collect()
}

#[test]
fn cross_price_uses_maker_identity_and_price() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let mut taker = incoming(2, side, taker_price, 3);
        let original = taker.original_order().clone();
        let priority = book.orders[0].priority();

        let result = book.execute_next_fill(&mut taker);

        assert_eq!(
            result,
            Ok(Some((
                OrderId::new(1),
                Price::try_new(maker_price).unwrap(),
                Qty::try_new(3).unwrap(),
            )))
        );

        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].priority(), priority);
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
    }
}

#[test]
fn exact_fill_removes_maker_and_exhausts_incoming() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let mut taker = incoming(2, side, taker_price, 10);
        let original = taker.original_order().clone();
        let next_priority = book.next_priority;

        assert_eq!(
            book.execute_next_fill(&mut taker),
            Ok(Some((
                OrderId::new(1),
                Price::try_new(maker_price).unwrap(),
                Qty::try_new(10).unwrap(),
            )))
        );

        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, next_priority);

        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
    }
}

#[test]
fn partial_maker_keeps_priority() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let priority = book.orders[0].priority();
        let next_priority = book.next_priority;

        let mut taker = incoming(2, side, taker_price, 6);

        assert!(matches!(
            book.execute_next_fill(&mut taker),
            Ok(Some((_, _, qty))) if qty.get() == 6
        ));

        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].remaining(), 4);
        assert_eq!(book.orders[0].priority(), priority);
        assert_eq!(book.next_priority, next_priority);
        assert_eq!(taker.remaining(), 0);
    }
}

#[test]
fn filled_maker_preserves_incoming_remainder() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 4)).unwrap();

        let mut taker = incoming(2, side, taker_price, 10);
        let original = taker.original_order().clone();

        assert!(matches!(
            book.execute_next_fill(&mut taker),
            Ok(Some((_, _, qty))) if qty.get() == 4
        ));

        assert!(book.orders.is_empty());

        // 剩余量保留在新到订单，不自动挂单。
        assert_eq!(taker.remaining(), 6);
        assert_eq!(taker.original_order(), &original);
    }
}

#[test]
fn execution_uses_current_incoming_remaining() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 8)).unwrap();

        let mut taker = incoming(2, side, taker_price, 10);

        taker.apply_fill(Qty::try_new(7).unwrap()).unwrap();

        let original = taker.original_order().clone();
        let priority = book.orders[0].priority();

        assert_eq!(
            book.execute_next_fill(&mut taker),
            Ok(Some((
                OrderId::new(1),
                Price::try_new(maker_price).unwrap(),
                Qty::try_new(3).unwrap(),
            )))
        );

        assert_eq!(book.orders[0].remaining(), 5);
        assert_eq!(book.orders[0].priority(), priority);
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
    }
}

#[test]
fn no_plan_preserves_all_state() {
    for (side, incoming_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        book.rest(order(2, Side::Buy, 95, 10)).unwrap();

        let mut taker = incoming(3, side, incoming_price, 10);

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let original = taker.original_order().clone();
        let remaining = taker.remaining();

        assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(taker.original_order(), &original);
        assert_eq!(taker.remaining(), remaining);
    }
}

#[test]
fn exhausted_incoming_returns_none() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let mut taker = incoming(2, side, taker_price, 10);

        taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();

        assert!(book.best_crossing_maker(taker.original_order()).is_some());

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let original = taker.original_order().clone();

        assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(taker.original_order(), &original);
        assert_eq!(taker.remaining(), 0);
    }
}
#[test]
fn multi_fill_respects_price_then_fifo_and_maker_prices_on_both_sides() {
    for (side, maker_side, taker_price, better, worse) in [
        (Side::Buy, Side::Sell, 120, 105, 110),
        (Side::Sell, Side::Buy, 80, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();
        // 故意让更差价格先挂入；同价订单的标识符与先进先出顺序相反。
        book.rest(order(90, maker_side, worse, 3)).unwrap();
        book.rest(order(30, maker_side, better, 2)).unwrap();
        book.rest(order(10, maker_side, better, 4)).unwrap();
        let next_before = book.next_priority;
        let mut taker = incoming(200, side, taker_price, 8);
        let original = taker.original_order().clone();

        let fills = book.execute_fills(&mut taker).unwrap();
        assert_eq!(
            fills,
            vec![
                (
                    OrderId::new(30),
                    Price::try_new(better).unwrap(),
                    Qty::try_new(2).unwrap()
                ),
                (
                    OrderId::new(10),
                    Price::try_new(better).unwrap(),
                    Qty::try_new(4).unwrap()
                ),
                (
                    OrderId::new(90),
                    Price::try_new(worse).unwrap(),
                    Qty::try_new(2).unwrap()
                ),
            ]
        );
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].original_order().order_id, OrderId::new(90));
        assert_eq!(book.orders[0].remaining(), 1);
        assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
        assert_eq!(book.next_priority, next_before);
    }
}

#[test]
fn multi_fill_keeps_unmatched_incoming_and_does_not_rest_it() {
    for (side, maker_side, taker_price, maker_price) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, maker_side, maker_price, 2)).unwrap();
        book.rest(order(2, maker_side, maker_price, 3)).unwrap();
        let next_before = book.next_priority;
        let mut taker = incoming(100, side, taker_price, 9);
        let original = taker.original_order().clone();

        assert_eq!(
            book.execute_fills(&mut taker).unwrap(),
            vec![
                (
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(2).unwrap()
                ),
                (
                    OrderId::new(2),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(3).unwrap()
                ),
            ]
        );
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, next_before);
        assert_eq!(taker.remaining(), 4);
        assert_eq!(taker.original_order(), &original);
    }
}

#[test]
fn multi_fill_stops_at_non_crossing_price_and_preserves_resting_order() {
    for (side, maker_side, taker_price, crossing, non_crossing) in [
        (Side::Buy, Side::Sell, 107, 105, 110),
        (Side::Sell, Side::Buy, 93, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, maker_side, non_crossing, 6)).unwrap();
        book.rest(order(2, maker_side, crossing, 3)).unwrap();
        let kept_original = book.orders[0].original_order().clone();
        let kept_priority = book.orders[0].priority();
        let next_before = book.next_priority;
        let mut taker = incoming(100, side, taker_price, 8);
        let original = taker.original_order().clone();

        assert_eq!(
            book.execute_fills(&mut taker).unwrap(),
            vec![(
                OrderId::new(2),
                Price::try_new(crossing).unwrap(),
                Qty::try_new(3).unwrap()
            )]
        );
        assert_eq!(taker.remaining(), 5);
        assert_eq!(taker.original_order(), &original);
        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].original_order(), &kept_original);
        assert_eq!(book.orders[0].remaining(), 6);
        assert_eq!(book.orders[0].priority(), kept_priority);
        assert_eq!(book.next_priority, next_before);
    }
}

#[test]
fn multi_fill_with_no_plan_or_exhausted_incoming_is_read_only() {
    for (side, taker_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, Side::Sell, 105, 10)).unwrap();
        book.rest(order(2, Side::Buy, 95, 10)).unwrap();
        let before = snapshot(&book);
        let next_before = book.next_priority;
        let mut taker = incoming(100, side, taker_price, 10);
        let original = taker.original_order().clone();

        assert!(book.execute_fills(&mut taker).unwrap().is_empty());
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(taker.remaining(), 10);
        assert_eq!(taker.original_order(), &original);
    }

    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, Side::Sell, 105, 10)).unwrap();
    let before = snapshot(&book);
    let next_before = book.next_priority;
    let mut taker = incoming(100, Side::Buy, 110, 10);
    taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();
    let original = taker.original_order().clone();

    assert!(book.execute_fills(&mut taker).unwrap().is_empty());
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, next_before);
    assert_eq!(taker.remaining(), 0);
    assert_eq!(taker.original_order(), &original);
}

#[test]
fn empty_book_execute_fills_preserves_all_state() {
    let mut book = ReferenceOrderBook::new();
    let mut taker = incoming(100, Side::Buy, 105, 10);

    let book_before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = taker.original_order().clone();
    let remaining_before = taker.remaining();

    let fills = book.execute_fills(&mut taker).unwrap();

    // 空簿不产生任何成交。
    assert!(fills.is_empty());

    // 订单簿及优先级计数器保持不变。
    assert_eq!(snapshot(&book), book_before);
    assert!(book.orders.is_empty());
    assert_eq!(book.next_priority, priority_before);

    // IncomingOrder 的原始订单及剩余量保持不变。
    assert_eq!(taker.original_order(), &original_before);
    assert_eq!(taker.remaining(), remaining_before);
}
#[test]
fn trade_event_uses_supplied_id_and_maker_price() {
    for (side, maker_side, maker_price, taker_price) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        let mut taker = incoming(2, side, taker_price, 3);
        let trade_id = TradeId::new(100);

        let trade = book
            .execute_next_fill_trade(&mut taker, trade_id)
            .unwrap()
            .unwrap();

        assert_eq!(trade.trade_id, trade_id);
        assert_eq!(trade.maker_order_id, OrderId::new(1));
        assert_eq!(trade.taker_order_id, OrderId::new(2));
        assert_eq!(trade.price, Price::try_new(maker_price).unwrap());
        assert_eq!(trade.qty.get(), 3);
        assert_eq!(trade.maker_side, maker_side);

        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(taker.remaining(), 0);
    }
}
#[test]
fn trade_event_preserves_execution_semantics() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 4)).unwrap();

    let mut taker = incoming(2, Side::Buy, 110, 10);

    let trade = book
        .execute_next_fill_trade(&mut taker, TradeId::new(101))
        .unwrap()
        .unwrap();

    assert_eq!(trade.qty.get(), 4);
    assert_eq!(trade.price.get(), 105);
    assert_eq!(trade.maker_side, Side::Sell);

    assert!(book.orders.is_empty());
    assert_eq!(taker.remaining(), 6);
    assert_eq!(taker.original_order().qty.get(), 10);
}
#[test]
fn no_fill_produces_no_trade_event() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 10)).unwrap();

    let mut taker = incoming(2, Side::Buy, 100, 10);

    let before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = taker.original_order().clone();
    let remaining_before = taker.remaining();

    let result = book
        .execute_next_fill_trade(&mut taker, TradeId::new(102))
        .unwrap();

    assert!(result.is_none());
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, priority_before);
    assert_eq!(taker.original_order(), &original_before);
    assert_eq!(taker.remaining(), remaining_before);
}
/// 在独立空簿上执行固定命令带，供确定性回归比较。
fn run_fixed_command_tape() -> (
    Vec<TradeEvent>,
    Vec<(LimitGtcOrder, u64, QueuePriority)>,
    u128,
) {
    // 每次独立从空簿开始。
    let mut book = ReferenceOrderBook::new();
    let mut trades = Vec::new();

    // 固定挂单顺序，优先级分别为 0、1、2、3。
    book.rest(order(30, Side::Sell, 105, 3)).unwrap();
    book.rest(order(10, Side::Sell, 105, 7)).unwrap();
    book.rest(order(20, Side::Buy, 95, 5)).unwrap();
    book.rest(order(40, Side::Sell, 110, 6)).unwrap();

    // 买入 @105，初始数量 8。
    let mut taker = incoming(100, Side::Buy, 105, 8);

    // 第一笔：挂单 30 完全成交 3。
    trades.push(
        book.execute_next_fill_trade(&mut taker, TradeId::new(501))
            .unwrap()
            .expect("first trade"),
    );

    // 第二笔：挂单 10 成交 5，剩余 2。
    trades.push(
        book.execute_next_fill_trade(&mut taker, TradeId::new(502))
            .unwrap()
            .expect("second trade"),
    );

    // 新到订单已耗尽，不应继续成交。
    assert_eq!(taker.remaining(), 0);
    assert_eq!(taker.original_order().qty.get(), 8);

    // 取消部分成交后的挂单 10。
    let canceled = book.cancel(OrderId::new(10)).unwrap();

    assert_eq!(canceled.original_order().order_id, OrderId::new(10));
    assert_eq!(canceled.original_order().qty.get(), 7);
    assert_eq!(canceled.remaining(), 2);
    assert_eq!(canceled.priority(), QueuePriority::new(1));

    let final_snapshot = snapshot(&book);
    let next_priority = book.next_priority;

    (trades, final_snapshot, next_priority)
}
#[test]
fn fixed_command_tape_is_deterministic() {
    let first = run_fixed_command_tape();
    let second = run_fixed_command_tape();

    // 两次完全独立运行，结果必须相同。
    assert_eq!(first, second);

    let (trades, final_book, next_priority) = first;

    // 手工定义预期事件顺序。
    let expected_trades = vec![
        TradeEvent {
            trade_id: TradeId::new(501),
            maker_order_id: OrderId::new(30),
            taker_order_id: OrderId::new(100),
            price: Price::try_new(105).unwrap(),
            qty: Qty::try_new(3).unwrap(),
            maker_side: Side::Sell,
        },
        TradeEvent {
            trade_id: TradeId::new(502),
            maker_order_id: OrderId::new(10),
            taker_order_id: OrderId::new(100),
            price: Price::try_new(105).unwrap(),
            qty: Qty::try_new(5).unwrap(),
            maker_side: Side::Sell,
        },
    ];

    assert_eq!(trades, expected_trades);

    // 挂单 30 因完全成交被移除。
    // 挂单 10 因取消被移除。
    // 剩余订单保持原来源顺序与优先级。
    let expected_book = vec![
        (order(20, Side::Buy, 95, 5), 5, QueuePriority::new(2)),
        (order(40, Side::Sell, 110, 6), 6, QueuePriority::new(3)),
    ];

    assert_eq!(final_book, expected_book);

    // 成交、移除和取消均不消耗新的优先级。
    assert_eq!(next_priority, 4);
}
