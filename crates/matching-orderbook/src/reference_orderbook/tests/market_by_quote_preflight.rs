use super::*;
use matching_domain::{QuoteBudget, UserId};

fn math(tick_size: i64, lot_size: u64) -> MarketMath {
    MarketMath::try_new(0, 0, 0, tick_size, lot_size).unwrap()
}

fn market(id: u128, side: Side, budget: u128) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        quote_budget: QuoteBudget::try_new(budget).unwrap(),
        market_protection: None,
    }
}

fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitOrder, u64, QueuePriority)>, u128) {
    (
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect(),
        book.next_priority,
    )
}

#[test]
fn market_by_quote_preflight_empty_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let book = ReferenceOrderBook::new();
        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote(&market(100, side, 1000), &math(1, 1))
            .unwrap();

        assert!(plan.fills.is_empty());
        assert_eq!(plan.required_trade_ids, 0);
        assert_eq!(plan.spent_quote.get(), 0);
        assert_eq!(plan.remaining_quote.get(), 1000);
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn market_by_quote_preflight_price_fifo_both_sides() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(20, maker_side, worse, 4)).unwrap();
        book.rest(limit(30, maker_side, better, 3)).unwrap();
        book.rest(limit(10, maker_side, better, 5)).unwrap();

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote(&market(100, taker_side, 2000), &math(1, 1))
            .unwrap();

        assert_eq!(plan.required_trade_ids, 3);

        assert_eq!(
            plan.fills
                .iter()
                .map(|fill| fill.maker_order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );

        assert_eq!(
            plan.fills
                .iter()
                .map(|fill| fill.maker_price.get())
                .collect::<Vec<_>>(),
            vec![better, better, worse]
        );

        assert_eq!(
            plan.fills
                .iter()
                .map(|fill| fill.qty.get())
                .collect::<Vec<_>>(),
            vec![3, 5, 4]
        );

        let expected_spent = 8 * better as u128 + 4 * worse as u128;

        assert_eq!(plan.spent_quote.get(), expected_spent);
        assert_eq!(plan.remaining_quote.get(), 2000 - expected_spent);

        assert_eq!(
            plan.fills
                .iter()
                .map(|fill| fill.notional.get())
                .sum::<u128>(),
            expected_spent
        );

        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn market_by_quote_preflight_partial_budget() {
    let mut book = ReferenceOrderBook::new();
    book.rest(limit(1, Side::Sell, 100, 10)).unwrap();

    let before = snapshot(&book);

    let plan = book
        .preflight_market_by_quote(&market(100, Side::Buy, 350), &math(1, 1))
        .unwrap();

    assert_eq!(plan.required_trade_ids, 1);
    assert_eq!(plan.fills[0].qty.get(), 3);
    assert_eq!(plan.fills[0].notional.get(), 300);
    assert_eq!(plan.spent_quote.get(), 300);
    assert_eq!(plan.remaining_quote.get(), 50);

    assert_eq!(snapshot(&book), before);
}

#[test]
fn market_by_quote_preflight_dust_stops_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 10)).unwrap();

        let before = snapshot(&book);

        let plan = book
            .preflight_market_by_quote(&market(100, taker_side, 99), &math(1, 1))
            .unwrap();

        assert!(plan.fills.is_empty());
        assert_eq!(plan.required_trade_ids, 0);
        assert_eq!(plan.spent_quote.get(), 0);
        assert_eq!(plan.remaining_quote.get(), 99);

        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn market_by_quote_preflight_duplicate_first() {
    let mut book = ReferenceOrderBook::new();

    // 价格 6 不满足 tick_size=5。
    book.rest(limit(1, Side::Sell, 6, 10)).unwrap();

    let before = snapshot(&book);

    let result = book.preflight_market_by_quote(&market(1, Side::Buy, 1000), &math(5, 1));

    assert_eq!(result, Err(RejectReason::DuplicateOrderId));
    assert_eq!(snapshot(&book), before);
}

#[test]
fn market_by_quote_preflight_invalid_tick_is_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.rest(limit(1, Side::Sell, 6, 10)).unwrap();

    let before = snapshot(&book);

    let result = book.preflight_market_by_quote(&market(100, Side::Buy, 1000), &math(5, 1));

    assert_eq!(result, Err(RejectReason::InvalidTick));
    assert_eq!(snapshot(&book), before);
}

#[test]
fn market_by_quote_preflight_invalid_lot_is_atomic() {
    let mut book = ReferenceOrderBook::new();

    // 挂单剩余量=11，不满足 lot_size=10。
    book.rest(limit(1, Side::Sell, 5, 11)).unwrap();

    let before = snapshot(&book);

    let result = book.preflight_market_by_quote(&market(100, Side::Buy, 1000), &math(1, 10));

    assert_eq!(result, Err(RejectReason::InvalidLot));
    assert_eq!(snapshot(&book), before);
}
