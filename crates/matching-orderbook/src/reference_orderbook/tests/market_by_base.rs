use super::*;
use matching_domain::{MarketByBase, UserId};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn market(id: u128, side: Side, qty: u64) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection: None,
    }
}

fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
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
fn market_by_base_empty_book_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        let before = snapshot(&book);

        let trades = book
            .execute_market_by_base(
                market(100, side, 10),
                &[],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert!(trades.is_empty());
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn market_by_base_partially_fills_maker_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 105, 10)).unwrap();

        let priority_before = book.next_priority;

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 3),
                &[TradeId::new(500)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, TradeId::new(500));
        assert_eq!(trades[0].maker_order_id, OrderId::new(1));
        assert_eq!(trades[0].taker_order_id, OrderId::new(100));
        assert_eq!(trades[0].price.get(), 105);
        assert_eq!(trades[0].qty.get(), 3);
        assert_eq!(trades[0].maker_side, maker_side);

        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.next_priority, priority_before);
    }
}
#[test]
fn market_by_base_respects_price_priority_and_fifo() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = ReferenceOrderBook::new();

        // 源 Vec 顺序故意不同于价格优先顺序和 OrderId 顺序。
        book.rest(limit(20, maker_side, worse, 4)).unwrap();
        book.rest(limit(30, maker_side, better, 3)).unwrap();
        book.rest(limit(10, maker_side, better, 5)).unwrap();

        let priority_before = book.next_priority;

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 10),
                &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 3);

        assert_eq!(
            trades.iter().map(|t| t.maker_order_id).collect::<Vec<_>>(),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );

        assert_eq!(
            trades.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
            vec![better, better, worse]
        );

        assert_eq!(
            trades.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
            vec![3, 5, 2]
        );

        assert_eq!(
            trades.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
            vec![TradeId::new(501), TradeId::new(502), TradeId::new(503)]
        );

        // 最后一张挂单部分成交，保留原始载荷和优先级。
        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].original_order().order_id, OrderId::new(20));
        assert_eq!(book.orders[0].remaining(), 2);
        assert_eq!(book.next_priority, priority_before);
    }
}
#[test]
fn market_by_base_discards_unfilled_taker_remainder() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 3)).unwrap();

        let priority_before = book.next_priority;

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 10),
                &[TradeId::new(600)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].qty.get(), 3);

        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, priority_before);
    }
}
#[test]
fn market_by_base_duplicate_id_is_failure_atomic() {
    let mut book = ReferenceOrderBook::new();

    book.rest(limit(1, Side::Sell, 100, 10)).unwrap();

    let before = snapshot(&book);

    let result = book.execute_market_by_base(
        market(1, Side::Buy, 5),
        &[],
        &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
    );

    assert_eq!(result, Err(RejectReason::DuplicateOrderId));
    assert_eq!(snapshot(&book), before);
}
#[test]
fn market_by_base_insufficient_trade_ids_preserves_state() {
    let mut book = ReferenceOrderBook::new();

    book.rest(limit(30, Side::Sell, 105, 3)).unwrap();
    book.rest(limit(10, Side::Sell, 110, 7)).unwrap();

    let before = snapshot(&book);

    // 需要两笔成交，却只提供一个 TradeId。
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.execute_market_by_base(
            market(100, Side::Buy, 10),
            &[TradeId::new(700)],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        );
    }));

    assert!(result.is_err());
    assert_eq!(snapshot(&book), before);
}
