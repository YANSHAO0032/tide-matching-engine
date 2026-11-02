//! 参考簿根命令成交身份与失败原子性。
use super::*;
use matching_domain::{ClusterId, ShardId, TradeIdCursor, UserId};

fn input(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

#[test]
fn cursor_allocates_per_real_trade_and_overflow_is_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 100, 1)).unwrap();
    book.rest(input(2, Side::Sell, 100, 1)).unwrap();
    let before = book.clone();
    let mut cursor =
        TradeIdCursor::with_next_index(ClusterId(7), ShardId(9), 12, u64::from(u32::MAX)).unwrap();
    let before_cursor = cursor.clone();
    assert_eq!(
        book.place_limit_gtc_with_cursor(input(3, Side::Buy, 100, 2), &mut cursor),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(book, before);
    assert_eq!(cursor, before_cursor);
    let mut cursor = TradeIdCursor::new(ClusterId(7), ShardId(9), 12);
    let events = book
        .place_limit_gtc_with_cursor(input(3, Side::Buy, 100, 2), &mut cursor)
        .unwrap();
    let ids = events
        .iter()
        .filter_map(|e| match e {
            EngineEvent::Trade(t) => Some(t.trade_id.0),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], (7u128 << 112) | (9u128 << 96) | (12u128 << 32));
    assert_eq!(ids[1], ids[0] + 1);
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn iceberg_replenishments_and_stp_cancels_count_only_trades() {
    let mut book = ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let iceberg = matching_domain::IcebergOrder::try_new(
        input(1, Side::Sell, 100, 4),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    book.place_iceberg(iceberg, &math, &[]).unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
    let events = book
        .place_limit_gtc_with_cursor(input(2, Side::Buy, 100, 4), &mut cursor)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count(),
        4
    );
    assert_eq!(cursor.next_index(), 4);

    let mut book = ReferenceOrderBook::new();
    book.rest(input(3, Side::Sell, 100, 1)).unwrap();
    let mut taker = input(4, Side::Buy, 100, 1);
    taker.user_id = UserId(3);
    taker.stp_mode = matching_domain::StpMode::CancelMaker;
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 4);
    let events = book
        .place_limit_gtc_with_cursor(taker, &mut cursor)
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, EngineEvent::OrderCanceled { .. }))
    );
    assert!(!events.iter().any(|e| matches!(e, EngineEvent::Trade(_))));
    assert_eq!(cursor.next_index(), 0);
}

#[test]
fn cursor_iceberg_incoming_allocates_only_actual_trades() {
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 100, 3)).unwrap();
    let iceberg = matching_domain::IcebergOrder::try_new(
        input(2, Side::Buy, 100, 4),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let events = book
        .place_iceberg_with_cursor(iceberg, &math, &mut cursor)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count(),
        1
    );
    assert_eq!(cursor.next_index(), 1);
    assert_eq!(book.best(Side::Buy).unwrap().total_remaining(), 1);
}

#[test]
fn stop_cursor_overflow_rejects_entire_root() {
    use matching_domain::{
        LogicalTime, PriceReference, PriceReferenceKind, ReservationId, StopLimitOrder, StopOrder,
        TimeInForce, TriggerExpiry,
    };
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 101, 1)).unwrap();
    book.rest(input(2, Side::Sell, 102, 1)).unwrap();
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        order_id: OrderId(3),
        user_id: UserId(3),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId(3),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        price: Price::try_new(102).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    book.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    book.trigger_stops_with_cursor(
        PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: 1,
            price: Price::try_new(90).unwrap(),
        },
        LogicalTime::new(1),
        1,
        &math,
        &mut cursor,
        |_| Ok(()),
    )
    .unwrap();
    let mut cursor =
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 6, u64::from(u32::MAX)).unwrap();
    let before = book.clone();
    let original_cursor = cursor.clone();
    let result = book.trigger_stops_with_cursor(
        PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: 2,
            price: Price::try_new(100).unwrap(),
        },
        LogicalTime::new(1),
        1,
        &math,
        &mut cursor,
        |_| Ok(()),
    );
    assert_eq!(result, Err(RejectReason::ArithmeticOverflow));
    assert_eq!(book, before);
    assert_eq!(cursor, original_cursor);
    let mut legacy = before.clone();
    let mut source = TradeIdCursor::new(ClusterId(1), ShardId(2), 6);
    let ids = [
        source.next_trade_id().unwrap(),
        source.next_trade_id().unwrap(),
    ];
    let reference = PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 2,
        price: Price::try_new(100).unwrap(),
    };
    let expected = legacy
        .trigger_stops(reference, LogicalTime::new(1), 1, &math, &ids, |_| Ok(()))
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 6);
    let actual = book
        .trigger_stops_with_cursor(
            reference,
            LogicalTime::new(1),
            1,
            &math,
            &mut cursor,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn stop_market_quote_iceberg_cursor_keeps_legacy_event_projection() {
    use matching_domain::{
        LogicalTime, PriceReference, PriceReferenceKind, QuoteBudget, ReservationId,
        StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce, TriggerExpiry,
    };
    let mut book = ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let iceberg = matching_domain::IcebergOrder::try_new(
        input(1, Side::Sell, 100, 4),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    book.place_iceberg(iceberg, &math, &[]).unwrap();
    let stop = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId(3),
        user_id: UserId(3),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId(3),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(200).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    book.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    let first = PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(90).unwrap(),
    };
    book.trigger_stops(first, LogicalTime::new(1), 1, &math, &[], |_| Ok(()))
        .unwrap();
    let mut legacy = book.clone();
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 7);
    let ids = [
        generator.next_trade_id().unwrap(),
        generator.next_trade_id().unwrap(),
    ];
    let next = PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 2,
        price: Price::try_new(100).unwrap(),
    };
    let expected = legacy
        .trigger_stops(next, LogicalTime::new(1), 1, &math, &ids, |_| Ok(()))
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 7);
    let actual = book
        .trigger_stops_with_cursor(next, LogicalTime::new(1), 1, &math, &mut cursor, |_| Ok(()))
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn immediate_cursor_matches_legacy_and_rejects_fok_without_consuming_ids() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 100, 2)).unwrap();
    let mut legacy = book.clone();
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 8);
    let ids = [generator.next_trade_id().unwrap()];
    let expected = legacy
        .place_limit_ioc(input(2, Side::Buy, 100, 3), &math, &ids)
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 8);
    let actual = book
        .place_limit_immediate_with_cursor(input(2, Side::Buy, 100, 3), &math, false, &mut cursor)
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
    assert_eq!(cursor.next_index(), 1);
    let mut empty = ReferenceOrderBook::new();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 9);
    assert_eq!(
        empty.place_limit_immediate_with_cursor(
            input(3, Side::Buy, 100, 2),
            &math,
            true,
            &mut cursor
        ),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(cursor.next_index(), 0);
}

#[test]
fn market_base_cursor_matches_legacy_trade_projection() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 100, 1)).unwrap();
    let mut legacy = book.clone();
    let order = matching_domain::MarketByBase {
        order_id: OrderId(2),
        user_id: UserId(2),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 10);
    let ids = [generator.next_trade_id().unwrap()];
    let expected = legacy
        .execute_market_by_base(order.clone(), &ids, &math)
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 10);
    let actual = book
        .execute_market_by_base_with_cursor(order, &math, &mut cursor)
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
    assert_eq!(cursor.next_index(), 1);
}

#[test]
fn unprotected_stp_market_base_cursor_preserves_legacy_numeric_path() {
    let math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
    let mut book = ReferenceOrderBook::new();
    let mut maker = input(1, Side::Sell, 100, 1);
    maker.user_id = UserId(1);
    book.rest(maker).unwrap();
    let mut legacy = book.clone();
    let order = matching_domain::MarketByBase {
        order_id: OrderId(2),
        user_id: UserId(2),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::CancelMaker,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 10);
    let ids = [generator.next_trade_id().unwrap()];
    let expected = legacy.execute_market_by_base(order.clone(), &ids, &math);
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 10);
    let actual = book.execute_market_by_base_with_cursor(order, &math, &mut cursor);
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
}

#[test]
fn market_quote_cursor_matches_legacy_trade_and_budget() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest(input(1, Side::Sell, 100, 2)).unwrap();
    let mut legacy = book.clone();
    let order = matching_domain::MarketByQuote {
        order_id: OrderId(2),
        user_id: UserId(2),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(150).unwrap(),
        market_protection: None,
    };
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 11);
    let ids = [generator.next_trade_id().unwrap()];
    let expected = legacy
        .execute_market_by_quote(order.clone(), &math, &ids)
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 11);
    let actual = book
        .execute_market_by_quote_with_cursor(order, &math, &mut cursor)
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book, legacy);
    assert_eq!(cursor.next_index(), 1);
}
