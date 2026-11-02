//! 根命令成交身份游标的生产簿原子性。
use super::*;
use matching_domain::{ClusterId, ShardId, TradeIdCursor};

#[test]
fn gtc_cursor_assigns_actual_trades_in_order_and_rolls_back_overflow() {
    let mut p = ProductionOrderBook::new(policy());
    p.rest(order(1, Side::Sell, 100, 1)).unwrap();
    p.rest(order(2, Side::Sell, 100, 1)).unwrap();
    let taker = order(3, Side::Buy, 100, 2);
    let mut exhausted =
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 9, u64::from(u32::MAX)).unwrap();
    let before = exhausted.clone();
    assert_eq!(
        p.place_limit_gtc_with_cursor(taker.clone(), &mut exhausted),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(exhausted, before);
    assert!(p.order(OrderId::new(1)).is_some());
    assert!(p.order(OrderId::new(2)).is_some());
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 9);
    let result = p.place_limit_gtc_with_cursor(taker, &mut cursor).unwrap();
    let ids: Vec<_> = result
        .iter()
        .filter_map(|e| {
            if let EngineEvent::Trade(t) = e {
                Some(t.trade_id)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        ids,
        vec![
            TradeId::new((1u128 << 112) | (2u128 << 96) | (9u128 << 32)),
            TradeId::new((1u128 << 112) | (2u128 << 96) | (9u128 << 32) | 1)
        ]
    );
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn stp_cancel_does_not_allocate_trade_id() {
    let mut book = ProductionOrderBook::new(policy());
    let mut maker = order(1, Side::Sell, 100, 1);
    maker.user_id = UserId::new(42);
    book.rest(maker).unwrap();
    let mut taker = order(2, Side::Buy, 100, 1);
    taker.user_id = UserId::new(42);
    taker.stp_mode = matching_domain::StpMode::CancelTaker;
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
    let events = book
        .place_limit_gtc_with_cursor(taker, &mut cursor)
        .unwrap();
    assert!(matches!(
        events.as_slice(),
        [EngineEvent::OrderCanceled { .. }]
    ));
    assert_eq!(cursor.next_index(), 0);
    assert!(book.order(OrderId::new(1)).is_some());
}

#[test]
fn iceberg_replenishment_allocates_only_for_real_trades() {
    let mut book = ProductionOrderBook::new(policy());
    let iceberg = matching_domain::IcebergOrder::try_new(
        order(1, Side::Sell, 100, 3),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.place_iceberg_post_only(iceberg, &math).unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
    let events = book
        .place_limit_gtc_with_cursor(order(2, Side::Buy, 100, 3), &mut cursor)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count(),
        3
    );
    assert_eq!(cursor.next_index(), 3);
}

#[test]
fn gtc_cursor_events_match_existing_execution() {
    for iceberg in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        if iceberg {
            let ice = matching_domain::IcebergOrder::try_new(
                order(1, Side::Sell, 100, 3),
                Qty::try_new(1).unwrap(),
            )
            .unwrap();
            book.place_iceberg_post_only(ice, &MarketMath::try_new(0, 0, 0, 1, 1).unwrap())
                .unwrap();
        } else {
            book.rest(order(1, Side::Sell, 100, 1)).unwrap();
            book.rest(order(2, Side::Sell, 100, 1)).unwrap();
        }
        let mut direct = book.clone();
        let mut expected_cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
        let ids: Vec<_> = (0..3)
            .map(|_| expected_cursor.next_trade_id().unwrap())
            .collect();
        let taker = order(5, Side::Buy, 100, 3);
        let expected = direct
            .place_limit_gtc_with_events(taker.clone(), &ids)
            .unwrap();
        let mut actual_cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
        let actual = book
            .place_limit_gtc_with_cursor(taker, &mut actual_cursor)
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            actual_cursor.next_index(),
            actual
                .iter()
                .filter(|event| matches!(event, EngineEvent::Trade(_)))
                .count() as u64
        );
    }
}

#[test]
fn immediate_cursor_fok_and_ioc_match_existing_and_rollback_overflow() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for fok in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Sell, 100, 1)).unwrap();
        book.rest(order(2, Side::Sell, 100, 1)).unwrap();
        let mut direct = book.clone();
        let taker = order(3, Side::Buy, 100, 2);
        let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
        let ids = [
            generator.next_trade_id().unwrap(),
            generator.next_trade_id().unwrap(),
        ];
        let expected = if fok {
            direct.place_limit_fok(taker.clone(), &math, &ids)
        } else {
            direct.place_limit_ioc(taker.clone(), &math, &ids)
        }
        .unwrap();
        let mut exhausted =
            TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 5, u64::from(u32::MAX))
                .unwrap();
        let before = book.clone();
        let initial = exhausted.clone();
        assert_eq!(
            book.place_limit_immediate_with_cursor(taker.clone(), &math, fok, &mut exhausted),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(book, before);
        assert_eq!(exhausted, initial);
        let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
        let result = book
            .place_limit_immediate_with_cursor(taker, &math, fok, &mut cursor)
            .unwrap();
        assert_eq!(result, expected);
    }
}

#[test]
fn base_market_cursor_matches_slice_with_real_trades() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    book.rest(order(2, Side::Sell, 101, 1)).unwrap();
    let mut direct = book.clone();
    let input = MarketByBase {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(2).unwrap(),
        market_protection: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let ids = [
        generator.next_trade_id().unwrap(),
        generator.next_trade_id().unwrap(),
    ];
    let expected = direct
        .execute_market_by_base(input.clone(), &ids, &math)
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let actual = book
        .execute_market_by_base_with_cursor(input, &math, &mut cursor)
        .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn quote_market_cursor_matches_slice_with_real_trades() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    book.rest(order(2, Side::Sell, 101, 1)).unwrap();
    let mut direct = book.clone();
    let input = MarketByQuote {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(201).unwrap(),
        market_protection: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let ids = [
        generator.next_trade_id().unwrap(),
        generator.next_trade_id().unwrap(),
    ];
    let expected = direct
        .execute_market_by_quote(input.clone(), &math, &ids)
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let actual = book
        .execute_market_by_quote_with_cursor(input, &math, &mut cursor)
        .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn active_iceberg_cursor_matches_legacy_and_rolls_back_overflow() {
    let mut p = ProductionOrderBook::new(policy());
    p.rest(order(1, Side::Sell, 100, 1)).unwrap();
    p.rest(order(2, Side::Sell, 100, 1)).unwrap();
    let ice = matching_domain::IcebergOrder::try_new(
        order(3, Side::Buy, 100, 3),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut exhausted =
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u64::from(u32::MAX)).unwrap();
    let before = p.clone();
    let saved = exhausted.clone();
    assert_eq!(
        p.place_iceberg_with_cursor(ice.clone(), &math, &mut exhausted),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(p, before);
    assert_eq!(exhausted, saved);
    let mut c = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
    let mut old = before;
    let mut ids = c.clone();
    let trade_ids = [ids.next_trade_id().unwrap(), ids.next_trade_id().unwrap()];
    assert_eq!(
        p.place_iceberg_with_cursor(ice.clone(), &math, &mut c),
        old.place_iceberg(ice, &math, &trade_ids)
    );
    assert_eq!(p, old);
    assert_eq!(c.next_index(), 2);
}
