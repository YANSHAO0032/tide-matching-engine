//! 生产簿价格带配置与缺失参考价的只读边界。
use super::*;
use matching_domain::{PriceBandConfig, PriceReferenceKind};
use proptest::prelude::*;

#[test]
fn actual_trade_last_trade_sequence_overflow_is_atomic_in_both_models() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let maker = order(1, Side::Sell, 100, 1);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let reference = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: u64::MAX,
        price: Price::try_new(99).unwrap(),
    };
    p.collect_triggered_stops(reference).unwrap();
    r.collect_triggered_stops(reference).unwrap();
    let before_p = p.clone();
    let before_r = r.clone();
    let taker = order(2, Side::Buy, 100, 1);
    assert_eq!(
        p.place_limit_gtc_with_events(taker.clone(), &[TradeId::new(1)]),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        r.place_limit_gtc_with_events(taker, &[TradeId::new(1)]),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(p, before_p);
    assert_eq!(r, before_r);
}

#[test]
fn timed_rest_duplicate_and_unsupported_stp_are_consistent() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let existing = order(1, Side::Buy, 100, 1);
    p.rest(existing.clone()).unwrap();
    r.rest(existing.clone()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    let tif = matching_domain::TimeInForce::Day { session_id: 9 };
    let now = matching_domain::LogicalTime::new(1);
    let before_p = p.clone();
    let before_r = r.clone();
    assert_eq!(
        p.rest_timed(existing.clone(), tif, now, 9),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        r.rest_timed(existing, tif, now, 9),
        Err(RejectReason::DuplicateOrderId)
    );
    let mut bad = order(2, Side::Buy, 100, 1);
    bad.stp_mode = matching_domain::StpMode::DecrementAndCancel;
    assert_eq!(
        p.rest_timed(bad.clone(), tif, now, 9),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        r.rest_timed(bad, tif, now, 9),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(p, before_p);
    assert_eq!(r, before_r);
}

#[test]
fn duplicate_limit_orders_are_rejected_before_missing_band_reference() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let input = order(1, Side::Buy, 100, 1);
    p.rest(input.clone()).unwrap();
    r.rest(input.clone()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    assert_eq!(
        p.place_limit_gtc_with_events(input.clone(), &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        r.place_limit_gtc_with_events(input.clone(), &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    let mut stp = input.clone();
    stp.stp_mode = matching_domain::StpMode::CancelMaker;
    assert_eq!(
        p.place_limit_gtc_with_events(stp.clone(), &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        r.place_limit_gtc_with_events(stp, &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        p.place_limit_ioc(input.clone(), &math, &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        r.place_limit_ioc(input.clone(), &math, &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        p.place_limit_fok(input.clone(), &math, &[]),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        r.place_limit_fok(input, &math, &[]),
        Err(RejectReason::DuplicateOrderId)
    );
}

#[test]
fn protection_stops_before_stp_and_preserves_blocked_maker() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let mut maker = order(1, Side::Sell, 106, 3);
    maker.stp_key = matching_domain::StpKey::new(7);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    let source = matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    };
    p.collect_triggered_stops(source).unwrap();
    r.collect_triggered_stops(source).unwrap();
    let taker = MarketByBase {
        order_id: OrderId::new(2),
        user_id: UserId::new(2),
        stp_key: matching_domain::StpKey::new(7),
        stp_mode: matching_domain::StpMode::CancelMaker,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let before = p.order(OrderId::new(1)).unwrap().clone();
    let actual = p.execute_market_by_base(taker.clone(), &[], &math).unwrap();
    let expected = r.execute_market_by_base(taker, &[], &math).unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.events(), [EngineEvent::MarketProtectionTriggered { next_price, source: matching_domain::ProtectionSource::MarketBand, .. }] if next_price.get() == 106)
    );
    assert_eq!(p.order(OrderId::new(1)).unwrap(), &before);
}

#[test]
fn timed_rest_requires_band_reference_in_both_models() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    for tif in [
        matching_domain::TimeInForce::Day { session_id: 9 },
        matching_domain::TimeInForce::Gtd {
            expire_at: matching_domain::LogicalTime::new(10),
        },
    ] {
        let before_p = p.clone();
        let before_r = r.clone();
        let input = order(1, Side::Buy, 100, 1);
        assert_eq!(
            p.rest_timed(input.clone(), tif, matching_domain::LogicalTime::new(1), 9),
            Err(RejectReason::ReferencePriceUnavailable)
        );
        assert_eq!(
            r.rest_timed(input, tif, matching_domain::LogicalTime::new(1), 9),
            Err(RejectReason::ReferencePriceUnavailable)
        );
        assert_eq!(p, before_p);
        assert_eq!(r, before_r);
    }
}

#[test]
fn market_math_mismatch_takes_precedence_over_missing_reference() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let wrong = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let before_p = p.clone();
    let before_r = r.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.execute_market_by_base(
            incoming.clone(),
            &[],
            &wrong
        )))
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| r.execute_market_by_base(
            incoming,
            &[],
            &wrong
        )))
        .is_err()
    );
    let quote = MarketByQuote {
        order_id: OrderId::new(6),
        user_id: UserId::new(6),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(100).unwrap(),
        market_protection: None,
    };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.execute_market_by_quote(
            quote.clone(),
            &wrong,
            &[]
        )))
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| r.execute_market_by_quote(
            quote,
            &wrong,
            &[]
        )))
        .is_err()
    );
    assert_eq!(p, before_p);
    assert_eq!(r, before_r);
}

proptest! {
    #[test]
    fn effective_boundary_binds_stricter_source(user in 1i64..1_000_000, band in 1i64..1_000_000, buy in any::<bool>()) {
        let side = if buy { Side::Buy } else { Side::Sell };
        let user_price = Price::try_new(user).unwrap();
        let band_price = Price::try_new(band).unwrap();
        let (effective, source) = matching_domain::market_order::effective_protection_with_source(side, Some(user_price), Some(band_price)).unwrap();
        let expected = if buy { user.min(band) } else { user.max(band) };
        prop_assert_eq!(effective.get(), expected);
        let expected_source = if user == band { matching_domain::ProtectionSource::Both } else if expected == user { matching_domain::ProtectionSource::User } else { matching_domain::ProtectionSource::MarketBand };
        prop_assert_eq!(source, expected_source);
    }

    #[test]
    fn static_band_contains_both_endpoints(reference in 10_000i64..1_000_000, bps in 0u32..10_000, price in 1i64..2_000_000) {
        let config = PriceBandConfig { reference_kind: PriceReferenceKind::MarkPrice, max_deviation_bps: bps };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let (lower, upper) = config.bounds(Price::try_new(reference).unwrap(), 1).unwrap();
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        p.set_price_band_config(Some((config, math))).unwrap();
        r.set_price_band_config(Some((config, math))).unwrap();
        let source = matching_domain::PriceReference { kind: PriceReferenceKind::MarkPrice, source_seq: 1, price: Price::try_new(reference).unwrap() };
        p.collect_triggered_stops(source).unwrap();
        r.collect_triggered_stops(source).unwrap();
        for candidate in [lower, upper, Price::try_new(price).unwrap()] {
            let expected = if candidate < lower || candidate > upper { Err(RejectReason::PriceBandExceeded) } else { Ok(()) };
            prop_assert_eq!(p.validate_band_price(candidate), expected);
            let mut test_r = r.clone();
            let mut test_p = p.clone();
            let input = order(99, Side::Buy, candidate.get(), 1);
            prop_assert_eq!(test_p.place_limit_post_only(input.clone()), expected);
            prop_assert_eq!(test_r.place_limit_post_only(input), expected);
        }
    }
}

#[test]
fn market_root_keeps_start_band_after_trade_changes_last_trade() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    for (id, price) in [(1, 104), (2, 108)] {
        let maker = order(id, Side::Sell, price, 1);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
    }
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let band = PriceBandConfig {
        reference_kind: PriceReferenceKind::LastTrade,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((band, math))).unwrap();
    r.set_price_band_config(Some((band, math))).unwrap();
    let initial = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    };
    p.collect_triggered_stops(initial).unwrap();
    r.collect_triggered_stops(initial).unwrap();
    let taker = MarketByBase {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(2).unwrap(),
        market_protection: None,
    };
    let actual = p
        .execute_market_by_base(taker.clone(), &[TradeId::new(1)], &math)
        .unwrap();
    let expected = r
        .execute_market_by_base(taker, &[TradeId::new(1)], &math)
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.events(), [EngineEvent::Trade(trade), EngineEvent::MarketProtectionTriggered { next_price, effective_worst_price, source: matching_domain::ProtectionSource::MarketBand, .. }] if trade.price.get() == 104 && next_price.get() == 108 && effective_worst_price.get() == 105)
    );
    assert_eq!(
        p.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        104
    );
    assert_eq!(p.order(OrderId::new(2)).unwrap().total_remaining(), 1);
}

#[test]
fn iceberg_market_math_mismatch_stops_before_mutation() {
    let mut book = ProductionOrderBook::new(policy());
    let configured = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let wrong = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        configured,
    )))
    .unwrap();
    book.collect_triggered_stops(matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    })
    .unwrap();
    let iceberg = matching_domain::IcebergOrder::try_new(
        order(1, Side::Buy, 100, 2),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    let before = book.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| book.place_iceberg(
            iceberg,
            &wrong,
            &[]
        )))
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| book.place_limit_ioc(
            order(2, Side::Buy, 100, 1),
            &wrong,
            &[]
        )))
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| book.place_limit_fok(
            order(3, Side::Buy, 100, 1),
            &wrong,
            &[]
        )))
        .is_err()
    );
    assert_eq!(book, before);
}

#[test]
fn ordinary_limit_trade_updates_last_trade_in_both_models() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    let maker = order(1, Side::Sell, 100, 1);
    production.rest(maker.clone()).unwrap();
    reference.rest(maker).unwrap();
    let incoming = order(2, Side::Buy, 100, 1);
    let p = production
        .place_limit_gtc_with_events(incoming.clone(), &[TradeId::new(1)])
        .unwrap();
    let r = reference
        .place_limit_gtc_with_events(incoming, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(p, r);
    assert_eq!(
        production
            .reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        100
    );
}

#[test]
fn ordinary_market_base_trade_updates_last_trade() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.execute_market_by_base(incoming, &[TradeId::new(1)], &math)
        .unwrap();
    assert_eq!(
        book.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        100
    );
}

#[test]
fn ordinary_market_quote_trade_updates_last_trade() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    let incoming = MarketByQuote {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(100).unwrap(),
        market_protection: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.execute_market_by_quote(incoming, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(
        book.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        100
    );
}

#[test]
fn ordinary_ioc_trade_updates_last_trade() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.place_limit_ioc(order(2, Side::Buy, 100, 1), &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(
        book.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        100
    );
}

#[test]
fn iceberg_taker_trade_updates_last_trade() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, 1)).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let iceberg = matching_domain::IcebergOrder::try_new(
        order(2, Side::Buy, 100, 2),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    book.place_iceberg(iceberg, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(
        book.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .price
            .get(),
        100
    );
}

#[test]
fn post_only_checks_both_band_sides_before_crossing() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    book.collect_triggered_stops(matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    })
    .unwrap();
    book.rest(order(1, Side::Sell, 105, 1)).unwrap();
    let before = book.clone();
    assert_eq!(
        book.place_limit_post_only(order(2, Side::Buy, 106, 1)),
        Err(RejectReason::PriceBandExceeded)
    );
    assert_eq!(
        book.place_limit_post_only(order(3, Side::Buy, 94, 1)),
        Err(RejectReason::PriceBandExceeded)
    );
    assert_eq!(book, before);
}

#[test]
fn market_base_requires_configured_band_reference() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let before = book.clone();
    assert_eq!(
        book.execute_market_by_base(
            incoming,
            &[],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
        ),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(book, before);
}

#[test]
fn market_base_stops_at_configured_band() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    book.collect_triggered_stops(matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    })
    .unwrap();
    rest_fixture(&mut book, order(1, Side::Sell, 106, 1));
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let result = book
        .execute_market_by_base(
            incoming,
            &[],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        )
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::MarketProtectionTriggered { next_price, effective_worst_price, source: matching_domain::ProtectionSource::MarketBand, .. }] if next_price.get() == 106 && effective_worst_price.get() == 105)
    );
    assert_eq!(book.order(OrderId::new(1)).unwrap().total_remaining(), 1);
}

#[test]
fn quote_market_band_stops_without_spending_budget() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    book.collect_triggered_stops(matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    })
    .unwrap();
    rest_fixture(&mut book, order(1, Side::Sell, 106, 1));
    let incoming = MarketByQuote {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(1000).unwrap(),
        market_protection: None,
    };
    let result = book.execute_market_by_quote(incoming, &math, &[]).unwrap();
    assert_eq!(result.spent_quote().get(), 0);
    assert_eq!(result.unused_quote().get(), 1000);
    assert!(matches!(
        result.events(),
        [EngineEvent::MarketProtectionTriggered {
            source: matching_domain::ProtectionSource::MarketBand,
            ..
        }]
    ));
}

#[test]
fn market_user_slippage_uses_configured_reference() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    book.collect_triggered_stops(matching_domain::PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    })
    .unwrap();
    rest_fixture(&mut book, order(1, Side::Sell, 102, 1));
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: Some(matching_domain::MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        }),
    };
    let result = book
        .execute_market_by_base(
            incoming,
            &[],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        )
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::MarketProtectionTriggered { effective_worst_price, source: matching_domain::ProtectionSource::User, .. }] if effective_worst_price.get() == 101)
    );
}

#[test]
fn market_explicit_worst_without_band_still_protects() {
    let mut book = ProductionOrderBook::new(policy());
    rest_fixture(&mut book, order(1, Side::Sell, 106, 1));
    let incoming = MarketByBase {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: Some(matching_domain::MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(Price::try_new(105).unwrap()),
        }),
    };
    let result = book
        .execute_market_by_base(
            incoming,
            &[TradeId::new(1)],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        )
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::MarketProtectionTriggered { effective_worst_price, source: matching_domain::ProtectionSource::User, .. }] if effective_worst_price.get() == 105)
    );
}

#[test]
fn quote_explicit_worst_without_band_preserves_budget() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    let maker = order(1, Side::Sell, 106, 1);
    production.rest(maker.clone()).unwrap();
    reference.rest(maker).unwrap();
    let incoming = MarketByQuote {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(1000).unwrap(),
        market_protection: Some(matching_domain::MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(Price::try_new(105).unwrap()),
        }),
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let p = production
        .execute_market_by_quote(incoming.clone(), &math, &[TradeId::new(1)])
        .unwrap();
    let r = reference
        .execute_market_by_quote(incoming, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(p, r);
    assert_eq!(p.unused_quote().get(), 1000);
    assert!(matches!(
        p.events(),
        [EngineEvent::MarketProtectionTriggered {
            source: matching_domain::ProtectionSource::User,
            ..
        }]
    ));
}

#[test]
fn gtc_requires_configured_reference_before_mutation() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    let before = book.clone();
    assert_eq!(
        book.rest(order(4, Side::Buy, 100, 1)),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_gtc(order(5, Side::Buy, 100, 1), &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_gtc_with_events(order(5, Side::Buy, 100, 1), &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    let mut stp = order(6, Side::Buy, 100, 1);
    stp.stp_mode = matching_domain::StpMode::CancelMaker;
    assert_eq!(
        book.place_limit_gtc_with_events(stp, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_ioc(order(7, Side::Buy, 100, 1), &math, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_fok(order(8, Side::Buy, 100, 1), &math, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_post_only(order(9, Side::Buy, 100, 1)),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    let iceberg = matching_domain::IcebergOrder::try_new(
        order(10, Side::Buy, 100, 2),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    assert_eq!(
        book.place_iceberg(iceberg.clone(), &math, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_iceberg_post_only(iceberg, &math),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(book, before);
}

#[test]
fn configured_band_requires_reference_and_rejects_invalid_bps_atomically() {
    let mut book = ProductionOrderBook::new(policy());
    let config = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 500,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((config, math))).unwrap();
    assert_eq!(
        book.price_band_bounds(),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.set_price_band_config(Some((
            PriceBandConfig {
                max_deviation_bps: 10_000,
                ..config
            },
            math
        ))),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(book.price_band_config(), Some((config, math)));
}
