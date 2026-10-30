//! 参考簿市场价格带配置及参考价边界。

use super::*;
use matching_domain::{PriceBandConfig, PriceReferenceKind};

#[test]
fn reference_market_base_respects_configured_band() {
    let mut book = ReferenceOrderBook::new();
    let maker = LimitOrder {
        order_id: OrderId::new(1),
        user_id: matching_domain::UserId::new(1),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Sell,
        price: Price::try_new(106).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    book.rest(maker).unwrap();
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
    let incoming = matching_domain::MarketByBase {
        order_id: OrderId::new(5),
        user_id: matching_domain::UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        qty: Qty::try_new(1).unwrap(),
        market_protection: None,
    };
    let execution = book
        .execute_market_by_base(
            incoming,
            &[],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        )
        .unwrap();
    assert!(
        matches!(execution.events(), [EngineEvent::MarketProtectionTriggered { next_price, effective_worst_price, source: ProtectionSource::MarketBand, .. }] if next_price.get() == 106 && effective_worst_price.get() == 105)
    );
}

#[test]
fn reference_limit_paths_require_configured_reference() {
    let mut book = ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 500,
        },
        math,
    )))
    .unwrap();
    let order = LimitOrder {
        order_id: OrderId::new(5),
        user_id: matching_domain::UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Buy,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let before = book.clone();
    assert_eq!(
        book.rest(order.clone()),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_gtc_with_events(order.clone(), &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_ioc(order.clone(), &math, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        book.place_limit_fok(order.clone(), &math, &[]),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    let iceberg = matching_domain::IcebergOrder::try_new(
        LimitOrder {
            qty: Qty::try_new(2).unwrap(),
            ..order.clone()
        },
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
    assert_eq!(
        book.place_limit_post_only(order),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(book, before);
}

#[test]
fn configured_band_requires_reference_and_rejects_invalid_bps_atomically() {
    let mut book = ReferenceOrderBook::new();
    let config = PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 100,
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

#[test]
fn static_price_checks_both_band_boundaries_inclusively() {
    let mut book = ReferenceOrderBook::new();
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
    for value in [95, 100, 105] {
        assert_eq!(
            book.validate_band_price(Price::try_new(value).unwrap()),
            Ok(())
        );
    }
    for value in [94, 106] {
        assert_eq!(
            book.validate_band_price(Price::try_new(value).unwrap()),
            Err(RejectReason::PriceBandExceeded)
        );
    }
}
