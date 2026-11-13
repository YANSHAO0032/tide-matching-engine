//! 第三资产费用使用版本化且精确的资产单位比率。
use matching_domain::{
    AssetId, AssetReserve, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier,
    MarketMath, Price, Qty, Side,
};
use matching_risk::LimitReservePolicy;

#[test]
fn priced_third_asset_is_persistent_and_required_by_reservation() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let asset = FeeAssetPolicy::ThirdPriced {
        asset: AssetId(3),
        quote_asset: AssetId(2),
        numerator: 3,
        denominator: 2,
    };
    let tier = FeeTier {
        maker_bps: 500,
        taker_bps: 1_000,
        rounding: FeeRounding::Ceiling,
    };
    let book = FeeScheduleBook::new(FeeSchedule::new(7, asset, [(1, tier)]).unwrap());
    let mut w = SnapshotEncoder::new(4096);
    book.write_snapshot(&mut w).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 4096).unwrap();
    assert_eq!(
        FeeScheduleBook::read_snapshot(&mut r, 10, 10).unwrap(),
        book
    );
    r.finish().unwrap();
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 4096).unwrap();
        assert!(FeeScheduleBook::read_snapshot(&mut r, 10, 10).is_err());
    }
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
        7,
        1,
        0,
    )
    .unwrap();
    assert_eq!(
        policy
            .required_versioned_reserves(
                &book,
                7,
                1,
                Side::Buy,
                Price::try_new(2).unwrap(),
                Qty::try_new(2).unwrap()
            )
            .unwrap(),
        vec![
            AssetReserve {
                asset: AssetId(2),
                atoms: 4
            },
            AssetReserve {
                asset: AssetId(3),
                atoms: 2
            }
        ]
    );
    let max_notional = (i64::MAX as u128) - 1;
    let max_fee = (max_notional * 3 / 2 * 1_000).div_ceil(10_000) * 2;
    assert_eq!(
        policy
            .required_versioned_reserves(
                &book,
                7,
                1,
                Side::Sell,
                Price::try_new(2).unwrap(),
                Qty::try_new(2).unwrap()
            )
            .unwrap(),
        vec![
            AssetReserve {
                asset: AssetId(1),
                atoms: 2
            },
            AssetReserve {
                asset: AssetId(3),
                atoms: max_fee
            }
        ]
    );
    assert!(
        FeeSchedule::new(
            8,
            FeeAssetPolicy::ThirdPriced {
                asset: AssetId(3),
                quote_asset: AssetId(2),
                numerator: 1,
                denominator: 0
            },
            [(1, tier)]
        )
        .is_err()
    );
}

#[test]
fn third_asset_requires_market_quote_and_exact_smallest_tick_lot() {
    use matching_domain::RejectReason;
    let tier = FeeTier {
        maker_bps: 100,
        taker_bps: 100,
        rounding: FeeRounding::Ceiling,
    };
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        7,
        1,
        0,
    )
    .unwrap();
    let priced = |asset, quote, numerator, denominator| {
        FeeScheduleBook::new(
            FeeSchedule::new(
                7,
                FeeAssetPolicy::ThirdPriced {
                    asset,
                    quote_asset: quote,
                    numerator,
                    denominator,
                },
                [(1, tier)],
            )
            .unwrap(),
        )
    };
    let (price, qty) = (Price::try_new(2).unwrap(), Qty::try_new(1).unwrap());
    assert_eq!(
        policy.required_versioned_reserves(
            &priced(AssetId(3), AssetId(2), 1, 2),
            7,
            1,
            Side::Buy,
            price,
            qty
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        policy.required_versioned_reserves(
            &priced(AssetId(3), AssetId(4), 1, 1),
            7,
            1,
            Side::Buy,
            price,
            qty
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        policy.required_versioned_reserves(
            &priced(AssetId(1), AssetId(2), 1, 1),
            7,
            1,
            Side::Buy,
            price,
            qty
        ),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn third_fee_settlement_debits_fee_asset_and_preserves_other_principal() {
    use matching_domain::{
        MarketId, OrderId, OriginSessionId, ReservationId, TradeEvent, TradeId, UserId,
    };
    use matching_risk::{
        ReservationRegistry, ReservationState, ReservationStatus, SettlementAsset,
        TradeSettlementContext, plan_trade_r2,
    };
    let tier = FeeTier {
        maker_bps: 1_000,
        taker_bps: 500,
        rounding: FeeRounding::Ceiling,
    };
    let book = FeeScheduleBook::new(
        FeeSchedule::new(
            7,
            FeeAssetPolicy::ThirdPriced {
                asset: AssetId(3),
                quote_asset: AssetId(2),
                numerator: 3,
                denominator: 2,
            },
            [(1, tier)],
        )
        .unwrap(),
    );
    let state = |id, user, principal_asset, principal| ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(user),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![
            AssetReserve {
                asset: principal_asset,
                atoms: principal,
            },
            AssetReserve {
                asset: AssetId(3),
                atoms: 5,
            },
        ],
        status: ReservationStatus::Active,
        fee_schedule_version: 7,
        fee_tier_id: 1,
    };
    let mut registry =
        ReservationRegistry::new([state(1, 11, AssetId(1), 2), state(2, 22, AssetId(2), 4)])
            .unwrap();
    let trade = TradeEvent {
        trade_id: TradeId(1),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(2).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Sell,
    };
    let context = TradeSettlementContext {
        market_id: MarketId(7),
        math: MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
        base: SettlementAsset {
            asset: AssetId(1),
            scale: 0,
        },
        quote: SettlementAsset {
            asset: AssetId(2),
            scale: 0,
        },
        fees: &book,
    };
    let (settlement, plan) = plan_trade_r2(
        context,
        &trade,
        &registry,
        ReservationId(1),
        ReservationId(2),
        &[ReservationId(1), ReservationId(2)],
    )
    .unwrap();
    assert_eq!(
        (settlement.maker_base_delta, settlement.maker_quote_delta),
        (-2, 4)
    );
    assert_eq!(
        (
            settlement.maker_fee.asset,
            settlement.maker_fee.atoms,
            settlement.taker_fee.atoms
        ),
        (AssetId(3), 1, 1)
    );
    assert_eq!(
        plan.releases()[0].reserves,
        vec![AssetReserve {
            asset: AssetId(3),
            atoms: 4
        }]
    );
    assert_eq!(
        plan.releases()[1].reserves,
        vec![AssetReserve {
            asset: AssetId(3),
            atoms: 4
        }]
    );
    registry.apply_r2(plan).unwrap();
    assert_eq!(
        registry.get(ReservationId(1)).unwrap().status,
        ReservationStatus::Released
    );
}
