//! 止损限价 child 必须重新核验已锁定的本地预留和费率。
use matching_domain::{
    AssetId, AssetReserve, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier,
    MarketId, MarketMath, OrderId, OriginSessionId, Price, PriceReferenceKind, Qty, RejectReason,
    ReservationId, Side, StopLimitOrder, StpKey, StpMode, TimeInForce, TriggerExpiry, UserId,
};
use matching_risk::{
    LimitReservePolicy, ReservationRegistry, ReservationState, ReservationStatus,
    check_stop_limit_reservation,
};

#[test]
fn trigger_limit_checks_pinned_version_identity_and_remaining_coverage() {
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let tier = FeeTier {
        maker_bps: 0,
        taker_bps: 1_000,
        rounding: FeeRounding::Ceiling,
    };
    let mut fees =
        FeeScheduleBook::new(FeeSchedule::new(1, FeeAssetPolicy::Quote, [(1, tier)]).unwrap());
    fees.activate(FeeSchedule::new(2, FeeAssetPolicy::Quote, [(1, tier)]).unwrap())
        .unwrap();
    let stop = StopLimitOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        order_id: OrderId(1),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(3),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(10).unwrap(),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    };
    let state = |atoms, version| ReservationState {
        reservation_id: ReservationId(3),
        order_id: OrderId(1),
        user_id: UserId(2),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: version,
        fee_tier_id: 1,
    };
    let valid = ReservationRegistry::new([state(22, 1)]).unwrap();
    assert_eq!(
        check_stop_limit_reservation(&policy, &fees, &valid, MarketId(7), &stop),
        Ok(())
    );
    let low = ReservationRegistry::new([state(21, 1)]).unwrap();
    assert_eq!(
        check_stop_limit_reservation(&policy, &fees, &low, MarketId(7), &stop),
        Err(RejectReason::InsufficientReservation)
    );
    let stale = ReservationRegistry::new([state(22, 2)]).unwrap();
    assert_eq!(
        check_stop_limit_reservation(&policy, &fees, &stale, MarketId(7), &stop),
        Err(RejectReason::FeeScheduleMismatch)
    );
    let pending =
        ReservationRegistry::new_with_pending_r2([state(22, 1)], [ReservationId(3)]).unwrap();
    assert_eq!(
        check_stop_limit_reservation(&policy, &fees, &pending, MarketId(7), &stop),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn trigger_market_rechecks_effective_worst_price_and_quote_budget() {
    use matching_domain::{QuoteBudget, StopMarketOrder, StopMarketQuantity};
    use matching_risk::check_stop_market_reservation;
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 1_000,
                    rounding: FeeRounding::Ceiling,
                },
            )],
        )
        .unwrap(),
    );
    let mut stop = StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId(1),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(3),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(10).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(2).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    };
    let state = |atoms| ReservationState {
        reservation_id: ReservationId(3),
        order_id: OrderId(1),
        user_id: UserId(2),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let registry = ReservationRegistry::new([state(22)]).unwrap();
    assert_eq!(
        check_stop_market_reservation(
            &policy,
            &fees,
            &registry,
            MarketId(7),
            &stop,
            Some(Price::try_new(10).unwrap())
        ),
        Ok(())
    );
    assert_eq!(
        check_stop_market_reservation(&policy, &fees, &registry, MarketId(7), &stop, None),
        Err(RejectReason::InsufficientReservation)
    );
    let mut sell = stop.clone();
    sell.side = Side::Sell;
    let mut sell_state = state(2);
    sell_state.reserves[0].asset = AssetId(1);
    let sell_registry = ReservationRegistry::new([sell_state]).unwrap();
    assert_eq!(
        check_stop_market_reservation(&policy, &fees, &sell_registry, MarketId(7), &sell, None),
        Ok(())
    );
    stop.quantity = StopMarketQuantity::Quote(QuoteBudget::try_new(20).unwrap());
    assert_eq!(
        check_stop_market_reservation(
            &policy,
            &fees,
            &registry,
            MarketId(7),
            &stop,
            Some(Price::try_new(10).unwrap())
        ),
        Err(RejectReason::InsufficientReservation)
    );
    let registry = ReservationRegistry::new([state(40)]).unwrap();
    assert_eq!(
        check_stop_market_reservation(
            &policy,
            &fees,
            &registry,
            MarketId(7),
            &stop,
            Some(Price::try_new(10).unwrap())
        ),
        Ok(())
    );
}

#[test]
fn quote_budget_fee_bound_covers_integer_split_oracle() {
    use matching_domain::QuoteBudget;
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    for rate in [1_u128, 999, 5_000, 10_000] {
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: rate as i32,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        );
        for budget in 2_u128..=30 {
            let reserved = policy
                .required_quote_budget_reserves(&fees, 1, 1, QuoteBudget::try_new(budget).unwrap())
                .unwrap()[0]
                .atoms;
            for first in 1..=budget / 2 {
                let a = 2 * first;
                for second in 0..=(budget - a) / 2 {
                    let b = 2 * second;
                    let fees_a = (a * rate).div_ceil(10_000);
                    let fees_b = (b * rate).div_ceil(10_000);
                    assert!(reserved >= a + b + fees_a + fees_b);
                }
            }
        }
    }
}
