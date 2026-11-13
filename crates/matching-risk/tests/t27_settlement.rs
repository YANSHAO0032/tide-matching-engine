//! 成交本金和已锁定费率的独立结算预检。
use matching_domain::{
    AssetId, AssetReserve, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier,
    MarketId, MarketMath, OrderId, OriginSessionId, Price, Qty, ReservationId, Side, TradeEvent,
    TradeId, UserId,
};
use matching_risk::{
    ReservationState, ReservationStatus, SettlementAsset, TradeSettlementContext,
    plan_trade_settlement,
};

fn reservation(id: u128, user: u64, version: u64) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(user),
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![
            AssetReserve {
                asset: AssetId(1),
                atoms: 100,
            },
            AssetReserve {
                asset: AssetId(2),
                atoms: 100,
            },
        ],
        status: ReservationStatus::Active,
        fee_schedule_version: version,
        fee_tier_id: 1,
    }
}
fn schedule(version: u64, maker: i32, taker: i32) -> FeeSchedule {
    FeeSchedule::new(
        version,
        FeeAssetPolicy::Quote,
        [(
            1,
            FeeTier {
                maker_bps: maker,
                taker_bps: taker,
                rounding: FeeRounding::TowardZero,
            },
        )],
    )
    .unwrap()
}
#[test]
fn trade_settlement_pins_each_party_version_and_preserves_principal() {
    let maker = reservation(1, 11, 1);
    let taker = reservation(2, 22, 2);
    let trade = TradeEvent {
        trade_id: TradeId::new(3),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Sell,
    };
    let mut book = FeeScheduleBook::new(schedule(1, 1_000, 0));
    book.activate(schedule(2, 0, 500)).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let plan = plan_trade_settlement(
        TradeSettlementContext {
            market_id: MarketId(7),
            math,
            base: SettlementAsset {
                asset: AssetId(1),
                scale: 0,
            },
            quote: SettlementAsset {
                asset: AssetId(2),
                scale: 0,
            },
            fees: &book,
        },
        &trade,
        &maker,
        &taker,
    )
    .unwrap();
    assert_eq!(plan.maker_base_delta, -2);
    assert_eq!(plan.maker_quote_delta, 20);
    assert_eq!(plan.maker_fee.atoms, 2);
    assert_eq!(plan.taker_fee.atoms, 1);
    assert_eq!(plan.maker_fee.schedule_version, 1);
    assert_eq!(plan.taker_fee.schedule_version, 2);
    let event = plan.to_event_payload();
    assert_eq!(event.base_delta.get(), -2);
    assert_eq!(event.quote_delta.get(), 20);
    assert_eq!(event.maker_fee.fee_schedule_version, 1);
    assert_eq!(event.taker_fee.fee_schedule_version, 2);
}

#[test]
fn settlement_rejects_invalid_identity_missing_fees_and_inexact_assets() {
    use matching_domain::RejectReason;
    use matching_risk::{AssetScaleError, SettlementPlanError};
    let maker = reservation(1, 11, 1);
    let mut taker = reservation(2, 22, 2);
    let trade = TradeEvent {
        trade_id: TradeId::new(3),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Buy,
    };
    let mut book = FeeScheduleBook::new(schedule(1, 1_000, 0));
    book.activate(schedule(2, 0, 500)).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let context = || TradeSettlementContext {
        market_id: MarketId(7),
        math,
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
    let plan = plan_trade_settlement(context(), &trade, &maker, &taker).unwrap();
    assert_eq!((plan.maker_base_delta, plan.maker_quote_delta), (2, -20));
    taker.order_id = OrderId(99);
    assert_eq!(
        plan_trade_settlement(context(), &trade, &maker, &taker).unwrap_err(),
        SettlementPlanError::InvalidReservation
    );
    taker.order_id = OrderId(2);
    taker.fee_schedule_version = 99;
    assert_eq!(
        plan_trade_settlement(context(), &trade, &maker, &taker).unwrap_err(),
        SettlementPlanError::FeeSchedule(RejectReason::FeeScheduleNotFound)
    );
    taker.fee_schedule_version = 2;
    let inexact = TradeSettlementContext {
        market_id: MarketId(7),
        math,
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
    let scaled_math = MarketMath::try_new(0, 1, 1, 1, 1).unwrap();
    let inexact = TradeSettlementContext {
        market_id: MarketId(7),
        math: scaled_math,
        ..inexact
    };
    assert_eq!(
        plan_trade_settlement(inexact, &trade, &maker, &taker).unwrap_err(),
        SettlementPlanError::AssetScale(AssetScaleError::NonIntegral)
    );
}

#[test]
fn settlement_rejects_unrepresentable_principal_and_unknown_third_asset_basis() {
    use matching_risk::SettlementPlanError;
    let maker = reservation(1, 11, 1);
    let taker = reservation(2, 22, 1);
    let trade = TradeEvent {
        trade_id: TradeId::new(3),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(i64::MAX).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Buy,
    };
    let book = FeeScheduleBook::new(schedule(1, 0, 0));
    let math = MarketMath::try_new(0, 0, 19, 1, 1).unwrap();
    let context = TradeSettlementContext {
        market_id: MarketId(7),
        math,
        base: SettlementAsset {
            asset: AssetId(1),
            scale: 0,
        },
        quote: SettlementAsset {
            asset: AssetId(2),
            scale: 19,
        },
        fees: &book,
    };
    assert_eq!(
        plan_trade_settlement(context, &trade, &maker, &taker).unwrap_err(),
        SettlementPlanError::Overflow
    );
    let third = FeeSchedule::new(
        1,
        FeeAssetPolicy::Third(AssetId(3)),
        [(
            1,
            FeeTier {
                maker_bps: 1,
                taker_bps: 1,
                rounding: FeeRounding::Floor,
            },
        )],
    )
    .unwrap();
    let book = FeeScheduleBook::new(third);
    let context = TradeSettlementContext {
        market_id: MarketId(7),
        math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
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
    assert_eq!(
        plan_trade_settlement(context, &trade, &maker, &taker).unwrap_err(),
        SettlementPlanError::ThirdAssetBasisUnavailable
    );
}

#[test]
fn trade_r2_combines_principal_fees_and_real_remainder_atomically() {
    use matching_risk::{ReservationRegistry, plan_trade_r2};
    let mut maker = reservation(1, 11, 1);
    maker.reserves = vec![AssetReserve {
        asset: AssetId(1),
        atoms: 3,
    }];
    let mut taker = reservation(2, 22, 2);
    taker.reserves = vec![AssetReserve {
        asset: AssetId(2),
        atoms: 25,
    }];
    let mut registry = ReservationRegistry::new([maker, taker]).unwrap();
    let before = registry.clone();
    let trade = TradeEvent {
        trade_id: TradeId::new(3),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Sell,
    };
    let mut book = FeeScheduleBook::new(schedule(1, 1_000, 0));
    book.activate(schedule(2, 0, 500)).unwrap();
    let context = TradeSettlementContext {
        market_id: MarketId(7),
        math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
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
    assert_eq!(settlement.maker_quote_delta, 20);
    assert_eq!(registry, before);
    assert_eq!(
        plan.releases()[0].reserves,
        vec![AssetReserve {
            asset: AssetId(1),
            atoms: 1
        }]
    );
    assert_eq!(
        plan.releases()[0].to_event(matching_domain::ReservationReleaseReason::UserCancel),
        matching_domain::EngineEvent::ReservationReleased {
            order_id: OrderId(1),
            reservation_id: ReservationId(1),
            reason: matching_domain::ReservationReleaseReason::UserCancel,
            released: Some(vec![AssetReserve {
                asset: AssetId(1),
                atoms: 1
            }]),
        }
    );
    assert_eq!(
        plan.releases()[1].reserves,
        vec![AssetReserve {
            asset: AssetId(2),
            atoms: 4
        }]
    );
    registry.apply_r2(plan).unwrap();
    assert_eq!(
        registry.get(ReservationId(1)).unwrap().status,
        ReservationStatus::Released
    );
    assert_eq!(
        registry.get(ReservationId(2)).unwrap().status,
        ReservationStatus::Released
    );
}

#[test]
fn trade_r2_rejects_uncovered_fee_and_preserves_registry() {
    use matching_domain::RejectReason;
    use matching_risk::{ReservationRegistry, SettlementPlanError, plan_trade_r2};
    let mut maker = reservation(1, 11, 1);
    maker.reserves = vec![AssetReserve {
        asset: AssetId(1),
        atoms: 2,
    }];
    let mut taker = reservation(2, 22, 1);
    taker.reserves = vec![AssetReserve {
        asset: AssetId(2),
        atoms: 20,
    }];
    let registry = ReservationRegistry::new([maker, taker]).unwrap();
    let before = registry.clone();
    let book = FeeScheduleBook::new(schedule(1, 20_000, 0));
    let context = TradeSettlementContext {
        market_id: MarketId(7),
        math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
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
    let trade = TradeEvent {
        trade_id: TradeId::new(3),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Sell,
    };
    assert_eq!(
        plan_trade_r2(
            context,
            &trade,
            &registry,
            ReservationId(1),
            ReservationId(2),
            &[ReservationId(1), ReservationId(2)]
        )
        .unwrap_err(),
        SettlementPlanError::Reservation(RejectReason::InsufficientReservation)
    );
    assert_eq!(registry, before);
}
