//! 检验结算预留候选的覆盖、确定性和失败原子性。
use matching_domain::{
    AssetId, AssetReserve, MarketId, OrderId, OriginSessionId, RejectReason, ReservationId, UserId,
};
use matching_risk::{ReservationRegistry, ReservationState, ReservationStatus, SettlementDebit};

fn registry() -> ReservationRegistry {
    ReservationRegistry::new([ReservationState {
        reservation_id: ReservationId(1),
        order_id: OrderId(2),
        user_id: UserId(3),
        market_id: MarketId(4),
        origin_session_id: OriginSessionId(5),
        version: 7,
        reserves: vec![
            AssetReserve {
                asset: AssetId(1),
                atoms: 10,
            },
            AssetReserve {
                asset: AssetId(2),
                atoms: 20,
            },
        ],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    }])
    .unwrap()
}

#[test]
fn r2_debits_and_terminal_release_are_atomic() {
    let mut book = registry();
    let before = book.clone();
    let debits = [
        SettlementDebit {
            reservation_id: ReservationId(1),
            asset: AssetId(1),
            atoms: 4,
        },
        SettlementDebit {
            reservation_id: ReservationId(1),
            asset: AssetId(2),
            atoms: 20,
        },
    ];
    let plan = book.plan_r2(&debits, &[ReservationId(1)]).unwrap();
    assert_eq!(book, before);
    assert_eq!(
        plan.releases()[0].reserves,
        vec![AssetReserve {
            asset: AssetId(1),
            atoms: 6
        }]
    );
    book.apply_r2(plan).unwrap();
    let state = book.get(ReservationId(1)).unwrap();
    assert_eq!(state.status, ReservationStatus::Released);
    assert!(state.reserves.is_empty());
    assert_eq!(state.version, 8);
}

#[test]
fn r2_rejects_insufficient_and_plan_drift_without_mutation() {
    let mut book = registry();
    let before = book.clone();
    let excessive = [SettlementDebit {
        reservation_id: ReservationId(1),
        asset: AssetId(1),
        atoms: 11,
    }];
    assert_eq!(
        book.plan_r2(&excessive, &[]).err(),
        Some(RejectReason::InsufficientReservation)
    );
    assert_eq!(book, before);
    let plan = book.plan_r2(&[], &[ReservationId(1)]).unwrap();
    let other = book
        .plan_r2(
            &[SettlementDebit {
                reservation_id: ReservationId(1),
                asset: AssetId(1),
                atoms: 1,
            }],
            &[],
        )
        .unwrap();
    book.apply_r2(other).unwrap();
    let changed = book.clone();
    assert!(book.apply_r2(plan).is_err());
    assert_eq!(book, changed);
}

#[test]
fn r2_aggregates_debits_deterministically_and_rejects_overflow() {
    let book = registry();
    let a = SettlementDebit {
        reservation_id: ReservationId(1),
        asset: AssetId(1),
        atoms: 2,
    };
    let b = SettlementDebit {
        reservation_id: ReservationId(1),
        asset: AssetId(1),
        atoms: 3,
    };
    let left = book.plan_r2(&[a, b], &[ReservationId(1)]).unwrap();
    let right = book.plan_r2(&[b, a], &[ReservationId(1)]).unwrap();
    assert_eq!(left, right);
    let huge = SettlementDebit {
        atoms: u128::MAX,
        ..a
    };
    assert_eq!(
        book.plan_r2(&[huge, a], &[]).err(),
        Some(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        book.plan_r2(&[], &[ReservationId(1), ReservationId(1)])
            .err(),
        Some(RejectReason::InvalidReservation)
    );
    assert_eq!(
        book.plan_r2(&[SettlementDebit { atoms: 0, ..a }], &[])
            .err(),
        Some(RejectReason::InvalidReservation)
    );
}

#[test]
fn fully_consumed_terminal_reservation_has_no_release_balance() {
    let mut book = registry();
    let debits = [
        SettlementDebit {
            reservation_id: ReservationId(1),
            asset: AssetId(1),
            atoms: 10,
        },
        SettlementDebit {
            reservation_id: ReservationId(1),
            asset: AssetId(2),
            atoms: 20,
        },
    ];
    assert_eq!(
        book.plan_r2(&debits, &[]).err(),
        Some(RejectReason::InsufficientReservation)
    );
    let plan = book.plan_r2(&debits, &[ReservationId(1)]).unwrap();
    assert!(plan.releases().is_empty());
    book.apply_r2(plan).unwrap();
    assert_eq!(
        book.get(ReservationId(1)).unwrap().status,
        ReservationStatus::Released
    );
}
#[test]
fn pending_reservation_keeps_fee_version_and_tier_live() {
    use matching_domain::{
        FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier, RejectReason,
        ReservationId,
    };
    let tier = FeeTier {
        maker_bps: 0,
        taker_bps: 0,
        rounding: FeeRounding::Floor,
    };
    let state = registry().get(ReservationId(1)).unwrap().clone();
    let version = state.fee_schedule_version;
    let registry =
        matching_risk::ReservationRegistry::new_with_pending_r2([state], [ReservationId(1)])
            .unwrap();
    let wrong = FeeScheduleBook::new(
        FeeSchedule::new(version, FeeAssetPolicy::Quote, [(999, tier)]).unwrap(),
    );
    assert_eq!(
        registry.validate_fee_references(&wrong),
        Err(RejectReason::FeeScheduleMismatch)
    );
    let correct = FeeScheduleBook::new(
        FeeSchedule::new(version, FeeAssetPolicy::Quote, [(1, tier)]).unwrap(),
    );
    assert_eq!(registry.validate_fee_references(&correct), Ok(()));
    let mut released = registry;
    let plan = released.plan_r2(&[], &[ReservationId(1)]).unwrap();
    released.apply_r2(plan).unwrap();
    assert_eq!(released.validate_fee_references(&wrong), Ok(()));
}
