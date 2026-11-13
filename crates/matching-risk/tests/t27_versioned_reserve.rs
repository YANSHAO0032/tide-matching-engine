//! 验证版本费率按最坏拆分情况计算预留。
use matching_domain::{
    AssetId, AssetReserve, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier,
    MarketMath, Price, Qty, Side,
};
use matching_risk::LimitReservePolicy;

#[test]
fn quote_and_base_fee_cover_both_roles_per_lot() {
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        2,
        0,
    )
    .unwrap();
    let fees = |asset| {
        FeeScheduleBook::new(
            FeeSchedule::new(
                1,
                asset,
                [(
                    2,
                    FeeTier {
                        maker_bps: 1_000,
                        taker_bps: 2_000,
                        rounding: FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        )
    };
    let price = Price::try_new(3).unwrap();
    let qty = Qty::try_new(3).unwrap();
    assert_eq!(
        policy
            .required_versioned_reserves(&fees(FeeAssetPolicy::Quote), 1, 2, Side::Buy, price, qty)
            .unwrap(),
        vec![AssetReserve {
            asset: AssetId(2),
            atoms: 12
        }]
    );
    assert_eq!(
        policy
            .required_versioned_reserves(&fees(FeeAssetPolicy::Quote), 1, 2, Side::Sell, price, qty)
            .unwrap(),
        vec![AssetReserve {
            asset: AssetId(1),
            atoms: 3
        }]
    );
    assert_eq!(
        policy
            .required_versioned_reserves(&fees(FeeAssetPolicy::Base), 1, 2, Side::Sell, price, qty)
            .unwrap(),
        vec![AssetReserve {
            asset: AssetId(1),
            atoms: 6
        }]
    );
    assert_eq!(
        policy
            .required_versioned_reserves(&fees(FeeAssetPolicy::Base), 1, 2, Side::Buy, price, qty)
            .unwrap(),
        vec![AssetReserve {
            asset: AssetId(2),
            atoms: 9
        }]
    );
}

#[test]
fn versioned_reserve_rejects_missing_version_tier_and_unbounded_sell_fee() {
    use matching_domain::RejectReason;
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        2,
        0,
    )
    .unwrap();
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                2,
                FeeTier {
                    maker_bps: -100,
                    taker_bps: 10_001,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    let (p, q) = (Price::try_new(3).unwrap(), Qty::try_new(1).unwrap());
    assert_eq!(
        policy.required_versioned_reserves(&fees, 2, 2, Side::Buy, p, q),
        Err(RejectReason::FeeScheduleNotFound)
    );
    assert!(
        policy
            .required_versioned_reserves(&fees, 1, 3, Side::Buy, p, q)
            .is_err()
    );
    let extra = (i64::MAX as u128).div_ceil(10_000);
    assert_eq!(
        policy
            .required_versioned_reserves(&fees, 1, 2, Side::Sell, p, q)
            .unwrap(),
        vec![
            AssetReserve {
                asset: AssetId(1),
                atoms: 1
            },
            AssetReserve {
                asset: AssetId(2),
                atoms: extra
            }
        ]
    );
    let third = FeeScheduleBook::new(
        FeeSchedule::new(
            1,
            FeeAssetPolicy::Third(AssetId(3)),
            [(
                2,
                FeeTier {
                    maker_bps: 1,
                    taker_bps: 1,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    );
    assert_eq!(
        policy.required_versioned_reserves(&third, 1, 2, Side::Buy, p, q),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn per_lot_upper_bound_covers_all_two_fill_splits_and_rounding_modes() {
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        2,
        0,
    )
    .unwrap();
    for rate in [1, 333, 2_000, 9_999, 10_000] {
        for rounding in [
            FeeRounding::Floor,
            FeeRounding::Ceiling,
            FeeRounding::TowardZero,
        ] {
            let fees = FeeScheduleBook::new(
                FeeSchedule::new(
                    1,
                    FeeAssetPolicy::Quote,
                    [(
                        2,
                        FeeTier {
                            maker_bps: rate,
                            taker_bps: rate,
                            rounding,
                        },
                    )],
                )
                .unwrap(),
            );
            let reserve = policy
                .required_versioned_reserves(
                    &fees,
                    1,
                    2,
                    Side::Buy,
                    Price::try_new(3).unwrap(),
                    Qty::try_new(9).unwrap(),
                )
                .unwrap()[0]
                .atoms;
            for first in 1_u128..9 {
                let fee_a = matching_risk::calculate_fee(3 * first, rate, rounding).unwrap();
                let fee_b = matching_risk::calculate_fee(3 * (9 - first), rate, rounding).unwrap();
                assert!(reserve >= 27 + u128::try_from(fee_a + fee_b).unwrap());
            }
        }
    }
}
