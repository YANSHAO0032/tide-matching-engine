use matching_domain::{AssetId, MarketMath, Price, Qty, Side};
use matching_risk::LimitReservePolicy;

#[test]
fn market_math_update_preserves_fee_and_rejects_asset_scale_changes() {
    let original = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy =
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), original, 7, 3, 1).unwrap();
    let next = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    let updated = policy.with_market_math(next).unwrap();
    assert_eq!(updated.market_math(), next);
    assert_eq!(updated.fee_schedule_version(), 7);
    assert_eq!(updated.fee_tier_id(), 3);
    let changed_price_scale = MarketMath::try_new(1, 0, 0, 10, 1).unwrap();
    assert_eq!(
        policy
            .with_market_math(changed_price_scale)
            .unwrap()
            .market_math(),
        changed_price_scale
    );
    let changed_scale = MarketMath::try_new(1, 0, 1, 1, 1).unwrap();
    assert!(policy.with_market_math(changed_scale).is_err());
    assert_eq!(policy.market_math(), original);
}

#[test]
fn buy_reserves_principal_and_per_lot_rounded_fee() {
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        7,
        3,
        1,
    )
    .unwrap();
    let reserves = policy
        .required_reserves(
            Side::Buy,
            Price::try_new(10).unwrap(),
            Qty::try_new(2).unwrap(),
        )
        .unwrap();
    assert_eq!(reserves[0].asset, AssetId(2));
    assert_eq!(reserves[0].atoms, 22);
}

#[test]
fn sell_reserves_base_and_invalid_inputs_fail_closed() {
    use matching_risk::LimitReserveError;
    let math = MarketMath::try_new(0, 0, 0, 2, 2).unwrap();
    assert_eq!(
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(1), 0), math, 1, 1, 0),
        Err(LimitReserveError::InvalidPolicy)
    );
    assert_eq!(
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 10_001),
        Err(LimitReserveError::InvalidPolicy)
    );
    let p = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 9, 4, 100).unwrap();
    assert_eq!((p.fee_schedule_version(), p.fee_tier_id()), (9, 4));
    let sell = p
        .required_reserves(
            Side::Sell,
            Price::try_new(10).unwrap(),
            Qty::try_new(4).unwrap(),
        )
        .unwrap();
    assert_eq!((sell[0].asset, sell[0].atoms), (AssetId(1), 4));
    assert!(
        p.required_reserves(
            Side::Sell,
            Price::try_new(11).unwrap(),
            Qty::try_new(4).unwrap()
        )
        .is_err()
    );
    assert!(
        p.required_reserves(
            Side::Buy,
            Price::try_new(10).unwrap(),
            Qty::try_new(3).unwrap()
        )
        .is_err()
    );
}

#[test]
fn buy_fee_overflow_is_rejected() {
    use matching_risk::LimitReserveError;
    let math = MarketMath::try_new(0, 0, 19, 1, 1).unwrap();
    let p = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 19), math, 1, 1, 10_000).unwrap();
    assert_eq!(
        p.required_reserves(
            Side::Buy,
            Price::try_new(i64::MAX).unwrap(),
            Qty::try_new(1).unwrap()
        ),
        Err(LimitReserveError::Overflow)
    );
}

#[test]
fn asset_scales_must_match_market_math() {
    use matching_risk::LimitReserveError;
    let math = MarketMath::try_new(2, 3, 5, 1, 1).unwrap();
    assert_eq!(
        LimitReservePolicy::new((AssetId(1), 2), (AssetId(2), 5), math, 1, 1, 0),
        Err(LimitReserveError::InvalidPolicy)
    );
    assert_eq!(
        LimitReservePolicy::new((AssetId(1), 3), (AssetId(2), 4), math, 1, 1, 0),
        Err(LimitReserveError::InvalidPolicy)
    );
    assert!(LimitReservePolicy::new((AssetId(1), 3), (AssetId(2), 5), math, 1, 1, 0).is_ok());
}

#[test]
fn buy_reserve_covers_differently_priced_and_split_makers() {
    let math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
    let policy =
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 137).unwrap();
    let reserved = policy
        .required_reserves(
            Side::Buy,
            Price::try_new(100).unwrap(),
            Qty::try_new(10).unwrap(),
        )
        .unwrap()[0]
        .atoms;
    // 三笔成交分别跨越一个、两个和两个最小手，逐笔独立向上取整。
    let fills = [(90_u128, 2_u128), (95, 4), (100, 4)];
    let actual: u128 = fills
        .iter()
        .map(|(price, qty)| {
            let principal = price * qty;
            principal + (principal * 137).div_ceil(10_000)
        })
        .sum();
    assert_eq!(actual, 975);
    assert_eq!(reserved, 1015);
    assert!(reserved >= actual);
}

#[test]
fn sell_maximum_fee_is_covered_by_each_fill_proceeds() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy =
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 10_000).unwrap();
    let reserves = policy
        .required_reserves(
            Side::Sell,
            Price::try_new(3).unwrap(),
            Qty::try_new(6).unwrap(),
        )
        .unwrap();
    assert_eq!((reserves[0].asset, reserves[0].atoms), (AssetId(1), 6));
    for (price, qty) in [(3_u128, 1_u128), (5, 2), (9, 3)] {
        let proceeds = price * qty;
        let fee = (proceeds * 10_000).div_ceil(10_000);
        assert_eq!(fee, proceeds);
        assert!(fee <= proceeds);
    }
}

#[test]
fn principal_and_total_reserve_overflow_fail_closed() {
    use matching_domain::MathError;
    use matching_risk::LimitReserveError;

    let math = MarketMath::try_new(0, 0, 19, 1, 1).unwrap();
    let policy =
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 19), math, 1, 1, 10_000).unwrap();
    let qty = Qty::try_new(u64::MAX).unwrap();
    assert_eq!(
        policy.required_reserves(Side::Buy, Price::try_new(1).unwrap(), qty),
        Err(LimitReserveError::Overflow)
    );
    assert_eq!(
        policy.required_reserves(Side::Buy, Price::try_new(2).unwrap(), qty),
        Err(LimitReserveError::Math(MathError::ArithmeticOverflow))
    );
}
