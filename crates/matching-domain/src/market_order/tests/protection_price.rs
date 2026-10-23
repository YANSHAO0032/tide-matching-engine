use super::*;

fn protection_price(
    reference: i64,
    side: Side,
    bps: u32,
    tick: i64,
) -> Result<Price, RejectReason> {
    slippage_protection_price(Price::try_new(reference).unwrap(), side, bps, tick)
}

#[test]
fn slippage_protection_price_applies_directional_rounding() {
    assert_eq!(
        protection_price(100_003, Side::Buy, 25, 10),
        Ok(Price::try_new(100_250).unwrap()),
    );

    assert_eq!(
        protection_price(100_003, Side::Sell, 25, 10),
        Ok(Price::try_new(99_760).unwrap()),
    );
}

#[test]
fn slippage_protection_price_zero_bps_still_aligns_directionally() {
    assert_eq!(
        protection_price(100_003, Side::Buy, 0, 10),
        Ok(Price::try_new(100_000).unwrap()),
    );

    assert_eq!(
        protection_price(100_003, Side::Sell, 0, 10),
        Ok(Price::try_new(100_010).unwrap()),
    );
}

#[test]
fn slippage_protection_price_accepts_maximum_valid_bps() {
    assert_eq!(
        protection_price(100_003, Side::Buy, 9_999, 10),
        Ok(Price::try_new(199_990).unwrap()),
    );

    assert_eq!(
        protection_price(100_003, Side::Sell, 9_999, 10),
        Ok(Price::try_new(20).unwrap()),
    );
}

#[test]
fn slippage_protection_price_rejects_bps_at_or_above_denominator() {
    for bps in [10_000, 10_001, u32::MAX] {
        assert_eq!(
            protection_price(100_003, Side::Buy, bps, 10),
            Err(RejectReason::InvalidSlippageBps),
        );

        assert_eq!(
            protection_price(100_003, Side::Sell, bps, 10),
            Err(RejectReason::InvalidSlippageBps),
        );
    }
}

#[test]
fn slippage_protection_price_rejects_non_positive_tick() {
    for tick in [0, -1, -10, i64::MIN] {
        assert_eq!(
            protection_price(100_003, Side::Buy, 25, tick),
            Err(RejectReason::InvalidTick),
        );

        assert_eq!(
            protection_price(100_003, Side::Sell, 25, tick),
            Err(RejectReason::InvalidTick),
        );
    }
}

#[test]
fn slippage_protection_price_zero_bps_preserves_aligned_reference() {
    let expected = Price::try_new(100_000).unwrap();

    assert_eq!(protection_price(100_000, Side::Buy, 0, 10), Ok(expected),);

    assert_eq!(protection_price(100_000, Side::Sell, 0, 10), Ok(expected),);
}

#[test]
fn slippage_protection_price_rejects_buy_result_above_price_range() {
    assert_eq!(
        protection_price(i64::MAX, Side::Buy, 1, 1),
        Err(RejectReason::ArithmeticOverflow),
    );
}

#[test]
fn slippage_protection_price_rejects_sell_tick_ceil_above_price_range() {
    assert_eq!(
        protection_price(i64::MAX, Side::Sell, 0, 10),
        Err(RejectReason::ArithmeticOverflow),
    );
}

#[test]
fn user_protection_price_returns_none_when_no_user_protection_is_configured() {
    let protection = MarketProtection {
        max_slippage_bps: None,
        worst_price: None,
    };

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Buy, 10,),
        Ok(None),
    );

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Sell, 10,),
        Ok(None),
    );

    // 没有任何候选时，不应为了 tick_size 人为制造错误。
    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Buy, 0,),
        Ok(None),
    );
}

#[test]
fn user_protection_price_uses_existing_bps_helper_when_only_bps_is_present() {
    let protection = MarketProtection {
        max_slippage_bps: Some(25),
        worst_price: None,
    };

    let expected_buy = price(100_250);
    let expected_sell = price(99_760);

    // 固定值先锁协议。
    assert_eq!(
        slippage_protection_price(price(100_003), Side::Buy, 25, 10,),
        Ok(expected_buy),
    );

    assert_eq!(
        slippage_protection_price(price(100_003), Side::Sell, 25, 10,),
        Ok(expected_sell),
    );

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Buy, 10,),
        Ok(Some(expected_buy)),
    );

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Sell, 10,),
        Ok(Some(expected_sell)),
    );
}

#[test]
fn user_protection_price_returns_aligned_explicit_worst_price_unchanged() {
    let explicit = price(100_200);

    let protection = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(explicit),
    };

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Buy, 10,),
        Ok(Some(explicit)),
    );

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Sell, 10,),
        Ok(Some(explicit)),
    );
}

#[test]
fn user_protection_price_buy_chooses_stricter_lower_candidate() {
    // 25 个基点 辅助函数:
    // 参考值=100_003, 价格步长=10
    // => 买入基点候选 = 100_250
    //
    // 显式=100_200 更严格。
    let protection = MarketProtection {
        max_slippage_bps: Some(25),
        worst_price: Some(price(100_200)),
    };

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Buy, 10,),
        Ok(Some(price(100_200))),
    );
}

#[test]
fn user_protection_price_sell_chooses_stricter_higher_candidate() {
    // 25 个基点 辅助函数:
    // 参考值=100_003, 价格步长=10
    // => 卖出基点候选 = 99_760
    //
    // 显式=99_800 更严格。
    let protection = MarketProtection {
        max_slippage_bps: Some(25),
        worst_price: Some(price(99_800)),
    };

    assert_eq!(
        protection.user_protection_price(price(100_003), Side::Sell, 10,),
        Ok(Some(price(99_800))),
    );
}

#[test]
fn user_protection_price_rejects_unaligned_explicit_worst_price() {
    let protection = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(price(100_003)),
    };

    assert_eq!(
        protection.user_protection_price(price(100_000), Side::Buy, 10,),
        Err(RejectReason::InvalidTick),
    );

    assert_eq!(
        protection.user_protection_price(price(100_000), Side::Sell, 10,),
        Err(RejectReason::InvalidTick),
    );
}

#[test]
fn user_protection_price_prioritizes_invalid_bps_over_explicit_tick_error() {
    let protection = MarketProtection {
        max_slippage_bps: Some(10_000),
        // 同时故意未对齐价格步长。
        worst_price: Some(price(100_003)),
    };

    assert_eq!(
        protection.user_protection_price(price(100_000), Side::Buy, 10,),
        Err(RejectReason::InvalidSlippageBps),
    );

    assert_eq!(
        protection.user_protection_price(price(100_000), Side::Sell, 10,),
        Err(RejectReason::InvalidSlippageBps),
    );
}

#[test]
fn user_protection_price_rejects_non_positive_tick_for_explicit_worst_price() {
    let protection = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(price(100_000)),
    };

    for tick in [0, -1, -10, i64::MIN] {
        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Buy, tick,),
            Err(RejectReason::InvalidTick),
        );

        assert_eq!(
            protection.user_protection_price(price(100_003), Side::Sell, tick,),
            Err(RejectReason::InvalidTick),
        );
    }
}

#[test]
fn maker_price_within_protection_allows_any_price_without_boundary() {
    assert!(maker_price_within_protection(Side::Buy, price(1), None,));
    assert!(maker_price_within_protection(
        Side::Buy,
        price(999_999),
        None,
    ));

    assert!(maker_price_within_protection(Side::Sell, price(1), None,));
    assert!(maker_price_within_protection(
        Side::Sell,
        price(999_999),
        None,
    ));
}

#[test]
fn maker_price_within_protection_buy_uses_inclusive_upper_boundary() {
    let boundary = Some(price(100_000));

    // 内侧：低于买入最大允许价格。
    assert!(maker_price_within_protection(
        Side::Buy,
        price(99_990),
        boundary,
    ));

    // 相等：边界具有包容性。
    assert!(maker_price_within_protection(
        Side::Buy,
        price(100_000),
        boundary,
    ));

    // 越界：高于买入最大允许价格。
    assert!(!maker_price_within_protection(
        Side::Buy,
        price(100_010),
        boundary,
    ));
}

#[test]
fn maker_price_within_protection_sell_uses_inclusive_lower_boundary() {
    let boundary = Some(price(100_000));

    // 内侧：高于卖出最低允许价格。
    assert!(maker_price_within_protection(
        Side::Sell,
        price(100_010),
        boundary,
    ));

    // 相等：边界具有包容性。
    assert!(maker_price_within_protection(
        Side::Sell,
        price(100_000),
        boundary,
    ));

    // 越界：低于卖出最低允许价格。
    assert!(!maker_price_within_protection(
        Side::Sell,
        price(99_990),
        boundary,
    ));
}

#[test]
fn maker_price_within_protection_handles_price_domain_boundaries() {
    let min = price(1);
    let max = price(i64::MAX);

    assert!(maker_price_within_protection(Side::Buy, min, Some(max),));
    assert!(maker_price_within_protection(Side::Buy, max, Some(max),));

    assert!(maker_price_within_protection(Side::Sell, max, Some(min),));
    assert!(maker_price_within_protection(Side::Sell, min, Some(min),));
}

#[test]
fn effective_protection_price_merges_optional_candidates() {
    let p100 = Price::try_new(100).unwrap();
    let p105 = Price::try_new(105).unwrap();
    let p110 = Price::try_new(110).unwrap();

    for side in [Side::Buy, Side::Sell] {
        // 两个候选都不存在。
        assert_eq!(effective_protection_price(side, None, None), None);

        // 仅有用户候选。
        assert_eq!(
            effective_protection_price(side, Some(p100), None),
            Some(p100)
        );

        // 仅有市场价格区间候选。
        assert_eq!(
            effective_protection_price(side, None, Some(p105)),
            Some(p105)
        );

        // 两值相等，不因来源改变结果。
        assert_eq!(
            effective_protection_price(side, Some(p105), Some(p105)),
            Some(p105)
        );
    }

    // 买入：取较小值，与候选参数位置无关。
    assert_eq!(
        effective_protection_price(Side::Buy, Some(p100), Some(p110)),
        Some(p100)
    );

    assert_eq!(
        effective_protection_price(Side::Buy, Some(p110), Some(p100)),
        Some(p100)
    );

    // 卖出：取较大值，与候选参数位置无关。
    assert_eq!(
        effective_protection_price(Side::Sell, Some(p100), Some(p110)),
        Some(p110)
    );

    assert_eq!(
        effective_protection_price(Side::Sell, Some(p110), Some(p100)),
        Some(p110)
    );
}
