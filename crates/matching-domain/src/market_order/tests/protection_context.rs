use super::*;

#[test]
fn effective_protection_at_command_start_composes_candidates() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
    let reference = Price::try_new(100_003).unwrap();

    let bps_only = MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: None,
    };

    let explicit_only = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(Price::try_new(100_050).unwrap()),
    };

    let band = Price::try_new(100_000).unwrap();

    let no_band_context = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: None,
    };

    let band_context = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: Some(band),
    };

    for side in [Side::Buy, Side::Sell] {
        // 1. 用户和市场价格区间均不存在。
        assert_eq!(
            effective_protection_at_command_start(None, &no_band_context, side, &math,),
            Ok(None)
        );

        // 2. 仅有市场价格区间，原样返回。
        assert_eq!(
            effective_protection_at_command_start(None, &band_context, side, &math,),
            Ok(Some(band))
        );

        // 3. 仅有用户基点。
        // 此处验证候选组合，基点计算由辅助函数覆盖。
        let expected_user = bps_only
            .user_protection_price(
                no_band_context.reference_price_at_command_start,
                side,
                math.tick_size(),
            )
            .unwrap();

        assert!(expected_user.is_some());

        assert_eq!(
            effective_protection_at_command_start(Some(&bps_only), &no_band_context, side, &math),
            Ok(expected_user)
        );
    }

    let lower_band_context = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: Some(Price::try_new(100_040).unwrap()),
    };

    let upper_band_context = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: Some(Price::try_new(100_060).unwrap()),
    };

    // 买入：市场价格区间更严格。
    assert_eq!(
        effective_protection_at_command_start(
            Some(&explicit_only),
            &lower_band_context,
            Side::Buy,
            &math,
        ),
        Ok(Some(Price::try_new(100_040).unwrap()))
    );

    // 买入：用户更严格。
    assert_eq!(
        effective_protection_at_command_start(
            Some(&explicit_only),
            &upper_band_context,
            Side::Buy,
            &math,
        ),
        Ok(Some(Price::try_new(100_050).unwrap()))
    );

    // 卖出：市场价格区间更严格。
    assert_eq!(
        effective_protection_at_command_start(
            Some(&explicit_only),
            &upper_band_context,
            Side::Sell,
            &math,
        ),
        Ok(Some(Price::try_new(100_060).unwrap()))
    );

    // 卖出：用户更严格。
    assert_eq!(
        effective_protection_at_command_start(
            Some(&explicit_only),
            &lower_band_context,
            Side::Sell,
            &math,
        ),
        Ok(Some(Price::try_new(100_050).unwrap()))
    );
}

#[test]
fn effective_protection_at_command_start_propagates_user_errors() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
    let reference = Price::try_new(100_003).unwrap();
    let band = Some(Price::try_new(100_000).unwrap());
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: band,
    };

    // 非法基点。
    let invalid_bps = MarketProtection {
        max_slippage_bps: Some(u32::MAX),
        worst_price: None,
    };

    // 显式价格不符合价格步长=10。
    let invalid_explicit = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(Price::try_new(100_003).unwrap()),
    };

    for side in [Side::Buy, Side::Sell] {
        // 1. 非法基点返回 3007。
        let expected_error = invalid_bps
            .user_protection_price(
                context.reference_price_at_command_start,
                side,
                math.tick_size(),
            )
            .unwrap_err();

        assert_eq!(expected_error as u16, 3007);

        // 即使市场价格区间有效，也必须原样传播用户错误。
        assert_eq!(
            effective_protection_at_command_start(Some(&invalid_bps), &context, side, &math,),
            Err(expected_error)
        );

        // 2. 显式价格未对齐：传播既有错误。
        let expected_error = invalid_explicit
            .user_protection_price(
                context.reference_price_at_command_start,
                side,
                math.tick_size(),
            )
            .unwrap_err();

        assert_eq!(
            effective_protection_at_command_start(Some(&invalid_explicit), &context, side, &math,),
            Err(expected_error)
        );
    }
}
#[test]
fn command_start_protection_context_has_value_semantics() {
    let reference = Price::try_new(100_003).unwrap();
    let band = Price::try_new(100_010).unwrap();

    // 1. 相同参考值，市场价格区间=None。
    let without_band = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: None,
    };

    // 2. 相同参考值，市场价格区间=Some。
    let with_band = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: Some(band),
    };

    assert_eq!(without_band.reference_price_at_command_start, reference);
    assert_eq!(without_band.market_band_worst, None);

    assert_eq!(with_band.reference_price_at_command_start, reference);
    assert_eq!(with_band.market_band_worst, Some(band));

    assert_eq!(without_band.clone(), without_band);
    assert_eq!(with_band.clone(), with_band);

    let debug = format!("{with_band:?}");
    assert!(debug.contains("CommandStartProtectionContext"));
    assert!(debug.contains("reference_price_at_command_start"));
    assert!(debug.contains("market_band_worst"));

    let same = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100_003).unwrap(),
        market_band_worst: Some(Price::try_new(100_010).unwrap()),
    };
    assert_eq!(with_band, same);

    assert_ne!(without_band, with_band);

    let changed_reference = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100_013).unwrap(),
        market_band_worst: Some(band),
    };
    assert_ne!(with_band, changed_reference);

    let changed_band = CommandStartProtectionContext {
        reference_price_at_command_start: reference,
        market_band_worst: Some(Price::try_new(100_020).unwrap()),
    };
    assert_ne!(with_band, changed_band);
}
