use super::*;

#[test]
fn protection_source_has_stable_u8_wire_codes() {
    let golden = [
        (ProtectionSource::User, 1u8),
        (ProtectionSource::MarketBand, 2u8),
        (ProtectionSource::Both, 3u8),
    ];

    // 明确固定单字节表示。
    assert_eq!(std::mem::size_of::<ProtectionSource>(), 1);

    // 固定期望线协议：数值必须与协议规格完全一致。
    for (source, expected) in golden {
        assert_eq!(source as u8, expected);
        assert_eq!([source as u8], [expected]);

        // 验证 `Copy`、`Clone`、`Debug`、`Eq` 的值语义。
        let copied = source;
        assert_eq!(copied, source);
        assert_eq!(source.clone(), source);
        assert!(!format!("{source:?}").is_empty());
    }

    assert_ne!(ProtectionSource::User, ProtectionSource::MarketBand);
    assert_ne!(ProtectionSource::User, ProtectionSource::Both);
    assert_ne!(ProtectionSource::MarketBand, ProtectionSource::Both);
}
#[test]
fn protection_source_is_exported_from_crate_root() {
    let source: crate::ProtectionSource = ProtectionSource::Both;

    assert_eq!(source as u8, 3);
}
#[test]
fn effective_protection_with_source_has_stable_attribution() {
    let p = |value| Price::try_new(value).unwrap();

    // 预期独立固定，不从被测函数生成。
    let cases = [
        // 无候选。
        (Side::Buy, None, None, None),
        (Side::Sell, None, None, None),
        // 只有用户候选。
        (
            Side::Buy,
            Some(p(100)),
            None,
            Some((p(100), ProtectionSource::User)),
        ),
        (
            Side::Sell,
            Some(p(100)),
            None,
            Some((p(100), ProtectionSource::User)),
        ),
        // 只有市场价格区间。
        (
            Side::Buy,
            None,
            Some(p(110)),
            Some((p(110), ProtectionSource::MarketBand)),
        ),
        (
            Side::Sell,
            None,
            Some(p(110)),
            Some((p(110), ProtectionSource::MarketBand)),
        ),
        // 买入：较小值更严格。
        (
            Side::Buy,
            Some(p(100)),
            Some(p(110)),
            Some((p(100), ProtectionSource::User)),
        ),
        (
            Side::Buy,
            Some(p(110)),
            Some(p(100)),
            Some((p(100), ProtectionSource::MarketBand)),
        ),
        // 卖出：较大值更严格。
        (
            Side::Sell,
            Some(p(110)),
            Some(p(100)),
            Some((p(110), ProtectionSource::User)),
        ),
        (
            Side::Sell,
            Some(p(100)),
            Some(p(110)),
            Some((p(110), ProtectionSource::MarketBand)),
        ),
        // 边界相等：两项共同决定有效边界。
        (
            Side::Buy,
            Some(p(100)),
            Some(p(100)),
            Some((p(100), ProtectionSource::Both)),
        ),
        (
            Side::Sell,
            Some(p(100)),
            Some(p(100)),
            Some((p(100), ProtectionSource::Both)),
        ),
    ];

    for (side, user, band, expected) in cases {
        let actual = effective_protection_with_source(side, user, band);

        assert_eq!(
            actual, expected,
            "side={side:?}, user={user:?}, band={band:?}"
        );

        // 新辅助函数与已有纯合并函数的有效价格必须一致。
        assert_eq!(
            actual.map(|(price, _)| price),
            effective_protection_price(side, user, band),
        );
    }
}

#[test]
fn command_start_with_source_preserves_candidate_attribution() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    let cases = [
        // 两方均无候选。
        (Side::Buy, None, None, None),
        (Side::Sell, None, None, None),
        // 仅用户候选。
        (
            Side::Buy,
            Some(100),
            None,
            Some((100, ProtectionSource::User)),
        ),
        (
            Side::Sell,
            Some(100),
            None,
            Some((100, ProtectionSource::User)),
        ),
        // 仅市场价格区间。
        (
            Side::Buy,
            None,
            Some(110),
            Some((110, ProtectionSource::MarketBand)),
        ),
        (
            Side::Sell,
            None,
            Some(110),
            Some((110, ProtectionSource::MarketBand)),
        ),
        // 买入：较小价格更严格。
        (
            Side::Buy,
            Some(100),
            Some(110),
            Some((100, ProtectionSource::User)),
        ),
        (
            Side::Buy,
            Some(110),
            Some(100),
            Some((100, ProtectionSource::MarketBand)),
        ),
        // 卖出：较大价格更严格。
        (
            Side::Sell,
            Some(110),
            Some(100),
            Some((110, ProtectionSource::User)),
        ),
        (
            Side::Sell,
            Some(100),
            Some(110),
            Some((110, ProtectionSource::MarketBand)),
        ),
        // 候选价格相等：Both。
        (
            Side::Buy,
            Some(100),
            Some(100),
            Some((100, ProtectionSource::Both)),
        ),
        (
            Side::Sell,
            Some(100),
            Some(100),
            Some((100, ProtectionSource::Both)),
        ),
    ];

    for (side, user, band, expected) in cases {
        let protection = user.map(|price| MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(price)),
        });

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: band.map(p),
        };

        let expected = expected.map(|(price, source)| (p(price), source));

        let attributed = effective_protection_at_command_start_with_source(
            protection.as_ref(),
            &context,
            side,
            &math,
        )
        .unwrap();

        assert_eq!(attributed, expected);

        // 旧接口严格等于新接口的价格投影。
        assert_eq!(
            effective_protection_at_command_start(protection.as_ref(), &context, side, &math,),
            Ok(expected.map(|(price, _)| price))
        );
    }
}

#[test]
fn command_start_with_source_reuses_user_bps_calculation() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    let protection = MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: None,
    };

    // 参考值=100000，滑点 100 个基点：
    // 买入用户边界=101000，卖出用户边界=99000。
    let cases = [
        (Side::Buy, None, 101_000, ProtectionSource::User),
        (Side::Sell, None, 99_000, ProtectionSource::User),
        (
            Side::Buy,
            Some(100_900),
            100_900,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Sell,
            Some(99_100),
            99_100,
            ProtectionSource::MarketBand,
        ),
        (Side::Buy, Some(101_000), 101_000, ProtectionSource::Both),
        (Side::Sell, Some(99_000), 99_000, ProtectionSource::Both),
    ];

    for (side, band, expected_price, expected_source) in cases {
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: band.map(p),
        };

        assert_eq!(
            effective_protection_at_command_start_with_source(
                Some(&protection),
                &context,
                side,
                &math,
            ),
            Ok(Some((p(expected_price), expected_source)))
        );

        assert_eq!(
            effective_protection_at_command_start(Some(&protection), &context, side, &math,),
            Ok(Some(p(expected_price)))
        );
    }
}

#[test]
fn command_start_with_source_propagates_original_errors() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    let cases = [
        // 非法基点。
        (
            Side::Buy,
            100_000,
            MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            },
        ),
        // 显式价格不符合价格步长=10。
        (
            Side::Sell,
            100_000,
            MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100_003)),
            },
        ),
        // 买入边界计算溢出。
        (
            Side::Buy,
            i64::MAX,
            MarketProtection {
                max_slippage_bps: Some(100),
                worst_price: None,
            },
        ),
    ];

    for (index, (side, reference, protection)) in cases.into_iter().enumerate() {
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(reference),
            market_band_worst: Some(p(100_000)),
        };

        // 以已验收的用户保护函数作为错误来源。
        let expected_error = protection
            .user_protection_price(
                context.reference_price_at_command_start,
                side,
                math.tick_size(),
            )
            .unwrap_err();

        if index == 0 {
            assert_eq!(expected_error as u16, 3007);
        }

        if index == 2 {
            assert_eq!(expected_error, RejectReason::ArithmeticOverflow);
        }

        assert_eq!(
            effective_protection_at_command_start_with_source(
                Some(&protection),
                &context,
                side,
                &math,
            ),
            Err(expected_error)
        );

        assert_eq!(
            effective_protection_at_command_start(Some(&protection), &context, side, &math,),
            Err(expected_error)
        );
    }
}
