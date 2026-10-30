use matching_domain::RejectReason;
use matching_domain::{Price, PriceBandConfig, PriceReferenceKind};

#[test]
fn price_band_reuses_directional_tick_rounding() {
    let config = PriceBandConfig {
        reference_kind: PriceReferenceKind::LastTrade,
        max_deviation_bps: 25,
    };
    let (lower, upper) = config.bounds(Price::try_new(100_003).unwrap(), 10).unwrap();
    assert_eq!((lower.get(), upper.get()), (99_760, 100_250));
}

#[test]
fn price_band_bounds_and_checked_failures() {
    let config = |bps| PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: bps,
    };
    let reference = Price::try_new(100).unwrap();
    assert_eq!(
        config(0).bounds(reference, 1).unwrap(),
        (reference, reference)
    );
    let (lower, upper) = config(9_999).bounds(reference, 1).unwrap();
    assert_eq!((lower.get(), upper.get()), (1, 199));
    let (lower, upper) = config(0)
        .bounds(Price::try_new(100_003).unwrap(), 10)
        .unwrap();
    assert_eq!((lower.get(), upper.get()), (100_010, 100_000));
    assert_eq!(
        config(10_000).bounds(reference, 1),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(
        config(10_000).bounds(reference, 0),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(
        config(1).bounds(reference, 0),
        Err(RejectReason::InvalidTick)
    );
    assert_eq!(
        config(u32::MAX).bounds(reference, 1),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(
        config(0).bounds(Price::try_new(1).unwrap(), 10),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        config(9_999).bounds(Price::try_new(i64::MAX).unwrap(), 1),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(RejectReason::PriceBandExceeded as u16, 3001);
    assert_eq!(RejectReason::ReferencePriceUnavailable as u16, 3002);
}

#[test]
fn price_band_integer_rounding_matches_independent_vectors() {
    for reference in [10_000_i64, 10_001, 100_003, 999_999] {
        for tick in [1_i64, 3, 10, 100] {
            for bps in [0_u32, 1, 25, 9_999] {
                let config = PriceBandConfig {
                    reference_kind: PriceReferenceKind::IndexPrice,
                    max_deviation_bps: bps,
                };
                let r = i128::from(reference);
                let t = i128::from(tick);
                let b = i128::from(bps);
                let raw_lower = (r * (10_000 - b) + 9_999) / 10_000;
                let expected_lower = ((raw_lower + t - 1) / t) * t;
                let expected_upper = ((r * (10_000 + b) / 10_000) / t) * t;
                let (lower, upper) = config
                    .bounds(Price::try_new(reference).unwrap(), tick)
                    .unwrap();
                assert_eq!(
                    (i128::from(lower.get()), i128::from(upper.get())),
                    (expected_lower, expected_upper)
                );
            }
        }
    }
}
