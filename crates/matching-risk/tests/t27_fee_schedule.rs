//! 费率版本不能覆盖且历史订单仍可读取其锁定配置。
use matching_domain::{AssetId, RejectReason};
use matching_risk::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};

fn schedule(version: u64, maker: i32) -> FeeSchedule {
    FeeSchedule::new(
        version,
        FeeAssetPolicy::Quote,
        [(
            7,
            FeeTier {
                maker_bps: maker,
                taker_bps: 20,
                rounding: FeeRounding::TowardZero,
            },
        )],
    )
    .unwrap()
}

#[test]
fn versions_are_immutable_and_pinned_tiers_remain_available() {
    let mut book = FeeScheduleBook::new(schedule(1, -5));
    book.activate(schedule(2, 10)).unwrap();
    assert_eq!(book.active_version(), 2);
    assert_eq!(book.resolve(1, 7).unwrap().maker_bps, -5);
    assert_eq!(book.resolve(2, 7).unwrap().maker_bps, 10);
    assert_eq!(
        book.schedule(1).unwrap().asset_policy(),
        FeeAssetPolicy::Quote
    );
    assert_eq!(book.schedule(1).unwrap().version(), 1);
    let before = book.clone();
    assert_eq!(
        book.activate(schedule(1, 30)),
        Err(RejectReason::FeeScheduleMismatch)
    );
    assert_eq!(book, before);
    assert_eq!(book.resolve(3, 7), Err(RejectReason::FeeScheduleNotFound));
    assert_eq!(book.resolve(2, 8), Err(RejectReason::FeeScheduleMismatch));
    assert_ne!(FeeAssetPolicy::Third(AssetId::new(4)), FeeAssetPolicy::Base);
}

#[test]
fn invalid_schedule_does_not_construct() {
    let tier = FeeTier {
        maker_bps: 0,
        taker_bps: 0,
        rounding: FeeRounding::Floor,
    };
    assert!(FeeSchedule::new(1, FeeAssetPolicy::Quote, []).is_err());
    assert!(FeeSchedule::new(1, FeeAssetPolicy::Quote, [(1, tier), (1, tier)]).is_err());
}

#[test]
fn fee_book_snapshot_roundtrip_and_truncation_are_checked() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut book = FeeScheduleBook::new(schedule(1, -5));
    book.activate(schedule(2, 10)).unwrap();
    let mut writer = SnapshotEncoder::new(4096);
    book.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 4096).unwrap();
    assert_eq!(
        FeeScheduleBook::read_snapshot(&mut reader, 4, 4).unwrap(),
        book
    );
    reader.finish().unwrap();
    for length in 0..bytes.len() {
        let mut reader = SnapshotDecoder::new(&bytes[..length], 4096).unwrap();
        assert!(FeeScheduleBook::read_snapshot(&mut reader, 4, 4).is_err());
    }
    let mut reader = SnapshotDecoder::new(&bytes, 4096).unwrap();
    assert!(FeeScheduleBook::read_snapshot(&mut reader, 1, 4).is_err());
    let mut invalid_version = bytes.clone();
    invalid_version[4] = 99;
    let mut reader = SnapshotDecoder::new(&invalid_version, 4096).unwrap();
    assert!(FeeScheduleBook::read_snapshot(&mut reader, 4, 4).is_err());
    let mut invalid_active = bytes.clone();
    invalid_active[12] = 99;
    let mut reader = SnapshotDecoder::new(&invalid_active, 4096).unwrap();
    assert!(FeeScheduleBook::read_snapshot(&mut reader, 4, 4).is_err());
    let mut writer = SnapshotEncoder::new(1);
    writer.u8(42).unwrap();
    assert!(book.write_snapshot(&mut writer).is_err());
    assert_eq!(writer.finish(), vec![42]);
}

#[test]
fn fee_snapshot_preserves_third_asset_and_each_rounding_direction() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let tiers = [
        (
            1,
            FeeTier {
                maker_bps: -1,
                taker_bps: 2,
                rounding: FeeRounding::Floor,
            },
        ),
        (
            2,
            FeeTier {
                maker_bps: 3,
                taker_bps: -4,
                rounding: FeeRounding::Ceiling,
            },
        ),
        (
            3,
            FeeTier {
                maker_bps: 5,
                taker_bps: 6,
                rounding: FeeRounding::TowardZero,
            },
        ),
    ];
    let book = FeeScheduleBook::new(
        FeeSchedule::new(9, FeeAssetPolicy::Third(AssetId::new(42)), tiers).unwrap(),
    );
    let mut writer = SnapshotEncoder::new(4096);
    book.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 4096).unwrap();
    assert_eq!(
        FeeScheduleBook::read_snapshot(&mut reader, 2, 3).unwrap(),
        book
    );
    reader.finish().unwrap();
    let mut reader = SnapshotDecoder::new(&bytes, 4096).unwrap();
    assert!(FeeScheduleBook::read_snapshot(&mut reader, 2, 2).is_err());
}

#[test]
fn fee_rounding_handles_signed_rebates_and_overflow() {
    use matching_risk::{FeeCalculationError, calculate_fee};
    assert_eq!(calculate_fee(101, 100, FeeRounding::Floor), Ok(1));
    assert_eq!(calculate_fee(101, 100, FeeRounding::Ceiling), Ok(2));
    assert_eq!(calculate_fee(101, -100, FeeRounding::Floor), Ok(-2));
    assert_eq!(calculate_fee(101, -100, FeeRounding::Ceiling), Ok(-1));
    assert_eq!(calculate_fee(101, -100, FeeRounding::TowardZero), Ok(-1));
    assert_eq!(
        calculate_fee(u128::MAX, 10_000, FeeRounding::Floor),
        Err(FeeCalculationError::Overflow)
    );
    assert_eq!(calculate_fee(u128::MAX, 0, FeeRounding::Floor), Ok(0));
    assert_eq!(
        calculate_fee(1_u128 << 127, -10_000, FeeRounding::Floor),
        Ok(i128::MIN)
    );
    assert!(calculate_fee(u128::MAX, 1, FeeRounding::Floor).is_ok());
}

#[test]
fn fee_calculation_matches_independent_signed_integer_oracle() {
    use matching_risk::calculate_fee;
    for basis in 0_i128..=137 {
        for rate in -103_i32..=103 {
            let numerator = basis * i128::from(rate);
            let quotient = numerator / 10_000;
            let remainder = numerator % 10_000;
            for rounding in [
                FeeRounding::Floor,
                FeeRounding::Ceiling,
                FeeRounding::TowardZero,
            ] {
                let expected = match rounding {
                    FeeRounding::Floor if remainder < 0 => quotient - 1,
                    FeeRounding::Ceiling if remainder > 0 => quotient + 1,
                    _ => quotient,
                };
                assert_eq!(
                    calculate_fee(u128::try_from(basis).unwrap(), rate, rounding),
                    Ok(expected)
                );
            }
        }
    }
}
