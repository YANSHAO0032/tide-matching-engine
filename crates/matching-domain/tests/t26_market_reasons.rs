//! 固定市场状态拒绝原因编号及其快照解码。
use matching_domain::{
    RejectReason,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder, decode_reject_reason, encode_reject_reason},
};

#[test]
fn market_state_reasons_keep_specified_wire_codes() {
    let cases = [
        (RejectReason::MarketNotOpen, 1002_u16),
        (RejectReason::MarketHalted, 1003),
        (RejectReason::MarketSuspended, 1004),
        (RejectReason::MarketClosed, 1005),
        (RejectReason::PreOpenWouldCross, 1006),
        (RejectReason::InvalidMarketStateTransition, 1010),
        (RejectReason::MarketConfigInvalidTick, 1011),
        (RejectReason::MarketConfigInvalidLot, 1012),
        (RejectReason::MarketConfigUnsupportedScale, 1013),
        (RejectReason::MarketConfigNonIntegralQuoteUnit, 1014),
        (RejectReason::MarketConfigArithmeticOverflow, 1015),
        (RejectReason::MinNotional, 2006),
        (RejectReason::MaxNotional, 2007),
    ];
    for (reason, code) in cases {
        let mut writer = SnapshotEncoder::new(2);
        encode_reject_reason(&mut writer, reason).unwrap();
        let bytes = writer.finish();
        assert_eq!(bytes, code.to_be_bytes());
        let mut reader = SnapshotDecoder::new(&bytes, 2).unwrap();
        assert_eq!(decode_reject_reason(&mut reader).unwrap(), reason);
        reader.finish().unwrap();
    }
}
