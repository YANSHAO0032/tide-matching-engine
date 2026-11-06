//! 验证快照基础编码的固定字节和有界解码。
use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
#[test]
fn round_trip_is_canonical_and_rejects_trailing_bytes() {
    let mut writer = SnapshotEncoder::new(128);
    writer.u8(7).unwrap();
    writer.u32(0x01020304).unwrap();
    writer.u64(9).unwrap();
    writer.bytes(b"abc").unwrap();
    let bytes = writer.finish();
    assert_eq!(
        bytes,
        vec![
            7, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 3, b'a', b'b', b'c'
        ]
    );
    let mut reader = SnapshotDecoder::new(&bytes, 128).unwrap();
    assert_eq!(reader.u8().unwrap(), 7);
    assert_eq!(reader.u32().unwrap(), 0x01020304);
    assert_eq!(reader.u64().unwrap(), 9);
    assert_eq!(reader.bytes().unwrap(), b"abc");
    assert!(reader.finish().is_ok());
    let mut bad = bytes.clone();
    bad.push(1);
    let mut reader = SnapshotDecoder::new(&bad, 128).unwrap();
    let _ = reader.u8().unwrap();
    let _ = reader.u32().unwrap();
    let _ = reader.u64().unwrap();
    let _ = reader.bytes().unwrap();
    assert!(reader.finish().is_err());
}
#[test]
fn byte_limit_and_truncated_length_fail_without_wrapping() {
    let mut writer = SnapshotEncoder::new(6);
    assert!(writer.bytes(b"abc").is_err());
    assert!(writer.finish().is_empty());
    let mut reader = SnapshotDecoder::new(&[0xff, 0xff, 0xff, 0xff], 128).unwrap();
    assert!(reader.bytes().is_err());
    assert!(SnapshotDecoder::new(&[1, 2], 1).is_err());
}

#[test]
fn limit_order_snapshot_preserves_fields_and_rejects_invalid_values() {
    use matching_domain::snapshot_wire::{decode_limit_order, encode_limit_order};
    use matching_domain::{LimitOrder, OrderId, Price, Qty, Side, StpKey, StpMode, UserId};
    let order = LimitOrder {
        order_id: OrderId(42),
        user_id: UserId(7),
        stp_key: StpKey::new(9),
        stp_mode: StpMode::CancelBoth,
        side: Side::Sell,
        price: Price::try_new(123).unwrap(),
        qty: Qty::try_new(5).unwrap(),
    };
    let mut writer = SnapshotEncoder::new(1024);
    encode_limit_order(&mut writer, &order).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(decode_limit_order(&mut reader).unwrap(), order);
    reader.finish().unwrap();
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 1024).unwrap();
        assert!(decode_limit_order(&mut r).is_err());
    }
    let mut invalid = bytes.clone();
    invalid[33] = 8;
    let mut r = SnapshotDecoder::new(&invalid, 1024).unwrap();
    assert!(decode_limit_order(&mut r).is_err());
    let mut invalid = bytes.clone();
    let n = invalid.len();
    invalid[n - 8..].fill(0);
    let mut r = SnapshotDecoder::new(&invalid, 1024).unwrap();
    assert!(decode_limit_order(&mut r).is_err());
}

#[test]
fn limit_order_encoding_failure_keeps_prefix_unchanged() {
    use matching_domain::snapshot_wire::encode_limit_order;
    use matching_domain::{LimitOrder, OrderId, Price, Qty, Side, StpKey, StpMode, UserId};
    let mut order = LimitOrder {
        order_id: OrderId(1),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let mut writer = SnapshotEncoder::new(50);
    writer.u8(9).unwrap();
    assert!(encode_limit_order(&mut writer, &order).is_err());
    assert_eq!(writer.finish(), vec![9]);
    order.stp_mode = StpMode::DecrementAndCancel;
    let mut writer = SnapshotEncoder::new(100);
    writer.u8(9).unwrap();
    assert!(encode_limit_order(&mut writer, &order).is_err());
    assert_eq!(writer.finish(), vec![9]);
}

#[test]
fn stop_order_snapshot_roundtrips_variants_and_rejects_truncation() {
    use matching_domain::snapshot_wire::{decode_stop_order, encode_stop_order};
    use matching_domain::{
        LogicalTime, MarketProtection, OrderId, Price, PriceReferenceKind, Qty, QuoteBudget,
        ReservationId, Side, StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder,
        StpKey, StpMode, TimeInForce, TriggerExpiry, UserId,
    };
    let limit = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::MarkPrice,
        order_id: OrderId::new(1),
        user_id: UserId::new(2),
        stp_key: StpKey::new(3),
        stp_mode: StpMode::CancelMaker,
        reservation_id: ReservationId::new(4),
        fee_schedule_version: 5,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        price: Price::try_new(101).unwrap(),
        qty: Qty::try_new(12).unwrap(),
        child_tif: TimeInForce::Gtd {
            expire_at: LogicalTime::new(99),
        },
        trigger_expiry: TriggerExpiry::Day { session_id: 7 },
    });
    let market = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::IndexPrice,
        market_protection: Some(MarketProtection {
            max_slippage_bps: Some(9999),
            worst_price: Some(Price::try_new(90).unwrap()),
        }),
        order_id: OrderId::new(8),
        user_id: UserId::new(9),
        stp_key: StpKey::new(10),
        stp_mode: StpMode::CancelBoth,
        reservation_id: ReservationId::new(11),
        fee_schedule_version: 12,
        side: Side::Sell,
        stop_price: Price::try_new(100).unwrap(),
        quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(u128::MAX).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(50),
        },
    });
    let mut base_market = market.clone();
    if let StopOrder::Market(ref mut order) = base_market {
        order.quantity = StopMarketQuantity::Base(Qty::try_new(19).unwrap());
        order.market_protection = None;
        order.trigger_expiry = TriggerExpiry::Gtc;
    }
    for order in [limit, market, base_market] {
        let mut writer = SnapshotEncoder::new(1024);
        encode_stop_order(&mut writer, &order).unwrap();
        let bytes = writer.finish();
        let mut reader = SnapshotDecoder::new(&bytes, 1024).unwrap();
        assert_eq!(decode_stop_order(&mut reader).unwrap(), order);
        reader.finish().unwrap();
        for end in 0..bytes.len() {
            let mut reader = SnapshotDecoder::new(&bytes[..end], 1024).unwrap();
            assert!(decode_stop_order(&mut reader).is_err());
        }
    }
}

#[test]
fn stop_snapshot_rejects_invalid_mode_reference_tif_and_slippage_without_writing() {
    use matching_domain::snapshot_wire::encode_stop_order;
    use matching_domain::{
        LogicalTime, MarketProtection, OrderId, Price, PriceReferenceKind, Qty, ReservationId,
        Side, StopMarketOrder, StopMarketQuantity, StopOrder, StpKey, StpMode, TimeInForce,
        TriggerExpiry, UserId,
    };
    let mut order = StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId::new(1),
        user_id: UserId::new(2),
        stp_key: StpKey::new(3),
        stp_mode: StpMode::None,
        reservation_id: ReservationId::new(4),
        fee_schedule_version: 5,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(1).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(9),
        },
    };
    let mut invalid = Vec::new();
    order.stp_mode = StpMode::DecrementAndCancel;
    invalid.push(order.clone());
    order.stp_mode = StpMode::None;
    order.trigger_reference = PriceReferenceKind::StaticOpening;
    invalid.push(order.clone());
    order.trigger_reference = PriceReferenceKind::LastTrade;
    order.child_tif = TimeInForce::Gtc;
    invalid.push(order.clone());
    order.child_tif = TimeInForce::Ioc;
    order.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(10_000),
        worst_price: None,
    });
    invalid.push(order);
    for order in invalid {
        let mut w = SnapshotEncoder::new(1024);
        w.u8(7).unwrap();
        assert!(encode_stop_order(&mut w, &StopOrder::Market(order)).is_err());
        assert_eq!(w.finish(), vec![7]);
    }
}

#[test]
fn reference_snapshot_preserves_source_sequences_and_rejects_duplicate_sources() {
    use matching_domain::snapshot_wire::{decode_reference_state, encode_reference_state};
    use matching_domain::{Price, PriceReference, PriceReferenceKind, PriceReferenceState};
    let mut state = PriceReferenceState::default();
    for (kind, seq, price) in [
        (PriceReferenceKind::MarkPrice, 7, 99),
        (PriceReferenceKind::LastTrade, 3, 100),
        (PriceReferenceKind::StaticOpening, 1, 95),
    ] {
        state
            .advance(PriceReference {
                kind,
                source_seq: seq,
                price: Price::try_new(price).unwrap(),
            })
            .unwrap();
    }
    let mut writer = SnapshotEncoder::new(100);
    encode_reference_state(&mut writer, &state).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 100).unwrap();
    assert_eq!(decode_reference_state(&mut reader).unwrap(), state);
    reader.finish().unwrap();
    let mut bad = bytes.clone();
    bad[18] = bad[1];
    let mut reader = SnapshotDecoder::new(&bad, 100).unwrap();
    assert!(decode_reference_state(&mut reader).is_err());
}

#[test]
fn market_math_snapshot_restores_configuration_and_rejects_invalid_tick() {
    use matching_domain::MarketMath;
    use matching_domain::snapshot_wire::{decode_market_math, encode_market_math};
    let math = MarketMath::try_new(2, 3, 2, 10, 1000).unwrap();
    let mut writer = SnapshotEncoder::new(100);
    encode_market_math(&mut writer, &math).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 100).unwrap();
    assert_eq!(decode_market_math(&mut reader).unwrap(), math);
    reader.finish().unwrap();
    let mut bad = bytes;
    bad[3..11].fill(0);
    let mut reader = SnapshotDecoder::new(&bad, 100).unwrap();
    assert!(decode_market_math(&mut reader).is_err());
}

#[test]
fn snapshot_reason_codes_roundtrip_and_reject_unknown() {
    use matching_domain::{
        RejectReason,
        snapshot_wire::{decode_reject_reason, encode_reject_reason},
    };
    for reason in [
        RejectReason::MarketNotFound,
        RejectReason::InvalidReservation,
        RejectReason::PrioritySpaceExhaustion,
    ] {
        let mut w = SnapshotEncoder::new(2);
        encode_reject_reason(&mut w, reason).unwrap();
        let b = w.finish();
        let mut r = SnapshotDecoder::new(&b, 2).unwrap();
        assert_eq!(decode_reject_reason(&mut r).unwrap(), reason);
    }
    let mut r = SnapshotDecoder::new(&[0xff, 0xff], 2).unwrap();
    assert!(decode_reject_reason(&mut r).is_err());
}
