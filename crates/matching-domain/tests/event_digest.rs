//! 验证事件哈希载荷的稳定字段和边界。
use matching_domain::{
    ClusterId, CommandId, EngineEvent, EventEnvelope, MarketId, OrderId, ShardId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
};

#[test]
fn canonical_event_encoding_preserves_identity_and_selected_payload() {
    use matching_domain::encode_event_for_hash;
    let first = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: Some(MarketId(7)),
        payload: EngineEvent::OrderAccepted {
            order_id: OrderId(8),
        },
    };
    let mut writer = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut writer, &first).unwrap();
    let bytes = writer.finish();
    let expected = [
        &63_u32.to_be_bytes()[..],
        &[1],
        &1_u16.to_be_bytes(),
        &2_u16.to_be_bytes(),
        &3_u64.to_be_bytes(),
        &4_u32.to_be_bytes(),
        &5_u128.to_be_bytes(),
        &6_u64.to_be_bytes(),
        &[1],
        &7_u32.to_be_bytes(),
        &[2],
        &8_u128.to_be_bytes(),
    ]
    .concat();
    assert_eq!(bytes, expected);
    let mut reader = SnapshotDecoder::new(&bytes, 1024).unwrap();
    let payload = reader.bytes().unwrap();
    reader.finish().unwrap();
    assert_eq!(payload[0], 1);
    assert_eq!(&payload[1..3], &1_u16.to_be_bytes());
    assert_eq!(&payload[3..5], &2_u16.to_be_bytes());
    let mut changed = first.clone();
    changed.payload = EngineEvent::OrderOpened {
        order_id: OrderId(8),
    };
    let mut w = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut w, &changed).unwrap();
    assert_ne!(bytes, w.finish());
    let mut w = SnapshotEncoder::new(1);
    w.u8(9).unwrap();
    assert!(encode_event_for_hash(&mut w, &first).is_err());
    assert_eq!(w.finish(), vec![9]);
}

#[test]
fn all_current_event_variants_have_distinct_versioned_encodings() {
    use matching_domain::{
        AmendedQuantity, CancelReason, MarketState, MarketStateReason, OrderRemaining, Price,
        ProtectionSource, Qty, QuoteAmount, RejectReason, ReservationId, ReservationReleaseReason,
        Side, TradeEvent, TradeId, TriggeredOrderKind, UserId, encode_event_for_hash,
    };
    let id = OrderId(1);
    let price = Price::try_new(10).unwrap();
    let qty = Qty::try_new(2).unwrap();
    let variants = vec![
        EngineEvent::OrderExpired {
            order_id: id,
            reason: matching_domain::ExpireReason::SessionEnded,
        },
        EngineEvent::MarketCreated {
            market_id: MarketId(4),
        },
        EngineEvent::MarketConfigUpdated {
            market_id: MarketId(4),
        },
        EngineEvent::SessionStarted {
            market_id: MarketId(4),
            session_id: 3,
        },
        EngineEvent::SessionEnded {
            market_id: MarketId(4),
            session_id: 3,
        },
        EngineEvent::MarketStateChanged {
            market_id: MarketId(4),
            old: MarketState::Closed,
            new: MarketState::PreOpen,
            reason: MarketStateReason::SessionStarted,
        },
        EngineEvent::UserControlEpochAdvanced {
            user_id: UserId(3),
            epoch: 4,
        },
        EngineEvent::CommandRejected {
            reason: RejectReason::InvalidPrice,
        },
        EngineEvent::OrderAccepted { order_id: id },
        EngineEvent::OrderOpened { order_id: id },
        EngineEvent::OrderAmended {
            order_id: id,
            old_price: None,
            new_price: Some(price),
            old_quantity: AmendedQuantity::BaseRemaining(qty),
            new_quantity: AmendedQuantity::QuoteBudget(QuoteAmount::new(3)),
            priority_retained: false,
        },
        EngineEvent::OrderPartiallyFilled {
            order_id: id,
            remaining: qty,
        },
        EngineEvent::OrderFilled { order_id: id },
        EngineEvent::OrderCanceled {
            order_id: id,
            remaining: OrderRemaining::Base(qty),
            reason: CancelReason::MassCancel,
        },
        EngineEvent::StopTriggered {
            order_id: id,
            child_kind: TriggeredOrderKind::Limit,
        },
        EngineEvent::TriggeredOrderRejected {
            order_id: id,
            reason: RejectReason::InvalidPrice,
        },
        EngineEvent::ReservationReleased {
            order_id: id,
            reservation_id: ReservationId(2),
            reason: ReservationReleaseReason::UserCancel,
        },
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId(3),
            maker_order_id: id,
            taker_order_id: OrderId(2),
            price,
            qty,
            maker_side: Side::Sell,
        }),
        EngineEvent::IcebergRefreshed {
            order_id: id,
            visible: qty,
        },
        EngineEvent::MarketProtectionTriggered {
            order_id: id,
            next_price: price,
            effective_worst_price: price,
            source: ProtectionSource::User,
        },
    ];
    let mut seen = std::collections::BTreeSet::new();
    for payload in variants {
        let event = EventEnvelope {
            cluster_id: ClusterId(1),
            shard_id: ShardId(2),
            shard_seq: 3,
            event_index: 0,
            command_id: CommandId(4),
            engine_time: 5,
            market_id: None,
            payload,
        };
        let mut w = SnapshotEncoder::new(1024);
        encode_event_for_hash(&mut w, &event).unwrap();
        assert!(seen.insert(w.finish()));
    }
    assert_eq!(seen.len(), 20);
}
