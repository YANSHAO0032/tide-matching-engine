//! 验证事件哈希载荷的稳定字段和边界。
use matching_domain::{
    ClusterId, CommandId, EngineEvent, EventEnvelope, MarketId, OrderId, ShardId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
};

#[test]
fn kill_switch_event_hash_distinguishes_scope_and_action() {
    use matching_domain::{KillScope, encode_event_for_hash};
    let mut event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: Some(MarketId(7)),
        payload: EngineEvent::KillSwitchActivated {
            scope: KillScope::Market(MarketId(7)),
        },
    };
    let encode = |event: &EventEnvelope| {
        let mut w = SnapshotEncoder::new(256);
        encode_event_for_hash(&mut w, event).unwrap();
        w.finish()
    };
    let active = encode(&event);
    event.payload = EngineEvent::KillSwitchCleared {
        scope: KillScope::Market(MarketId(7)),
    };
    assert_ne!(encode(&event), active);
    event.payload = EngineEvent::KillSwitchActivated {
        scope: KillScope::Market(MarketId(8)),
    };
    assert_ne!(encode(&event), active);
}

#[test]
fn circuit_breaker_event_hash_covers_market_and_reason() {
    use matching_domain::{CircuitBreakerReason, encode_event_for_hash};
    let mut event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: Some(MarketId(7)),
        payload: EngineEvent::CircuitBreakerTriggered {
            market_id: MarketId(7),
            reason: CircuitBreakerReason::ManualRiskSignal,
        },
    };
    let encode = |event: &EventEnvelope| {
        let mut w = SnapshotEncoder::new(256);
        encode_event_for_hash(&mut w, event).unwrap();
        w.finish()
    };
    let expected = encode(&event);
    event.payload = EngineEvent::CircuitBreakerTriggered {
        market_id: MarketId(7),
        reason: CircuitBreakerReason::InternalSafety,
    };
    assert_ne!(encode(&event), expected);
    event.payload = EngineEvent::CircuitBreakerTriggered {
        market_id: MarketId(8),
        reason: CircuitBreakerReason::ManualRiskSignal,
    };
    assert_ne!(encode(&event), expected);
}

#[test]
fn release_amounts_have_distinct_encoding_and_preserve_legacy_bytes() {
    use matching_domain::{
        AssetId, AssetReserve, ReservationId, ReservationReleaseReason, encode_event_for_hash,
    };
    let mut event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: None,
        payload: EngineEvent::ReservationReleased {
            order_id: OrderId(8),
            reservation_id: ReservationId(9),
            reason: ReservationReleaseReason::UserCancel,
            released: None,
        },
    };
    let encode = |event: &EventEnvelope| {
        let mut w = SnapshotEncoder::new(1024);
        encode_event_for_hash(&mut w, event).unwrap();
        w.finish()
    };
    let legacy = encode(&event);
    let tail = [
        &[10_u8][..],
        &8_u128.to_be_bytes(),
        &9_u128.to_be_bytes(),
        &(ReservationReleaseReason::UserCancel as u16).to_be_bytes(),
    ]
    .concat();
    assert!(legacy.ends_with(&tail));
    if let EngineEvent::ReservationReleased { released, .. } = &mut event.payload {
        *released = Some(vec![AssetReserve {
            asset: AssetId(1),
            atoms: 7,
        }]);
    }
    let real = encode(&event);
    assert_ne!(real, legacy);
    if let EngineEvent::ReservationReleased {
        released: Some(items),
        ..
    } = &mut event.payload
    {
        items[0].atoms = 8;
    }
    assert_ne!(encode(&event), real);
}

#[test]
fn release_hash_accepts_all_assets_that_fit_the_caller_limit() {
    use matching_domain::{
        AssetId, AssetReserve, ReservationId, ReservationReleaseReason, encode_event_for_hash,
    };
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: None,
        payload: EngineEvent::ReservationReleased {
            order_id: OrderId(8),
            reservation_id: ReservationId(9),
            reason: ReservationReleaseReason::MassCancel,
            released: Some(
                (0..100)
                    .map(|asset| AssetReserve {
                        asset: AssetId(asset),
                        atoms: 1,
                    })
                    .collect(),
            ),
        },
    };
    let mut w = SnapshotEncoder::new(4096);
    encode_event_for_hash(&mut w, &event).unwrap();
    assert!(w.finish().len() > 1024);
}

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
        EngineEvent::SettlementDelta(matching_domain::SettlementDelta {
            trade_id: matching_domain::TradeId::new(1),
            maker_user: UserId(1),
            taker_user: UserId(2),
            base_asset: matching_domain::AssetId(1),
            quote_asset: matching_domain::AssetId(2),
            base_delta: matching_domain::SignedAmount::new(-2),
            quote_delta: matching_domain::SignedAmount::new(20),
            maker_fee: matching_domain::FeeDelta {
                asset: matching_domain::AssetId(2),
                amount: matching_domain::SignedAmount::new(1),
                fee_schedule_version: 1,
                fee_tier_id: 1,
            },
            taker_fee: matching_domain::FeeDelta {
                asset: matching_domain::AssetId(2),
                amount: matching_domain::SignedAmount::new(2),
                fee_schedule_version: 2,
                fee_tier_id: 1,
            },
            maker_reservation_id: ReservationId(1),
            taker_reservation_id: ReservationId(2),
        }),
        EngineEvent::FeeScheduleUpdated { version: 3 },
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
            released: None,
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
    assert_eq!(seen.len(), 22);
}

#[test]
fn fee_schedule_updated_hash_contains_stable_tag_and_version() {
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 4,
        command_id: CommandId(5),
        engine_time: 6,
        market_id: None,
        payload: EngineEvent::FeeScheduleUpdated { version: 9 },
    };
    let mut writer = SnapshotEncoder::new(1024);
    matching_domain::encode_event_for_hash(&mut writer, &event).unwrap();
    let bytes = writer.finish();
    assert_eq!(&bytes[bytes.len() - 9..], &[20, 0, 0, 0, 0, 0, 0, 0, 9]);
}

#[test]
fn settlement_delta_hash_distinguishes_both_pinned_fee_versions() {
    use matching_domain::{
        AssetId, FeeDelta, ReservationId, SettlementDelta, SignedAmount, TradeId, UserId,
    };
    let fee = |version| FeeDelta {
        asset: AssetId(2),
        amount: SignedAmount::new(1),
        fee_schedule_version: version,
        fee_tier_id: 1,
    };
    let payload = SettlementDelta {
        trade_id: TradeId::new(1),
        maker_user: UserId(2),
        taker_user: UserId(3),
        base_asset: AssetId(1),
        quote_asset: AssetId(2),
        base_delta: SignedAmount::new(-2),
        quote_delta: SignedAmount::new(20),
        maker_fee: fee(4),
        taker_fee: fee(5),
        maker_reservation_id: ReservationId(6),
        taker_reservation_id: ReservationId(7),
    };
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 0,
        command_id: CommandId(4),
        engine_time: 5,
        market_id: Some(MarketId(7)),
        payload: EngineEvent::SettlementDelta(payload.clone()),
    };
    let encode = |e: &EventEnvelope| {
        let mut w = SnapshotEncoder::new(1024);
        matching_domain::encode_event_for_hash(&mut w, e).unwrap();
        w.finish()
    };
    let original = encode(&event);
    let mut changed = event.clone();
    if let EngineEvent::SettlementDelta(ref mut value) = changed.payload {
        value.maker_fee.fee_schedule_version = 8;
    }
    assert_ne!(original, encode(&changed));
    let mut changed = event.clone();
    if let EngineEvent::SettlementDelta(ref mut value) = changed.payload {
        value.taker_fee.fee_schedule_version = 9;
    }
    assert_ne!(original, encode(&changed));
}
