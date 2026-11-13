//! 验证市场状态与交易时段的转换规则。
use matching_domain::{MarketLifecycle, MarketState, RejectReason};

#[test]
fn halt_preserves_preopen_revalidation_requirement_in_snapshot() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut lifecycle = MarketLifecycle::new_closed();
    lifecycle.start_session(1).unwrap();
    lifecycle.halt().unwrap();
    assert!(lifecycle.preopen_revalidation_required());
    let mut writer = SnapshotEncoder::new(32);
    lifecycle.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 32).unwrap();
    let restored = MarketLifecycle::read_snapshot(&mut reader).unwrap();
    assert_eq!(restored, lifecycle);
    assert_eq!(
        lifecycle.resume(),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    let mut normal = MarketLifecycle::new_closed();
    normal.start_session(1).unwrap();
    normal.open(1).unwrap();
    normal.halt().unwrap();
    assert!(!normal.preopen_revalidation_required());
}

#[test]
fn ended_session_is_persisted_and_cannot_end_twice() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut market = MarketLifecycle::new_closed();
    market.start_session(7).unwrap();
    market.open(7).unwrap();
    assert_eq!(market.end_session(8), Err(RejectReason::InvalidSession));
    market.end_session(7).unwrap();
    assert!(!market.session_active());
    assert_eq!(market.end_session(7), Err(RejectReason::InvalidSession));
    let mut writer = SnapshotEncoder::new(32);
    market.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 32).unwrap();
    let mut restored = MarketLifecycle::read_snapshot(&mut reader).unwrap();
    assert_eq!(restored, market);
    assert!(!restored.session_active());
    restored.close().unwrap();
    restored.start_session(8).unwrap();
    assert!(restored.session_active());
}

#[test]
fn ended_preopen_session_cannot_be_opened() {
    let mut market = MarketLifecycle::new_closed();
    market.start_session(7).unwrap();
    market.end_session(7).unwrap();
    assert_eq!(market.open(7), Err(RejectReason::InvalidSession));
    assert_eq!(market.state(), MarketState::PreOpen);
}

#[test]
fn ended_suspended_session_cannot_resume() {
    let mut market = MarketLifecycle::new_closed();
    market.start_session(7).unwrap();
    market.open(7).unwrap();
    market.suspend().unwrap();
    market.end_session(7).unwrap();
    assert_eq!(market.resume(), Err(RejectReason::InvalidSession));
    assert_eq!(market.state(), MarketState::Suspended);
}

#[test]
fn active_lifecycle_keeps_original_snapshot_encoding() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut bytes = vec![1, 1, 1];
    bytes.extend_from_slice(&7_u64.to_be_bytes());
    let mut reader = SnapshotDecoder::new(&bytes, 32).unwrap();
    let lifecycle = MarketLifecycle::read_snapshot(&mut reader).unwrap();
    assert!(lifecycle.session_active());
    let mut writer = SnapshotEncoder::new(32);
    lifecycle.write_snapshot(&mut writer).unwrap();
    assert_eq!(writer.finish(), bytes);
}

#[test]
fn only_closed_session_can_start_and_only_matching_preopen_session_can_open() {
    let mut market = MarketLifecycle::new_closed();
    assert_eq!(market.state(), MarketState::Closed);
    assert_eq!(
        market.open(7),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    market.start_session(7).unwrap();
    assert_eq!(market.state(), MarketState::PreOpen);
    assert_eq!(market.session_id(), Some(7));
    assert_eq!(
        market.start_session(8),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    assert_eq!(market.open(8), Err(RejectReason::InvalidSession));
    assert_eq!(market.state(), MarketState::PreOpen);
    market.open(7).unwrap();
    assert_eq!(market.state(), MarketState::Open);
    assert_eq!(
        market.open(7),
        Err(RejectReason::InvalidMarketStateTransition)
    );
}

#[test]
fn suspend_resume_and_close_are_explicit_and_failure_is_atomic() {
    let mut market = MarketLifecycle::new_closed();
    assert_eq!(
        market.resume(),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    market.start_session(1).unwrap();
    market.open(1).unwrap();
    market.suspend().unwrap();
    assert_eq!(market.state(), MarketState::Suspended);
    assert_eq!(
        market.suspend(),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    market.resume().unwrap();
    market.close().unwrap();
    assert_eq!(market.state(), MarketState::Closed);
    assert_eq!(
        market.resume(),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    assert_eq!(market.state(), MarketState::Closed);
    market.start_session(2).unwrap();
    assert_eq!(market.session_id(), Some(2));
}

#[test]
fn market_state_filters_limit_and_cancel_without_mutation() {
    let mut market = MarketLifecycle::new_closed();
    assert_eq!(market.check_limit(false), Err(RejectReason::MarketClosed));
    assert_eq!(market.check_cancel(), Err(RejectReason::MarketClosed));
    market.start_session(3).unwrap();
    assert_eq!(market.check_limit(false), Ok(()));
    assert_eq!(
        market.check_limit(true),
        Err(RejectReason::PreOpenWouldCross)
    );
    assert_eq!(market.check_cancel(), Ok(()));
    market.open(3).unwrap();
    assert_eq!(market.check_limit(true), Ok(()));
    market.suspend().unwrap();
    assert_eq!(
        market.check_limit(false),
        Err(RejectReason::MarketSuspended)
    );
    assert_eq!(market.check_cancel(), Ok(()));
}

#[test]
fn preopen_rejects_immediate_limit_policies_without_changing_state() {
    use matching_domain::TimeInForce;
    let mut market = MarketLifecycle::new_closed();
    market.start_session(3).unwrap();
    for tif in [TimeInForce::Ioc, TimeInForce::Fok] {
        assert_eq!(
            market.check_limit_tif(tif, false),
            Err(RejectReason::InvalidOrderCombination)
        );
    }
    assert_eq!(market.check_limit_tif(TimeInForce::Gtc, false), Ok(()));
    assert_eq!(market.state(), MarketState::PreOpen);
    market.open(3).unwrap();
    assert_eq!(market.check_limit_tif(TimeInForce::Ioc, true), Ok(()));
}

#[test]
fn market_and_amend_permissions_match_market_state() {
    let mut market = MarketLifecycle::new_closed();
    assert_eq!(market.check_market_order(), Err(RejectReason::MarketClosed));
    assert_eq!(market.check_amend(false), Err(RejectReason::MarketClosed));
    market.start_session(3).unwrap();
    assert_eq!(
        market.check_market_order(),
        Err(RejectReason::MarketNotOpen)
    );
    assert_eq!(
        market.check_amend(true),
        Err(RejectReason::PreOpenWouldCross)
    );
    assert_eq!(market.check_amend(false), Ok(()));
    market.open(3).unwrap();
    assert_eq!(market.check_market_order(), Ok(()));
    assert_eq!(market.check_amend(true), Ok(()));
    market.suspend().unwrap();
    assert_eq!(
        market.check_market_order(),
        Err(RejectReason::MarketSuspended)
    );
    assert_eq!(
        market.check_amend(false),
        Err(RejectReason::MarketSuspended)
    );
}

#[test]
fn snapshot_preserves_state_and_session_and_rejects_invalid_combinations() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut original = MarketLifecycle::new_closed();
    original.start_session(17).unwrap();
    original.open(17).unwrap();
    original.suspend().unwrap();
    let mut writer = SnapshotEncoder::new(32);
    original.write_snapshot(&mut writer).unwrap();
    let encoded = writer.finish();
    let mut reader = SnapshotDecoder::new(&encoded, 32).unwrap();
    assert_eq!(
        MarketLifecycle::read_snapshot(&mut reader).unwrap(),
        original
    );
    reader.finish().unwrap();
    for invalid in [[1, 1, 0], [1, 99, 0], [9, 4, 0]] {
        let mut reader = SnapshotDecoder::new(&invalid, 32).unwrap();
        assert!(MarketLifecycle::read_snapshot(&mut reader).is_err());
    }
}

#[test]
fn failed_lifecycle_encoding_does_not_modify_outer_buffer() {
    use matching_domain::snapshot_wire::SnapshotEncoder;
    let mut writer = SnapshotEncoder::new(2);
    writer.u8(9).unwrap();
    assert!(
        MarketLifecycle::new_closed()
            .write_snapshot(&mut writer)
            .is_err()
    );
    assert_eq!(writer.finish(), vec![9]);
}

#[test]
fn management_event_hash_encodes_state_reason_and_session() {
    use matching_domain::snapshot_wire::SnapshotEncoder;
    use matching_domain::{
        ClusterId, CommandId, EngineEvent, EventEnvelope, MarketId, MarketStateReason, ShardId,
        encode_event_for_hash,
    };
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 0,
        command_id: CommandId(4),
        engine_time: 0,
        market_id: Some(MarketId(5)),
        payload: EngineEvent::MarketStateChanged {
            market_id: MarketId(5),
            old: MarketState::Closed,
            new: MarketState::PreOpen,
            reason: MarketStateReason::SessionStarted,
        },
    };
    let mut a = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut a, &event).unwrap();
    let mut changed = event.clone();
    if let EngineEvent::MarketStateChanged { reason, .. } = &mut changed.payload {
        *reason = MarketStateReason::Admin;
    }
    let mut b = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut b, &changed).unwrap();
    assert_ne!(a.finish(), b.finish());
    assert_eq!(MarketStateReason::SessionStarted as u16, 1408);
}

#[test]
fn management_event_encodings_distinguish_all_lifecycle_events() {
    use matching_domain::snapshot_wire::SnapshotEncoder;
    use matching_domain::{
        ClusterId, CommandId, EngineEvent, EventEnvelope, MarketId, ShardId, encode_event_for_hash,
    };
    let market_id = MarketId(5);
    let events = [
        EngineEvent::MarketCreated { market_id },
        EngineEvent::MarketConfigUpdated { market_id },
        EngineEvent::SessionStarted {
            market_id,
            session_id: 7,
        },
        EngineEvent::SessionEnded {
            market_id,
            session_id: 7,
        },
    ];
    let mut encodings = std::collections::BTreeSet::new();
    for payload in events {
        let envelope = EventEnvelope {
            cluster_id: ClusterId(1),
            shard_id: ShardId(2),
            shard_seq: 3,
            event_index: 0,
            command_id: CommandId(4),
            engine_time: 0,
            market_id: Some(market_id),
            payload,
        };
        let mut writer = SnapshotEncoder::new(1024);
        encode_event_for_hash(&mut writer, &envelope).unwrap();
        assert!(encodings.insert(writer.finish()));
    }
}

#[test]
fn order_expiry_event_hash_captures_its_stable_reason() {
    use matching_domain::snapshot_wire::SnapshotEncoder;
    use matching_domain::{
        ClusterId, CommandId, EngineEvent, EventEnvelope, ExpireReason, MarketId, OrderId, ShardId,
        encode_event_for_hash,
    };
    let mut event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: 0,
        command_id: CommandId(4),
        engine_time: 0,
        market_id: Some(MarketId(5)),
        payload: EngineEvent::OrderExpired {
            order_id: OrderId(6),
            reason: ExpireReason::SessionEnded,
        },
    };
    let mut a = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut a, &event).unwrap();
    event.payload = EngineEvent::OrderExpired {
        order_id: OrderId(6),
        reason: ExpireReason::MarketClosed,
    };
    let mut b = SnapshotEncoder::new(1024);
    encode_event_for_hash(&mut b, &event).unwrap();
    assert_ne!(a.finish(), b.finish());
}

#[test]
fn restored_states_follow_the_limit_and_cancel_permission_matrix() {
    use matching_domain::snapshot_wire::SnapshotDecoder;
    let cases = [
        (0_u8, Ok(()), Err(RejectReason::PreOpenWouldCross), Ok(())),
        (1, Ok(()), Ok(()), Ok(())),
        (
            2,
            Err(RejectReason::MarketHalted),
            Err(RejectReason::MarketHalted),
            Ok(()),
        ),
        (
            3,
            Err(RejectReason::MarketSuspended),
            Err(RejectReason::MarketSuspended),
            Ok(()),
        ),
        (
            4,
            Err(RejectReason::MarketClosed),
            Err(RejectReason::MarketClosed),
            Err(RejectReason::MarketClosed),
        ),
    ];
    for (state_tag, non_crossing, crossing, cancel) in cases {
        let mut bytes = vec![1, state_tag, 1];
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        let mut reader = SnapshotDecoder::new(&bytes, 32).unwrap();
        let state = MarketLifecycle::read_snapshot(&mut reader).unwrap();
        assert_eq!(state.check_limit(false), non_crossing);
        assert_eq!(state.check_limit(true), crossing);
        assert_eq!(state.check_cancel(), cancel);
    }
}

#[test]
fn numeric_config_changes_respect_suspended_empty_and_history_guards() {
    use matching_domain::{MarketMath, MarketMathUpdateError, validate_market_math_update};
    let old = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let new_tick = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    let new_scale = MarketMath::try_new(1, 0, 0, 10, 1).unwrap();
    assert_eq!(
        validate_market_math_update(old, new_tick, MarketState::Open, false, false),
        Err(MarketMathUpdateError::TickLotRequiresSuspendedEmpty)
    );
    assert_eq!(
        validate_market_math_update(old, new_tick, MarketState::Suspended, true, false),
        Err(MarketMathUpdateError::TickLotRequiresSuspendedEmpty)
    );
    assert_eq!(
        validate_market_math_update(old, new_tick, MarketState::Suspended, false, false),
        Ok(())
    );
    assert_eq!(
        validate_market_math_update(old, new_scale, MarketState::Suspended, true, false),
        Err(MarketMathUpdateError::ScaleInUse)
    );
    assert_eq!(
        validate_market_math_update(old, new_scale, MarketState::Suspended, false, true),
        Err(MarketMathUpdateError::ScaleInUse)
    );
    assert_eq!(
        validate_market_math_update(old, old, MarketState::Open, true, true),
        Ok(())
    );
}
