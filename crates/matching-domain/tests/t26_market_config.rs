//! 验证市场配置的数值约束及变更边界。
use matching_domain::{
    ClosePolicy, ExecutionLimits, MarketConfig, MarketId, OrderLimits, Qty, QuoteAmount,
};

fn config() -> MarketConfig {
    MarketConfig {
        market_id: MarketId(7),
        price_scale: 0,
        qty_scale: 0,
        quote_scale: 0,
        tick_size: 1,
        lot_size: 1,
        order_limits: OrderLimits {
            min_qty: Qty::try_new(1).unwrap(),
            max_qty: Qty::try_new(10).unwrap(),
            min_notional: QuoteAmount::new(1),
            max_notional: QuoteAmount::new(100),
        },
        price_band: None,
        active_fee_schedule_version: 1,
        close_policy: ClosePolicy::CancelAll,
        execution_limits: ExecutionLimits {
            max_internal_actions_per_command: 10,
            max_trades_per_command: 10,
            priority_exhaustion_guard: 1,
            synthetic_priority_ceiling: None,
        },
    }
}

#[test]
fn market_tracks_referenced_history_across_snapshots() {
    use matching_domain::{
        ManagedMarket,
        snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
    };
    let mut market = ManagedMarket::new(config()).unwrap();
    assert!(!market.has_referenced_history());
    let mut writer = SnapshotEncoder::new(512);
    market.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 512).unwrap();
    assert!(
        !ManagedMarket::read_snapshot(&mut reader)
            .unwrap()
            .has_referenced_history()
    );
    let mut legacy = bytes.clone();
    legacy[0] = 1;
    let mut reader = SnapshotDecoder::new(&legacy, 512).unwrap();
    let restored_legacy = ManagedMarket::read_snapshot(&mut reader).unwrap();
    assert!(restored_legacy.has_referenced_history());
    let mut encoded_legacy = SnapshotEncoder::new(512);
    restored_legacy.write_snapshot(&mut encoded_legacy).unwrap();
    assert_eq!(encoded_legacy.finish(), legacy);
    market.record_referenced_history();
    let mut writer = SnapshotEncoder::new(512);
    market.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 512).unwrap();
    assert!(
        ManagedMarket::read_snapshot(&mut reader)
            .unwrap()
            .has_referenced_history()
    );
}

#[test]
fn recorded_history_cannot_be_overridden_by_update_checks() {
    use matching_domain::{
        ManagedMarket, MarketConfigUpdateChecks, MarketConfigUpdateError, MarketMathUpdateError,
    };
    let mut original = config();
    original.tick_size = 10;
    let mut market = ManagedMarket::new(original.clone()).unwrap();
    market.record_referenced_history();
    let before = market.clone();
    original.price_scale = 1;
    let checks = MarketConfigUpdateChecks {
        has_active_orders: false,
        has_referenced_history: false,
        existing_orders_valid: true,
        fee_version_available: true,
    };
    assert_eq!(
        market.update_config(original, checks),
        Err(MarketConfigUpdateError::Math(
            MarketMathUpdateError::ScaleInUse
        ))
    );
    assert_eq!(market, before);
}

#[test]
fn configuration_checks_numeric_exactness_limits_and_priority_guard() {
    let original = config();
    assert!(original.validate().is_ok());
    let mut bad = original.clone();
    bad.tick_size = 0;
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.lot_size = 0;
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.price_scale = 1;
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.order_limits.max_qty = Qty::try_new(1).unwrap();
    bad.order_limits.min_qty = Qty::try_new(2).unwrap();
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.order_limits.max_notional = QuoteAmount::new(0);
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.execution_limits.priority_exhaustion_guard = 0;
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.execution_limits.priority_exhaustion_guard = u128::MAX;
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.price_band = Some(matching_domain::PriceBandConfig {
        reference_kind: matching_domain::PriceReferenceKind::LastTrade,
        max_deviation_bps: 10_000,
    });
    assert!(bad.validate().is_err());
    bad = original;
    bad.execution_limits.synthetic_priority_ceiling = Some(0);
    assert!(bad.validate().is_err());
}

#[test]
fn configuration_retains_specific_math_validation_errors() {
    use matching_domain::{MarketConfigError, MathConfigError};
    let mut bad = config();
    bad.tick_size = 0;
    assert_eq!(
        bad.validate(),
        Err(MarketConfigError::Math(MathConfigError::InvalidTick))
    );
    bad = config();
    bad.lot_size = 0;
    assert_eq!(
        bad.validate(),
        Err(MarketConfigError::Math(MathConfigError::InvalidLot))
    );
    bad = config();
    bad.quote_scale = 39;
    assert_eq!(
        bad.validate(),
        Err(MarketConfigError::Math(MathConfigError::UnsupportedScale))
    );
    bad = config();
    bad.price_scale = 1;
    assert_eq!(
        bad.validate(),
        Err(MarketConfigError::Math(
            MathConfigError::NonIntegralQuoteUnit
        ))
    );
    bad = config();
    bad.quote_scale = 38;
    bad.tick_size = 4;
    assert_eq!(
        bad.validate(),
        Err(MarketConfigError::Math(MathConfigError::ArithmeticOverflow))
    );
}

#[test]
fn invalid_price_band_config_uses_the_stable_slippage_reason() {
    let mut bad = config();
    bad.price_band = Some(matching_domain::PriceBandConfig {
        reference_kind: matching_domain::PriceReferenceKind::LastTrade,
        max_deviation_bps: 10_000,
    });
    assert_eq!(
        bad.validate().unwrap_err().to_reject_reason(),
        matching_domain::RejectReason::InvalidSlippageBps
    );
}

#[test]
fn configuration_snapshot_roundtrip_and_truncation() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut original = config();
    original.price_band = Some(matching_domain::PriceBandConfig {
        reference_kind: matching_domain::PriceReferenceKind::MarkPrice,
        max_deviation_bps: 35,
    });
    original.execution_limits.synthetic_priority_ceiling = Some(99);
    let mut writer = SnapshotEncoder::new(512);
    original.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    for len in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..len], 512).unwrap();
        assert!(MarketConfig::read_snapshot(&mut r).is_err());
    }
    let mut r = SnapshotDecoder::new(&bytes, 512).unwrap();
    assert_eq!(MarketConfig::read_snapshot(&mut r).unwrap(), original);
    r.finish().unwrap();
    let mut unknown_version = bytes.clone();
    unknown_version[0] = 2;
    let mut r = SnapshotDecoder::new(&unknown_version, 512).unwrap();
    assert!(MarketConfig::read_snapshot(&mut r).is_err());
}

#[test]
fn failed_configuration_encoding_does_not_modify_outer_buffer() {
    use matching_domain::snapshot_wire::SnapshotEncoder;
    let mut writer = SnapshotEncoder::new(4);
    writer.u8(9).unwrap();
    assert!(config().write_snapshot(&mut writer).is_err());
    assert_eq!(writer.finish(), vec![9]);
}

#[test]
fn start_session_plans_ordered_events_and_rejects_nonempty_book() {
    use matching_domain::{
        EngineEvent, ManagedMarket, MarketState, MarketStateReason, RejectReason,
    };
    let mut market = ManagedMarket::new(config()).unwrap();
    let before = market.clone();
    assert_eq!(
        market.start_session(7, false),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    assert_eq!(market, before);
    let events = market.start_session(7, true).unwrap();
    assert_eq!(
        events,
        vec![
            EngineEvent::SessionStarted {
                market_id: MarketId(7),
                session_id: 7
            },
            EngineEvent::MarketStateChanged {
                market_id: MarketId(7),
                old: MarketState::Closed,
                new: MarketState::PreOpen,
                reason: MarketStateReason::SessionStarted
            },
        ]
    );
    assert_eq!(market.lifecycle().session_id(), Some(7));
}

#[test]
fn open_requires_matching_session_and_ready_price_band_without_mutation() {
    use matching_domain::{
        EngineEvent, ManagedMarket, MarketState, MarketStateReason, PriceBandConfig,
        PriceReferenceKind, RejectReason,
    };
    let mut cfg = config();
    cfg.price_band = Some(PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 50,
    });
    let mut market = ManagedMarket::new(cfg).unwrap();
    market.start_session(7, true).unwrap();
    let before = market.clone();
    assert_eq!(
        market.open(8, true, true),
        Err(RejectReason::InvalidSession)
    );
    assert_eq!(
        market.open(7, false, true),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(
        market.open(7, true, false),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    assert_eq!(market, before);
    assert_eq!(
        market.open(7, true, true).unwrap(),
        vec![EngineEvent::MarketStateChanged {
            market_id: MarketId(7),
            old: MarketState::PreOpen,
            new: MarketState::Open,
            reason: MarketStateReason::Opened,
        }]
    );
}

#[test]
fn suspend_and_resume_emit_ordered_state_changes_and_guard_reference() {
    use matching_domain::{
        EngineEvent, ManagedMarket, MarketState, MarketStateReason, PriceBandConfig,
        PriceReferenceKind, RejectReason,
    };
    let mut cfg = config();
    cfg.price_band = Some(PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 50,
    });
    let mut market = ManagedMarket::new(cfg).unwrap();
    market.start_session(7, true).unwrap();
    market.open(7, true, true).unwrap();
    assert_eq!(
        market.suspend().unwrap(),
        vec![EngineEvent::MarketStateChanged {
            market_id: MarketId(7),
            old: MarketState::Open,
            new: MarketState::Suspended,
            reason: MarketStateReason::Admin
        }]
    );
    let before = market.clone();
    assert_eq!(
        market.resume(false),
        Err(RejectReason::ReferencePriceUnavailable)
    );
    assert_eq!(market, before);
    assert_eq!(
        market.resume(true).unwrap(),
        vec![EngineEvent::MarketStateChanged {
            market_id: MarketId(7),
            old: MarketState::Suspended,
            new: MarketState::Open,
            reason: MarketStateReason::Admin
        }]
    );
}

#[test]
fn close_requires_completed_cleanup_before_state_change() {
    use matching_domain::{
        EngineEvent, ManagedMarket, MarketState, MarketStateReason, RejectReason,
    };
    let mut market = ManagedMarket::new(config()).unwrap();
    market.start_session(7, true).unwrap();
    market.open(7, true, true).unwrap();
    let before = market.clone();
    assert_eq!(
        market.close(false),
        Err(RejectReason::InvalidMarketStateTransition)
    );
    assert_eq!(market, before);
    assert_eq!(
        market.close(true).unwrap(),
        vec![EngineEvent::MarketStateChanged {
            market_id: MarketId(7),
            old: MarketState::Open,
            new: MarketState::Closed,
            reason: MarketStateReason::Admin
        }]
    );
    assert_eq!(
        market.resume(true),
        Err(RejectReason::InvalidMarketStateTransition)
    );
}

#[test]
fn managed_market_snapshot_preserves_config_and_lifecycle() {
    use matching_domain::{
        ManagedMarket,
        snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
    };
    let mut market = ManagedMarket::new(config()).unwrap();
    market.start_session(7, true).unwrap();
    market.open(7, true, true).unwrap();
    market.suspend().unwrap();
    let mut encoder = SnapshotEncoder::new(1024);
    market.write_snapshot(&mut encoder).unwrap();
    let bytes = encoder.finish();
    for n in 0..bytes.len() {
        let mut decoder = SnapshotDecoder::new(&bytes[..n], 1024).unwrap();
        assert!(ManagedMarket::read_snapshot(&mut decoder).is_err());
    }
    let mut decoder = SnapshotDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(ManagedMarket::read_snapshot(&mut decoder).unwrap(), market);
    decoder.finish().unwrap();
    let mut small = SnapshotEncoder::new(2);
    small.u8(9).unwrap();
    assert!(market.write_snapshot(&mut small).is_err());
    assert_eq!(small.finish(), vec![9]);
}

#[test]
fn managed_config_update_checks_candidate_and_emits_only_after_success() {
    use matching_domain::{
        EngineEvent, ManagedMarket, MarketConfigUpdateChecks, MarketConfigUpdateError,
    };
    let mut market = ManagedMarket::new(config()).unwrap();
    let before = market.clone();
    let checks = MarketConfigUpdateChecks {
        has_active_orders: false,
        has_referenced_history: false,
        existing_orders_valid: true,
        fee_version_available: true,
    };
    let mut candidate = config();
    candidate.tick_size = 2;
    assert!(matches!(
        market.update_config(candidate.clone(), checks),
        Err(MarketConfigUpdateError::Math(_))
    ));
    assert_eq!(market, before);
    market.start_session(7, true).unwrap();
    market.open(7, true, true).unwrap();
    market.suspend().unwrap();
    let before = market.clone();
    let mut untrusted = checks;
    untrusted.existing_orders_valid = false;
    assert_eq!(
        market.update_config(candidate.clone(), untrusted),
        Err(MarketConfigUpdateError::ExistingOrdersInvalid)
    );
    assert_eq!(market, before);
    untrusted = checks;
    untrusted.fee_version_available = false;
    assert_eq!(
        market.update_config(candidate.clone(), untrusted),
        Err(MarketConfigUpdateError::FeeVersionUnavailable)
    );
    assert_eq!(market, before);
    untrusted = checks;
    untrusted.has_active_orders = true;
    assert!(matches!(
        market.update_config(candidate.clone(), untrusted),
        Err(MarketConfigUpdateError::Math(_))
    ));
    assert_eq!(market, before);
    assert_eq!(
        market.update_config(candidate.clone(), checks).unwrap(),
        vec![EngineEvent::MarketConfigUpdated {
            market_id: MarketId(7)
        }]
    );
    assert_eq!(market.config(), &candidate);
}

#[test]
fn management_commands_preserve_every_wal_payload_and_reject_truncation() {
    use matching_domain::{
        CloseMarket, Command, CommandEnvelope, CommandId, CreateMarket, EndSession, OpenMarket,
        ResumeMarket, StartSession, SuspendMarket, UpdateMarket, decode_wal_command,
        encode_wal_command,
    };
    let id = MarketId(7);
    let commands = [
        Command::CreateMarket(Box::new(CreateMarket { config: config() })),
        Command::UpdateMarket(Box::new(UpdateMarket { config: config() })),
        Command::StartSession(StartSession {
            market_id: id,
            new_session_id: 8,
        }),
        Command::OpenMarket(OpenMarket {
            market_id: id,
            session_id: 8,
        }),
        Command::SuspendMarket(SuspendMarket { market_id: id }),
        Command::ResumeMarket(ResumeMarket { market_id: id }),
        Command::EndSession(EndSession {
            market_id: id,
            session_id: 8,
        }),
        Command::CloseMarket(CloseMarket { market_id: id }),
    ];
    let mut encodings = std::collections::BTreeSet::new();
    for (expected_tag, payload) in [4_u8, 5, 3, 6, 7, 8, 9, 10].into_iter().zip(commands) {
        let command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 2,
            client_seq: 3,
            market_id: Some(id),
            user_id: None,
            route_epoch: Some(4),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload,
        };
        let encoded = encode_wal_command(&command).unwrap();
        assert_eq!(encoded[50], expected_tag);
        assert!(encodings.insert(encoded.clone()));
        assert_eq!(decode_wal_command(&encoded).unwrap(), command);
        for n in 0..encoded.len() {
            assert!(decode_wal_command(&encoded[..n]).is_err());
        }
    }
}

#[test]
fn invalid_numeric_configuration_is_preserved_in_command_wal() {
    use matching_domain::{
        Command, CommandEnvelope, CommandId, UpdateMarket, decode_wal_command, encode_wal_command,
    };
    for invalid_tick in [true, false] {
        let mut invalid = config();
        if invalid_tick {
            invalid.tick_size = 0;
        } else {
            invalid.lot_size = 0;
        }
        let mut snapshot = matching_domain::snapshot_wire::SnapshotEncoder::new(256);
        snapshot.u8(77).unwrap();
        assert!(invalid.write_snapshot(&mut snapshot).is_err());
        assert_eq!(snapshot.finish(), vec![77]);
        let command = CommandEnvelope {
            command_id: CommandId(9),
            session_id: 1,
            client_seq: 1,
            market_id: Some(invalid.market_id),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::UpdateMarket(Box::new(UpdateMarket { config: invalid })),
        };
        let encoded = encode_wal_command(&command).unwrap();
        assert_eq!(decode_wal_command(&encoded).unwrap(), command);
    }
}
