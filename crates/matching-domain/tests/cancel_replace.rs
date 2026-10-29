use matching_domain::{
    AssetId, AssetReserve, CancelReason, CancelReplaceOrder, CancelReplaceRejection, CommandId,
    EngineEvent, LogicalTime, MarketId, OrderFlags, OrderId, OrderType, Price, Qty, RejectReason,
    ReplacementOrder, ReplacementOrderKind, ReservationAdjustmentToken, ReservationId,
    ReservationReleaseReason, ReservationToken, ReservationTransition, Side, TimeInForce,
    UnadoptedReservation, UserId, validate_cancel_replace_old_type,
    validate_replacement_combination,
};
use smallvec::smallvec;

fn flags(post_only: bool) -> OrderFlags {
    OrderFlags {
        post_only,
        reduce_only: false,
    }
}

fn token() -> ReservationToken {
    ReservationToken {
        token_id: 1,
        command_id: CommandId(2),
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        reserves: smallvec![AssetReserve {
            asset: AssetId(0x0102_0304),
            atoms: 8
        }],
        fee_schedule_version: 9,
        fee_tier_id: 10,
        risk_key_id: 11,
        signature: [0x55; 64],
    }
}

fn command() -> CancelReplaceOrder {
    CancelReplaceOrder {
        market_id: MarketId(6),
        user_id: UserId(5),
        old_order_id: OrderId(7),
        replacement: ReplacementOrder {
            market_id: MarketId(6),
            user_id: UserId(5),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            order_id: OrderId(4),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
            kind: ReplacementOrderKind::Limit {
                tif: TimeInForce::Gtc,
                flags: flags(false),
            },
        },
        reservation_transition: ReservationTransition::ReuseOld { adjustment: None },
    }
}

#[test]
fn replacement_identity_is_explicit_and_cannot_equal_old_id() {
    let command = command();
    assert_eq!(command.validate_structure(), Ok(()));
    let mut changed = command.clone();
    changed.replacement.order_id = changed.old_order_id;
    assert_eq!(
        changed.validate_structure(),
        Err(RejectReason::DuplicateOrderId)
    );
    let mut changed = command.clone();
    changed.replacement.user_id = UserId(99);
    assert_eq!(
        changed.validate_structure(),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(changed.replacement.user_id, UserId(99));
    let mut changed = command;
    changed.replacement.market_id = MarketId(99);
    assert_eq!(
        changed.validate_structure(),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(changed.replacement.market_id, MarketId(99));
}

#[test]
fn supported_replacements_and_old_types_match_frozen_matrix() {
    let mut command = command();
    for old in [OrderType::Limit, OrderType::Iceberg, OrderType::StopLimit] {
        assert_eq!(validate_cancel_replace_old_type(old), Ok(()));
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Fok,
            TimeInForce::Day { session_id: 1 },
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(100),
            },
        ] {
            command.replacement.kind = ReplacementOrderKind::Limit {
                tif,
                flags: flags(false),
            };
            assert_eq!(command.validate_structure(), Ok(()));
        }
        command.replacement.kind = ReplacementOrderKind::Limit {
            tif: TimeInForce::Gtc,
            flags: flags(true),
        };
        assert_eq!(command.validate_structure(), Ok(()));
        for post_only in [false, true] {
            command.replacement.kind = ReplacementOrderKind::Iceberg {
                peak_size: Qty::try_new(3).unwrap(),
                tif: TimeInForce::Gtc,
                flags: flags(post_only),
            };
            assert_eq!(command.validate_structure(), Ok(()));
        }
    }
}

#[test]
fn unsupported_type_and_tif_inputs_have_stable_rejection() {
    let tifs = [
        TimeInForce::Gtc,
        TimeInForce::Ioc,
        TimeInForce::Fok,
        TimeInForce::Day { session_id: 1 },
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        },
    ];
    for kind in [
        OrderType::Market,
        OrderType::StopMarket,
        OrderType::StopLimit,
    ] {
        for tif in tifs {
            for post_only in [false, true] {
                assert_eq!(
                    validate_replacement_combination(kind, tif, flags(post_only)),
                    Err(RejectReason::InvalidOrderCombination)
                );
            }
        }
    }
    for tif in tifs.into_iter().filter(|tif| *tif != TimeInForce::Gtc) {
        assert_eq!(
            validate_replacement_combination(OrderType::Limit, tif, flags(true)),
            Err(RejectReason::InvalidOrderCombination)
        );
        for post_only in [false, true] {
            assert_eq!(
                validate_replacement_combination(OrderType::Iceberg, tif, flags(post_only)),
                Err(RejectReason::InvalidOrderCombination)
            );
        }
    }
    for old in [OrderType::Market, OrderType::StopMarket] {
        assert_eq!(
            validate_cancel_replace_old_type(old),
            Err(RejectReason::InvalidOrderCombination)
        );
    }
    for kind in [OrderType::Limit, OrderType::Iceberg] {
        let mut reduce_only = flags(false);
        reduce_only.reduce_only = true;
        assert_eq!(
            validate_replacement_combination(kind, TimeInForce::Gtc, reduce_only),
            Err(RejectReason::InvalidOrderCombination)
        );
    }
}

#[test]
fn replacement_structure_rejects_unsupported_tif_and_invalid_iceberg_peak() {
    let mut command = command();
    command.replacement.kind = ReplacementOrderKind::Limit {
        tif: TimeInForce::Day { session_id: 1 },
        flags: flags(true),
    };
    assert_eq!(
        command.validate_structure(),
        Err(RejectReason::InvalidOrderCombination)
    );
    command.replacement.kind = ReplacementOrderKind::Iceberg {
        peak_size: Qty::try_new(3).unwrap(),
        tif: TimeInForce::Ioc,
        flags: flags(false),
    };
    assert_eq!(
        command.validate_structure(),
        Err(RejectReason::InvalidOrderCombination)
    );
    for peak in [10, 11] {
        command.replacement.kind = ReplacementOrderKind::Iceberg {
            peak_size: Qty::try_new(peak).unwrap(),
            tif: TimeInForce::Gtc,
            flags: flags(false),
        };
        assert_eq!(
            command.validate_structure(),
            Err(RejectReason::InvalidIceberg)
        );
    }
}

#[test]
fn reservation_transition_preserves_independent_token_values() {
    let none = ReservationTransition::ReuseOld { adjustment: None };
    let adjustment = ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId(2),
        reservation_id: ReservationId(8),
        order_id: OrderId(7),
        user_id: UserId(5),
        market_id: MarketId(6),
        expected_reservation_version: 12,
        target_reserves: token().reserves,
        fee_schedule_version: 9,
        fee_tier_id: 10,
        risk_key_id: 11,
        signature: [0x55; 64],
    };
    let some = ReservationTransition::ReuseOld {
        adjustment: Some(adjustment.clone()),
    };
    let new = ReservationTransition::ReplaceWithNew {
        new_reservation: token(),
    };
    assert_ne!(none, some);
    assert_ne!(some, new);
    assert_eq!(new, new.clone());
    let ReservationTransition::ReuseOld {
        adjustment: Some(retained),
    } = some
    else {
        panic!()
    };
    assert_eq!(retained, adjustment);
    assert_eq!(retained.order_id, OrderId(7));
    let mut changed = token();
    changed.reservation_id = ReservationId(99);
    assert_ne!(
        new,
        ReservationTransition::ReplaceWithNew {
            new_reservation: changed
        }
    );
}

#[test]
fn reservation_token_payload_has_fixed_golden_encoding() {
    let token = token();
    let mut expected = Vec::new();
    for last_byte in [1, 2, 3, 4] {
        expected.extend_from_slice(&[0; 15]);
        expected.push(last_byte);
    }
    expected.extend_from_slice(&[0; 7]);
    expected.push(5);
    expected.extend_from_slice(&[0, 0, 0, 6]);
    expected.extend_from_slice(&[0, 0, 0, 1]);
    expected.extend_from_slice(&[1, 2, 3, 4]);
    expected.extend_from_slice(&[0; 15]);
    expected.push(8);
    expected.extend_from_slice(&[0; 7]);
    expected.push(9);
    expected.extend_from_slice(&[0, 0, 0, 10]);
    expected.extend_from_slice(&[0, 0, 0, 11]);
    assert_eq!(token.canonical_signing_payload().unwrap(), expected);
    assert_eq!(expected.len(), 116);
    let mut changed = token;
    changed.signature = [0xaa; 64];
    assert_eq!(changed.canonical_signing_payload().unwrap(), expected);
    changed.reserves.push(AssetReserve {
        asset: AssetId(0x0506_0708),
        atoms: u128::MAX,
    });
    let payload = changed.canonical_signing_payload().unwrap();
    assert_eq!(&payload[76..80], &[0, 0, 0, 2]);
    assert_eq!(&payload[100..104], &[5, 6, 7, 8]);
    assert_eq!(&payload[104..120], &[0xff; 16]);
}

#[test]
fn reservation_token_payload_binds_each_authorization_field() {
    let original = token();
    let baseline = original.canonical_signing_payload().unwrap();
    let mut cases = Vec::new();
    macro_rules! changed {
        ($field:ident, $value:expr) => {{
            let mut token = original.clone();
            token.$field = $value;
            cases.push(token);
        }};
    }
    changed!(token_id, 99);
    changed!(command_id, CommandId(99));
    changed!(reservation_id, ReservationId(99));
    changed!(order_id, OrderId(99));
    changed!(user_id, UserId(99));
    changed!(market_id, MarketId(99));
    changed!(fee_schedule_version, 99);
    changed!(fee_tier_id, 99);
    changed!(risk_key_id, 99);
    let mut changed_asset = original.clone();
    changed_asset.reserves[0].asset = AssetId(99);
    cases.push(changed_asset);
    let mut changed_atoms = original;
    changed_atoms.reserves[0].atoms = 99;
    cases.push(changed_atoms);
    for token in cases {
        assert_ne!(token.canonical_signing_payload().unwrap(), baseline);
    }
}

#[test]
fn reservation_token_payload_rejects_noncanonical_assets() {
    let mut token = token();
    token.reserves.clear();
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );
    token.reserves.push(AssetReserve {
        asset: AssetId(1),
        atoms: 0,
    });
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );
    token.reserves[0].atoms = 1;
    for next_asset in [1, 0] {
        token.reserves.truncate(1);
        token.reserves.push(AssetReserve {
            asset: AssetId(next_asset),
            atoms: 2,
        });
        assert_eq!(
            token.canonical_signing_payload(),
            Err(RejectReason::InvalidReservation)
        );
    }
}

#[test]
fn rejection_cleanup_is_a_result_identity_separate_from_engine_events() {
    let mut command = command();
    assert_eq!(
        command.rejection(RejectReason::InvalidOrderCombination),
        CancelReplaceRejection {
            reason: RejectReason::InvalidOrderCombination,
            unadopted_reservation: None,
        }
    );
    let mut unverified = token();
    unverified.reserves.clear();
    unverified.signature = [0; 64];
    assert_eq!(
        unverified.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );
    command.reservation_transition = ReservationTransition::ReplaceWithNew {
        new_reservation: unverified,
    };
    let before = command.clone();
    for reason in [
        RejectReason::DuplicateOrderId,
        RejectReason::OrderNotFound,
        RejectReason::InvalidReservation,
        RejectReason::InsufficientReservation,
    ] {
        let result = command.rejection(reason);
        assert_eq!(result, command.rejection(reason));
        assert_eq!(result.reason, reason);
        assert_eq!(
            result.unadopted_reservation,
            Some(UnadoptedReservation {
                reservation_id: ReservationId(3),
            })
        );
        assert_ne!(
            std::any::TypeId::of::<CancelReplaceRejection>(),
            std::any::TypeId::of::<EngineEvent>()
        );
        assert_eq!(command, before);
    }
}

#[test]
fn stable_cancel_and_release_codes_preserve_neighbors() {
    assert_eq!(CancelReason::CancelReplace as u16, 1109);
    assert_eq!(CancelReason::IocRemainder as u16, 1114);
    assert_eq!(ReservationReleaseReason::FilledRemainder as u16, 1301);
    assert_eq!(ReservationReleaseReason::UserCancel as u16, 1302);
    assert_eq!(ReservationReleaseReason::TriggerRejected as u16, 1305);
}
