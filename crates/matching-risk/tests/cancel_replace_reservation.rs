use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, CommandId, MarketId, OrderId, OriginSessionId, RejectReason,
    ReservationAdjustmentToken, ReservationId, ReservationToken, ReservationTransition, UserId,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationApplyError, ReservationRegistry, ReservationState,
    ReservationStatus, RiskKeySet,
};

fn reserves(atoms: u128) -> Vec<AssetReserve> {
    vec![
        AssetReserve {
            asset: AssetId(1),
            atoms,
        },
        AssetReserve {
            asset: AssetId(2),
            atoms: 5,
        },
    ]
}

fn fixture() -> (
    SigningKey,
    RiskKeySet,
    ReservationState,
    ExpectedReplacementReservation,
) {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        origin_session_id: OriginSessionId(7),
        version: 7,
        reserves: reserves(20),
        status: ReservationStatus::Active,
        fee_schedule_version: 9,
        fee_tier_id: 10,
    };
    let expected = ExpectedReplacementReservation {
        command_id: CommandId(2),
        old_reservation_id: state.reservation_id,
        old_order_id: state.order_id,
        new_order_id: OrderId(8),
        user_id: state.user_id,
        market_id: state.market_id,
        expected_version: state.version,
        fee_schedule_version: 9,
        fee_tier_id: 10,
        required_reserves: reserves(15),
    };
    (signing, keys, state, expected)
}

fn new_token(signing: &SigningKey, e: &ExpectedReplacementReservation) -> ReservationToken {
    let mut token = ReservationToken {
        token_id: 11,
        command_id: e.command_id,
        reservation_id: ReservationId(12),
        order_id: e.new_order_id,
        user_id: e.user_id,
        market_id: e.market_id,
        reserves: reserves(30).into(),
        fee_schedule_version: e.fee_schedule_version,
        fee_tier_id: e.fee_tier_id,
        risk_key_id: 31,
        signature: [0; 64],
    };
    sign_new(signing, &mut token);
    token
}

fn sign_new(signing: &SigningKey, token: &mut ReservationToken) {
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
}

fn adjustment(
    signing: &SigningKey,
    e: &ExpectedReplacementReservation,
) -> ReservationAdjustmentToken {
    let mut token = ReservationAdjustmentToken {
        token_id: 13,
        command_id: e.command_id,
        reservation_id: e.old_reservation_id,
        order_id: e.old_order_id,
        user_id: e.user_id,
        market_id: e.market_id,
        expected_reservation_version: e.expected_version,
        target_reserves: reserves(40).into(),
        fee_schedule_version: e.fee_schedule_version,
        fee_tier_id: e.fee_tier_id,
        risk_key_id: 31,
        signature: [0; 64],
    };
    sign_adjustment(signing, &mut token);
    token
}

fn sign_adjustment(signing: &SigningKey, token: &mut ReservationAdjustmentToken) {
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
}

#[test]
fn reuse_without_token_plans_twice_without_mutation_and_only_apply_rebinds() {
    let (_, keys, state, expected) = fixture();
    let mut registry = ReservationRegistry::new([state.clone()]).unwrap();
    let before = registry.clone();
    let transition = ReservationTransition::ReuseOld { adjustment: None };
    let first = registry
        .plan_transition(&keys, &expected, &transition)
        .unwrap();
    let second = registry
        .plan_transition(&keys, &expected, &transition)
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(registry, before);
    assert_eq!(first.active_state().order_id, expected.new_order_id);
    assert_eq!(first.pending_r2_state(), None);
    registry.apply_transition(first.clone()).unwrap();
    let active = registry.get(state.reservation_id).unwrap();
    assert_eq!(active.order_id, expected.new_order_id);
    assert_eq!(active.status, ReservationStatus::Active);
    assert_eq!(active.reservation_id, state.reservation_id);
    assert_eq!(active.version, 8);
    assert_eq!(active.reserves, expected.required_reserves);
    let after = registry.clone();
    assert_eq!(
        registry.apply_transition(first),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(registry, after);
}

#[test]
fn signed_reuse_binds_old_until_apply_and_covers_increased_exposure() {
    let (signing, keys, state, mut expected) = fixture();
    expected.required_reserves = reserves(35);
    let mut registry = ReservationRegistry::new([state.clone()]).unwrap();
    assert_eq!(
        registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReuseOld { adjustment: None }
            )
            .unwrap_err()
            .reason,
        RejectReason::InsufficientReservation
    );
    let token = adjustment(&signing, &expected);
    let transition = ReservationTransition::ReuseOld {
        adjustment: Some(token.clone()),
    };
    let plan = registry
        .plan_transition(&keys, &expected, &transition)
        .unwrap();
    assert_eq!(registry.get(state.reservation_id), Some(&state));
    registry.apply_transition(plan).unwrap();
    assert_eq!(
        registry.get(state.reservation_id).unwrap().order_id,
        expected.new_order_id
    );
    assert_eq!(
        registry
            .get(state.reservation_id)
            .unwrap()
            .reserves
            .as_slice(),
        token.target_reserves.as_slice()
    );
}

#[test]
fn pending_r2_replace_registers_new_and_terminalizes_only_old_binding_at_apply() {
    let (signing, keys, state, expected) = fixture();
    let token = new_token(&signing, &expected);
    let mut registry = ReservationRegistry::new([state.clone()]).unwrap();
    let before = registry.clone();
    let transition = ReservationTransition::ReplaceWithNew {
        new_reservation: token.clone(),
    };
    let first = registry
        .plan_transition(&keys, &expected, &transition)
        .unwrap();
    assert_eq!(
        first,
        registry
            .plan_transition(&keys, &expected, &transition)
            .unwrap()
    );
    assert_eq!(registry, before);
    assert_eq!(registry.get(token.reservation_id), None);
    assert_eq!(first.pending_r2_state(), Some(&state));
    registry.apply_transition(first.clone()).unwrap();
    let old = registry.get(state.reservation_id).unwrap();
    assert_eq!(old, &state);
    assert!(registry.is_pending_r2(state.reservation_id));
    let new = registry.get(token.reservation_id).unwrap();
    assert!(!registry.is_pending_r2(token.reservation_id));
    assert_eq!(new.status, ReservationStatus::Active);
    assert_eq!(new.order_id, expected.new_order_id);
    assert_eq!(new.version, 0);
    assert_eq!(new.reserves.as_slice(), token.reserves.as_slice());
    assert_eq!((new.fee_schedule_version, new.fee_tier_id), (9, 10));
    registry
        .validate_order_bindings([(new.order_id, new)])
        .unwrap();
    assert_eq!(
        registry.validate_order_bindings([(state.order_id, &state)]),
        Err(RejectReason::InvalidReservation)
    );
    let after = registry.clone();
    assert_eq!(
        registry.apply_transition(first),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(registry, after);
}

#[test]
fn new_token_rejects_each_signed_identity_key_fee_and_coverage_failure_atomically() {
    let (signing, keys, state, expected) = fixture();
    let registry = ReservationRegistry::new([state]).unwrap();
    let before = registry.clone();
    for case in 0..11 {
        let mut token = new_token(&signing, &expected);
        match case {
            0 => token.command_id = CommandId(99),
            1 => token.user_id = UserId(99),
            2 => token.market_id = MarketId(99),
            3 => token.order_id = expected.old_order_id,
            4 => token.reservation_id = expected.old_reservation_id,
            5 => token.fee_schedule_version += 1,
            6 => token.fee_tier_id += 1,
            7 => token.reserves[0].atoms = 14,
            8 => token.risk_key_id = 99,
            9 => token.reserves[1].asset = AssetId(3),
            10 => (),
            _ => unreachable!(),
        }
        sign_new(&signing, &mut token);
        if case == 10 {
            token.signature[0] ^= 1;
        }
        let id = token.reservation_id;
        let rejection = registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReplaceWithNew {
                    new_reservation: token,
                },
            )
            .unwrap_err();
        assert_eq!(
            rejection.reason,
            if case == 7 || case == 9 {
                RejectReason::InsufficientReservation
            } else {
                RejectReason::InvalidReservation
            },
            "case {case}"
        );
        assert_eq!(rejection.unadopted_reservation.unwrap().reservation_id, id);
        assert_eq!(registry, before);
    }
}

#[test]
fn adjustment_rejects_bad_signature_all_identity_version_fee_and_coverage_cases() {
    let (signing, keys, state, expected) = fixture();
    let registry = ReservationRegistry::new([state]).unwrap();
    let before = registry.clone();
    for case in 0..12 {
        let mut token = adjustment(&signing, &expected);
        match case {
            0 => token.command_id = CommandId(99),
            1 => token.user_id = UserId(99),
            2 => token.market_id = MarketId(99),
            3 => token.order_id = expected.new_order_id,
            4 => token.reservation_id = ReservationId(99),
            5 => token.expected_reservation_version += 1,
            6 => token.fee_schedule_version += 1,
            7 => token.fee_tier_id += 1,
            8 => token.target_reserves[0].atoms = 14,
            9 => token.risk_key_id = 99,
            10 => token.target_reserves[1].asset = AssetId(3),
            11 => (),
            _ => unreachable!(),
        }
        sign_adjustment(&signing, &mut token);
        if case == 11 {
            token.signature[0] ^= 1;
        }
        let rejection = registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReuseOld {
                    adjustment: Some(token),
                },
            )
            .unwrap_err();
        assert_eq!(
            rejection.reason,
            if case == 8 || case == 10 {
                RejectReason::InsufficientReservation
            } else {
                RejectReason::InvalidReservation
            },
            "case {case}"
        );
        assert_eq!(rejection.unadopted_reservation, None);
        assert_eq!(registry, before);
    }
}

#[test]
fn rejects_noncanonical_assets_in_tokens_and_trusted_requirements() {
    let (signing, keys, state, expected) = fixture();
    let registry = ReservationRegistry::new([state]).unwrap();
    let before = registry.clone();
    for bad in [
        vec![],
        reserves(0),
        vec![reserves(1)[0]; 2],
        vec![reserves(1)[1], reserves(1)[0]],
    ] {
        let mut new = new_token(&signing, &expected);
        new.reserves = bad.clone().into();
        assert_eq!(
            registry
                .plan_transition(
                    &keys,
                    &expected,
                    &ReservationTransition::ReplaceWithNew {
                        new_reservation: new
                    }
                )
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        let mut token = adjustment(&signing, &expected);
        token.target_reserves = bad.clone().into();
        assert_eq!(
            registry
                .plan_transition(
                    &keys,
                    &expected,
                    &ReservationTransition::ReuseOld {
                        adjustment: Some(token)
                    }
                )
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        let mut changed = expected.clone();
        changed.required_reserves = bad;
        assert_eq!(
            registry
                .plan_transition(
                    &keys,
                    &changed,
                    &ReservationTransition::ReuseOld { adjustment: None }
                )
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        assert_eq!(registry, before);
    }
}

#[test]
fn stale_expected_version_inactive_old_fee_and_old_identity_mismatch_reject() {
    let (_, keys, state, expected) = fixture();
    for case in 0..9 {
        let mut changed = state.clone();
        match case {
            0 => changed.version += 1,
            1 => changed.status = ReservationStatus::Released,
            2 => changed.fee_schedule_version += 1,
            3 => changed.fee_tier_id += 1,
            4 => changed.order_id = OrderId(99),
            5 => changed.user_id = UserId(99),
            6 => changed.market_id = MarketId(99),
            7 => changed.reservation_id = ReservationId(99),
            8 => changed.reserves[0].atoms = 14,
            _ => unreachable!(),
        }
        let registry = ReservationRegistry::new([changed]).unwrap();
        let before = registry.clone();
        assert_eq!(
            registry
                .plan_transition(
                    &keys,
                    &expected,
                    &ReservationTransition::ReuseOld { adjustment: None }
                )
                .unwrap_err()
                .reason,
            if case == 8 {
                RejectReason::InsufficientReservation
            } else {
                RejectReason::InvalidReservation
            }
        );
        assert_eq!(registry, before);
    }
}

#[test]
fn overflow_is_rejected_before_reuse_changes_any_financial_state() {
    let (signing, keys, mut state, mut expected) = fixture();
    state.version = u64::MAX;
    expected.expected_version = u64::MAX;
    let registry = ReservationRegistry::new([state]).unwrap();
    let before = registry.clone();
    for transition in [
        ReservationTransition::ReuseOld { adjustment: None },
        ReservationTransition::ReuseOld {
            adjustment: Some(adjustment(&signing, &expected)),
        },
    ] {
        assert_eq!(
            registry
                .plan_transition(&keys, &expected, &transition)
                .unwrap_err()
                .reason,
            RejectReason::ArithmeticOverflow
        );
        assert_eq!(registry, before);
    }
}

#[test]
fn pending_r2_replace_preserves_max_old_version_without_financial_increment() {
    let (signing, keys, mut state, mut expected) = fixture();
    state.version = u64::MAX;
    expected.expected_version = u64::MAX;
    let mut registry = ReservationRegistry::new([state.clone()]).unwrap();
    let before = registry.clone();
    let token = new_token(&signing, &expected);
    let transition = ReservationTransition::ReplaceWithNew {
        new_reservation: token.clone(),
    };
    let plan = registry
        .plan_transition(&keys, &expected, &transition)
        .unwrap();
    assert_eq!(registry, before);
    registry.apply_transition(plan).unwrap();
    assert_eq!(registry.get(state.reservation_id), Some(&state));
    assert!(registry.is_pending_r2(state.reservation_id));
    let new = registry.get(token.reservation_id).unwrap();
    assert_eq!(new.version, 0);
    registry
        .validate_order_bindings([(new.order_id, new)])
        .unwrap();
}

#[test]
fn duplicate_ids_are_rejected_including_released_registry_entries_and_new_order_binding() {
    let (signing, keys, state, expected) = fixture();
    assert_eq!(
        ReservationRegistry::new([state.clone(), state.clone()]),
        Err(RejectReason::InvalidReservation)
    );
    let mut other = state.clone();
    other.reservation_id = ReservationId(12);
    assert_eq!(
        ReservationRegistry::new([state.clone(), other.clone()]),
        Err(RejectReason::InvalidReservation)
    );
    other.order_id = OrderId(99);
    for status in [ReservationStatus::Active, ReservationStatus::Released] {
        other.status = status;
        let registry = ReservationRegistry::new([state.clone(), other.clone()]).unwrap();
        let before = registry.clone();
        let new = new_token(&signing, &expected);
        assert_eq!(
            registry
                .plan_transition(
                    &keys,
                    &expected,
                    &ReservationTransition::ReplaceWithNew {
                        new_reservation: new
                    }
                )
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        assert_eq!(registry, before);
    }
    other.status = ReservationStatus::Active;
    other.order_id = expected.new_order_id;
    let registry = ReservationRegistry::new([state, other]).unwrap();
    assert_eq!(
        registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReuseOld { adjustment: None }
            )
            .unwrap_err()
            .reason,
        RejectReason::InvalidReservation
    );
}

#[test]
fn registry_rejects_bad_snapshot_assets_and_detects_order_map_drift() {
    let (_, _, state, _) = fixture();
    let registry = ReservationRegistry::new([state.clone()]).unwrap();
    registry
        .validate_order_bindings([(state.order_id, &state)])
        .unwrap();
    assert_eq!(
        registry.validate_order_bindings([(OrderId(99), &state)]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.validate_order_bindings([]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.validate_order_bindings([(state.order_id, &state), (state.order_id, &state)]),
        Err(RejectReason::InvalidReservation)
    );
    let mut wrong = state.clone();
    wrong.version += 1;
    assert_eq!(
        registry.validate_order_bindings([(wrong.order_id, &wrong)]),
        Err(RejectReason::InvalidReservation)
    );
    for bad in [
        vec![],
        reserves(0),
        vec![reserves(1)[0]; 2],
        vec![reserves(1)[1], reserves(1)[0]],
    ] {
        let mut wrong = state.clone();
        wrong.reserves = bad;
        assert_eq!(
            ReservationRegistry::new([wrong]),
            Err(RejectReason::InvalidReservation)
        );
    }
}

#[test]
fn apply_detects_new_reservation_or_new_order_collision_before_touching_old() {
    let (signing, keys, state, expected) = fixture();
    let mut other = state.clone();
    other.reservation_id = ReservationId(100);
    other.order_id = OrderId(101);
    let mut other_expected = expected.clone();
    other_expected.old_reservation_id = other.reservation_id;
    other_expected.old_order_id = other.order_id;
    other_expected.new_order_id = OrderId(102);
    for same_order in [false, true] {
        let mut registry = ReservationRegistry::new([state.clone(), other.clone()]).unwrap();
        let pending = registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReplaceWithNew {
                    new_reservation: new_token(&signing, &expected),
                },
            )
            .unwrap();
        let competing = if same_order {
            let mut changed = other_expected.clone();
            changed.new_order_id = expected.new_order_id;
            registry.plan_transition(
                &keys,
                &changed,
                &ReservationTransition::ReuseOld { adjustment: None },
            )
        } else {
            registry.plan_transition(
                &keys,
                &other_expected,
                &ReservationTransition::ReplaceWithNew {
                    new_reservation: new_token(&signing, &other_expected),
                },
            )
        }
        .unwrap();
        registry.apply_transition(competing).unwrap();
        let before = registry.clone();
        assert_eq!(
            registry.apply_transition(pending),
            Err(ReservationApplyError::StateChanged)
        );
        assert_eq!(registry, before);
        assert_eq!(registry.get(state.reservation_id), Some(&state));
    }
}

#[test]
fn signed_asset_set_cannot_add_unrequested_assets_and_key_rotation_revokes_old_key() {
    let (signing, keys, state, expected) = fixture();
    let registry = ReservationRegistry::new([state]).unwrap();
    let before = registry.clone();
    let mut new = new_token(&signing, &expected);
    new.reserves.push(AssetReserve {
        asset: AssetId(3),
        atoms: 1,
    });
    sign_new(&signing, &mut new);
    let mut adjust = adjustment(&signing, &expected);
    adjust.target_reserves.push(AssetReserve {
        asset: AssetId(3),
        atoms: 1,
    });
    sign_adjustment(&signing, &mut adjust);
    for transition in [
        ReservationTransition::ReplaceWithNew {
            new_reservation: new,
        },
        ReservationTransition::ReuseOld {
            adjustment: Some(adjust),
        },
    ] {
        assert_eq!(
            registry
                .plan_transition(&keys, &expected, &transition)
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        assert_eq!(registry, before);
    }
    let rotated = RiskKeySet::new(2, []).unwrap();
    for transition in [
        ReservationTransition::ReplaceWithNew {
            new_reservation: new_token(&signing, &expected),
        },
        ReservationTransition::ReuseOld {
            adjustment: Some(adjustment(&signing, &expected)),
        },
    ] {
        assert!(
            registry
                .plan_transition(&keys, &expected, &transition)
                .is_ok()
        );
        assert_eq!(
            registry
                .plan_transition(&rotated, &expected, &transition)
                .unwrap_err()
                .reason,
            RejectReason::InvalidReservation
        );
        assert_eq!(registry, before);
    }
}

#[test]
fn pending_r2_terminal_batch_is_pure_and_preserves_all_financial_fields() {
    let (_, _, state, _) = fixture();
    let mut maker = state.clone();
    maker.reservation_id = ReservationId(20);
    maker.order_id = OrderId(21);
    let mut registry = ReservationRegistry::new([state.clone(), maker.clone()]).unwrap();
    let before = registry.clone();
    assert_eq!(
        registry.validate_order_bindings([]),
        Err(RejectReason::InvalidReservation)
    );
    let first = registry
        .plan_binding_transitions([&state, &maker], [])
        .unwrap();
    let second = registry
        .plan_binding_transitions([&state, &maker], [])
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(registry, before);
    registry.apply_binding_transition(first.clone()).unwrap();
    for snapshot in [&state, &maker] {
        assert!(registry.is_pending_r2(snapshot.reservation_id));
        assert_eq!(registry.get(snapshot.reservation_id), Some(snapshot));
    }
    registry.validate_order_bindings([]).unwrap();
    assert_eq!(
        registry.validate_order_bindings([(state.order_id, &state)]),
        Err(RejectReason::InvalidReservation)
    );
    let after = registry.clone();
    assert_eq!(
        registry.apply_binding_transition(first),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(registry, after);
}

#[test]
fn pending_r2_restore_rejects_unknown_duplicate_and_released_ownership() {
    let (_, _, state, _) = fixture();
    for ids in [vec![ReservationId(99)], vec![state.reservation_id; 2]] {
        assert_eq!(
            ReservationRegistry::new_with_pending_r2([state.clone()], ids),
            Err(RejectReason::InvalidReservation)
        );
    }
    let mut released = state.clone();
    released.status = ReservationStatus::Released;
    assert_eq!(
        ReservationRegistry::new_with_pending_r2([released.clone()], [released.reservation_id]),
        Err(RejectReason::InvalidReservation)
    );
    let registry = ReservationRegistry::new([released.clone()]).unwrap();
    registry.validate_order_bindings([]).unwrap();
    assert_eq!(
        registry.validate_order_bindings([(released.order_id, &released)]),
        Err(RejectReason::InvalidReservation)
    );
    for absent in [&state, &released] {
        assert_eq!(
            registry.plan_binding_transitions([absent], []),
            Err(RejectReason::InvalidReservation)
        );
    }
}

#[test]
fn pending_r2_can_resume_only_the_existing_financial_order_snapshot() {
    let (_, _, state, _) = fixture();
    let mut registry =
        ReservationRegistry::new_with_pending_r2([state.clone()], [state.reservation_id]).unwrap();
    let before = registry.clone();
    let first = registry.plan_binding_transitions([], [&state]).unwrap();
    assert_eq!(
        first,
        registry.plan_binding_transitions([], [&state]).unwrap()
    );
    assert_eq!(registry, before);
    let mut wrong = state.clone();
    wrong.order_id = OrderId(99);
    assert_eq!(
        registry.plan_binding_transitions([], [&wrong]),
        Err(RejectReason::InvalidReservation)
    );
    registry.apply_binding_transition(first).unwrap();
    assert!(!registry.is_pending_r2(state.reservation_id));
    assert_eq!(registry.get(state.reservation_id), Some(&state));
    registry
        .validate_order_bindings([(state.order_id, &state)])
        .unwrap();
    assert_eq!(
        registry.plan_binding_transitions([], [&state]),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn pending_r2_batch_rejects_duplicate_requests_and_stale_snapshots_without_mutation() {
    let (_, _, state, _) = fixture();
    let registry = ReservationRegistry::new([state.clone()]).unwrap();
    let before = registry.clone();
    assert_eq!(
        registry.plan_binding_transitions([&state, &state], []),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.plan_binding_transitions([&state], [&state]),
        Err(RejectReason::InvalidReservation)
    );
    let mut wrong = state.clone();
    wrong.reserves[0].atoms += 1;
    assert_eq!(
        registry.plan_binding_transitions([&wrong], []),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(registry, before);
    let pending =
        ReservationRegistry::new_with_pending_r2([state.clone()], [state.reservation_id]).unwrap();
    assert_eq!(
        pending.plan_binding_transitions([], [&state, &state]),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn pending_r2_batch_rejects_existing_and_in_batch_active_order_collisions() {
    let (_, _, state, _) = fixture();
    let mut other = state.clone();
    other.reservation_id = ReservationId(20);
    assert_eq!(
        ReservationRegistry::new([state.clone(), other.clone()]),
        Err(RejectReason::InvalidReservation)
    );
    let registry = ReservationRegistry::new_with_pending_r2(
        [state.clone(), other.clone()],
        [state.reservation_id],
    )
    .unwrap();
    registry
        .validate_order_bindings([(other.order_id, &other)])
        .unwrap();
    assert_eq!(
        registry.plan_binding_transitions([], [&state]),
        Err(RejectReason::InvalidReservation)
    );
    let mut all_pending = ReservationRegistry::new_with_pending_r2(
        [state.clone(), other.clone()],
        [state.reservation_id, other.reservation_id],
    )
    .unwrap();
    assert_eq!(
        all_pending.plan_binding_transitions([], [&state, &other]),
        Err(RejectReason::InvalidReservation)
    );
    let swap = registry
        .plan_binding_transitions([&other], [&state])
        .unwrap();
    let mut registry = registry;
    registry.apply_binding_transition(swap).unwrap();
    registry
        .validate_order_bindings([(state.order_id, &state)])
        .unwrap();
    let resume = all_pending.plan_binding_transitions([], [&state]).unwrap();
    all_pending.apply_binding_transition(resume).unwrap();
    all_pending
        .validate_order_bindings([(state.order_id, &state)])
        .unwrap();
}

#[test]
fn pending_r2_batch_apply_detects_binding_and_financial_drift_before_any_write() {
    let (_, _, state, _) = fixture();
    let mut maker = state.clone();
    maker.reservation_id = ReservationId(20);
    maker.order_id = OrderId(21);
    let mut registry = ReservationRegistry::new([state.clone(), maker.clone()]).unwrap();
    let batch = registry
        .plan_binding_transitions([&state, &maker], [])
        .unwrap();
    let maker_only = registry.plan_binding_transitions([&maker], []).unwrap();
    registry.apply_binding_transition(maker_only).unwrap();
    let before = registry.clone();
    assert_eq!(
        registry.apply_binding_transition(batch),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(registry, before);
    assert!(!registry.is_pending_r2(state.reservation_id));

    for case in 0..7 {
        let original = ReservationRegistry::new([state.clone(), maker.clone()]).unwrap();
        let batch = original
            .plan_binding_transitions([&state, &maker], [])
            .unwrap();
        let mut changed = maker.clone();
        match case {
            0 => changed.version += 1,
            1 => changed.reserves[0].atoms += 1,
            2 => changed.fee_schedule_version += 1,
            3 => changed.fee_tier_id += 1,
            4 => changed.status = ReservationStatus::Released,
            5 => changed.user_id = UserId(99),
            6 => changed.market_id = MarketId(99),
            _ => unreachable!(),
        }
        let mut drifted = ReservationRegistry::new([state.clone(), changed]).unwrap();
        let before = drifted.clone();
        assert_eq!(
            drifted.apply_binding_transition(batch),
            Err(ReservationApplyError::StateChanged)
        );
        assert_eq!(drifted, before, "case {case}");
    }
}

#[test]
fn pending_r2_binding_drift_blocks_previously_planned_financial_rebind() {
    let (_, keys, state, expected) = fixture();
    let mut registry = ReservationRegistry::new([state.clone()]).unwrap();
    let financial = registry
        .plan_transition(
            &keys,
            &expected,
            &ReservationTransition::ReuseOld { adjustment: None },
        )
        .unwrap();
    let terminal = registry.plan_binding_transitions([&state], []).unwrap();
    registry.apply_binding_transition(terminal).unwrap();
    let before = registry.clone();
    assert_eq!(
        registry.apply_transition(financial),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(registry, before);
    assert_eq!(
        registry
            .plan_transition(
                &keys,
                &expected,
                &ReservationTransition::ReuseOld { adjustment: None }
            )
            .unwrap_err()
            .reason,
        RejectReason::InvalidReservation
    );
}
