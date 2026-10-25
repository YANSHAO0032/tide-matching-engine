use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, CommandId, MarketId, OrderId, RejectReason, ReservationAdjustmentToken,
    ReservationId, UserId,
};
use matching_risk::{
    ExpectedAmendReservation, ReservationState, RiskKeySet, verify_amend_reservation,
};

fn fixture() -> (
    SigningKey,
    RiskKeySet,
    ReservationState,
    ExpectedAmendReservation,
    ReservationAdjustmentToken,
) {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(4, [(31, signing.verifying_key())]).unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        version: 7,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 10,
        }],
    };
    let expected = ExpectedAmendReservation {
        command_id: CommandId(2),
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        fee_schedule_version: 9,
        fee_tier_id: 10,
        required_reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 20,
        }],
    };
    let mut token = ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId(2),
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        expected_reservation_version: 7,
        target_reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 21,
        }]
        .into(),
        fee_schedule_version: 9,
        fee_tier_id: 10,
        risk_key_id: 31,
        signature: [0; 64],
    };
    sign(&signing, &mut token);
    (signing, keys, state, expected, token)
}

fn sign(signing: &SigningKey, token: &mut ReservationAdjustmentToken) {
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
}

#[test]
fn valid_signature_verifies_without_mutating_state_and_explicit_apply_is_once() {
    let (_, keys, mut state, expected, token) = fixture();
    let before = state.clone();
    let verified = verify_amend_reservation(&keys, &state, &expected, &token).unwrap();
    assert_eq!(state, before);
    state.apply_verified_adjustment(&verified).unwrap();
    assert_eq!(state.version, 8);
    assert_eq!(state.reserves.as_slice(), token.target_reserves.as_slice());
    let after = state.clone();
    assert_eq!(
        state.apply_verified_adjustment(&verified),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(state, after);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn unknown_key_and_bad_signature_are_rejected() {
    let (signing, keys, state, expected, mut token) = fixture();
    token.risk_key_id = 32;
    sign(&signing, &mut token);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
    token.risk_key_id = 31;
    sign(&signing, &mut token);
    token.signature[0] ^= 1;
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
    sign(&signing, &mut token);
    token.target_reserves[0].atoms += 1;
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn key_set_has_version_and_rejects_duplicate_key_ids() {
    let (signing, keys, _, _, _) = fixture();
    assert_eq!(keys.version(), 4);
    assert!(matches!(
        RiskKeySet::new(
            4,
            [(31, signing.verifying_key()), (31, signing.verifying_key())]
        ),
        Err(RejectReason::InvalidReservation)
    ));
}

#[test]
fn each_identity_mismatch_and_stale_version_are_rejected() {
    let (signing, keys, state, expected, token) = fixture();
    let mut cases = Vec::new();
    let mut changed = token.clone();
    changed.command_id = CommandId(99);
    cases.push(changed);
    let mut changed = token.clone();
    changed.reservation_id = ReservationId(99);
    cases.push(changed);
    let mut changed = token.clone();
    changed.order_id = OrderId(99);
    cases.push(changed);
    let mut changed = token.clone();
    changed.user_id = UserId(99);
    cases.push(changed);
    let mut changed = token.clone();
    changed.market_id = MarketId(99);
    cases.push(changed);
    let mut changed = token.clone();
    changed.expected_reservation_version = 8;
    cases.push(changed);
    for mut changed in cases {
        sign(&signing, &mut changed);
        assert_eq!(
            verify_amend_reservation(&keys, &state, &expected, &changed),
            Err(RejectReason::InvalidReservation)
        );
    }
}

#[test]
fn each_active_state_identity_mismatch_is_rejected() {
    let (_, keys, state, expected, token) = fixture();
    let mut cases = Vec::new();
    let mut changed = state.clone();
    changed.reservation_id = ReservationId(99);
    cases.push(changed);
    let mut changed = state.clone();
    changed.order_id = OrderId(99);
    cases.push(changed);
    let mut changed = state.clone();
    changed.user_id = UserId(99);
    cases.push(changed);
    let mut changed = state;
    changed.market_id = MarketId(99);
    cases.push(changed);
    for changed in cases {
        assert_eq!(
            verify_amend_reservation(&keys, &changed, &expected, &token),
            Err(RejectReason::InvalidReservation)
        );
    }
}

#[test]
fn empty_required_reserves_cannot_bypass_coverage() {
    let (_, keys, state, mut expected, token) = fixture();
    expected.required_reserves.clear();
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
}

fn reject_noncanonical_required(required: Vec<AssetReserve>) {
    let (signing, keys, state, mut expected, mut token) = fixture();
    token.target_reserves.push(AssetReserve {
        asset: AssetId(2),
        atoms: 21,
    });
    sign(&signing, &mut token);
    // 对照先证明签名、身份和逐资产覆盖都有效，拒绝只能来自本次需求列表。
    assert!(verify_amend_reservation(&keys, &state, &expected, &token).is_ok());
    expected.required_reserves = required;
    let before = state.clone();
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &token),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(state, before);
}

#[test]
fn zero_required_reserve_is_rejected_by_signed_verifier() {
    reject_noncanonical_required(vec![AssetReserve {
        asset: AssetId(1),
        atoms: 0,
    }]);
}

#[test]
fn duplicate_required_asset_is_rejected_by_signed_verifier() {
    reject_noncanonical_required(vec![
        AssetReserve {
            asset: AssetId(1),
            atoms: 10,
        },
        AssetReserve {
            asset: AssetId(1),
            atoms: 11,
        },
    ]);
}

#[test]
fn out_of_order_required_assets_are_rejected_by_signed_verifier() {
    reject_noncanonical_required(vec![
        AssetReserve {
            asset: AssetId(2),
            atoms: 10,
        },
        AssetReserve {
            asset: AssetId(1),
            atoms: 11,
        },
    ]);
}

#[test]
fn fee_policy_and_insufficient_coverage_are_rejected() {
    let (signing, keys, state, expected, token) = fixture();
    let mut changed = token.clone();
    changed.fee_schedule_version = 10;
    sign(&signing, &mut changed);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &changed),
        Err(RejectReason::InvalidReservation)
    );
    changed = token.clone();
    changed.fee_tier_id = 11;
    sign(&signing, &mut changed);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &changed),
        Err(RejectReason::InvalidReservation)
    );
    changed = token;
    changed.target_reserves[0].atoms = 19;
    sign(&signing, &mut changed);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &changed),
        Err(RejectReason::InsufficientReservation)
    );
    changed.target_reserves[0].asset = AssetId(2);
    sign(&signing, &mut changed);
    assert_eq!(
        verify_amend_reservation(&keys, &state, &expected, &changed),
        Err(RejectReason::InsufficientReservation)
    );
}

#[test]
fn version_overflow_does_not_partially_apply() {
    let (signing, keys, mut state, expected, mut token) = fixture();
    state.version = u64::MAX;
    token.expected_reservation_version = u64::MAX;
    sign(&signing, &mut token);
    let verified = verify_amend_reservation(&keys, &state, &expected, &token).unwrap();
    let before = state.clone();
    assert_eq!(
        state.apply_verified_adjustment(&verified),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(state, before);
}
