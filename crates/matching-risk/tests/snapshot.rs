//! 预留状态快照的编码及校验。
use matching_domain::{
    AssetId, AssetReserve, MarketId, OrderId, OriginSessionId, ReservationId, UserId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
};
use matching_risk::{
    ReservationState, ReservationStatus, decode_reservation_snapshot, encode_reservation_snapshot,
};

#[test]
fn reservation_snapshot_roundtrip_preserves_full_state_and_rejects_corruption() {
    let state = ReservationState {
        reservation_id: ReservationId::new(1),
        order_id: OrderId::new(2),
        user_id: UserId::new(3),
        market_id: MarketId(4),
        origin_session_id: OriginSessionId(5),
        version: 6,
        reserves: vec![
            AssetReserve {
                asset: AssetId(3),
                atoms: 7,
            },
            AssetReserve {
                asset: AssetId(4),
                atoms: 8,
            },
        ],
        status: ReservationStatus::Active,
        fee_schedule_version: 9,
        fee_tier_id: 10,
    };
    let mut w = SnapshotEncoder::new(1024);
    encode_reservation_snapshot(&mut w, &state).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(decode_reservation_snapshot(&mut r).unwrap(), state);
    r.finish().unwrap();
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 1024).unwrap();
        assert!(decode_reservation_snapshot(&mut r).is_err());
    }
    let mut invalid = state.clone();
    invalid.reserves[1].asset = invalid.reserves[0].asset;
    let mut w = SnapshotEncoder::new(1024);
    w.u8(1).unwrap();
    assert!(encode_reservation_snapshot(&mut w, &invalid).is_err());
    assert_eq!(w.finish(), vec![1]);
}

#[test]
fn registry_snapshot_retains_released_and_pending_ownership() {
    use matching_risk::ReservationRegistry;
    let state = ReservationState {
        reservation_id: ReservationId::new(1),
        order_id: OrderId::new(2),
        user_id: UserId::new(3),
        market_id: MarketId(4),
        origin_session_id: OriginSessionId(5),
        version: 6,
        reserves: vec![AssetReserve {
            asset: AssetId(3),
            atoms: 7,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 9,
        fee_tier_id: 10,
    };
    let mut released = state.clone();
    released.reservation_id = ReservationId::new(10);
    released.status = ReservationStatus::Released;
    let registry =
        ReservationRegistry::new_with_pending_r2([state, released], [ReservationId::new(1)])
            .unwrap();
    let mut w = SnapshotEncoder::new(1024);
    registry.write_snapshot(&mut w).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 1024).unwrap();
    let restored = ReservationRegistry::read_snapshot(&mut r).unwrap();
    r.finish().unwrap();
    assert_eq!(registry, restored);
    assert!(restored.is_pending_r2(ReservationId::new(1)));
    assert_eq!(
        restored.get(ReservationId::new(10)).unwrap().status,
        ReservationStatus::Released
    );
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 1024).unwrap();
        assert!(ReservationRegistry::read_snapshot(&mut r).is_err());
    }
}

#[test]
fn snapshot_trusted_place_policy_and_keys_roundtrip() {
    use matching_domain::MarketMath;
    use matching_risk::{LimitReservePolicy, RiskKeySet};
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        9,
        2,
        15,
    )
    .unwrap();
    let key = ed25519_dalek::SigningKey::from_bytes(&[5; 32]).verifying_key();
    let keys = RiskKeySet::new(3, [(7, key)]).unwrap();
    let mut w = SnapshotEncoder::new(1000);
    policy.write_snapshot(&mut w).unwrap();
    keys.write_snapshot(&mut w).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 1000).unwrap();
    assert_eq!(LimitReservePolicy::read_snapshot(&mut r).unwrap(), policy);
    let decoded = RiskKeySet::read_snapshot(&mut r, 1).unwrap();
    r.finish().unwrap();
    let mut w = SnapshotEncoder::new(1000);
    policy.write_snapshot(&mut w).unwrap();
    decoded.write_snapshot(&mut w).unwrap();
    assert_eq!(w.finish(), bytes);
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 1000).unwrap();
        let ok = LimitReservePolicy::read_snapshot(&mut r)
            .and_then(|_| RiskKeySet::read_snapshot(&mut r, 1));
        assert!(ok.is_err());
    }
}

#[test]
fn released_empty_reservation_roundtrips_but_active_empty_is_rejected() {
    let mut state = ReservationState {
        reservation_id: ReservationId::new(80),
        order_id: OrderId::new(81),
        user_id: UserId::new(82),
        market_id: MarketId(83),
        origin_session_id: OriginSessionId(84),
        version: 0,
        reserves: vec![],
        status: ReservationStatus::Released,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let registry = matching_risk::ReservationRegistry::new([state.clone()]).unwrap();
    let mut writer = SnapshotEncoder::new(1024);
    registry.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(
        matching_risk::ReservationRegistry::read_snapshot(&mut reader).unwrap(),
        registry
    );
    reader.finish().unwrap();
    state.status = ReservationStatus::Active;
    assert!(matching_risk::ReservationRegistry::new([state.clone()]).is_err());
    let mut writer = SnapshotEncoder::new(1024);
    assert!(encode_reservation_snapshot(&mut writer, &state).is_err());
}
