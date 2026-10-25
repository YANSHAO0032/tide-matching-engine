use matching_domain::{
    AssetId, AssetReserve, CommandId, MarketId, OrderId, RejectReason, ReservationAdjustmentToken,
    ReservationId, UserId,
};
use smallvec::smallvec;

fn token() -> ReservationAdjustmentToken {
    ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId(2),
        reservation_id: ReservationId(3),
        order_id: OrderId(4),
        user_id: UserId(5),
        market_id: MarketId(6),
        expected_reservation_version: 7,
        target_reserves: smallvec![AssetReserve {
            asset: AssetId(0x0102_0304),
            atoms: 8,
        }],
        fee_schedule_version: 9,
        fee_tier_id: 10,
        risk_key_id: 11,
        signature: [0x55; 64],
    }
}

#[test]
fn canonical_payload_has_fixed_order_and_big_endian_asset_id() {
    let token = token();
    let payload = token.canonical_signing_payload().unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&[0; 15]);
    expected.push(1);
    expected.extend_from_slice(&[0; 15]);
    expected.push(2);
    expected.extend_from_slice(&[0; 15]);
    expected.push(3);
    expected.extend_from_slice(&[0; 15]);
    expected.push(4);
    expected.extend_from_slice(&[0; 7]);
    expected.push(5);
    expected.extend_from_slice(&[0, 0, 0, 6]);
    expected.extend_from_slice(&[0; 7]);
    expected.push(7);
    expected.extend_from_slice(&[0, 0, 0, 1]);
    expected.extend_from_slice(&[1, 2, 3, 4]);
    expected.extend_from_slice(&[0; 15]);
    expected.push(8);
    expected.extend_from_slice(&[0; 7]);
    expected.push(9);
    expected.extend_from_slice(&[0, 0, 0, 10]);
    expected.extend_from_slice(&[0, 0, 0, 11]);
    assert_eq!(payload, expected);

    let mut changed_signature = token;
    changed_signature.signature = [0xaa; 64];
    assert_eq!(
        changed_signature.canonical_signing_payload().unwrap(),
        expected
    );
}

#[test]
fn canonical_payload_rejects_empty_zero_and_non_increasing_reserves() {
    let mut token = token();
    token.target_reserves.clear();
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );

    token.target_reserves.push(AssetReserve {
        asset: AssetId(1),
        atoms: 0,
    });
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );

    token.target_reserves[0].atoms = 1;
    token.target_reserves.push(AssetReserve {
        asset: AssetId(1),
        atoms: 2,
    });
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );

    token.target_reserves[1].asset = AssetId(0);
    assert_eq!(
        token.canonical_signing_payload(),
        Err(RejectReason::InvalidReservation)
    );
}

#[test]
fn canonical_payload_encodes_two_ordered_reserves_with_count_two() {
    let mut token = token();
    token.target_reserves.push(AssetReserve {
        asset: AssetId(0x0506_0708),
        atoms: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
    });
    let payload = token.canonical_signing_payload().unwrap();
    assert_eq!(&payload[84..88], &[0, 0, 0, 2]);
    assert_eq!(&payload[88..92], &[1, 2, 3, 4]);
    assert_eq!(&payload[108..112], &[5, 6, 7, 8]);
    assert_eq!(
        &payload[112..128],
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    );
    assert_eq!(payload.len(), 144);
}
