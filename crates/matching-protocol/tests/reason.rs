use matching_protocol::{ReasonCode, decode_rejection, encode_rejection};

#[test]
fn reason_numbers_roundtrip_and_unknown_values_are_preserved() {
    let bytes = encode_rejection(42, ReasonCode::ENGINE_BUSY, 64).unwrap();
    assert_eq!(
        decode_rejection(&bytes, 64).unwrap(),
        (42, ReasonCode::ENGINE_BUSY)
    );
    let bytes = encode_rejection(7, ReasonCode::new(65500), 64).unwrap();
    assert_eq!(
        decode_rejection(&bytes, 64).unwrap(),
        (7, ReasonCode::new(65500))
    );
    assert_eq!(ReasonCode::ENGINE_BUSY.get(), 5002);
}

#[test]
fn acceptance_returns_command_order_and_sequence() {
    use matching_protocol::{Acceptance, decode_acceptance, encode_acceptance};
    let answer = Acceptance {
        command_id: 1,
        order_id: 2,
        shard_seq: 3,
    };
    let bytes = encode_acceptance(7, answer, 128).unwrap();
    assert_eq!(bytes.as_slice(), include_bytes!("golden/acceptance_v1.bin"));
    assert_eq!(decode_acceptance(&bytes, 128).unwrap(), (7, answer));
}
