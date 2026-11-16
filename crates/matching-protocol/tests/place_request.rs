use matching_protocol::{
    PlaceRequest, PlaceSide, StpPolicy, decode_place_request, encode_place_request,
};

#[test]
fn place_request_roundtrip_and_schema_validation() {
    let request = PlaceRequest {
        key: 5,
        market: 7,
        price: 10,
        qty: 2,
        stp_key: 3,
        user_control_epoch: 4,
        side: PlaceSide::Buy,
        stp: StpPolicy::CancelMaker,
        post_only: true,
        client_order_id: Some("hello".into()),
    };
    let bytes = encode_place_request(99, &request, 512, 32).unwrap();
    assert_eq!(bytes.as_slice(), include_bytes!("golden/place_v1.bin"));
    let (id, decoded) = decode_place_request(&bytes, 512, 32).unwrap();
    assert_eq!(id, 99);
    assert_eq!(decoded, request);
    assert!(decode_place_request(&bytes, 512, 4).is_err());
    let mut bad = request.clone();
    bad.qty = 0;
    assert!(encode_place_request(99, &bad, 512, 32).is_err());
}

#[test]
fn valid_crc_cannot_hide_invalid_schema_or_unknown_values() {
    use matching_protocol::{decode_frame, encode_frame};
    let request = PlaceRequest {
        key: 5,
        market: 7,
        price: 10,
        qty: 2,
        stp_key: 3,
        user_control_epoch: 4,
        side: PlaceSide::Buy,
        stp: StpPolicy::CancelMaker,
        post_only: false,
        client_order_id: Some("a".into()),
    };
    let bytes = encode_place_request(9, &request, 256, 32).unwrap();
    let frame = decode_frame(&bytes, 256).unwrap();
    for (offset, byte) in [(52, 2), (53, 4), (54, 2), (55, 2), (58, 0xff)] {
        let mut malformed = frame.clone();
        malformed.payload[offset] = byte;
        let bytes = encode_frame(&malformed, 256).unwrap();
        assert!(
            decode_place_request(&bytes, 256, 32).is_err(),
            "offset={offset}"
        );
    }
    let mut unknown = frame;
    unknown.message_type = 42;
    assert!(decode_place_request(&encode_frame(&unknown, 256).unwrap(), 256, 32).is_err());
}
