use matching_protocol::{Frame, FrameError, decode_frame, encode_frame};

#[test]
fn frame_round_trip_and_fixed_wire_layout() {
    let frame = Frame {
        message_type: 7,
        flags: 2,
        request_id: 0x0102_0304_0506_0708,
        payload: b"abc".to_vec(),
    };
    let bytes = encode_frame(&frame, 64).unwrap();
    assert_eq!(
        &bytes[..],
        &[
            0x54, 0x49, 0x44, 0x45, 0, 1, 0, 7, 0, 2, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 3, 0x61,
            0x62, 0x63, 0x49, 0x36, 0xd1, 0x9a
        ]
    );
    assert_eq!(bytes.as_slice(), include_bytes!("golden/frame_v1.bin"));
    assert_eq!(decode_frame(&bytes, 64).unwrap(), frame);
}

#[test]
fn rejects_corruption_truncation_and_oversize_before_payload_allocation() {
    let frame = Frame {
        message_type: 1,
        flags: 0,
        request_id: 3,
        payload: vec![9; 16],
    };
    let bytes = encode_frame(&frame, 128).unwrap();
    for i in 0..bytes.len() {
        assert!(decode_frame(&bytes[..i], 128).is_err());
    }
    let mut bad = bytes.clone();
    bad[22] ^= 1;
    assert_eq!(decode_frame(&bad, 128), Err(FrameError::Checksum));
    assert_eq!(decode_frame(&bytes, 25), Err(FrameError::TooLarge));
    let mut huge = bytes;
    huge[18..22].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(decode_frame(&huge, 128), Err(FrameError::TooLarge));
}

#[test]
fn deterministic_mutation_fuzz_never_accepts_changed_valid_frame() {
    let original = Frame {
        message_type: 42,
        flags: 3,
        request_id: 77,
        payload: (0..128).collect(),
    };
    let bytes = encode_frame(&original, 1024).unwrap();
    for position in 0..bytes.len() {
        for bit in 0..8 {
            let mut mutated = bytes.clone();
            mutated[position] ^= 1 << bit;
            assert!(
                decode_frame(&mutated, 1024).is_err(),
                "position={position} bit={bit}"
            );
        }
    }
    let mut state = 0x1234_5678_u32;
    for length in 0..1024 {
        let mut input = vec![0; length];
        for byte in &mut input {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *byte = state as u8;
        }
        if let Ok(frame) = decode_frame(&input, 1024) {
            assert_eq!(encode_frame(&frame, 1024).unwrap(), input);
        }
    }
}

#[test]
fn streaming_header_rejects_oversized_length_before_body_read() {
    use matching_protocol::frame_length;
    let frame = Frame {
        message_type: 1,
        flags: 0,
        request_id: 0,
        payload: vec![0; 50],
    };
    let bytes = encode_frame(&frame, 128).unwrap();
    assert_eq!(frame_length(&bytes[..22], 128), Ok(bytes.len()));
    assert_eq!(frame_length(&bytes[..22], 40), Err(FrameError::TooLarge));
    assert_eq!(frame_length(&bytes[..21], 128), Err(FrameError::Truncated));
}
