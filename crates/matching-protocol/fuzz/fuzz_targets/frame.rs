#![no_main]
use libfuzzer_sys::fuzz_target;
use matching_protocol::{Frame, decode_acceptance, decode_frame, decode_place_request, decode_rejection, encode_acceptance, encode_frame, encode_place_request, encode_rejection, frame_length};

fuzz_target!(|data: &[u8]| {
    let _ = frame_length(data, 4096);
    if let Ok(frame) = decode_frame(data, 4096) {
        assert_eq!(encode_frame(&frame, 4096).unwrap(), data);
    }
    if let Ok((id, request)) = decode_place_request(data, 4096, 256) {
        assert_eq!(encode_place_request(id, &request, 4096, 256).unwrap(), data);
    }
    if let Ok((id, reason)) = decode_rejection(data, 4096) {
        assert_eq!(encode_rejection(id, reason, 4096).unwrap(), data);
    }
    if let Ok((id, accepted)) = decode_acceptance(data, 4096) {
        assert_eq!(encode_acceptance(id, accepted, 4096).unwrap(), data);
    }
    if data.len() <= 4000 {
        let valid_frame = encode_frame(&Frame { message_type: 1, flags: 0, request_id: 42, payload: data.to_vec() }, 4096).unwrap();
        if let Ok((id, request)) = decode_place_request(&valid_frame, 4096, 256) {
            assert_eq!(encode_place_request(id, &request, 4096, 256).unwrap(), valid_frame);
        }
        let reason_frame = encode_frame(&Frame { message_type: 2, flags: 0, request_id: 43, payload: data.to_vec() }, 4096).unwrap();
        if let Ok((id, reason)) = decode_rejection(&reason_frame, 4096) {
            assert_eq!(encode_rejection(id, reason, 4096).unwrap(), reason_frame);
        }
        let acceptance_frame = encode_frame(&Frame { message_type: 3, flags: 0, request_id: 44, payload: data.to_vec() }, 4096).unwrap();
        if let Ok((id, accepted)) = decode_acceptance(&acceptance_frame, 4096) {
            assert_eq!(encode_acceptance(id, accepted, 4096).unwrap(), acceptance_frame);
        }
    }
});
