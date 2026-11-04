//! 验证记录的规范编码及损坏检测。
use matching_persistence::{WalError, WalRecord};

fn record() -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch: 3,
        shard_seq: 4,
        engine_time: 5,
        command_id: 6,
        payload: vec![1, 2, 3],
    }
}

#[test]
fn roundtrip_and_reject_corruption_and_truncation() {
    let record = record();
    let bytes = record.encode().unwrap();
    assert_eq!(WalRecord::decode(&bytes).unwrap(), (record, bytes.len()));
    for end in 0..bytes.len() {
        assert_eq!(
            WalRecord::decode(&bytes[..end]).unwrap_err(),
            WalError::Truncated
        );
    }
    let mut corrupted = bytes;
    corrupted[60] ^= 1;
    assert_eq!(
        WalRecord::decode(&corrupted).unwrap_err(),
        WalError::Checksum
    );
}

#[test]
fn arbitrary_input_does_not_panic() {
    let mut seed = 0x1234_5678_u32;
    for len in 0..1024 {
        let mut input = Vec::with_capacity(len);
        for _ in 0..len {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            input.push(seed as u8);
        }
        assert!(std::panic::catch_unwind(|| WalRecord::decode(&input)).is_ok());
    }
}

#[test]
fn header_integrity_is_checked_before_using_corrupted_payload_length() {
    let mut bytes = record().encode().unwrap();
    bytes[52] ^= 1;
    assert_eq!(
        WalRecord::decode(&bytes).unwrap_err(),
        WalError::HeaderChecksum
    );
}

#[test]
fn canonical_header_uses_big_endian_and_independent_checksum() {
    let bytes = record().encode().unwrap();
    assert_eq!(&bytes[0..4], b"WAL1");
    assert_eq!(&bytes[4..12], &[0, 1, 0, 1, 0, 2, 0, 0]);
    assert_eq!(&bytes[20..28], &4_u64.to_be_bytes());
    assert_eq!(&bytes[36..52], &6_u128.to_be_bytes());
    assert_eq!(&bytes[52..56], &3_u32.to_be_bytes());
    assert_eq!(
        &bytes[56..60],
        &matching_persistence::crc32c(&bytes[..56]).to_be_bytes()
    );
    assert_eq!(&bytes[60..63], &[1, 2, 3]);
}

#[test]
fn deterministic_mutation_fuzz_never_panics() {
    let original = record().encode().unwrap();
    let mut seed = 0x7139_82ad_u32;
    for _ in 0..20_000 {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let mut bytes = original.clone();
        let at = (seed as usize) % bytes.len();
        bytes[at] ^= ((seed >> 8) as u8) | 1;
        if seed & 3 == 0 {
            bytes.truncate((seed as usize) % bytes.len());
        }
        assert!(std::panic::catch_unwind(|| WalRecord::decode(&bytes)).is_ok());
    }
}

#[test]
fn unknown_flags_are_rejected() {
    let mut entry = record();
    entry.flags = 1;
    assert_eq!(entry.encode().unwrap_err(), WalError::InvalidHeader);
    let mut bytes = record().encode().unwrap();
    bytes[11] = 1;
    let header_crc = matching_persistence::crc32c(&bytes[..56]);
    bytes[56..60].copy_from_slice(&header_crc.to_be_bytes());
    assert_eq!(
        WalRecord::decode(&bytes).unwrap_err(),
        WalError::InvalidHeader
    );
}

#[test]
fn zero_shard_sequence_is_not_a_durable_mutation() {
    let mut entry = record();
    entry.shard_seq = 0;
    assert_eq!(entry.encode().unwrap_err(), WalError::InvalidHeader);
}
