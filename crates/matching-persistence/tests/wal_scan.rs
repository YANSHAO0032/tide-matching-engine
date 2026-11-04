//! 验证扫描序号与损坏处理。
use matching_persistence::{ScanError, WalRecord, scan_segment};

fn record(seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch: 3,
        shard_seq: seq,
        engine_time: 5,
        command_id: u128::from(seq),
        payload: vec![1, 2, 3],
    }
}

#[test]
fn torn_tail_is_reported_only_for_last_segment() {
    let first = record(1).encode().unwrap();
    let second = record(2).encode().unwrap();
    let mut bytes = first.clone();
    bytes.extend_from_slice(&second[..second.len() - 1]);
    let result = scan_segment(&bytes, true).unwrap();
    assert_eq!(result.valid_len, first.len());
    assert!(result.torn_tail);
    assert_eq!(result.records, vec![record(1)]);
    assert!(scan_segment(&bytes, false).is_err());
}

#[test]
fn middle_corruption_and_sequence_gap_are_errors() {
    let mut bytes = record(1).encode().unwrap();
    bytes[30] ^= 1;
    bytes.extend(record(2).encode().unwrap());
    assert_eq!(scan_segment(&bytes, true).unwrap_err(), ScanError::Corrupt);
    let mut bytes = record(1).encode().unwrap();
    bytes.extend(record(3).encode().unwrap());
    assert_eq!(scan_segment(&bytes, true).unwrap_err(), ScanError::Sequence);
}

#[test]
fn corrupted_middle_length_never_hides_later_record() {
    let mut bytes = record(1).encode().unwrap();
    bytes[52] ^= 1;
    bytes.extend(record(2).encode().unwrap());
    assert_eq!(scan_segment(&bytes, true).unwrap_err(), ScanError::Corrupt);
}

#[test]
fn torn_record_with_valid_header_must_not_hide_sequence_gap() {
    let mut bytes = record(1).encode().unwrap();
    let bad = record(3).encode().unwrap();
    bytes.extend_from_slice(&bad[..bad.len() - 1]);
    assert_eq!(scan_segment(&bytes, true).unwrap_err(), ScanError::Sequence);
}

#[test]
fn fixed_seed_thousand_truncations_retain_exact_complete_prefix() {
    let frames: Vec<_> = (1..=12).map(|seq| record(seq).encode().unwrap()).collect();
    let bytes: Vec<_> = frames.iter().flatten().copied().collect();
    let mut seed = 0x73ab_9125_u32;
    for _ in 0..1000 {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let cut = (seed as usize) % (bytes.len() + 1);
        let mut valid = 0usize;
        let mut count = 0u64;
        for frame in &frames {
            if valid + frame.len() > cut {
                break;
            }
            valid += frame.len();
            count += 1;
        }
        let scanned = scan_segment(&bytes[..cut], true).unwrap();
        assert_eq!(scanned.valid_len, valid);
        assert_eq!(scanned.records, (1..=count).map(record).collect::<Vec<_>>());
        assert_eq!(scanned.torn_tail, cut != valid);
    }
}

#[test]
fn mutated_scan_input_never_panics() {
    let original = record(1).encode().unwrap();
    let mut seed = 0x562e_22b1_u32;
    for _ in 0..20_000 {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let mut bytes = original.clone();
        let at = seed as usize % bytes.len();
        bytes[at] ^= ((seed >> 8) as u8) | 1;
        if seed & 3 == 0 {
            bytes.truncate(seed as usize % bytes.len());
        }
        assert!(std::panic::catch_unwind(|| scan_segment(&bytes, true)).is_ok());
    }
}
