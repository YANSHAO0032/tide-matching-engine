//! 验证文件轮转和跨 segment 顺序检查。
use matching_persistence::{SegmentWriter, WalRecord, scan_segments};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);

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
fn rotation_uses_first_seq_and_scan_preserves_order() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let mut writer = SegmentWriter::new(&path, 70).unwrap();
    writer.append(&record(1)).unwrap();
    writer.append(&record(2)).unwrap();
    drop(writer);
    let scan = scan_segments(&path).unwrap();
    assert_eq!(scan.records, vec![record(1), record(2)]);
    assert_eq!(scan.segments.len(), 2);
    assert_eq!(
        scan.segments[0].checksum,
        matching_persistence::crc32c(&record(1).encode().unwrap())
    );
    assert!(path.join("00000000000000000001.wal").exists());
    assert!(path.join("00000000000000000002.wal").exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn torn_tail_can_be_explicitly_truncated_to_complete_record() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let first = record(1).encode().unwrap();
    let next = record(2).encode().unwrap();
    let mut bytes = first.clone();
    bytes.extend_from_slice(&next[..next.len() - 1]);
    let file = path.join("00000000000000000001.wal");
    std::fs::write(&file, bytes).unwrap();
    assert!(matching_persistence::truncate_torn_tail(&path).unwrap());
    assert_eq!(std::fs::read(&file).unwrap(), first);
    assert!(!matching_persistence::truncate_torn_tail(&path).unwrap());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn nonfinal_torn_and_cross_segment_sequence_gap_fail_closed() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let first = path.join("00000000000000000001.wal");
    let later = path.join("00000000000000000003.wal");
    std::fs::write(&first, record(1).encode().unwrap()).unwrap();
    std::fs::write(&later, record(3).encode().unwrap()).unwrap();
    assert!(scan_segments(&path).is_err());
    let mut damaged = record(1).encode().unwrap();
    damaged.pop();
    std::fs::write(&first, damaged).unwrap();
    assert!(scan_segments(&path).is_err());
    assert!(matching_persistence::truncate_torn_tail(&path).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn torn_first_record_header_must_match_segment_name_and_previous_shard() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::write(
        path.join("00000000000000000001.wal"),
        record(1).encode().unwrap(),
    )
    .unwrap();
    let second = record(3).encode().unwrap();
    std::fs::write(
        path.join("00000000000000000002.wal"),
        &second[..second.len() - 1],
    )
    .unwrap();
    assert!(scan_segments(&path).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn thousand_physical_truncations_keep_only_complete_records() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let file = path.join("00000000000000000001.wal");
    let frames: Vec<_> = (1..=12).map(|seq| record(seq).encode().unwrap()).collect();
    let all: Vec<_> = frames.iter().flatten().copied().collect();
    let mut seed = 0x49a1_72d3_u32;
    for _ in 0..1000 {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let cut = (seed as usize) % (all.len() + 1);
        std::fs::write(&file, &all).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap()
            .set_len(cut as u64)
            .unwrap();
        let mut valid = 0usize;
        let mut count = 0u64;
        for frame in &frames {
            if valid + frame.len() > cut {
                break;
            }
            valid += frame.len();
            count += 1;
        }
        let scan = scan_segments(&path).unwrap();
        assert_eq!(scan.records, (1..=count).map(record).collect::<Vec<_>>());
        assert_eq!(scan.segments[0].valid_len, valid as u64);
        assert_eq!(
            matching_persistence::truncate_torn_tail(&path).unwrap(),
            valid != cut
        );
        assert_eq!(std::fs::metadata(&file).unwrap().len(), valid as u64);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unexpected_segment_name_is_never_silently_skipped() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::write(
        path.join("00000000000000000001.WAL"),
        record(1).encode().unwrap(),
    )
    .unwrap();
    assert!(scan_segments(&path).is_err());
    assert!(SegmentWriter::new(&path, 70).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn writer_propagates_rotation_failure_and_stops() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let mut writer = SegmentWriter::new(&path, 70).unwrap();
    writer.append(&record(1)).unwrap();
    // 创建同名目标使下一次轮转失败，必须返回文件错误。
    std::fs::write(path.join("00000000000000000002.wal"), []).unwrap();
    assert_eq!(
        writer.append(&record(2)).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    std::fs::remove_file(path.join("00000000000000000002.wal")).unwrap();
    assert!(writer.append(&record(2)).is_err());
    assert!(!path.join("00000000000000000002.wal").exists());
    drop(writer);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sequence_corruption_permanently_stops_low_level_writer() {
    let path = std::env::temp_dir().join(format!(
        "matching-wal-seq-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let mut writer = SegmentWriter::new(&path, 1024).unwrap();
    writer.append(&record(1)).unwrap();
    assert!(writer.append(&record(3)).is_err());
    assert!(writer.append(&record(2)).is_err());
    drop(writer);
    assert_eq!(scan_segments(&path).unwrap().records, vec![record(1)]);
    std::fs::remove_dir_all(path).unwrap();
}
