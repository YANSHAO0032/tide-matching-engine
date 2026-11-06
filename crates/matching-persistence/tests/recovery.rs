//! 验证可信提交水位限制恢复范围。
use matching_persistence::{SegmentWriter, WalRecord, recover_committed};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn record(seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch: 3,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![1],
    }
}
#[test]
fn recovery_limits_records_to_trusted_prefix_and_rejects_missing_committed_data() {
    let dir = std::env::temp_dir().join(format!(
        "recovery-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let mut writer = SegmentWriter::new(&dir, 1024).unwrap();
    for seq in 1..=3 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    assert_eq!(
        recover_committed(&dir, 0, 2, 1, 2).unwrap(),
        vec![record(1), record(2)]
    );
    assert!(recover_committed(&dir, 0, 4, 1, 2).is_err());
    assert!(recover_committed(&dir, 0, 2, 9, 2).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn random_truncation_never_returns_a_missing_committed_record() {
    let dir = std::env::temp_dir().join(format!(
        "recovery-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let file = dir.join("00000000000000000001.wal");
    let records: Vec<_> = (1..=10).map(record).collect();
    let bytes: Vec<_> = records.iter().flat_map(|r| r.encode().unwrap()).collect();
    let record_len = records[0].encode().unwrap().len();
    let mut seed = 73_u64;
    for _ in 0..1000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let len = (seed as usize) % (bytes.len() + 1);
        std::fs::write(&file, &bytes[..len]).unwrap();
        let complete = len / record_len;
        let recovered = recover_committed(&dir, 0, complete as u64, 1, 2).unwrap();
        assert_eq!(recovered, records[..complete]);
        if complete < records.len() {
            assert!(recover_committed(&dir, 0, complete as u64 + 1, 1, 2).is_err());
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_rejects_middle_corruption_and_segment_sequence_gap() {
    let dir = std::env::temp_dir().join(format!(
        "recovery-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let first = dir.join("00000000000000000001.wal");
    let second = dir.join("00000000000000000003.wal");
    std::fs::write(&first, record(1).encode().unwrap()).unwrap();
    std::fs::write(&second, record(3).encode().unwrap()).unwrap();
    assert!(recover_committed(&dir, 0, 1, 1, 2).is_err());
    std::fs::remove_file(&second).unwrap();
    let mut bytes = record(1).encode().unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    bytes.extend(record(2).encode().unwrap());
    std::fs::write(&first, bytes).unwrap();
    assert!(recover_committed(&dir, 0, 1, 1, 2).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_requires_exact_sequence_after_snapshot_and_rejects_torn_committed_tail() {
    let dir = std::env::temp_dir().join(format!(
        "recovery-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let file = dir.join("00000000000000000002.wal");
    let mut bytes = record(2).encode().unwrap();
    let tail = record(3).encode().unwrap();
    bytes.extend_from_slice(&tail[..tail.len() - 1]);
    std::fs::write(&file, bytes).unwrap();
    assert_eq!(
        recover_committed(&dir, 1, 2, 1, 2).unwrap(),
        vec![record(2)]
    );
    assert!(recover_committed(&dir, 1, 3, 1, 2).is_err());
    assert!(recover_committed(&dir, 0, 2, 1, 2).is_err());
    assert!(recover_committed(&dir, 2, 1, 1, 2).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn manifest_driven_recovery_excludes_unsynced_tail_and_requires_committed_data() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, recover_local,
    };
    use std::time::Instant;
    let root = std::env::temp_dir().join(format!("t23-local-recover-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = RegisteredAppender::new(SegmentWriter::new(&wal, 1024).unwrap(), manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 10,
            max_delay_micros: 1000000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batch = DurableBatcher::new_with_config(writer, config, 3).unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    batch.flush().unwrap();
    batch.append_at(&record(2), Instant::now()).unwrap();
    drop(batch);
    assert_eq!(
        recover_local(&wal, &manifest, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    let file = wal.join("00000000000000000001.wal");
    let mut bytes = std::fs::read(&file).unwrap();
    bytes.truncate(record(1).encode().unwrap().len() - 1);
    std::fs::write(file, bytes).unwrap();
    assert!(recover_local(&wal, &manifest, 0, 1, 2).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn random_truncation_with_durable_manifest_never_loses_an_acknowledged_prefix() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, recover_local,
    };
    let root = std::env::temp_dir().join(format!(
        "t23-truncation-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 5,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    drop(manager);
    let records: Vec<_> = (1..=10).map(record).collect();
    let bytes: Vec<_> = records.iter().flat_map(|r| r.encode().unwrap()).collect();
    let committed_bytes = records[..5]
        .iter()
        .map(|r| r.encode().unwrap().len())
        .sum::<usize>();
    let file = wal.join("00000000000000000001.wal");
    let mut seed = 397_u64;
    for _ in 0..1000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let len = (seed as usize) % (bytes.len() + 1);
        std::fs::write(&file, &bytes[..len]).unwrap();
        let result = recover_local(&wal, &manifest, 0, 1, 2);
        if len < committed_bytes {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap(), records[..5]);
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_fails_closed_when_manifest_is_missing_but_wal_exists() {
    use matching_persistence::recover_local;
    let root = std::env::temp_dir().join(format!(
        "t23-missing-manifest-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::write(
        wal.join("00000000000000000001.wal"),
        record(1).encode().unwrap(),
    )
    .unwrap();
    assert!(recover_local(&wal, &manifest, 0, 1, 2).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn repair_removes_complete_uncommitted_tail_and_syncs_the_retained_prefix() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, repair_local_wal,
    };
    use std::time::Instant;
    let root = std::env::temp_dir().join(format!(
        "t23-repair-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = RegisteredAppender::new(SegmentWriter::new(&wal, 1024).unwrap(), manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 10,
            max_delay_micros: 1000000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batch = DurableBatcher::new_with_config(writer, config, 3).unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    batch.flush().unwrap();
    batch.append_at(&record(2), Instant::now()).unwrap();
    drop(batch);
    assert_eq!(
        repair_local_wal(&wal, &manifest, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    assert_eq!(
        std::fs::read(wal.join("00000000000000000001.wal")).unwrap(),
        record(1).encode().unwrap()
    );
    assert_eq!(
        repair_local_wal(&wal, &manifest, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn repair_removes_uncommitted_rotated_segment_without_changing_committed_segment() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, repair_local_wal,
    };
    use std::time::Instant;
    let root = std::env::temp_dir().join(format!(
        "t23-repair-rotated-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = RegisteredAppender::new(SegmentWriter::new(&wal, 65).unwrap(), manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 10,
            max_delay_micros: 1000000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batch = DurableBatcher::new_with_config(writer, config, 3).unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    batch.flush().unwrap();
    batch.append_at(&record(2), Instant::now()).unwrap();
    drop(batch);
    assert_eq!(
        repair_local_wal(&wal, &manifest, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    assert!(!wal.join("00000000000000000002.wal").exists());
    assert_eq!(
        std::fs::read(wal.join("00000000000000000001.wal")).unwrap(),
        record(1).encode().unwrap()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn repair_rejects_damage_to_committed_bytes_without_mutation() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, repair_local_wal,
    };
    let root = std::env::temp_dir().join(format!(
        "t23-repair-damage-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    drop(manager);
    let file = wal.join("00000000000000000001.wal");
    let mut bytes = record(1).encode().unwrap();
    bytes.pop();
    std::fs::write(&file, &bytes).unwrap();
    assert!(repair_local_wal(&wal, &manifest, 0, 1, 2).is_err());
    assert_eq!(std::fs::read(&file).unwrap(), bytes);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn first_unflushed_record_recovers_to_empty_committed_prefix() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, recover_local, repair_local_wal,
    };
    use std::time::Instant;
    let root = std::env::temp_dir().join(format!(
        "t23-first-unflushed-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = RegisteredAppender::new(SegmentWriter::new(&wal, 1024).unwrap(), manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 10,
            max_delay_micros: 1000000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batch = DurableBatcher::new_with_config(writer, config, 3).unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    drop(batch);
    assert!(recover_local(&wal, &manifest, 0, 1, 2).unwrap().is_empty());
    assert!(
        repair_local_wal(&wal, &manifest, 0, 1, 2)
            .unwrap()
            .is_empty()
    );
    assert_eq!(std::fs::read_dir(&wal).unwrap().count(), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_rejects_manifest_active_segment_that_does_not_contain_watermark() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, recover_local,
    };
    let root = std::env::temp_dir().join(format!(
        "t23-active-mismatch-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut writer = SegmentWriter::new(&wal, 1024).unwrap();
    writer.append(&record(1)).unwrap();
    writer.append(&record(2)).unwrap();
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 2,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    drop(manager);
    assert!(recover_local(&wal, &manifest, 0, 1, 2).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_and_repair_accept_verified_previous_snapshot_and_reject_other_boundaries() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, recover_local, repair_local_wal,
    };
    let root = std::env::temp_dir().join(format!(
        "t25-fallback-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut writer = SegmentWriter::new(&wal, 4096).unwrap();
    for n in 1..=5 {
        writer.append(&record(n)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 5,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 11,
            snapshot_seq: 2,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 12,
            snapshot_seq: 4,
        })
        .unwrap();
    drop(manager);
    assert_eq!(
        recover_local(&wal, &manifest, 2, 1, 2).unwrap(),
        vec![record(3), record(4), record(5)]
    );
    assert_eq!(
        repair_local_wal(&wal, &manifest, 2, 1, 2).unwrap(),
        vec![record(3), record(4), record(5)]
    );
    assert!(recover_local(&wal, &manifest, 3, 1, 2).is_err());
    assert!(repair_local_wal(&wal, &manifest, 3, 1, 2).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_uses_existing_strict_manifest_lock_without_reopening() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, recover_local_with_manifest,
    };
    let root = std::env::temp_dir().join(format!(
        "t25-held-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut writer = SegmentWriter::new(&wal, 4096).unwrap();
    writer.append(&record(1)).unwrap();
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    assert!(recover_local_with_manifest(&wal, &manager, 0, 1, 2).is_err());
    drop(manager);
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open_recovery(&manifest).is_err());
    assert_eq!(
        recover_local_with_manifest(&wal, &manager, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tail_repair_uses_existing_strict_manifest_lock() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, repair_local_wal_with_manifest,
    };
    let root = std::env::temp_dir().join(format!(
        "t25-repair-held-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut writer = SegmentWriter::new(&wal, 4096).unwrap();
    writer.append(&record(1)).unwrap();
    writer.append(&record(2)).unwrap();
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    assert!(repair_local_wal_with_manifest(&wal, &manager, 0, 1, 2).is_err());
    drop(manager);
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    assert_eq!(
        repair_local_wal_with_manifest(&wal, &manager, 0, 1, 2).unwrap(),
        vec![record(1)]
    );
    assert_eq!(
        std::fs::read(wal.join("00000000000000000001.wal")).unwrap(),
        record(1).encode().unwrap()
    );
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovered_appender_repairs_tail_and_continues_from_committed_watermark() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode,
    };
    let root = std::env::temp_dir().join(format!(
        "t25-resume-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir_all(&wal).unwrap();
    std::fs::create_dir_all(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = SegmentWriter::new(&wal, 4096).unwrap();
    let appender = RegisteredAppender::new(writer, manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    batcher
        .append_at(&record(1), std::time::Instant::now())
        .unwrap();
    drop(batcher);
    use std::io::Write;
    let path = wal.join("00000000000000000001.wal");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&record(2).encode().unwrap())
        .unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let appender = RegisteredAppender::recover_local(&wal, 4096, manager, 0, 1, 2).unwrap();
    let mut batcher = DurableBatcher::resume_local(appender, config, 3)
        .unwrap()
        .into_boxed();
    assert_eq!(batcher.committed(), 1);
    batcher
        .append_at(&record(2), std::time::Instant::now())
        .unwrap();
    assert_eq!(batcher.committed(), 2);
    drop(batcher);
    assert_eq!(
        matching_persistence::recover_local(&wal, &manifest, 0, 1, 2).unwrap(),
        vec![record(1), record(2)]
    );
    std::fs::remove_dir_all(root).unwrap();
}
