//! 验证单写者状态推进及持久化失败的关闭行为。
use matching_persistence::{ManifestIntent, ManifestManager, ManifestState, ManifestStorage};
use std::io;

#[derive(Default)]
struct Store {
    /// 实际发生的持久步骤。
    calls: Vec<&'static str>,
    /// 指定失败的步骤序号。
    fail_at: usize,
}
impl Store {
    fn stage(&mut self, name: &'static str) -> io::Result<()> {
        self.calls.push(name);
        if self.calls.len() == self.fail_at {
            return Err(io::Error::other("故障注入"));
        }
        Ok(())
    }
}
impl ManifestStorage for Store {
    fn write_inactive_tmp(&mut self, _: u64, _: &ManifestState) -> io::Result<()> {
        self.stage("write")
    }
    fn sync_tmp(&mut self) -> io::Result<()> {
        self.stage("sync")
    }
    fn rename_inactive_slot(&mut self) -> io::Result<()> {
        self.stage("rename")
    }
    fn sync_directory(&mut self) -> io::Result<()> {
        self.stage("directory")
    }
}

#[test]
fn publishes_only_after_each_persistence_step_succeeds() {
    for failure in 1..=4 {
        let mut manager = ManifestManager::new(
            Store {
                fail_at: failure,
                ..Store::default()
            },
            ManifestState::default(),
        );
        assert!(
            manager
                .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 4 })
                .is_err()
        );
        assert_eq!(manager.state().generation, 0);
        assert!(
            manager
                .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 4 })
                .is_err()
        );
    }
    let mut manager = ManifestManager::new(Store::default(), ManifestState::default());
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 4 })
        .unwrap();
    assert_eq!(manager.state().generation, 1);
    assert_eq!(manager.state().replication_safe_seq, 4);
    assert_eq!(
        manager.storage().calls,
        ["write", "sync", "rename", "directory"]
    );
}

#[test]
fn intents_preserve_unrelated_fields_and_reject_rollback() {
    let mut manager = ManifestManager::new(Store::default(), ManifestState::default());
    manager
        .submit(ManifestIntent::SegmentRolled { first_seq: 10 })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 3,
            snapshot_seq: 9,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 8 })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1, 4],
            retained_first: 10,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    assert_eq!(manager.state().generation, 4);
    assert_eq!(manager.state().active_segment, Some(10));
    assert_eq!(manager.state().latest_snapshot, Some((3, 9)));
    assert_eq!(manager.state().replication_safe_seq, 8);
    assert_eq!(manager.state().gc_plan, vec![1, 4]);
    assert!(
        manager
            .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 7 })
            .is_err()
    );
    assert_eq!(manager.state().generation, 4);
    manager
        .submit(ManifestIntent::WalGcCompleted {
            segments: vec![1, 4],
            through_seq: 9,
        })
        .unwrap();
    assert!(manager.state().gc_plan.is_empty());
}

#[test]
fn disk_slots_recover_latest_and_reject_second_writer() {
    use matching_persistence::FileManifestStorage;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "manifest-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open(&dir).is_err());
    manager
        .submit(ManifestIntent::SegmentRolled { first_seq: 1 })
        .unwrap();
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 7 })
        .unwrap();
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 8 })
        .unwrap();
    let expected = manager.state().clone();
    drop(manager);
    let recovered = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert_eq!(recovered.state(), &expected);
    drop(recovered);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn corrupted_newest_slot_uses_older_valid_generation() {
    use matching_persistence::FileManifestStorage;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "manifest-corrupt-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::SegmentRolled { first_seq: 1 })
        .unwrap();
    let previous = manager.state().clone();
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 7 })
        .unwrap();
    drop(manager);
    std::fs::write(dir.join("manifest-b"), b"corrupt").unwrap();
    let recovered = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert_eq!(recovered.state(), &previous);
    drop(recovered);
    std::fs::write(dir.join("manifest-a"), b"corrupt").unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open(&dir).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn disk_roundtrip_preserves_snapshot_and_inflight_gc_plan() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("manifest-gc-state-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::SegmentRolled { first_seq: 10 })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 44,
            snapshot_seq: 9,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1, 4],
            retained_first: 10,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    let expected = manager.state().clone();
    drop(manager);
    let mut recovered = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert_eq!(recovered.state(), &expected);
    recovered
        .submit(ManifestIntent::WalGcCompleted {
            segments: vec![1, 4],
            through_seq: 9,
        })
        .unwrap();
    let expected = recovered.state().clone();
    drop(recovered);
    let reopened = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert_eq!(reopened.state(), &expected);
    assert!(reopened.state().gc_plan.is_empty());
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_open_rejects_corrupt_slot_instead_of_rolling_back_acknowledged_watermark() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t23-strict-manifest-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&dir).unwrap();
    assert_eq!(manager.state().local_durable_seq, 2);
    drop(manager);
    std::fs::write(dir.join("manifest-b"), b"corrupt").unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open_recovery(&dir).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn strict_recovery_rejects_a_mixed_legacy_slot_pair() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t23-legacy-manifest-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    let path = dir.join("manifest-a");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&1u16.to_be_bytes());
    let len = bytes.len();
    bytes.truncate(len - 41);
    bytes.extend_from_slice(&matching_persistence::crc32c(&bytes).to_be_bytes());
    std::fs::write(path, bytes).unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open_recovery(&dir).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn legacy_manifest_cannot_be_upgraded_by_unrelated_intents() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t23-no-legacy-upgrade-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::SegmentRolled { first_seq: 1 })
        .unwrap();
    drop(manager);
    let path = dir.join("manifest-a");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&1u16.to_be_bytes());
    let len = bytes.len();
    bytes.truncate(len - 41);
    bytes.extend_from_slice(&matching_persistence::crc32c(&bytes).to_be_bytes());
    std::fs::write(path, bytes).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert!(
        manager
            .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 1 })
            .is_err()
    );
    drop(manager);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn fallback_manifest_cannot_advance_after_slot_corruption() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t23-no-corrupt-upgrade-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    std::fs::write(dir.join("manifest-b"), b"corrupt").unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert!(
        manager
            .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 1 })
            .is_err()
    );
    drop(manager);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn missing_slot_cannot_be_hidden_by_followup_writes() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t23-no-missing-upgrade-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    std::fs::remove_file(dir.join("manifest-b")).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    assert!(
        manager
            .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 1 })
            .is_err()
    );
    drop(manager);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn version_two_slot_migrates_without_losing_durable_watermark() {
    use matching_persistence::FileManifestStorage;
    let dir = std::env::temp_dir().join(format!("t24-v2-migration-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&dir).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 2,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    for name in ["manifest-a", "manifest-b"] {
        let path = dir.join(name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4..6].copy_from_slice(&2_u16.to_be_bytes());
        let len = bytes.len();
        bytes.truncate(len - 21);
        let checksum = matching_persistence::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        std::fs::write(path, bytes).unwrap();
    }
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&dir).unwrap();
    assert_eq!(manager.state().local_durable_seq, 2);
    assert_eq!(manager.state().previous_snapshot, None);
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 3,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&dir).unwrap();
    assert_eq!(manager.state().local_durable_seq, 3);
    drop(manager);
    std::fs::remove_dir_all(dir).unwrap();
}
