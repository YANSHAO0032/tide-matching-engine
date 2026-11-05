//! 验证 WAL 回收不会删除恢复仍需要的段。
use matching_persistence::{
    FileManifestStorage, ManifestIntent, ManifestManager, SegmentWriter, WalGcPolicy, WalRecord,
    run_wal_gc,
};
use std::time::Duration;

fn record(seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch: 1,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![1],
    }
}

#[test]
fn gc_keeps_fallback_replay_and_one_older_segment() {
    let root = std::env::temp_dir().join(format!("t23-gc-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert_eq!(
        run_wal_gc(&wal, &trash, &mut manager, &policy).unwrap(),
        vec![1, 2]
    );
    for seq in 3..=6 {
        assert!(wal.join(format!("{seq:020}.wal")).exists());
    }
    assert!(manager.state().gc_plan.is_empty());
    assert_eq!(manager.state().wal_gc_safe_seq, 2);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn interrupted_plan_finishes_after_rename_or_unlink() {
    let root = std::env::temp_dir().join(format!("t23-gc-resume-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1, 2],
            retained_first: 3,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    for seq in 3..=6 {
        std::fs::write(
            wal.join(format!("{seq:020}.wal")),
            record(seq).encode().unwrap(),
        )
        .unwrap();
    }
    std::fs::write(
        trash.join("00000000000000000001.wal"),
        record(1).encode().unwrap(),
    )
    .unwrap();
    drop(manager);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert_eq!(
        run_wal_gc(&wal, &trash, &mut manager, &policy).unwrap(),
        vec![1, 2]
    );
    assert!(!trash.join("00000000000000000001.wal").exists());
    assert!(manager.state().gc_plan.is_empty());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_obeys_retention_and_replication_floor() {
    let root = std::env::temp_dir().join(format!("t23-gc-bounds-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::ReplicationSafeSeqAdvanced { seq: 1 })
        .unwrap();
    let mut policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: Some(1),
        recovery_floor: 5,
        min_retention: Duration::from_secs(3600),
    };
    assert!(
        run_wal_gc(&wal, &trash, &mut manager, &policy)
            .unwrap()
            .is_empty()
    );
    policy.min_retention = Duration::ZERO;
    assert_eq!(
        run_wal_gc(&wal, &trash, &mut manager, &policy).unwrap(),
        vec![1]
    );
    assert!(wal.join("00000000000000000002.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_refuses_a_nonprefix_plan_before_deleting_any_file() {
    let root = std::env::temp_dir().join(format!("t23-gc-invalid-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![2],
            retained_first: 3,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(wal.join("00000000000000000002.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_rejects_snapshot_ahead_of_durable_wal() {
    let root = std::env::temp_dir().join(format!("t23-gc-snapshot-ahead-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 7,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 5,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 7,
        replication_safe_seq: None,
        recovery_floor: 7,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(wal.join("00000000000000000001.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_requires_strict_manifest_open_before_any_deletion() {
    let root = std::env::temp_dir().join(format!("t23-gc-strict-open-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        writer.append(&record(seq)).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(wal.join("00000000000000000001.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn interrupted_gc_rejects_missing_first_retained_segment() {
    let root = std::env::temp_dir().join(format!("t23-gc-missing-retained-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    for seq in 3..=6 {
        std::fs::write(
            wal.join(format!("{seq:020}.wal")),
            record(seq).encode().unwrap(),
        )
        .unwrap();
    }
    std::fs::write(
        trash.join("00000000000000000001.wal"),
        record(1).encode().unwrap(),
    )
    .unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1],
            retained_first: 2,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(trash.join("00000000000000000001.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_rejects_wal_belonging_to_another_shard_without_deleting() {
    let root = std::env::temp_dir().join(format!("t23-gc-wrong-shard-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    let mut writer = SegmentWriter::new(&wal, 65).unwrap();
    for seq in 1..=6 {
        let mut foreign = record(seq);
        foreign.shard_id = 9;
        writer.append(&foreign).unwrap();
    }
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(wal.join("00000000000000000001.wal").exists());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_resume_rejects_different_identity_from_persisted_plan() {
    let root = std::env::temp_dir().join(format!("t23-gc-plan-identity-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    for seq in 3..=6 {
        let mut foreign = record(seq);
        foreign.shard_id = 9;
        std::fs::write(
            wal.join(format!("{seq:020}.wal")),
            foreign.encode().unwrap(),
        )
        .unwrap();
    }
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1, 2],
            retained_first: 3,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    drop(manager);
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 9,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert_eq!(manager.state().gc_plan, vec![1, 2]);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_resume_rejects_foreign_trash_before_unlink() {
    let root = std::env::temp_dir().join(format!("t23-gc-foreign-trash-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let trash = root.join("trash");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&trash).unwrap();
    for seq in 3..=6 {
        std::fs::write(
            wal.join(format!("{seq:020}.wal")),
            record(seq).encode().unwrap(),
        )
        .unwrap();
    }
    let mut foreign = record(1);
    foreign.cluster_id = 9;
    let foreign_path = trash.join("00000000000000000001.wal");
    std::fs::write(&foreign_path, foreign.encode().unwrap()).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 6,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 2,
            snapshot_seq: 5,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::WalGcPlanned {
            segments: vec![1, 2],
            retained_first: 3,
            cluster_id: 1,
            shard_id: 2,
        })
        .unwrap();
    let policy = WalGcPolicy {
        cluster_id: 1,
        shard_id: 2,
        oldest_snapshot_seq: 4,
        latest_snapshot_id: 2,
        latest_snapshot_seq: 5,
        replication_safe_seq: None,
        recovery_floor: 5,
        min_retention: Duration::ZERO,
    };
    assert!(run_wal_gc(&wal, &trash, &mut manager, &policy).is_err());
    assert!(foreign_path.exists());
    assert_eq!(manager.state().gc_plan, vec![1, 2]);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}
