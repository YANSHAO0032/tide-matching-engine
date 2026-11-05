//! 验证批量同步与连续持久前缀边界。
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableAppender, DurableBatcher, Profile, ReplicationMode,
    WalRecord,
};
use std::io;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Appender {
    /// 已调用的追加次数。
    writes: usize,
    /// 已调用的同步次数。
    syncs: usize,
    /// 是否注入同步故障。
    fail_sync: bool,
}
impl DurableAppender for Appender {
    fn would_rotate(&self, _: &WalRecord) -> io::Result<bool> {
        Ok(false)
    }
    fn append(&mut self, _: &WalRecord) -> io::Result<()> {
        self.writes += 1;
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        self.syncs += 1;
        if self.fail_sync {
            Err(io::Error::other("同步失败"))
        } else {
            Ok(())
        }
    }
}
fn config(mode: DurabilityMode, replication: ReplicationMode) -> DurabilityConfig {
    DurabilityConfig {
        profile: Profile::Development,
        mode,
        replication,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    }
}
fn record(seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 1,
        flags: 0,
        epoch: 1,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![1],
    }
}

#[test]
fn group_commit_and_standby_ack_only_advance_contiguous_prefix() {
    let mut batch = DurableBatcher::new_with_config(
        Appender::default(),
        config(
            DurabilityMode::GroupCommit {
                max_batch: 2,
                max_delay_micros: 100,
            },
            ReplicationMode::StandbyDurable,
        ),
        1,
    )
    .unwrap();
    let now = Instant::now();
    assert!(batch.append_at(&record(1), now).is_err());
    batch.set_standby_available(true);
    batch.append_at(&record(1), now).unwrap();
    assert_eq!(batch.committed(), 0);
    batch.append_at(&record(2), now).unwrap();
    assert_eq!(batch.local_durable(), 2);
    assert_eq!(batch.committed(), 0);
    assert!(batch.ack_standby_epoch(1, 2, 2).is_err());
    batch.ack_standby_epoch(1, 1, 1).unwrap();
    assert_eq!(batch.committed(), 1);
    batch.ack_standby_epoch(1, 2, 2).unwrap();
    assert_eq!(batch.committed(), 2);
    batch.set_standby_available(false);
    assert!(batch.append_at(&record(3), now).is_err());
    assert_eq!(batch.committed(), 2);
    batch.set_standby_available(true);
    batch.append_at(&record(3), now).unwrap();
    batch.flush_due(now + Duration::from_micros(100)).unwrap();
    assert_eq!(batch.local_durable(), 3);
    assert_eq!(batch.committed(), 2);
}

#[test]
fn sync_failure_halts_future_appends_without_advancing_commit() {
    let mut batch = DurableBatcher::new_with_config(
        Appender {
            fail_sync: true,
            ..Appender::default()
        },
        config(DurabilityMode::FsyncEach, ReplicationMode::LocalDurable),
        1,
    )
    .unwrap();
    assert!(batch.append_at(&record(1), Instant::now()).is_err());
    assert_eq!(batch.committed(), 0);
    assert!(batch.append_at(&record(2), Instant::now()).is_err());
}

#[test]
fn stale_primary_epoch_cannot_advance_standby_prefix() {
    let mut batch = DurableBatcher::new_with_config(
        Appender::default(),
        config(DurabilityMode::FsyncEach, ReplicationMode::StandbyDurable),
        1,
    )
    .unwrap();
    batch.set_standby_available(true);
    let mut stale = record(1);
    stale.epoch = 2;
    assert!(batch.append_at(&stale, Instant::now()).is_err());
    assert_eq!(batch.local_durable(), 0);
    batch.append_at(&record(1), Instant::now()).unwrap();
    assert!(batch.ack_standby_epoch(2, 1, 1).is_err());
    assert_eq!(batch.committed(), 0);
    batch.ack_standby_epoch(1, 1, 1).unwrap();
    assert_eq!(batch.committed(), 1);
}

#[test]
fn expired_batch_is_synced_before_accepting_later_record() {
    let mut batch = DurableBatcher::new_with_config(
        Appender::default(),
        config(
            DurabilityMode::GroupCommit {
                max_batch: 10,
                max_delay_micros: 100,
            },
            ReplicationMode::LocalDurable,
        ),
        1,
    )
    .unwrap();
    let start = Instant::now();
    batch.append_at(&record(1), start).unwrap();
    batch
        .append_at(&record(2), start + Duration::from_micros(101))
        .unwrap();
    assert_eq!(batch.local_durable(), 1);
    assert_eq!(batch.committed(), 1);
    batch.flush().unwrap();
    assert_eq!(batch.committed(), 2);
}

#[test]
fn batch_timer_rejects_backwards_instant_without_committing() {
    let mut batch = DurableBatcher::new_with_config(
        Appender::default(),
        config(
            DurabilityMode::GroupCommit {
                max_batch: 10,
                max_delay_micros: 100,
            },
            ReplicationMode::LocalDurable,
        ),
        1,
    )
    .unwrap();
    let start = Instant::now();
    batch.append_at(&record(1), start).unwrap();
    assert!(batch.flush_due(start - Duration::from_micros(1)).is_err());
    assert_eq!(batch.committed(), 0);
    batch.flush().unwrap();
    assert_eq!(batch.committed(), 1);
}

#[test]
fn real_segment_rotation_syncs_previous_batch_before_opening_new_segment() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter, scan_segments,
    };
    let root = std::env::temp_dir().join(format!("t22-batch-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("segments");
    let manifest = root.join("manifest");
    std::fs::create_dir(&path).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let writer = SegmentWriter::new(&path, 70).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let writer = RegisteredAppender::new(writer, manager).unwrap();
    let mut batch = DurableBatcher::new_with_config(
        writer,
        config(
            DurabilityMode::GroupCommit {
                max_batch: 10,
                max_delay_micros: 100,
            },
            ReplicationMode::LocalDurable,
        ),
        1,
    )
    .unwrap();
    let now = Instant::now();
    batch.append_at(&record(1), now).unwrap();
    assert_eq!(batch.committed(), 0);
    batch.append_at(&record(2), now).unwrap();
    assert_eq!(batch.committed(), 1);
    batch.flush().unwrap();
    assert_eq!(batch.committed(), 2);
    drop(batch);
    assert_eq!(
        scan_segments(&path).unwrap().records,
        vec![record(1), record(2)]
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn durable_segment_is_registered_before_commit_watermark_advances() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    let root = std::env::temp_dir().join(format!("t22-registered-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let segments = root.join("segments");
    let manifest = root.join("manifest");
    std::fs::create_dir(&segments).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(&segments, 70).unwrap(), manager).unwrap();
    let mut batch = DurableBatcher::new_with_config(
        appender,
        config(DurabilityMode::FsyncEach, ReplicationMode::LocalDurable),
        1,
    )
    .unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    assert_eq!(batch.committed(), 1);
    assert_eq!(batch.appender().manifest_state().active_segment, Some(1));
    batch.append_at(&record(2), Instant::now()).unwrap();
    assert_eq!(batch.committed(), 2);
    assert_eq!(batch.appender().manifest_state().active_segment, Some(2));
    drop(batch);
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    assert_eq!(manager.state().active_segment, Some(2));
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fresh_writer_rejects_nonempty_manifest_even_without_active_segment() {
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    let root = std::env::temp_dir().join(format!("t22-stale-manifest-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let segments = root.join("segments");
    let manifest = root.join("manifest");
    std::fs::create_dir(&segments).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::SnapshotCommitted {
            snapshot_id: 1,
            snapshot_seq: 9,
        })
        .unwrap();
    assert!(RegisteredAppender::new(SegmentWriter::new(&segments, 70).unwrap(), manager).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_failure_after_wal_sync_cannot_advance_commit() {
    use matching_persistence::{
        ManifestManager, ManifestState, ManifestStorage, RegisteredAppender, SegmentWriter,
    };
    struct Failing {
        /// 已尝试写入的清单代数。
        writes: usize,
    }
    impl ManifestStorage for Failing {
        fn write_inactive_tmp(&mut self, _: u64, _: &ManifestState) -> io::Result<()> {
            self.writes += 1;
            if self.writes == 3 {
                Err(io::Error::other("manifest 故障"))
            } else {
                Ok(())
            }
        }
        fn sync_tmp(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn rename_inactive_slot(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn sync_directory(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let dir = std::env::temp_dir().join(format!("t22-manifest-failure-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let writer = SegmentWriter::new(&dir, 1024).unwrap();
    let manager = ManifestManager::new(Failing { writes: 0 }, ManifestState::default());
    let appender = RegisteredAppender::new(writer, manager).unwrap();
    let mut batch = DurableBatcher::new_with_config(
        appender,
        config(DurabilityMode::FsyncEach, ReplicationMode::LocalDurable),
        1,
    )
    .unwrap();
    assert!(batch.append_at(&record(1), Instant::now()).is_err());
    assert_eq!(batch.local_durable(), 0);
    assert_eq!(batch.committed(), 0);
    assert!(batch.append_at(&record(2), Instant::now()).is_err());
    drop(batch);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn constructor_rejects_ha_local_durability_without_explicit_loss_acceptance() {
    let mut config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    assert!(DurableBatcher::new_with_config(Appender::default(), config, 1).is_err());
    config.allow_data_loss_on_failover = true;
    assert!(DurableBatcher::new_with_config(Appender::default(), config, 1).is_ok());
}

#[test]
fn synced_batch_recovers_local_durable_watermark_from_manifest() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    let root = std::env::temp_dir().join(format!("t23-watermark-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(&wal, 1024).unwrap(), manager).unwrap();
    let mut batch = DurableBatcher::new_with_config(
        appender,
        config(DurabilityMode::FsyncEach, ReplicationMode::LocalDurable),
        1,
    )
    .unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    batch.append_at(&record(2), Instant::now()).unwrap();
    drop(batch);
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    assert_eq!(manager.state().local_durable_seq, 2);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_rejects_missing_second_slot_after_first_ack() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    let root = std::env::temp_dir().join(format!("t23-missing-slot-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(&wal, 1024).unwrap(), manager).unwrap();
    let mut batch = DurableBatcher::new_with_config(
        appender,
        config(DurabilityMode::FsyncEach, ReplicationMode::LocalDurable),
        1,
    )
    .unwrap();
    batch.append_at(&record(1), Instant::now()).unwrap();
    drop(batch);
    assert!(manifest.join("manifest-b").exists());
    std::fs::remove_file(manifest.join("manifest-b")).unwrap();
    assert!(ManifestManager::<FileManifestStorage>::open_recovery(&manifest).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn startup_fails_before_wal_write_if_second_manifest_slot_fails() {
    use matching_persistence::{
        ManifestManager, ManifestState, ManifestStorage, RegisteredAppender, SegmentWriter,
    };
    struct FailSecond {
        /// 已尝试写入的清单代数。
        writes: usize,
    }
    impl ManifestStorage for FailSecond {
        fn write_inactive_tmp(&mut self, _: u64, _: &ManifestState) -> io::Result<()> {
            self.writes += 1;
            if self.writes == 2 {
                Err(io::Error::other("第二槽故障"))
            } else {
                Ok(())
            }
        }
        fn sync_tmp(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn rename_inactive_slot(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn sync_directory(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let dir = std::env::temp_dir().join(format!("t23-second-slot-fault-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let manager = ManifestManager::new(FailSecond { writes: 0 }, ManifestState::default());
    assert!(RegisteredAppender::new(SegmentWriter::new(&dir, 1024).unwrap(), manager).is_err());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(dir).unwrap();
}
