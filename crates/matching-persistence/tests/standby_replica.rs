//! 验证备节点只确认真实同步的连续 WAL 前缀。
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableAppender, DurableBatcher, Profile, ReplicaDurableAck,
    ReplicationMode, StandbyReplica, WalRecord,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Instant;

struct Appender {
    /// 记录持久同步调用次数。
    syncs: Arc<AtomicUsize>,
    /// 记录 WAL 追加调用次数。
    writes: Arc<AtomicUsize>,
    /// 控制持久同步是否返回模拟错误。
    fail: Arc<AtomicBool>,
}
impl DurableAppender for Appender {
    fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
        Ok(false)
    }
    fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn sync(&mut self) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(std::io::Error::other("模拟同步失败"))
        } else {
            Ok(())
        }
    }
}
fn record(epoch: u64, seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![1],
    }
}
fn replica(
    syncs: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
) -> StandbyReplica<Appender> {
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 8,
            max_delay_micros: 1000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(
        Appender {
            syncs,
            writes,
            fail,
        },
        config,
        3,
    )
    .unwrap();
    StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap()
}

#[test]
fn durable_ack_requires_contiguous_identity_and_successful_sync() {
    let syncs = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let mut standby = replica(syncs.clone(), writes.clone(), fail.clone());
    assert!(
        standby
            .append_batch(&[record(3, 2)], Instant::now())
            .is_err()
    );
    assert!(
        standby
            .append_batch(&[record(4, 1)], Instant::now())
            .is_err()
    );
    assert!(
        standby
            .append_batch(&[record(3, 1), record(3, 3)], Instant::now())
            .is_err()
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(syncs.load(Ordering::SeqCst), 0);
    assert_eq!(
        standby
            .append_batch(&[record(3, 1), record(3, 2)], Instant::now())
            .unwrap(),
        ReplicaDurableAck {
            cluster_id: 1,
            shard_id: 2,
            epoch: 3,
            first_seq: 1,
            through_seq: 2
        }
    );
    assert_eq!(syncs.load(Ordering::SeqCst), 1);
    fail.store(true, Ordering::SeqCst);
    assert!(
        standby
            .append_batch(&[record(3, 3)], Instant::now())
            .is_err()
    );
    fail.store(false, Ordering::SeqCst);
    assert!(
        standby
            .append_batch(&[record(3, 3)], Instant::now())
            .is_err()
    );
    assert_eq!(syncs.load(Ordering::SeqCst), 2);
}

#[test]
fn persistent_epoch_barrier_serializes_batches_and_requires_authority_on_restart() {
    use matching_persistence::{FencedStandbyReplica, ReplicaEpochStore};
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-fenced-standby-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let store = ReplicaEpochStore::create(root.join("epoch"), 1, 2, 3, 0).unwrap();
    let syncs = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let standby = replica(syncs, writes.clone(), fail);
    let mut fenced = FencedStandbyReplica::new(standby, store).unwrap();
    assert!(
        fenced
            .append_batch(&[record(3, 1)], Instant::now())
            .is_err()
    );
    assert!(
        fenced
            .activate(3, |_, _, _| Err(std::io::Error::other("无写权")))
            .is_err()
    );
    fenced.activate(3, |_, _, _| Ok(())).unwrap();
    assert!(
        fenced
            .append_batch(&[record(3, 1)], Instant::now())
            .is_err()
    );
    assert!(fenced.confirm_reconciled(1, |_, _, _| Ok(())).is_err());
    assert!(
        fenced
            .confirm_reconciled(0, |_, _, _| Err(std::io::Error::other("权威失效")))
            .is_err()
    );
    assert!(
        fenced
            .append_batch(&[record(3, 1)], Instant::now())
            .is_err()
    );
    fenced.confirm_reconciled(0, |_, _, _| Ok(())).unwrap();
    fenced
        .append_batch(&[record(3, 1)], Instant::now())
        .unwrap();
    assert!(
        fenced
            .activate(4, |_, _, _| Err(std::io::Error::other("无写权")))
            .is_err()
    );
    assert!(
        fenced
            .append_batch(&[record(3, 2)], Instant::now())
            .is_err()
    );
    assert_eq!(
        fenced
            .activate(4, |cluster, shard, epoch| {
                assert_eq!((cluster, shard, epoch), (1, 2, 4));
                Ok(())
            })
            .unwrap(),
        1
    );
    assert!(
        fenced
            .append_batch(&[record(4, 2)], Instant::now())
            .is_err()
    );
    fenced.confirm_reconciled(1, |_, _, _| Ok(())).unwrap();
    assert!(
        fenced
            .append_batch(&[record(3, 2)], Instant::now())
            .is_err()
    );
    fenced
        .append_batch(&[record(4, 2)], Instant::now())
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    drop(fenced);
    let store = ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
    assert_eq!((store.state().epoch, store.state().durable_seq), (4, 1));
    drop(store);
    let target = root.canonicalize().unwrap();
    assert_eq!(
        target.parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-fenced-standby-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn acknowledged_prefix_is_recoverable_from_real_standby_wal() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("tide-standby-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let manifest = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        manifest,
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 8,
            max_delay_micros: 1000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let records = vec![record(3, 1), record(3, 2)];
    assert_eq!(
        standby
            .append_batch(&records, Instant::now())
            .unwrap()
            .through_seq,
        2
    );
    drop(standby);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        records
    );
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap();
    let batcher = DurableBatcher::resume_local(appender, config, 3).unwrap();
    let mut resumed = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    assert_eq!(
        resumed
            .append_batch(&[record(3, 3)], Instant::now())
            .unwrap(),
        ReplicaDurableAck {
            cluster_id: 1,
            shard_id: 2,
            epoch: 3,
            first_seq: 3,
            through_seq: 3
        }
    );
    drop(resumed);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(3, 1), record(3, 2), record(3, 3)]
    );
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-standby-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn replicated_resume_uses_strict_local_baseline_and_requires_ready_factory() {
    use matching_persistence::{
        FileManifestStorage, ManifestManager, RegisteredAppender, SegmentWriter,
    };
    struct ReplicaAppender {
        /// 实际本地 WAL 持久化组件。
        local: RegisteredAppender<FileManifestStorage>,
        /// 控制模拟复制链的就绪状态。
        ready: bool,
        /// 等待生成备节点确认的记录。
        pending: Option<WalRecord>,
        /// 待交付的备节点持久确认。
        ack: Option<ReplicaDurableAck>,
    }
    impl DurableAppender for ReplicaAppender {
        fn would_rotate(&self, record: &WalRecord) -> std::io::Result<bool> {
            self.local.would_rotate(record)
        }
        fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
            self.local.append(record)?;
            self.pending = Some(record.clone());
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.local.sync()?;
            let record = self.pending.take().unwrap();
            self.ack = Some(ReplicaDurableAck {
                cluster_id: record.cluster_id,
                shard_id: record.shard_id,
                epoch: record.epoch,
                first_seq: record.shard_seq,
                through_seq: record.shard_seq,
            });
            Ok(())
        }
        fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
            self.ack.take()
        }
        fn replication_ready(&self) -> Option<bool> {
            Some(self.ready)
        }
        fn checkpoint_digest(
            &mut self,
            seq: u64,
            state: [u8; 32],
            events: [u8; 32],
        ) -> std::io::Result<()> {
            assert_eq!((seq, state, events), (3, [1; 32], [2; 32]));
            Ok(())
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-replicated-resume-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let local_config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let config = DurabilityConfig {
        profile: Profile::Production,
        replication: ReplicationMode::StandbyDurable,
        ha_enabled: true,
        ..local_config
    };
    let manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(root.join("wal"), 4096).unwrap(), manager)
            .unwrap();
    let mut local = DurableBatcher::new_with_config(appender, local_config, 3).unwrap();
    local.append_at(&record(3, 1), Instant::now()).unwrap();
    local.append_at(&record(3, 2), Instant::now()).unwrap();
    drop(local);
    let open = || {
        let manager =
            ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap()
    };
    let build = |local, ready| ReplicaAppender {
        local,
        ready,
        pending: None,
        ack: None,
    };
    assert!(
        DurableBatcher::resume_replicated_with(open(), local_config, 4, |local, _| {
            Ok(build(local, true))
        })
        .is_err()
    );
    assert!(
        DurableBatcher::resume_replicated_with(open(), config, 0, |local, _| {
            Ok(build(local, true))
        })
        .is_err()
    );
    assert!(
        DurableBatcher::resume_replicated_with(open(), config, 4, |local, baseline| {
            assert_eq!(baseline, 2);
            Ok(build(local, false))
        })
        .is_err()
    );
    assert!(
        DurableBatcher::resume_replicated_with(
            open(),
            config,
            4,
            |_local, baseline| -> std::io::Result<ReplicaAppender> {
                assert_eq!(baseline, 2);
                Err(std::io::Error::other("权威前缀不一致"))
            }
        )
        .is_err()
    );
    let mut resumed =
        DurableBatcher::resume_replicated_with(open(), config, 4, |local, baseline| {
            assert_eq!(baseline, 2);
            Ok(build(local, true))
        })
        .unwrap();
    assert_eq!(resumed.local_durable(), 2);
    assert_eq!(resumed.committed(), 2);
    assert!(resumed.checkpoint_digest(1, [1; 32], [2; 32]).is_err());
    resumed.append_at(&record(4, 3), Instant::now()).unwrap();
    assert_eq!(resumed.committed(), 3);
    resumed.checkpoint_digest(3, [1; 32], [2; 32]).unwrap();
    drop(resumed);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(3, 1), record(3, 2), record(4, 3)]
    );
    let target = root.canonicalize().unwrap();
    assert_eq!(
        target.parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replicated-resume-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn fenced_standby_reopens_real_wal_without_reactivating_old_epoch() {
    use matching_persistence::{
        FencedStandbyReplica, FileManifestStorage, ManifestManager, RegisteredAppender,
        ReplicaEpochStore, SegmentWriter,
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("tide-standby-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(root.join("wal"), 4096).unwrap(), manager)
            .unwrap();
    let batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let store = ReplicaEpochStore::create(root.join("epoch"), 1, 2, 3, 0).unwrap();
    let mut standby = FencedStandbyReplica::new(replica, store).unwrap();
    assert!(standby.fenced_records(0).is_err());
    standby.activate(3, |_, _, _| Ok(())).unwrap();
    assert!(standby.fenced_records(0).unwrap().is_empty());
    standby.confirm_reconciled(0, |_, _, _| Ok(())).unwrap();
    standby
        .append_batch(&[record(3, 1)], Instant::now())
        .unwrap();
    standby.activate(4, |_, _, _| Ok(())).unwrap();
    assert_eq!(standby.fenced_records(0).unwrap(), vec![record(3, 1)]);
    assert!(standby.fenced_records(1).is_err());
    standby.confirm_reconciled(1, |_, _, _| Ok(())).unwrap();
    assert!(standby.fenced_records(0).is_err());
    drop(standby);
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap();
    let batcher = DurableBatcher::resume_local(appender, config, 4).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 4, 8).unwrap();
    let store = ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
    let mut standby = FencedStandbyReplica::new(replica, store).unwrap();
    assert!(
        standby
            .append_batch(&[record(4, 2)], Instant::now())
            .is_err()
    );
    assert!(standby.activate(3, |_, _, _| Ok(())).is_err());
    standby.activate(4, |_, _, _| Ok(())).unwrap();
    assert_eq!(standby.fenced_records(0).unwrap(), vec![record(3, 1)]);
    standby.confirm_reconciled(1, |_, _, _| Ok(())).unwrap();
    assert!(
        standby
            .append_batch(&[record(3, 2)], Instant::now())
            .is_err()
    );
    standby
        .append_batch(&[record(4, 2)], Instant::now())
        .unwrap();
    drop(standby);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(3, 1), record(4, 2)]
    );
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap();
    let batcher = DurableBatcher::resume_local(appender, config, 4).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 4, 8).unwrap();
    let store = ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
    let mut standby = FencedStandbyReplica::new(replica, store).unwrap();
    standby.activate(4, |_, _, _| Ok(())).unwrap();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(root.join("wal").join("00000000000000000001.wal"))
            .unwrap();
        file.write_all(&[0]).unwrap();
        file.sync_all().unwrap();
    }
    assert!(standby.fenced_records(0).is_err());
    drop(standby);
    let target = root.canonicalize().unwrap();
    assert_eq!(
        target.parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-standby-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn primary_advances_commit_only_for_matching_durable_replica_ack() {
    struct AckAppender {
        /// 待交付的备节点持久确认。
        ack: Option<ReplicaDurableAck>,
        /// 记录持久确认是否已经交付。
        delivered: bool,
    }
    impl DurableAppender for AckAppender {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.delivered = true;
            Ok(())
        }
        fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
            if self.delivered {
                self.ack.take()
            } else {
                None
            }
        }
    }
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::StandbyDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    let ack = ReplicaDurableAck {
        cluster_id: 1,
        shard_id: 2,
        epoch: 3,
        first_seq: 1,
        through_seq: 1,
    };
    let mut good = DurableBatcher::new_with_config(
        AckAppender {
            ack: Some(ack),
            delivered: false,
        },
        config,
        3,
    )
    .unwrap();
    good.set_standby_available(true);
    good.append_at(&record(3, 1), Instant::now()).unwrap();
    assert_eq!(good.committed(), 1);
    for invalid in [
        ReplicaDurableAck {
            cluster_id: 9,
            ..ack
        },
        ReplicaDurableAck { shard_id: 9, ..ack },
        ReplicaDurableAck { epoch: 2, ..ack },
        ReplicaDurableAck {
            first_seq: 2,
            ..ack
        },
        ReplicaDurableAck {
            through_seq: 2,
            ..ack
        },
    ] {
        let mut wrong = DurableBatcher::new_with_config(
            AckAppender {
                ack: Some(invalid),
                delivered: false,
            },
            config,
            3,
        )
        .unwrap();
        wrong.set_standby_available(true);
        assert!(wrong.append_at(&record(3, 1), Instant::now()).is_err());
        assert_eq!(wrong.committed(), 0);
        assert!(wrong.append_at(&record(3, 2), Instant::now()).is_err());
    }
}
