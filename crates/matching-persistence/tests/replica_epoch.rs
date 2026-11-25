//! 验证备节点接任栅栏的持久纪元与损坏处理。
use matching_persistence::{ReplicaEpochState, ReplicaEpochStore};
use std::io::Write;

#[test]
fn crash_child_holds_synced_fence_until_terminated() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, SegmentWriter, StandbyReplica, WalRecord,
    };
    let Some(root) = std::env::var_os("TIDE_FENCE_CRASH_CHILD_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap(),
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    let mut replica = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let records: Vec<_> = (1..=2)
        .map(|seq| WalRecord {
            cluster_id: 1,
            shard_id: 2,
            flags: 0,
            epoch: 3,
            shard_seq: seq,
            engine_time: 0,
            command_id: u128::from(seq),
            payload: vec![7],
        })
        .collect();
    assert_eq!(
        replica
            .append_batch(&records, std::time::Instant::now())
            .unwrap()
            .through_seq,
        2
    );
    let mut store = ReplicaEpochStore::create(root.join("epoch.log"), 1, 2, 3, 0).unwrap();
    store.advance(3, 4, 2).unwrap();
    std::fs::write(root.join("ready"), b"ready").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(15));
    drop(store);
    drop(replica);
}

#[test]
fn synced_fence_survives_abrupt_process_termination() {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FencedStandbyReplica,
        FileManifestStorage, ManifestManager, Profile, RegisteredAppender, ReplicationMode,
        StandbyReplica,
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-epoch-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "crash_child_holds_synced_fence_until_terminated"])
        .env("TIDE_FENCE_CRASH_CHILD_DIR", &root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    let mut child = command.spawn().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !root.join("ready").exists() && std::time::Instant::now() < deadline {
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let ready = root.join("ready").exists();
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(ready);
    assert!(!status.success());
    let store = ReplicaEpochStore::open(root.join("epoch.log"), 1, 2).unwrap();
    assert_eq!((store.state().epoch, store.state().durable_seq), (4, 2));
    drop(store);
    let records =
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| (record.epoch, record.shard_seq, record.payload.as_slice()))
            .collect::<Vec<_>>(),
        vec![(3, 1, &[7][..]), (3, 2, &[7][..])]
    );
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::resume_local(appender, config, 4).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 4, 8).unwrap();
    let store = ReplicaEpochStore::open(root.join("epoch.log"), 1, 2).unwrap();
    let mut standby = FencedStandbyReplica::new(replica, store).unwrap();
    assert_eq!(standby.activate(4, |_, _, _| Ok(())).unwrap(), 2);
    assert_eq!(standby.fenced_records(0).unwrap(), records);
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
            .starts_with("tide-replica-epoch-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn standby_fence_is_monotonic_exclusive_and_recovers() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-epoch-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("epoch.log");
    let mut store = ReplicaEpochStore::create(&path, 1, 2, 3, 0).unwrap();
    assert!(ReplicaEpochStore::open(&path, 1, 2).is_err());
    assert!(store.advance(3, 3, 0).is_err());
    assert!(store.advance(2, 4, 0).is_err());
    store.advance(3, 4, 2).unwrap();
    assert!(store.advance(4, 5, 1).is_err());
    drop(store);
    let store = ReplicaEpochStore::open(&path, 1, 2).unwrap();
    assert_eq!(
        store.state(),
        ReplicaEpochState {
            cluster_id: 1,
            shard_id: 2,
            epoch: 4,
            durable_seq: 2
        }
    );
    drop(store);
    assert!(ReplicaEpochStore::open(&path, 1, 9).is_err());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[1])
        .unwrap();
    assert!(ReplicaEpochStore::open(&path, 1, 2).is_err());
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-epoch-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn external_tail_change_stops_the_writer_before_new_fence() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-epoch-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("epoch.log");
    let mut store = ReplicaEpochStore::create(&path, 1, 2, 3, 0).unwrap();
    let result = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[0]);
    match result {
        Ok(()) => {
            assert!(store.advance(3, 4, 0).is_err());
            assert!(store.advance(3, 4, 0).is_err());
            drop(store);
            assert!(ReplicaEpochStore::open(&path, 1, 2).is_err());
        }
        Err(error) => {
            assert_eq!(error.raw_os_error(), Some(33));
            store.advance(3, 4, 0).unwrap();
            drop(store);
            assert_eq!(
                ReplicaEpochStore::open(&path, 1, 2).unwrap().state().epoch,
                4
            );
        }
    }
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
            .starts_with("tide-replica-epoch-")
    );
    std::fs::remove_dir_all(target).unwrap();
}
