//! 权威日志与本地恢复前缀的逐条比对。
use matching_control::{EtcdClient, FencedLog};
use matching_domain::{
    AssetId, ClusterId, Command, CommandEnvelope, CommandId, MarketId, MarketMath, MassCancelShard,
    ShardCancelScope, ShardId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
    Profile, RegisteredAppender, ReplicationMode, SegmentWriter, WalRecord,
};
use matching_risk::{LimitReservePolicy, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable,
    SnapshotRestoreConfig,
};
use std::time::{Duration, Instant};

#[test]
fn authenticated_recovery_implements_runtime_coordinator() {
    fn requires_coordinator<T: matching_runtime::ReplicatedRecoveryCoordinator>() {}
    requires_coordinator::<matching_control::AuthenticatedRecovery>();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn bound_writer_verifies_local_recovery_against_authoritative_records() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-fenced-recovery-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    let manifest = root.join("manifest");
    let snapshots = root.join("snapshots");
    std::fs::create_dir(&wal).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    std::fs::create_dir(&snapshots).unwrap();
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let cluster = ClusterId(171);
    let shard = ShardId(172);
    let market = MarketId(173);
    let log = FencedLog::new(
        client,
        &format!("/tide-test/fenced-recovery/{}-{stamp}", std::process::id()),
        cluster,
        shard,
    )
    .unwrap();
    let mut writer = log
        .acquire_bound(30, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    let make = |seq| {
        let command = CommandEnvelope {
            command_id: CommandId(u128::from(seq)),
            session_id: seq,
            client_seq: 1,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        };
        WalRecord {
            cluster_id: cluster.0,
            shard_id: shard.0,
            flags: 0,
            epoch: writer.epoch(),
            shard_seq: seq,
            engine_time: 0,
            command_id: u128::from(seq),
            payload: matching_domain::encode_wal_command(&command).unwrap(),
        }
    };
    let first = make(1);
    let second = make(2);
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(&wal, 4096).unwrap(), manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, writer.epoch()).unwrap();
    batcher.append_at(&first, Instant::now()).unwrap();
    batcher.append_at(&second, Instant::now()).unwrap();
    drop(batcher);
    writer.append(&first).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard,
        primary_epoch: writer.epoch(),
        routes: RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: shard,
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let policies = [(market, QueuePriorityPolicy::try_new(100).unwrap())];
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let deadline = || Instant::now() + Duration::from_secs(5);
    let genesis = writer
        .recover_verified_genesis_prefix(&wal, &manager, &trusted, &policies, deadline())
        .unwrap();
    assert_eq!(genesis.commands().len(), 1);
    assert_eq!(genesis.snapshot().image.snapshot_seq(), 0);
    let recovered = writer
        .recover_verified_local_prefix(&wal, &snapshots, &manager, &trusted, &policies, deadline())
        .unwrap()
        .unwrap();
    assert_eq!(recovered.commands().len(), 1);
    assert_eq!(recovered.commands()[0].shard_seq, 1);
    let mut different = second;
    different.engine_time = 1;
    writer.append(&different).unwrap();
    let behind_wal = root.join("behind-wal");
    let behind_manifest = root.join("behind-manifest");
    std::fs::create_dir(&behind_wal).unwrap();
    std::fs::create_dir(&behind_manifest).unwrap();
    let behind_manager = ManifestManager::<FileManifestStorage>::open(&behind_manifest).unwrap();
    let behind = RegisteredAppender::new(
        SegmentWriter::new(&behind_wal, 4096).unwrap(),
        behind_manager,
    )
    .unwrap();
    let mut batcher = DurableBatcher::new_with_config(behind, config, writer.epoch()).unwrap();
    batcher.append_at(&first, Instant::now()).unwrap();
    drop(batcher);
    let mut behind_manager =
        ManifestManager::<FileManifestStorage>::open_recovery(&behind_manifest).unwrap();
    writer
        .extend_verified_local_prefix(&behind_wal, &mut behind_manager, 4096, deadline())
        .unwrap();
    drop(behind_manager);
    assert_eq!(
        matching_persistence::recover_local(&behind_wal, &behind_manifest, 0, cluster.0, shard.0)
            .unwrap(),
        vec![first.clone(), different.clone()]
    );
    assert!(
        writer
            .recover_verified_local_prefix(
                &wal,
                &snapshots,
                &manager,
                &trusted,
                &policies,
                deadline()
            )
            .is_err()
    );
    assert!(writer.is_locally_invalidated());
    writer.revoke().unwrap();
    let mismatch_wal = root.join("mismatch-wal");
    let mismatch_manifest = root.join("mismatch-manifest");
    std::fs::create_dir(&mismatch_wal).unwrap();
    std::fs::create_dir(&mismatch_manifest).unwrap();
    let mismatch_manager =
        ManifestManager::<FileManifestStorage>::open(&mismatch_manifest).unwrap();
    let local = RegisteredAppender::new(
        SegmentWriter::new(&mismatch_wal, 4096).unwrap(),
        mismatch_manager,
    )
    .unwrap();
    let mut batcher = DurableBatcher::new_with_config(local, config, 1).unwrap();
    let mut altered = first;
    altered.engine_time = 99;
    batcher.append_at(&altered, Instant::now()).unwrap();
    drop(batcher);
    let mut mismatch_manager =
        ManifestManager::<FileManifestStorage>::open_recovery(&mismatch_manifest).unwrap();
    let before = std::fs::read(mismatch_wal.join("00000000000000000001.wal")).unwrap();
    let mut next = log
        .acquire_bound(30, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    assert!(
        next.extend_verified_local_prefix(&mismatch_wal, &mut mismatch_manager, 4096, deadline())
            .is_err()
    );
    assert!(next.is_locally_invalidated());
    assert_eq!(mismatch_manager.state().local_durable_seq, 1);
    assert_eq!(
        std::fs::read(mismatch_wal.join("00000000000000000001.wal")).unwrap(),
        before
    );
    next.revoke().unwrap();
}
