//! 验证有可信租约时的历史纪元回放与损坏日志失败关闭。
mod t22_support;
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
    Profile, RegisteredAppender, ReplicationMode, SegmentWriter, WalRecord,
};
use matching_risk::{LimitReservePolicy, RiskKeySet};
use matching_runtime::{
    ControlStore, IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState,
    RouteTable, SnapshotRestoreConfig,
};
use std::time::Instant;

#[test]
fn replay_rejects_future_epoch_without_mutating_wal() {
    reject_epochs(&[3]);
}
#[test]
fn replay_rejects_epoch_regression_without_mutating_wal() {
    reject_epochs(&[2, 1]);
}

fn reject_epochs(epochs: &[u64]) {
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    let wal = temp.0.join("wal");
    let manifest = temp.0.join("manifest");
    let cluster = ClusterId(2);
    let shard = ShardId(43);
    let store = ControlStore::new(temp.0.join("lease"), cluster, shard);
    drop(store.acquire_primary().unwrap());
    let lease = std::sync::Arc::new(store.acquire_primary().unwrap());
    assert_eq!(lease.epoch(), 2);
    let durability = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    for (index, epoch) in epochs.iter().copied().enumerate() {
        let manager = if index == 0 {
            ManifestManager::<FileManifestStorage>::open(&manifest).unwrap()
        } else {
            ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap()
        };
        let appender = if index == 0 {
            RegisteredAppender::new(SegmentWriter::new(&wal, 4096).unwrap(), manager).unwrap()
        } else {
            RegisteredAppender::recover_local(&wal, 4096, manager, 0, cluster.get(), shard.get())
                .unwrap()
        };
        let mut batcher = if index == 0 {
            DurableBatcher::new_with_config(appender, durability, epoch).unwrap()
        } else {
            DurableBatcher::resume_local(appender, durability, epoch).unwrap()
        };
        let id = u128::try_from(index + 1).unwrap();
        let cmd = CommandEnvelope {
            command_id: CommandId(id),
            session_id: 1,
            client_seq: id as u64,
            market_id: None,
            user_id: Some(UserId(1)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(id),
                user_id: UserId(1),
                new_epoch: id as u64,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: epoch,
            }),
        };
        batcher
            .append_at(
                &WalRecord {
                    cluster_id: cluster.get(),
                    shard_id: shard.get(),
                    flags: 0,
                    epoch,
                    shard_seq: id as u64,
                    engine_time: 0,
                    command_id: id,
                    payload: encode_wal_command(&cmd).unwrap(),
                },
                Instant::now(),
            )
            .unwrap();
        drop(batcher);
    }
    let market = MarketId(1);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: shard,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 4,
            terminal_history: 4,
            client_id_bytes: 8,
        },
    )
    .unwrap()
    .with_primary_lease(lease);
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard,
        primary_epoch: 2,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let before = std::fs::read(wal.join("00000000000000000001.wal")).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let result = matching_runtime::recover_snapshot_records_with_genesis(
        &wal,
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay();
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(wal.join("00000000000000000001.wal")).unwrap(),
        before
    );
}
