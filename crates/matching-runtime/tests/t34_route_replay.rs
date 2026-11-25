//! 路由版本异常的已提交 WAL 重放验证。
mod t22_support;
use matching_domain::{
    AssetId, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime, MarketId, MarketMath,
    MassCancelShard, ShardCancelScope, ShardId,
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

#[test]
fn replay_rejects_future_and_regressing_route_epochs() {
    for epochs in [&[3_u64][..], &[2_u64, 1][..]] {
        let dir = t22_support::TestDirectory::new();
        let snapshots = dir.0.join("snapshots");
        std::fs::create_dir(&snapshots).unwrap();
        let market = MarketId(70);
        let shard = ShardId(71);
        let cluster = ClusterId(72);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: shard,
            route_epoch: 2,
            state: RouteState::Active,
        }])
        .unwrap();
        let manager = ManifestManager::<FileManifestStorage>::open(dir.0.join("manifest")).unwrap();
        let appender = RegisteredAppender::new(
            SegmentWriter::new(dir.0.join("wal"), 4096).unwrap(),
            manager,
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
        let mut batcher = DurableBatcher::new_with_config(appender, config, 1).unwrap();
        for (index, epoch) in epochs.iter().enumerate() {
            let seq = u64::try_from(index).unwrap().checked_add(1).unwrap();
            let command = CommandEnvelope {
                command_id: CommandId(u128::from(seq)),
                session_id: seq,
                client_seq: 1,
                market_id: Some(market),
                user_id: None,
                route_epoch: Some(*epoch),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::MassCancelShard(MassCancelShard {
                    scope: ShardCancelScope::Market { market_id: market },
                }),
            };
            batcher
                .append_at(
                    &WalRecord {
                        cluster_id: cluster.get(),
                        shard_id: shard.get(),
                        flags: 0,
                        epoch: 1,
                        shard_seq: seq,
                        engine_time: LogicalTime::new(0).get(),
                        command_id: u128::from(seq),
                        payload: matching_domain::encode_wal_command(&command).unwrap(),
                    },
                    std::time::Instant::now(),
                )
                .unwrap();
        }
        drop(batcher);
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
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
            primary_epoch: 1,
            routes,
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let path = dir.0.join("wal").join("00000000000000000001.wal");
        let before = std::fs::read(&path).unwrap();
        let manager =
            ManifestManager::<FileManifestStorage>::open_recovery(dir.0.join("manifest")).unwrap();
        let records = matching_runtime::recover_snapshot_records_with_genesis(
            dir.0.join("wal"),
            snapshots,
            &manager,
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
        )
        .unwrap()
        .unwrap();
        assert!(records.replay().is_err());
        assert_eq!(std::fs::read(path).unwrap(), before);
    }
}
