//! 多分片独立队列、持久序号和恢复的组合验证。
mod t22_support;
use matching_domain::{
    AssetId, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime, MarketId, MarketMath,
    MassCancelShard, ShardCancelScope, ShardId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteControlStore,
    RouteCoordinator, RouteState, RouteTable, Shard, ShardError, SnapshotRestoreConfig,
};
use std::time::{Duration, Instant};

fn retry<T>(mut action: impl FnMut() -> Result<T, ShardError>) -> T {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        match action() {
            Ok(value) => return value,
            Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
            Err(error) => panic!("分片入口失败: {error:?}"),
        }
    }
}

fn config(market: MarketId) -> PlaceSupportConfig {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
}

fn cancel(id: u128, market: MarketId, epoch: u64) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId(id),
        session_id: u64::try_from(id).unwrap(),
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(epoch),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    }
}

#[test]
fn replicated_start_rejects_unready_or_uncoordinated_factory() {
    let dir = t22_support::TestDirectory::new();
    let shard = ShardId(82);
    let market = MarketId(81);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: shard,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let result = Shard::start_replicated_group_place_support(
        (shard, ClusterId(967)),
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (
            IdempotencyConfig::default(),
            1,
            8,
            config(market).require_route_authority(),
        ),
        ((dir.0.join("wal"), dir.0.join("manifest")), (1, 1_000)),
        Ok,
    );
    assert!(matches!(result, Err(ShardError::InvalidConfig)));
    let called = std::cell::Cell::new(false);
    let result = Shard::start_replicated_group_place_support(
        (shard, ClusterId(967)),
        routes,
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 1, 8, config(market)),
        ((dir.0.join("wal"), dir.0.join("manifest")), (1, 1_000)),
        |local| {
            called.set(true);
            Ok(local)
        },
    );
    assert!(matches!(result, Err(ShardError::InvalidConfig)));
    assert!(!called.get());
}

#[test]
fn digest_checkpoint_failure_disconnects_reply_before_candidate_is_installed() {
    use matching_persistence::{
        DurableAppender, FileManifestStorage, RegisteredAppender, ReplicaDurableAck, WalRecord,
    };
    struct FailingDigest {
        /// 负责实际本地 WAL 写入的持久化组件。
        local: RegisteredAppender<FileManifestStorage>,
        /// 待生成备节点确认的 WAL 记录。
        pending: Option<WalRecord>,
    }
    impl DurableAppender for FailingDigest {
        fn would_rotate(&self, record: &WalRecord) -> std::io::Result<bool> {
            self.local.would_rotate(record)
        }
        fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
            self.local.append(record)?;
            self.pending = Some(record.clone());
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.local.sync()
        }
        fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
            self.pending.take().map(|r| ReplicaDurableAck {
                cluster_id: r.cluster_id,
                shard_id: r.shard_id,
                epoch: r.epoch,
                first_seq: r.shard_seq,
                through_seq: r.shard_seq,
            })
        }
        fn replication_ready(&self) -> Option<bool> {
            Some(true)
        }
        fn digest_checkpoint_required(&self) -> bool {
            true
        }
        fn checkpoint_digest(&mut self, seq: u64, _: [u8; 32], _: [u8; 32]) -> std::io::Result<()> {
            assert_eq!(seq, 1);
            Err(std::io::Error::other("模拟摘要持久化失败"))
        }
    }
    let dir = t22_support::TestDirectory::new();
    let control = t22_support::TestDirectory::new();
    let cluster = ClusterId(968);
    let shard_id = ShardId(82);
    let market = MarketId(81);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start_replicated_group_place_support(
        (shard_id, cluster),
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (
            IdempotencyConfig::default(),
            1,
            8,
            config(market).require_route_authority(),
        ),
        ((dir.0.join("wal"), dir.0.join("manifest")), (1, 1_000)),
        |local| {
            Ok(FailingDigest {
                local,
                pending: None,
            })
        },
    )
    .unwrap();
    let handle = shard.handle();
    let store = RouteControlStore::create_for_cluster(&control.0, cluster, routes, 8, 8).unwrap();
    let coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
    let reply = retry(|| handle.submit_command(cancel(1, market, 1)));
    assert!(matches!(
        reply.recv_timeout(Duration::from_secs(5)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    ));
    assert!(matches!(
        shard.shutdown(Duration::from_secs(5)),
        Err(ShardError::Failed)
    ));
    drop(coordinator);
    drop(shard);
    assert_eq!(
        matching_persistence::recover_local(
            dir.0.join("wal"),
            dir.0.join("manifest"),
            0,
            cluster.0,
            shard_id.0
        )
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn replicated_start_waits_for_route_and_fails_closed_when_replica_unready() {
    use matching_persistence::{
        DurableAppender, FileManifestStorage, RegisteredAppender, ReplicaDurableAck, WalRecord,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct FakeReplica {
        /// 负责实际本地 WAL 写入的持久化组件。
        local: RegisteredAppender<FileManifestStorage>,
        /// 控制模拟复制链是否就绪。
        ready: Arc<AtomicBool>,
        /// 待生成备节点确认的 WAL 记录。
        pending: Option<WalRecord>,
    }
    impl DurableAppender for FakeReplica {
        fn would_rotate(&self, record: &WalRecord) -> std::io::Result<bool> {
            self.local.would_rotate(record)
        }
        fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
            self.local.append(record)?;
            self.pending = Some(record.clone());
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.local.sync()
        }
        fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
            self.pending.take().map(|record| ReplicaDurableAck {
                cluster_id: record.cluster_id,
                shard_id: record.shard_id,
                epoch: record.epoch,
                first_seq: record.shard_seq,
                through_seq: record.shard_seq,
            })
        }
        fn replication_ready(&self) -> Option<bool> {
            Some(self.ready.load(Ordering::SeqCst))
        }
    }
    let dir = t22_support::TestDirectory::new();
    let control = t22_support::TestDirectory::new();
    let cluster = ClusterId(969);
    let shard_id = ShardId(82);
    let market = MarketId(81);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let ready = Arc::new(AtomicBool::new(true));
    let adapter_ready = ready.clone();
    let mut shard = Shard::start_replicated_group_place_support(
        (shard_id, cluster),
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (
            IdempotencyConfig::default(),
            1,
            8,
            config(market).require_route_authority(),
        ),
        ((dir.0.join("wal"), dir.0.join("manifest")), (1, 1_000)),
        |local| {
            Ok(FakeReplica {
                local,
                ready: adapter_ready,
                pending: None,
            })
        },
    )
    .unwrap();
    let handle = shard.handle();
    assert!(matches!(
        retry(|| handle.submit_command(cancel(1, market, 1)))
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::EngineBusy
        ))
    ));
    assert!(
        matching_persistence::scan_segments(dir.0.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let store =
        RouteControlStore::create_for_cluster(&control.0, cluster, routes.clone(), 8, 8).unwrap();
    let coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
    assert_eq!(
        retry(|| handle.submit_command(cancel(1, market, 1)))
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    let digest = retry(|| handle.take_snapshot(1))
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    ready.store(false, Ordering::SeqCst);
    assert!(matches!(
        retry(|| handle.submit_command(cancel(2, market, 1))).recv_timeout(Duration::from_secs(2)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    ));
    assert!(matches!(
        shard.shutdown(Duration::from_secs(2)),
        Err(ShardError::Failed)
    ));
    drop(shard);
    drop(coordinator);
    assert_eq!(
        matching_persistence::scan_segments(dir.0.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    struct Coordinator {
        /// 恢复核验时使用的预期状态摘要。
        digest: [u8; 32],
        /// 控制模拟复制链是否就绪。
        ready: Arc<AtomicBool>,
    }
    impl matching_runtime::ReplicatedRecoveryCoordinator for Coordinator {
        type Appender = FakeReplica;
        fn recover(
            &mut self,
            wal: &std::path::Path,
            snapshots: &std::path::Path,
            manager: &mut matching_persistence::ManifestManager<FileManifestStorage>,
            trusted: &SnapshotRestoreConfig,
            policies: &[(MarketId, QueuePriorityPolicy)],
        ) -> std::io::Result<matching_runtime::SnapshotRecoveryRecords> {
            matching_runtime::recover_verified_snapshot_records_with_genesis(
                wal,
                snapshots,
                manager,
                trusted,
                policies,
                1,
                |record| {
                    if record.shard_seq != 1 {
                        return Err(std::io::Error::other("权威前缀不一致"));
                    }
                    Ok(())
                },
            )?
            .ok_or_else(|| std::io::Error::other("没有可用快照"))
        }
        fn verify_replay(
            &mut self,
            replay: &matching_runtime::SnapshotReplayResult,
        ) -> std::io::Result<()> {
            if replay.image.state_digest(1024 * 1024).unwrap() != self.digest {
                return Err(std::io::Error::other("重放状态不一致"));
            }
            Ok(())
        }
        fn finish(
            self,
            local: RegisteredAppender<FileManifestStorage>,
            baseline: u64,
            _replay: &matching_runtime::SnapshotReplayResult,
        ) -> std::io::Result<FakeReplica> {
            if baseline != 1 {
                return Err(std::io::Error::other("恢复基线不一致"));
            }
            Ok(FakeReplica {
                local,
                ready: self.ready,
                pending: None,
            })
        }
    }
    std::fs::create_dir(dir.0.join("snapshots")).unwrap();
    ready.store(true, Ordering::SeqCst);
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 2,
        routes: routes.clone(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(config(market).require_route_authority()),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let wal_path = dir.0.join("wal").join("00000000000000000001.wal");
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    let original_wal = std::fs::read(&wal_path).unwrap();
    let mut wrong_digest = digest;
    wrong_digest[0] ^= 1;
    assert!(
        Shard::recover_replicated_group_with_genesis(
            dir.0.join("wal"),
            dir.0.join("manifest"),
            dir.0.join("snapshots"),
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (1, 1_000),
            Coordinator {
                digest: wrong_digest,
                ready: ready.clone()
            },
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&wal_path).unwrap(), original_wal);
    let mut recovered = Shard::recover_replicated_group_with_genesis(
        dir.0.join("wal"),
        dir.0.join("manifest"),
        dir.0.join("snapshots"),
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (1, 1_000),
        Coordinator { digest, ready },
    )
    .unwrap();
    assert!(matches!(
        retry(|| recovered.handle().submit_command(cancel(3, market, 1)))
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::EngineBusy
        ))
    ));
    let store = RouteControlStore::open_for_cluster(&control.0, cluster, 8, 8).unwrap();
    let coordinator = RouteCoordinator::new(store, vec![recovered.handle()]).unwrap();
    assert_eq!(
        retry(|| recovered.handle().submit_command(cancel(3, market, 1)))
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    recovered.shutdown(Duration::from_secs(2)).unwrap();
    drop(coordinator);
}

#[test]
fn coordinated_durable_shard_rejects_before_route_authority_is_installed() {
    for use_snapshot in [false, true] {
        let dir = t22_support::TestDirectory::new();
        let control = t22_support::TestDirectory::new();
        std::fs::create_dir(dir.0.join("snapshots")).unwrap();
        let cluster = ClusterId(if use_snapshot { 965 } else { 966 });
        let shard_id = ShardId(82);
        let market = MarketId(81);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (
                IdempotencyConfig::default(),
                1,
                8,
                config(market).require_route_authority(),
            ),
            ((dir.0.join("wal"), dir.0.join("manifest")), (1, 1_000)),
        )
        .unwrap();
        let handle = shard.handle();
        let health = matching_runtime::Health::new(vec![handle.clone()]).unwrap();
        for dependency in [
            matching_runtime::HealthDependency::Recovery,
            matching_runtime::HealthDependency::Persistence,
            matching_runtime::HealthDependency::Control,
        ] {
            health.set_healthy(dependency, true);
        }
        assert!(!health.ready());
        assert!(matches!(
            retry(|| handle.submit_command(cancel(1, market, 1)))
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                matching_domain::RejectReason::EngineBusy
            ))
        ));
        assert_eq!(
            retry(|| handle.barrier(1))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .1,
            0
        );
        assert!(
            matching_persistence::scan_segments(dir.0.join("wal"))
                .unwrap()
                .records
                .is_empty()
        );
        let store =
            RouteControlStore::create_for_cluster(&control.0, cluster, routes.clone(), 8, 8)
                .unwrap();
        let coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
        assert!(health.ready());
        assert_eq!(
            retry(|| handle.submit_command(cancel(1, market, 1)))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .seq,
            1
        );
        if use_snapshot {
            let image = retry(|| handle.take_snapshot(1))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap();
            let mut writer = matching_runtime::SnapshotWriter::start(
                handle.clone(),
                dir.0.join("snapshots"),
                1024 * 1024,
            )
            .unwrap();
            writer
                .submit(1, image)
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap();
            writer.shutdown().unwrap();
        }
        shard.shutdown(Duration::from_secs(2)).unwrap();
        drop(coordinator);
        let trusted = SnapshotRestoreConfig {
            cluster,
            shard: shard_id,
            primary_epoch: 1,
            routes,
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(config(market).require_route_authority()),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let mut recovered = Shard::recover_group_local_with_genesis(
            dir.0.join("wal"),
            dir.0.join("manifest"),
            dir.0.join("snapshots"),
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (1, 1_000),
        )
        .unwrap();
        let recovered_handle = recovered.handle();
        assert!(!recovered_handle.observation().route_ready);
        assert!(matches!(
            retry(|| recovered_handle.submit_command(cancel(2, market, 1)))
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                matching_domain::RejectReason::EngineBusy
            ))
        ));
        assert_eq!(
            retry(|| recovered_handle.barrier(2))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .1,
            1
        );
        let store = RouteControlStore::open_for_cluster(&control.0, cluster, 8, 8).unwrap();
        let coordinator = RouteCoordinator::new(store, vec![recovered_handle.clone()]).unwrap();
        assert!(recovered_handle.observation().route_ready);
        assert_eq!(
            retry(|| recovered_handle.submit_command(cancel(2, market, 1)))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .seq,
            2
        );
        recovered.shutdown(Duration::from_secs(2)).unwrap();
        drop(coordinator);
    }
}

#[test]
fn two_live_shards_cannot_share_an_empty_wal_directory() {
    let first = t22_support::TestDirectory::new();
    let second = t22_support::TestDirectory::new();
    let routes = RouteTable::new([
        MarketRoute {
            market_id: MarketId(101),
            shard_id: ShardId(102),
            route_epoch: 1,
            state: RouteState::Active,
        },
        MarketRoute {
            market_id: MarketId(103),
            shard_id: ShardId(104),
            route_epoch: 1,
            state: RouteState::Active,
        },
    ])
    .unwrap();
    let start = |id, market, manifest: &std::path::Path| {
        Shard::start_group_place_support(
            id,
            ClusterId(995),
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 1, 8, config(market)),
            ((first.0.join("wal"), manifest.to_path_buf()), (1, 1_000)),
        )
    };
    let mut first_shard = start(ShardId(102), MarketId(101), &first.0.join("manifest")).unwrap();
    let duplicate = start(ShardId(104), MarketId(103), &second.0.join("manifest"));
    assert!(duplicate.is_err());
    first_shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn different_shards_progress_independently_and_recover_their_own_wal() {
    let one = t22_support::TestDirectory::new();
    let two = t22_support::TestDirectory::new();
    let control = t22_support::TestDirectory::new();
    for dir in [&one.0, &two.0] {
        std::fs::create_dir(dir.join("snapshots")).unwrap();
    }
    let cluster = ClusterId(991);
    let ids = [ShardId(11), ShardId(12)];
    let markets = [MarketId(21), MarketId(22)];
    let routes = RouteTable::new((0..2).map(|i| MarketRoute {
        market_id: markets[i],
        shard_id: ids[i],
        route_epoch: 1,
        state: RouteState::Active,
    }))
    .unwrap();
    let start = |i: usize, dir: &std::path::Path, group| {
        Shard::start_group_place_support(
            ids[i],
            cluster,
            routes.clone(),
            vec![(markets[i], QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 1, 8, config(markets[i])),
            ((dir.join("wal"), dir.join("manifest")), group),
        )
        .unwrap()
    };
    let mut slow = start(0, &one.0, (8, 2_000_000));
    let mut fast = start(1, &two.0, (1, 1_000));
    let store =
        RouteControlStore::create_for_cluster(&control.0, cluster, routes.clone(), 8, 8).unwrap();
    let coordinator = RouteCoordinator::new(store, vec![slow.handle(), fast.handle()]).unwrap();
    let stale = retry(|| fast.handle().submit_command(cancel(3, markets[1], 0)))
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        stale,
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::StaleRouteEpoch
        ))
    ));
    let first = retry(|| slow.handle().submit_command(cancel(1, markets[0], 1)));
    let second = retry(|| slow.handle().submit_command(cancel(2, markets[0], 1)));
    let other = retry(|| fast.handle().submit_command(cancel(3, markets[1], 1)));
    assert_eq!(
        other
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert!(matches!(
        first.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        first
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        second
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    let images: Vec<_> = [&slow, &fast]
        .iter()
        .map(|shard| {
            retry(|| shard.handle().take_snapshot(1))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
        })
        .collect();
    for (index, ((shard, dir), image)) in [&slow, &fast]
        .into_iter()
        .zip([&one.0, &two.0])
        .zip(images)
        .enumerate()
    {
        let mut writer = matching_runtime::SnapshotWriter::start(
            shard.handle(),
            dir.join("snapshots"),
            1024 * 1024,
        )
        .unwrap();
        if index == 0 {
            assert!(
                matching_runtime::SnapshotWriter::start(
                    fast.handle(),
                    dir.join("snapshots"),
                    1024 * 1024
                )
                .is_err()
            );
        }
        writer
            .submit(1, image)
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        writer.shutdown().unwrap();
    }
    let slow_suffix = retry(|| slow.handle().submit_command(cancel(4, markets[0], 1)));
    let fast_suffix = retry(|| fast.handle().submit_command(cancel(5, markets[1], 1)));
    assert_eq!(
        fast_suffix
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    assert_eq!(
        slow_suffix
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        3
    );
    let digests: Vec<_> = [&slow, &fast]
        .iter()
        .map(|shard| {
            retry(|| shard.handle().take_snapshot(2))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .state_digest(1024 * 1024)
                .unwrap()
        })
        .collect();
    slow.shutdown(Duration::from_secs(5)).unwrap();
    fast.shutdown(Duration::from_secs(5)).unwrap();
    drop(coordinator);
    assert_eq!(
        matching_persistence::scan_segments(one.0.join("wal"))
            .unwrap()
            .records
            .len(),
        3
    );
    assert_eq!(
        matching_persistence::scan_segments(two.0.join("wal"))
            .unwrap()
            .records
            .len(),
        2
    );
    for (i, dir) in [&one.0, &two.0].iter().enumerate() {
        let trusted = SnapshotRestoreConfig {
            cluster,
            shard: ids[i],
            primary_epoch: 1,
            routes: routes.clone(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(config(markets[i])),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let mut recovered = Shard::recover_group_local_with_genesis(
            dir.join("wal"),
            dir.join("manifest"),
            dir.join("snapshots"),
            &trusted,
            &[(markets[i], QueuePriorityPolicy::try_new(100).unwrap())],
            (1, 1_000),
        )
        .unwrap();
        let snapshot = retry(|| recovered.handle().take_snapshot(2))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.state_digest(1024 * 1024).unwrap(), digests[i]);
        recovered.shutdown(Duration::from_secs(5)).unwrap();
    }
    let first_wal = one.0.join("wal").join("00000000000000000001.wal");
    let mut damaged = std::fs::read(&first_wal).unwrap();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(&first_wal, damaged).unwrap();
    let trusted = |i| SnapshotRestoreConfig {
        cluster,
        shard: ids[i],
        primary_epoch: 1,
        routes: routes.clone(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(config(markets[i])),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    assert!(
        Shard::recover_group_local_with_genesis(
            one.0.join("wal"),
            one.0.join("manifest"),
            one.0.join("snapshots"),
            &trusted(0),
            &[(markets[0], QueuePriorityPolicy::try_new(100).unwrap())],
            (1, 1_000),
        )
        .is_err()
    );
    let mut unaffected = Shard::recover_group_local_with_genesis(
        two.0.join("wal"),
        two.0.join("manifest"),
        two.0.join("snapshots"),
        &trusted(1),
        &[(markets[1], QueuePriorityPolicy::try_new(100).unwrap())],
        (1, 1_000),
    )
    .unwrap();
    let image = retry(|| unaffected.handle().take_snapshot(3))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(image.state_digest(1024 * 1024).unwrap(), digests[1]);
    unaffected.shutdown(Duration::from_secs(5)).unwrap();
}
