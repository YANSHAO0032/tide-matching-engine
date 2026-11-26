//! 核验真实控制存储中的路由持久化和版本竞争。
use matching_control::{EtcdClient, EtcdRouteAuthority};
use matching_domain::{ClusterId, MarketId, ShardId};
use matching_runtime::{MarketRoute, RouteAuthority, RouteState, RouteTable};
use std::time::Duration;

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn startup_with_revoked_authority_closes_shard() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteCoordinator, Shard, ShardError};
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    let key = format!("/tide-test/route-startup-lost/{}", std::process::id());
    let cluster = ClusterId(914);
    let shard_id = ShardId(915);
    let market = MarketId(916);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut authority =
        EtcdRouteAuthority::create(client.clone(), &key, cluster, routes.clone()).unwrap();
    authority.acquire_lease(10).unwrap();
    let owner = client
        .get(format!("{key}/owner").as_bytes())
        .unwrap()
        .unwrap()
        .1;
    let lease = i64::from_be_bytes(owner.try_into().unwrap());
    let endpoint = endpoints.split(',').next().unwrap();
    ureq::post(&format!("{endpoint}/v3/lease/revoke"))
        .set("Content-Type", "application/json")
        .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
        .unwrap();
    let mut shard = Shard::start_with_config(
        shard_id,
        cluster,
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
        matching_runtime::ShardConfig {
            require_route_authority: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(RouteCoordinator::with_authority(authority, vec![shard.handle()]).is_err());
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn monitored_route_lease_remains_exclusive_past_initial_ttl() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteCoordinator, Shard};
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    let key = format!("/tide-test/route-monitor/{}", std::process::id());
    let cluster = ClusterId(911);
    let shard_id = ShardId(912);
    let market = MarketId(913);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut authority =
        EtcdRouteAuthority::create(client.clone(), &key, cluster, routes.clone()).unwrap();
    authority.acquire_lease(2).unwrap();
    let revision = authority.load().unwrap().0;
    assert!(
        authority
            .validate_monitor_interval(Duration::from_secs(2))
            .is_err()
    );
    assert!(
        authority
            .validate_monitor_interval(Duration::from_millis(200))
            .is_ok()
    );
    let mut competitor = EtcdRouteAuthority::open(client.clone(), &key, cluster).unwrap();
    let mut shard = Shard::start_with_config(
        shard_id,
        cluster,
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
        matching_runtime::ShardConfig {
            require_route_authority: true,
            ..Default::default()
        },
    )
    .unwrap();
    let coordinator = RouteCoordinator::with_authority(authority, vec![shard.handle()]).unwrap();
    let before_monitor = matching_domain::LimitOrder {
        order_id: matching_domain::OrderId(1),
        user_id: matching_domain::UserId(1),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: matching_domain::Side::Buy,
        price: matching_domain::Price::try_new(1).unwrap(),
        qty: matching_domain::Qty::try_new(1).unwrap(),
    };
    assert!(matches!(
        shard
            .handle()
            .submit(
                matching_domain::CommandId(1),
                market,
                1,
                matching_runtime::Mutation::Place(before_monitor)
            )
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        Err(matching_domain::RejectReason::EngineBusy)
    ));
    assert_eq!(
        shard
            .handle()
            .barrier(1)
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .1,
        0
    );
    let monitor = coordinator
        .start_monitor(Duration::from_millis(200))
        .unwrap();
    let next = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 2,
        state: RouteState::Active,
    }])
    .unwrap();
    let updated = monitor
        .update(revision, next.clone(), Duration::from_secs(2))
        .unwrap();
    assert!(updated > revision);
    assert_eq!(competitor.read_snapshot().unwrap(), (updated, next));
    let order = matching_domain::LimitOrder {
        order_id: matching_domain::OrderId(1),
        user_id: matching_domain::UserId(1),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: matching_domain::Side::Buy,
        price: matching_domain::Price::try_new(1).unwrap(),
        qty: matching_domain::Qty::try_new(1).unwrap(),
    };
    let handle = shard.handle();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let submit = |epoch| loop {
        match handle.submit(
            matching_domain::CommandId(1),
            market,
            epoch,
            matching_runtime::Mutation::Place(order.clone()),
        ) {
            Ok(reply) => break reply,
            Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(error) => panic!("分片入口失败: {error:?}"),
        }
    };
    assert!(matches!(
        submit(1).recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(matching_domain::RejectReason::StaleRouteEpoch)
    ));
    assert_eq!(
        submit(2)
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    std::thread::sleep(Duration::from_secs(5));
    assert!(shard.handle().observation().ingress_open);
    assert_eq!(
        competitor.acquire_lease(2).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    let owner = client
        .get(format!("{key}/owner").as_bytes())
        .unwrap()
        .unwrap()
        .1;
    let lease = i64::from_be_bytes(owner.try_into().unwrap());
    let endpoint = endpoints.split(',').next().unwrap();
    ureq::post(&format!("{endpoint}/v3/lease/revoke"))
        .set("Content-Type", "application/json")
        .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while shard.handle().observation().ingress_open && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!shard.handle().observation().ingress_open);
    competitor.acquire_lease(2).unwrap();
    competitor.revoke_lease().unwrap();
    drop(monitor);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn route_is_persistent_and_stale_compare_cannot_overwrite() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let key = format!("/tide-test/routes/{}", std::process::id());
    let initial = RouteTable::new([MarketRoute {
        market_id: MarketId(7),
        shard_id: ShardId(3),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut first =
        EtcdRouteAuthority::create(client.clone(), &key, ClusterId(9), initial.clone()).unwrap();
    let mut second = EtcdRouteAuthority::open(client, &key, ClusterId(9)).unwrap();
    first.acquire_lease(10).unwrap();
    assert_eq!(
        second.acquire_lease(10).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    let (revision, loaded) = second.read_snapshot().unwrap();
    assert_eq!(loaded, initial);
    let next = RouteTable::new([MarketRoute {
        market_id: MarketId(7),
        shard_id: ShardId(3),
        route_epoch: 2,
        state: RouteState::Active,
    }])
    .unwrap();
    let newer = first.compare_and_swap(revision, next.clone()).unwrap();
    assert!(newer > revision);
    assert!(first.compare_and_swap(revision, initial).is_err());
    assert_eq!(second.read_snapshot().unwrap(), (newer, next));
    first.revoke_lease().unwrap();
    second.acquire_lease(10).unwrap();
    assert!(
        first
            .compare_and_swap(newer, second.load().unwrap().1)
            .is_err()
    );
    second.revoke_lease().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn expired_route_owner_cannot_write_after_successor_acquires() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let key = format!("/tide-test/route-expiry/{}", std::process::id());
    let initial = RouteTable::new([MarketRoute {
        market_id: MarketId(7),
        shard_id: ShardId(3),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut old =
        EtcdRouteAuthority::create(client.clone(), &key, ClusterId(9), initial.clone()).unwrap();
    let mut next = EtcdRouteAuthority::open(client, &key, ClusterId(9)).unwrap();
    old.acquire_lease(1).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match next.acquire_lease(10) {
            Ok(()) => break,
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100))
            }
            Err(error) => panic!("后继路由租约失败: {error}"),
        }
    }
    let (revision, routes) = next.load().unwrap();
    assert!(old.load().is_err());
    assert!(old.keep_alive().is_err());
    assert!(old.is_locally_invalidated());
    assert!(old.compare_and_swap(revision, routes.clone()).is_err());
    assert_eq!(next.load().unwrap(), (revision, initial));
    assert!(next.compare_and_swap(revision, routes).is_ok());
    next.revoke_lease().unwrap();
}
/// 真实三节点路由权威贯穿冻结、在途旧请求、恢复同次迁移及最终重复确认。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn monitored_migration_persists_both_phases_and_preserves_identity_on_retry() {
    use matching_domain::{AssetId, CommandId, LogicalTime, MarketMath, OrderId, RejectReason};
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
    use matching_runtime::{
        FeedBuilder, IdempotencyConfig, MigrationId, MigrationStorage, Mutation, PlaceLimits,
        PlaceSupportConfig, RouteCoordinator, Router, RouterError, Shard, SourceEvent,
        SourcePayload,
    };
    fn admitted<T>(mut submit: impl FnMut() -> Result<T, RouterError>) -> T {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match submit() {
                Ok(reply) => return reply,
                Err(RouterError::Shard(matching_runtime::ShardError::Busy))
                    if std::time::Instant::now() < deadline =>
                {
                    std::thread::yield_now()
                }
                Err(error) => panic!("分片入口拒绝测试请求 {error:?}"),
            }
        }
    }
    let timeout = Duration::from_secs(10);
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(2),
        true,
    )
    .unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = format!("/tide-test/migration/{}-{stamp}", std::process::id());
    let id = MigrationId {
        cluster: ClusterId(82),
        market: MarketId(7),
        source: ShardId(8),
        target: ShardId(3),
        old_epoch: 18,
    };
    let other = MarketId(19);
    let initial = MarketRoute {
        market_id: id.market,
        shard_id: id.source,
        route_epoch: id.old_epoch,
        state: RouteState::Active,
    };
    let fixed = MarketRoute {
        market_id: other,
        shard_id: id.target,
        route_epoch: 1,
        state: RouteState::Active,
    };
    let table = |route| RouteTable::new([route, fixed]).unwrap();
    let finished = MarketRoute {
        shard_id: id.target,
        route_epoch: 19,
        ..initial
    };
    let mut owner =
        EtcdRouteAuthority::create(client.clone(), &key, id.cluster, table(initial)).unwrap();
    let observer = EtcdRouteAuthority::open(client, &key, id.cluster).unwrap();
    owner.acquire_lease(10).unwrap();
    let revision = owner.load().unwrap().0;
    assert!(owner.compare_and_swap(revision, table(finished)).is_err());
    assert_eq!(
        observer.read_snapshot().unwrap(),
        (revision, table(initial))
    );

    let root = std::env::temp_dir().join(format!(
        "tide-etcd-migration-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut shards = Vec::new();
    for (shard, market) in [(id.source, id.market), (id.target, other)] {
        let dir = root.join(shard.0.to_string());
        for name in ["wal", "manifest", "snapshots"] {
            std::fs::create_dir_all(dir.join(name)).unwrap();
        }
        let policy = LimitReservePolicy::new(
            (AssetId(1), 0),
            (AssetId(2), 0),
            MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            1,
            1,
            0,
        )
        .unwrap();
        let config = PlaceSupportConfig::new(
            RiskKeySet::new(1, []).unwrap(),
            [(market, policy)],
            PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap()
        .require_route_authority();
        shards.push(
            Shard::start_group_place_support(
                shard,
                id.cluster,
                table(initial),
                vec![(market, QueuePriorityPolicy::try_new(10).unwrap())],
                (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
                (IdempotencyConfig::default(), 1, 8, config),
                ((dir.join("wal"), dir.join("manifest")), (1, 1000)),
            )
            .unwrap(),
        );
    }
    std::fs::create_dir(root.join("archive")).unwrap();
    let handles: Vec<_> = shards.iter().map(Shard::handle).collect();
    let router = Router::new(
        table(initial),
        [
            (id.source, handles[0].clone()),
            (id.target, handles[1].clone()),
        ],
    )
    .unwrap();
    let mut monitor = RouteCoordinator::with_authority(owner, handles.clone())
        .unwrap()
        .start_monitor(Duration::from_millis(200))
        .unwrap();
    let storage = MigrationStorage {
        archive: root.join("archive"),
        source_snapshot: (root.join(id.source.0.to_string()).join("snapshots"), 1),
        target_snapshot: (root.join(id.target.0.to_string()).join("snapshots"), 1),
        max_bytes: 1024 * 1024,
        timeout,
    };
    let feed = FeedBuilder::new(id.market);
    let mut wrong = feed.clone();
    wrong
        .apply(&SourceEvent {
            market_id: id.market,
            sequence: 1,
            payload: SourcePayload::Boundary,
        })
        .unwrap();
    assert!(
        storage
            .migrate_monitored(&mut monitor, id, Some(&wrong))
            .is_err()
    );
    assert_eq!(
        observer.read_snapshot().unwrap(),
        (revision, table(initial))
    );
    // 制造归档临时文件写入故障，以便从独立权威读取冻结后的持久路由。
    let blocked_file = storage.archive.join(format!(
        "market-{}-{}-{}-{}-{}/snapshot-1.migration.tmp",
        id.cluster.0, id.market.0, id.source.0, id.target.0, id.old_epoch
    ));
    std::fs::create_dir_all(&blocked_file).unwrap();
    assert!(
        storage
            .migrate_monitored(&mut monitor, id, Some(&feed))
            .is_err()
    );
    let (frozen_rev, frozen) = observer.read_snapshot().unwrap();
    assert!(frozen_rev > revision);
    assert_eq!(
        frozen,
        table(MarketRoute {
            route_epoch: 19,
            state: RouteState::Migrating {
                source: id.source,
                target: id.target
            },
            ..initial
        })
    );
    for handle in &handles {
        for (epoch, reason) in [
            (18, RejectReason::StaleRouteEpoch),
            (19, RejectReason::MigrationInProgress),
        ] {
            assert!(
                matches!(admitted(|| handle.submit(CommandId(2),id.market,epoch,Mutation::Cancel(OrderId(2))).map_err(RouterError::Shard)).recv_timeout(timeout).unwrap(),Err(r) if r==reason)
            );
        }
        assert_eq!(
            admitted(|| handle.barrier(1).map_err(RouterError::Shard))
                .recv_timeout(timeout)
                .unwrap()
                .1,
            0
        );
    }
    assert!(
        monitor
            .update(frozen_rev, table(finished), timeout)
            .is_err()
    );
    assert_eq!(observer.read_snapshot().unwrap(), (frozen_rev, frozen));
    std::fs::remove_dir(blocked_file).unwrap();
    let result = storage
        .migrate_monitored(&mut monitor, id, Some(&feed))
        .unwrap();
    assert_eq!(
        result.boundary,
        feed.migration_boundary(id.source, id.target, 19)
    );
    let confirmed = observer.read_snapshot().unwrap();
    assert!(confirmed.0 > frozen_rev);
    assert_eq!(confirmed.1, table(finished));
    assert_eq!(
        storage
            .migrate_monitored(&mut monitor, id, None)
            .unwrap()
            .boundary,
        result.boundary
    );
    assert_eq!(observer.read_snapshot().unwrap(), confirmed);
    assert!(matches!(
        router.submit(CommandId(2), id.market, 18, Mutation::Cancel(OrderId(2))),
        Err(RouterError::Rejected(RejectReason::StaleRouteEpoch))
    ));
    // 低层命令到达新所有者后，由正式资金模式拒绝；若仍投递到旧分片则为过期路由。
    assert!(matches!(
        admitted(|| router.submit(CommandId(2), id.market, 19, Mutation::Cancel(OrderId(2))))
            .recv_timeout(timeout)
            .unwrap(),
        Err(RejectReason::InvalidOrderCombination)
    ));
    drop(monitor);
    for shard in &mut shards {
        shard.shutdown(timeout).unwrap();
    }
    drop(shards);
    let directory = root.canonicalize().unwrap();
    assert_eq!(
        directory.parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    std::fs::remove_dir_all(directory).unwrap();
}
