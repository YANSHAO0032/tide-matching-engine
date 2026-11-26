//! 验证源、目标分片切换时的路由纪元、同价队列、行情连续性与持久回放。
use matching_domain::{ClusterId, MarketId, ShardId};
use matching_runtime::MigrationId;

#[test]
fn migration_epoch_and_distinct_owners_are_checked_before_io() {
    let id = MigrationId {
        cluster: ClusterId(1),
        market: MarketId(2),
        source: ShardId(3),
        target: ShardId(4),
        old_epoch: 7,
    };
    assert_eq!(id.new_epoch().unwrap(), 8);
    assert!(
        MigrationId {
            target: id.source,
            ..id
        }
        .new_epoch()
        .is_err()
    );
    assert!(
        MigrationId {
            old_epoch: u64::MAX,
            ..id
        }
        .new_epoch()
        .is_err()
    );
}

mod t22_support;

use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, Command, CommandEnvelope, CommandId, EngineEvent, LimitOrder,
    LogicalTime, MarketMath, MassCancelShard, OrderId, PlaceLimitOrder, Price, Qty, RejectReason,
    ReservationId, ReservationToken, ShardCancelScope, Side, StpKey, StpMode, UserId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_persistence::{FileManifestStorage, ManifestManager};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    EventHasher, FeedBuilder, FormalError, FormalExecution, FormalOutcome, IdempotencyConfig,
    MarketRoute, MigrationStorage, PlaceLimits, PlaceSupportConfig, RouteControlStore,
    RouteCoordinator, RouteState, RouteTable, Shard, ShardError, SnapshotImage,
    SnapshotRestoreConfig, recover_snapshot_records_with_genesis,
};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};
use t22_support::TestDirectory;

const WAIT: Duration = Duration::from_secs(10);
const LIMIT: usize = 4 * 1024 * 1024;
const MOVE: MarketId = MarketId(1);
const STAY: MarketId = MarketId(2);
const OTHER: MarketId = MarketId(3);
const A: ShardId = ShardId(11);
const B: ShardId = ShardId(12);
const CLUSTER: ClusterId = ClusterId(1035);

fn retry<T>(mut submit: impl FnMut() -> Result<T, ShardError>) -> T {
    let end = Instant::now() + WAIT;
    loop {
        match submit() {
            Ok(reply) => return reply,
            Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
            Err(e) => panic!("分片入队失败 {e:?}"),
        }
    }
}

fn config(markets: &[MarketId]) -> PlaceSupportConfig {
    let keys = RiskKeySet::new(1, [(1, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    PlaceSupportConfig::new(
        keys,
        markets.iter().map(|&m| (m, policy)),
        PlaceLimits {
            active_orders: 32,
            terminal_history: 32,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .require_route_authority()
}

fn policies(markets: &[MarketId]) -> Vec<(MarketId, QueuePriorityPolicy)> {
    markets
        .iter()
        .map(|&m| (m, QueuePriorityPolicy::try_new(100).unwrap()))
        .collect()
}

fn start(
    cluster: ClusterId,
    shard: ShardId,
    root: &Path,
    markets: &[MarketId],
    routes: &RouteTable,
) -> Shard {
    fs::create_dir(root.join("snapshots")).unwrap();
    Shard::start_group_place_support(
        shard,
        cluster,
        routes.clone(),
        policies(markets),
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 1, 16, config(markets)),
        ((root.join("wal"), root.join("manifest")), (1, 1000)),
    )
    .unwrap()
}

fn restore(shard: ShardId, markets: &[MarketId], routes: &RouteTable) -> SnapshotRestoreConfig {
    SnapshotRestoreConfig {
        cluster: CLUSTER,
        shard,
        primary_epoch: 1,
        routes: routes.clone(),
        user_capacity: 16,
        idempotency: IdempotencyConfig::default(),
        place: Some(config(markets)),
        expect_registry: true,
        max_markets: 3,
        max_bytes: LIMIT,
    }
}

fn place(n: u64, side: Side, qty: u64, epoch: u64) -> CommandEnvelope {
    let cid = CommandId(u128::from(n));
    let uid = UserId(n);
    let oid = OrderId(u128::from(n));
    let (asset, atoms) = match side {
        Side::Buy => (AssetId(2), qty.checked_mul(10).unwrap()),
        Side::Sell => (AssetId(1), qty),
    };
    let mut token = ReservationToken {
        token_id: cid.0,
        command_id: cid,
        reservation_id: ReservationId(cid.0),
        order_id: oid,
        user_id: uid,
        market_id: MOVE,
        reserves: vec![AssetReserve {
            asset,
            atoms: u128::from(atoms),
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    token.signature = SigningKey::from_bytes(&[7; 32])
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    CommandEnvelope {
        command_id: cid,
        session_id: n,
        client_seq: 1,
        market_id: Some(MOVE),
        user_id: Some(uid),
        route_epoch: Some(epoch),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: oid,
                user_id: uid,
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            },
            client_order_id: Some(format!("id-{n}")),
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    }
}

fn run(shard: &Shard, command: &CommandEnvelope) -> Result<FormalExecution, FormalError> {
    retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(WAIT)
        .unwrap()
}
fn snap(shard: &Shard) -> SnapshotImage {
    retry(|| shard.handle().take_snapshot(33))
        .recv_timeout(WAIT)
        .unwrap()
        .unwrap()
}
fn record(execution: &FormalExecution, hash: &mut EventHasher, feed: &mut FeedBuilder) {
    for event in &execution.events {
        hash.update(event).unwrap();
    }
    for event in execution
        .market_data_source
        .get(&MOVE)
        .into_iter()
        .flatten()
    {
        feed.apply(event).unwrap();
    }
}

#[test]
fn durable_handoff_preserves_fifo_feed_and_replayed_events_across_owner_change() {
    let first = TestDirectory::new();
    let second = TestDirectory::new();
    let control = TestDirectory::new();
    let archive = TestDirectory::new();
    let routes = RouteTable::new([(MOVE, A), (STAY, A), (OTHER, B)].map(
        |(market_id, shard_id)| MarketRoute {
            market_id,
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        },
    ))
    .unwrap();
    let mut a = start(CLUSTER, A, &first.0, &[MOVE, STAY], &routes);
    let mut b = start(CLUSTER, B, &second.0, &[OTHER], &routes);
    let store =
        RouteControlStore::create_for_cluster(&control.0, CLUSTER, routes.clone(), 8, 16).unwrap();
    let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
    let mut before_hash = EventHasher::new();
    let mut feed = FeedBuilder::new(MOVE);
    // 源的业务序号推进至远高于目标；无行情的其他市场命令必须保留在源分片。
    for n in 100..164 {
        let command = CommandEnvelope {
            command_id: CommandId(n),
            session_id: u64::try_from(n).unwrap(),
            client_seq: 1,
            market_id: Some(STAY),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: STAY },
            }),
        };
        record(&run(&a, &command).unwrap(), &mut before_hash, &mut feed);
    }
    // 先产生一笔真实成交，保证迁移的是非零成交流水位。
    for (n, side) in [
        (70, Side::Sell),
        (71, Side::Buy),
        (89, Side::Buy),
        (82, Side::Buy),
    ] {
        let exec = run(&a, &place(n, side, 1, 1)).unwrap();
        assert_eq!(exec.outcome, FormalOutcome::Placed(OrderId(u128::from(n))));
        record(&exec, &mut before_hash, &mut feed);
    }
    let frozen = snap(&a);
    assert_eq!(frozen.snapshot_seq(), 68);
    assert_eq!(snap(&b).snapshot_seq(), 0);
    let before = frozen.market_state_digest(MOVE, LIMIT).unwrap();
    let priorities = frozen.next_priorities(MOVE).unwrap();
    assert!(feed.watermarks().1.trade > 0);

    let id = MigrationId {
        cluster: CLUSTER,
        market: MOVE,
        source: A,
        target: B,
        old_epoch: 1,
    };
    let storage = MigrationStorage {
        archive: archive.0.clone(),
        source_snapshot: (first.0.join("snapshots"), 801),
        target_snapshot: (second.0.join("snapshots"), 802),
        max_bytes: LIMIT,
        timeout: WAIT,
    };
    // 行情构建器滞后时，保留活动路由和双方的业务水位。
    assert!(
        storage
            .migrate(&mut coordinator, id, Some(&FeedBuilder::new(MOVE)))
            .is_err()
    );
    assert_eq!(coordinator.verify_authority().unwrap(), 1);
    assert!(run(&a, &place(90, Side::Buy, 1, 2)).is_err());
    assert_eq!(snap(&a).snapshot_seq(), 68);
    // 持有源快照写锁，迫使流程在目标快照已登记、源快照尚未安装时中断。
    let writer_lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(storage.source_snapshot.0.join("snapshot.lock"))
        .unwrap();
    writer_lock.try_lock().unwrap();
    assert!(storage.migrate(&mut coordinator, id, Some(&feed)).is_err());
    assert!(storage.target_snapshot.0.join("snapshot-802.bin").is_file());
    assert!(!storage.source_snapshot.0.join("snapshot-801.bin").exists());
    assert!(storage.read_boundary(id).is_err());
    drop(writer_lock);
    a.shutdown(WAIT).unwrap();
    b.shutdown(WAIT).unwrap();
    drop(coordinator);
    let end = Instant::now() + WAIT;
    let store = loop {
        match RouteControlStore::open_for_cluster(&control.0, CLUSTER, 8, 16) {
            Ok(store) => break store,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < end => {
                std::thread::yield_now()
            }
            Err(e) => panic!("{e}"),
        }
    };
    let migrating = store.read().unwrap().1.clone();
    assert_eq!(
        migrating.get(MOVE).unwrap().state,
        RouteState::Migrating {
            source: A,
            target: B
        }
    );
    let src_cfg = restore(A, &[STAY, MOVE], &migrating);
    let dst_cfg = restore(B, &[OTHER, MOVE], &migrating);
    {
        let manager =
            ManifestManager::<FileManifestStorage>::open_recovery(first.0.join("manifest"))
                .unwrap();
        let replay = recover_snapshot_records_with_genesis(
            first.0.join("wal"),
            &storage.source_snapshot.0,
            &manager,
            &src_cfg,
            &policies(&[MOVE, STAY]),
        )
        .unwrap()
        .unwrap()
        .replay()
        .unwrap();
        assert_eq!(replay.event_hash, before_hash.finish());
        assert_eq!(
            replay.image.market_state_digest(MOVE, LIMIT).unwrap(),
            before
        );
    }
    let mut a = Shard::recover_group_local_with_genesis(
        first.0.join("wal"),
        first.0.join("manifest"),
        &storage.source_snapshot.0,
        &src_cfg,
        &policies(&[MOVE, STAY]),
        (1, 1000),
    )
    .unwrap();
    let mut b = Shard::recover_group_local(
        second.0.join("wal"),
        second.0.join("manifest"),
        &storage.target_snapshot.0,
        &dst_cfg,
        (1, 1000),
    )
    .unwrap();
    let mut coordinator = RouteCoordinator::new(store, vec![a.handle(), b.handle()]).unwrap();
    let result = storage.migrate(&mut coordinator, id, None).unwrap();
    assert_eq!(result.market_digest, before);
    assert_eq!(storage.read_boundary(id).unwrap(), result.boundary);
    assert_eq!(result.boundary, feed.migration_boundary(A, B, 2));
    assert_eq!(result.builder.digest(), feed.digest());
    let again = storage.migrate(&mut coordinator, id, None).unwrap();
    assert_eq!(again.boundary, result.boundary);
    let installed = snap(&b);
    assert_eq!(installed.market_state_digest(MOVE, LIMIT).unwrap(), before);
    assert_eq!(installed.next_priorities(MOVE).unwrap(), priorities);
    assert!(snap(&a).order(MOVE, OrderId(89)).is_err());
    assert!(installed.reservation(ReservationId(89)).is_some());
    assert_eq!(
        installed.client_order(UserId(89), "id-89"),
        Some((MOVE, OrderId(89)))
    );
    for shard in [&a, &b] {
        assert!(matches!(
            run(shard, &place(82, Side::Buy, 1, 1)),
            Err(FormalError::Rejected(RejectReason::StaleRouteEpoch))
        ));
    }
    let retried = run(&b, &place(82, Side::Buy, 1, 2)).unwrap();
    assert!(retried.deduplicated);
    assert_eq!(retried.seq, 68);
    let mut target_feed = result.builder;
    let mut after_hash = EventHasher::new();
    let new = run(&b, &place(6, Side::Buy, 1, 2)).unwrap();
    assert_eq!(new.seq, 1);
    assert_eq!(
        new.market_data_source[&MOVE][0].sequence,
        feed.watermarks().0.checked_add(1).unwrap()
    );
    record(&new, &mut after_hash, &mut target_feed);
    assert_eq!(
        snap(&b)
            .order(MOVE, OrderId(6))
            .unwrap()
            .unwrap()
            .priority()
            .get(),
        priorities.0
    );
    let trade = run(&b, &place(7, Side::Sell, 3, 2)).unwrap();
    let makers: Vec<_> = trade
        .events
        .iter()
        .filter_map(|e| match &e.payload {
            EngineEvent::Trade(t) => Some(t.maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(makers, [OrderId(89), OrderId(82), OrderId(6)]);
    record(&trade, &mut after_hash, &mut target_feed);
    assert_eq!(
        target_feed.watermarks().1.trade,
        feed.watermarks().1.trade.checked_add(3).unwrap()
    );
    let last = snap(&b);
    let digest = last.state_digest(LIMIT).unwrap();
    let final_routes = last.routes().clone();
    // 即使正常路由随后又推进了纪元，也能用原身份补记遗漏的管理边界。
    let advanced = RouteTable::new(final_routes.routes().map(|r| {
        if r.market_id == MOVE {
            MarketRoute {
                route_epoch: 3,
                ..*r
            }
        } else {
            *r
        }
    }))
    .unwrap();
    coordinator.update(3, advanced, WAIT).unwrap();
    let historic = storage.migrate(&mut coordinator, id, None).unwrap();
    assert_eq!(historic.boundary, result.boundary);
    assert_eq!(historic.builder.digest(), feed.digest());
    a.shutdown(WAIT).unwrap();
    b.shutdown(WAIT).unwrap();
    drop(coordinator);
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(second.0.join("manifest")).unwrap();
    let replay = recover_snapshot_records_with_genesis(
        second.0.join("wal"),
        &storage.target_snapshot.0,
        &manager,
        &restore(B, &[OTHER, MOVE], &final_routes),
        &[],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.image.state_digest(LIMIT).unwrap(), digest);
    assert_eq!(replay.event_hash, after_hash.finish());
    let mut replay_feed = again.builder;
    for execution in replay.executions {
        for event in execution
            .market_data_source
            .get(&MOVE)
            .into_iter()
            .flatten()
        {
            replay_feed.apply(event).unwrap();
        }
    }
    assert_eq!(replay_feed.digest(), target_feed.digest());
}

#[path = "t35_migration/interrupted.rs"]
mod interrupted;
