use matching_domain::{ClusterId, MarketId, ShardId};
use matching_runtime::{MarketRoute, RouteControlStore, RouteState, RouteTable};

fn reopen_after_drain(
    dir: &std::path::Path,
    cluster: ClusterId,
    max_routes: usize,
    max_records: u64,
) -> RouteControlStore {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match RouteControlStore::open_for_cluster(dir, cluster, max_routes, max_records) {
            Ok(store) => return store,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::yield_now()
            }
            Err(error) => panic!("无法恢复控制权威: {error}"),
        }
    }
}

#[test]
fn route_store_rejects_wrong_cluster_identity() {
    use matching_domain::ClusterId;
    use matching_runtime::{RouteCoordinator, Shard};
    let dir = std::env::temp_dir().join(format!(
        "tide-route-cluster-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let table = RouteTable::new([]).unwrap();
    drop(RouteControlStore::create_for_cluster(&dir, ClusterId(4), table.clone(), 8, 8).unwrap());
    assert!(RouteControlStore::open_for_cluster(&dir, ClusterId(5), 8, 8).is_err());
    assert_eq!(
        RouteControlStore::open_for_cluster(&dir, ClusterId(4), 8, 8)
            .unwrap()
            .read()
            .unwrap(),
        (1, &table)
    );
    let path = dir.join("routes.log");
    let original = std::fs::read(&path).unwrap();
    let mut damaged = original.clone();
    damaged[5] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    assert!(RouteControlStore::open_for_cluster(&dir, ClusterId(5), 8, 8).is_err());
    std::fs::write(&path, original).unwrap();
    let store = RouteControlStore::open_for_cluster(&dir, ClusterId(4), 8, 8).unwrap();
    let mut shard = Shard::start(ShardId(9), ClusterId(5), table, vec![]).unwrap();
    assert!(RouteCoordinator::new(store, vec![shard.handle()]).is_err());
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn coordinator_keeps_authority_until_accepted_commands_drain() {
    use matching_domain::{
        ApplyUserControlFenceShard, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime,
        UserFenceAction, UserId,
    };
    use matching_risk::ReservationRegistry;
    use matching_runtime::{IdempotencyConfig, RouteCoordinator, Shard, ShardError};
    use std::time::{Duration, Instant};
    let dir = std::env::temp_dir().join(format!(
        "tide-route-drain-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    for part in ["control", "wal", "manifest"] {
        std::fs::create_dir(dir.join(part)).unwrap();
    }
    let table = RouteTable::new([]).unwrap();
    let store =
        RouteControlStore::create_for_cluster(dir.join("control"), ClusterId(993), table, 4, 4)
            .unwrap();
    let mut shard = Shard::start_group_fence_only(
        ShardId(8),
        ClusterId(993),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 8),
        (dir.join("wal"), dir.join("manifest")),
        (8, 2_000_000),
    )
    .unwrap();
    let handle = shard.handle();
    let coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(1)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(1),
            user_id: UserId(1),
            new_epoch: 1,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 1,
        }),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let reply = loop {
        match handle.submit_command(command.clone()) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    drop(coordinator);
    match RouteControlStore::open_for_cluster(dir.join("control"), ClusterId(993), 4, 4) {
        Ok(store) => {
            assert!(
                reply
                    .try_recv()
                    .expect("权威释放前必须完成已接收命令")
                    .is_ok()
            );
            drop(store);
        }
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        }
    }
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let reopened = reopen_after_drain(&dir.join("control"), ClusterId(993), 4, 4);
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn authority_watch_detects_out_of_band_revision_and_closes_ingress() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::sync::{Arc, Mutex};
    struct Authority(/** 共享的路由版本和快照。 */ Arc<Mutex<(u64, RouteTable)>>);
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(997)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let table = RouteTable::new([MarketRoute {
        market_id: MarketId(91),
        shard_id: ShardId(92),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = Arc::new(Mutex::new((1, table.clone())));
    let mut shard = Shard::start(
        ShardId(92),
        ClusterId(997),
        table,
        vec![(
            MarketId(91),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let mut coordinator =
        RouteCoordinator::with_authority(Authority(state.clone()), vec![shard.handle()]).unwrap();
    state.lock().unwrap().0 = 2;
    assert!(coordinator.verify_authority().is_err());
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
    drop(coordinator);
}

#[test]
fn coordinator_startup_fences_if_authority_cannot_renew() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    struct Authority(/** 模拟权威返回的路由快照。 */ RouteTable);
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(970)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.0.clone()))
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("续约失败"))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(970),
        routes.clone(),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    assert!(RouteCoordinator::with_authority(Authority(routes), vec![shard.handle()]).is_err());
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn coordinator_startup_fences_if_authority_changes_during_validation() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Authority {
        /// 测试权威返回或保存的路由快照。
        routes: RouteTable,
        /// 用于控制读取结果的调用次数。
        reads: AtomicUsize,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(969)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            let revision = if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                1
            } else {
                2
            };
            Ok((revision, self.routes.clone()))
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(969),
        routes.clone(),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    assert!(
        RouteCoordinator::with_authority(
            Authority {
                routes,
                reads: AtomicUsize::new(0),
            },
            vec![shard.handle()],
        )
        .is_err()
    );
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn leased_authority_rejects_shard_started_without_route_gate() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    struct Authority(/** 模拟权威返回的路由快照。 */ RouteTable);
    impl RouteAuthority for Authority {
        fn cluster_id(&self) -> ClusterId {
            ClusterId(964)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.0.clone()))
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(964),
        routes.clone(),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    assert!(RouteCoordinator::with_authority(Authority(routes), vec![shard.handle()]).is_err());
    assert!(!shard.handle().observation().ingress_open);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn leased_authority_waits_for_monitor_before_admitting_mutations() {
    use matching_domain::{
        CommandId, LimitOrder, OrderId, Price, Qty, RejectReason, Side, StpKey, StpMode, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{Mutation, RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::time::{Duration, Instant};
    struct Authority(/** 模拟权威返回的路由快照。 */ RouteTable);
    impl RouteAuthority for Authority {
        fn cluster_id(&self) -> ClusterId {
            ClusterId(968)
        }
        fn validate_monitor_interval(&self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.0.clone()))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start_with_config(
        ShardId(82),
        ClusterId(968),
        routes.clone(),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
        matching_runtime::ShardConfig {
            require_route_authority: true,
            ..Default::default()
        },
    )
    .unwrap();
    let handle = shard.handle();
    let mut coordinator =
        RouteCoordinator::with_authority(Authority(routes.clone()), vec![handle.clone()]).unwrap();
    assert!(
        coordinator
            .update(1, routes, Duration::from_secs(2))
            .is_err()
    );
    let order = LimitOrder {
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(1).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let submit = || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let receiver = loop {
            match handle.submit(
                CommandId(1),
                MarketId(81),
                1,
                Mutation::Place(order.clone()),
            ) {
                Ok(receiver) => break receiver,
                Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("变更入队失败：{error:?}"),
            }
        };
        receiver.recv_timeout(Duration::from_secs(2)).unwrap()
    };
    assert!(matches!(submit(), Err(RejectReason::EngineBusy)));
    let deadline = Instant::now() + Duration::from_secs(2);
    let barrier = loop {
        match handle.barrier(1) {
            Ok(receiver) => break receiver,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("屏障入队失败：{error:?}"),
        }
    };
    assert_eq!(barrier.recv_timeout(Duration::from_secs(2)).unwrap().1, 0);
    let monitor = coordinator.start_monitor(Duration::from_secs(1)).unwrap();
    assert_eq!(submit().unwrap().seq, 1);
    drop(monitor);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn coordinator_startup_pauses_mutation_during_authority_read() {
    use matching_domain::{
        CommandId, LimitOrder, OrderId, Price, Qty, RejectReason, Side, StpKey, StpMode, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{Mutation, RouteAuthority, RouteCoordinator, Shard};
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;
    struct Authority {
        /// 测试权威返回或保存的路由快照。
        routes: RouteTable,
        /// 通知测试线程操作已进入等待位置。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 控制被阻塞操作继续执行。
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(971)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            if let Some(release) = self.release.lock().unwrap().take() {
                self.entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(3)).unwrap();
            }
            Ok((1, self.routes.clone()))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(971),
        routes.clone(),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
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
    assert!(health.ready());
    let (entered_tx, entered_rx) = sync_channel(1);
    let (release_tx, release_rx) = sync_channel(1);
    let worker_handle = handle.clone();
    let starter = std::thread::spawn(move || {
        RouteCoordinator::with_authority(
            Authority {
                routes,
                entered: entered_tx,
                release: std::sync::Mutex::new(Some(release_rx)),
            },
            vec![worker_handle],
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(!health.ready());
    let order = LimitOrder {
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(1).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let result = handle
        .submit(CommandId(1), MarketId(81), 1, Mutation::Place(order))
        .unwrap()
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    release_tx.send(()).unwrap();
    let coordinator = starter.join().unwrap().unwrap();
    assert!(health.ready());
    assert!(matches!(result, Err(RejectReason::EngineBusy)));
    assert_eq!(
        handle
            .barrier(1)
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .1,
        0
    );
    drop(coordinator);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn failed_startup_retains_authority_until_accepted_work_finishes() {
    use matching_domain::{
        ApplyUserControlFenceShard, Command, CommandEnvelope, CommandId, LogicalTime,
        UserFenceAction, UserId,
    };
    use matching_risk::ReservationRegistry;
    use matching_runtime::{
        IdempotencyConfig, RouteAuthority, RouteCoordinator, Shard, ShardError,
    };
    use std::time::{Duration, Instant};
    struct Failing(/** 用于模拟错误的路由存储。 */ RouteControlStore);
    impl RouteAuthority for Failing {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(974)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Err(std::io::Error::other("读取失败"))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    impl Drop for Failing {
        fn drop(&mut self) {
            let _ = &self.0;
        }
    }
    let dir = std::env::temp_dir().join(format!(
        "tide-startup-drain-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    for part in ["control", "wal", "manifest"] {
        std::fs::create_dir(dir.join(part)).unwrap();
    }
    let store = RouteControlStore::create_for_cluster(
        dir.join("control"),
        ClusterId(974),
        RouteTable::new([]).unwrap(),
        4,
        4,
    )
    .unwrap();
    let mut shard = Shard::start_group_fence_only(
        ShardId(8),
        ClusterId(974),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 8),
        (dir.join("wal"), dir.join("manifest")),
        (8, 2_000_000),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(1)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(1),
            user_id: UserId(1),
            new_epoch: 1,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 1,
        }),
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    let reply = loop {
        match shard.handle().submit_command(command.clone()) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    assert!(RouteCoordinator::with_authority(Failing(store), vec![shard.handle()]).is_err());
    match RouteControlStore::open_for_cluster(dir.join("control"), ClusterId(974), 4, 4) {
        Ok(store) => {
            assert!(reply.try_recv().is_ok(), "已释放权威但命令仍在处理");
            drop(store);
        }
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            let _ = reply.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }
    shard.shutdown(Duration::from_secs(5)).unwrap();
    drop(reopen_after_drain(
        &dir.join("control"),
        ClusterId(974),
        4,
        4,
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_initial_authority_read_fences_unowned_shard() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    struct Authority;
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(976)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Err(std::io::Error::other("存储不可用"))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(81),
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(976),
        routes,
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    assert!(RouteCoordinator::with_authority(Authority, vec![shard.handle()]).is_err());
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn timed_out_monitored_update_can_commit_and_must_fail_closed() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    use std::sync::{Arc, Mutex, mpsc::sync_channel};
    use std::time::Duration;
    struct Authority {
        /// 共享的路由版本和快照。
        state: Arc<Mutex<(u64, RouteTable)>>,
        /// 通知测试线程操作已进入等待位置。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 控制被阻塞操作继续执行。
        release: std::sync::mpsc::Receiver<()>,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(981)
        }
        fn validate_monitor_interval(&self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok(self.state.lock().unwrap().clone())
        }
        fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> std::io::Result<u64> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(3)).unwrap();
            let mut state = self.state.lock().unwrap();
            assert_eq!(state.0, expected);
            *state = (expected + 1, routes);
            Ok(state.0)
        }
    }
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let state = Arc::new(Mutex::new((1, routes(1))));
    let (entered_tx, entered_rx) = sync_channel(1);
    let (release_tx, release_rx) = sync_channel(1);
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(981),
        routes(1),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let monitor = RouteCoordinator::with_authority(
        Authority {
            state: state.clone(),
            entered: entered_tx,
            release: release_rx,
        },
        vec![shard.handle()],
    )
    .unwrap()
    .start_monitor(Duration::from_secs(10))
    .unwrap();
    std::thread::scope(|scope| {
        let update = scope.spawn(|| monitor.update(1, routes(2), Duration::from_millis(100)));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            update.join().unwrap().unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );
        release_tx.send(()).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while shard.handle().observation().ingress_open && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!shard.handle().observation().ingress_open);
    assert_eq!(*state.lock().unwrap(), (2, routes(2)));
    drop(monitor);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn direct_update_does_not_resume_after_renewal_failure() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    struct Authority(
        /** 共享路由版本及快照。 */ std::sync::Mutex<(u64, RouteTable)>,
        /** 续约调用次数。 */ usize,
    );
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(980)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            self.1 += 1;
            if self.1 == 1 {
                Ok(())
            } else {
                Err(std::io::Error::other("租约失效"))
            }
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> std::io::Result<u64> {
            let mut state = self.0.lock().unwrap();
            assert_eq!(state.0, expected);
            *state = (expected + 1, routes);
            Ok(state.0)
        }
    }
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(980),
        routes(1),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let mut coordinator = RouteCoordinator::with_authority(
        Authority(std::sync::Mutex::new((1, routes(1))), 0),
        vec![shard.handle()],
    )
    .unwrap();
    assert!(
        coordinator
            .update(1, routes(2), std::time::Duration::from_secs(2))
            .is_err()
    );
    assert!(!shard.handle().observation().ingress_open);
    drop(coordinator);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn monitored_update_does_not_resume_after_renewal_failure() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    struct Authority {
        /// 共享的路由版本和快照。
        state: std::sync::Arc<std::sync::Mutex<(u64, RouteTable)>>,
        /// 记录或控制租约续约调用次数。
        renewals: usize,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(982)
        }
        fn validate_monitor_interval(&self, _: std::time::Duration) -> std::io::Result<()> {
            Ok(())
        }
        fn renew(&mut self) -> std::io::Result<()> {
            self.renewals += 1;
            if self.renewals <= 2 {
                Ok(())
            } else {
                Err(std::io::Error::other("租约失效"))
            }
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok(self.state.lock().unwrap().clone())
        }
        fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> std::io::Result<u64> {
            let mut state = self.state.lock().unwrap();
            assert_eq!(state.0, expected);
            *state = (expected + 1, routes);
            Ok(state.0)
        }
    }
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let state = std::sync::Arc::new(std::sync::Mutex::new((1, routes(1))));
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(982),
        routes(1),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let monitor =
        RouteCoordinator::with_authority(Authority { state, renewals: 0 }, vec![shard.handle()])
            .unwrap()
            .start_monitor(std::time::Duration::from_secs(10))
            .unwrap();
    assert!(
        monitor
            .update(1, routes(2), std::time::Duration::from_secs(2))
            .is_err()
    );
    assert!(!shard.handle().observation().ingress_open);
    drop(monitor);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn monitored_coordinator_can_update_routes_without_stopping_monitor() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteCoordinator, Shard};
    let dir = std::env::temp_dir().join(format!(
        "tide-monitor-update-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let store =
        RouteControlStore::create_for_cluster(&dir, ClusterId(983), routes(1), 4, 4).unwrap();
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(983),
        routes(1),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let monitor = RouteCoordinator::new(store, vec![shard.handle()])
        .unwrap()
        .start_monitor(std::time::Duration::from_millis(20))
        .unwrap();
    assert_eq!(
        monitor
            .update(1, routes(2), std::time::Duration::from_secs(2))
            .unwrap(),
        2
    );
    assert!(shard.handle().observation().ingress_open);
    drop(monitor);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
    let reopened = reopen_after_drain(&dir, ClusterId(983), 4, 4);
    assert_eq!(reopened.read().unwrap(), (2, &routes(2)));
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn monitor_start_fails_closed_if_initial_renewal_fails() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    struct Authority(
        /** 模拟权威的路由快照。 */ RouteTable,
        /** 续约调用次数。 */ usize,
    );
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(984)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.0.clone()))
        }
        fn validate_monitor_interval(&self, _: std::time::Duration) -> std::io::Result<()> {
            Ok(())
        }
        fn renew(&mut self) -> std::io::Result<()> {
            self.1 += 1;
            if self.1 == 1 {
                Ok(())
            } else {
                Err(std::io::Error::other("续约失败"))
            }
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let table = RouteTable::new([MarketRoute {
        market_id: MarketId(91),
        shard_id: ShardId(92),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(92),
        ClusterId(984),
        table.clone(),
        vec![(
            MarketId(91),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let coordinator =
        RouteCoordinator::with_authority(Authority(table, 0), vec![shard.handle()]).unwrap();
    assert!(
        coordinator
            .start_monitor(std::time::Duration::from_secs(1))
            .is_err()
    );
    assert!(!shard.handle().observation().ingress_open);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn background_monitor_closes_ingress_on_authority_loss() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Authority {
        /// 控制模拟权威是否仍然有效。
        alive: Arc<AtomicBool>,
        /// 模拟权威当前返回的路由表。
        table: RouteTable,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(995)
        }
        fn validate_monitor_interval(&self, _: std::time::Duration) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.table.clone()))
        }
        fn renew(&mut self) -> std::io::Result<()> {
            if self.alive.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(std::io::Error::other("续约失败"))
            }
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let table = RouteTable::new([MarketRoute {
        market_id: MarketId(91),
        shard_id: ShardId(92),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let alive = Arc::new(AtomicBool::new(true));
    let mut shard = Shard::start(
        ShardId(92),
        ClusterId(995),
        table.clone(),
        vec![(
            MarketId(91),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let coordinator = RouteCoordinator::with_authority(
        Authority {
            alive: alive.clone(),
            table,
        },
        vec![shard.handle()],
    )
    .unwrap();
    let monitor = coordinator
        .start_monitor(std::time::Duration::from_millis(10))
        .unwrap();
    alive.store(false, Ordering::Release);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while shard.handle().observation().ingress_open && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    drop(monitor);
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn coordinator_fails_closed_when_confirmation_fails_after_cas() {
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    struct Authority {
        /// 共享的路由版本和快照。
        state: Arc<Mutex<(u64, RouteTable)>>,
        /// 用于控制读取结果的调用次数。
        reads: Arc<AtomicUsize>,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(996)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            if self
                .reads
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_add(1)
                })
                .unwrap()
                == 3
            {
                return Err(std::io::Error::other("确认响应丢失"));
            }
            Ok(self.state.lock().unwrap().clone())
        }
        fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> std::io::Result<u64> {
            let mut state = self.state.lock().unwrap();
            if state.0 != expected {
                return Err(std::io::Error::other("版本冲突"));
            }
            *state = (expected.checked_add(1).unwrap(), routes);
            Ok(state.0)
        }
    }
    let table = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let state = Arc::new(Mutex::new((1, table(1))));
    let reads = Arc::new(AtomicUsize::new(0));
    let authority = || Authority {
        state: state.clone(),
        reads: reads.clone(),
    };
    let start = |routes| {
        Shard::start(
            ShardId(82),
            ClusterId(996),
            routes,
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap()
    };
    let mut shard = start(table(1));
    let mut coordinator =
        RouteCoordinator::with_authority(authority(), vec![shard.handle()]).unwrap();
    assert!(
        coordinator
            .update(1, table(2), Duration::from_secs(2))
            .is_err()
    );
    assert_eq!(*state.lock().unwrap(), (2, table(2)));
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    shard.shutdown(Duration::from_secs(2)).unwrap();
    drop(coordinator);
    let mut recovered = start(table(2));
    let recovered_coordinator =
        RouteCoordinator::with_authority(authority(), vec![recovered.handle()]).unwrap();
    recovered.shutdown(Duration::from_secs(2)).unwrap();
    drop(recovered_coordinator);
}

#[test]
fn coordinator_closes_when_authority_reports_unpersisted_success() {
    use matching_domain::ClusterId;
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::time::Duration;
    struct Faulty(/** 测试使用的路由快照。 */ RouteTable);
    impl RouteAuthority for Faulty {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(994)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((1, self.0.clone()))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            Ok(2)
        }
    }
    let table = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let mut shard = Shard::start(
        ShardId(82),
        ClusterId(994),
        table(1),
        vec![(
            MarketId(81),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let mut coordinator =
        RouteCoordinator::with_authority(Faulty(table(1)), vec![shard.handle()]).unwrap();
    assert!(
        coordinator
            .update(1, table(2), Duration::from_secs(2))
            .is_err()
    );
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    drop(coordinator);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn authority_read_failure_closes_running_shard() {
    use matching_domain::ClusterId;
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard, ShardError};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;
    struct Authority {
        /// 控制模拟权威是否返回失败。
        fail: Arc<AtomicBool>,
        /// 测试权威返回或保存的路由快照。
        routes: RouteTable,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(992)
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("权威不可用"));
            }
            Ok((1, self.routes.clone()))
        }
        fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> std::io::Result<u64> {
            unreachable!()
        }
    }
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(61),
        shard_id: ShardId(62),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(62),
        ClusterId(992),
        routes.clone(),
        vec![(
            MarketId(61),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let authority = Authority {
        fail: fail.clone(),
        routes: routes.clone(),
    };
    let mut coordinator =
        RouteCoordinator::with_authority(authority, vec![shard.handle()]).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(
        coordinator
            .update(1, routes, Duration::from_secs(2))
            .is_err()
    );
    assert!(matches!(shard.handle().barrier(1), Err(ShardError::Closed)));
    drop(coordinator);
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn coordinator_accepts_injected_control_authority() {
    use matching_domain::ClusterId;
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteAuthority, RouteCoordinator, Shard};
    use std::time::Duration;
    struct Authority {
        /// 模拟权威当前的持久版本。
        revision: u64,
        /// 测试权威返回或保存的路由快照。
        routes: RouteTable,
    }
    impl RouteAuthority for Authority {
        fn requires_monitor(&self) -> bool {
            false
        }
        fn renew(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn cluster_id(&self) -> ClusterId {
            ClusterId(990)
        }
        fn load(&self) -> std::io::Result<(u64, RouteTable)> {
            Ok((self.revision, self.routes.clone()))
        }
        fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> std::io::Result<u64> {
            if self.revision != expected {
                return Err(std::io::Error::other("版本冲突"));
            }
            self.revision = self
                .revision
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("版本耗尽"))?;
            self.routes = routes;
            Ok(self.revision)
        }
    }
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(31),
            shard_id: ShardId(32),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let mut shard = Shard::start(
        ShardId(32),
        ClusterId(990),
        routes(1),
        vec![(
            MarketId(31),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let authority = Authority {
        revision: 1,
        routes: routes(1),
    };
    let mut coordinator =
        RouteCoordinator::with_authority(authority, vec![shard.handle()]).unwrap();
    assert_eq!(
        coordinator
            .update(1, routes(2), Duration::from_secs(2))
            .unwrap(),
        2
    );
    assert_eq!(coordinator.verify_authority().unwrap(), 2);
    shard.shutdown(Duration::from_secs(2)).unwrap();
    drop(coordinator);
}

#[test]
fn coordinator_publishes_store_revision_after_shard_barrier() {
    use matching_domain::{
        ClusterId, CommandId, LimitOrder, OrderId, Price, Qty, Side, StpKey, StpMode, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{Mutation, RouteCoordinator, Shard};
    use std::time::Duration;
    let dir = std::env::temp_dir().join(format!(
        "tide-route-coord-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(1),
            shard_id: ShardId(2),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let store =
        RouteControlStore::create_for_cluster(&dir, ClusterId(444), routes(1), 16, 16).unwrap();
    let mut shard = Shard::start(
        ShardId(2),
        ClusterId(444),
        routes(1),
        vec![(
            MarketId(1),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let handle = shard.handle();
    let mut coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
    let other = dir.join("other");
    std::fs::create_dir(&other).unwrap();
    let conflicting =
        RouteControlStore::create_for_cluster(&other, ClusterId(444), routes(1), 16, 16).unwrap();
    assert!(RouteCoordinator::new(conflicting, vec![handle.clone()]).is_err());
    assert_eq!(
        coordinator
            .update(1, routes(2), Duration::from_secs(2))
            .unwrap(),
        2
    );
    assert!(
        coordinator
            .update(1, routes(1), Duration::from_secs(2))
            .is_err()
    );
    let order = LimitOrder {
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(1).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let until = std::time::Instant::now() + Duration::from_secs(2);
    let send = |epoch| loop {
        match handle.submit(
            CommandId(1),
            MarketId(1),
            epoch,
            Mutation::Place(order.clone()),
        ) {
            Ok(v) => break v,
            Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < until => {
                std::thread::yield_now()
            }
            Err(e) => panic!("入口失败: {e:?}"),
        }
    };
    assert!(matches!(
        send(1).recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(matching_domain::RejectReason::StaleRouteEpoch)
    ));
    assert_eq!(
        send(2)
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    drop(coordinator);
    assert!(matches!(
        handle.barrier(1),
        Err(matching_runtime::ShardError::Closed)
    ));
    shard.shutdown(Duration::from_secs(2)).unwrap();
    let reopened = reopen_after_drain(&dir, ClusterId(444), 16, 16);
    assert_eq!(reopened.read().unwrap(), (2, &routes(2)));
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn coordinator_closes_ingress_when_durable_update_fails() {
    use matching_domain::ClusterId;
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{RouteCoordinator, Shard, ShardError};
    use std::time::Duration;
    let dir = std::env::temp_dir().join(format!(
        "tide-route-failed-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let routes = |epoch| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(1),
            shard_id: ShardId(2),
            route_epoch: epoch,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let store =
        RouteControlStore::create_for_cluster(&dir, ClusterId(445), routes(1), 1, 1).unwrap();
    let mut shard = Shard::start(
        ShardId(2),
        ClusterId(445),
        routes(1),
        vec![(
            MarketId(1),
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let handle = shard.handle();
    let mut coordinator = RouteCoordinator::new(store, vec![handle.clone()]).unwrap();
    assert!(
        coordinator
            .update(1, routes(2), Duration::from_secs(2))
            .is_err()
    );
    assert!(matches!(handle.barrier(1), Err(ShardError::Closed)));
    assert!(
        coordinator
            .update(1, routes(2), Duration::from_secs(2))
            .is_err()
    );
    drop(coordinator);
    shard.shutdown(Duration::from_secs(2)).unwrap();
    let reopened = reopen_after_drain(&dir, ClusterId(445), 1, 1);
    assert_eq!(reopened.read().unwrap(), (1, &routes(1)));
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn confirmed_truncation_is_rejected_and_unconfirmed_tail_is_removed() {
    use std::io::Write;
    let dir = std::env::temp_dir().join(format!(
        "tide-route-damage-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(1),
        shard_id: ShardId(2),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    drop(
        RouteControlStore::create_for_cluster(&dir, ClusterId(446), routes.clone(), 16, 16)
            .unwrap(),
    );
    let path = dir.join("routes.log");
    let original = std::fs::read(&path).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[1, 2, 3]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let store = RouteControlStore::open_for_cluster(&dir, ClusterId(446), 16, 16).unwrap();
    assert_eq!(store.read().unwrap(), (1, &routes));
    drop(store);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let mut corrupt = original.clone();
    corrupt[8] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(RouteControlStore::open_for_cluster(&dir, ClusterId(446), 16, 16).is_err());
    std::fs::write(&path, &original).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len((original.len() - 1) as u64)
        .unwrap();
    assert!(RouteControlStore::open_for_cluster(&dir, ClusterId(446), 16, 16).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
