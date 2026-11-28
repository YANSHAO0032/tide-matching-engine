//! 真实 etcd 协调者接任与两个持久工作线程的端到端验证。
#[path = "../../matching-runtime/tests/t36_support/tcp.rs"]
mod tcp;
use ed25519_dalek::{Signer, SigningKey};
use matching_control::{EtcdClient, EtcdControlStore};
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::cross_control::{
    ControlAdmission, ControlAuthority, ControlKind, ControlPhase, CrossShardCoordinator,
};
use matching_runtime::{
    ControlStore, FormalError, FormalOutcome, IdempotencyConfig, MarketRoute, PlaceLimits,
    PlaceSupportConfig, RouteState, RouteTable, Shard,
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const WAIT: Duration = Duration::from_secs(5);
const CLUSTER: ClusterId = ClusterId(1036);
const USER: UserId = UserId(7);
const IDS: [ShardId; 2] = [ShardId(1), ShardId(2)];

fn client() -> EtcdClient {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        WAIT,
        true,
    )
    .unwrap()
}

fn start(id: ShardId, root: &Path, reader: Arc<dyn ControlAdmission>) -> Shard {
    let cluster = reader.cluster_id();
    let path = root.join(id.get().to_string());
    std::fs::create_dir(&path).unwrap();
    for name in ["manifest", "wal"] {
        std::fs::create_dir(path.join(name)).unwrap();
    }
    let lease = ControlStore::new(path.join("primary"), cluster, id)
        .acquire_primary()
        .unwrap();
    let epoch = lease.epoch();
    let market = MarketId(u32::from(id.get()));
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let routes = RouteTable::new(IDS.map(|shard_id| MarketRoute {
        market_id: MarketId(u32::from(shard_id.get())),
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }))
    .unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 16,
            terminal_history: 16,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .with_primary_lease(Arc::new(lease))
    .with_control_admission(reader);
    Shard::start_group_place_support(
        id,
        cluster,
        routes,
        vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), epoch, 16, place),
        ((path.join("wal"), path.join("manifest")), (4, 10_000)),
    )
    .unwrap()
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群与子进程"]
fn process_exit_after_durable_shard_ack_resumes_the_other_shard() {
    const CRASH_CLUSTER: ClusterId = ClusterId(1037);
    const PARENT: CommandId = CommandId(201);
    const KIND: ControlKind = ControlKind::UserMassCancel(USER);
    const CHILD_NAME: &str = "TIDE_CONTROL_CRASH_AFTER_REAL_ACK";

    if let Ok(name) = std::env::var(CHILD_NAME) {
        let prefix = format!("/tide-test/{name}");
        let reader = Arc::new(EtcdControlStore::open(client(), &prefix, CRASH_CLUSTER).unwrap());
        let a = start(IDS[0], &std::env::temp_dir().join(name), reader);
        assert_eq!(
            submit(&a, place(21, USER, IDS[0], 0)),
            Ok(FormalOutcome::Placed(OrderId(21)))
        );
        let owner =
            Arc::new(EtcdControlStore::acquire(client(), &prefix, CRASH_CLUSTER, 3).unwrap());
        let coordinator =
            CrossShardCoordinator::new(owner.clone(), CRASH_CLUSTER, &IDS, WAIT).unwrap();
        coordinator.attach(a.handle()).unwrap();
        coordinator.prepare(PARENT, KIND).unwrap();
        let deadline = Instant::now() + WAIT;
        loop {
            assert!(Instant::now() < deadline, "子进程未取得首个持久回执");
            owner.keep_alive().unwrap();
            match coordinator.step(PARENT) {
                Ok(op) if !op.acks.is_empty() => {
                    assert_eq!(op.phase, ControlPhase::AwaitingAcks);
                    assert_eq!(op.acks.len(), 1);
                    assert_eq!(
                        (op.acks[&IDS[0]].shard_seq, op.acks[&IDS[0]].canceled),
                        (2, 1)
                    );
                    // 直接终止进程，既不关闭分片线程，也不撤销协调者租约。
                    std::process::exit(36);
                }
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::yield_now(),
                Err(e) => panic!("子进程派发控制命令失败 {e}"),
            }
        }
    }

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("fanout-exit-{}-{stamp}", std::process::id());
    let prefix = format!("/tide-test/{name}");
    let root = std::env::temp_dir().join(&name);
    std::fs::create_dir(&root).unwrap();
    // 初始化命名空间并准备 B 的两笔订单，随后让子进程独占协调者租约。
    let bootstrap = EtcdControlStore::acquire(client(), &prefix, CRASH_CLUSTER, 60).unwrap();
    let reader = Arc::new(EtcdControlStore::open(client(), &prefix, CRASH_CLUSTER).unwrap());
    let mut b = start(IDS[1], &root, reader.clone());
    for id in [22_u64, 23] {
        assert_eq!(
            submit(&b, place(id, USER, IDS[1], 0)),
            Ok(FormalOutcome::Placed(OrderId(id.into())))
        );
    }
    bootstrap.revoke().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "process_exit_after_durable_shard_ack_resumes_the_other_shard",
            "--ignored",
        ])
        .env(CHILD_NAME, &name);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        child.creation_flags(0x0800_0000);
    }
    let output = child.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(36),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    let client = client();
    let next = loop {
        assert_eq!(
            reader.check(USER).unwrap().2,
            Some(RejectReason::ControlFenceActive)
        );
        match EtcdControlStore::acquire(client.clone(), &prefix, CRASH_CLUSTER, 60) {
            Ok(owner) => break Arc::new(owner),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::WouldBlock
                ) && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("子进程退出后接任租约失败 {e}"),
        }
    };
    let partial = next.load(PARENT).unwrap();
    assert_eq!(next.pending().unwrap(), vec![PARENT]);
    assert_eq!(partial.phase, ControlPhase::AwaitingAcks);
    assert_eq!(partial.acks.len(), 1);
    let first = partial.acks[&IDS[0]];
    assert_eq!((first.shard_seq, first.canceled), (2, 1));
    assert!(contains(&b, 2, 22) && contains(&b, 2, 23));
    let successor = CrossShardCoordinator::new(next.clone(), CRASH_CLUSTER, &IDS, WAIT).unwrap();
    // 已确认的 A 仍处于退出状态，接任者只连接 B。
    successor.attach(b.handle()).unwrap();
    assert_eq!(successor.prepare(PARENT, KIND).unwrap(), partial);
    let done = successor.resume(PARENT, Instant::now() + WAIT).unwrap();
    assert_eq!(done.phase, ControlPhase::Done);
    assert_eq!(done.targets, partial.targets);
    assert_eq!(done.acks[&IDS[0]], first);
    assert_eq!(
        (done.acks[&IDS[1]].shard_seq, done.acks[&IDS[1]].canceled),
        (3, 2)
    );
    assert_eq!(done.total_canceled().unwrap(), 3);
    assert!(!contains(&b, 2, 22) && !contains(&b, 2, 23));
    assert_eq!(reader.check(USER).unwrap(), (0, 1, None));
    let path = root.join("1");
    let wal = matching_persistence::recover_local(
        path.join("wal"),
        path.join("manifest"),
        0,
        CRASH_CLUSTER.0,
        IDS[0].0,
    )
    .unwrap();
    assert_eq!(wal.len(), 2);
    assert!(matches!(
        decode_wal_command(&wal[0].payload).unwrap().payload,
        Command::PlaceLimit(_)
    ));
    let cmd = decode_wal_command(&wal[1].payload).unwrap();
    assert_eq!(
        (wal[1].shard_seq, cmd.command_id),
        (first.shard_seq, KIND.derive(PARENT, IDS[0]))
    );
    assert!(
        matches!(cmd.payload,Command::ApplyUserControlFenceShard(f) if f.parent_command_id == PARENT && f.user_id == USER && f.action == UserFenceAction::FenceAndCancelAll)
    );
    b.shutdown(WAIT).unwrap();
    next.revoke().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn place(id: u64, user: UserId, shard: ShardId, epoch: u64) -> CommandEnvelope {
    let market = MarketId(u32::from(shard.get()));
    let cid = CommandId(u128::from(id));
    let oid = OrderId(cid.0);
    let mut token = ReservationToken {
        token_id: cid.0,
        command_id: cid,
        reservation_id: ReservationId(cid.0),
        order_id: oid,
        user_id: user,
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 1,
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
        session_id: id,
        client_seq: 1,
        market_id: Some(market),
        user_id: Some(user),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: oid,
                user_id: user,
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: epoch,
            post_only: false,
            reservation: token,
        })),
    }
}

/// 分片入队允许暂时背压；成功入队后只等待该次回复。
fn enqueue<T>(
    mut action: impl FnMut() -> Result<T, matching_runtime::ShardError>,
) -> Result<T, matching_runtime::ShardError> {
    let end = Instant::now() + WAIT;
    loop {
        match action() {
            Err(matching_runtime::ShardError::Busy) if Instant::now() < end => {
                std::thread::yield_now()
            }
            result => return result,
        }
    }
}

fn submit(shard: &Shard, cmd: CommandEnvelope) -> Result<FormalOutcome, FormalError> {
    let rx = enqueue(|| shard.handle().submit_command(cmd.clone()))
        .map_err(|e| FormalError::Rejected(e.ingress_reason().expect("入口拒绝应有稳定原因")))?;
    Ok(rx.recv_timeout(WAIT).unwrap()?.outcome)
}
fn contains(shard: &Shard, market: u32, id: u128) -> bool {
    enqueue(|| shard.handle().order(MarketId(market), OrderId(id)))
        .unwrap()
        .recv_timeout(WAIT)
        .unwrap()
        .unwrap()
        .is_some()
}
fn barrier(shard: &Shard) -> u64 {
    enqueue(|| shard.barrier(0))
        .unwrap()
        .recv_timeout(WAIT)
        .unwrap()
        .1
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn owner_restart_after_one_real_ack_finishes_without_duplicate_cancels() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("control-e2e-{}-{stamp}", std::process::id());
    let prefix = format!("/tide-test/{name}");
    let root = std::env::temp_dir().join(name);
    std::fs::create_dir(&root).unwrap();
    let first = Arc::new(EtcdControlStore::acquire(client(), &prefix, CLUSTER, 60).unwrap());
    let reader = Arc::new(EtcdControlStore::open(client(), &prefix, CLUSTER).unwrap());
    let mut a = start(IDS[0], &root, reader.clone());
    let mut b = start(IDS[1], &root, reader.clone());
    let parent = CommandId(101);
    let kind = ControlKind::UserMassCancel(USER);
    let old = CrossShardCoordinator::new(first.clone(), CLUSTER, &IDS, WAIT).unwrap();
    old.attach(a.handle()).unwrap();
    for (shard, sid, id) in [(&a, IDS[0], 1), (&b, IDS[1], 2)] {
        assert!(matches!(
            submit(shard, place(id, USER, sid, 0)),
            Ok(FormalOutcome::Placed(_))
        ));
    }
    assert!(matches!(
        submit(&b, place(3, UserId(8), IDS[1], 0)),
        Ok(FormalOutcome::Placed(_))
    ));
    assert!(matches!(
        submit(&b, place(11, USER, IDS[1], 0)),
        Ok(FormalOutcome::Placed(_))
    ));
    let prepared = old.prepare(parent, kind).unwrap();
    let end = Instant::now() + WAIT;
    loop {
        match old.step(parent) {
            Ok(record) if !record.acks.is_empty() => break,
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::yield_now(),
            Err(e) => panic!("首个分片派发失败 {e}"),
        }
        assert!(Instant::now() < end, "未收到首个分片的回执");
    }
    let partial = first.load(parent).unwrap();
    assert_eq!(partial.acks.len(), 1);
    assert_eq!(partial.acks[&IDS[0]].canceled, 1);
    assert_eq!(partial.phase, ControlPhase::AwaitingAcks);
    let prefix_a = barrier(&a);
    assert!(!contains(&a, 1, 1) && contains(&b, 2, 2));
    let previous_stalls = old.telemetry().snapshot().0;
    assert_eq!(
        old.step(parent).unwrap_err().kind(),
        std::io::ErrorKind::NotConnected
    );
    let health = matching_runtime::Health::new(vec![a.handle()])
        .unwrap()
        .with_control_telemetry(old.telemetry());
    let metric = format!(
        "control_fanout_stalled_total {}",
        previous_stalls.checked_add(1).unwrap()
    );
    assert!(
        health
            .prometheus()
            .unwrap()
            .lines()
            .any(|line| line == metric)
    );
    assert!(old.abort(parent).is_err());
    let prefix_b = barrier(&b);
    tcp::assert_rejected(
        &root,
        MarketId(2),
        &[a.handle(), b.handle()],
        &[(1, USER.0, 4006), (4, USER.0, 4006), (5, USER.0, 4006)],
    );
    assert_eq!((barrier(&a), barrier(&b)), (prefix_a, prefix_b));
    assert_eq!(first.load(parent).unwrap(), partial);
    assert_eq!(
        submit(&b, place(4, USER, IDS[1], 1)),
        Err(FormalError::Rejected(RejectReason::ControlFenceActive))
    );
    assert_eq!(barrier(&b), prefix_b);
    // 分片仍等待协调器重连时，受控用户能按订单身份主动撤单并查询结果。
    let order_id = OrderId(11);
    let cancel = CommandEnvelope {
        command_id: CommandId(112),
        session_id: 7,
        client_seq: 112,
        market_id: Some(MarketId(2)),
        user_id: Some(USER),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::Cancel(CancelOrder {
            market_id: MarketId(2),
            user_id: USER,
            order_id,
            client_order_id: None,
        }),
    };
    assert_eq!(submit(&b, cancel), Ok(FormalOutcome::Canceled(order_id)));
    assert!(!contains(&b, 2, 11) && contains(&b, 2, 2));
    let after_cancel = barrier(&b);
    assert_eq!(after_cancel, prefix_b + 1);
    first.revoke().unwrap();
    drop(old);
    let next = Arc::new(EtcdControlStore::acquire(client(), &prefix, CLUSTER, 60).unwrap());
    let coordinator = CrossShardCoordinator::new(next.clone(), CLUSTER, &IDS, WAIT).unwrap();
    coordinator.attach(a.handle()).unwrap();
    coordinator.attach(b.handle()).unwrap();
    assert_eq!(next.pending().unwrap(), vec![parent]);
    assert_eq!(
        coordinator.prepare(parent, kind).unwrap().targets,
        prepared.targets
    );
    let progress = coordinator.recover_pending(Instant::now() + WAIT).unwrap();
    assert_eq!(progress[&parent].as_ref().unwrap().acks.len(), 2);
    let done = coordinator.resume(parent, Instant::now() + WAIT).unwrap();
    assert_eq!(done.phase, ControlPhase::Done);
    assert_eq!(done.total_canceled().unwrap(), 2);
    assert_eq!(barrier(&a), prefix_a);
    assert_eq!(barrier(&b), after_cancel + 1);
    assert!(!contains(&b, 2, 2) && contains(&b, 2, 3));
    assert_eq!(coordinator.prepare(parent, kind).unwrap(), done);
    assert_eq!(
        submit(&a, place(5, USER, IDS[0], 0)),
        Err(FormalError::Rejected(RejectReason::StaleUserControlEpoch))
    );
    assert!(matches!(
        submit(&a, place(6, USER, IDS[0], 1)),
        Ok(FormalOutcome::Placed(_))
    ));
    for (id, active) in [(102, true), (103, false)] {
        let parent = CommandId(id);
        coordinator
            .prepare(
                parent,
                ControlKind::GlobalKill {
                    active,
                    cancel_all: active,
                },
            )
            .unwrap();
        let record = coordinator.resume(parent, Instant::now() + WAIT).unwrap();
        assert_eq!(record.total_canceled().unwrap(), if active { 2 } else { 0 });
        assert_eq!(
            reader.check(USER).unwrap().2,
            active.then_some(RejectReason::KillSwitchActive)
        );
        if active {
            assert!(!contains(&a, 1, 6) && !contains(&b, 2, 3));
            assert_eq!(
                submit(&b, place(7, UserId(8), IDS[1], 0)),
                Err(FormalError::Rejected(RejectReason::KillSwitchActive))
            );
        }
    }
    assert!(matches!(
        submit(&b, place(8, UserId(8), IDS[1], 0)),
        Ok(FormalOutcome::Placed(_))
    ));
    let user_kill = CommandId(104);
    coordinator
        .prepare(
            user_kill,
            ControlKind::UserKill {
                user: USER,
                active: true,
                cancel_all: false,
            },
        )
        .unwrap();
    assert_eq!(
        coordinator
            .resume(user_kill, Instant::now() + WAIT)
            .unwrap()
            .total_canceled()
            .unwrap(),
        0
    );
    assert_eq!(
        submit(&a, place(9, USER, IDS[0], 2)),
        Err(FormalError::Rejected(RejectReason::KillSwitchActive))
    );
    assert!(matches!(
        submit(&a, place(10, UserId(8), IDS[0], 0)),
        Ok(FormalOutcome::Placed(_))
    ));
    let all = CommandId(105);
    coordinator
        .prepare(all, ControlKind::GlobalMassCancel)
        .unwrap();
    assert_eq!(
        coordinator
            .resume(all, Instant::now() + WAIT)
            .unwrap()
            .total_canceled()
            .unwrap(),
        2
    );
    assert_eq!(
        reader.check(USER).unwrap().2,
        Some(RejectReason::KillSwitchActive)
    );
    assert!(!contains(&a, 1, 10) && !contains(&b, 2, 8));
    a.shutdown(WAIT).unwrap();
    b.shutdown(WAIT).unwrap();
    next.revoke().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
