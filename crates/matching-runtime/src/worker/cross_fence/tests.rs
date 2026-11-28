//! 核验跨分片控制在本地 WAL 的单序号执行、回执及恢复。
mod ingress_race;
use crate::test_directory::TestDirectory;
use crate::{
    cross_control::{ControlAdmission, ControlKind},
    *,
};
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use std::{sync::Arc, time::Duration};

const MARKET: MarketId = MarketId(1);
const SHARD: ShardId = ShardId(36);
const CLUSTER: ClusterId = ClusterId(1036);
/// 模拟控制权威发布的全局和用户纪元，并可把用户置于等待分片确认的状态。
struct PublishedEpoch {
    /// 模拟控制权威已经发布的全局纪元。
    global: std::sync::atomic::AtomicU64,
    /// 用户一的跨分片操作是否正在等待回执。
    fencing: std::sync::atomic::AtomicBool,
    /// 模拟用户一的已发布纪元，其他用户保持初始值。
    user_one: std::sync::atomic::AtomicU64,
}
impl ControlAdmission for PublishedEpoch {
    fn cluster_id(&self) -> ClusterId {
        CLUSTER
    }
    fn namespace(&self) -> String {
        "local-fence-test".to_owned()
    }
    fn check(&self, user: UserId) -> std::io::Result<(u64, u64, Option<RejectReason>)> {
        let user_epoch = if user == UserId(1) {
            self.user_one.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        Ok((
            self.global.load(std::sync::atomic::Ordering::SeqCst),
            user_epoch,
            (user == UserId(1) && self.fencing.load(std::sync::atomic::Ordering::SeqCst))
                .then_some(RejectReason::ControlFenceActive),
        ))
    }
}

const WAIT: Duration = Duration::from_secs(10);

/// 构造绑定订单、命令、用户和指定用户纪元的已签名限价委托。
fn place(id: u64, user: UserId, user_epoch: u64) -> CommandEnvelope {
    let n = u128::from(id);
    let mut reservation = ReservationToken {
        token_id: n,
        command_id: CommandId(n),
        reservation_id: ReservationId(n),
        order_id: OrderId(n),
        user_id: user,
        market_id: MARKET,
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
    reservation.signature = SigningKey::from_bytes(&[7; 32])
        .sign(&reservation.canonical_signing_payload().unwrap())
        .to_bytes();
    CommandEnvelope {
        command_id: CommandId(n),
        session_id: id,
        client_seq: 1,
        market_id: Some(MARKET),
        user_id: Some(user),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(n),
                user_id: user,
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: Some(format!("order-{id}")),
            user_control_epoch: user_epoch,
            post_only: false,
            reservation,
        })),
    }
}
/// 用父命令和目标分片派生子命令身份，写入当前投递的主节点纪元。
fn control(kind: ControlKind, parent: u64, epoch: u64, owner: u64) -> CommandEnvelope {
    let user = match kind {
        ControlKind::UserMassCancel(u) | ControlKind::UserKill { user: u, .. } => Some(u),
        _ => None,
    };
    let action = match kind {
        ControlKind::UserKill {
            active, cancel_all, ..
        }
        | ControlKind::GlobalKill { active, cancel_all } => {
            UserFenceAction::Kill { active, cancel_all }
        }
        _ => UserFenceAction::FenceAndCancelAll,
    };
    let parent_command_id = CommandId(u128::from(parent));
    CommandEnvelope {
        command_id: kind.derive(parent_command_id, SHARD),
        session_id: parent,
        client_seq: 1,
        market_id: None,
        user_id: user,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: match user {
            Some(user_id) => Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id,
                user_id,
                new_epoch: epoch,
                action,
                expected_primary_epoch: owner,
            }),
            None => Command::ApplyGlobalControlFenceShard(ApplyGlobalControlFenceShard {
                parent_command_id,
                new_epoch: epoch,
                action,
                expected_primary_epoch: owner,
            }),
        },
    }
}
/// 经各自的正式入口提交命令，遇到临时背压时在测试期限内重试。
fn run(shard: &Shard, command: &CommandEnvelope) -> Result<FormalExecution, FormalError> {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        let handle = shard.handle();
        let rx = if super::coordinated(&command.payload) {
            handle.submit_control_command("local-fence-test", command.clone())
        } else {
            handle.submit_command(command.clone())
        };
        match rx {
            Ok(rx) => return rx.recv_timeout(WAIT).unwrap(),
            Err(ShardError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(e) => panic!("分片请求未入队 {e:?}"),
        }
    }
}

/// 在真实分片上核对原序事件、控制回执淘汰、租约接任及单笔撤单的持久结果。
#[test]
fn atomic_user_cancel_and_global_kill_survive_cache_eviction_and_recovery() {
    let dir = TestDirectory::new();
    let snapshots = dir.0.join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    let store = ControlStore::new(dir.0.join("lease"), CLUSTER, SHARD);
    let lease = Arc::new(store.acquire_primary().unwrap());
    let owner = lease.epoch();
    let routes = RouteTable::new([MarketRoute {
        market_id: MARKET,
        shard_id: SHARD,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let published = Arc::new(PublishedEpoch {
        global: 0.into(),
        fencing: false.into(),
        user_one: 0.into(),
    });
    let config = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap(),
        [(MARKET, policy)],
        PlaceLimits {
            active_orders: 16,
            terminal_history: 16,
            client_id_bytes: 16,
        },
    )
    .unwrap()
    .with_control_admission(published.clone());
    let limits = IdempotencyConfig {
        command_capacity: 1,
        ..Default::default()
    };
    let policies = [(MARKET, QueuePriorityPolicy::try_new(1).unwrap())];
    let mut shard = Shard::start_group_place_support(
        SHARD,
        CLUSTER,
        routes.clone(),
        policies.to_vec(),
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (
            limits,
            owner,
            8,
            config.clone().with_primary_lease(lease.clone()),
        ),
        ((dir.0.join("wal"), dir.0.join("manifest")), (4, 1000)),
    )
    .unwrap();
    for (n, u) in [(31, 1), (32, 2), (33, 1)] {
        assert!(matches!(
            run(&shard, &place(n, UserId(u), 0)).unwrap().outcome,
            FormalOutcome::Placed(_)
        ));
    }
    let kind = ControlKind::UserMassCancel(UserId(1));
    let cancel = control(kind, 40, 1, owner);
    let ack = run(&shard, &cancel).unwrap();
    published
        .user_one
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(ack.seq, 4);
    assert_eq!(
        ack.outcome,
        FormalOutcome::MassCancel(ShardMassCancelResult { canceled_orders: 2 })
    );
    assert!(matches!(
        ack.events.last().unwrap().payload,
        EngineEvent::UserControlEpochAdvanced {
            user_id: UserId(1),
            epoch: 1
        }
    ));
    let canceled: Vec<_> = ack
        .events
        .iter()
        .filter_map(|e| match e.payload {
            EngineEvent::OrderCanceled {
                order_id,
                reason: CancelReason::MassCancel,
                ..
            } => Some(order_id),
            _ => None,
        })
        .collect();
    assert_eq!(canceled, [OrderId(31), OrderId(33)]);
    assert_eq!(
        ack.events
            .iter()
            .filter(|e| matches!(
                e.payload,
                EngineEvent::ReservationReleased {
                    reason: ReservationReleaseReason::MassCancel,
                    released: Some(_),
                    ..
                }
            ))
            .count(),
        2
    );
    assert_eq!(
        run(&shard, &place(34, UserId(1), 0)).unwrap_err(),
        FormalError::Rejected(RejectReason::StaleUserControlEpoch)
    );
    let kill = control(
        ControlKind::GlobalKill {
            active: true,
            cancel_all: true,
        },
        41,
        1,
        owner,
    );
    let halted = run(&shard, &kill).unwrap();
    published
        .global
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        halted.outcome,
        FormalOutcome::MassCancel(ShardMassCancelResult { canceled_orders: 1 })
    );
    assert!(halted.events.iter().any(|e| matches!(
        e.payload,
        EngineEvent::KillSwitchActivated {
            scope: KillScope::Global
        }
    )));
    assert_eq!(
        run(&shard, &place(35, UserId(2), 0)).unwrap().outcome,
        FormalOutcome::Rejected(RejectReason::KillSwitchActive)
    );
    let duplicate = run(&shard, &cancel).unwrap();
    assert!(duplicate.deduplicated && duplicate.events.is_empty());
    assert_eq!(
        (duplicate.seq, duplicate.outcome),
        (ack.seq, ack.outcome.clone())
    );
    let image = shard
        .handle()
        .take_snapshot(7)
        .unwrap()
        .recv_timeout(WAIT)
        .unwrap()
        .unwrap();
    let mut trusted = SnapshotRestoreConfig {
        cluster: CLUSTER,
        shard: SHARD,
        primary_epoch: owner,
        routes,
        user_capacity: 8,
        idempotency: limits,
        place: Some(config.clone().with_primary_lease(lease.clone())),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1_000_000,
    };
    let bytes = image.encode_snapshot(trusted.max_bytes).unwrap();
    let digest = image.state_digest(trusted.max_bytes).unwrap();
    assert_eq!(
        SnapshotImage::decode_snapshot(&bytes, &trusted)
            .unwrap()
            .state_digest(trusted.max_bytes)
            .unwrap(),
        digest
    );
    drop(image);
    shard.shutdown(WAIT).unwrap();
    drop(shard);
    drop(trusted.place.take());
    drop(lease);
    let next = Arc::new(store.acquire_primary().unwrap());
    trusted.primary_epoch = next.epoch();
    trusted.place = Some(config.with_primary_lease(next.clone()));
    let mut unbound = trusted.clone();
    unbound.place.as_mut().unwrap().control_admission = None;
    assert!(matches!(
        Shard::recover_group_local_with_genesis(
            dir.0.join("wal"),
            dir.0.join("manifest"),
            snapshots.clone(),
            &unbound,
            &policies,
            (4, 1000)
        ),
        Err(ShardError::InvalidConfig)
    ));
    let mut restored = Shard::recover_group_local_with_genesis(
        dir.0.join("wal"),
        dir.0.join("manifest"),
        &snapshots,
        &trusted,
        &policies,
        (4, 1000),
    )
    .unwrap();
    assert_eq!(
        run(&restored, &cancel).unwrap_err(),
        FormalError::Rejected(RejectReason::StalePrimaryEpoch)
    );
    let retry = run(&restored, &control(kind, 40, 1, next.epoch())).unwrap();
    assert!(retry.deduplicated);
    assert_eq!((retry.seq, retry.outcome), (ack.seq, ack.outcome));
    let clear = control(
        ControlKind::GlobalKill {
            active: false,
            cancel_all: false,
        },
        42,
        2,
        next.epoch(),
    );
    assert_eq!(run(&restored, &clear).unwrap().seq, 7);
    published
        .global
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        run(&restored, &place(36, UserId(1), 1)).unwrap().outcome,
        FormalOutcome::Placed(_)
    ));
    assert!(
        run(
            &restored,
            &control(
                ControlKind::GlobalKill {
                    active: true,
                    cancel_all: true
                },
                41,
                1,
                next.epoch()
            )
        )
        .is_err()
    );
    assert!(matches!(
        run(&restored, &place(38, UserId(1), 1)).unwrap().outcome,
        FormalOutcome::Placed(OrderId(38))
    ));
    published
        .fencing
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        run(&restored, &place(37, UserId(1), 1)).unwrap_err(),
        FormalError::Rejected(RejectReason::ControlFenceActive)
    );
    let targeted = |cid: u64, user: UserId, alias: Option<&str>| CommandEnvelope {
        command_id: CommandId(cid.into()),
        session_id: cid,
        client_seq: 1,
        market_id: Some(MARKET),
        user_id: Some(user),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::Cancel(CancelOrder {
            market_id: MARKET,
            user_id: user,
            order_id: OrderId(36),
            client_order_id: alias.map(str::to_owned),
        }),
    };
    // 同一 ID 的其他用户和错误客户端别名都看不到活动订单。
    for request in [
        targeted(51, UserId(2), None),
        targeted(52, UserId(1), Some("order-38")),
    ] {
        assert_eq!(
            run(&restored, &request).unwrap().outcome,
            FormalOutcome::Rejected(RejectReason::OrderNotFound)
        );
    }
    for alias in ["", "x".repeat(17).as_str()] {
        assert_eq!(
            run(&restored, &targeted(53, UserId(1), Some(alias))).unwrap_err(),
            FormalError::Rejected(RejectReason::InvalidOrderCombination)
        );
    }
    let cancel = targeted(53, UserId(1), Some("order-36"));
    let mut stale = cancel.clone();
    stale.route_epoch = Some(0);
    assert_eq!(
        run(&restored, &stale).unwrap_err(),
        FormalError::Rejected(RejectReason::StaleRouteEpoch)
    );
    let execution = run(&restored, &cancel).unwrap();
    assert_eq!(execution.seq, 12);
    assert_eq!(execution.outcome, FormalOutcome::Canceled(OrderId(36)));
    assert!(matches!(execution.events.as_slice(),[
        EventEnvelope{payload:EngineEvent::OrderCanceled{order_id:OrderId(36),reason:CancelReason::UserRequested,..},..},
        EventEnvelope{payload:EngineEvent::ReservationReleased{order_id:OrderId(36),reason:ReservationReleaseReason::UserCancel,released:Some(assets),..},..}
    ] if assets.len()==1 && assets[0].atoms==1));
    let snap = restored
        .handle()
        .take_snapshot(12)
        .unwrap()
        .recv_timeout(WAIT)
        .unwrap()
        .unwrap();
    assert!(snap.order(MARKET, OrderId(36)).unwrap().is_none());
    assert!(snap.order(MARKET, OrderId(38)).unwrap().is_some());
    let bytes = snap.encode_snapshot(trusted.max_bytes).unwrap();
    assert_eq!(
        SnapshotImage::decode_snapshot(&bytes, &trusted)
            .unwrap()
            .state_digest(trusted.max_bytes)
            .unwrap(),
        snap.state_digest(trusted.max_bytes).unwrap()
    );
    restored.shutdown(WAIT).unwrap();
    drop(restored);
    let mut again = Shard::recover_group_local_with_genesis(
        dir.0.join("wal"),
        dir.0.join("manifest"),
        &snapshots,
        &trusted,
        &policies,
        (4, 1000),
    )
    .unwrap();
    let retry = run(&again, &cancel).unwrap();
    assert!(retry.deduplicated && retry.events.is_empty());
    assert_eq!(
        (retry.seq, retry.outcome),
        (execution.seq, execution.outcome)
    );
    direct_gateway_place(&again.handle(), &dir.0, &published);
    again.shutdown(WAIT).unwrap();
}

/// 直接使用已认证会话，在非零的全局控制纪元准备新意图并提交到真实持久分片。
fn direct_gateway_place(
    shard: &ShardHandle,
    directory: &std::path::Path,
    published: &PublishedEpoch,
) {
    struct Auth;
    impl GatewayAuthenticator for Auth {
        fn authenticate(&self, _: u64, _: &[u8]) -> Option<GatewayPrincipal> {
            Some(GatewayPrincipal {
                user: UserId(1),
                stp_key: StpKey::new(0),
            })
        }
    }
    let journal = SessionControlJournal::create_with_policy(
        directory.join("direct.session"),
        UserId(1),
        OriginSessionId(101),
        102,
        vec![SessionTarget {
            market: MARKET,
            route_epoch: 1,
            command_id: CommandId(103),
        }],
        1,
        false,
    )
    .unwrap();
    let mut session = GatewaySession::open(&Auth, 1, b"", journal, 4, 32).unwrap();
    let frame = matching_protocol::encode_place_request(
        1,
        &matching_protocol::PlaceRequest {
            key: 99,
            market: MARKET.0,
            price: 10,
            qty: 1,
            stp_key: 0,
            user_control_epoch: 1,
            side: matching_protocol::PlaceSide::Sell,
            stp: matching_protocol::StpPolicy::None,
            post_only: false,
            client_order_id: None,
        },
        1024,
        32,
    )
    .unwrap();
    let bucket = matching_protocol::BucketConfig {
        capacity: 8,
        refill_interval: 1,
    };
    let mut limiter =
        matching_protocol::GatewayRateLimiters::new(bucket, bucket, bucket, 4).unwrap();
    let mut store = GatewayIntentStore::create(directory.join("direct.intents"), 4, 1024).unwrap();
    assert!(matches!(
        session.prepare_wire_for_shard(
            (&frame, 1024),
            shard,
            &mut store,
            &mut limiter,
            0,
            || panic!("被栅栏拒绝的请求分配了身份")
        ),
        Err(GatewayError::Rejected(RejectReason::ControlFenceActive))
    ));
    published
        .fencing
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (_, prepared) = session
        .prepare_wire_for_shard((&frame, 1024), shard, &mut store, &mut limiter, 0, || {
            Ok((CommandId(99), OrderId(99)))
        })
        .unwrap();
    let Command::PlaceLimit(input) = place(99, UserId(1), 1).payload else {
        unreachable!()
    };
    let token = store
        .authorize(&prepared, || Ok(input.reservation))
        .unwrap();
    let seq = store.client_seq(prepared.command_id()).unwrap();
    let reply = session
        .submit_place(&prepared, token, seq, &mut limiter, 0, shard)
        .unwrap()
        .recv_timeout(WAIT)
        .unwrap()
        .unwrap();
    assert_eq!(reply.outcome, FormalOutcome::Placed(OrderId(99)));
}
