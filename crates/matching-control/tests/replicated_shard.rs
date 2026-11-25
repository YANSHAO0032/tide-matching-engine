//! 验证正式分片通过认证复制写入双侧真实 WAL。
use matching_control::{
    EtcdClient, EtcdRouteAuthority, FencedLog, ReplicatedAppender, TlsReplicaStream,
};
use matching_domain::{
    AssetId, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime, MarketId, MarketMath,
    MassCancelShard, ShardCancelScope, ShardId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableBatcher, FencedStandbyReplica, FileManifestStorage,
    ManifestManager, Profile, RegisteredAppender, ReplicaEpochStore, ReplicationMode,
    SegmentWriter, StandbyReplica,
};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteCoordinator, RouteState,
    RouteTable, Shard, ShardError, SnapshotRestoreConfig,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn fee_update_command(id: u128, market: MarketId) -> CommandEnvelope {
    let tier = matching_domain::FeeTier {
        maker_bps: 0,
        taker_bps: 0,
        rounding: matching_domain::FeeRounding::TowardZero,
    };
    let schedule =
        matching_domain::FeeSchedule::new(1, matching_domain::FeeAssetPolicy::Quote, [(1, tier)])
            .unwrap();
    CommandEnvelope {
        command_id: CommandId(id),
        session_id: u64::try_from(id).unwrap(),
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::UpdateFeeSchedule(Box::new(matching_domain::UpdateFeeSchedule {
            schedule,
            history: Vec::new(),
        })),
    }
}

fn signed_limit_command(
    id: u128,
    market: MarketId,
    side: matching_domain::Side,
) -> CommandEnvelope {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, LimitOrder, OrderId, PlaceLimitOrder, Price, Qty, ReservationId,
        ReservationToken, StpKey, StpMode, UserId,
    };
    let user = UserId(u64::try_from(id).unwrap());
    let mut reservation = ReservationToken {
        token_id: id,
        command_id: CommandId(id),
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: user,
        market_id: market,
        reserves: vec![AssetReserve {
            asset: if side == matching_domain::Side::Sell {
                AssetId(1)
            } else {
                AssetId(2)
            },
            atoms: if side == matching_domain::Side::Sell {
                2
            } else {
                20
            },
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
        command_id: CommandId(id),
        session_id: u64::try_from(id).unwrap(),
        client_seq: 1,
        market_id: Some(market),
        user_id: Some(user),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(id),
                user_id: user,
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation,
        })),
    }
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_commits_only_after_authenticated_standby_wal() {
    run_shard_with_replication(false, false, false, false, false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_stops_when_writer_lease_is_revoked_during_standby_sync() {
    run_shard_with_replication(true, false, false, false, false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_recovery_rejects_bad_digest_before_replica_resume() {
    run_shard_with_replication(false, true, false, false, false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_recovers_committed_tail_after_digest_write_failure() {
    run_shard_with_replication(false, false, true, false, false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_recovers_first_commit_without_digest_checkpoint() {
    run_shard_with_replication(false, false, false, true, false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_discards_local_tail_not_synced_by_standby() {
    run_shard_with_replication(false, false, false, false, true, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn bad_replay_digest_preserves_local_ahead_wal() {
    run_shard_with_replication(false, true, false, false, true, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn shard_recovers_when_local_wal_is_behind_authority() {
    run_shard_with_replication(false, false, true, false, false, true);
}

fn run_shard_with_replication(
    revoke_during_sync: bool,
    bad_digest: bool,
    lag_digest: bool,
    first_digest_failure: bool,
    local_ahead: bool,
    local_behind: bool,
) {
    struct DigestFault<A> {
        /// 被测试包装器持有的实际持久化组件。
        inner: A,
        /// 需要注入摘要登记失败的分片序号。
        fail_seq: Option<u64>,
    }
    impl<A: matching_persistence::DurableAppender> matching_persistence::DurableAppender
        for DigestFault<A>
    {
        fn would_rotate(&self, r: &matching_persistence::WalRecord) -> std::io::Result<bool> {
            self.inner.would_rotate(r)
        }
        fn append(&mut self, r: &matching_persistence::WalRecord) -> std::io::Result<()> {
            self.inner.append(r)
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.inner.sync()
        }
        fn take_replica_ack(&mut self) -> Option<matching_persistence::ReplicaDurableAck> {
            self.inner.take_replica_ack()
        }
        fn replication_ready(&self) -> Option<bool> {
            self.inner.replication_ready()
        }
        fn digest_checkpoint_required(&self) -> bool {
            true
        }
        fn checkpoint_digest(
            &mut self,
            seq: u64,
            state: [u8; 32],
            events: [u8; 32],
        ) -> std::io::Result<()> {
            if self.fail_seq == Some(seq) {
                return Err(std::io::Error::other("模拟检查点登记前故障"));
            }
            self.inner.checkpoint_digest(seq, state, events)
        }
        fn commit_snapshot(&mut self, id: u64, seq: u64) -> std::io::Result<()> {
            self.inner.commit_snapshot(id, seq)
        }
    }
    struct SyncGate {
        /// 被测试包装器持有的实际持久化组件。
        inner: RegisteredAppender<FileManifestStorage>,
        /// 通知测试线程备节点同步已经开始。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 等待测试线程放行备节点同步。
        release: std::sync::mpsc::Receiver<()>,
        /// 控制第二次同步是否在落盘前失败。
        fail_second: bool,
        /// 记录备节点同步调用次数。
        sync_count: usize,
    }
    impl matching_persistence::DurableAppender for SyncGate {
        fn would_rotate(&self, record: &matching_persistence::WalRecord) -> std::io::Result<bool> {
            self.inner.would_rotate(record)
        }
        fn append(&mut self, record: &matching_persistence::WalRecord) -> std::io::Result<()> {
            self.inner.append(record)
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.sync_count += 1;
            self.entered.send(()).map_err(std::io::Error::other)?;
            self.release
                .recv_timeout(Duration::from_secs(5))
                .map_err(std::io::Error::other)?;
            if self.fail_second && self.sync_count == 2 {
                return Err(std::io::Error::other("模拟备节点同步前故障"));
            }
            self.inner.sync()
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-replicated-shard-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    for name in [
        "primary-wal",
        "primary-manifest",
        "standby-wal",
        "standby-manifest",
        "control",
        "snapshots",
    ] {
        std::fs::create_dir(root.join(name)).unwrap();
    }
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let trade_crash = std::env::var_os("TIDE_T34_SHARD_TRADE").is_some();
    let lost_reply = std::env::var_os("TIDE_T34_LOST_REPLY").is_some();
    let cluster = ClusterId(if lost_reply {
        123
    } else if trade_crash {
        122
    } else if std::env::var_os("TIDE_T34_SHARD_CRASH_MARKER").is_some() {
        121
    } else if local_behind {
        120
    } else if local_ahead && bad_digest {
        119
    } else if local_ahead {
        118
    } else if first_digest_failure {
        117
    } else if lag_digest {
        116
    } else if revoke_during_sync {
        114
    } else if bad_digest {
        115
    } else {
        111
    });
    let shard_id = ShardId(112);
    let market = MarketId(113);
    let log = FencedLog::new(
        client.clone(),
        &format!("/tide-test/replicated-shard/{}-{stamp}", std::process::id()),
        cluster,
        shard_id,
    )
    .unwrap();
    let client_cert = include_bytes!("fixtures/test-client.der");
    let server_cert = include_bytes!("fixtures/test-server.der");
    let ca = include_bytes!("fixtures/test-ca.der");
    let writer = log.acquire_bound(30, client_cert).unwrap();
    let identity = writer.replica_identity();
    let client_config = Arc::new(
        matching_control::replica_client_config(
            &[ca],
            vec![CertificateDer::from(client_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-client-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let server_config = Arc::new(
        matching_control::replica_server_config(
            &[ca],
            vec![CertificateDer::from(server_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-server-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let local = RegisteredAppender::new(
        SegmentWriter::new(root.join("standby-wal"), 4096).unwrap(),
        ManifestManager::<FileManifestStorage>::open(root.join("standby-manifest")).unwrap(),
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
    let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let batcher = DurableBatcher::new_with_config(
        SyncGate {
            inner: local,
            entered,
            release: release_rx,
            fail_second: local_ahead,
            sync_count: 0,
        },
        config,
        identity.epoch,
    )
    .unwrap();
    let replica = StandbyReplica::new(batcher, cluster.0, shard_id.0, identity.epoch, 8).unwrap();
    let epochs =
        ReplicaEpochStore::create(root.join("epoch"), cluster.0, shard_id.0, identity.epoch, 0)
            .unwrap();
    let mut standby = FencedStandbyReplica::new(replica, epochs).unwrap();
    let deadline = || Instant::now() + Duration::from_secs(5);
    matching_control::activate_replica_writer(&log, &mut standby, identity, deadline()).unwrap();
    standby
        .confirm_reconciled(0, |_, _, _| {
            log.verify_bound_replica_writer(identity, client_cert, deadline())
        })
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut recovery_digests = None;
    let mut recovery_events = Vec::new();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream =
                TlsReplicaStream::accept(socket, server_config.clone(), client_cert, deadline())
                    .unwrap();
            matching_control::receive_authenticated_replication_batch(
                &mut stream,
                &log,
                &mut standby,
                identity,
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
            if lag_digest || local_ahead || trade_crash {
                let result = matching_control::receive_authenticated_replication_batch(
                    &mut stream,
                    &log,
                    &mut standby,
                    identity,
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(5),
                );
                if local_ahead {
                    assert!(result.is_err());
                } else {
                    result.unwrap();
                }
            }
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let stream = TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let place = PlaceSupportConfig::new(
            if trade_crash {
                RiskKeySet::new(
                    1,
                    [(
                        1,
                        ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key(),
                    )],
                )
                .unwrap()
            } else {
                RiskKeySet::new(1, []).unwrap()
            },
            [(market, policy)],
            PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap()
        .require_route_authority();
        let mut shard = Shard::start_replicated_group_place_support(
            (shard_id, cluster),
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), identity.epoch, 8, place),
            (
                (root.join("primary-wal"), root.join("primary-manifest")),
                (1, 1_000),
            ),
            |local| {
                ReplicatedAppender::new_authenticated(
                    local,
                    stream,
                    writer,
                    32 * 1024 * 1024,
                    Duration::from_secs(5),
                )
                .map(|inner| DigestFault {
                    inner,
                    fail_seq: if first_digest_failure {
                        Some(1)
                    } else if lag_digest {
                        Some(2)
                    } else {
                        None
                    },
                })
            },
        )
        .unwrap();
        let handle = shard.handle();
        let mut authority = EtcdRouteAuthority::create(
            client.clone(),
            &format!("/tide-test/replicated-route/{}-{stamp}", std::process::id()),
            cluster,
            routes,
        )
        .unwrap();
        authority.acquire_lease(30).unwrap();
        let coordinator =
            RouteCoordinator::with_authority(authority, vec![handle.clone()]).unwrap();
        let mut command = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
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
        if std::env::var_os("TIDE_T34_SHARD_CRASH_MARKER").is_some() {
            command = fee_update_command(1, market);
        }
        let end = deadline();
        let before = loop {
            match handle.submit_command(command.clone()) {
                Ok(receiver) => break receiver,
                Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
                Err(error) => panic!("入队失败：{error:?}"),
            }
        };
        assert!(matches!(
            before.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                matching_domain::RejectReason::EngineBusy
            ))
        ));
        assert_eq!(log.committed().unwrap(), 0);
        let monitor = coordinator
            .start_monitor(Duration::from_millis(200))
            .unwrap();
        let end = deadline();
        let receiver = loop {
            match handle.submit_command(command.clone()) {
                Ok(receiver) => break receiver,
                Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
                Err(error) => panic!("入队失败：{error:?}"),
            }
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(log.committed().unwrap(), 0);
        let receiver = if lost_reply {
            drop(receiver);
            None
        } else {
            Some(receiver)
        };
        if revoke_during_sync {
            let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
            let endpoint = endpoints.split(',').next().unwrap();
            ureq::post(&format!("{endpoint}/v3/lease/revoke"))
                .set("Content-Type", "application/json")
                .send_string(&format!("{{\"ID\":\"{}\"}}", identity.lease_id))
                .unwrap();
        }
        release.send(()).unwrap();
        if revoke_during_sync {
            assert!(matches!(
                receiver.unwrap().recv_timeout(Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
            ));
            assert_eq!(log.committed().unwrap(), 0);
            assert!(matches!(
                shard.shutdown(Duration::from_secs(5)),
                Err(ShardError::Failed)
            ));
        } else if first_digest_failure {
            assert!(matches!(
                receiver.unwrap().recv_timeout(Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
            ));
            assert_eq!(log.committed().unwrap(), 1);
            assert!(log.read_digest_checkpoint(1).unwrap().is_none());
            assert!(matches!(
                shard.shutdown(Duration::from_secs(5)),
                Err(ShardError::Failed)
            ));
            recovery_digests = Some(([0; 32], [0; 32]));
        } else {
            let events = if let Some(receiver) = receiver {
                let execution = receiver
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
                assert_eq!(execution.seq, 1);
                execution.events
            } else {
                vec![matching_domain::EventEnvelope {
                    cluster_id: cluster,
                    shard_id,
                    shard_seq: 1,
                    event_index: 0,
                    command_id: CommandId(1),
                    engine_time: 0,
                    market_id: Some(market),
                    payload: matching_domain::EngineEvent::FeeScheduleUpdated { version: 1 },
                }]
            };
            let mut event_hasher = matching_runtime::EventHasher::new();
            for event in &events {
                event_hasher.update(event).unwrap();
            }
            recovery_events = events;
            let until = deadline();
            let snapshot_reply = loop {
                match handle.take_snapshot(1) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                    Err(error) => panic!("快照入队失败：{error:?}"),
                }
            };
            let snapshot = snapshot_reply
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            recovery_digests = Some((
                snapshot.state_digest(1024 * 1024).unwrap(),
                event_hasher.finish(),
            ));
            let checkpoint = log.read_digest_checkpoint(1).unwrap().unwrap();
            assert_eq!(
                (checkpoint.state_digest, checkpoint.event_hash),
                recovery_digests.unwrap()
            );
            let mut snapshot_writer = matching_runtime::SnapshotWriter::start(
                handle.clone(),
                root.join("snapshots"),
                1024 * 1024,
            )
            .unwrap();
            snapshot_writer
                .submit(1, snapshot)
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            snapshot_writer.shutdown().unwrap();
            assert_eq!(log.committed().unwrap(), 1);
            if trade_crash {
                let maker = signed_limit_command(2, market, matching_domain::Side::Sell);
                let end = deadline();
                let reply = loop {
                    match handle.submit_command(maker.clone()) {
                        Ok(reply) => break reply,
                        Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
                        Err(error) => panic!("挂单入队失败：{error:?}"),
                    }
                };
                entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                release.send(()).unwrap();
                let result = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
                assert_eq!(result.seq, 2);
                assert!(matches!(
                    result.events.last().unwrap().payload,
                    matching_domain::EngineEvent::OrderOpened {
                        order_id: matching_domain::OrderId(2)
                    }
                ));
                assert_eq!(log.committed().unwrap(), 2);
            }
            if let Some(marker) = std::env::var_os("TIDE_T34_SHARD_CRASH_MARKER") {
                std::fs::write(
                    marker,
                    format!(
                        "{}\n{}\n{}\n{}",
                        root.display(),
                        stamp,
                        std::process::id(),
                        identity.lease_id
                    ),
                )
                .unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            if lag_digest || local_ahead {
                let mut tail = command.clone();
                tail.command_id = CommandId(2);
                tail.session_id = 2;
                let until = deadline();
                let reply = loop {
                    match handle.submit_command(tail.clone()) {
                        Ok(reply) => break reply,
                        Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                        Err(error) => panic!("尾部入队失败：{error:?}"),
                    }
                };
                entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                release.send(()).unwrap();
                assert!(matches!(
                    reply.recv_timeout(Duration::from_secs(5)),
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
                ));
                assert_eq!(log.committed().unwrap(), if local_ahead { 1 } else { 2 });
                assert!(log.read_digest_checkpoint(2).unwrap().is_none());
                assert!(matches!(
                    shard.shutdown(Duration::from_secs(5)),
                    Err(ShardError::Failed)
                ));
            } else {
                shard.shutdown(Duration::from_secs(5)).unwrap();
            }
        }
        drop(monitor);
        server.join().unwrap();
    });
    drop(standby);
    if let Some(digests) = recovery_digests {
        if local_behind {
            let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(
                root.join("primary-manifest"),
            )
            .unwrap();
            matching_persistence::reconcile_local_prefix(
                root.join("primary-wal"),
                &mut manager,
                1,
                cluster.0,
                shard_id.0,
                |records, through| {
                    assert_eq!(through, 1);
                    assert_eq!(records.len(), 1);
                    Ok(())
                },
            )
            .unwrap();
            drop(manager);
            assert_eq!(
                matching_persistence::recover_local(
                    root.join("primary-wal"),
                    root.join("primary-manifest"),
                    0,
                    cluster.0,
                    shard_id.0
                )
                .unwrap()
                .len(),
                1
            );
            assert_eq!(log.committed().unwrap(), 2);
        }
        if !first_digest_failure {
            std::fs::write(root.join("snapshots").join("snapshot-1.bin"), [0_u8]).unwrap();
        }
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
        let endpoint = endpoints.split(',').next().unwrap();
        ureq::post(&format!("{endpoint}/v3/lease/revoke"))
            .set("Content-Type", "application/json")
            .send_string(&format!("{{\"ID\":\"{}\"}}", identity.lease_id))
            .unwrap();
        let next = log.acquire_bound(30, client_cert).unwrap();
        let next_identity = next.replica_identity();
        assert_eq!(next_identity.epoch, identity.epoch + 1);
        let manager =
            ManifestManager::<FileManifestStorage>::open_recovery(root.join("standby-manifest"))
                .unwrap();
        let local = RegisteredAppender::recover_local(
            root.join("standby-wal"),
            4096,
            manager,
            0,
            cluster.0,
            shard_id.0,
        )
        .unwrap();
        let batcher = DurableBatcher::resume_local(local, config, identity.epoch).unwrap();
        let replica =
            StandbyReplica::new(batcher, cluster.0, shard_id.0, identity.epoch, 8).unwrap();
        let epochs = ReplicaEpochStore::open(root.join("epoch"), cluster.0, shard_id.0).unwrap();
        let mut standby = FencedStandbyReplica::new(replica, epochs).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::scope(|scope| {
            let server = scope.spawn(|| {
                let (socket, _) = listener.accept().unwrap();
                let mut stream = TlsReplicaStream::accept(
                    socket,
                    server_config.clone(),
                    client_cert,
                    deadline(),
                )
                .unwrap();
                assert_eq!(
                    matching_control::serve_authenticated_fenced_prefix(
                        &mut stream,
                        &log,
                        &mut standby,
                        next_identity,
                        0,
                        deadline()
                    )
                    .unwrap(),
                    if lag_digest { 2 } else { 1 }
                );
                let resumed = matching_control::receive_authenticated_replica_resume(
                    &mut stream,
                    &log,
                    &mut standby,
                    next_identity,
                    0,
                    deadline(),
                );
                if bad_digest {
                    assert!(resumed.is_err());
                    let mut record = log.read(1).unwrap().unwrap();
                    record.epoch = next_identity.epoch;
                    record.shard_seq = 2;
                    assert!(standby.append_batch(&[record], Instant::now()).is_err());
                    return;
                }
                assert_eq!(resumed.unwrap(), if lag_digest { 2 } else { 1 });
                matching_control::receive_authenticated_replication_batch(
                    &mut stream,
                    &log,
                    &mut standby,
                    next_identity,
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(5),
                )
                .unwrap();
            });
            let socket = std::net::TcpStream::connect(address).unwrap();
            let stream = TlsReplicaStream::connect(
                socket,
                client_config.clone(),
                ServerName::try_from("server.test".to_string()).unwrap(),
                server_cert,
                deadline(),
            )
            .unwrap();
            let routes = RouteTable::new([MarketRoute {
                market_id: market,
                shard_id,
                route_epoch: 1,
                state: RouteState::Active,
            }])
            .unwrap();
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
            .unwrap()
            .require_route_authority();
            let trusted = SnapshotRestoreConfig {
                cluster,
                shard: shard_id,
                primary_epoch: next_identity.epoch,
                routes: routes.clone(),
                user_capacity: 8,
                idempotency: IdempotencyConfig::default(),
                place: Some(place),
                expect_registry: true,
                max_markets: 1,
                max_bytes: 1024 * 1024,
            };
            let mut digests = digests;
            if bad_digest {
                digests.0[0] ^= 1;
            }
            let recovery = if bad_digest {
                matching_control::AuthenticatedRecovery::new(
                    next,
                    stream,
                    digests,
                    1024 * 1024,
                    32 * 1024 * 1024,
                    Duration::from_secs(5),
                )
            } else {
                matching_control::AuthenticatedRecovery::from_authority(
                    next,
                    stream,
                    1024 * 1024,
                    32 * 1024 * 1024,
                    Duration::from_secs(5),
                )
            }
            .unwrap();
            let result = Shard::recover_replicated_group_with_genesis(
                root.join("primary-wal"),
                root.join("primary-manifest"),
                root.join("snapshots"),
                &trusted,
                &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
                (1, 1_000),
                recovery,
            );
            if bad_digest {
                assert!(result.is_err());
                assert_eq!(log.committed().unwrap(), 1);
                server.join().unwrap();
                return;
            }
            let mut shard = result.unwrap();
            let mut authority = EtcdRouteAuthority::create(
                client.clone(),
                &format!("/tide-test/recovered-route/{}-{stamp}", std::process::id()),
                cluster,
                routes,
            )
            .unwrap();
            authority.acquire_lease(30).unwrap();
            let coordinator =
                RouteCoordinator::with_authority(authority, vec![shard.handle()]).unwrap();
            let monitor = coordinator
                .start_monitor(Duration::from_millis(200))
                .unwrap();
            if lag_digest || first_digest_failure {
                let retried_seq = if lag_digest { 2 } else { 1 };
                let tail = CommandEnvelope {
                    command_id: CommandId(u128::from(retried_seq)),
                    session_id: retried_seq,
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
                let until = deadline();
                let reply = loop {
                    match shard.handle().submit_command(tail.clone()) {
                        Ok(reply) => break reply,
                        Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                        Err(error) => panic!("尾部重试入队失败：{error:?}"),
                    }
                };
                let result = reply.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
                assert!(result.deduplicated);
                assert_eq!(result.seq, retried_seq);
                assert_eq!(log.committed().unwrap(), retried_seq);
            }
            let command = CommandEnvelope {
                command_id: CommandId(if lag_digest { 3 } else { 2 }),
                session_id: if lag_digest { 3 } else { 2 },
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
            let until = deadline();
            let receiver = loop {
                match shard.handle().submit_command(command.clone()) {
                    Ok(receiver) => break receiver,
                    Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                    Err(error) => panic!("入队失败：{error:?}"),
                }
            };
            let execution = receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            let expected_seq = if lag_digest { 3 } else { 2 };
            assert_eq!(execution.seq, expected_seq);
            let mut event_hasher = matching_runtime::EventHasher::new();
            for event in recovery_events.iter().chain(&execution.events) {
                event_hasher.update(event).unwrap();
            }
            let until = deadline();
            let snapshot_reply = loop {
                match shard.handle().take_snapshot(2) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                    Err(error) => panic!("快照入队失败：{error:?}"),
                }
            };
            let image = snapshot_reply
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            let checkpoint = log.read_digest_checkpoint(expected_seq).unwrap().unwrap();
            assert_eq!(
                checkpoint.state_digest,
                image.state_digest(1024 * 1024).unwrap()
            );
            assert_eq!(checkpoint.event_hash, event_hasher.finish());
            let until = deadline();
            let duplicate = loop {
                match shard.handle().submit_command(command.clone()) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                    Err(error) => panic!("重复命令入队失败：{error:?}"),
                }
            }
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
            assert!(duplicate.deduplicated);
            assert_eq!(duplicate.seq, expected_seq);
            assert_eq!(
                log.read_digest_checkpoint(expected_seq).unwrap(),
                Some(checkpoint)
            );
            assert_eq!(log.committed().unwrap(), expected_seq);
            shard.shutdown(Duration::from_secs(5)).unwrap();
            drop(monitor);
            server.join().unwrap();
        });
        drop(standby);
    }
    let primary = matching_persistence::recover_local(
        root.join("primary-wal"),
        root.join("primary-manifest"),
        0,
        cluster.0,
        shard_id.0,
    )
    .unwrap();
    let replica = matching_persistence::recover_local(
        root.join("standby-wal"),
        root.join("standby-manifest"),
        0,
        cluster.0,
        shard_id.0,
    )
    .unwrap();
    assert_eq!(
        primary.len(),
        if bad_digest && local_ahead {
            2
        } else if revoke_during_sync || bad_digest {
            1
        } else if lag_digest {
            3
        } else {
            2
        }
    );
    if bad_digest && local_ahead {
        assert_eq!(replica.len(), 1);
        assert_eq!(primary[0], replica[0]);
    } else {
        assert_eq!(primary, replica);
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
            .starts_with("tide-replicated-shard-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn killed_formal_primary_recovers_in_another_process() {
    run_killed_formal_primary(false, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn killed_primary_recovers_active_order_and_trades() {
    run_killed_formal_primary(true, false);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn killed_primary_recovers_command_whose_reply_was_lost() {
    run_killed_formal_primary(false, true);
}

fn run_killed_formal_primary(trade: bool, lost_reply: bool) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let parent_dir =
        std::env::temp_dir().join(format!("tide-shard-parent-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&parent_dir).unwrap();
    let marker = parent_dir.join("ready");
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "shard_commits_only_after_authenticated_standby_wal",
            "--ignored",
        ])
        .env("TIDE_T34_SHARD_CRASH_MARKER", &marker)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    if trade {
        command.env("TIDE_T34_SHARD_TRADE", "1");
    }
    if lost_reply {
        command.env("TIDE_T34_LOST_REPLY", "1");
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    let mut child = command.spawn().unwrap();
    let wait_until = Instant::now() + Duration::from_secs(15);
    while !marker.exists() && Instant::now() < wait_until {
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready = marker.exists();
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(ready);
    let marker_text = std::fs::read_to_string(&marker).unwrap();
    let mut parts = marker_text.lines();
    let root = std::path::PathBuf::from(parts.next().unwrap());
    let child_stamp = parts.next().unwrap();
    let child_pid = parts.next().unwrap();
    let lease = parts.next().unwrap();
    assert!(parts.next().is_none());
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
            .starts_with("tide-replicated-shard-")
    );
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let cluster = ClusterId(if lost_reply {
        123
    } else if trade {
        122
    } else {
        121
    });
    let shard_id = ShardId(112);
    let market = MarketId(113);
    let log = FencedLog::new(
        client.clone(),
        &format!("/tide-test/replicated-shard/{child_pid}-{child_stamp}"),
        cluster,
        shard_id,
    )
    .unwrap();
    let recovered_seq = if trade { 2 } else { 1 };
    assert_eq!(log.committed().unwrap(), recovered_seq);
    assert!(
        log.acquire_bound(10, include_bytes!("fixtures/test-client.der"))
            .is_err()
    );
    ureq::post(&format!(
        "{}/v3/lease/revoke",
        endpoints.split(',').next().unwrap()
    ))
    .set("Content-Type", "application/json")
    .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
    .unwrap();
    let writer = log
        .acquire_bound(30, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    let identity = writer.replica_identity();
    assert_eq!(identity.epoch, 2);
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("standby-manifest"))
            .unwrap();
    let local = RegisteredAppender::recover_local(
        root.join("standby-wal"),
        4096,
        manager,
        0,
        cluster.0,
        shard_id.0,
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
    let batcher = DurableBatcher::resume_local(local, config, 1).unwrap();
    let standby = StandbyReplica::new(batcher, cluster.0, shard_id.0, 1, 8).unwrap();
    let epochs = ReplicaEpochStore::open(root.join("epoch"), cluster.0, shard_id.0).unwrap();
    let mut standby = FencedStandbyReplica::new(standby, epochs).unwrap();
    let ca = include_bytes!("fixtures/test-ca.der");
    let server_cert = include_bytes!("fixtures/test-server.der");
    let client_cert = include_bytes!("fixtures/test-client.der");
    let server_config = Arc::new(
        matching_control::replica_server_config(
            &[ca],
            vec![CertificateDer::from(server_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-server-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let client_config = Arc::new(
        matching_control::replica_client_config(
            &[ca],
            vec![CertificateDer::from(client_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-client-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let deadline = || Instant::now() + Duration::from_secs(8);
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream =
                TlsReplicaStream::accept(socket, server_config, client_cert, deadline()).unwrap();
            assert_eq!(
                matching_control::serve_authenticated_fenced_prefix(
                    &mut stream,
                    &log,
                    &mut standby,
                    identity,
                    0,
                    deadline()
                )
                .unwrap(),
                recovered_seq
            );
            assert_eq!(
                matching_control::receive_authenticated_replica_resume(
                    &mut stream,
                    &log,
                    &mut standby,
                    identity,
                    0,
                    deadline()
                )
                .unwrap(),
                recovered_seq
            );
            matching_control::receive_authenticated_replication_batch(
                &mut stream,
                &log,
                &mut standby,
                identity,
                8,
                32 * 1024 * 1024,
                Duration::from_secs(8),
            )
            .unwrap();
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let stream = TlsReplicaStream::connect(
            socket,
            client_config,
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let place = PlaceSupportConfig::new(
            if trade {
                RiskKeySet::new(
                    1,
                    [(
                        1,
                        ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key(),
                    )],
                )
                .unwrap()
            } else {
                RiskKeySet::new(1, []).unwrap()
            },
            [(market, policy)],
            PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap()
        .require_route_authority();
        let trusted = SnapshotRestoreConfig {
            cluster,
            shard: shard_id,
            primary_epoch: identity.epoch,
            routes: routes.clone(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let recovery = matching_control::AuthenticatedRecovery::from_authority(
            writer,
            stream,
            1024 * 1024,
            32 * 1024 * 1024,
            Duration::from_secs(8),
        )
        .unwrap();
        let mut shard = Shard::recover_replicated_group_with_genesis(
            root.join("primary-wal"),
            root.join("primary-manifest"),
            root.join("snapshots"),
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (1, 1_000),
            recovery,
        )
        .unwrap();
        let mut authority = EtcdRouteAuthority::create(
            client.clone(),
            &format!(
                "/tide-test/cross-process-route/{}-{stamp}",
                std::process::id()
            ),
            cluster,
            routes,
        )
        .unwrap();
        authority.acquire_lease(30).unwrap();
        let coordinator =
            RouteCoordinator::with_authority(authority, vec![shard.handle()]).unwrap();
        let monitor = coordinator
            .start_monitor(Duration::from_millis(200))
            .unwrap();
        let make = |id| fee_update_command(id, market);
        let submit = |command: CommandEnvelope| {
            let end = deadline();
            loop {
                match shard.handle().submit_command(command.clone()) {
                    Ok(reply) => {
                        break reply.recv_timeout(Duration::from_secs(8)).unwrap().unwrap();
                    }
                    Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
                    Err(error) => panic!("命令入队失败：{error:?}"),
                }
            }
        };
        let duplicate = submit(if trade {
            signed_limit_command(2, market, matching_domain::Side::Sell)
        } else {
            make(1)
        });
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.seq, recovered_seq);
        if trade {
            let order = shard
                .handle()
                .order(market, matching_domain::OrderId(2))
                .unwrap()
                .recv_timeout(Duration::from_secs(8))
                .unwrap()
                .unwrap();
            assert!(order.is_some());
        }
        let execution = submit(if trade {
            signed_limit_command(3, market, matching_domain::Side::Buy)
        } else {
            make(2)
        });
        let final_seq = recovered_seq + 1;
        assert_eq!(execution.seq, final_seq);
        assert_eq!(log.committed().unwrap(), final_seq);
        if trade {
            let trades: Vec<_> = execution
                .events
                .iter()
                .filter_map(|event| match &event.payload {
                    matching_domain::EngineEvent::Trade(trade) => Some(trade),
                    _ => None,
                })
                .collect();
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].maker_order_id, matching_domain::OrderId(2));
            assert_eq!(trades[0].taker_order_id, matching_domain::OrderId(3));
            assert_eq!(
                trades[0].price,
                matching_domain::Price::try_new(10).unwrap()
            );
            assert_eq!(trades[0].qty, matching_domain::Qty::try_new(2).unwrap());
            assert!(execution.events.iter().any(|event| matches!(
                event.payload,
                matching_domain::EngineEvent::SettlementDelta(_)
            )));
            let remaining = shard
                .handle()
                .order(market, matching_domain::OrderId(2))
                .unwrap()
                .recv_timeout(Duration::from_secs(8))
                .unwrap()
                .unwrap();
            assert!(remaining.is_none());
        } else {
            assert_eq!(execution.events.len(), 1);
            assert!(matches!(
                execution.events[0].payload,
                matching_domain::EngineEvent::CommandRejected {
                    reason: matching_domain::RejectReason::FeeScheduleMismatch
                }
            ));
        }
        let checkpoint = log.read_digest_checkpoint(final_seq).unwrap().unwrap();
        assert_eq!(checkpoint.shard_seq, final_seq);
        let first_event = matching_domain::EventEnvelope {
            cluster_id: cluster,
            shard_id,
            shard_seq: 1,
            event_index: 0,
            command_id: CommandId(1),
            engine_time: 0,
            market_id: Some(market),
            payload: matching_domain::EngineEvent::FeeScheduleUpdated { version: 1 },
        };
        let mut event_hasher = matching_runtime::EventHasher::new();
        event_hasher.update(&first_event).unwrap();
        assert_eq!(
            log.read_digest_checkpoint(1).unwrap().unwrap().event_hash,
            event_hasher.clone().finish()
        );
        if trade {
            for (index, payload) in [
                matching_domain::EngineEvent::OrderAccepted {
                    order_id: matching_domain::OrderId(2),
                },
                matching_domain::EngineEvent::OrderOpened {
                    order_id: matching_domain::OrderId(2),
                },
            ]
            .into_iter()
            .enumerate()
            {
                event_hasher
                    .update(&matching_domain::EventEnvelope {
                        cluster_id: cluster,
                        shard_id,
                        shard_seq: 2,
                        event_index: u32::try_from(index).unwrap(),
                        command_id: CommandId(2),
                        engine_time: 0,
                        market_id: Some(market),
                        payload,
                    })
                    .unwrap();
            }
            assert_eq!(
                log.read_digest_checkpoint(2).unwrap().unwrap().event_hash,
                event_hasher.clone().finish()
            );
        }
        for event in &execution.events {
            event_hasher.update(event).unwrap();
        }
        assert_eq!(checkpoint.event_hash, event_hasher.finish());
        let until = deadline();
        let snapshot_reply = loop {
            match shard.handle().take_snapshot(2) {
                Ok(reply) => break reply,
                Err(ShardError::Busy) if Instant::now() < until => std::thread::yield_now(),
                Err(error) => panic!("快照入队失败：{error:?}"),
            }
        };
        let image = snapshot_reply
            .recv_timeout(Duration::from_secs(8))
            .unwrap()
            .unwrap();
        assert_eq!(
            checkpoint.state_digest,
            image.state_digest(1024 * 1024).unwrap()
        );
        shard.shutdown(Duration::from_secs(8)).unwrap();
        drop(monitor);
        server.join().unwrap();
    });
    drop(standby);
    let primary = matching_persistence::recover_local(
        root.join("primary-wal"),
        root.join("primary-manifest"),
        0,
        cluster.0,
        shard_id.0,
    )
    .unwrap();
    let replica = matching_persistence::recover_local(
        root.join("standby-wal"),
        root.join("standby-manifest"),
        0,
        cluster.0,
        shard_id.0,
    )
    .unwrap();
    assert_eq!(primary, replica);
    assert_eq!(primary.len(), if trade { 3 } else { 2 });
    std::fs::remove_dir_all(target).unwrap();
    std::fs::remove_file(marker).unwrap();
    std::fs::remove_dir(parent_dir).unwrap();
}
