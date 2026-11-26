//! 验证网络请求在路由版本并发变化时的重试身份和持久顺序。
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, ClusterId, LogicalTime, MarketId, MarketMath, OriginSessionId,
    ReservationId, ShardId, StpKey, UserId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use std::sync::mpsc::sync_channel;

struct Auth;
impl crate::GatewayAuthenticator for Auth {
    fn authenticate(&self, key: u64, secret: &[u8]) -> Option<crate::GatewayPrincipal> {
        (key == 7 && secret == b"secret").then_some(crate::GatewayPrincipal {
            user: UserId(7),
            stp_key: StpKey::new(0),
        })
    }
}

#[test]
fn tcp_stale_route_retries_same_identity_without_wal_gap() {
    stale_request_crosses_route_change(false);
}

#[test]
fn tcp_inflight_place_reselects_migrated_owner_and_reuses_authorization() {
    stale_request_crosses_route_change(true);
}

/// 在网关取得旧路由后切换纪元或所有者，验证单次授权、命令身份和实际落盘位置。
fn stale_request_crosses_route_change(move_market: bool) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("tide-gateway-route-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::create_dir(dir.join("wal")).unwrap();
    std::fs::create_dir(dir.join("manifest")).unwrap();
    /// 测试结束时清理的临时目录。
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let target = self.0.canonicalize();
            let parent = std::env::temp_dir().canonicalize();
            let (Ok(target), Ok(parent)) = (target, parent) else {
                eprintln!("无法确认测试目录，保留目录");
                return;
            };
            if target.parent() != Some(parent.as_path())
                || !target
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("tide-gateway-route-"))
            {
                eprintln!("测试目录不在预期临时目录中，保留目录");
                return;
            }
            if let Err(error) = std::fs::remove_dir_all(target) {
                eprintln!("测试目录清理失败: {error}");
            }
        }
    }
    let _cleanup = Cleanup(dir.clone());
    let (market, shard_id, cluster) = (
        MarketId(81),
        ShardId(82),
        ClusterId(if move_market { 1038 } else { 963 }),
    );
    let routes = |epoch| {
        crate::RouteTable::new([crate::MarketRoute {
            market_id: market,
            shard_id,
            route_epoch: epoch,
            state: crate::RouteState::Active,
        }])
        .unwrap()
    };
    let signing = SigningKey::from_bytes(&[7; 32]);
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let config = crate::PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        crate::PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let mut shard = crate::Shard::start_with_durable_place_support(
        shard_id,
        cluster,
        routes(1),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (crate::IdempotencyConfig::default(), 1, 8, config),
        (dir.join("wal"), dir.join("manifest")),
    )
    .unwrap();
    let target_id = ShardId(83);
    let mut target = if move_market {
        for name in [
            "target-wal",
            "target-manifest",
            "source-snapshots",
            "target-snapshots",
            "archive",
        ] {
            std::fs::create_dir(dir.join(name)).unwrap();
        }
        let target_config = crate::PlaceSupportConfig::empty_for_migration(
            RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
            crate::PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap();
        Some(
            crate::Shard::start_with_durable_place_support(
                target_id,
                cluster,
                routes(1),
                vec![],
                (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
                (crate::IdempotencyConfig::default(), 1, 8, target_config),
                (dir.join("target-wal"), dir.join("target-manifest")),
            )
            .unwrap(),
        )
    } else {
        None
    };
    let mut shards = vec![shard.handle()];
    shards.extend(target.iter().map(crate::Shard::handle));
    let control_dir = dir.join("control");
    std::fs::create_dir(&control_dir).unwrap();
    let store =
        crate::RouteControlStore::create_for_cluster(&control_dir, cluster, routes(1), 8, 8)
            .unwrap();
    let mut coordinator = crate::RouteCoordinator::new(store, shards.clone()).unwrap();
    let journal = crate::SessionControlJournal::create_with_policy(
        dir.join("session.log"),
        UserId(7),
        OriginSessionId(22),
        199,
        vec![crate::SessionTarget {
            market,
            route_epoch: 1,
            command_id: CommandId(51),
        }],
        1,
        false,
    )
    .unwrap();
    let mut gateway = crate::GatewaySession::open(&Auth, 7, b"secret", journal, 2, 32).unwrap();
    let (entered, entered_rx) = sync_channel(1);
    let (release, release_rx) = sync_channel(1);
    gateway.route_snapshot_hook = Some(Box::new(move |epoch| {
        entered.send(epoch).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let intents = crate::GatewayIntentStore::create(dir.join("intents.log"), 2, 1024).unwrap();
    let bucket = matching_protocol::BucketConfig {
        capacity: 8,
        refill_interval: 1,
    };
    let shared = GatewayShared::new(
        intents,
        matching_protocol::GatewayRateLimiters::new(bucket, bucket, bucket, 2).unwrap(),
        || 0,
    );
    let health = crate::Health::new(shards.to_vec()).unwrap();
    for dependency in [
        crate::HealthDependency::Recovery,
        crate::HealthDependency::Persistence,
        crate::HealthDependency::Control,
    ] {
        health.set_healthy(dependency, true);
    }
    let wire = matching_protocol::encode_place_request(
        42,
        &matching_protocol::PlaceRequest {
            key: 3,
            market: market.0,
            price: 10,
            qty: 2,
            stp_key: 0,
            user_control_epoch: 0,
            side: matching_protocol::PlaceSide::Buy,
            stp: matching_protocol::StpPolicy::None,
            post_only: false,
            client_order_id: None,
        },
        1024,
        32,
    )
    .unwrap();
    let mut token = ReservationToken {
        token_id: 3,
        command_id: CommandId(3),
        reservation_id: ReservationId(3),
        order_id: OrderId(3),
        user_id: UserId(7),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 20,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let allocations = std::sync::atomic::AtomicUsize::new(0);
    let authorizations = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut transport = GatewayTransport {
                session: &mut gateway,
                shared: &shared,
                health: &health,
                shards: &shards,
                max_frame: 1024,
                timeout: Duration::from_secs(5),
                allocate: || {
                    allocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok((CommandId(3), OrderId(3)))
                },
                authorize: |_: &crate::PreparedPlace| {
                    authorizations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(token.clone())
                },
            };
            transport.serve(&mut socket).unwrap();
        });
        let mut client = std::net::TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.write_all(&wire).unwrap();
        assert_eq!(entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        if move_market {
            let migration = crate::MigrationStorage {
                archive: dir.join("archive"),
                source_snapshot: (dir.join("source-snapshots"), 1),
                target_snapshot: (dir.join("target-snapshots"), 1),
                max_bytes: 1024 * 1024,
                timeout: Duration::from_secs(5),
            };
            migration
                .migrate(
                    &mut coordinator,
                    crate::MigrationId {
                        cluster,
                        market,
                        source: shard_id,
                        target: target_id,
                        old_epoch: 1,
                    },
                    Some(&crate::FeedBuilder::new(market)),
                )
                .unwrap();
        } else {
            coordinator
                .update(1, routes(2), Duration::from_secs(5))
                .unwrap();
        }
        release.send(()).unwrap();
        let mut header = [0; matching_protocol::HEADER_LEN];
        client.read_exact(&mut header).unwrap();
        let len = matching_protocol::frame_length(&header, 1024).unwrap();
        let mut reply = header.to_vec();
        reply.resize(len, 0);
        client
            .read_exact(&mut reply[matching_protocol::HEADER_LEN..])
            .unwrap();
        let (request_id, accepted) = matching_protocol::decode_acceptance(&reply, 1024).unwrap();
        assert_eq!(
            (
                request_id,
                accepted.command_id,
                accepted.order_id,
                accepted.shard_seq
            ),
            (42, 3, 3, 1)
        );
        drop(client);
        server.join().unwrap();
    });
    assert_eq!(allocations.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(authorizations.load(std::sync::atomic::Ordering::SeqCst), 1);
    let observed = shard.handle().observation();
    if let Some(target) = target.as_mut() {
        assert_eq!((observed.commands, observed.rejects), (1, 1));
        let accepted = target.handle().observation();
        assert_eq!((accepted.commands, accepted.rejects), (1, 0));
        target.shutdown(Duration::from_secs(5)).unwrap();
    } else {
        assert_eq!((observed.commands, observed.rejects), (2, 1));
    }
    shard.shutdown(Duration::from_secs(5)).unwrap();
    drop(coordinator);
    let wal = if move_market {
        assert!(
            matching_persistence::scan_segments(dir.join("wal"))
                .unwrap()
                .records
                .is_empty()
        );
        dir.join("target-wal")
    } else {
        dir.join("wal")
    };
    let records = matching_persistence::scan_segments(wal).unwrap().records;
    assert_eq!(records.len(), 1);
    let command = matching_domain::decode_wal_command(&records[0].payload).unwrap();
    assert_eq!(
        (command.command_id, command.route_epoch),
        (CommandId(3), Some(2))
    );
}
