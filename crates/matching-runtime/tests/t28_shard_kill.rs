//! 验证分片开关跨本地市场的确定性撤单及持久恢复。
mod t22_support;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    ControlCoordinator, ControlStore, IdempotencyConfig, MarketRoute, PlaceLimits,
    PlaceSupportConfig, RouteState, RouteTable, Shard,
};
use std::time::{Duration, Instant};

fn retry<T>(mut action: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match action() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(error) => panic!("请求未入队: {error:?}"),
        }
    }
}

#[test]
fn shard_kill_cancels_markets_in_order_and_replays() {
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    let cluster = ClusterId(41);
    let shard_id = ShardId(41);
    let store = ControlStore::new(temp.0.join("primary.lease"), cluster, shard_id);
    let lease = std::sync::Arc::new(store.acquire_primary().unwrap());
    let epoch = lease.epoch();
    let routes = RouteTable::new([
        MarketRoute {
            market_id: MarketId(1),
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        },
        MarketRoute {
            market_id: MarketId(2),
            shard_id,
            route_epoch: 1,
            state: RouteState::Active,
        },
    ])
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(MarketId(1), policy), (MarketId(2), policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .with_primary_lease(lease.clone());
    let policies = [
        (MarketId(1), QueuePriorityPolicy::try_new(1).unwrap()),
        (MarketId(2), QueuePriorityPolicy::try_new(1).unwrap()),
    ];
    let mut shard = Shard::start_group_place_support(
        shard_id,
        cluster,
        routes.clone(),
        policies.to_vec(),
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), epoch, 8, place.clone()),
        ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
    )
    .unwrap();
    let mut live = Vec::new();
    for (id, market) in [(1_u128, MarketId(2)), (2, MarketId(1))] {
        let user = UserId(id as u64);
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
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
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let cmd = CommandEnvelope {
            command_id: CommandId(id),
            session_id: 1,
            client_seq: id as u64,
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
                    side: Side::Sell,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(1).unwrap(),
                },
                client_order_id: None,
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        };
        live.push(
            retry(|| shard.handle().submit_command(cmd.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
    }
    let (id, action) = ControlCoordinator::shard_kill_subcommand(&lease, CommandId(50), true, true);
    let control = CommandEnvelope {
        command_id: id,
        session_id: 2,
        client_seq: 1,
        market_id: None,
        user_id: None,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ShardKillSwitch(action),
    };
    let killed = retry(|| shard.handle().submit_command(control.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let cancellations: Vec<_> = killed
        .events
        .iter()
        .filter_map(|event| match event.payload {
            EngineEvent::OrderCanceled {
                order_id,
                reason: CancelReason::KillSwitch,
                ..
            } => Some((event.market_id.unwrap(), order_id)),
            _ => None,
        })
        .collect();
    assert_eq!(
        cancellations,
        vec![(MarketId(1), OrderId(2)), (MarketId(2), OrderId(1))]
    );
    assert_eq!(
        killed
            .events
            .iter()
            .filter(|event| matches!(
                event.payload,
                EngineEvent::ReservationReleased {
                    reason: ReservationReleaseReason::KillSwitch,
                    released: Some(_),
                    ..
                }
            ))
            .count(),
        2
    );
    live.push(killed);
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: epoch,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 2,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &policies,
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    let mut hash = matching_runtime::EventHasher::new();
    assert_eq!(replay.executions.len(), live.len());
    for (actual, expected) in replay.executions.iter().zip(&live) {
        assert_eq!(actual.events, expected.events);
        for event in &expected.events {
            hash.update(event).unwrap();
        }
    }
    assert_eq!(replay.event_hash, hash.finish());
    drop(manager);
    let mut recovered = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        &snapshots,
        &trusted,
        &policies,
        (4, 10_000),
    )
    .unwrap();
    let mut token = ReservationToken {
        token_id: 3,
        command_id: CommandId(3),
        reservation_id: ReservationId(3),
        order_id: OrderId(3),
        user_id: UserId(3),
        market_id: MarketId(1),
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
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let order = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 3,
        client_seq: 1,
        market_id: Some(MarketId(1)),
        user_id: Some(UserId(3)),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(3),
                user_id: UserId(3),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    };
    let rejected = retry(|| recovered.handle().submit_command(order.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected.outcome,
        matching_runtime::FormalOutcome::Rejected(RejectReason::KillSwitchActive)
    );
    let (id, clear) =
        ControlCoordinator::shard_kill_subcommand(&lease, CommandId(51), false, false);
    let clear = CommandEnvelope {
        command_id: id,
        session_id: 2,
        client_seq: 2,
        market_id: None,
        user_id: None,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ShardKillSwitch(clear),
    };
    retry(|| recovered.handle().submit_command(clear.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let mut accepted = order;
    accepted.command_id = CommandId(4);
    accepted.client_seq = 2;
    if let Command::PlaceLimit(ref mut place) = accepted.payload {
        place.order.order_id = OrderId(4);
        place.reservation.token_id = 4;
        place.reservation.command_id = CommandId(4);
        place.reservation.reservation_id = ReservationId(4);
        place.reservation.order_id = OrderId(4);
        place.reservation.signature = signing
            .sign(&place.reservation.canonical_signing_payload().unwrap())
            .to_bytes();
    }
    let accepted = retry(|| recovered.handle().submit_command(accepted.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(matches!(
        accepted.outcome,
        matching_runtime::FormalOutcome::Placed(OrderId(4))
    ));
    let mut writer =
        matching_runtime::SnapshotWriter::start(recovered.handle(), snapshots.clone(), 1024 * 1024)
            .unwrap();
    let checkpoint = retry(|| recovered.handle().take_snapshot(2))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer
        .submit(2, checkpoint)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.shutdown().unwrap();
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    drop(recovered);
    drop(replay);
    drop(image);
    let routes = trusted.routes.clone();
    drop(trusted);
    drop(lease);
    let next = std::sync::Arc::new(store.acquire_primary().unwrap());
    assert_eq!(next.epoch(), epoch + 1);
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(MarketId(1), policy), (MarketId(2), policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .with_primary_lease(next.clone());
    let next_trusted = matching_runtime::SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: next.epoch(),
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 2,
        max_bytes: 1024 * 1024,
    };
    let mut restarted = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        &snapshots,
        &next_trusted,
        &policies,
        (4, 10_000),
    )
    .unwrap();
    assert!(
        retry(|| restarted.handle().order(MarketId(1), OrderId(4)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .is_some()
    );
    let (id, action) = ControlCoordinator::shard_kill_subcommand(&next, CommandId(52), true, false);
    let control = CommandEnvelope {
        command_id: id,
        session_id: 4,
        client_seq: 1,
        market_id: None,
        user_id: None,
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ShardKillSwitch(action),
    };
    let killed = retry(|| restarted.handle().submit_command(control.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        killed.outcome,
        matching_runtime::FormalOutcome::ShardControl
    );
    restarted.shutdown(Duration::from_secs(5)).unwrap();
    drop(restarted);
    let routes = next_trusted.routes.clone();
    drop(next_trusted);
    drop(next);
    let third = std::sync::Arc::new(store.acquire_primary().unwrap());
    assert_eq!(third.epoch(), epoch + 2);
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(MarketId(1), policy), (MarketId(2), policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .with_primary_lease(third.clone());
    let third_trusted = matching_runtime::SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: third.epoch(),
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 2,
        max_bytes: 1024 * 1024,
    };
    let mut last = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        &snapshots,
        &third_trusted,
        &policies,
        (4, 10_000),
    )
    .unwrap();
    let duplicate = retry(|| last.handle().submit_command(control.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(matches!(
        duplicate,
        Err(matching_runtime::FormalError::Rejected(
            RejectReason::StalePrimaryEpoch
        ))
    ));
    let (id, refreshed) =
        ControlCoordinator::shard_kill_subcommand(&third, CommandId(52), true, false);
    assert_eq!(id, control.command_id);
    let mut retry_control = control;
    retry_control.payload = Command::ShardKillSwitch(refreshed);
    let duplicate = retry(|| last.handle().submit_command(retry_control.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.seq, killed.seq);
    last.shutdown(Duration::from_secs(5)).unwrap();
}
