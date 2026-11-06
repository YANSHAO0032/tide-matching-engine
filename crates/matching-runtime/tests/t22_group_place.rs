//! 验证正式限价入口的可信启动与客户端身份生命周期。
mod t22_support;
use matching_domain::{AssetId, ClusterId, LogicalTime, MarketId, MarketMath, ShardId};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
};

/// 在有界队列暂时繁忙时重试尚未入队的请求。
fn retry<T>(mut submit: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match submit() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < until => {
                std::thread::yield_now()
            }
            Err(error) => panic!("入口失败: {error:?}"),
        }
    }
}
#[test]
fn group_place_and_mass_cancel_write_before_reply() {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, Command, CommandEnvelope, CommandId, LimitOrder, MassCancelShard, OrderId,
        PlaceLimitOrder, Price, Qty, ReservationId, ReservationToken, ShardCancelScope, Side,
        StpKey, StpMode, UserId,
    };
    use std::time::Duration;
    let market = MarketId(124);
    let shard_id = ShardId(124);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap();
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
        keys,
        [(market, policy)],
        PlaceLimits {
            active_orders: 2,
            terminal_history: 2,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let temp = t22_support::TestDirectory::new();
    let base = &temp.0;
    let shared = base.join("wal");
    assert!(
        Shard::start_group_place_support(
            shard_id,
            ClusterId(1),
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 1, 8, config.clone()),
            ((shared.clone(), shared.join(".")), (8, 20_000))
        )
        .is_err()
    );
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
    let mut shard = Shard::start_group_place_support(
        shard_id,
        ClusterId(1),
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 1, 8, config.clone()),
        ((base.join("wal"), base.join("manifest")), (8, 200_000)),
    )
    .unwrap();
    let make = |command_id: u128, order_id: u128, client_seq: u64| {
        let mut token = ReservationToken {
            token_id: command_id,
            command_id: CommandId(command_id),
            reservation_id: ReservationId(command_id),
            order_id: OrderId(order_id),
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
        CommandEnvelope {
            command_id: CommandId(command_id),
            session_id: 11,
            client_seq,
            market_id: Some(market),
            user_id: Some(UserId(7)),
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(order_id),
                    user_id: UserId(7),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(2).unwrap(),
                },
                client_order_id: Some("client-1".into()),
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        }
    };
    let command = make(1, 1, 1);
    let mut stale = command.clone();
    stale.route_epoch = Some(999);
    assert!(
        retry(|| shard.handle().submit_command(stale.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let mut huge_stale = command.clone();
    huge_stale.route_epoch = Some(999);
    if let Command::PlaceLimit(place) = &mut huge_stale.payload {
        place.client_order_id = Some("x".repeat(16 * 1024 * 1024 + 1));
    }
    let rejection = retry(|| shard.handle().submit_command(huge_stale.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    assert_eq!(rejection, matching_domain::RejectReason::StaleRouteEpoch);
    assert!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let first = retry(|| shard.handle().submit_command(command.clone().clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(first.seq, 1);
    let snapshot_dir = base.join("snapshot");
    std::fs::create_dir(&snapshot_dir).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(1),
        shard: shard_id,
        primary_epoch: 1,
        routes: routes.clone(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(config.clone()),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let before_rejected_recovery =
        std::fs::read(base.join("wal").join("00000000000000000001.wal")).unwrap();
    assert!(
        Shard::recover_group_local(
            base.join("wal"),
            base.join("manifest"),
            &snapshot_dir,
            &trusted,
            (8, 200_000),
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(base.join("wal").join("00000000000000000001.wal")).unwrap(),
        before_rejected_recovery
    );
    let mut shard = Shard::recover_group_local_with_genesis(
        base.join("wal"),
        base.join("manifest"),
        &snapshot_dir,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (8, 200_000),
    )
    .unwrap();
    let mut snapshot_writer =
        matching_runtime::SnapshotWriter::start(shard.handle(), snapshot_dir.clone(), 1024 * 1024)
            .unwrap();
    let snapshot = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    snapshot_writer
        .submit(1, snapshot)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    assert!(
        retry(|| shard.handle().submit_command(command.clone().clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .deduplicated
    );
    let mut huge_conflict = command.clone();
    if let Command::PlaceLimit(place) = &mut huge_conflict.payload {
        place.client_order_id = Some("z".repeat(16 * 1024 * 1024 + 1));
    }
    let rejected = retry(|| shard.handle().submit_command(huge_conflict.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    assert_eq!(rejected, matching_runtime::FormalError::ProtocolError);
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    let mut conflict = command.clone();
    if let Command::PlaceLimit(place) = &mut conflict.payload {
        place.order.qty = Qty::try_new(1).unwrap();
    }
    assert!(
        retry(|| shard.handle().submit_command(conflict.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    let mut taker = make(2, 2, 2);
    taker.user_id = Some(UserId(8));
    taker.session_id = 12;
    if let Command::PlaceLimit(place) = &mut taker.payload {
        place.order.user_id = UserId(8);
        place.order.side = Side::Sell;
        place.order.qty = Qty::try_new(1).unwrap();
        place.client_order_id = Some("client-2".into());
        place.reservation.user_id = UserId(8);
        place.reservation.reserves[0].asset = AssetId(1);
        place.reservation.reserves[0].atoms = 1;
        place.reservation.signature = signing
            .sign(&place.reservation.canonical_signing_payload().unwrap())
            .to_bytes();
    }
    let filled_reply = retry(|| shard.handle().submit_command(taker.clone()));
    assert!(matches!(
        filled_reply.recv_timeout(Duration::from_millis(20)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    let cancel = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 11,
        client_seq: 3,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    };
    let cancel_reply = retry(|| shard.handle().submit_command(cancel.clone()));
    let barrier = retry(|| shard.handle().barrier(77));
    let filled = filled_reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(filled.seq, 2);
    let expected_id = matching_domain::TradeId((1_u128 << 112) | (124_u128 << 96) | (2_u128 << 32));
    let payloads: Vec<_> = filled
        .events
        .iter()
        .map(|event| event.payload.clone())
        .collect();
    assert_eq!(
        payloads,
        vec![
            matching_domain::EngineEvent::OrderAccepted {
                order_id: OrderId(2)
            },
            matching_domain::EngineEvent::Trade(matching_domain::TradeEvent {
                trade_id: expected_id,
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                maker_side: Side::Buy
            }),
            matching_domain::EngineEvent::OrderPartiallyFilled {
                order_id: OrderId(1),
                remaining: Qty::try_new(1).unwrap()
            },
            matching_domain::EngineEvent::OrderFilled {
                order_id: OrderId(2)
            },
        ]
    );
    for (index, event) in filled.events.iter().enumerate() {
        assert_eq!(event.event_index, index as u32);
        assert_eq!(event.shard_seq, 2);
    }
    let result = cancel_reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        barrier.recv_timeout(Duration::from_secs(5)).unwrap(),
        (77, 3)
    );
    assert_eq!(result.seq, 3);
    let digest = retry(|| shard.handle().take_snapshot(2))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    let mut event_hash = matching_runtime::EventHasher::new();
    for event in filled.events.iter().chain(&result.events) {
        event_hash.update(event).unwrap();
    }
    let records = matching_persistence::scan_segments(base.join("wal"))
        .unwrap()
        .records;
    assert_eq!(records.len(), 3);
    assert_eq!(
        matching_domain::decode_wal_command(&records[2].payload).unwrap(),
        cancel
    );
    snapshot_writer.shutdown().unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let restore = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(1),
        shard: shard_id,
        primary_epoch: 1,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(config),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(base.join("manifest")).unwrap();
    let batch = matching_runtime::recover_snapshot_records(
        base.join("wal"),
        &snapshot_dir,
        &manager,
        &restore,
    )
    .unwrap()
    .unwrap();
    let replay = batch.replay().unwrap();
    assert_eq!(replay.executions[0].events, filled.events);
    assert_eq!(replay.executions[1].events, result.events);
    assert_eq!(replay.event_hash, event_hash.finish());
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    drop(manager);
    let mut restarted = Shard::recover_group_local(
        base.join("wal"),
        base.join("manifest"),
        &snapshot_dir,
        &restore,
        (8, 10_000),
    )
    .unwrap();
    let duplicate = retry(|| restarted.handle().submit_command(cancel.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.seq, 3);
    let mut next = make(4, 4, 4);
    if let Command::PlaceLimit(place) = &mut next.payload {
        place.client_order_id = Some("client-4".into());
    }
    assert_eq!(
        retry(|| restarted.handle().submit_command(next.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        4
    );
    restarted.shutdown(Duration::from_secs(5)).unwrap();
}
