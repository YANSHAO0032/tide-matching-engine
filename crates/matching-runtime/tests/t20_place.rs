//! 验证正式限价入口的可信启动与客户端身份生命周期。
use matching_domain::{AssetId, ClusterId, LogicalTime, MarketId, MarketMath, ShardId};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
};

#[test]
fn place_retry_and_client_order_identity_follow_actual_order_lifecycle() {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, Command, CommandEnvelope, CommandId, EngineEvent, LimitOrder,
        MassCancelShard, OrderId, PlaceLimitOrder, Price, Qty, ReservationId, ReservationToken,
        ShardCancelScope, Side, StpKey, StpMode, UserId,
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
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    book.bind_market(market).unwrap();
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
    let mut shard = Shard::start_with_place_support(
        shard_id,
        ClusterId(1),
        routes.clone(),
        vec![(market, book)],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 8, config.clone()),
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
    let run = |command: CommandEnvelope| {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let reply = loop {
            match shard.handle().submit_command(command.clone()) {
                Ok(reply) => break reply,
                Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < deadline => {
                    std::thread::yield_now()
                }
                Err(error) => panic!("命令尚未入队: {error:?}"),
            }
        };
        reply.recv_timeout(Duration::from_secs(5)).unwrap()
    };
    let first_command = make(1, 1, 1);
    let first = run(first_command.clone()).unwrap();
    assert_eq!(first.seq, 1);
    let image = shard
        .handle()
        .take_snapshot(99)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let restore = matching_runtime::SnapshotRestoreConfig {
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
    let bytes = image.encode_snapshot(restore.max_bytes).unwrap();
    let recovered = matching_runtime::SnapshotImage::decode_snapshot(&bytes, &restore).unwrap();
    assert_eq!(
        image.canonical_state(restore.max_bytes).unwrap(),
        recovered.canonical_state(restore.max_bytes).unwrap()
    );
    assert_eq!(
        recovered.client_order(UserId(7), "client-1"),
        Some((market, OrderId(1)))
    );
    assert!(recovered.contains_command(&first_command).unwrap());
    assert!(recovered.reservation(ReservationId(1)).is_some());
    assert!(first.events.iter().any(
        |e| matches!(e.payload, EngineEvent::OrderAccepted { order_id } if order_id == OrderId(1))
    ));
    let mut invalid_collision = first_command.clone();
    if let Command::PlaceLimit(place) = &mut invalid_collision.payload {
        place.client_order_id = Some(String::new());
    }
    assert_eq!(
        run(invalid_collision.clone()).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    invalid_collision.command_id = CommandId(99);
    assert_eq!(
        run(invalid_collision).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    let mut conflicting_user = first_command.clone();
    conflicting_user.user_id = Some(UserId(99));
    assert_eq!(
        run(conflicting_user).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    let retry = run(first_command).unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.seq, 1);
    assert!(retry.events.is_empty());
    let duplicate = run(make(2, 2, 2)).unwrap();
    assert!(matches!(
        duplicate.outcome,
        matching_runtime::FormalOutcome::Rejected(
            matching_domain::RejectReason::DuplicateClientOrderId
        )
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
    run(cancel).unwrap();
    let retained = run(make(4, 3, 4)).unwrap();
    assert!(matches!(
        retained.outcome,
        matching_runtime::FormalOutcome::Rejected(
            matching_domain::RejectReason::DuplicateClientOrderId
        )
    ));
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn formal_place_starts_with_explicit_market_and_capacity() {
    let market = MarketId(123);
    let shard_id = ShardId(123);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    book.bind_market(market).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let config = PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 2,
            terminal_history: 2,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let mut shard = Shard::start_with_place_support(
        shard_id,
        ClusterId(1),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 8, config),
    )
    .unwrap();
    shard.shutdown(std::time::Duration::from_secs(5)).unwrap();
}
