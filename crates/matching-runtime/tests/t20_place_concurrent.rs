//! 验证真实限价单并发重复投递仅执行一次。
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, ClusterId, Command, CommandEnvelope, CommandId, EngineEvent, LimitOrder,
    LogicalTime, MarketId, MarketMath, OrderId, OriginSessionId, PlaceLimitOrder, Price, Qty,
    ReservationId, ReservationToken, ShardId, Side, StpKey, StpMode, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
    ShardError,
};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// 仅对尚未入队的繁忙结果进行有期限重试。
fn retry<T>(mut f: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match f() {
            Ok(v) => return v,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("入口失败: {e:?}"),
        }
    }
}

#[test]
fn concurrent_signed_place_has_one_winner_and_preserves_winner_session() {
    const PRODUCERS: usize = 64;
    let market = MarketId(225);
    let shard_id = ShardId(225);
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
            active_orders: 1,
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
    let mut token = ReservationToken {
        token_id: 1,
        command_id: CommandId(1),
        reservation_id: ReservationId(1),
        order_id: OrderId(1),
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
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 0,
        client_seq: 0,
        market_id: Some(market),
        user_id: Some(UserId(7)),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(1),
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
    };
    let gate = Arc::new(Barrier::new(PRODUCERS + 1));
    let mut threads = Vec::new();
    for i in 0..PRODUCERS {
        let handle = shard.handle();
        let gate = Arc::clone(&gate);
        let mut cmd = command.clone();
        cmd.session_id = 1000 + i as u64;
        cmd.client_seq = i as u64 + 1;
        threads.push(std::thread::spawn(move || {
            gate.wait();
            let response = retry(|| handle.submit_command(cmd.clone()));
            (
                cmd.session_id,
                response
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap()
                    .unwrap(),
            )
        }));
    }
    gate.wait();
    let mut winners = Vec::new();
    let mut accepted = 0;
    let mut summaries = Vec::new();
    for thread in threads {
        let (session, result) = thread.join().unwrap();
        assert_eq!(result.seq, 1);
        assert_eq!(
            result.outcome,
            matching_runtime::FormalOutcome::Placed(OrderId(1))
        );
        if result.deduplicated {
            assert!(result.events.is_empty());
        } else {
            winners.push(session);
        }
        accepted += result.events.iter().filter(|e| matches!(e.payload, EngineEvent::OrderAccepted { order_id } if order_id == OrderId(1))).count();
        summaries.push(result.summary);
    }
    assert_eq!(winners.len(), 1);
    assert_eq!(accepted, 1);
    assert!(summaries.iter().all(|s| *s == summaries[0]));
    let (reservation, pending) = retry(|| shard.handle().reservation(ReservationId(1)))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(!pending);
    assert_eq!(reservation.origin_session_id, OriginSessionId(winners[0]));
    assert_eq!(
        retry(|| shard.handle().barrier(1))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (1, 1)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}
