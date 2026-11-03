//! 验证正式下单与用户栅栏的入口顺序和重试语义。
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    ApplyUserControlFenceShard, AssetId, AssetReserve, ClusterId, Command, CommandEnvelope,
    CommandId, EngineEvent, LimitOrder, LogicalTime, MarketId, MarketMath, OrderId,
    OriginSessionId, PlaceLimitOrder, Price, Qty, RejectReason, ReservationId, ReservationToken,
    ShardId, Side, StpKey, StpMode, UserFenceAction, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
    ShardError,
};
use std::time::{Duration, Instant};

/// 仅对尚未入队的繁忙结果进行有期限重试。
fn retry<T>(mut f: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match f() {
            Ok(v) => return v,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("入口失败: {e:?}"),
        }
    }
}

#[test]
fn signed_place_obeys_fence_and_retry_preserves_original_session_at_capacity() {
    let market = MarketId(224);
    let shard_id = ShardId(224);
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
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
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
    let make_fence = |id, epoch| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 90,
        client_seq: id as u64,
        market_id: None,
        user_id: Some(UserId(7)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(100 + id),
            user_id: UserId(7),
            new_epoch: epoch,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 1,
        }),
    };
    let make_place = |id: u128, epoch| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
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
            command_id: CommandId(id),
            session_id: 11,
            client_seq: id as u64,
            market_id: Some(market),
            user_id: Some(UserId(7)),
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: UserId(7),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(2).unwrap(),
                },
                client_order_id: Some(format!("client-{id}")),
                user_control_epoch: epoch,
                post_only: false,
                reservation: token,
            })),
        }
    };
    let run = |c: CommandEnvelope| {
        retry(|| shard.handle().submit_command(c.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
    };
    assert_eq!(run(make_fence(1, 5)).unwrap().seq, 1);
    let mut stale = make_place(2, 4);
    stale.route_epoch = Some(0);
    assert_eq!(
        run(stale.clone()).unwrap_err(),
        RejectReason::StaleRouteEpoch
    );
    stale.route_epoch = Some(1);
    assert_eq!(run(stale).unwrap_err(), RejectReason::StaleUserControlEpoch);
    assert_eq!(
        retry(|| shard.handle().barrier(1))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (1, 1)
    );
    let place = make_place(2, 5);
    let first = run(place.clone()).unwrap();
    assert_eq!(first.seq, 2);
    assert!(first.events.iter().any(
        |e| matches!(e.payload, EngineEvent::OrderAccepted { order_id } if order_id == OrderId(2))
    ));
    assert_eq!(run(make_place(3, 5)).unwrap_err(), RejectReason::EngineBusy);
    let mut retried = place.clone();
    retried.session_id = 12;
    retried.client_seq = 1;
    let result = run(retried).unwrap();
    assert!(result.deduplicated);
    assert_eq!(result.seq, 2);
    assert!(result.events.is_empty());
    let (reservation, pending) = retry(|| shard.handle().reservation(ReservationId(2)))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(!pending);
    assert_eq!(reservation.origin_session_id, OriginSessionId(11));
    assert_eq!(run(make_fence(4, 6)).unwrap().seq, 3);
    assert_eq!(run(place).unwrap_err(), RejectReason::StaleUserControlEpoch);
    assert_eq!(
        retry(|| shard.handle().barrier(2))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (2, 3)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}
