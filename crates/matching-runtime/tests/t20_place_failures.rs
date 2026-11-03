//! 正式下单拒绝和启动配置的集成验证。
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, ClusterId, Command, CommandEnvelope, CommandId, EngineEvent, LimitOrder,
    LogicalTime, MarketId, MarketMath, OrderId, PlaceLimitOrder, Price, PriceBandConfig,
    PriceReferenceKind, Qty, RejectReason, ReservationId, ReservationToken, ShardId, Side, StpKey,
    StpMode, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    FormalOutcome, IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState,
    RouteTable, Shard,
};
use std::time::Duration;

/// 仅对尚未入队的繁忙结果进行有期限重试。
fn retry<T>(mut f: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match f() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(error) => panic!("提交失败: {error:?}"),
        }
    }
}

const MARKET: MarketId = MarketId(333);
const SHARD: ShardId = ShardId(333);

/// 构造测试市场的路由快照。
fn routes() -> RouteTable {
    RouteTable::new([MarketRoute {
        market_id: MARKET,
        shard_id: SHARD,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap()
}
/// 构造绑定测试市场的空订单簿。
fn book() -> ProductionOrderBook {
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    book.bind_market(MARKET).unwrap();
    book
}
/// 根据数值规则构造可信测试配置。
fn config(math: MarketMath) -> PlaceSupportConfig {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    PlaceSupportConfig::new(
        keys,
        [(MARKET, policy)],
        PlaceLimits {
            active_orders: 20,
            terminal_history: 20,
            client_id_bytes: 32,
        },
    )
    .unwrap()
}
/// 构造指定身份、方向和标志的测试命令。
fn command(id: u128, side: Side, post_only: bool) -> CommandEnvelope {
    let mut token = ReservationToken {
        token_id: id,
        command_id: CommandId(id),
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        market_id: MARKET,
        reserves: vec![AssetReserve {
            asset: if side == Side::Buy {
                AssetId(2)
            } else {
                AssetId(1)
            },
            atoms: if side == Side::Buy { 20 } else { 2 },
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    sign(&mut token);
    CommandEnvelope {
        command_id: CommandId(id),
        session_id: 10,
        client_seq: id as u64,
        market_id: Some(MARKET),
        user_id: Some(UserId(id as u64)),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(id as u64),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: Some(format!("c{id}")),
            user_control_epoch: 0,
            post_only,
            reservation: token,
        })),
    }
}
/// 使用测试私钥签署规范预留载荷。
fn sign(token: &mut ReservationToken) {
    token.signature = SigningKey::from_bytes(&[7; 32])
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
}

#[test]
fn rejected_place_preserves_book_reservations_and_priority_and_is_deduplicated() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut shard = Shard::start_with_place_support(
        SHARD,
        ClusterId(333),
        routes(),
        vec![(MARKET, book())],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 8, config(math)),
    )
    .unwrap();
    let handle = shard.handle();
    let run = |cmd: CommandEnvelope| {
        retry(|| handle.submit_command(cmd.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
    };
    let query = |id| {
        retry(|| handle.order(MARKET, OrderId(id)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
    };
    let reservation = |id| {
        retry(|| handle.reservation(ReservationId(id)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
    };
    assert_eq!(run(command(1, Side::Sell, false)).seq, 1);
    let maker = query(1).unwrap();
    let original_reservation = reservation(1).unwrap();
    let mut cases = Vec::new();
    let mut invalid_signature = command(2, Side::Buy, false);
    if let Command::PlaceLimit(ref mut place) = invalid_signature.payload {
        place.reservation.signature[0] ^= 1;
    }
    cases.push((invalid_signature, RejectReason::InvalidReservation));
    let mut wrong_identity = command(3, Side::Buy, false);
    if let Command::PlaceLimit(ref mut place) = wrong_identity.payload {
        place.reservation.user_id = UserId(99);
        sign(&mut place.reservation);
    }
    cases.push((wrong_identity, RejectReason::InvalidReservation));
    let mut insufficient = command(4, Side::Buy, false);
    if let Command::PlaceLimit(ref mut place) = insufficient.payload {
        place.reservation.reserves = vec![AssetReserve {
            asset: AssetId(2),
            atoms: 19,
        }]
        .into();
        sign(&mut place.reservation);
    }
    cases.push((insufficient, RejectReason::InsufficientReservation));
    cases.push((
        command(5, Side::Buy, true),
        RejectReason::PostOnlyWouldCross,
    ));
    for (offset, (cmd, reason)) in cases.into_iter().enumerate() {
        let id = cmd.command_id.0;
        let result = run(cmd.clone());
        assert_eq!(result.seq, offset as u64 + 2);
        assert_eq!(result.outcome, FormalOutcome::Rejected(reason));
        assert_eq!(result.events.len(), 1);
        assert_eq!(
            result.events[0].payload,
            EngineEvent::CommandRejected { reason }
        );
        let retry = run(cmd);
        assert!(retry.deduplicated);
        assert_eq!(retry.seq, result.seq);
        assert_eq!(retry.summary, result.summary);
        assert!(retry.events.is_empty());
        assert_eq!(query(1), Some(maker.clone()));
        assert_eq!(query(id), None);
        assert_eq!(reservation(1), Some(original_reservation.clone()));
        assert_eq!(reservation(id), None);
    }
    assert_eq!(run(command(6, Side::Sell, false)).seq, 6);
    assert_eq!(
        query(6).unwrap().priority().get(),
        maker.priority().get() + 1
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn startup_rejects_market_math_mismatch() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let different = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
    let mut book = book();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::LastTrade,
            max_deviation_bps: 100,
        },
        different,
    )))
    .unwrap();
    assert!(
        Shard::start_with_place_support(
            SHARD,
            ClusterId(334),
            routes(),
            vec![(MARKET, book)],
            ReservationRegistry::new([]).unwrap(),
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 1, 8, config(math))
        )
        .is_err()
    );
}

#[test]
fn startup_rejects_preexisting_unreserved_order() {
    use matching_domain::TradeIdCursor;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = book();
    let Command::PlaceLimit(place) = command(1, Side::Sell, false).payload else {
        unreachable!()
    };
    book.place_limit_gtc_with_cursor(
        place.order,
        &mut TradeIdCursor::new(ClusterId(335), SHARD, 1),
    )
    .unwrap();
    assert!(book.order(OrderId(1)).is_some());
    assert!(
        Shard::start_with_place_support(
            SHARD,
            ClusterId(335),
            routes(),
            vec![(MARKET, book)],
            ReservationRegistry::new([]).unwrap(),
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 1, 8, config(math))
        )
        .is_err()
    );
}

#[test]
fn startup_rejects_preexisting_reserved_order() {
    use matching_domain::{OriginSessionId, TimeInForce, TradeIdCursor};
    use matching_orderbook::production_orderbook::{
        ProductionCancelReplaceContext, ProductionPlaceOrder,
    };
    use matching_risk::ExpectedNewReservation;

    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap();
    let Command::PlaceLimit(place) = command(1, Side::Sell, false).payload else {
        unreachable!()
    };
    let expected = ExpectedNewReservation {
        command_id: CommandId(1),
        order_id: OrderId(1),
        user_id: UserId(1),
        market_id: MARKET,
        origin_session_id: OriginSessionId::new(10),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        required_reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 2,
        }],
    };
    let mut registry = ReservationRegistry::new([]).unwrap();
    let plan = registry
        .plan_new_reservation(&keys, &expected, &place.reservation)
        .unwrap();
    let state = plan.active_state().clone();
    registry.apply_new_reservation(plan).unwrap();
    let mut book = book();
    book.place_with_registry_and_cursor(
        ProductionPlaceOrder::Limit {
            order: place.order,
            time_in_force: TimeInForce::Gtc,
            post_only: false,
        },
        state,
        &mut registry,
        ProductionCancelReplaceContext {
            math: &math,
            now: LogicalTime::new(0),
            current_session_id: 10,
            trade_ids: &[],
        },
        &mut TradeIdCursor::new(ClusterId(336), SHARD, 1),
    )
    .unwrap();
    assert!(book.order(OrderId(1)).is_some());
    assert!(registry.get(ReservationId(1)).is_some());
    assert!(
        Shard::start_with_place_support(
            SHARD,
            ClusterId(336),
            routes(),
            vec![(MARKET, book)],
            registry,
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 1, 8, config(math))
        )
        .is_err()
    );
}
