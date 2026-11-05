//! 验证分片内用户栅栏顺序及去重。
use matching_domain::{
    ApplyUserControlFenceShard, ClusterId, Command, CommandEnvelope, CommandId, EngineEvent,
    LogicalTime, RejectReason, ShardId, UserFenceAction, UserId,
};
use matching_risk::ReservationRegistry;
use matching_runtime::{IdempotencyConfig, RouteTable, Shard};
use std::time::{Duration, Instant};

/// 仅对尚未入队的繁忙结果进行有期限重试。
fn retry<T>(mut f: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match f() {
            Ok(v) => return v,
            Err(matching_runtime::ShardError::Busy) if Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(e) => panic!("入口失败: {e:?}"),
        }
    }
}

#[test]
fn fence_is_sequenced_once_and_checks_primary_before_dedup() {
    let mut shard = Shard::start_with_user_control(
        ShardId(1),
        ClusterId(1),
        RouteTable::new([]).unwrap(),
        vec![],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 1),
    )
    .unwrap();
    let mut command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(2)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(9),
            user_id: UserId(2),
            new_epoch: 5,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let run = |c: CommandEnvelope| {
        retry(|| shard.handle().submit_command(c.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
    };
    let first = run(command.clone()).unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(
        first.events[0].payload,
        EngineEvent::UserControlEpochAdvanced {
            user_id: UserId(2),
            epoch: 5
        }
    );
    assert!(run(command.clone()).unwrap().deduplicated);
    if let Command::ApplyUserControlFenceShard(f) = &mut command.payload {
        f.expected_primary_epoch = 6;
    }
    assert_eq!(run(command).unwrap_err(), RejectReason::StalePrimaryEpoch);
    assert_eq!(
        retry(|| shard.handle().barrier(1))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (1, 1)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn capacity_and_unsupported_action_leave_sequence_and_existing_fence_unchanged() {
    let mut shard = Shard::start_with_user_control(
        ShardId(2),
        ClusterId(1),
        RouteTable::new([]).unwrap(),
        vec![],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 8, 1),
    )
    .unwrap();
    let mut cmd = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(1)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(9),
            user_id: UserId(1),
            new_epoch: 5,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 8,
        }),
    };
    let run = |c: CommandEnvelope| {
        retry(|| shard.handle().submit_command(c.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
    };
    run(cmd.clone()).unwrap();
    let mut changed_action = cmd.clone();
    if let Command::ApplyUserControlFenceShard(f) = &mut changed_action.payload {
        f.action = UserFenceAction::FenceAndCancelAll;
    }
    assert_eq!(
        run(changed_action).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    let mut changed_user = cmd.clone();
    changed_user.user_id = Some(UserId(7));
    assert_eq!(
        run(changed_user).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    let mut conflict = cmd.clone();
    if let Command::ApplyUserControlFenceShard(f) = &mut conflict.payload {
        f.new_epoch = 6;
    }
    assert_eq!(
        run(conflict).unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    cmd.command_id = CommandId(2);
    cmd.client_seq = 2;
    cmd.user_id = Some(UserId(2));
    if let Command::ApplyUserControlFenceShard(f) = &mut cmd.payload {
        f.user_id = UserId(2);
    }
    assert_eq!(run(cmd.clone()).unwrap_err(), RejectReason::EngineBusy);
    if let Command::ApplyUserControlFenceShard(f) = &mut cmd.payload {
        f.action = UserFenceAction::FenceAndCancelAll;
    }
    assert_eq!(
        run(cmd.clone()).unwrap_err(),
        RejectReason::InvalidOrderCombination
    );
    cmd.user_id = Some(UserId(1));
    if let Command::ApplyUserControlFenceShard(f) = &mut cmd.payload {
        f.user_id = UserId(1);
        f.new_epoch = 3;
        f.action = UserFenceAction::FenceOnly;
    }
    let next = run(cmd).unwrap();
    assert_eq!(next.seq, 2);
    assert_eq!(
        next.events[0].payload,
        EngineEvent::UserControlEpochAdvanced {
            user_id: UserId(1),
            epoch: 5
        }
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn place_checks_route_and_epoch_before_business_execution_without_consuming_sequence() {
    use matching_domain::{
        LimitOrder, MarketId, OrderId, PlaceLimitOrder, Price, Qty, ReservationId,
        ReservationToken, Side, StpKey, StpMode,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{MarketRoute, RouteState};
    let market = MarketId(10);
    let routes = RouteTable::new([
        MarketRoute {
            market_id: market,
            shard_id: ShardId(4),
            route_epoch: 3,
            state: RouteState::Active,
        },
        MarketRoute {
            market_id: MarketId(11),
            shard_id: ShardId(5),
            route_epoch: 3,
            state: RouteState::Active,
        },
    ])
    .unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    book.bind_market(market).unwrap();
    let mut shard = Shard::start_with_user_control(
        ShardId(4),
        ClusterId(2),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 2),
    )
    .unwrap();
    let run = |c: CommandEnvelope| {
        retry(|| shard.handle().submit_command(c.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
    };
    let fence = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        route_epoch: None,
        user_id: Some(UserId(5)),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(9),
            user_id: UserId(5),
            new_epoch: 5,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 1,
        }),
    };
    run(fence).unwrap();
    let mut cmd = CommandEnvelope {
        command_id: CommandId(2),
        session_id: 1,
        client_seq: 2,
        market_id: Some(market),
        route_epoch: Some(2),
        user_id: Some(UserId(5)),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(4),
                user_id: UserId(5),
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: ReservationToken {
                token_id: 1,
                command_id: CommandId(2),
                reservation_id: ReservationId(3),
                order_id: OrderId(4),
                user_id: UserId(5),
                market_id: market,
                reserves: Default::default(),
                fee_schedule_version: 1,
                fee_tier_id: 1,
                risk_key_id: 1,
                signature: [0; 64],
            },
        })),
    };
    assert_eq!(run(cmd.clone()).unwrap_err(), RejectReason::StaleRouteEpoch);
    cmd.route_epoch = None;
    assert_eq!(run(cmd.clone()).unwrap_err(), RejectReason::StaleRouteEpoch);
    cmd.route_epoch = Some(3);
    cmd.market_id = Some(MarketId(11));
    assert_eq!(run(cmd.clone()).unwrap_err(), RejectReason::StaleRouteEpoch);
    cmd.market_id = Some(MarketId(12));
    assert_eq!(run(cmd.clone()).unwrap_err(), RejectReason::StaleRouteEpoch);
    cmd.market_id = None;
    assert_eq!(
        run(cmd.clone()).unwrap_err(),
        RejectReason::InvalidOrderCombination
    );
    cmd.market_id = Some(market);
    cmd.user_id = Some(UserId(6));
    if let Command::PlaceLimit(place) = &mut cmd.payload {
        place.user_control_epoch = 5;
    }
    assert_eq!(
        run(cmd.clone()).unwrap_err(),
        RejectReason::InvalidOrderCombination
    );
    cmd.user_id = Some(UserId(5));
    if let Command::PlaceLimit(place) = &mut cmd.payload {
        place.user_control_epoch = 0;
    }
    assert_eq!(run(cmd).unwrap_err(), RejectReason::StaleUserControlEpoch);
    assert_eq!(
        retry(|| shard.handle().barrier(1))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (1, 1)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}
