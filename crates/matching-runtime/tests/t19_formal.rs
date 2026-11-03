//! 验证正式批量撤单封装及预留归属。
use matching_domain::{
    AssetId, AssetReserve, ClusterId, Command, CommandEnvelope, CommandId, EngineEvent, LimitOrder,
    LogicalTime, MarketId, MassCancelShard, OrderId, OriginSessionId, Price, Qty, ReservationId,
    ShardCancelScope, ShardId, Side, StpKey, StpMode, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{ReservationRegistry, ReservationState, ReservationStatus};
use matching_runtime::{MarketRoute, RouteState, RouteTable, Shard, ShardError};
use std::time::{Duration, Instant};

/// 仅对尚未入队的繁忙结果做有期限重试。
fn retry<T>(mut call: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match call() {
            Ok(value) => return value,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("分片入口失败: {error:?}"),
        }
    }
}

#[test]
fn formal_mass_cancel_emits_enveloped_events_and_preserves_pending_r2() {
    let market = MarketId(19);
    let shard_id = ShardId(19);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 3,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(51),
        order_id: OrderId(1),
        user_id: UserId(2),
        market_id: market,
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(market).unwrap();
    book.rest_with_reservation(
        LimitOrder {
            order_id: OrderId(1),
            user_id: UserId(2),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(2).unwrap(),
        },
        state.clone(),
    )
    .unwrap();
    let mut second_state = state.clone();
    second_state.reservation_id = ReservationId(52);
    second_state.order_id = OrderId(2);
    book.rest_with_reservation(
        LimitOrder {
            order_id: OrderId(2),
            user_id: UserId(2),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(3).unwrap(),
        },
        second_state.clone(),
    )
    .unwrap();
    let registry = ReservationRegistry::new([state, second_state]).unwrap();
    let mut shard = Shard::start_with_registry(
        shard_id,
        ClusterId(619),
        routes,
        vec![(market, book)],
        registry,
        LogicalTime::new(17),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(44),
        session_id: 7,
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(3),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    };
    let mut stale = command.clone();
    stale.route_epoch = Some(2);
    assert_eq!(
        retry(|| shard.handle().submit_command(stale.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err(),
        matching_domain::RejectReason::StaleRouteEpoch,
    );
    assert_eq!(
        retry(|| shard.handle().barrier(1))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (1, 0)
    );
    assert_eq!(
        retry(|| shard.handle().submit(
            CommandId(40),
            market,
            3,
            matching_runtime::Mutation::Cancel(OrderId(1))
        ))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err(),
        matching_domain::RejectReason::InvalidOrderCombination,
    );
    let result = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(result.events.len(), 2);
    assert_eq!(
        result.outcome,
        matching_runtime::FormalOutcome::MassCancel(matching_domain::ShardMassCancelResult {
            canceled_orders: 2
        })
    );
    let event = &result.events[0];
    assert_eq!(
        (
            event.command_id,
            event.shard_seq,
            event.event_index,
            event.engine_time,
            event.market_id
        ),
        (CommandId(44), 1, 0, 17, Some(market))
    );
    assert!(matches!(
        event.payload,
        EngineEvent::OrderCanceled {
            order_id: OrderId(1),
            ..
        }
    ));
    assert_eq!(result.events[1].event_index, 1);
    assert!(matches!(
        result.events[1].payload,
        EngineEvent::OrderCanceled {
            order_id: OrderId(2),
            ..
        }
    ));
    assert!(
        retry(|| shard.handle().reservation(ReservationId(51)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .1
    );
    assert!(
        retry(|| shard.handle().reservation(ReservationId(52)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .1
    );
    let no_op = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(no_op.seq, 2);
    assert_eq!(no_op.command_id, CommandId(44));
    assert_eq!(
        no_op.outcome,
        matching_runtime::FormalOutcome::MassCancel(matching_domain::ShardMassCancelResult {
            canceled_orders: 0
        })
    );
    assert!(no_op.events.is_empty());
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn startup_rejects_registry_active_order_not_owned_by_any_book() {
    let market = MarketId(71);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(71),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(90),
        order_id: OrderId(91),
        user_id: UserId(2),
        market_id: MarketId(72),
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(market).unwrap();
    let result = Shard::start_with_registry(
        ShardId(71),
        ClusterId(671),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([state]).unwrap(),
        LogicalTime::new(0),
    );
    assert!(result.is_err(), "未被分片订单簿持有的活动预留必须拒绝启动");
}

#[test]
fn startup_rejects_orders_expired_at_initial_logical_time() {
    let market = MarketId(81);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(81),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(80),
        order_id: OrderId(81),
        user_id: UserId(2),
        market_id: market,
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(market).unwrap();
    book.rest_timed_with_reservation(
        LimitOrder {
            order_id: OrderId(81),
            user_id: UserId(2),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        },
        matching_domain::TimeInForce::Gtd {
            expire_at: LogicalTime::new(5),
        },
        LogicalTime::new(0),
        1,
        state.clone(),
    )
    .unwrap();
    let result = Shard::start_with_registry(
        ShardId(81),
        ClusterId(681),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([state]).unwrap(),
        LogicalTime::new(5),
    );
    assert!(result.is_err(), "到期订单不能以较晚的逻辑时间重新接入");
}

#[test]
fn startup_rejects_untriggered_stop_expired_at_initial_logical_time() {
    use matching_domain::{
        PriceReferenceKind, QuoteBudget, StopMarketOrder, StopMarketQuantity, StopOrder,
        TimeInForce, TriggerExpiry,
    };
    let market = MarketId(82);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(82),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(82),
        order_id: OrderId(82),
        user_id: UserId(2),
        market_id: market,
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(market).unwrap();
    let stop = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId(82),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(82),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(11).unwrap(),
        quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(19).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(5),
        },
    });
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
        .unwrap();
    let result = Shard::start_with_registry(
        ShardId(82),
        ClusterId(682),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([state]).unwrap(),
        LogicalTime::new(5),
    );
    assert!(result.is_err(), "到期止损订单不能以较晚的逻辑时间重新接入");
}

#[test]
fn formal_mass_cancel_preserves_quote_remainder_for_untriggered_stop() {
    use matching_domain::{
        OrderRemaining, PriceReferenceKind, QuoteAmount, QuoteBudget, StopMarketOrder,
        StopMarketQuantity, StopOrder, TimeInForce, TriggerExpiry,
    };
    let market = MarketId(83);
    let shard_id = ShardId(83);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(83),
        order_id: OrderId(83),
        user_id: UserId(2),
        market_id: market,
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    book.bind_market(market).unwrap();
    let stop = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId(83),
        user_id: UserId(2),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId(83),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(11).unwrap(),
        quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(19).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    book.place_stop_with_reservation(stop, LogicalTime::new(0), 1, state.clone())
        .unwrap();
    let mut shard = Shard::start_with_registry(
        shard_id,
        ClusterId(683),
        routes,
        vec![(market, book)],
        ReservationRegistry::new([state]).unwrap(),
        LogicalTime::new(0),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(83),
        session_id: 7,
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: Some(100),
        received_at_unix_ns: Some(200),
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    };
    let result = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(
        matches!(result.events[0].payload,EngineEvent::OrderCanceled { order_id:OrderId(83), remaining:OrderRemaining::Quote(value), .. } if value==QuoteAmount::new(19))
    );
    assert_eq!(result.events[0].engine_time, 0);
    assert!(
        retry(|| shard.handle().reservation(ReservationId(83)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .1
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn concurrent_formal_commands_replay_in_allocated_sequence_order() {
    use std::sync::{Arc, Barrier};
    let market = MarketId(91);
    let shard_id = ShardId(91);
    let cluster = ClusterId(691);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let state = ReservationState {
        reservation_id: ReservationId(91),
        order_id: OrderId(91),
        user_id: UserId(2),
        market_id: market,
        origin_session_id: OriginSessionId(7),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let mut initial_book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    initial_book.bind_market(market).unwrap();
    initial_book
        .rest_with_reservation(
            LimitOrder {
                order_id: OrderId(91),
                user_id: UserId(2),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            state.clone(),
        )
        .unwrap();
    let initial_registry = ReservationRegistry::new([state]).unwrap();
    let make = |id| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 7,
        client_seq: id as u64,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    };
    let mut first = Shard::start_with_registry(
        shard_id,
        cluster,
        routes.clone(),
        vec![(market, initial_book.clone())],
        initial_registry.clone(),
        LogicalTime::new(9),
    )
    .unwrap();
    let start = Arc::new(Barrier::new(17));
    let mut tasks = Vec::new();
    for id in 1..=16u128 {
        let h = first.handle();
        let start = Arc::clone(&start);
        let cmd = make(id);
        tasks.push(std::thread::spawn(move || {
            start.wait();
            retry(|| h.submit_command(cmd.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
        }));
    }
    start.wait();
    let mut transcript: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
    transcript.sort_unstable_by_key(|x| x.seq);
    assert_eq!(
        transcript.iter().map(|x| x.seq).collect::<Vec<_>>(),
        (1..=16).collect::<Vec<_>>()
    );
    first.shutdown(Duration::from_secs(5)).unwrap();
    let mut second = Shard::start_with_registry(
        shard_id,
        cluster,
        routes,
        vec![(market, initial_book)],
        initial_registry,
        LogicalTime::new(9),
    )
    .unwrap();
    for previous in transcript {
        let replay = retry(|| second.handle().submit_command(make(previous.command_id.0)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(replay.seq, previous.seq);
        assert_eq!(replay.command_id, previous.command_id);
        assert_eq!(replay.outcome, previous.outcome);
        assert_eq!(replay.events, previous.events);
    }
    second.shutdown(Duration::from_secs(5)).unwrap();
}
