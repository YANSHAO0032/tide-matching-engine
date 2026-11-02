//! 验证真实订单簿通过单写者执行时保持成交及自成交防护事件。
use matching_domain::{
    ClusterId, CommandId, EngineEvent, LimitOrder, MarketId, OrderId, Price, Qty, ShardId, Side,
    StpKey, StpMode, TradeIdCursor, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_runtime::{
    MarketRoute, Mutation, MutationOutcome, RouteState, RouteTable, Shard, ShardError,
};
use std::time::{Duration, Instant};

fn order(id: u128, user: u64, side: Side) -> LimitOrder {
    LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(user),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    }
}

#[test]
fn stp_place_and_real_trade_match_direct_production_execution() {
    let market = MarketId(91);
    let cluster = ClusterId(79);
    let shard_id = ShardId(3);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 8,
        state: RouteState::Active,
    }])
    .unwrap();
    let policy = QueuePriorityPolicy::try_new(1024).unwrap();
    let mut direct = ProductionOrderBook::new(policy);
    let mut shard = Shard::start(
        shard_id,
        cluster,
        routes,
        vec![(market, ProductionOrderBook::new(policy))],
    )
    .unwrap();
    let handle = shard.handle();
    let mut taker = order(3, 1, Side::Buy);
    taker.stp_mode = StpMode::CancelMaker;
    for (index, input) in [order(1, 1, Side::Sell), order(2, 2, Side::Sell), taker]
        .into_iter()
        .enumerate()
    {
        let seq = index as u64 + 1;
        let mut cursor = TradeIdCursor::new(cluster, shard_id, seq);
        let expected = direct
            .place_limit_gtc_with_cursor(input.clone(), &mut cursor)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let reply = loop {
            match handle.submit(
                CommandId(seq as u128),
                market,
                8,
                Mutation::Place(input.clone()),
            ) {
                Ok(reply) => break reply,
                Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("入口失败: {error:?}"),
            }
        };
        let execution = reply.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
        assert_eq!(execution.seq, seq);
        assert_eq!(execution.command_id, CommandId(seq as u128));
        let MutationOutcome::Placed(actual) = execution.result.unwrap() else {
            panic!("响应类型不符")
        };
        assert_eq!(actual, expected);
        if seq == 3 {
            assert!(
                actual
                    .iter()
                    .any(|event| matches!(event, EngineEvent::OrderCanceled { .. }))
            );
            assert_eq!(
                actual
                    .iter()
                    .filter(|event| matches!(event, EngineEvent::Trade(_)))
                    .count(),
                1
            );
        }
    }
    shard.shutdown(Duration::from_secs(3)).unwrap();
}

#[test]
fn wait_strategies_preserve_the_same_ordered_execution() {
    use matching_runtime::{ShardConfig, WaitStrategy};
    let strategies = [
        WaitStrategy::BusySpin,
        WaitStrategy::Yield,
        WaitStrategy::Park,
        WaitStrategy::Hybrid,
    ];
    for (i, strategy) in strategies.into_iter().enumerate() {
        let market = MarketId(80);
        let routes = RouteTable::new([MarketRoute {
            market_id: market,
            shard_id: ShardId(8),
            route_epoch: 2,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start_with_config(
            ShardId(8),
            ClusterId(100 + i as u16),
            routes,
            vec![(
                market,
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
            ShardConfig {
                wait_strategy: strategy,
                ..ShardConfig::default()
            },
        )
        .unwrap();
        let handle = shard.handle();
        for id in 1..=2u128 {
            let deadline = Instant::now() + Duration::from_secs(3);
            let reply = loop {
                match handle.submit(
                    CommandId(id),
                    market,
                    2,
                    Mutation::Place(order(id, id as u64, Side::Buy)),
                ) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
                    Err(error) => panic!("入口失败: {error:?}"),
                }
            };
            let result = reply.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
            assert_eq!(result.seq, id as u64);
            assert!(result.result.is_ok());
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        let barrier = loop {
            match handle.barrier(10) {
                Ok(reply) => break reply,
                Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("屏障失败: {error:?}"),
            }
        };
        assert_eq!(
            barrier.recv_timeout(Duration::from_secs(3)).unwrap(),
            (10, 2)
        );
        shard.shutdown(Duration::from_secs(3)).unwrap();
    }
}

#[test]
fn queued_queries_observe_prior_but_not_later_mutations() {
    let market = MarketId(812);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(7),
        ClusterId(814),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let handle = shard.handle();
    let deadline = Instant::now() + Duration::from_secs(3);
    let first = loop {
        match handle.submit(
            CommandId(1),
            market,
            5,
            Mutation::Place(order(1, 1, Side::Buy)),
        ) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    let query_first = loop {
        match handle.order(market, OrderId(1)) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("查询失败: {error:?}"),
        }
    };
    let query_later = loop {
        match handle.order(market, OrderId(2)) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("查询失败: {error:?}"),
        }
    };
    let second = loop {
        match handle.submit(
            CommandId(2),
            market,
            5,
            Mutation::Place(order(2, 2, Side::Buy)),
        ) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    assert_eq!(
        first
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert!(
        query_first
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap()
            .is_some()
    );
    assert!(
        query_later
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        second
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    shard.shutdown(Duration::from_secs(3)).unwrap();
}

#[test]
fn abandoned_query_reply_does_not_stop_the_worker() {
    let market = MarketId(813);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(7),
        ClusterId(815),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let handle = shard.handle();
    let deadline = Instant::now() + Duration::from_secs(3);
    let query = loop {
        match handle.order(market, OrderId(1)) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("查询失败: {error:?}"),
        }
    };
    drop(query);
    let command = loop {
        match handle.submit(
            CommandId(1),
            market,
            5,
            Mutation::Place(order(1, 1, Side::Buy)),
        ) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    let executed = command
        .recv_timeout(Duration::from_secs(3))
        .unwrap()
        .unwrap();
    assert_eq!(executed.seq, 1);
    assert!(executed.result.is_ok());
    shard.shutdown(Duration::from_secs(3)).unwrap();
}

#[test]
fn each_producers_successful_enqueue_order_is_preserved() {
    use std::sync::{Arc, Barrier};
    let market = MarketId(914);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(7),
        ClusterId(916),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let start = Arc::new(Barrier::new(21));
    let mut workers = Vec::new();
    for producer in 0..20u128 {
        let handle = shard.handle();
        let start = Arc::clone(&start);
        workers.push(std::thread::spawn(move || {
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut replies = Vec::new();
            for step in 0..3u128 {
                let id = producer * 3 + step + 1;
                let reply = loop {
                    match handle.submit(
                        CommandId(id),
                        market,
                        5,
                        Mutation::Place(order(id, id as u64, Side::Buy)),
                    ) {
                        Ok(reply) => break reply,
                        Err(ShardError::Busy) if Instant::now() < deadline => {
                            std::thread::yield_now()
                        }
                        Err(error) => panic!("入口失败: {error:?}"),
                    }
                };
                replies.push(reply);
            }
            let sequences: Vec<_> = replies
                .into_iter()
                .map(|rx| {
                    let result = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
                    assert!(result.result.is_ok());
                    result.seq
                })
                .collect();
            assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
            sequences
        }));
    }
    start.wait();
    let mut all: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (1..=60).collect::<Vec<_>>());
    shard.shutdown(Duration::from_secs(3)).unwrap();
}

#[test]
fn successful_cancel_returns_the_real_removed_node() {
    let market = MarketId(915);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let policy = QueuePriorityPolicy::try_new(1024).unwrap();
    let mut direct = ProductionOrderBook::new(policy);
    let mut initial = ProductionOrderBook::new(policy);
    let input = order(12, 5, Side::Buy);
    direct.rest(input.clone()).unwrap();
    initial.rest(input).unwrap();
    let expected = direct.cancel(OrderId(12)).unwrap();
    let mut shard =
        Shard::start(ShardId(7), ClusterId(917), routes, vec![(market, initial)]).unwrap();
    let handle = shard.handle();
    let deadline = Instant::now() + Duration::from_secs(3);
    let reply = loop {
        match handle.submit(CommandId(8), market, 5, Mutation::Cancel(OrderId(12))) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    };
    let execution = reply.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
    assert_eq!(execution.seq, 1);
    assert_eq!(
        execution.result.unwrap(),
        MutationOutcome::Canceled(expected)
    );
    shard.shutdown(Duration::from_secs(3)).unwrap();
}

#[test]
fn barrier_ack_follows_prior_mutations_and_query() {
    let market = MarketId(916);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(7),
        ClusterId(919),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    let handle = shard.handle();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut replies = Vec::new();
    for id in 1..=3u128 {
        let reply = loop {
            match handle.submit(
                CommandId(id),
                market,
                5,
                Mutation::Place(order(id, id as u64, Side::Buy)),
            ) {
                Ok(reply) => break reply,
                Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("入口失败: {error:?}"),
            }
        };
        replies.push(reply);
    }
    let query = loop {
        match handle.order(market, OrderId(2)) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("查询失败: {error:?}"),
        }
    };
    let barrier = loop {
        match handle.barrier(72) {
            Ok(reply) => break reply,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("屏障失败: {error:?}"),
        }
    };
    assert_eq!(
        barrier.recv_timeout(Duration::from_secs(3)).unwrap(),
        (72, 3)
    );
    for (i, reply) in replies.into_iter().enumerate() {
        let result = reply
            .try_recv()
            .expect("屏障确认时先前变更必须已回复")
            .unwrap();
        assert_eq!(result.seq, i as u64 + 1);
    }
    assert!(
        query
            .try_recv()
            .expect("屏障确认时查询必须已回复")
            .unwrap()
            .is_some()
    );
    shard.shutdown(Duration::from_secs(3)).unwrap();
}
