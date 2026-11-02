use matching_domain::{ClusterId, CommandId, MarketId, OrderId, ShardId};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_runtime::{MarketRoute, Mutation, MutationEnvelope, RouteState, RouteTable, Shard};
use std::time::Duration;

#[test]
fn command_identity_survives_the_worker_and_business_rejection() {
    let market = MarketId(3);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(1),
        route_epoch: 7,
        state: RouteState::Active,
    }])
    .unwrap();
    let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    let mut shard = Shard::start(ShardId(1), ClusterId(1), routes, vec![(market, book)]).unwrap();
    let input = MutationEnvelope {
        command_id: CommandId(123),
        market_id: market,
        route_epoch: 7,
        payload: Mutation::Cancel(OrderId(9)),
    };
    let executed = shard
        .handle()
        .submit_envelope(input)
        .unwrap()
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(executed.command_id, CommandId(123));
    assert_eq!(executed.seq, 1);
    assert!(executed.result.is_err());
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn queue_busy_maps_to_the_stable_ingress_reason() {
    use matching_domain::RejectReason;
    use matching_runtime::ShardError;
    assert_eq!(
        ShardError::Busy.ingress_reason(),
        Some(RejectReason::EngineBusy)
    );
    assert_eq!(ShardError::Closed.ingress_reason(), None);
}

#[test]
fn unknown_market_is_not_reported_as_a_missing_order() {
    use matching_domain::RejectReason;
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(3),
        shard_id: ShardId(1),
        route_epoch: 7,
        state: RouteState::Active,
    }])
    .unwrap();
    let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    let mut shard = Shard::start(
        ShardId(1),
        ClusterId(103),
        routes,
        vec![(MarketId(3), book)],
    )
    .unwrap();
    let reply = shard
        .handle()
        .order(MarketId(4), OrderId(99))
        .unwrap()
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(reply, Err(RejectReason::MarketNotFound));
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn shutdown_timeout_does_not_silently_detach_the_worker() {
    use matching_runtime::ShardError;
    let market = MarketId(913);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(7),
        route_epoch: 5,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start(
        ShardId(7),
        ClusterId(918),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap();
    assert_eq!(shard.shutdown(Duration::ZERO), Err(ShardError::Timeout));
    shard.shutdown(Duration::from_secs(3)).unwrap();
}
