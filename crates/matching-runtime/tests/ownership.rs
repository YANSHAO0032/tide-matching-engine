//! 验证分片运行时的市场归属边界。
use matching_domain::{ClusterId, MarketId, ShardId};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_runtime::{MarketRoute, RouteState, RouteTable, Shard};

#[test]
fn same_market_cannot_be_owned_by_two_live_shards() {
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(3),
        shard_id: ShardId(1),
        route_epoch: 7,
        state: RouteState::Active,
    }])
    .unwrap();
    let book = || ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut first = Shard::start(
        ShardId(1),
        ClusterId(1),
        routes.clone(),
        vec![(MarketId(3), book())],
    )
    .unwrap();
    let second = Shard::start(
        ShardId(1),
        ClusterId(1),
        routes,
        vec![(MarketId(3), book())],
    );
    assert!(second.is_err(), "同一市场不可被两个活跃工作线程同时持有");
    first.shutdown(std::time::Duration::from_secs(1)).unwrap();
}

#[test]
fn worker_failure_disconnects_already_accepted_waiters() {
    use matching_domain::{
        LimitOrder, MarketByBase, MarketMath, OrderId, Price, PriceBandConfig, PriceReference,
        PriceReferenceKind, Qty, Side, StpKey, StpMode, UserId,
    };
    use matching_runtime::{Mutation, ShardError};
    use std::time::{Duration, Instant};
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(4),
        shard_id: ShardId(2),
        route_epoch: 7,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    for n in 1..=3000u128 {
        book.rest(LimitOrder {
            order_id: OrderId(n),
            user_id: UserId(n as u64),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Sell,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        })
        .unwrap();
    }
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.trigger_stops(
        PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 1,
            price: Price::try_new(100).unwrap(),
        },
        matching_domain::LogicalTime::new(1),
        1,
        &math,
        &[],
        |_| Ok(()),
    )
    .unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 0,
        },
        math,
    )))
    .unwrap();
    let shard = Shard::start(ShardId(2), ClusterId(1), routes, vec![(MarketId(4), book)]).unwrap();
    let handle = shard.handle();
    let submit = |mutation: Mutation| {
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            match handle.submit(
                matching_domain::CommandId(1),
                MarketId(4),
                7,
                mutation.clone(),
            ) {
                Ok(r) => break r,
                Err(ShardError::Busy) if Instant::now() < end => std::thread::yield_now(),
                Err(e) => panic!("提交失败 {e:?}"),
            }
        }
    };
    let first = submit(Mutation::Place(LimitOrder {
        order_id: OrderId(4000),
        user_id: UserId(4000),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(3000).unwrap(),
    }));
    let broken = submit(Mutation::MarketByBase(
        MarketByBase {
            order_id: OrderId(4001),
            user_id: UserId(4001),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            qty: Qty::try_new(1).unwrap(),
            market_protection: None,
        },
        MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
    ));
    let pending = submit(Mutation::Cancel(OrderId(4002)));
    assert!(first.recv_timeout(Duration::from_secs(20)).unwrap().is_ok());
    assert!(broken.recv_timeout(Duration::from_secs(2)).is_err());
    assert!(
        matches!(
            pending.recv_timeout(Duration::from_secs(2)),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        ),
        "故障时排队请求必须得到断连而不是无限等待"
    );
}

#[test]
fn same_cluster_and_shard_cannot_have_two_independent_sequencers() {
    let book = || ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let route = |market| {
        RouteTable::new([MarketRoute {
            market_id: MarketId(market),
            shard_id: ShardId(12),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap()
    };
    let mut first = Shard::start(
        ShardId(12),
        ClusterId(102),
        route(100),
        vec![(MarketId(100), book())],
    )
    .unwrap();
    let second = Shard::start(
        ShardId(12),
        ClusterId(102),
        route(101),
        vec![(MarketId(101), book())],
    );
    assert!(second.is_err(), "同一集群分片不能拥有两个独立序列分配器");
    first.shutdown(std::time::Duration::from_secs(1)).unwrap();
}

#[test]
fn low_level_shard_rejects_book_with_reservation_shadow() {
    use matching_domain::{
        AssetId, AssetReserve, LimitOrder, OrderId, OriginSessionId, Price, Qty, ReservationId,
        Side, StpKey, StpMode, UserId,
    };
    use matching_risk::{ReservationState, ReservationStatus};
    let market = MarketId(321);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(19),
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let order = LimitOrder {
        order_id: OrderId(1),
        user_id: UserId(1),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    let state = ReservationState {
        reservation_id: ReservationId(1),
        order_id: OrderId(1),
        user_id: UserId(1),
        market_id: market,
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 100,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    book.rest_with_reservation(order, state).unwrap();
    assert!(Shard::start(ShardId(19), ClusterId(999), routes, vec![(market, book)]).is_err());
}
