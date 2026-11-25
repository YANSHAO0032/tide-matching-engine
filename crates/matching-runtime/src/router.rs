//! 根据显式路由快照向指定分片投递命令。

use crate::{Execution, Mutation, RouteState, RouteTable, ShardError, ShardHandle};
use matching_domain::{CommandId, MarketId, RejectReason, ShardId};
use std::collections::BTreeMap;
use std::sync::mpsc::Receiver;

/// 显式路由或目标分片的入口错误。
#[derive(Debug, PartialEq, Eq)]
pub enum RouterError {
    /// 路由状态或版本被拒绝，不分配业务序号。
    Rejected(
        /// 前置路由校验使用的稳定拒绝原因。
        RejectReason,
    ),
    /// 快照、分片身份或可用句柄不一致。
    InvalidConfig,
    /// 目标分片的入口或运行状态错误。
    Shard(
        /// 目标分片返回的入口或运行状态错误。
        ShardError,
    ),
}

/// 唯一不可变路由快照和显式分片句柄集合。
pub struct Router {
    /// 各分片共享一致的进程内路由快照。
    routes: RouteTable,
    /// 显式分片身份与其生产者入口的映射。
    shards: BTreeMap<ShardId, ShardHandle>,
}

impl Router {
    /// 核对句柄身份和路由快照，拒绝重复分片或缺失目标。
    pub fn new(
        routes: RouteTable,
        handles: impl IntoIterator<Item = (ShardId, ShardHandle)>,
    ) -> Result<Self, RouterError> {
        let mut shards = BTreeMap::new();
        for (id, handle) in handles {
            if handle.shard_id() != id
                || handle.routes().map_err(RouterError::Shard)?.as_ref() != &routes
                || shards.insert(id, handle).is_some()
            {
                return Err(RouterError::InvalidConfig);
            }
        }
        for route in routes.routes() {
            if !shards.contains_key(&route.shard_id) {
                return Err(RouterError::InvalidConfig);
            }
            if let RouteState::Migrating { source, target } = route.state
                && (!shards.contains_key(&source) || !shards.contains_key(&target))
            {
                return Err(RouterError::InvalidConfig);
            }
        }
        Ok(Self { routes, shards })
    }

    /// 使用固定所有权定位分片，再按当前快照检查版本；目标分片继续复核。
    pub fn submit(
        &self,
        command_id: CommandId,
        market: MarketId,
        epoch: u64,
        mutation: Mutation,
    ) -> Result<Receiver<Result<Execution, RejectReason>>, RouterError> {
        let route = self
            .routes
            .get(market)
            .ok_or(RouterError::Rejected(RejectReason::StaleRouteEpoch))?;
        let shard = self
            .shards
            .get(&route.shard_id)
            .ok_or(RouterError::InvalidConfig)?;
        let current = shard.routes().map_err(RouterError::Shard)?;
        current
            .validate(market, route.shard_id, epoch)
            .map_err(RouterError::Rejected)?;
        shard
            .submit(command_id, market, epoch, mutation)
            .map_err(RouterError::Shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MarketRoute, RouteState, Shard};
    use matching_domain::{
        ClusterId, LimitOrder, OrderId, Price, Qty, Side, StpKey, StpMode, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use std::time::Duration;

    fn order(id: u128) -> LimitOrder {
        LimitOrder {
            order_id: OrderId(id),
            user_id: UserId(1),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        }
    }

    fn retry<T>(mut operation: impl FnMut() -> Result<T, ShardError>) -> T {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match operation() {
                Ok(value) => return value,
                Err(ShardError::Busy) if std::time::Instant::now() < deadline => {
                    std::thread::yield_now()
                }
                Err(error) => panic!("分片入口失败: {error:?}"),
            }
        }
    }

    #[test]
    fn route_barrier_rejects_mutations_until_resumed() {
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(10),
            shard_id: ShardId(2),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = Shard::start(
            ShardId(2),
            ClusterId(2004),
            routes,
            vec![(
                MarketId(10),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        retry(|| handle.pause_route_mutations())
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let rejected =
            retry(|| handle.submit(CommandId(1), MarketId(10), 1, Mutation::Place(order(1))))
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
        assert!(matches!(rejected, Err(RejectReason::EngineBusy)));
        let fence = matching_domain::CommandEnvelope {
            command_id: CommandId(2),
            session_id: 1,
            client_seq: 1,
            market_id: None,
            user_id: Some(UserId(1)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: matching_domain::Command::ApplyUserControlFenceShard(
                matching_domain::ApplyUserControlFenceShard {
                    parent_command_id: CommandId(2),
                    user_id: UserId(1),
                    new_epoch: 1,
                    action: matching_domain::UserFenceAction::FenceOnly,
                    expected_primary_epoch: 1,
                },
            ),
        };
        let formal = retry(|| handle.submit_command(fence.clone()))
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(matches!(
            formal,
            Err(crate::FormalError::Rejected(RejectReason::EngineBusy))
        ));
        retry(|| handle.resume_route_mutations())
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let result =
            retry(|| handle.submit(CommandId(1), MarketId(10), 1, Mutation::Place(order(1))))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap();
        assert_eq!(result.seq, 1);
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn router_accepts_current_epoch_after_route_refresh() {
        let make_routes = |epoch| {
            RouteTable::new([MarketRoute {
                market_id: MarketId(10),
                shard_id: ShardId(2),
                route_epoch: epoch,
                state: RouteState::Active,
            }])
            .unwrap()
        };
        let mut shard = Shard::start(
            ShardId(2),
            ClusterId(2010),
            make_routes(1),
            vec![(
                MarketId(10),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let router = Router::new(make_routes(1), [(ShardId(2), handle.clone())]).unwrap();
        retry(|| handle.install_routes(make_routes(2)))
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(matches!(
            router.submit(CommandId(1), MarketId(10), 1, Mutation::Place(order(1))),
            Err(RouterError::Rejected(RejectReason::StaleRouteEpoch))
        ));
        let result = router
            .submit(CommandId(1), MarketId(10), 2, Mutation::Place(order(1)))
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(result.seq, 1);
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn route_refresh_is_ordered_with_mutations_without_consuming_sequence() {
        let make_routes = |epoch| {
            RouteTable::new([MarketRoute {
                market_id: MarketId(10),
                shard_id: ShardId(2),
                route_epoch: epoch,
                state: RouteState::Active,
            }])
            .unwrap()
        };
        let mut shard = Shard::start(
            ShardId(2),
            ClusterId(2003),
            make_routes(7),
            vec![(
                MarketId(10),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let first =
            retry(|| handle.submit(CommandId(1), MarketId(10), 7, Mutation::Place(order(1))));
        let refresh = retry(|| handle.install_routes(make_routes(8)));
        let stale =
            retry(|| handle.submit(CommandId(2), MarketId(10), 7, Mutation::Place(order(2))));
        assert_eq!(
            first
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .seq,
            1
        );
        refresh
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(matches!(
            stale.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(RejectReason::StaleRouteEpoch)
        ));
        let second =
            retry(|| handle.submit(CommandId(2), MarketId(10), 8, Mutation::Place(order(2))))
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap();
        assert_eq!(second.seq, 2);
        assert!(matches!(
            retry(|| handle.install_routes(make_routes(7)))
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Err(ShardError::InvalidConfig)
        ));
        let reassigned = RouteTable::new([MarketRoute {
            market_id: MarketId(10),
            shard_id: ShardId(3),
            route_epoch: 9,
            state: RouteState::Active,
        }])
        .unwrap();
        assert!(matches!(
            retry(|| handle.install_routes(reassigned.clone()))
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Err(ShardError::InvalidConfig)
        ));
        let migrating = RouteTable::new([
            *make_routes(8).get(MarketId(10)).unwrap(),
            MarketRoute {
                market_id: MarketId(11),
                shard_id: ShardId(3),
                route_epoch: 1,
                state: RouteState::Migrating {
                    source: ShardId(3),
                    target: ShardId(2),
                },
            },
        ])
        .unwrap();
        assert!(matches!(
            retry(|| handle.install_routes(migrating.clone()))
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Err(ShardError::InvalidConfig)
        ));
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn explicit_routes_use_separate_shards_and_preserve_caller_epoch() {
        let table = RouteTable::new([
            MarketRoute {
                market_id: MarketId(10),
                shard_id: ShardId(2),
                route_epoch: 7,
                state: RouteState::Active,
            },
            MarketRoute {
                market_id: MarketId(11),
                shard_id: ShardId(1),
                route_epoch: 3,
                state: RouteState::Active,
            },
        ])
        .unwrap();
        let book = || ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        let mut one = Shard::start(
            ShardId(1),
            ClusterId(2000),
            table.clone(),
            vec![(MarketId(11), book())],
        )
        .unwrap();
        let mut two = Shard::start(
            ShardId(2),
            ClusterId(2000),
            table.clone(),
            vec![(MarketId(10), book())],
        )
        .unwrap();
        let router = Router::new(
            table,
            [(ShardId(1), one.handle()), (ShardId(2), two.handle())],
        )
        .unwrap();
        assert!(matches!(
            router.submit(CommandId(1), MarketId(10), 6, Mutation::Place(order(1))),
            Err(RouterError::Rejected(RejectReason::StaleRouteEpoch))
        ));
        let a = router
            .submit(CommandId(1), MarketId(10), 7, Mutation::Place(order(1)))
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let b = router
            .submit(CommandId(1), MarketId(11), 3, Mutation::Place(order(2)))
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!((a.seq, b.seq), (1, 1));
        one.shutdown(Duration::from_secs(2)).unwrap();
        two.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn rejects_mismatched_snapshot_duplicate_and_missing_shard() {
        let route = MarketRoute {
            market_id: MarketId(10),
            shard_id: ShardId(2),
            route_epoch: 7,
            state: RouteState::Active,
        };
        let table = RouteTable::new([route]).unwrap();
        let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        let mut shard = Shard::start(
            ShardId(2),
            ClusterId(2001),
            table.clone(),
            vec![(MarketId(10), book)],
        )
        .unwrap();
        let handle = shard.handle();
        assert!(matches!(
            Router::new(table.clone(), []),
            Err(RouterError::InvalidConfig)
        ));
        assert!(matches!(
            Router::new(
                table.clone(),
                [(ShardId(2), handle.clone()), (ShardId(2), handle.clone())]
            ),
            Err(RouterError::InvalidConfig)
        ));
        assert!(matches!(
            Router::new(table.clone(), [(ShardId(1), handle.clone())]),
            Err(RouterError::InvalidConfig)
        ));
        let different = RouteTable::new([MarketRoute {
            route_epoch: 8,
            ..route
        }])
        .unwrap();
        assert!(matches!(
            Router::new(different, [(ShardId(2), handle)]),
            Err(RouterError::InvalidConfig)
        ));
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn migration_and_unknown_market_are_pre_seq_rejections() {
        let route = MarketRoute {
            market_id: MarketId(10),
            shard_id: ShardId(2),
            route_epoch: 7,
            state: RouteState::Migrating {
                source: ShardId(2),
                target: ShardId(1),
            },
        };
        let table = RouteTable::new([route]).unwrap();
        let mut one = Shard::start(ShardId(1), ClusterId(2002), table.clone(), vec![]).unwrap();
        let mut two = Shard::start(ShardId(2), ClusterId(2002), table.clone(), vec![]).unwrap();
        let router = Router::new(
            table,
            [(ShardId(1), one.handle()), (ShardId(2), two.handle())],
        )
        .unwrap();
        assert!(matches!(
            router.submit(CommandId(1), MarketId(10), 7, Mutation::Place(order(1))),
            Err(RouterError::Rejected(RejectReason::MigrationInProgress))
        ));
        assert!(matches!(
            router.submit(CommandId(1), MarketId(99), 7, Mutation::Place(order(1))),
            Err(RouterError::Rejected(RejectReason::StaleRouteEpoch))
        ));
        assert_eq!(
            one.handle()
                .barrier(1)
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .1,
            0
        );
        assert_eq!(
            two.handle()
                .barrier(1)
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .1,
            0
        );
        one.shutdown(Duration::from_secs(2)).unwrap();
        two.shutdown(Duration::from_secs(2)).unwrap();
    }
}
