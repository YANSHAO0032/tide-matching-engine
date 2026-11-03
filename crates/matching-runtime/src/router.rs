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
                || handle.routes() != &routes
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

    /// 使用显式路由投递变更；调用方版本原样传给目标分片复核。
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
        self.routes
            .validate(market, route.shard_id, epoch)
            .map_err(RouterError::Rejected)?;
        self.shards
            .get(&route.shard_id)
            .ok_or(RouterError::InvalidConfig)?
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
