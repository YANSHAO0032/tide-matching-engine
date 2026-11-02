//! 显式市场路由快照与分片侧校验。

use matching_domain::{MarketId, RejectReason, ShardId};
use std::collections::BTreeMap;

/// 显式路由的运行状态；迁移执行本身不在本模块中。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteState {
    /// 当前市场可由指定分片执行。
    Active,
    /// 市场迁移期间停止新的常规写入。
    Migrating {
        /// 原所属分片。
        source: ShardId,
        /// 目标分片。
        target: ShardId,
    },
}

/// 单个市场的进程内路由快照。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketRoute {
    /// 市场身份。
    pub market_id: MarketId,
    /// 生效的所属分片。
    pub shard_id: ShardId,
    /// 调用方必须显式携带的路由版本。
    pub route_epoch: u64,
    /// 当前路由状态。
    pub state: RouteState,
}

/// 路由配置存在重复市场身份。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DuplicateMarketRoute;

/// 唯一市场映射的不可变内存快照，不代表持久化控制存储。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteTable {
    /// 以市场身份确定唯一配置，不依赖哈希或取模路由。
    routes: BTreeMap<MarketId, MarketRoute>,
}

impl RouteTable {
    /// 建立快照；拒绝重复市场而不覆盖既有配置。
    pub fn new(
        routes: impl IntoIterator<Item = MarketRoute>,
    ) -> Result<Self, DuplicateMarketRoute> {
        let mut table = BTreeMap::new();
        for route in routes {
            if table.insert(route.market_id, route).is_some() {
                return Err(DuplicateMarketRoute);
            }
        }
        Ok(Self { routes: table })
    }

    /// 查询显式市场路由；找不到时返回空。
    pub fn get(&self, market_id: MarketId) -> Option<&MarketRoute> {
        self.routes.get(&market_id)
    }

    /// 依市场身份顺序读取所有路由。
    pub fn routes(&self) -> impl Iterator<Item = &MarketRoute> {
        self.routes.values()
    }

    /// 分配序号前检查状态、所有权及调用方携带的版本。
    pub fn validate(
        &self,
        market_id: MarketId,
        shard_id: ShardId,
        route_epoch: u64,
    ) -> Result<(), RejectReason> {
        let route = self.get(market_id).ok_or(RejectReason::StaleRouteEpoch)?;
        if matches!(route.state, RouteState::Migrating { .. }) {
            return Err(RejectReason::MigrationInProgress);
        }
        if route.shard_id != shard_id || route.route_epoch != route_epoch {
            return Err(RejectReason::StaleRouteEpoch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active() -> MarketRoute {
        MarketRoute {
            market_id: MarketId(7),
            shard_id: ShardId(3),
            route_epoch: 8,
            state: RouteState::Active,
        }
    }

    #[test]
    fn route_requires_exact_owner_and_caller_epoch() {
        let table = RouteTable::new([active()]).unwrap();
        assert_eq!(table.validate(MarketId(7), ShardId(3), 8), Ok(()));
        assert_eq!(
            table.validate(MarketId(7), ShardId(4), 8),
            Err(RejectReason::StaleRouteEpoch)
        );
        assert_eq!(
            table.validate(MarketId(7), ShardId(3), 7),
            Err(RejectReason::StaleRouteEpoch)
        );
        assert_eq!(
            table.validate(MarketId(9), ShardId(3), 8),
            Err(RejectReason::StaleRouteEpoch)
        );
    }

    #[test]
    fn migrating_and_duplicate_routes_are_rejected() {
        let mut route = active();
        route.state = RouteState::Migrating {
            source: ShardId(3),
            target: ShardId(4),
        };
        let table = RouteTable::new([route]).unwrap();
        assert_eq!(
            table.validate(MarketId(7), ShardId(3), 8),
            Err(RejectReason::MigrationInProgress)
        );
        assert!(RouteTable::new([active(), active()]).is_err());
    }
}
