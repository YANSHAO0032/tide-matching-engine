//! 市场迁移使用同一份路由权威，源端冻结与目标端切换分别提交。

use super::{
    MarketRoute, MonitorCommand, RouteAuthority, RouteCoordinator, RouteMonitor, RouteState,
};
use crate::{MigrationId, RouteTable, ShardHandle};
use matching_domain::ShardId;
use std::{io, time::Duration};

/// 外围协调器完成大块数据传输后才请求第二次比较写入。
#[derive(Clone, Copy)]
pub(crate) enum MigrationAction {
    /// 只核验现有阶段。
    Read,
    /// 冻结源和目标，参数依次为预检时两侧的持久水位。
    Begin((u64, u64)),
    /// 目标分片已接收市场、源分片已撤出市场，两侧镜像均已登记，可以切换所有权。
    Finish,
}

/// 仅供持久迁移流程使用；通用路由更新不能切换所有权。
pub(crate) trait MigrationRoutes {
    /// 返回该权威实际管理的分片入口。
    fn migration_shard(&self, shard: ShardId) -> io::Result<ShardHandle>;

    /// 在路由写者所属线程执行单个阶段，快照传输期间监视线程继续续约。
    fn migration_action(
        &mut self,
        id: MigrationId,
        action: MigrationAction,
        timeout: Duration,
    ) -> io::Result<MarketRoute>;

    /// 核验当前权威和迁移身份。
    fn migration_route(&mut self, id: MigrationId, timeout: Duration) -> io::Result<MarketRoute> {
        self.migration_action(id, MigrationAction::Read, timeout)
    }

    /// 只允许从指定源和旧纪元冻结，也接受相同迁移的重复冻结。
    fn begin_migration(
        &mut self,
        id: MigrationId,
        prefixes: (u64, u64),
        timeout: Duration,
    ) -> io::Result<()> {
        self.migration_action(id, MigrationAction::Begin(prefixes), timeout)
            .map(|_| ())
    }

    /// 提交目标所有权并保留冻结时已分配的纪元。
    fn finish_migration(&mut self, id: MigrationId, timeout: Duration) -> io::Result<()> {
        self.migration_action(id, MigrationAction::Finish, timeout)
            .map(|_| ())
    }
}

fn find_shard(shards: &[ShardHandle], id: ShardId) -> io::Result<ShardHandle> {
    shards
        .iter()
        .find(|s| s.shard_id() == id)
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "迁移分片未登记"))
}

impl MigrationRoutes for RouteMonitor {
    fn migration_shard(&self, shard: ShardId) -> io::Result<ShardHandle> {
        find_shard(&self.shards, shard)
    }

    fn migration_action(
        &mut self,
        id: MigrationId,
        action: MigrationAction,
        timeout: Duration,
    ) -> io::Result<MarketRoute> {
        self.request(timeout, |reply| MonitorCommand::Migration {
            id,
            action,
            timeout,
            reply,
        })
    }
}

impl<A: RouteAuthority> MigrationRoutes for RouteCoordinator<A> {
    fn migration_shard(&self, shard: ShardId) -> io::Result<ShardHandle> {
        find_shard(&self.shards, shard)
    }

    fn migration_action(
        &mut self,
        id: MigrationId,
        action: MigrationAction,
        timeout: Duration,
    ) -> io::Result<MarketRoute> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.apply_migration(id, action, timeout)
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.fence_ingress();
                std::panic::resume_unwind(payload)
            }
        }
    }
}

impl<A: RouteAuthority> RouteCoordinator<A> {
    /// 从权威读取当前完整路由表，只改动迁移身份指定的市场。
    pub(super) fn apply_migration(
        &mut self,
        id: MigrationId,
        action: MigrationAction,
        timeout: Duration,
    ) -> io::Result<MarketRoute> {
        if self.awaiting_monitor
            || self.cluster != id.cluster
            || id.source == id.target
            || timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "迁移身份、期限或监视状态无效",
            ));
        }
        self.migration_shard(id.source)?;
        self.migration_shard(id.target)?;
        let (version, current) = self.read_verified()?;
        let old = MarketRoute {
            market_id: id.market,
            shard_id: id.source,
            route_epoch: id.old_epoch,
            state: RouteState::Active,
        };
        let moving = MarketRoute {
            route_epoch: id.new_epoch()?,
            state: RouteState::Migrating {
                source: id.source,
                target: id.target,
            },
            ..old
        };
        let active = MarketRoute {
            shard_id: id.target,
            state: RouteState::Active,
            ..moving
        };
        let observed = current.get(id.market).copied();
        let next = match (action, observed) {
            // 只有持久交接已存在时，上层才能用更晚的纪元补记历史切换边界。
            (MigrationAction::Read, Some(r))
                if [old, moving, active].contains(&r) || r.route_epoch > active.route_epoch =>
            {
                return Ok(r);
            }
            (MigrationAction::Begin(_), Some(r)) if r == moving => return Ok(r),
            (MigrationAction::Finish, Some(r)) if r == active => return Ok(r),
            (MigrationAction::Begin(_), Some(r))
                if r == old && current.routes().all(|r| r.state == RouteState::Active) =>
            {
                moving
            }
            (MigrationAction::Finish, Some(r)) if r == moving => active,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "迁移阶段或权威路由不相符",
                ));
            }
        };
        let routes = RouteTable::new(
            current
                .routes()
                .map(|r| if r.market_id == id.market { next } else { *r }),
        )
        .map_err(|_| super::invalid())?;
        let checked_prefixes = match action {
            MigrationAction::Begin(prefixes) => Some(prefixes),
            _ => None,
        };
        self.publish_routes(
            version,
            &current,
            routes,
            timeout,
            Some((id.source, id.target)),
            checked_prefixes,
        )?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Mutation, RouteControlStore, Shard, ShardError};
    use matching_domain::{ClusterId, CommandId, MarketId, OrderId, RejectReason};
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};

    #[test]
    fn changed_preflight_prefixes_resume_both_shards_without_committing_freeze() {
        let id = MigrationId {
            cluster: ClusterId(7300),
            source: ShardId(2),
            target: ShardId(1),
            market: MarketId(11),
            old_epoch: 4,
        };
        let routes = RouteTable::new([(id.source, id.market), (id.target, MarketId(12))].map(
            |(shard_id, market_id)| MarketRoute {
                market_id,
                shard_id,
                route_epoch: 4,
                state: RouteState::Active,
            },
        ))
        .unwrap();
        let dir = std::env::temp_dir().join(format!(
            "tide-migration-prefix-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut shards = Vec::new();
        for route in routes.routes() {
            let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(42).unwrap());
            shards.push(
                Shard::start(
                    route.shard_id,
                    id.cluster,
                    routes.clone(),
                    vec![(route.market_id, book)],
                )
                .unwrap(),
            );
        }
        let handles: Vec<_> = shards.iter().map(Shard::handle).collect();
        let store =
            RouteControlStore::create_for_cluster(&dir, id.cluster, routes.clone(), 4, 8).unwrap();
        let timeout = Duration::from_secs(3);
        let mut coordinator = RouteCoordinator::new(store, handles.clone()).unwrap();
        for (i, prefixes) in [(1, 0), (0, 1)].into_iter().enumerate() {
            assert_eq!(
                coordinator
                    .begin_migration(id, prefixes, timeout)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(coordinator.verify_authority().unwrap(), 1);

            let handle = &handles[i];
            assert!(handle.observation().route_ready);
            assert_eq!(
                crate::route_store::await_reply(std::time::Instant::now() + timeout, || handle
                    .barrier(0))
                .unwrap()
                .1,
                0
            );
        }
        coordinator.begin_migration(id, (0, 0), timeout).unwrap();
        assert_eq!(coordinator.verify_authority().unwrap(), 2);
        coordinator.begin_migration(id, (0, 0), timeout).unwrap();
        assert_eq!(coordinator.verify_authority().unwrap(), 2);
        for handle in &handles {
            let deadline = std::time::Instant::now() + timeout;
            let response = loop {
                match handle.submit(CommandId(9), id.market, 5, Mutation::Cancel(OrderId(9))) {
                    Ok(reply) => break reply,
                    Err(ShardError::Busy) if std::time::Instant::now() < deadline => {
                        std::thread::yield_now()
                    }
                    Err(error) => panic!("前置拒绝请求未入队: {error:?}"),
                }
            };
            assert!(matches!(
                response.recv_timeout(timeout).unwrap(),
                Err(RejectReason::MigrationInProgress)
            ));
            assert_eq!(
                crate::route_store::await_reply(std::time::Instant::now() + timeout, || handle
                    .barrier(0))
                .unwrap()
                .1,
                0
            );
        }
        for shard in &mut shards {
            shard.shutdown(timeout).unwrap();
        }
        drop(coordinator);
        let directory = dir.canonicalize().unwrap();
        assert_eq!(
            directory.parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
