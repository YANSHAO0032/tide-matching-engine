//! 两个分片在迁移屏障处的市场状态交接。
use super::*;
use matching_domain::snapshot_wire::{SnapshotEncoder, SnapshotWireError};
use sha2::{Digest, Sha256};

impl SnapshotImage {
    /// 计算随市场迁移的撮合、资金、身份及行情源状态摘要。
    /// 分片控制权威及回执在迁移预检和冻结后另行核对，用户下限在目标逐用户取最大值。逐订单行情视图由行情检查点核验。
    pub fn market_state_digest(
        &self,
        market: MarketId,
        limit: usize,
    ) -> Result<[u8; 32], SnapshotWireError> {
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(b"matching-migration-market-v1")?;
        w.u32(market.get())?;
        self.markets
            .get(&market)
            .ok_or(SnapshotWireError)?
            .write_canonical_state(&mut w)?;
        if let Some(managed) = self.managed_markets.get(&market) {
            w.u8(1)?;
            managed.write_snapshot(&mut w)?;
        } else {
            w.u8(0)?;
        }
        if let Some(registry) = &self.registry {
            w.u8(1)?;
            registry
                .market_partition(market)
                .map_err(|_| SnapshotWireError)?
                .write_snapshot(&mut w)?;
        } else {
            w.u8(0)?;
        }
        if let Some(window) = &self.dedup {
            w.u8(1)?;
            window.write_market_state(market, self.shard, &mut w)?;
        } else {
            w.u8(0)?;
        }
        if let Some(place) = &self.place {
            w.u8(1)?;
            place
                .config
                .policies
                .get(&market)
                .ok_or(SnapshotWireError)?
                .write_snapshot(&mut w)?;
            place.ids.write_market_state(market, &mut w)?;
            w.u64(self.market_event_seq(market).ok_or(SnapshotWireError)?)?;
        } else {
            w.u8(0)?;
        }
        Ok(Sha256::digest(w.finish()).into())
    }

    /// 创建隔离迁移候选；普通快照写入器不能直接提交此候选。
    fn migration_candidate(&self, routes: &RouteTable) -> Result<Self, SnapshotWireError> {
        let place = self
            .place
            .as_ref()
            .map(|s| -> Result<_, SnapshotWireError> {
                Ok(PlaceRuntimeState {
                    config: s.config.clone(),
                    ids: s
                        .ids
                        .try_clone_preallocated()
                        .map_err(|_| SnapshotWireError)?,
                    market_data: s.market_data.clone(),
                    fee_book: s.fee_book.clone(),
                    shard_kill_active: s.shard_kill_active,
                })
            })
            .transpose()?;
        Ok(Self {
            replay_route_floor: Some(Arc::new(routes.clone())),
            genesis_baseline: false,
            origin: None,
            start_allowed: false,
            request_id: self.request_id,
            snapshot_seq: self.snapshot_seq,
            cluster: self.cluster,
            shard: self.shard,
            now: self.now,
            primary_epoch: self.primary_epoch,
            routes: Arc::new(routes.clone()),
            markets: self.markets.clone(),
            managed_markets: self.managed_markets.clone(),
            registry: self.registry.clone(),
            user_epochs: self
                .user_epochs
                .try_clone_preallocated()
                .map_err(|_| SnapshotWireError)?,
            dedup: self
                .dedup
                .as_ref()
                .map(DedupWindow::try_clone_preallocated)
                .transpose()
                .map_err(|_| SnapshotWireError)?,
            place,
        })
    }

    /// 把两个活动切面的路由置为预期冻结阶段，供协调器按正式流程预演完整交接。
    /// 权威提交冻结前还要核对两个分片序号。
    pub(crate) fn simulate_migration_freeze(
        mut self,
        mut target: Self,
        id: crate::MigrationId,
    ) -> Result<(Self, Self), SnapshotWireError> {
        let epoch = id.new_epoch().map_err(|_| SnapshotWireError)?;
        let route = self
            .routes
            .get(id.market)
            .copied()
            .ok_or(SnapshotWireError)?;
        if self.identity() != (id.cluster, id.source)
            || target.identity() != (id.cluster, id.target)
            || self.routes != target.routes
            || route.shard_id != id.source
            || route.state != RouteState::Active
            || route.route_epoch != id.old_epoch
        {
            return Err(SnapshotWireError);
        }
        let moving = crate::MarketRoute {
            state: RouteState::Migrating {
                source: id.source,
                target: id.target,
            },
            route_epoch: epoch,
            ..route
        };
        let frozen = RouteTable::new(
            self.routes
                .routes()
                .map(|r| if r.market_id == id.market { moving } else { *r }),
        )
        .map_err(|_| SnapshotWireError)?;
        self.routes = Arc::new(frozen);
        target.routes = Arc::clone(&self.routes);
        Ok((self, target))
    }

    /// 从双方冻结镜像准备切换后的完整分片镜像。任一检查失败保留输入，成功不代表路由已切换。
    pub fn transfer_market_to(
        &mut self,
        target: &mut Self,
        market: MarketId,
        routes_after: &RouteTable,
        limit: usize,
    ) -> Result<[u8; 32], SnapshotWireError> {
        let frozen = self.routes.get(market).ok_or(SnapshotWireError)?;
        let active = routes_after.get(market).ok_or(SnapshotWireError)?;
        if self.cluster != target.cluster
            || self.shard == target.shard
            || self.now != target.now
            || self.routes != target.routes
            || frozen.shard_id != self.shard
            || frozen.state
                != (RouteState::Migrating {
                    source: self.shard,
                    target: target.shard,
                })
            || active.route_epoch != frozen.route_epoch
            || active.shard_id != target.shard
            || active.state != RouteState::Active
            || self
                .routes
                .routes()
                .filter(|r| r.market_id != market)
                .ne(routes_after.routes().filter(|r| r.market_id != market))
            || target.markets.contains_key(&market)
            || self.registry.is_some() != target.registry.is_some()
            || self.place.is_some() != target.place.is_some()
            || self.dedup.is_some() != target.dedup.is_some()
        {
            return Err(SnapshotWireError);
        }
        let digest = self.market_state_digest(market, limit)?;
        let mut src = self.migration_candidate(routes_after)?;
        let mut dst = target.migration_candidate(routes_after)?;
        dst.markets.insert(
            market,
            src.markets.remove(&market).ok_or(SnapshotWireError)?,
        );
        if let Some(state) = src.managed_markets.remove(&market) {
            dst.managed_markets.insert(market, state);
        }
        if let (Some(s), Some(t)) = (&mut src.registry, &mut dst.registry) {
            s.transfer_market_to(t, market)
                .map_err(|_| SnapshotWireError)?;
            t.validate_order_bindings(dst.markets.values().flat_map(|b| b.reservation_snapshots()))
                .map_err(|_| SnapshotWireError)?;
        }
        if let (Some(s), Some(t)) = (&mut src.dedup, &mut dst.dedup) {
            s.transfer_market_to(t, market, src.shard)
                .map_err(|_| SnapshotWireError)?;
        }
        dst.user_epochs
            .inherit_floors(&src.user_epochs)
            .map_err(|_| SnapshotWireError)?;
        if let (Some(s), Some(t)) = (&mut src.place, &mut dst.place) {
            let mut source_keys = SnapshotEncoder::new(limit);
            let mut target_keys = SnapshotEncoder::new(limit);
            s.config.keys.write_snapshot(&mut source_keys)?;
            t.config.keys.write_snapshot(&mut target_keys)?;
            let active_count = dst
                .markets
                .values()
                .try_fold(0usize, |sum, b| {
                    sum.checked_add(b.reservation_snapshots().len())
                })
                .ok_or(SnapshotWireError)?;
            let authority = |config: &PlaceSupportConfig| {
                config
                    .control_admission
                    .as_ref()
                    .map(|binding| (binding.0.cluster_id(), binding.0.namespace()))
            };
            if authority(&s.config) != authority(&t.config)
                || source_keys.finish() != target_keys.finish()
                || s.fee_book != t.fee_book
                || s.config.managed_only != t.config.managed_only
                || active_count > t.config.limits.active_orders
            {
                return Err(SnapshotWireError);
            }
            let policy = s.config.policies.remove(&market).ok_or(SnapshotWireError)?;
            if t.config.policies.insert(market, policy).is_some() {
                return Err(SnapshotWireError);
            }
            s.ids
                .transfer_market_to(&mut t.ids, market)
                .map_err(|_| SnapshotWireError)?;
            s.market_data
                .as_mut()
                .ok_or(SnapshotWireError)?
                .transfer_market_to(t.market_data.as_mut().ok_or(SnapshotWireError)?, market)
                .map_err(|_| SnapshotWireError)?;
        }
        src.state_digest(limit)?;
        dst.state_digest(limit)?;
        if dst.market_state_digest(market, limit)? != digest {
            return Err(SnapshotWireError);
        }
        *self = src;
        *target = dst;
        Ok(digest)
    }
}

#[cfg(test)]
mod tests;
