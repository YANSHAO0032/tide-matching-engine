//! 分片不可变镜像的规范编码及可信配置校验。
use super::*;
use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder, SnapshotWireError};

/// 恢复时由外部可信配置提供的身份、容量和路由约束。
#[derive(Clone)]
pub struct SnapshotRestoreConfig {
    /// 预期集群身份。
    pub cluster: ClusterId,
    /// 预期分片身份。
    pub shard: ShardId,
    /// 预期主节点纪元。
    pub primary_epoch: u64,
    /// 从可信路由来源装载的完整路由。
    pub routes: RouteTable,
    /// 用户控制下限的预分配容量。
    pub user_capacity: usize,
    /// 去重窗口的预分配容量。
    pub idempotency: IdempotencyConfig,
    /// 可信下单配置；须与镜像完全相同。
    pub place: Option<PlaceSupportConfig>,
    /// 分片是否启用预留登记与正式命令窗口。
    pub expect_registry: bool,
    /// 允许恢复的市场数量上限。
    pub max_markets: usize,
    /// 允许解析的镜像字节上限。
    pub max_bytes: usize,
}

/// 已验证的快照候选；仍须回放其后的已提交 WAL 才能恢复持久分片。
pub struct SnapshotRecoveryCandidate {
    /// 已验证的快照状态。
    pub image: SnapshotImage,
    /// 权威清单记录的本地持久前缀，不能因回退快照而回退。
    pub committed_through: u64,
    /// 是否因最新快照不可用而选择上一份。
    pub used_previous: bool,
}

/// 将路由转换为按市场顺序排列的规范字节。
fn route_bytes(routes: &RouteTable, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
    let mut w = SnapshotEncoder::new(limit);
    let count = routes.routes().count();
    w.u32(u32::try_from(count).map_err(|_| SnapshotWireError)?)?;
    for route in routes.routes() {
        w.u32(route.market_id.get())?;
        w.u16(route.shard_id.get())?;
        w.u64(route.route_epoch)?;
        match route.state {
            RouteState::Active => w.u8(0)?,
            RouteState::Migrating { source, target } => {
                w.u8(1)?;
                w.u16(source.get())?;
                w.u16(target.get())?
            }
        }
    }
    Ok(w.finish())
}

/// 将完整可信下单配置编码，以便恢复时逐字节核验。
fn place_bytes(config: &PlaceSupportConfig, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
    let mut w = SnapshotEncoder::new(limit);
    config.keys.write_snapshot(&mut w)?;
    for value in [
        config.limits.active_orders,
        config.limits.terminal_history,
        config.limits.client_id_bytes,
    ] {
        w.u64(u64::try_from(value).map_err(|_| SnapshotWireError)?)?;
    }
    w.u32(u32::try_from(config.policies.len()).map_err(|_| SnapshotWireError)?)?;
    for (market, policy) in &config.policies {
        w.u32(market.get())?;
        policy.write_snapshot(&mut w)?;
    }
    if config.managed_only {
        w.u8(1)?;
    }
    Ok(w.finish())
}

impl SnapshotImage {
    /// 核验市场管理状态与本地订单簿及价格带一致。
    fn validate_managed_markets(&self) -> Result<(), SnapshotWireError> {
        if let (Some(fees), Some(registry)) = (
            self.place
                .as_ref()
                .and_then(|place| place.fee_book.as_ref()),
            self.registry.as_ref(),
        ) {
            registry
                .validate_fee_references(fees)
                .map_err(|_| SnapshotWireError)?;
        }
        for (id, state) in &self.managed_markets {
            let book = self.markets.get(id).ok_or(SnapshotWireError)?;
            let math = state.config().validate().map_err(|_| SnapshotWireError)?;
            let has_active_orders = book.best_price(matching_domain::Side::Buy).is_some()
                || book.best_price(matching_domain::Side::Sell).is_some()
                || !book.stop_orders(matching_domain::Side::Buy).is_empty()
                || !book.stop_orders(matching_domain::Side::Sell).is_empty();
            if let Some(band) = state.config().price_band {
                let needs_reference = state.lifecycle().state()
                    == matching_domain::MarketState::Open
                    || (state.lifecycle().state() == matching_domain::MarketState::PreOpen
                        && has_active_orders);
                if needs_reference && !book.has_reference_price(band.reference_kind) {
                    return Err(SnapshotWireError);
                }
                if book.has_reference_price(band.reference_kind) {
                    book.price_band_bounds().map_err(|_| SnapshotWireError)?;
                }
            }
            if !state.has_referenced_history()
                && (book.next_queue_priority() != 0
                    || book.next_stop_priority() != 0
                    || book.best_price(matching_domain::Side::Buy).is_some()
                    || book.best_price(matching_domain::Side::Sell).is_some()
                    || !book.stop_orders(matching_domain::Side::Buy).is_empty()
                    || !book.stop_orders(matching_domain::Side::Sell).is_empty())
            {
                return Err(SnapshotWireError);
            }
            let limits = state.config().execution_limits;
            let priority = matching_orderbook::QueuePriorityPolicy::try_with_synthetic_ceiling(
                limits.priority_exhaustion_guard,
                limits.synthetic_priority_ceiling,
            )
            .map_err(|_| SnapshotWireError)?;
            if book.priority_policy() != priority {
                return Err(SnapshotWireError);
            }
            if let Some(place) = &self.place {
                let policy = place.config.policies.get(id).ok_or(SnapshotWireError)?;
                let fee_version_available = place
                    .fee_book
                    .as_ref()
                    .map(|fees| {
                        fees.schedule(state.config().active_fee_schedule_version)
                            .is_ok()
                    })
                    .unwrap_or_else(|| {
                        policy.fee_schedule_version() == state.config().active_fee_schedule_version
                    });
                if policy.market_math() != math || !fee_version_available {
                    return Err(SnapshotWireError);
                }
            }
            if state.config().market_id != *id
                || book.price_band_config() != state.config().price_band.map(|band| (band, math))
                || (state.lifecycle().state() == matching_domain::MarketState::Closed
                    && (book.best_price(matching_domain::Side::Buy).is_some()
                        || book.best_price(matching_domain::Side::Sell).is_some()
                        || !book.stop_orders(matching_domain::Side::Buy).is_empty()
                        || !book.stop_orders(matching_domain::Side::Sell).is_empty()))
            {
                return Err(SnapshotWireError);
            }
        }
        Ok(())
    }
    /// 计算当前可用业务状态的 SHA-256 摘要。
    pub fn state_digest(&self, limit: usize) -> Result<[u8; 32], SnapshotWireError> {
        use sha2::{Digest, Sha256};
        Ok(Sha256::digest(self.canonical_state(limit)?).into())
    }
    /// 按业务状态顺序编码快照，供后续状态摘要计算；不包含请求身份和物理槽位。
    pub fn canonical_state(&self, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
        self.validate_managed_markets()?;
        let mut w = SnapshotEncoder::new(limit);
        w.u16(1)?;
        w.u16(self.cluster.get())?;
        w.u16(self.shard.get())?;
        w.u64(self.snapshot_seq)?;
        w.u64(self.now.get())?;
        w.u64(self.primary_epoch)?;
        w.bytes(&route_bytes(&self.routes, limit)?)?;
        w.u32(u32::try_from(self.markets.len()).map_err(|_| SnapshotWireError)?)?;
        for (market, book) in &self.markets {
            w.u32(market.get())?;
            book.write_canonical_state(&mut w)?;
        }
        match &self.registry {
            None => w.u8(0)?,
            Some(registry) => {
                w.u8(1)?;
                registry.write_snapshot(&mut w)?;
            }
        }
        self.user_epochs.write_canonical_state(&mut w)?;
        match &self.dedup {
            None => w.u8(0)?,
            Some(window) => {
                w.u8(1)?;
                window.write_canonical_state(&mut w)?;
            }
        }
        match &self.place {
            None => w.u8(0)?,
            Some(place) => {
                w.u8(1)?;
                w.bytes(&place_bytes(&place.config, limit)?)?;
                place.ids.write_canonical_state(&mut w)?;
            }
        }
        // 空集合保留旧版本的规范摘要，避免改变已提交快照的校验值。
        if !self.managed_markets.is_empty() {
            w.u8(1)?;
            w.u32(u32::try_from(self.managed_markets.len()).map_err(|_| SnapshotWireError)?)?;
            for (market, state) in &self.managed_markets {
                w.u32(market.get())?;
                state.write_snapshot(&mut w)?;
            }
        }
        if let Some(book) = self
            .place
            .as_ref()
            .and_then(|place| place.fee_book.as_ref())
        {
            w.u8(2)?;
            book.write_snapshot(&mut w)?;
        }
        if self
            .place
            .as_ref()
            .is_some_and(|place| place.shard_kill_active)
        {
            w.u8(3)?;
        }
        Ok(w.finish())
    }
    /// 选择最新可校验快照；损坏或缺失时尝试清单记录的上一份。
    ///
    /// 返回候选不代表 WAL 已重放，调用方须从快照之后恢复到权威持久水位。
    pub fn read_recovery_candidate(
        directory: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &SnapshotRestoreConfig,
    ) -> std::io::Result<Option<SnapshotRecoveryCandidate>> {
        let directory = directory.as_ref();
        if !manager.recovery_checked() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "清单未经过严格恢复检查",
            ));
        }
        let committed = manager.state().local_durable_seq;
        if manager
            .state()
            .latest_snapshot
            .is_some_and(|(_, seq)| seq > committed)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "清单快照超过本地持久前缀",
            ));
        }
        let check = |image: Self, seq: u64| -> std::io::Result<Self> {
            if image.snapshot_seq != seq || seq > committed || seq < manager.state().wal_gc_safe_seq
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "快照不在可恢复前缀内",
                ));
            }
            Ok(image)
        };
        match Self::read_committed_snapshot(directory, manager, config) {
            Ok(Some(image)) => {
                let seq = image.snapshot_seq;
                Ok(Some(SnapshotRecoveryCandidate {
                    image: check(image, seq)?,
                    committed_through: committed,
                    used_previous: false,
                }))
            }
            Ok(None) => Ok(None),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::InvalidData | std::io::ErrorKind::NotFound
                ) =>
            {
                let Some((_, seq, bytes)) =
                    matching_persistence::read_previous_snapshot(directory, manager)?
                else {
                    return Err(error);
                };
                let image = Self::decode_snapshot(&bytes, config).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "上一快照镜像无效")
                })?;
                Ok(Some(SnapshotRecoveryCandidate {
                    image: check(image, seq)?,
                    committed_through: committed,
                    used_previous: true,
                }))
            }
            Err(error) => Err(error),
        }
    }
    /// 从严格恢复检查通过的清单读取镜像，核对文件与载荷序号。
    pub fn read_committed_snapshot(
        directory: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &SnapshotRestoreConfig,
    ) -> std::io::Result<Option<Self>> {
        if !manager.recovery_checked() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "清单未经过严格恢复检查",
            ));
        }
        let Some((_, seq, bytes)) =
            matching_persistence::read_committed_snapshot(directory, manager)?
        else {
            return Ok(None);
        };
        let mut image = Self::decode_snapshot(&bytes, config)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "快照镜像无效"))?;
        if image.snapshot_seq != seq
            || seq > manager.state().local_durable_seq
            || seq < manager.state().wal_gc_safe_seq
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "快照序号不一致或位于不可恢复前缀",
            ));
        }
        image.start_allowed = seq == manager.state().local_durable_seq;
        Ok(Some(image))
    }
    /// 将独立内存镜像编码为有界确定性载荷；不提交或同步文件。
    pub fn encode_snapshot(&self, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
        let mut w = SnapshotEncoder::new(limit);
        let fee_book = self
            .place
            .as_ref()
            .and_then(|place| place.fee_book.as_ref());
        let shard_kill_active = self
            .place
            .as_ref()
            .is_some_and(|place| place.shard_kill_active);
        w.u16(match (fee_book.is_some(), shard_kill_active) {
            (false, false) => 3,
            (true, false) => 4,
            (false, true) => 5,
            (true, true) => 6,
        })?;
        w.bytes(env!("CARGO_PKG_VERSION").as_bytes())?;
        w.u128(self.request_id)?;
        w.u64(self.snapshot_seq)?;
        w.u16(self.cluster.get())?;
        w.u16(self.shard.get())?;
        w.u64(self.now.get())?;
        w.u64(self.primary_epoch)?;
        w.bytes(&route_bytes(&self.routes, limit)?)?;
        w.u32(u32::try_from(self.markets.len()).map_err(|_| SnapshotWireError)?)?;
        for (market, book) in &self.markets {
            w.u32(market.get())?;
            book.write_snapshot(&mut w)?;
        }
        match &self.registry {
            None => w.u8(0)?,
            Some(registry) => {
                w.u8(1)?;
                registry.write_snapshot(&mut w)?;
            }
        }
        self.user_epochs.write_snapshot(&mut w)?;
        match &self.dedup {
            None => w.u8(0)?,
            Some(dedup) => {
                w.u8(1)?;
                dedup.write_snapshot(&mut w)?;
            }
        }
        match &self.place {
            None => w.u8(0)?,
            Some(place) => {
                w.u8(1)?;
                w.bytes(&place_bytes(&place.config, limit)?)?;
                place.ids.write_snapshot(&mut w)?;
            }
        }
        w.u32(u32::try_from(self.managed_markets.len()).map_err(|_| SnapshotWireError)?)?;
        for (market, state) in &self.managed_markets {
            w.u32(market.get())?;
            state.write_snapshot(&mut w)?;
        }
        if let Some(book) = fee_book {
            book.write_snapshot(&mut w)?;
        }
        w.bytes(&self.state_digest(limit)?)?;
        Ok(w.finish())
    }
    /// 使用外部可信身份、容量与配置恢复镜像并检查跨组件约束。
    pub fn decode_snapshot(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
    ) -> Result<Self, SnapshotWireError> {
        let mut r = SnapshotDecoder::new(bytes, config.max_bytes)?;
        let version = r.u16()?;
        if !(2..=6).contains(&version) {
            return Err(SnapshotWireError);
        }
        if r.bytes()? != env!("CARGO_PKG_VERSION").as_bytes() {
            return Err(SnapshotWireError);
        }
        let request_id = r.u128()?;
        let snapshot_seq = r.u64()?;
        let cluster = ClusterId::new(r.u16()?);
        let shard = ShardId::new(r.u16()?);
        let now = LogicalTime::new(r.u64()?);
        let primary_epoch = r.u64()?;
        let historical_epoch_allowed = config
            .place
            .as_ref()
            .and_then(|place| place.primary_lease.as_ref())
            .is_some_and(|lease| {
                primary_epoch > 0
                    && primary_epoch <= config.primary_epoch
                    && lease.verify(cluster, shard, config.primary_epoch).is_ok()
            });
        if cluster != config.cluster
            || shard != config.shard
            || (primary_epoch != config.primary_epoch && !historical_epoch_allowed)
            || r.bytes()? != route_bytes(&config.routes, config.max_bytes)?
        {
            return Err(SnapshotWireError);
        }
        let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        if count > config.max_markets || count > r.remaining() / 8 {
            return Err(SnapshotWireError);
        }
        let mut markets = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let market = MarketId::new(r.u32()?);
            if previous.is_some_and(|p| p >= market) {
                return Err(SnapshotWireError);
            }
            previous = Some(market);
            let book = ProductionOrderBook::read_snapshot(&mut r)?;
            if book.market_id().is_some_and(|id| id != market)
                || config
                    .routes
                    .get(market)
                    .is_none_or(|route| route.shard_id != shard)
            {
                return Err(SnapshotWireError);
            }
            let mut probe = book.clone();
            if !probe.expire_gtd_through(now).is_empty()
                || !probe.expire_stop_gtd_through(now).is_empty()
            {
                return Err(SnapshotWireError);
            }
            markets.insert(market, book);
        }
        if config.routes.routes().any(|route| {
            route.shard_id == shard
                && route.state == RouteState::Active
                && !markets.contains_key(&route.market_id)
        }) {
            return Err(SnapshotWireError);
        }
        let registry = match r.u8()? {
            0 => None,
            1 => Some(ReservationRegistry::read_snapshot(&mut r)?),
            _ => return Err(SnapshotWireError),
        };
        if registry.is_some() != config.expect_registry {
            return Err(SnapshotWireError);
        }
        if let Some(registry) = &registry {
            if markets
                .iter()
                .any(|(id, book)| book.market_id() != Some(*id))
            {
                return Err(SnapshotWireError);
            }
            for (market, book) in &markets {
                book.plan_mass_cancel(
                    matching_domain::ShardCancelScope::Market { market_id: *market },
                    registry,
                )
                .map_err(|_| SnapshotWireError)?;
            }
            registry
                .validate_order_bindings(
                    markets
                        .values()
                        .flat_map(|book| book.reservation_snapshots()),
                )
                .map_err(|_| SnapshotWireError)?;
        } else if markets
            .values()
            .any(|book| !book.reservation_snapshots().is_empty())
        {
            return Err(SnapshotWireError);
        }
        let user_epochs = UserControlEpochs::read_snapshot(&mut r, config.user_capacity)?;
        let dedup = match r.u8()? {
            0 => None,
            1 => Some(DedupWindow::read_snapshot(
                &mut r,
                config.idempotency,
                snapshot_seq,
            )?),
            _ => return Err(SnapshotWireError),
        };
        if dedup.is_some() != registry.is_some() {
            return Err(SnapshotWireError);
        }
        let mut encoded_place = None;
        let mut place = match r.u8()? {
            0 => {
                if config.place.is_some() {
                    return Err(SnapshotWireError);
                }
                None
            }
            1 => {
                let expected = config.place.as_ref().ok_or(SnapshotWireError)?;
                encoded_place = Some(r.bytes()?.to_vec());
                let limits = expected.limits;
                let ids = ClientOrderIds::read_snapshot(
                    &mut r,
                    limits.active_orders,
                    limits.terminal_history,
                    limits.client_id_bytes,
                )?;
                if registry.is_none()
                    || !ids.validate_active(|market, id, user| {
                        markets
                            .get(&market)
                            .and_then(|book| book.order(id))
                            .is_some_and(|node| node.original_order().user_id == user)
                    })
                {
                    return Err(SnapshotWireError);
                }
                Some(PlaceRuntimeState {
                    config: expected.clone(),
                    ids,
                    fee_book: None,
                    shard_kill_active: false,
                })
            }
            _ => return Err(SnapshotWireError),
        };
        let mut managed_markets = BTreeMap::new();
        if version >= 3 {
            let managed_count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
            if managed_count > config.max_markets || managed_count > markets.len() {
                return Err(SnapshotWireError);
            }
            let mut previous = None;
            for _ in 0..managed_count {
                let market = MarketId::new(r.u32()?);
                if previous.is_some_and(|old| old >= market) {
                    return Err(SnapshotWireError);
                }
                previous = Some(market);
                let state = matching_domain::ManagedMarket::read_snapshot(&mut r)?;
                let book = markets.get(&market).ok_or(SnapshotWireError)?;
                let math = state.config().validate().map_err(|_| SnapshotWireError)?;
                if state.config().market_id != market
                    || book.price_band_config()
                        != state.config().price_band.map(|band| (band, math))
                    || (state.lifecycle().state() == matching_domain::MarketState::Closed
                        && (book.best_price(matching_domain::Side::Buy).is_some()
                            || book.best_price(matching_domain::Side::Sell).is_some()
                            || !book.stop_orders(matching_domain::Side::Buy).is_empty()
                            || !book.stop_orders(matching_domain::Side::Sell).is_empty()))
                {
                    return Err(SnapshotWireError);
                }
                managed_markets.insert(market, state);
            }
        }
        if version == 4 || version == 6 {
            let state = place.as_mut().ok_or(SnapshotWireError)?;
            state.fee_book = Some(matching_domain::FeeScheduleBook::read_snapshot(
                &mut r,
                config.max_bytes / 13,
                config.max_bytes / 21,
            )?);
        }
        if version >= 5 {
            place.as_mut().ok_or(SnapshotWireError)?.shard_kill_active = true;
        }
        if let Some(place) = &mut place {
            for (market, state) in &managed_markets {
                let policy = place
                    .config
                    .policies
                    .get_mut(market)
                    .ok_or(SnapshotWireError)?;
                *policy = policy
                    .with_market_math(state.config().validate().map_err(|_| SnapshotWireError)?)
                    .map_err(|_| SnapshotWireError)?;
            }
            if encoded_place.as_deref()
                != Some(place_bytes(&place.config, config.max_bytes)?.as_slice())
                || place.config.policies.iter().any(|(market, policy)| {
                    markets.get(market).is_none_or(|book| {
                        book.price_band_config()
                            .is_some_and(|(_, math)| math != policy.market_math())
                    })
                })
            {
                return Err(SnapshotWireError);
            }
        }
        let digest: [u8; 32] = r.bytes()?.try_into().map_err(|_| SnapshotWireError)?;
        r.finish()?;
        let image = Self {
            genesis_baseline: false,
            origin: None,
            start_allowed: false,
            request_id,
            snapshot_seq,
            cluster,
            shard,
            now,
            primary_epoch,
            routes: Arc::new(config.routes.clone()),
            markets,
            managed_markets,
            registry,
            user_epochs,
            dedup,
            place,
        };
        if image.state_digest(config.max_bytes)? != digest {
            return Err(SnapshotWireError);
        }
        Ok(image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_sequence_snapshot_rejects_older_wal_epoch() {
        use matching_domain::{
            ApplyUserControlFenceShard, AssetId, Command, CommandEnvelope, CommandId, MarketMath,
            UserFenceAction, UserId,
        };
        use matching_risk::{LimitReservePolicy, RiskKeySet};
        let path = std::env::temp_dir().join(format!(
            "tide-t28-epoch-{}-{:?}.lease",
            std::process::id(),
            std::thread::current().id()
        ));
        let store = crate::ControlStore::new(&path, ClusterId(2), ShardId(3));
        drop(store.acquire_primary().unwrap());
        drop(store.acquire_primary().unwrap());
        let lease = Arc::new(store.acquire_primary().unwrap());
        let market = MarketId(5);
        let routes = RouteTable::new([crate::MarketRoute {
            market_id: market,
            shard_id: ShardId(3),
            route_epoch: 1,
            state: crate::RouteState::Active,
        }])
        .unwrap();
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let place = crate::PlaceSupportConfig::new(
            RiskKeySet::new(1, []).unwrap(),
            [(market, policy)],
            crate::PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap()
        .with_primary_lease(lease.clone());
        let trusted = SnapshotRestoreConfig {
            cluster: ClusterId(2),
            shard: ShardId(3),
            primary_epoch: 3,
            routes,
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let mut candidate = super::super::empty_recovery_candidate(
            &trusted,
            &[(
                market,
                matching_orderbook::QueuePriorityPolicy::try_new(1).unwrap(),
            )],
            1,
        )
        .unwrap();
        candidate.image.primary_epoch = 2;
        let bytes = candidate.image.encode_snapshot(trusted.max_bytes).unwrap();
        candidate.image = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
        let cmd = CommandEnvelope {
            command_id: CommandId(1),
            session_id: 1,
            client_seq: 1,
            market_id: None,
            user_id: Some(UserId(1)),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(1),
                user_id: UserId(1),
                new_epoch: 1,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: 1,
            }),
        };
        let result = super::super::replay_snapshot_records(
            candidate,
            vec![crate::RecoveredCommand {
                shard_seq: 1,
                epoch: 1,
                engine_time: 0,
                command: cmd,
            }],
        );
        assert!(result.is_err());
        drop(trusted);
        drop(lease);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn shard_kill_state_is_in_snapshot_and_digest_with_or_without_fee_book() {
        use matching_domain::{
            AssetId, FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier, MarketMath,
        };
        use matching_risk::{LimitReservePolicy, RiskKeySet};
        let market = MarketId(5);
        let routes = RouteTable::new([crate::MarketRoute {
            market_id: market,
            shard_id: ShardId(3),
            route_epoch: 1,
            state: crate::RouteState::Active,
        }])
        .unwrap();
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let place = crate::PlaceSupportConfig::new(
            RiskKeySet::new(1, []).unwrap(),
            [(market, policy)],
            crate::PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap();
        let trusted = SnapshotRestoreConfig {
            cluster: ClusterId(2),
            shard: ShardId(3),
            primary_epoch: 7,
            routes,
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        for fee in [false, true] {
            let mut image = super::super::empty_recovery_candidate(
                &trusted,
                &[(
                    market,
                    matching_orderbook::QueuePriorityPolicy::try_new(1).unwrap(),
                )],
                0,
            )
            .unwrap()
            .image;
            if fee {
                image.place.as_mut().unwrap().fee_book = Some(FeeScheduleBook::new(
                    FeeSchedule::new(
                        1,
                        FeeAssetPolicy::Quote,
                        [(
                            1,
                            FeeTier {
                                maker_bps: 0,
                                taker_bps: 0,
                                rounding: FeeRounding::Floor,
                            },
                        )],
                    )
                    .unwrap(),
                ));
            }
            let inactive_digest = image.state_digest(trusted.max_bytes).unwrap();
            let inactive = image.encode_snapshot(trusted.max_bytes).unwrap();
            assert_eq!(
                u16::from_be_bytes([inactive[0], inactive[1]]),
                if fee { 4 } else { 3 }
            );
            image.place.as_mut().unwrap().shard_kill_active = true;
            assert_ne!(
                image.state_digest(trusted.max_bytes).unwrap(),
                inactive_digest
            );
            let active = image.encode_snapshot(trusted.max_bytes).unwrap();
            assert_eq!(
                u16::from_be_bytes([active[0], active[1]]),
                if fee { 6 } else { 5 }
            );
            let restored = SnapshotImage::decode_snapshot(&active, &trusted).unwrap();
            assert!(restored.place.as_ref().unwrap().shard_kill_active);
            assert_eq!(
                restored.state_digest(trusted.max_bytes).unwrap(),
                image.state_digest(trusted.max_bytes).unwrap()
            );
        }
    }

    /// 市场配置与生命周期均属于快照和摘要的一部分，旧空状态仍可独立恢复。
    #[test]
    fn managed_market_snapshot_preserves_state_and_changes_digest() {
        use matching_domain::{
            ClosePolicy, ExecutionLimits, MarketConfig, OrderLimits, Qty, QuoteAmount,
        };
        let empty = SnapshotRestoreConfig {
            cluster: ClusterId(2),
            shard: ShardId(3),
            primary_epoch: 7,
            routes: RouteTable::new([]).unwrap(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: None,
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let mut image = super::super::empty_recovery_candidate(&empty, &[], 0)
            .unwrap()
            .image;
        let market_id = MarketId(5);
        let routes = RouteTable::new([crate::MarketRoute {
            market_id,
            shard_id: ShardId(3),
            route_epoch: 1,
            state: crate::RouteState::Active,
        }])
        .unwrap();
        image.routes = Arc::new(routes.clone());
        let mut book =
            ProductionOrderBook::new(matching_orderbook::QueuePriorityPolicy::try_new(1).unwrap());
        book.bind_market(market_id).unwrap();
        image.markets.insert(market_id, book);
        let config = MarketConfig {
            market_id,
            price_scale: 0,
            qty_scale: 0,
            quote_scale: 0,
            tick_size: 1,
            lot_size: 1,
            order_limits: OrderLimits {
                min_qty: Qty::try_new(1).unwrap(),
                max_qty: Qty::try_new(10).unwrap(),
                min_notional: QuoteAmount::new(1),
                max_notional: QuoteAmount::new(100),
            },
            price_band: None,
            active_fee_schedule_version: 1,
            close_policy: ClosePolicy::CancelAll,
            execution_limits: ExecutionLimits {
                max_internal_actions_per_command: 10,
                max_trades_per_command: 10,
                priority_exhaustion_guard: 1,
                synthetic_priority_ceiling: None,
            },
        };
        let mut managed = matching_domain::ManagedMarket::new(config).unwrap();
        managed.start_session(17, true).unwrap();
        let before = image.state_digest(empty.max_bytes).unwrap();
        image.managed_markets.insert(market_id, managed.clone());
        assert_ne!(before, image.state_digest(empty.max_bytes).unwrap());
        let state = image.managed_markets.get_mut(&market_id).unwrap();
        let mut altered = state.config().clone();
        altered.execution_limits.priority_exhaustion_guard = 2;
        state
            .update_config(
                altered,
                matching_domain::MarketConfigUpdateChecks {
                    has_active_orders: false,
                    has_referenced_history: false,
                    existing_orders_valid: true,
                    fee_version_available: true,
                },
            )
            .unwrap();
        assert!(image.state_digest(empty.max_bytes).is_err());
        image.managed_markets.insert(market_id, managed.clone());
        let bytes = image.encode_snapshot(empty.max_bytes).unwrap();
        let trusted = SnapshotRestoreConfig { routes, ..empty };
        let restored = SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap();
        assert_eq!(restored.managed_markets.get(&market_id), Some(&managed));
        assert_eq!(
            restored.state_digest(trusted.max_bytes).unwrap(),
            image.state_digest(trusted.max_bytes).unwrap()
        );
        let mut restored = restored;
        restored.start_allowed = true;
        assert!(Shard::start_in_memory_from_snapshot(restored, &trusted).is_err());
        let empty_book = image.markets[&market_id].clone();
        image
            .markets
            .get_mut(&market_id)
            .unwrap()
            .rest(matching_domain::LimitOrder {
                order_id: matching_domain::OrderId(1),
                user_id: matching_domain::UserId(1),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                side: matching_domain::Side::Buy,
                price: matching_domain::Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            })
            .unwrap();
        assert!(image.state_digest(trusted.max_bytes).is_err());
        image
            .markets
            .get_mut(&market_id)
            .unwrap()
            .cancel(matching_domain::OrderId(1))
            .unwrap();
        assert!(image.state_digest(trusted.max_bytes).is_err());
        image
            .managed_markets
            .get_mut(&market_id)
            .unwrap()
            .record_referenced_history();
        assert!(image.state_digest(trusted.max_bytes).is_ok());
        let state = image.managed_markets.get_mut(&market_id).unwrap();
        let mut band_config = state.config().clone();
        band_config.price_band = Some(matching_domain::PriceBandConfig {
            reference_kind: matching_domain::PriceReferenceKind::LastTrade,
            max_deviation_bps: 100,
        });
        state
            .update_config(
                band_config.clone(),
                matching_domain::MarketConfigUpdateChecks {
                    has_active_orders: false,
                    has_referenced_history: true,
                    existing_orders_valid: true,
                    fee_version_available: true,
                },
            )
            .unwrap();
        let book = image.markets.get_mut(&market_id).unwrap();
        book.rest(matching_domain::LimitOrder {
            order_id: matching_domain::OrderId(2),
            user_id: matching_domain::UserId(1),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            side: matching_domain::Side::Buy,
            price: matching_domain::Price::try_new(10).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        })
        .unwrap();
        book.set_price_band_config(Some((
            band_config.price_band.unwrap(),
            band_config.validate().unwrap(),
        )))
        .unwrap();
        assert!(image.state_digest(trusted.max_bytes).is_err());
        let book = image.markets.get_mut(&market_id).unwrap();
        book.set_price_band_config(None).unwrap();
        let math = band_config.validate().unwrap();
        book.trigger_stops(
            matching_domain::PriceReference {
                kind: matching_domain::PriceReferenceKind::LastTrade,
                source_seq: 1,
                price: matching_domain::Price::try_new(i64::MAX).unwrap(),
            },
            LogicalTime::new(0),
            17,
            &math,
            &[],
            |_| Ok(()),
        )
        .unwrap();
        book.set_price_band_config(Some((band_config.price_band.unwrap(), math)))
            .unwrap();
        assert!(image.state_digest(trusted.max_bytes).is_err());
        image.markets.insert(market_id, empty_book);
        let managed = image.managed_markets.remove(&market_id).unwrap();
        image.managed_markets.insert(MarketId(6), managed);
        assert!(image.encode_snapshot(trusted.max_bytes).is_err());
    }

    /// 空管理状态的新版镜像与既有版本摘要相容。
    #[test]
    fn version_two_snapshot_restores_with_empty_managed_state() {
        let config = SnapshotRestoreConfig {
            cluster: ClusterId(2),
            shard: ShardId(3),
            primary_epoch: 7,
            routes: RouteTable::new([]).unwrap(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: None,
            expect_registry: true,
            max_markets: 0,
            max_bytes: 1024 * 1024,
        };
        let image = super::super::empty_recovery_candidate(&config, &[], 0)
            .unwrap()
            .image;
        let mut bytes = image.encode_snapshot(config.max_bytes).unwrap();
        let count_start = bytes.len() - 40;
        assert_eq!(&bytes[count_start..count_start + 4], &[0, 0, 0, 0]);
        bytes.drain(count_start..count_start + 4);
        bytes[1] = 2;
        let restored = SnapshotImage::decode_snapshot(&bytes, &config).unwrap();
        assert!(restored.managed_markets.is_empty());
        assert_eq!(
            restored.state_digest(config.max_bytes).unwrap(),
            image.state_digest(config.max_bytes).unwrap()
        );
    }
    /// 检查软件 SHA-256 后端的标准输入与固定摘要。
    #[test]
    fn sha256_matches_standard_abc_vector() {
        use sha2::{Digest, Sha256};
        let expected: [u8; 32] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        assert_eq!(<[u8; 32]>::from(Sha256::digest(b"abc")), expected);
    }
}
