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

/// 解码历史路由以验证旧快照摘要；当前所有权仍由外部路由决定。
fn read_routes(bytes: &[u8], limit: usize) -> Result<RouteTable, SnapshotWireError> {
    let mut r = SnapshotDecoder::new(bytes, limit)?;
    let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
    if count > r.remaining() / 15 {
        return Err(SnapshotWireError);
    }
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(count)
        .map_err(|_| SnapshotWireError)?;
    let mut previous = None;
    for _ in 0..count {
        let market_id = MarketId::new(r.u32()?);
        if previous.is_some_and(|id| id >= market_id) {
            return Err(SnapshotWireError);
        }
        previous = Some(market_id);
        let shard_id = ShardId::new(r.u16()?);
        let route_epoch = r.u64()?;
        let state = match r.u8()? {
            0 => RouteState::Active,
            1 => RouteState::Migrating {
                source: ShardId::new(r.u16()?),
                target: ShardId::new(r.u16()?),
            },
            _ => return Err(SnapshotWireError),
        };
        routes.push(crate::MarketRoute {
            market_id,
            shard_id,
            route_epoch,
            state,
        });
    }
    r.finish()?;
    RouteTable::new(routes).map_err(|_| SnapshotWireError)
}

/// 判断路由是否涉及本分片，包括迁移中的源和目标。
fn involves_shard(route: &crate::MarketRoute, shard: ShardId) -> bool {
    route.shard_id == shard
        || matches!(route.state, RouteState::Migrating { source, target } if source == shard || target == shard)
}

/// 核对本分片涉及的历史路由与权威路由。活动所有者可推进纪元；冻结期间兼容相同冻结路由、
/// 冻结前指向源分片的活动路由，以及已持久安装的切换后目标路由。
fn compatible_routes(old: &RouteTable, current: &RouteTable, shard: ShardId) -> bool {
    fn compatible(prior: &crate::MarketRoute, next: &crate::MarketRoute) -> bool {
        if prior == next {
            return true;
        }
        if prior.state != RouteState::Active {
            return false;
        }
        match next.state {
            RouteState::Active => {
                prior.shard_id == next.shard_id && prior.route_epoch <= next.route_epoch
            }
            RouteState::Migrating { source, target } => {
                next.shard_id == source
                    && source != target
                    && ((prior.shard_id == source && prior.route_epoch < next.route_epoch)
                        || (prior.shard_id == target && prior.route_epoch == next.route_epoch))
            }
        }
    }

    old.routes()
        .filter(|route| involves_shard(route, shard))
        .all(|route| {
            current
                .get(route.market_id)
                .is_some_and(|next| compatible(route, next))
        })
        && current
            .routes()
            .filter(|route| involves_shard(route, shard))
            .all(|route| {
                old.get(route.market_id)
                    .is_some_and(|prior| compatible(prior, route))
            })
}
/// 写入分片本地信任的公钥和容量，迁移不会改变这些配置。
fn write_place_header(
    config: &PlaceSupportConfig,
    w: &mut SnapshotEncoder,
) -> Result<(), SnapshotWireError> {
    config.keys.write_snapshot(w)?;
    for value in [
        config.limits.active_orders,
        config.limits.terminal_history,
        config.limits.client_id_bytes,
    ] {
        w.u64(u64::try_from(value).map_err(|_| SnapshotWireError)?)?;
    }
    Ok(())
}

/// 从受保护的迁移交接或其已登记快照恢复新加入市场的预留策略。
/// 其余策略、容量和密钥继续通过完整配置的规范字节逐项核对。
fn restore_migrating_policy(
    config: &mut PlaceSupportConfig,
    bytes: &[u8],
    market: MarketId,
    owned: bool,
    limit: usize,
) -> Result<(), SnapshotWireError> {
    if !owned {
        config.policies.remove(&market);
        return Ok(());
    }
    if config.policies.contains_key(&market) {
        return Ok(());
    }
    let mut header = SnapshotEncoder::new(limit);
    write_place_header(config, &mut header)?;
    let header = header.finish();
    let mut r = SnapshotDecoder::new(
        bytes
            .strip_prefix(header.as_slice())
            .ok_or(SnapshotWireError)?,
        limit,
    )?;
    let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
    if config.policies.len().checked_add(1) != Some(count) || count > r.remaining() / 8 {
        return Err(SnapshotWireError);
    }
    for _ in 0..count {
        let id = MarketId(r.u32()?);
        let policy = matching_risk::LimitReservePolicy::read_snapshot(&mut r)?;
        if id == market {
            config.policies.insert(market, policy);
            return Ok(());
        }
    }
    Err(SnapshotWireError)
}

/// 将完整可信下单配置编码，以便恢复时逐字节核验。
fn place_bytes(config: &PlaceSupportConfig, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
    let mut w = SnapshotEncoder::new(limit);
    write_place_header(config, &mut w)?;
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
    /// 按业务顺序编码分片状态以计算摘要。幂等窗口保留命令和会话身份，快照请求号与物理槽位不参与摘要。
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
        if let Some(data) = self
            .place
            .as_ref()
            .and_then(|place| place.market_data.as_ref())
        {
            data.validate_books(&self.markets)
                .map_err(|_| SnapshotWireError)?;
            if data.last_applied_shard_seq() != self.snapshot_seq {
                return Err(SnapshotWireError);
            }
            let watermarks = data.market_watermarks();
            if watermarks.len() != self.markets.len() || watermarks.keys().ne(self.markets.keys()) {
                return Err(SnapshotWireError);
            }
            w.u8(4)?;
            w.u32(u32::try_from(watermarks.len()).map_err(|_| SnapshotWireError)?)?;
            for (market, seq) in watermarks {
                w.u32(market.0)?;
                w.u64(seq)?;
            }
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
        Self::read_recovery_candidate_inner(directory, manager, config, false)
    }
    /// 供已委托外围验证快照和写权的复制恢复路径读取历史纪元。
    pub(crate) fn read_replicated_recovery_candidate(
        directory: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &SnapshotRestoreConfig,
    ) -> std::io::Result<Option<SnapshotRecoveryCandidate>> {
        Self::read_recovery_candidate_inner(directory, manager, config, true)
    }
    fn read_recovery_candidate_inner(
        directory: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &SnapshotRestoreConfig,
        replicated: bool,
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
        match Self::read_committed_snapshot_inner(directory, manager, config, replicated) {
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
                let image =
                    Self::decode_snapshot_inner(&bytes, config, replicated).map_err(|_| {
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
        Self::read_committed_snapshot_inner(directory, manager, config, false)
    }
    fn read_committed_snapshot_inner(
        directory: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &SnapshotRestoreConfig,
        replicated: bool,
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
        let mut image = Self::decode_snapshot_inner(&bytes, config, replicated)
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
        let market_data = self
            .place
            .as_ref()
            .and_then(|place| place.market_data.as_ref());
        w.u16(
            match (market_data.is_some(), fee_book.is_some(), shard_kill_active) {
                (false, false, false) => 3,
                (false, true, false) => 4,
                (false, false, true) => 5,
                (false, true, true) => 6,
                (true, false, false) => 7,
                (true, true, false) => 8,
                (true, false, true) => 9,
                (true, true, true) => 10,
            },
        )?;
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
        if let Some(data) = market_data {
            let watermarks = data.market_watermarks();
            w.u32(u32::try_from(watermarks.len()).map_err(|_| SnapshotWireError)?)?;
            for (market, seq) in watermarks {
                w.u32(market.0)?;
                w.u64(seq)?;
            }
            w.bytes(
                &data
                    .encode_checkpoint(limit)
                    .map_err(|_| SnapshotWireError)?,
            )?;
        }
        w.bytes(&self.state_digest(limit)?)?;
        Ok(w.finish())
    }
    /// 使用外部可信身份、容量与配置恢复镜像并检查跨组件约束。
    pub fn decode_snapshot(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
    ) -> Result<Self, SnapshotWireError> {
        Self::decode_snapshot_inner(bytes, config, false)
    }
    fn decode_snapshot_inner(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
        replicated: bool,
    ) -> Result<Self, SnapshotWireError> {
        let migrating = config.routes.routes().find_map(|r| match r.state {
            RouteState::Migrating { source, target }
                if source == config.shard || target == config.shard =>
            {
                Some(r.market_id)
            }
            _ => None,
        });
        Self::decode_with_migration(bytes, config, replicated, migrating)
    }

    /// 在已冻结的分片上验证交接档案中的切换后镜像。
    pub(super) fn decode_migration_snapshot(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
        market: MarketId,
    ) -> Result<Self, SnapshotWireError> {
        Self::decode_with_migration(bytes, config, false, Some(market))
    }

    fn decode_with_migration(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
        replicated: bool,
        migrating: Option<MarketId>,
    ) -> Result<Self, SnapshotWireError> {
        let mut r = SnapshotDecoder::new(bytes, config.max_bytes)?;
        let version = r.u16()?;
        if !(2..=10).contains(&version) {
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
        let historical_epoch_allowed =
            (replicated && primary_epoch > 0 && primary_epoch <= config.primary_epoch)
                || config
                    .place
                    .as_ref()
                    .and_then(|place| place.primary_lease.as_ref())
                    .is_some_and(|lease| {
                        primary_epoch > 0
                            && primary_epoch <= config.primary_epoch
                            && lease.verify(cluster, shard, config.primary_epoch).is_ok()
                    });
        let historical_routes = read_routes(r.bytes()?, config.max_bytes)?;
        if cluster != config.cluster
            || shard != config.shard
            || (primary_epoch != config.primary_epoch && !historical_epoch_allowed)
            || !compatible_routes(&historical_routes, &config.routes, shard)
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
                || historical_routes
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
        if historical_routes.routes().any(|route| {
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
                    market_data: None,
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
        if matches!(version, 4 | 6 | 8 | 10) {
            let state = place.as_mut().ok_or(SnapshotWireError)?;
            state.fee_book = Some(matching_domain::FeeScheduleBook::read_snapshot(
                &mut r,
                config.max_bytes / 13,
                config.max_bytes / 21,
            )?);
        }
        if matches!(version, 5 | 6 | 9 | 10) {
            place.as_mut().ok_or(SnapshotWireError)?.shard_kill_active = true;
        }
        if version >= 7 {
            let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
            if count != markets.len() || count > config.max_markets || count > r.remaining() / 12 {
                return Err(SnapshotWireError);
            }
            let mut watermarks = BTreeMap::new();
            for _ in 0..count {
                let market = MarketId(r.u32()?);
                let seq = r.u64()?;
                if watermarks.insert(market, seq).is_some() {
                    return Err(SnapshotWireError);
                }
            }
            if watermarks.keys().ne(markets.keys()) {
                return Err(SnapshotWireError);
            }
            let bytes = r.bytes()?;
            let data = crate::MarketDataState::decode_checkpoint(
                bytes,
                config.max_bytes,
                snapshot_seq,
                &watermarks,
            )
            .map_err(|_| SnapshotWireError)?;
            if data.last_applied_shard_seq() != snapshot_seq
                || data.market_watermarks() != watermarks
            {
                return Err(SnapshotWireError);
            }
            place.as_mut().ok_or(SnapshotWireError)?.market_data = Some(data);
        }
        if let Some(place) = &mut place {
            if let Some(market) = migrating {
                restore_migrating_policy(
                    &mut place.config,
                    encoded_place.as_deref().ok_or(SnapshotWireError)?,
                    market,
                    markets.contains_key(&market),
                    config.max_bytes,
                )?;
            }
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
        let mut image = Self {
            replay_route_floor: Some(Arc::new(historical_routes.clone())),
            genesis_baseline: false,
            origin: None,
            start_allowed: false,
            request_id,
            snapshot_seq,
            cluster,
            shard,
            now,
            primary_epoch,
            routes: Arc::new(historical_routes),
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
        image.routes = Arc::new(config.routes.clone());
        Ok(image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_recovery_requires_either_the_original_owner_or_the_exact_staged_epoch() {
        let market = MarketId(87);
        let source = ShardId(3);
        let target = ShardId(4);
        let table = |shard, route_epoch, state| {
            RouteTable::new([crate::MarketRoute {
                market_id: market,
                shard_id: shard,
                route_epoch,
                state,
            }])
            .unwrap()
        };
        let frozen = table(source, 4, RouteState::Migrating { source, target });
        let before = table(source, 3, RouteState::Active);
        let bootstrapped = table(source, 2, RouteState::Active);
        let staged = table(target, 4, RouteState::Active);
        for shard in [source, target] {
            for valid in [&before, &bootstrapped, &staged] {
                assert!(compatible_routes(valid, &frozen, shard));
            }
            for invalid in [
                table(source, 4, RouteState::Active),
                table(target, 2, RouteState::Active),
                table(ShardId(8), 3, RouteState::Active),
            ] {
                assert!(!compatible_routes(&invalid, &frozen, shard));
            }
            assert!(!compatible_routes(&before, &staged, shard));
        }
    }
    #[test]
    fn replay_respects_snapshot_route_floor_and_current_ceiling() {
        use matching_domain::{
            AssetId, Command, CommandEnvelope, CommandId, MarketMath, MassCancelShard,
            ShardCancelScope,
        };
        use matching_risk::{LimitReservePolicy, RiskKeySet};
        let market = MarketId(5);
        let routes = |epoch| {
            RouteTable::new([crate::MarketRoute {
                market_id: market,
                shard_id: ShardId(3),
                route_epoch: epoch,
                state: RouteState::Active,
            }])
            .unwrap()
        };
        let policy = LimitReservePolicy::new(
            (AssetId(1), 0),
            (AssetId(2), 0),
            MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            1,
            1,
            0,
        )
        .unwrap();
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
        let mut trusted = SnapshotRestoreConfig {
            cluster: ClusterId(2),
            shard: ShardId(3),
            primary_epoch: 1,
            routes: routes(1),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let image = super::super::empty_recovery_candidate(
            &trusted,
            &[(
                market,
                matching_orderbook::QueuePriorityPolicy::try_new(1).unwrap(),
            )],
            0,
        )
        .unwrap()
        .image;
        let encoded = image.encode_snapshot(trusted.max_bytes).unwrap();
        trusted.routes = routes(2);
        for (epoch, accepted) in [(0, false), (1, true), (2, true), (3, false)] {
            let image = SnapshotImage::decode_snapshot(&encoded, &trusted).unwrap();
            let candidate = SnapshotRecoveryCandidate {
                image,
                committed_through: 1,
                used_previous: false,
            };
            let command = CommandEnvelope {
                command_id: CommandId(1),
                session_id: 1,
                client_seq: 1,
                market_id: Some(market),
                user_id: None,
                route_epoch: Some(epoch),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::MassCancelShard(MassCancelShard {
                    scope: ShardCancelScope::Market { market_id: market },
                }),
            };
            let record = crate::RecoveredCommand {
                shard_seq: 1,
                epoch: 1,
                engine_time: 0,
                command,
            };
            assert_eq!(
                super::super::replay_snapshot_records(candidate, vec![record]).is_ok(),
                accepted,
                "路由版本 {epoch}"
            );
        }
    }

    #[test]
    fn snapshot_recovery_accepts_unrelated_shard_route_change() {
        let mut config = SnapshotRestoreConfig {
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
        let image = super::super::empty_recovery_candidate(&config, &[], 0)
            .unwrap()
            .image;
        let bytes = image.encode_snapshot(config.max_bytes).unwrap();
        config.routes = RouteTable::new([crate::MarketRoute {
            market_id: MarketId(9),
            shard_id: ShardId(4),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let recovered = SnapshotImage::decode_snapshot(&bytes, &config).unwrap();
        assert_eq!(recovered.routes.as_ref(), &config.routes);
        config.routes = RouteTable::new([crate::MarketRoute {
            market_id: MarketId(9),
            shard_id: ShardId(3),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        assert!(SnapshotImage::decode_snapshot(&bytes, &config).is_err());
    }

    #[test]
    fn historical_routes_reject_truncation_and_noncanonical_order() {
        let route = |id| crate::MarketRoute {
            market_id: MarketId(id),
            shard_id: ShardId(4),
            route_epoch: 1,
            state: RouteState::Active,
        };
        let table = RouteTable::new([route(1), route(2)]).unwrap();
        let mut bytes = route_bytes(&table, 1024).unwrap();
        assert_eq!(read_routes(&bytes, 1024).unwrap(), table);
        assert!(read_routes(&bytes[..bytes.len() - 1], 1024).is_err());
        bytes[4..8].copy_from_slice(&3_u32.to_be_bytes());
        assert!(read_routes(&bytes, 1024).is_err());
    }

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
            // 本测试固定旧格式的开关兼容性；新行情快照另有独立测试。
            image.place.as_mut().unwrap().market_data = None;
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
            let mut newer = trusted.clone();
            newer.routes = RouteTable::new([crate::MarketRoute {
                market_id: market,
                shard_id: ShardId(3),
                route_epoch: 2,
                state: RouteState::Active,
            }])
            .unwrap();
            let restored_with_newer = SnapshotImage::decode_snapshot(&inactive, &newer).unwrap();
            assert_eq!(restored_with_newer.routes.as_ref(), &newer.routes);
            let newer_bytes = restored_with_newer
                .encode_snapshot(newer.max_bytes)
                .unwrap();
            assert!(SnapshotImage::decode_snapshot(&newer_bytes, &trusted).is_err());
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
