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
    Ok(w.finish())
}

impl SnapshotImage {
    /// 计算当前可用业务状态的 SHA-256 摘要。
    pub fn state_digest(&self, limit: usize) -> Result<[u8; 32], SnapshotWireError> {
        use sha2::{Digest, Sha256};
        Ok(Sha256::digest(self.canonical_state(limit)?).into())
    }
    /// 按业务状态顺序编码快照，供后续状态摘要计算；不包含请求身份和物理槽位。
    pub fn canonical_state(&self, limit: usize) -> Result<Vec<u8>, SnapshotWireError> {
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
        w.u16(2)?;
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
        w.bytes(&self.state_digest(limit)?)?;
        Ok(w.finish())
    }
    /// 使用外部可信身份、容量与配置恢复镜像并检查跨组件约束。
    pub fn decode_snapshot(
        bytes: &[u8],
        config: &SnapshotRestoreConfig,
    ) -> Result<Self, SnapshotWireError> {
        let mut r = SnapshotDecoder::new(bytes, config.max_bytes)?;
        if r.u16()? != 2 {
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
        if cluster != config.cluster
            || shard != config.shard
            || primary_epoch != config.primary_epoch
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
        let place = match r.u8()? {
            0 => {
                if config.place.is_some() {
                    return Err(SnapshotWireError);
                }
                None
            }
            1 => {
                let expected = config.place.as_ref().ok_or(SnapshotWireError)?;
                if r.bytes()? != place_bytes(expected, config.max_bytes)? {
                    return Err(SnapshotWireError);
                }
                let limits = expected.limits;
                let ids = ClientOrderIds::read_snapshot(
                    &mut r,
                    limits.active_orders,
                    limits.terminal_history,
                    limits.client_id_bytes,
                )?;
                if registry.is_none()
                    || expected.policies.iter().any(|(market, policy)| {
                        markets.get(market).is_none_or(|book| {
                            book.price_band_config()
                                .is_some_and(|(_, math)| math != policy.market_math())
                        })
                    })
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
                })
            }
            _ => return Err(SnapshotWireError),
        };
        let digest: [u8; 32] = r.bytes()?.try_into().map_err(|_| SnapshotWireError)?;
        r.finish()?;
        let image = Self {
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
