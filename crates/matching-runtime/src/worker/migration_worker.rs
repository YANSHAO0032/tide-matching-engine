//! 停写期间的分片快照捕获、持久安装和所有权检查。
use super::*;
use crate::{MarketRoute, MigrationId};
use matching_persistence::DurableAppender;
use std::{io, path::PathBuf};

/// 普通快照和受迁移身份约束的快照共用同一捕获顺序点。
pub(super) enum SnapshotReply {
    /// 普通调用者接收镜像或容量错误。
    Ordinary(SyncSender<Result<SnapshotImage, SnapshotError>>),
    /// 按迁移身份获取冻结镜像，结果通过第二个参数指定的通道返回。
    Migration(MigrationId, SyncSender<io::Result<SnapshotImage>>),
}
impl SnapshotReply {
    pub(super) fn reject_migration(self) {
        if let Self::Migration(_, reply) = self {
            let _ = reply.send(Err(io::Error::other("迁移身份、持久前缀或停写状态不一致")));
        }
    }
    pub(super) fn send(self, image: Result<SnapshotImage, SnapshotError>) {
        match self {
            Self::Ordinary(reply) => {
                let _ = reply.send(image);
            }
            Self::Migration(_, reply) => {
                let _ = reply.send(image.map_err(|_| io::Error::other("迁移切面捕获失败")));
            }
        }
    }
}

/// 只允许已登记迁移的源和目标分片捕获或安装迁移状态。
pub(super) fn matches_freeze(
    id: MigrationId,
    cluster: ClusterId,
    shard: ShardId,
    routes: &RouteTable,
) -> bool {
    let Ok(epoch) = id.new_epoch() else {
        return false;
    };
    id.cluster == cluster
        && (shard == id.source || shard == id.target)
        && routes.get(id.market)
            == Some(&MarketRoute {
                market_id: id.market,
                shard_id: id.source,
                route_epoch: epoch,
                state: RouteState::Migrating {
                    source: id.source,
                    target: id.target,
                },
            })
}

pub(super) fn frozen_reason(
    routes: &RouteTable,
    shard: ShardId,
    market: Option<MarketId>,
    epoch: Option<u64>,
) -> RejectReason {
    if let Some((market, epoch)) = market.zip(epoch)
        && routes
            .get(market)
            .is_some_and(|r| matches!(r.state, RouteState::Migrating { .. }))
    {
        return routes.validate(market, shard, epoch).unwrap_err();
    }
    RejectReason::EngineBusy
}

pub(super) fn final_routes(id: MigrationId, routes: &RouteTable) -> io::Result<RouteTable> {
    let epoch = id.new_epoch()?;
    RouteTable::new(routes.routes().map(|route| {
        if route.market_id == id.market {
            MarketRoute {
                market_id: id.market,
                shard_id: id.target,
                route_epoch: epoch,
                state: RouteState::Active,
            }
        } else {
            *route
        }
    }))
    .map_err(|_| io::Error::other("迁移后路由重复"))
}

/// 分片仅在处于暂停顺序点且市场成员与目标路由一致时承认所有权切换。
pub(super) fn routes_installable(
    shard: ShardId,
    paused: bool,
    old: &RouteTable,
    new: &RouteTable,
    markets: &BTreeMap<MarketId, ProductionOrderBook>,
) -> bool {
    let valid = new.routes().all(|next| {
        let holds_book = markets.contains_key(&next.market_id);
        if next.state == RouteState::Active && next.shard_id == shard && !holds_book {
            return false;
        }
        let Some(prior) = old.get(next.market_id) else {
            return next.shard_id != shard
                && !matches!(next.state, RouteState::Migrating { source, target } if shard == source || shard == target);
        };
        if prior == next {
            return true;
        }
        match (prior.state, next.state) {
            (RouteState::Active, RouteState::Active) => {
                prior.shard_id == next.shard_id && prior.route_epoch <= next.route_epoch
            }
            (RouteState::Active, RouteState::Migrating { source, target }) => {
                source != target
                    && prior.shard_id == source
                    && next.shard_id == source
                    && prior.route_epoch.checked_add(1) == Some(next.route_epoch)
                    && (paused || (shard != source && shard != target))
            }
            (RouteState::Migrating { source, target }, RouteState::Active) => {
                let participates = shard == source || shard == target;
                prior.shard_id == source
                    && next.shard_id == target
                    && prior.route_epoch == next.route_epoch
                    && (!participates || (paused && holds_book == (shard == target)))
            }
            _ => false,
        }
    });
    valid
        && old
            .routes()
            .all(|prior| prior.shard_id != shard || new.get(prior.market_id).is_some())
}

/// 已由协调器从受保护迁移归档取出的单分片镜像。
pub(super) struct MigrationInstall {
    /// 权威控制面中的迁移身份。
    pub(super) migration: MigrationId,
    /// 包含来源序号、身份与摘要的分片快照。
    payload: Arc<[u8]>,
    /// 持久恢复时对应的受保护快照目录。
    directory: PathBuf,
    /// 本次安装的唯一文件身份。
    snapshot_id: u64,
    /// 单镜像解码的最大长度。
    pub(super) limit: usize,
    /// 镜像进入清单并安装后回复。
    pub(super) reply: SyncSender<io::Result<()>>,
}

impl MigrationInstall {
    pub(super) fn install(
        &self,
        config: &SnapshotRestoreConfig,
        seq: u64,
        now: LogicalTime,
        gate: &mut LocalDurableCommit,
        already_installed: bool,
    ) -> io::Result<SnapshotImage> {
        if self.payload.len() > self.limit
            || !gate.batcher.supports_local_migration()
            || gate.batcher.committed() != seq
        {
            return Err(io::Error::other("迁移镜像超过边界或缺少完整本地提交前缀"));
        }
        let image =
            SnapshotImage::decode_migration_snapshot(&self.payload, config, self.migration.market)
                .map_err(|_| io::Error::other("迁移镜像解码或可信策略校验失败"))?;
        if image.snapshot_seq != seq
            || image.now != now
            || image.replay_route_floor.as_deref() != Some(&config.routes)
            || image.markets.contains_key(&self.migration.market)
                != (self.migration.target == config.shard)
        {
            return Err(io::Error::other("迁移镜像不属于当前分片切面"));
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join("snapshot.lock"))?;
        lock.try_lock()
            .map_err(|e| io::Error::other(format!("快照目录正在使用: {e}")))?;
        if already_installed
            && (!gate
                .batcher
                .appender()
                .is_migration_snapshot(self.snapshot_id, seq)
                || !self
                    .directory
                    .join(format!("snapshot-{}.bin", self.snapshot_id))
                    .try_exists()?)
        {
            return Err(io::Error::other("已安装的迁移快照缺失或身份不匹配"));
        }
        matching_persistence::write_migration_snapshot_file(
            &self.directory,
            self.snapshot_id,
            seq,
            &self.payload,
        )?;
        gate.batcher
            .commit_migration_snapshot(self.snapshot_id, seq)?;
        Ok(image)
    }
}

impl ShardHandle {
    /// 在改写控制权威前确认当前分片具有本地持久迁移能力。
    pub(crate) fn supports_local_migration(&self) -> bool {
        self.local_migration
    }

    /// 在同一有界队列的迁移停写点取得完整一致切面。
    pub(crate) fn migration_snapshot(
        &self,
        id: MigrationId,
    ) -> Result<Receiver<io::Result<SnapshotImage>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(
            Message::Snapshot(
                u128::from(id.market.get()),
                SnapshotReply::Migration(id, tx),
            ),
            IngressClass::Control,
        )?;
        Ok(rx)
    }
    /// 向停写的分片投递安装请求。工作线程先持久化镜像并登记清单，再更新内存并回复；协调器随后切换路由。
    pub(crate) fn install_migration_image(
        &self,
        id: MigrationId,
        payload: Arc<[u8]>,
        directory: PathBuf,
        snapshot_id: u64,
        limit: usize,
    ) -> Result<Receiver<io::Result<()>>, ShardError> {
        let (tx, rx) = sync_channel(1);
        self.push(
            Message::InstallMigration(Box::new(MigrationInstall {
                migration: id,
                payload,
                directory,
                snapshot_id,
                limit,
                reply: tx,
            })),
            IngressClass::Control,
        )?;
        Ok(rx)
    }
}
