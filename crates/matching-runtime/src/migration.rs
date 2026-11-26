//! 在持久路由切换前转移两个分片的停市切面和同机行情检查点。

use crate::route_store::MigrationRoutes;
use crate::{
    FeedBuilder, MarketMigrationBoundary, MarketRoute, RouteAuthority, RouteCoordinator,
    RouteMonitor, RouteState, RouteTable, ShardError, ShardHandle, SnapshotImage,
};
use matching_domain::{
    ClusterId, MarketId, ShardId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder, SnapshotWireError},
};
use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, mpsc::Receiver},
    time::{Duration, Instant},
};

/// 唯一标识一次市场迁移；相同身份可用于中断后续办。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationId {
    /// 持有权威路由的集群。
    pub cluster: ClusterId,
    /// 正在转移的市场。
    pub market: MarketId,
    /// 切换前的所有者。
    pub source: ShardId,
    /// 切换后的所有者。
    pub target: ShardId,
    /// 切换前活动路由的纪元。
    pub old_epoch: u64,
}

impl MigrationId {
    /// 校验不同的所有者并计算目标路由纪元。
    pub fn new_epoch(self) -> io::Result<u64> {
        if self.source == self.target {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "市场迁移要求不同分片",
            ));
        }
        self.old_epoch
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "路由纪元已耗尽"))
    }
}

/// 受信管理端指定的持久目录及单次操作边界。
pub struct MigrationStorage {
    /// 跨协调器重启保留的专用迁移目录。
    pub archive: PathBuf,
    /// 源分片恢复时读取的快照目录及此次专用快照身份。
    pub source_snapshot: (PathBuf, u64),
    /// 目标分片恢复时读取的快照目录及此次专用快照身份。
    pub target_snapshot: (PathBuf, u64),
    /// 两份撮合镜像、行情检查点及封装的总字节上限。
    pub max_bytes: usize,
    /// 等待路由与分片队列回复的总期限。
    pub timeout: Duration,
}

/// 管理端切换确认及目标行情进程开始消费后缀所需的检查点。
pub struct MigrationResult {
    /// 管理行情边界，重试同一次迁移返回同一边界。
    pub boundary: MarketMigrationBoundary,
    /// 目标行情构建器从下一条已提交市场源继续。
    pub builder: FeedBuilder,
    /// 包含优先级和订单簿的迁移市场摘要。
    pub market_digest: [u8; 32],
}

/// 首次安装前同步到可信目录的不可变切面，重试时以其中的原始字节为准。
struct Prepared {
    /// 源分片撤出市场后的完整切面。
    source: Arc<[u8]>,
    /// 目标分片接收市场后的完整切面。
    target: Arc<[u8]>,
    /// 两侧已核对相等的市场摘要。
    digest: [u8; 32],
    /// 对应源屏障的完整行情构建器。
    builder: FeedBuilder,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl MigrationId {
    fn active_target(self) -> io::Result<MarketRoute> {
        Ok(MarketRoute {
            market_id: self.market,
            shard_id: self.target,
            route_epoch: self.new_epoch()?,
            state: RouteState::Active,
        })
    }
}

/// 只重试尚未入队的控制请求。回复超时后由同一迁移身份核对持久结果。
fn at_barrier<T>(
    deadline: Instant,
    mut submit: impl FnMut() -> Result<Receiver<T>, ShardError>,
) -> io::Result<T> {
    loop {
        let wait = remaining(deadline)?;
        match submit() {
            Ok(rx) => {
                return rx
                    .recv_timeout(wait)
                    .map_err(|e| io::Error::other(format!("迁移屏障结果未知: {e}")));
            }
            Err(ShardError::Busy) => std::thread::yield_now(),
            Err(err) => return Err(io::Error::other(format!("迁移控制请求失败: {err:?}"))),
        }
    }
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "迁移控制等待超时"))
}

impl Prepared {
    fn capture(
        id: MigrationId,
        mut source: SnapshotImage,
        mut target: SnapshotImage,
        feed: &FeedBuilder,
        limit: usize,
    ) -> io::Result<Self> {
        if feed.market_id() != id.market
            || source.market_data_builder(id.market).as_ref() != Some(feed.mbo_view())
        {
            return Err(invalid("行情构建器尚未到达源市场屏障或可见队列不匹配"));
        }
        let active = id.active_target()?;
        let routes = RouteTable::new(source.routes().routes().map(|route| {
            if route.market_id == id.market {
                active
            } else {
                *route
            }
        }))
        .map_err(|_| invalid("路由包含重复市场"))?;
        let digest = source
            .transfer_market_to(&mut target, id.market, &routes, limit)
            .map_err(|_| invalid("迁移切面或目标容量不兼容"))?;
        let encode = |image: &SnapshotImage| {
            image
                .encode_snapshot(limit)
                .map(Arc::from)
                .map_err(|_| invalid("无法编码迁移镜像"))
        };
        Ok(Self {
            source: encode(&source)?,
            target: encode(&target)?,
            digest,
            builder: feed.clone(),
        })
    }

    fn encode(&self, id: MigrationId, storage: &MigrationStorage) -> io::Result<Vec<u8>> {
        let feed = self
            .builder
            .encode_checkpoint(storage.max_bytes)
            .map_err(|_| invalid("行情检查点编码失败"))?;
        let mut w = SnapshotEncoder::new(storage.max_bytes);
        (|| {
            w.raw_bytes(b"MIG1")?;
            w.u16(id.cluster.0)?;
            w.u32(id.market.0)?;
            w.u16(id.source.0)?;
            w.u16(id.target.0)?;
            w.u64(id.old_epoch)?;
            w.u64(storage.source_snapshot.1)?;
            w.u64(storage.target_snapshot.1)?;
            w.bytes(&self.digest)?;
            w.u64(self.builder.watermarks().0)?;
            w.bytes(&feed)?;
            w.bytes(&self.source)?;
            w.bytes(&self.target)
        })()
        .map_err(|_| invalid("迁移交接超过容量"))?;
        Ok(w.finish())
    }

    fn decode(bytes: &[u8], id: MigrationId, storage: &MigrationStorage) -> io::Result<Self> {
        (|| {
            let mut r = SnapshotDecoder::new(bytes, storage.max_bytes)?;
            if r.u32()? != u32::from_be_bytes(*b"MIG1")
                || r.u16()? != id.cluster.0
                || r.u32()? != id.market.0
                || r.u16()? != id.source.0
                || r.u16()? != id.target.0
                || r.u64()? != id.old_epoch
                || r.u64()? != storage.source_snapshot.1
                || r.u64()? != storage.target_snapshot.1
            {
                return Err(SnapshotWireError);
            }
            let digest = r.bytes()?.try_into().map_err(|_| SnapshotWireError)?;
            let source_seq = r.u64()?;
            let builder = FeedBuilder::decode_checkpoint(
                r.bytes()?,
                storage.max_bytes,
                id.market,
                source_seq,
            )
            .map_err(|_| SnapshotWireError)?;
            if builder.watermarks().0 != source_seq {
                return Err(SnapshotWireError);
            }
            let source = Arc::from(r.bytes()?);
            let target = Arc::from(r.bytes()?);
            r.finish()?;
            Ok(Self {
                source,
                target,
                digest,
                builder,
            })
        })()
        .map_err(|_| invalid("持久迁移交接损坏或身份不匹配"))
    }
}

impl MigrationStorage {
    fn operation_directory(&self, id: MigrationId) -> PathBuf {
        self.archive.join(format!(
            "market-{}-{}-{}-{}-{}",
            id.cluster.0, id.market.0, id.source.0, id.target.0, id.old_epoch
        ))
    }

    /// 从该次迁移的管理日志读取已经确认的切换边界。回复丢失后可按原身份重试补写。
    pub fn read_boundary(&self, id: MigrationId) -> io::Result<MarketMigrationBoundary> {
        let (tag, epoch, bytes) = matching_persistence::read_snapshot(
            self.operation_directory(id).join("snapshot-2.bin"),
        )?;
        let route_epoch = id.new_epoch()?;
        if tag != 2 || epoch != route_epoch {
            return Err(invalid("管理边界身份错误"));
        }
        (|| {
            let mut r = SnapshotDecoder::new(&bytes, 64)?;
            if r.u32()? != u32::from_be_bytes(*b"MBD1")
                || r.u16()? != id.cluster.0
                || r.u32()? != id.market.0
                || r.u16()? != id.source.0
                || r.u16()? != id.target.0
                || r.u64()? != route_epoch
            {
                return Err(SnapshotWireError);
            }
            let last_market_event_seq = r.u64()?;
            let last_feed_seq = crate::FeedSeqSnapshot {
                mbo: r.u64()?,
                mbp: r.u64()?,
                bbo: r.u64()?,
                trade: r.u64()?,
            };
            r.finish()?;
            Ok(MarketMigrationBoundary {
                market_id: id.market,
                old_shard_id: id.source,
                new_shard_id: id.target,
                route_epoch,
                last_market_event_seq,
                last_feed_seq,
            })
        })()
        .map_err(|_| invalid("管理边界损坏或与该次迁移不符"))
    }

    /// 在本地权威协调器上执行或重试迁移。首次生成交接时需要来自已提交行情源的构建器。
    pub fn migrate<A: RouteAuthority>(
        &self,
        coordinator: &mut RouteCoordinator<A>,
        id: MigrationId,
        feed: Option<&FeedBuilder>,
    ) -> io::Result<MigrationResult> {
        self.run(coordinator, id, feed)
    }

    /// 在监视线程继续维护租约期间执行迁移；所有路由写入都由持有权威的线程完成。
    pub fn migrate_monitored(
        &self,
        monitor: &mut RouteMonitor,
        id: MigrationId,
        feed: Option<&FeedBuilder>,
    ) -> io::Result<MigrationResult> {
        self.run(monitor, id, feed)
    }

    fn run(
        &self,
        coordinator: &mut impl MigrationRoutes,
        id: MigrationId,
        feed: Option<&FeedBuilder>,
    ) -> io::Result<MigrationResult> {
        let active = id.active_target()?;
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| invalid("迁移等待期限溢出"))?;
        if !(1..=256 * 1024 * 1024).contains(&self.max_bytes) || self.timeout.is_zero() {
            return Err(invalid("迁移容量或等待期限无效"));
        }
        let source_handle = coordinator.migration_shard(id.source)?;
        let target_handle = coordinator.migration_shard(id.target)?;
        if [&source_handle, &target_handle]
            .into_iter()
            .any(|s| s.cluster_id() != id.cluster || !s.supports_local_migration())
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "分片持久模式缺少可恢复的迁移快照交接",
            ));
        }
        let roots = [
            self.archive.canonicalize()?,
            self.source_snapshot.0.canonicalize()?,
            self.target_snapshot.0.canonicalize()?,
        ];
        if roots.iter().any(|p| !p.is_dir())
            || roots[0] == roots[1]
            || roots[0] == roots[2]
            || roots[1] == roots[2]
        {
            return Err(invalid("迁移存档和两个分片快照需要三个独立目录"));
        }
        let directory = self.operation_directory(id);
        match fs::create_dir(&directory) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error),
        }
        matching_persistence::sync_directory(&roots[0])?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("migration.lock"))?;
        lock.try_lock()
            .map_err(|err| io::Error::other(format!("迁移目录被占用: {err}")))?;
        let current = coordinator.migration_route(id, remaining(deadline)?)?;
        let archived = directory.join("snapshot-1.bin").try_exists()?;
        let completed = current == active || current.route_epoch > active.route_epoch;
        if !archived && (completed || feed.is_none()) {
            return Err(invalid("缺少该次迁移的交接或源行情构建器"));
        }
        if !archived {
            for (dir, file_id) in [&self.source_snapshot, &self.target_snapshot] {
                if dir.join(format!("snapshot-{file_id}.bin")).try_exists()? {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "迁移快照身份已被占用",
                    ));
                }
            }
        }
        if !completed && current.state == RouteState::Active && current.shard_id == id.source {
            if archived {
                return Err(invalid("活动源路由与已同步迁移交接冲突"));
            }
            let inspect = |handle: &ShardHandle| {
                at_barrier(deadline, || handle.take_snapshot(u128::from(id.market.0)))?
                    .map_err(|_| invalid("无法捕获迁移预检切面"))
            };
            let source = inspect(&source_handle)?;
            let target = inspect(&target_handle)?;
            let prefixes = (source.snapshot_seq(), target.snapshot_seq());
            let (source, target) = source
                .simulate_migration_freeze(target, id)
                .map_err(|_| invalid("市场状态、分片配置或目标容量不支持迁移"))?;
            // 归档容量同时包含两张镜像、行情检查点和外层字段；进入持久停写前按实际格式验证。
            Prepared::capture(
                id,
                source,
                target,
                feed.ok_or_else(|| invalid("缺少源行情切面"))?,
                self.max_bytes,
            )?
            .encode(id, self)?;
            coordinator.begin_migration(id, prefixes, remaining(deadline)?)?;
        }
        let prepared = if archived {
            let (file_id, epoch, bytes) =
                matching_persistence::read_snapshot(directory.join("snapshot-1.bin"))?;
            if file_id != 1 || epoch != active.route_epoch {
                return Err(invalid("迁移交接外层身份不匹配"));
            }
            // 上一次调用可能在重命名后、同步目录前中断。
            matching_persistence::sync_directory(&directory)?;
            Prepared::decode(&bytes, id, self)?
        } else {
            let source = at_barrier(deadline, || source_handle.migration_snapshot(id))??;
            let target = at_barrier(deadline, || target_handle.migration_snapshot(id))??;
            let prepared = Prepared::capture(
                id,
                source,
                target,
                feed.ok_or_else(|| invalid("缺少源行情切面"))?,
                self.max_bytes,
            )?;
            let bytes = prepared.encode(id, self)?;
            matching_persistence::write_migration_snapshot_file(
                &directory,
                1,
                active.route_epoch,
                &bytes,
            )?;
            prepared
        };
        if !completed {
            self.install(
                &target_handle,
                id,
                &prepared.target,
                &self.target_snapshot,
                deadline,
            )?;
            self.install(
                &source_handle,
                id,
                &prepared.source,
                &self.source_snapshot,
                deadline,
            )?;
            coordinator.finish_migration(id, remaining(deadline)?)?;
        }
        let boundary =
            prepared
                .builder
                .migration_boundary(id.source, id.target, active.route_epoch);
        let mut audit = SnapshotEncoder::new(64);
        (|| {
            audit.raw_bytes(b"MBD1")?;
            audit.u16(id.cluster.0)?;
            audit.u32(id.market.0)?;
            audit.u16(id.source.0)?;
            audit.u16(id.target.0)?;
            audit.u64(boundary.route_epoch)?;
            audit.u64(boundary.last_market_event_seq)?;
            let seq = boundary.last_feed_seq;
            for watermark in [seq.mbo, seq.mbp, seq.bbo, seq.trade] {
                audit.u64(watermark)?;
            }
            Ok::<_, SnapshotWireError>(())
        })()
        .map_err(|_| invalid("管理边界编码失败"))?;
        matching_persistence::write_migration_snapshot_file(
            &directory,
            2,
            active.route_epoch,
            &audit.finish(),
        )?;
        Ok(MigrationResult {
            boundary,
            builder: prepared.builder,
            market_digest: prepared.digest,
        })
    }

    fn install(
        &self,
        handle: &ShardHandle,
        id: MigrationId,
        bytes: &Arc<[u8]>,
        dest: &(PathBuf, u64),
        deadline: Instant,
    ) -> io::Result<()> {
        at_barrier(deadline, || {
            handle.install_migration_image(
                id,
                Arc::clone(bytes),
                dest.0.clone(),
                dest.1,
                self.max_bytes,
            )
        })?
    }
}
