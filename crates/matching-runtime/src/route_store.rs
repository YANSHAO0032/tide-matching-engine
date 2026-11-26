//! 单机持久路由日志、路由权威接口，以及向分片安装权威路由的协调器。
use crate::{MarketRoute, RouteState, RouteTable};
use matching_domain::{
    ClusterId, MarketId, ShardId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
};
use matching_persistence::{FileManifestStorage, ManifestIntent, ManifestManager};
use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

mod migration;
use migration::MigrationAction;
pub(crate) use migration::MigrationRoutes;

/// 绑定权威版本、集群和已核验新表的迁移写入许可，只能由本进程的协调器创建。
pub struct MigrationRouteChange<'a> {
    /// 已核对的集群。
    cluster: ClusterId,
    /// 先前线性一致读取的权威版本。
    expected: u64,
    /// 只能写入此份完整路由表。
    routes: &'a RouteTable,
}

impl MigrationRouteChange<'_> {
    /// 校验适配器绑定的集群后，取得一次比较写入的版本与快照。
    pub fn for_cluster(&self, cluster: ClusterId) -> io::Result<(u64, &RouteTable)> {
        if self.cluster != cluster {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "迁移路由集群不匹配",
            ));
        }
        Ok((self.expected, self.routes))
    }
}

/// 单进程持有的持久路由权威；仅适用于可信单机文件系统。
pub struct RouteControlStore {
    /// 日志绑定的集群身份。
    cluster: ClusterId,
    /// 独占路由日志。
    file: File,
    /// 已确认日志前缀。
    manifest: ManifestManager<FileManifestStorage>,
    /// 最近确认的路由表。
    routes: RouteTable,
    /// 单次快照最大市场数。
    max_routes: usize,
    /// 日志最大记录数。
    max_records: u64,
    /// 持久化失败后不再允许读写。
    failed: bool,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "路由日志损坏")
}
fn limit(max: usize) -> io::Result<usize> {
    max.checked_mul(19)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(invalid)
}
/// 按市场身份的确定顺序编码有界路由快照。
pub fn encode_route_snapshot(routes: &RouteTable, max: usize) -> io::Result<Vec<u8>> {
    let count = routes.routes().count();
    if count > max {
        return Err(invalid());
    }
    let mut w = SnapshotEncoder::new(limit(max)?);
    w.u32(u32::try_from(count).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    for route in routes.routes() {
        w.u32(route.market_id.get())
            .and_then(|_| w.u16(route.shard_id.get()))
            .and_then(|_| w.u64(route.route_epoch))
            .map_err(|_| invalid())?;
        match route.state {
            RouteState::Active => w.u8(0).map_err(|_| invalid())?,
            RouteState::Migrating { source, target } => {
                w.u8(1)
                    .and_then(|_| w.u16(source.get()))
                    .and_then(|_| w.u16(target.get()))
                    .map_err(|_| invalid())?;
            }
        }
    }
    Ok(w.finish())
}
/// 解码并验证有界路由快照。
pub fn decode_route_snapshot(bytes: &[u8], max: usize) -> io::Result<RouteTable> {
    let mut r = SnapshotDecoder::new(bytes, limit(max)?).map_err(|_| invalid())?;
    let count = usize::try_from(r.u32().map_err(|_| invalid())?).map_err(|_| invalid())?;
    if count > max || count > r.remaining() / 15 {
        return Err(invalid());
    }
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(count)
        .map_err(|_| io::Error::other("路由内存不足"))?;
    let mut previous = None;
    for _ in 0..count {
        let market_id = MarketId::new(r.u32().map_err(|_| invalid())?);
        if previous.is_some_and(|p| p >= market_id) {
            return Err(invalid());
        }
        previous = Some(market_id);
        let shard_id = ShardId::new(r.u16().map_err(|_| invalid())?);
        let route_epoch = r.u64().map_err(|_| invalid())?;
        let state = match r.u8().map_err(|_| invalid())? {
            0 => RouteState::Active,
            1 => RouteState::Migrating {
                source: ShardId::new(r.u16().map_err(|_| invalid())?),
                target: ShardId::new(r.u16().map_err(|_| invalid())?),
            },
            _ => return Err(invalid()),
        };
        routes.push(MarketRoute {
            market_id,
            shard_id,
            route_epoch,
            state,
        });
    }
    r.finish().map_err(|_| invalid())?;
    RouteTable::new(routes).map_err(|_| invalid())
}
fn lock(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|e| match e {
        TryLockError::WouldBlock => io::Error::new(io::ErrorKind::WouldBlock, "路由存储已被占用"),
        TryLockError::Error(e) => e,
    })
}

impl RouteControlStore {
    /// 建立独占日志，持久确认初始路由后返回。
    pub fn create_for_cluster(
        dir: impl AsRef<Path>,
        cluster: ClusterId,
        initial: RouteTable,
        max_routes: usize,
        max_records: u64,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        if max_records == 0 {
            return Err(invalid());
        }
        let _ = encode_route_snapshot(&initial, max_routes)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join("routes.log"))?;
        lock(&file)?;
        let mut header = [0_u8; 6];
        header[..4].copy_from_slice(b"RTC2");
        header[4..].copy_from_slice(&cluster.get().to_be_bytes());
        file.write_all(&header)?;
        file.write_all(&matching_persistence::crc32c(&header).to_be_bytes())?;
        file.sync_all()?;
        matching_persistence::sync_directory(dir)?;
        let state = dir.join("routes-manifest");
        std::fs::create_dir(&state)?;
        matching_persistence::sync_directory(dir)?;
        let mut manifest = ManifestManager::<FileManifestStorage>::open(state)?;
        manifest.submit(ManifestIntent::Initialize)?;
        manifest.submit(ManifestIntent::RecoveryAnchor)?;
        let mut store = Self {
            cluster,
            file,
            manifest,
            routes: RouteTable::default(),
            max_routes,
            max_records,
            failed: false,
        };
        store.compare_and_swap(0, initial)?;
        Ok(store)
    }
    /// 恢复已确认前缀；已确认记录损坏或缺失时失败关闭。
    pub fn open_for_cluster(
        dir: impl AsRef<Path>,
        cluster: ClusterId,
        max_routes: usize,
        max_records: u64,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join("routes.log"))?;
        lock(&file)?;
        let manifest =
            ManifestManager::<FileManifestStorage>::open_recovery(dir.join("routes-manifest"))?;
        let count = manifest.state().local_durable_seq;
        if count == 0 || count > max_records {
            return Err(invalid());
        }
        let mut header = [0; 6];
        file.read_exact(&mut header)?;
        let mut crc = [0_u8; 4];
        file.read_exact(&mut crc)?;
        if &header[..4] != b"RTC2"
            || header[4..] != cluster.get().to_be_bytes()
            || matching_persistence::crc32c(&header) != u32::from_be_bytes(crc)
        {
            return Err(invalid());
        }
        let mut routes = RouteTable::default();
        for _ in 0..count {
            let mut len = [0; 4];
            file.read_exact(&mut len)?;
            let size = usize::try_from(u32::from_be_bytes(len)).map_err(|_| invalid())?;
            if size > limit(max_routes)? || size < 4 {
                return Err(invalid());
            }
            let mut data = Vec::new();
            data.try_reserve_exact(size)
                .map_err(|_| io::Error::other("路由记录内存不足"))?;
            data.resize(size, 0);
            file.read_exact(&mut data)?;
            let mut crc = [0; 4];
            file.read_exact(&mut crc)?;
            let mut checked = Vec::new();
            checked
                .try_reserve_exact(size.checked_add(4).ok_or_else(invalid)?)
                .map_err(|_| io::Error::other("路由记录内存不足"))?;
            checked.extend_from_slice(&len);
            checked.extend_from_slice(&data);
            if matching_persistence::crc32c(&checked) != u32::from_be_bytes(crc) {
                return Err(invalid());
            }
            routes = decode_route_snapshot(&data, max_routes)?;
        }
        let end = file.stream_position()?;
        if file.metadata()?.len() != end {
            file.set_len(end)?;
            file.sync_all()?;
        }
        Ok(Self {
            cluster,
            file,
            manifest,
            routes,
            max_routes,
            max_records,
            failed: false,
        })
    }
    /// 返回已确认的版本和不可变路由。
    pub fn read(&self) -> io::Result<(u64, &RouteTable)> {
        if self.failed {
            return Err(io::Error::other("路由存储已停止"));
        }
        Ok((self.manifest.state().local_durable_seq, &self.routes))
    }
    /// 版本相符时同步完整新表并提交持久前缀；失败后关闭实例。
    pub(crate) fn compare_and_swap(
        &mut self,
        expected: u64,
        routes: RouteTable,
    ) -> io::Result<u64> {
        if self.failed {
            return Err(io::Error::other("路由存储已停止"));
        }
        if self.manifest.state().local_durable_seq != expected {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "路由版本不匹配",
            ));
        }
        let next = expected
            .checked_add(1)
            .filter(|n| *n <= self.max_records)
            .ok_or_else(invalid)?;
        let data = encode_route_snapshot(&routes, self.max_routes)?;
        let len = u32::try_from(data.len())
            .map_err(|_| invalid())?
            .to_be_bytes();
        let mut record = Vec::new();
        record
            .try_reserve_exact(data.len().checked_add(8).ok_or_else(invalid)?)
            .map_err(|_| io::Error::other("路由记录内存不足"))?;
        record.extend_from_slice(&len);
        record.extend_from_slice(&data);
        record.extend_from_slice(&matching_persistence::crc32c(&record).to_be_bytes());
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|()| self.file.sync_all())
            .and_then(|()| {
                self.manifest.submit(ManifestIntent::LocalDurableAdvanced {
                    through_seq: next,
                    first_seq: 1,
                })
            });
        if let Err(e) = result {
            self.failed = true;
            return Err(e);
        }
        self.routes = routes;
        Ok(next)
    }
}

/// 路由控制权威；生产实现须保证线性一致读取、持久化 CAS 和有效所有权。
///
/// 在该值被丢弃前必须持续保持其所有权，租约续期由实现者负责；
/// 租约失效后必须关闭相关分片入口；周期监视不替代分片执行侧 fencing。
pub trait RouteAuthority: Send + 'static {
    /// 返回此权威永久绑定的集群身份。
    fn cluster_id(&self) -> ClusterId;
    /// 是否必须由后台监视线程持续续持后才能开放变更。
    ///
    /// 默认要求监视；仅无过期时间且独占性持续至对象释放的权威可返回 false。
    fn requires_monitor(&self) -> bool {
        true
    }
    /// 读取当前已持久确认的完整路由和版本。
    fn load(&self) -> io::Result<(u64, RouteTable)>;
    /// 检查监视间隔是否适用于权威的续持方式。
    fn validate_monitor_interval(&self, _interval: Duration) -> io::Result<()> {
        Err(io::Error::other("路由权威未验证监视间隔"))
    }
    /// 续持权威；适配器未实现时失败关闭。
    fn renew(&mut self) -> io::Result<()> {
        Err(io::Error::other("路由权威不支持自动续持"))
    }
    /// 比较版本并原子持久化新路由；失败必须显式返回。
    fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> io::Result<u64>;

    /// 持久化协调器核验的迁移阶段；未支持迁移的适配器拒绝该操作。
    fn commit_migration(&mut self, _change: MigrationRouteChange<'_>) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "路由权威未支持迁移",
        ))
    }
}

/// 单机权威的封装；只能由协调器构建。
pub struct LocalRouteAuthority(RouteControlStore);

impl RouteAuthority for LocalRouteAuthority {
    fn requires_monitor(&self) -> bool {
        false
    }
    fn validate_monitor_interval(&self, interval: Duration) -> io::Result<()> {
        if interval.is_zero() {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "监视间隔为零"))
        } else {
            Ok(())
        }
    }
    fn renew(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn cluster_id(&self) -> ClusterId {
        self.0.cluster
    }
    fn load(&self) -> io::Result<(u64, RouteTable)> {
        let (revision, routes) = self.0.read()?;
        Ok((revision, routes.clone()))
    }

    fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> io::Result<u64> {
        if !self.0.read()?.1.allows_epoch_update(&routes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "所有权变更须由迁移流程提交",
            ));
        }
        self.0.compare_and_swap(expected, routes)
    }

    fn commit_migration(&mut self, change: MigrationRouteChange<'_>) -> io::Result<u64> {
        let (expected, routes) = change.for_cluster(self.cluster_id())?;
        self.0.compare_and_swap(expected, routes.clone())
    }
}

/// 协调控制存储和本进程分片的路由版本切换。
pub struct RouteCoordinator<A: RouteAuthority = LocalRouteAuthority> {
    /// 当前持久路由的唯一写者。
    store: Option<A>,
    /// 按分片身份排列的分片入口。
    shards: Vec<crate::ShardHandle>,
    /// 操作失败后不再允许继续协调。
    failed: bool,
    /// 租约权威等待监视线程接管期间保持分片暂停。
    awaiting_monitor: bool,
    /// 在本进程唯一认领的集群身份。
    cluster: ClusterId,
    /// 最近一次由此协调器确认的持久版本。
    revision: u64,
}

/// 在外围周期核验路由权威的线程句柄。
pub struct RouteMonitor {
    /// 监视线程实际管理的分片，供迁移定位同一工作线程。
    shards: Vec<crate::ShardHandle>,
    /// 通知线程结束的有界通道。
    stop: std::sync::mpsc::SyncSender<MonitorCommand>,
    /// 核验线程；退出后协调器负责关闭并排空入口。
    join: Option<std::thread::JoinHandle<()>>,
}

enum MonitorCommand {
    /// 停止权威监视线程。
    Stop,
    /// 在续约线程中核验身份并执行单次迁移路由操作。
    Migration {
        /// 绑定集群、市场、源和目标的迁移身份。
        id: crate::MigrationId,
        /// 当前请求的路由阶段。
        action: MigrationAction,
        /// 控制消息可等待的时间。
        timeout: Duration,
        /// 返回权威路由或失败原因。
        reply: std::sync::mpsc::SyncSender<io::Result<MarketRoute>>,
    },
    /// 在监视线程持有权威期间更新路由。
    Update(
        /// 预期的当前路由版本。
        u64,
        /// 待持久化并安装的完整路由表。
        RouteTable,
        /// 等待队列和更新完成的总期限。
        Duration,
        /// 接收新版本或更新错误的回复端。
        std::sync::mpsc::SyncSender<io::Result<u64>>,
    ),
}

impl RouteMonitor {
    /// 投递有界路由更新；等待超时时结果未知，调用方不得假设未提交。
    pub fn update(&self, expected: u64, routes: RouteTable, timeout: Duration) -> io::Result<u64> {
        if timeout.is_zero() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "更新时间为零"));
        }
        self.request(timeout, |reply| {
            MonitorCommand::Update(expected, routes, timeout, reply)
        })
    }

    /// 投递一次权威操作并等待确认，超时后只允许核对持久结果。
    fn request<T>(
        &self,
        timeout: Duration,
        command: impl FnOnce(std::sync::mpsc::SyncSender<io::Result<T>>) -> MonitorCommand,
    ) -> io::Result<T> {
        let (reply, receiver) = std::sync::mpsc::sync_channel(1);
        self.stop
            .try_send(command(reply))
            .map_err(|error| match error {
                std::sync::mpsc::TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "路由监视入口繁忙")
                }
                std::sync::mpsc::TrySendError::Disconnected(_) => {
                    io::Error::other("路由监视已停止")
                }
            })?;
        receiver
            .recv_timeout(timeout)
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "路由更新结果未知"))?
    }
}

impl Drop for RouteMonitor {
    fn drop(&mut self) {
        let _ = self.stop.send(MonitorCommand::Stop);
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            eprintln!("路由监视线程异常退出");
        }
    }
}

fn active_coordinators() -> &'static Mutex<std::collections::BTreeSet<ClusterId>> {
    static ACTIVE: OnceLock<Mutex<std::collections::BTreeSet<ClusterId>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(std::collections::BTreeSet::new()))
}

/// 建立协调器期间保持集群登记；失败时对未受其他协调器管理的入口失败关闭。
struct StartupClaim<A: RouteAuthority> {
    /// 已认领的集群。
    cluster: ClusterId,
    /// 仅在构造失败时封闭的分片。
    shards: Vec<crate::ShardHandle>,
    /// 完成后转交给协调器。
    armed: bool,
    /// 构造失败后须持有至分片完成排空的权威。
    authority: Option<A>,
}
impl<A: RouteAuthority> Drop for StartupClaim<A> {
    fn drop(&mut self) {
        if self.armed {
            for shard in &self.shards {
                shard.fence_route_ingress();
            }
            let authority = self.authority.take().expect("启动守卫持有权威");
            release_after_drain(authority, std::mem::take(&mut self.shards), self.cluster);
        }
    }
}

impl RouteCoordinator<LocalRouteAuthority> {
    /// 绑定单机持久控制存储；该适配器不具备跨主机复制。
    pub fn new(store: RouteControlStore, shards: Vec<crate::ShardHandle>) -> io::Result<Self> {
        Self::with_authority(LocalRouteAuthority(store), shards)
    }
}

impl<A: RouteAuthority> RouteCoordinator<A> {
    /// 在外围周期核验权威，发现错误时关闭入口；不能替代分片执行侧 fencing。
    pub fn start_monitor(mut self, interval: Duration) -> io::Result<RouteMonitor> {
        if interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "路由监视间隔无效",
            ));
        }
        self.store
            .as_ref()
            .expect("协调器持有路由权威")
            .validate_monitor_interval(interval)?;
        if let Err(error) = self.store.as_mut().expect("协调器持有路由权威").renew() {
            self.fence_ingress();
            return Err(error);
        }
        self.verify_authority()?;
        let (stop, receiver) = std::sync::mpsc::sync_channel(1);
        let (started, startup) = std::sync::mpsc::sync_channel(1);
        let monitor_shards = self.shards.clone();
        let join = std::thread::Builder::new()
            .name("route-monitor".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let start_result = if self.awaiting_monitor {
                        let deadline = Instant::now()
                            .checked_add(Duration::from_secs(5))
                            .ok_or_else(invalid);
                        deadline.and_then(|deadline| {
                            for shard in &self.shards {
                                await_control(deadline, || shard.resume_route_mutations())?;
                            }
                            self.awaiting_monitor = false;
                            Ok(())
                        })
                    } else {
                        Ok(())
                    };
                    if start_result.is_err() {
                        self.fence_ingress();
                    }
                    if started.send(start_result).is_err() || self.failed {
                        self.fence_ingress();
                        return;
                    }
                    loop {
                        match receiver.recv_timeout(interval) {
                            Ok(MonitorCommand::Stop)
                            | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                            Ok(MonitorCommand::Update(expected, routes, timeout, reply)) => {
                                let result = self.update_inner(expected, routes, timeout);
                                let _ = reply.send(result);
                            }
                            Ok(MonitorCommand::Migration {
                                id,
                                action,
                                timeout,
                                reply,
                            }) => {
                                let result = self.apply_migration(id, action, timeout);
                                let _ = reply.send(result);
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        }
                        if self.failed {
                            break;
                        }
                        if self
                            .store
                            .as_mut()
                            .expect("协调器持有路由权威")
                            .renew()
                            .is_err()
                        {
                            self.fence_ingress();
                            break;
                        }
                        if self.verify_authority().is_err() {
                            break;
                        }
                    }
                }));
                if let Err(payload) = outcome {
                    self.fence_ingress();
                    std::panic::resume_unwind(payload);
                }
            })?;
        match startup.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => (),
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                for shard in &monitor_shards {
                    shard.fence_route_ingress();
                }
                return Err(io::Error::other("路由监视启动未确认"));
            }
        }
        Ok(RouteMonitor {
            stop,
            shards: monitor_shards,
            join: Some(join),
        })
    }
    /// 校验全部分片属于同一集群、身份唯一且与持久路由一致。
    pub fn with_authority(store: A, shards: Vec<crate::ShardHandle>) -> io::Result<Self> {
        Self::with_authority_timeout(store, shards, Duration::from_secs(5))
    }

    /// 以有界等待暂停分片、核验权威并恢复；控制消息超时后结果可能未知。
    pub fn with_authority_timeout(
        store: A,
        mut shards: Vec<crate::ShardHandle>,
        timeout: Duration,
    ) -> io::Result<Self> {
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "启动等待时间为零",
            ));
        }
        shards.sort_by_key(crate::ShardHandle::shard_id);
        if shards.is_empty()
            || shards.windows(2).any(|s| {
                s[0].shard_id() == s[1].shard_id() || s[0].cluster_id() != s[1].cluster_id()
            })
        {
            return Err(invalid());
        }
        let cluster = shards[0].cluster_id();
        let mut claim = StartupClaim {
            cluster,
            shards: shards.clone(),
            armed: false,
            authority: Some(store),
        };
        if !active_coordinators()
            .lock()
            .map_err(|_| io::Error::other("路由协调器登记不可用"))?
            .insert(cluster)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "集群已有路由协调器",
            ));
        }
        claim.armed = true;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(invalid)?;
        for shard in &shards {
            await_control(deadline, || shard.pause_route_mutations())?;
        }
        if claim
            .authority
            .as_ref()
            .expect("启动守卫持有权威")
            .cluster_id()
            != cluster
        {
            return Err(invalid());
        }
        let (revision, routes) = claim.authority.as_ref().expect("启动守卫持有权威").load()?;
        for shard in &shards {
            if !shard.observation().ingress_open
                || shard
                    .routes()
                    .map_err(|_| io::Error::other("分片路由不可用"))?
                    .as_ref()
                    != &routes
            {
                return Err(invalid());
            }
        }
        let contains = |id| shards.iter().any(|shard| shard.shard_id() == id);
        if routes.routes().filter(|r| r.state != RouteState::Active).count() > 1
            || routes.routes().any(|r| !contains(r.shard_id)
                || matches!(r.state, RouteState::Migrating { source, target }
                    if r.shard_id != source || source == target || !contains(target) || r.route_epoch == 0))
        {
            return Err(invalid());
        }
        let authority = claim.authority.as_mut().expect("启动守卫持有权威");
        authority.renew()?;
        let (confirmed_revision, confirmed_routes) = authority.load()?;
        if confirmed_revision != revision || confirmed_routes != routes {
            return Err(io::Error::other("启动恢复前路由权威发生变化"));
        }
        let awaiting_monitor = authority.requires_monitor();
        if awaiting_monitor && shards.iter().any(|shard| !shard.route_gated_at_start()) {
            return Err(io::Error::other("租约权威要求分片启动时暂停变更"));
        }
        if !awaiting_monitor {
            for shard in &shards {
                await_control(deadline, || shard.resume_route_mutations())?;
            }
        }
        claim.armed = false;
        Ok(Self {
            store: claim.authority.take(),
            shards,
            failed: false,
            awaiting_monitor,
            cluster,
            revision,
        })
    }

    /// 读取权威并核对本地分片；错误或外部版本变化时关闭所有入口。
    fn read_verified(&mut self) -> io::Result<(u64, RouteTable)> {
        if self.failed {
            return Err(io::Error::other("路由协调器已停止"));
        }
        let (version, current) = match self.store.as_ref().expect("协调器持有路由权威").load()
        {
            Ok(state) => state,
            Err(error) => {
                self.fence_ingress();
                return Err(error);
            }
        };
        for shard in &self.shards {
            if version != self.revision
                || shard.routes().map(|routes| routes.as_ref() == &current) != Ok(true)
            {
                self.fence_ingress();
                return Err(io::Error::other("持久路由与分片路由不一致"));
            }
        }
        Ok((version, current))
    }

    /// 主动核验权威版本与分片路由；可由外围监视循环调用。
    pub fn verify_authority(&mut self) -> io::Result<u64> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.read_verified())) {
            Ok(result) => result.map(|(revision, _)| revision),
            Err(payload) => {
                self.fence_ingress();
                std::panic::resume_unwind(payload)
            }
        }
    }

    /// 永久标记失权并关闭外部入口。
    fn fence_ingress(&mut self) {
        self.failed = true;
        for shard in &self.shards {
            shard.fence_route_ingress();
        }
    }

    /// 暂停并排空各分片，提交新版本，再安装并恢复；故障后协调器停止。
    /// `timeout` 限制控制消息等待；权威存储 I/O 的超时由适配器负责。
    pub fn update(
        &mut self,
        expected: u64,
        routes: RouteTable,
        timeout: Duration,
    ) -> io::Result<u64> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.update_inner(expected, routes, timeout)
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.fence_ingress();
                std::panic::resume_unwind(payload)
            }
        }
    }

    fn update_inner(
        &mut self,
        expected: u64,
        routes: RouteTable,
        timeout: Duration,
    ) -> io::Result<u64> {
        if self.awaiting_monitor {
            return Err(io::Error::other("路由监视尚未启动"));
        }
        let (version, current) = self.read_verified()?;
        if version != expected {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "路由版本不匹配",
            ));
        }
        if !current.allows_epoch_update(&routes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "本入口不支持所有权变更",
            ));
        }
        self.publish_routes(expected, &current, routes, timeout, None, None)
    }

    /// 暂停受影响的分片，确认持久转换后让所有分片看到同一份完整路由。
    fn publish_routes(
        &mut self,
        expected: u64,
        previous: &RouteTable,
        routes: RouteTable,
        timeout: Duration,
        participants: Option<(ShardId, ShardId)>,
        prefixes: Option<(u64, u64)>,
    ) -> io::Result<u64> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(invalid)?;
        let participating = |shard: &&crate::ShardHandle| {
            participants.is_none_or(|(source, target)| {
                shard.shard_id() == source || shard.shard_id() == target
            })
        };
        self.failed = true;
        let result = (|| {
            for shard in self.shards.iter().filter(participating) {
                await_control(deadline, || shard.pause_route_mutations())?;
            }
            // 停写前可能有在途命令。任一水位变化就恢复原路由，调用方重新预检。
            let mut checked = true;
            if let (Some((source, target)), Some((source_seq, target_seq))) =
                (participants, prefixes)
            {
                for (id, seq) in [(source, source_seq), (target, target_seq)] {
                    let handle = self.migration_shard(id)?;
                    let (_, applied) = await_reply(deadline, || handle.barrier(0))?;
                    checked &= applied == seq;
                }
            }
            let store = self.store.as_mut().expect("协调器持有路由权威");
            let (revision, confirmed) = if checked {
                let revision = if participants.is_some() {
                    store.commit_migration(MigrationRouteChange {
                        cluster: self.cluster,
                        expected,
                        routes: &routes,
                    })?
                } else {
                    store.compare_and_swap(expected, routes.clone())?
                };
                let (confirmed_revision, confirmed_routes) = store.load()?;
                if revision <= expected
                    || confirmed_revision != revision
                    || confirmed_routes != routes
                {
                    return Err(io::Error::other("路由权威未确认预期版本"));
                }
                for shard in &self.shards {
                    await_control(deadline, || shard.install_routes(routes.clone()))?;
                }
                (revision, &routes)
            } else {
                (expected, previous)
            };
            store.renew()?;
            let (current_revision, current_routes) = store.load()?;
            if current_revision != revision || &current_routes != confirmed {
                return Err(io::Error::other("恢复前路由权威发生变化"));
            }
            for shard in self.shards.iter().filter(participating) {
                await_control(deadline, || shard.resume_route_mutations())?;
            }
            Ok(revision)
        })();
        if let Ok(revision) = &result {
            self.revision = *revision;
            self.failed = false;
        } else {
            self.fence_ingress();
        }
        if result.as_ref().is_ok_and(|version| *version == expected) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "迁移预检后分片水位已变化",
            ));
        }
        result
    }
}

impl<A: RouteAuthority> Drop for RouteCoordinator<A> {
    fn drop(&mut self) {
        for shard in &self.shards {
            if std::thread::panicking() {
                shard.fence_route_ingress();
            } else {
                shard.close_route_ingress();
            }
        }
        let authority = self.store.take().expect("协调器持有路由权威");
        let shards = std::mem::take(&mut self.shards);
        let cluster = self.cluster;
        release_after_drain(authority, shards, cluster);
    }
}

/// 等待已接受消息到达顺序屏障后，才释放权威及本进程登记。
fn release_after_drain<A: RouteAuthority>(
    authority: A,
    shards: Vec<crate::ShardHandle>,
    cluster: ClusterId,
) {
    if shards
        .iter()
        .all(|shard| !shard.observation().thread_running)
    {
        drop(authority);
        if let Ok(mut active) = active_coordinators().lock() {
            active.remove(&cluster);
        }
        return;
    }
    // 线程创建失败时保留权威，避免锁被释放而尚有命令执行。
    let retained = std::sync::Arc::new(Mutex::new(Some((authority, shards, cluster))));
    let worker = std::sync::Arc::clone(&retained);
    if std::thread::Builder::new()
        .name("route-drain".into())
        .spawn(move || {
            let Some((authority, shards, cluster)) =
                worker.lock().ok().and_then(|mut state| state.take())
            else {
                return;
            };
            let drained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for shard in &shards {
                    shard.drain_closed_route_ingress();
                }
            }));
            if drained.is_err() {
                // 无法证明排空时保留权威，禁止其他协调器接管。
                eprintln!("路由排空异常，控制权威保持占用");
                std::mem::forget(authority);
                return;
            }
            drop(authority);
            if let Ok(mut active) = active_coordinators().lock() {
                active.remove(&cluster);
            }
        })
        .is_err()
    {
        eprintln!("无法创建路由排空线程，控制权威保持占用");
        std::mem::forget(retained);
    }
}

/// 重试未入队的控制消息；超时后的已入队消息可能仍会执行。
fn await_control(
    deadline: Instant,
    submit: impl FnMut() -> Result<
        std::sync::mpsc::Receiver<Result<(), crate::ShardError>>,
        crate::ShardError,
    >,
) -> io::Result<()> {
    await_reply(deadline, submit)?
        .map_err(|error| io::Error::other(format!("路由控制失败: {error:?}")))
}

/// 在总期限内取得有界控制消息的回复，只重试尚未入队的请求。
fn await_reply<T>(
    deadline: Instant,
    mut submit: impl FnMut() -> Result<std::sync::mpsc::Receiver<T>, crate::ShardError>,
) -> io::Result<T> {
    let receiver = loop {
        if Instant::now() >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "路由控制超时"));
        }
        match submit() {
            Ok(receiver) => break receiver,
            Err(crate::ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => return Err(io::Error::other(format!("路由控制入口失败: {error:?}"))),
        }
    };
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "路由控制超时"))?;
    receiver
        .recv_timeout(remaining)
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "路由控制回复未知"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn monitor_start_timeout_fences_before_late_resume() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        use std::sync::mpsc::sync_channel;
        /// 模拟权威的路由快照。
        struct Authority(RouteTable);
        impl RouteAuthority for Authority {
            fn cluster_id(&self) -> ClusterId {
                ClusterId(967)
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                Ok((1, self.0.clone()))
            }
            fn renew(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn validate_monitor_interval(&self, _: Duration) -> io::Result<()> {
                Ok(())
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                unreachable!()
            }
        }
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = crate::Shard::start_with_config(
            ShardId(82),
            ClusterId(967),
            routes.clone(),
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
            crate::ShardConfig {
                require_route_authority: true,
                ..Default::default()
            },
        )
        .unwrap();
        let handle = shard.handle();
        let coordinator =
            RouteCoordinator::with_authority(Authority(routes), vec![handle.clone()]).unwrap();
        let (entered, entered_rx) = sync_channel(1);
        let (release, release_rx) = sync_channel(1);
        handle.block_for_test(entered, release_rx).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(coordinator.start_monitor(Duration::from_secs(1)).is_err());
        assert!(handle.routes_are_fenced());
        release.send(()).unwrap();
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn startup_failure_retains_authority_until_blocked_worker_drains() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc::sync_channel,
        };
        struct Authority {
            /// 记录模拟权威对象是否已经释放。
            dropped: Arc<AtomicBool>,
            /// 控制读取时是否模拟异常。
            panic_on_load: bool,
            /// 模拟权威所属的集群。
            cluster: ClusterId,
            /// 用于在权威读取时阻塞分片的句柄。
            handle: crate::ShardHandle,
            /// 向测试线程交付解除分片阻塞的发送端。
            release_out: std::sync::mpsc::SyncSender<std::sync::mpsc::SyncSender<()>>,
        }
        impl Drop for Authority {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Release);
            }
        }
        impl RouteAuthority for Authority {
            fn requires_monitor(&self) -> bool {
                false
            }
            fn cluster_id(&self) -> ClusterId {
                self.cluster
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                let (entered_tx, entered_rx) = sync_channel(1);
                let (release_tx, release_rx) = sync_channel(1);
                self.handle.block_for_test(entered_tx, release_rx).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                self.release_out.send(release_tx).unwrap();
                if self.panic_on_load {
                    panic!("模拟读取异常");
                }
                Err(io::Error::other("模拟读取故障"))
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                unreachable!()
            }
        }
        for panic_on_load in [false, true] {
            let cluster = if panic_on_load {
                ClusterId(972)
            } else {
                ClusterId(973)
            };
            let routes = RouteTable::new([MarketRoute {
                market_id: MarketId(81),
                shard_id: ShardId(82),
                route_epoch: 1,
                state: RouteState::Active,
            }])
            .unwrap();
            let mut shard = crate::Shard::start(
                ShardId(82),
                cluster,
                routes,
                vec![(
                    MarketId(81),
                    ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
                )],
            )
            .unwrap();
            let handle = shard.handle();
            let (release_out, release_in) = sync_channel(1);
            let dropped = Arc::new(AtomicBool::new(false));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                RouteCoordinator::with_authority(
                    Authority {
                        dropped: dropped.clone(),
                        panic_on_load,
                        cluster,
                        handle: handle.clone(),
                        release_out,
                    },
                    vec![handle.clone()],
                )
            }));
            if panic_on_load {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(!dropped.load(Ordering::Acquire), "分片排空前释放了权威");
            assert!(active_coordinators().lock().unwrap().contains(&cluster));
            let release_tx = release_in.recv_timeout(Duration::from_secs(2)).unwrap();
            release_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while !dropped.load(Ordering::Acquire) && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(dropped.load(Ordering::Acquire));
            shard.shutdown(Duration::from_secs(2)).unwrap();
        }
    }
    #[test]
    fn initial_authority_identity_panic_fences_unowned_shard() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        struct Authority;
        impl RouteAuthority for Authority {
            fn requires_monitor(&self) -> bool {
                false
            }
            fn cluster_id(&self) -> ClusterId {
                panic!("模拟权威身份异常")
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                unreachable!()
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                unreachable!()
            }
        }
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = crate::Shard::start(
            ShardId(82),
            ClusterId(975),
            routes,
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            RouteCoordinator::with_authority(Authority, vec![handle.clone()])
        }));
        assert!(result.is_err());
        assert!(!handle.observation().ingress_open);
        assert!(handle.routes_are_fenced());
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn direct_update_panic_fences_even_if_caller_catches_it() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        /// 模拟权威的路由快照。
        struct Authority(RouteTable);
        impl RouteAuthority for Authority {
            fn requires_monitor(&self) -> bool {
                false
            }
            fn cluster_id(&self) -> ClusterId {
                ClusterId(977)
            }
            fn renew(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                Ok((1, self.0.clone()))
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                panic!("模拟写入异常")
            }
        }
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = crate::Shard::start(
            ShardId(82),
            ClusterId(977),
            routes.clone(),
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let mut coordinator =
            RouteCoordinator::with_authority(Authority(routes.clone()), vec![handle.clone()])
                .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            coordinator.update(1, routes, Duration::from_secs(2))
        }));
        assert!(result.is_err());
        assert!(!handle.observation().ingress_open);
        assert!(handle.routes_are_fenced());
        drop(coordinator);
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn startup_renewal_panic_fences_instead_of_graceful_drain() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        /// 模拟路由权威，依次保存路由表与续约时是否触发异常的标志。
        struct Authority(RouteTable, bool);
        impl RouteAuthority for Authority {
            fn requires_monitor(&self) -> bool {
                false
            }
            fn cluster_id(&self) -> ClusterId {
                ClusterId(978)
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                Ok((1, self.0.clone()))
            }
            fn validate_monitor_interval(&self, _: Duration) -> io::Result<()> {
                Ok(())
            }
            fn renew(&mut self) -> io::Result<()> {
                if self.1 {
                    panic!("模拟启动续约异常")
                }
                self.1 = true;
                Ok(())
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                unreachable!()
            }
        }
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = crate::Shard::start(
            ShardId(82),
            ClusterId(978),
            routes.clone(),
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let coordinator =
            RouteCoordinator::with_authority(Authority(routes, false), vec![handle.clone()])
                .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            coordinator.start_monitor(Duration::from_millis(10))
        }));
        assert!(result.is_err());
        assert!(!handle.observation().ingress_open);
        assert!(handle.routes_are_fenced());
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn monitor_panic_fences_instead_of_graceful_drain() {
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Authority {
            /// 模拟权威保存的路由快照。
            routes: RouteTable,
            /// 记录模拟权威的续约次数。
            renewals: Arc<AtomicUsize>,
        }
        impl RouteAuthority for Authority {
            fn requires_monitor(&self) -> bool {
                false
            }
            fn cluster_id(&self) -> ClusterId {
                ClusterId(979)
            }
            fn load(&self) -> io::Result<(u64, RouteTable)> {
                Ok((1, self.routes.clone()))
            }
            fn validate_monitor_interval(&self, _: Duration) -> io::Result<()> {
                Ok(())
            }
            fn renew(&mut self) -> io::Result<()> {
                if self.renewals.fetch_add(1, Ordering::SeqCst) > 1 {
                    panic!("模拟续约异常");
                }
                Ok(())
            }
            fn compare_and_swap(&mut self, _: u64, _: RouteTable) -> io::Result<u64> {
                unreachable!()
            }
        }
        let routes = RouteTable::new([MarketRoute {
            market_id: MarketId(81),
            shard_id: ShardId(82),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let mut shard = crate::Shard::start(
            ShardId(82),
            ClusterId(979),
            routes.clone(),
            vec![(
                MarketId(81),
                ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
            )],
        )
        .unwrap();
        let handle = shard.handle();
        let monitor = RouteCoordinator::with_authority(
            Authority {
                routes,
                renewals: Arc::new(AtomicUsize::new(0)),
            },
            vec![handle.clone()],
        )
        .unwrap()
        .start_monitor(Duration::from_millis(10))
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while handle.observation().ingress_open && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(!handle.observation().ingress_open);
        assert!(handle.routes_are_fenced());
        drop(monitor);
        shard.shutdown(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn expired_control_deadline_does_not_enqueue_new_message() {
        let mut called = false;
        let result = await_control(Instant::now() - Duration::from_secs(1), || {
            called = true;
            Err(crate::ShardError::Closed)
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(!called);
    }
    #[test]
    fn durable_route_cas_recovers_and_rejects_stale_revision() {
        let dir = std::env::temp_dir().join(format!(
            "tide-route-store-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir(&dir).unwrap();
        let initial = RouteTable::new([MarketRoute {
            market_id: MarketId(1),
            shard_id: ShardId(2),
            route_epoch: 1,
            state: RouteState::Active,
        }])
        .unwrap();
        let updated = RouteTable::new([
            *initial.get(MarketId(1)).unwrap(),
            MarketRoute {
                market_id: MarketId(2),
                shard_id: ShardId(3),
                route_epoch: 1,
                state: RouteState::Active,
            },
        ])
        .unwrap();
        let mut store =
            RouteControlStore::create_for_cluster(&dir, ClusterId(4), initial.clone(), 16, 16)
                .unwrap();
        assert_eq!(store.read().unwrap(), (1, &initial));
        assert!(RouteControlStore::open_for_cluster(&dir, ClusterId(4), 16, 16).is_err());
        assert_eq!(store.compare_and_swap(1, updated.clone()).unwrap(), 2);
        assert!(store.compare_and_swap(1, initial).is_err());
        drop(store);
        let store = RouteControlStore::open_for_cluster(&dir, ClusterId(4), 16, 16).unwrap();
        assert_eq!(store.read().unwrap(), (2, &updated));
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
