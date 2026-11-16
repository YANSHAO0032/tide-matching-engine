//! 管理操作的有界单机审计日志；文件操作只在管理入口执行。
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const HEADER: &[u8; 4] = b"AUD2";
const RECORD_LEN: usize = 90;

/// 受审计的操作类别；编号属于日志格式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AuditAction {
    /// 创建市场。
    MarketCreate = 1,
    /// 更新市场配置。
    MarketUpdate = 2,
    /// 市场状态或时段变化。
    MarketState = 3,
    /// 熔断开关或保护状态变化。
    KillSwitch = 4,
    /// 费率配置变化。
    FeeUpdate = 5,
    /// 控制栅栏或主节点纪元操作。
    Control = 6,
    /// 快照操作。
    Snapshot = 7,
    /// 身份认证失败。
    AuthenticationFailure = 8,
    /// 外部参考价格变更。
    ReferenceUpdate = 9,
    /// 管理入口发起的批量撤单。
    MassCancel = 10,
}

/// 一次管理操作的可追溯阶段。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AuditOutcome {
    /// 派发前已记录意图，崩溃后结果可能未知。
    Attempt = 1,
    /// 操作成功。
    Succeeded = 2,
    /// 操作明确拒绝或失败。
    Rejected = 3,
    /// 超时或断连导致结果未知。
    Unknown = 4,
}

/// 不含凭据或原始请求的固定字段审计记录。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditRecord {
    /// 可信认证方提供的操作者身份。
    pub actor_id: u64,
    /// 与业务命令关联的身份；非业务操作由调用方提供审计身份。
    pub command_id: u128,
    /// 操作类别。
    pub action: AuditAction,
    /// 本条记录所处的阶段。
    pub outcome: AuditOutcome,
    /// 若适用，记录市场身份。
    pub market_id: Option<u32>,
    /// 若适用，记录目标路由纪元。
    pub route_epoch: Option<u64>,
    /// 若适用，记录实际或预期的主节点纪元。
    pub primary_epoch: Option<u64>,
    /// 若适用，记录集群与分片身份。
    pub scope: Option<(u16, u16)>,
    /// 规范业务命令的 SHA-256 摘要，不包含密钥或认证凭据。
    pub command_digest: [u8; 32],
}

/// 独占的有界审计文件；日志不是业务 WAL。
pub struct AuditLog {
    /// 保持文件锁并顺序追加。
    file: File,
    /// 单写者清单记录已同步并确认的连续审计前缀。
    manifest: matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
    /// 已验证的记录。
    records: Vec<AuditRecord>,
    /// 记录条数上限。
    max_records: usize,
    /// 写入或同步失败后不再接受记录。
    failed: bool,
}

/// 管理派发或审计持久化失败。
#[derive(Debug)]
pub enum AuditDispatchError {
    /// 审计写入失败；如果命令已经入队，业务结果可能未知。
    Audit(io::Error),
    /// 非管理命令不能通过管理入口。
    NotAdmin,
    /// 目标没有正式持久命令入口。
    NotDurable,
    /// 命令无法编码为规范格式。
    InvalidCommand,
    /// 分片未接受命令。
    Ingress(crate::ShardError),
    /// 快照写入队列未接受请求。
    SnapshotIngress(crate::SnapshotWriterError),
    /// 快照写入或清单提交失败。
    SnapshotIo(io::Error),
    /// 主节点租约操作失败。
    Lease(crate::LeaseError),
    /// 命令已入队但在期限内没有得到结果。
    OutcomeUnknown,
}

impl AuditLog {
    /// 在管理线程审计并获取本机主节点租约；审计失败时不交付租约。
    pub fn acquire_primary(
        &mut self,
        actor_id: u64,
        operation_id: u128,
        store: &crate::ControlStore,
    ) -> Result<crate::PrimaryLease, AuditDispatchError> {
        use sha2::{Digest, Sha256};
        if self.failed || self.max_records.saturating_sub(self.records.len()) < 2 {
            return Err(AuditDispatchError::Audit(io::Error::other("审计容量不足")));
        }
        let (cluster, shard) = store.identity();
        let mut digest = Sha256::new();
        digest.update(b"tide/audit/primary-lease/v1");
        digest.update(cluster.get().to_be_bytes());
        digest.update(shard.get().to_be_bytes());
        let mut record = AuditRecord {
            actor_id,
            command_id: operation_id,
            action: AuditAction::Control,
            outcome: AuditOutcome::Attempt,
            market_id: None,
            route_epoch: None,
            primary_epoch: None,
            scope: Some((cluster.get(), shard.get())),
            command_digest: digest.finalize().into(),
        };
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        let result = store.acquire_primary();
        record.outcome = match &result {
            Ok(_) => AuditOutcome::Succeeded,
            Err(crate::LeaseError::Io(_)) => AuditOutcome::Unknown,
            Err(_) => AuditOutcome::Rejected,
        };
        record.primary_epoch = result.as_ref().ok().map(crate::PrimaryLease::epoch);
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        result.map_err(AuditDispatchError::Lease)
    }

    /// 在管理线程审计快照写入和清单登记；超时意味着结果未知。
    pub fn write_snapshot(
        &mut self,
        actor_id: u64,
        operation_id: u128,
        writer: &crate::SnapshotWriter,
        snapshot_id: u64,
        image: crate::SnapshotImage,
        timeout: std::time::Duration,
    ) -> Result<(), AuditDispatchError> {
        use sha2::{Digest, Sha256};
        if self.failed || self.max_records.saturating_sub(self.records.len()) < 2 {
            return Err(AuditDispatchError::Audit(io::Error::other("审计容量不足")));
        }
        let (cluster, shard) = image.identity();
        let mut digest = Sha256::new();
        digest.update(b"tide/audit/snapshot/v1");
        digest.update(cluster.get().to_be_bytes());
        digest.update(shard.get().to_be_bytes());
        digest.update(snapshot_id.to_be_bytes());
        digest.update(image.snapshot_seq().to_be_bytes());
        let mut record = AuditRecord {
            actor_id,
            command_id: operation_id,
            action: AuditAction::Snapshot,
            outcome: AuditOutcome::Attempt,
            market_id: None,
            route_epoch: None,
            primary_epoch: None,
            scope: Some((cluster.get(), shard.get())),
            command_digest: digest.finalize().into(),
        };
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        let receiver = match writer.submit(snapshot_id, image) {
            Ok(receiver) => receiver,
            Err(error) => {
                record.outcome = AuditOutcome::Rejected;
                self.append(&record).map_err(AuditDispatchError::Audit)?;
                return Err(AuditDispatchError::SnapshotIngress(error));
            }
        };
        let result = receiver.recv_timeout(timeout);
        record.outcome = match &result {
            Ok(Ok(())) => AuditOutcome::Succeeded,
            Ok(Err(_)) => AuditOutcome::Unknown,
            Err(_) => AuditOutcome::Unknown,
        };
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        result
            .map_err(|_| AuditDispatchError::OutcomeUnknown)?
            .map_err(AuditDispatchError::SnapshotIo)
    }

    /// 先同步管理意图，再向持久分片派发并同步记录结果。
    ///
    /// 本调用在管理线程执行文件同步和等待，不能在撮合线程调用。调用方负责
    /// 验证操作者权限；超时或结果审计失败时须按原命令身份查询或重试。
    pub fn dispatch(
        &mut self,
        actor_id: u64,
        shard: &crate::ShardHandle,
        command: matching_domain::CommandEnvelope,
        timeout: std::time::Duration,
    ) -> Result<Result<crate::FormalExecution, crate::FormalError>, AuditDispatchError> {
        use matching_domain::Command;
        use sha2::{Digest, Sha256};
        let action = match &command.payload {
            Command::CreateMarket(_) => AuditAction::MarketCreate,
            Command::UpdateMarket(_) => AuditAction::MarketUpdate,
            Command::StartSession(_)
            | Command::OpenMarket(_)
            | Command::SuspendMarket(_)
            | Command::ResumeMarket(_)
            | Command::EndSession(_)
            | Command::CloseMarket(_) => AuditAction::MarketState,
            Command::ShardKillSwitch(_)
            | Command::MarketKillSwitch(_)
            | Command::TriggerCircuitBreaker(_) => AuditAction::KillSwitch,
            Command::UpdateFeeSchedule(_) => AuditAction::FeeUpdate,
            Command::UpdateReferencePrice(_) => AuditAction::ReferenceUpdate,
            Command::ApplyUserControlFenceShard(_) => AuditAction::Control,
            Command::MassCancelShard(_) => AuditAction::MassCancel,
            Command::PlaceLimit(_) => {
                return Err(AuditDispatchError::NotAdmin);
            }
        };
        if !shard.has_durable_formal() {
            return Err(AuditDispatchError::NotDurable);
        }
        if self.failed || self.max_records.saturating_sub(self.records.len()) < 2 {
            return Err(AuditDispatchError::Audit(io::Error::other("审计容量不足")));
        }
        let bytes = matching_domain::encode_wal_command(&command)
            .map_err(|_| AuditDispatchError::InvalidCommand)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let mut record = AuditRecord {
            actor_id,
            command_id: command.command_id.0,
            action,
            outcome: AuditOutcome::Attempt,
            market_id: command.market_id.map(|market| market.0),
            route_epoch: command.route_epoch,
            primary_epoch: match &command.payload {
                Command::ShardKillSwitch(input) => Some(input.expected_primary_epoch),
                Command::ApplyUserControlFenceShard(input) => Some(input.expected_primary_epoch),
                _ => None,
            },
            scope: Some((shard.cluster_id().get(), shard.shard_id().get())),
            command_digest: digest,
        };
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        let receiver = match shard.submit_command(command) {
            Ok(receiver) => receiver,
            Err(error) => {
                record.outcome = AuditOutcome::Rejected;
                self.append(&record).map_err(AuditDispatchError::Audit)?;
                return Err(AuditDispatchError::Ingress(error));
            }
        };
        let result = receiver.recv_timeout(timeout);
        record.outcome = match &result {
            Ok(Ok(execution))
                if !matches!(execution.outcome, crate::FormalOutcome::Rejected(_)) =>
            {
                AuditOutcome::Succeeded
            }
            Ok(_) => AuditOutcome::Rejected,
            Err(_) => AuditOutcome::Unknown,
        };
        self.append(&record).map_err(AuditDispatchError::Audit)?;
        result.map_err(|_| AuditDispatchError::OutcomeUnknown)
    }

    /// 创建新日志，已存在的路径不会覆盖。
    pub fn create(path: impl AsRef<Path>, max_records: usize) -> io::Result<Self> {
        if max_records == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "审计容量为零"));
        }
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.try_lock()?;
        file.write_all(HEADER)?;
        file.sync_all()?;
        matching_persistence::sync_directory(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        let state_dir = manifest_dir(path);
        std::fs::create_dir(&state_dir)?;
        matching_persistence::sync_directory(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        let mut manifest = matching_persistence::ManifestManager::open(&state_dir)?;
        manifest.submit(matching_persistence::ManifestIntent::Initialize)?;
        manifest.submit(matching_persistence::ManifestIntent::RecoveryAnchor)?;
        Ok(Self {
            file,
            manifest,
            records: Vec::new(),
            max_records,
            failed: false,
        })
    }

    /// 独占并严格检查已有日志；截断、损坏与超过上限均拒绝。
    pub fn open(path: impl AsRef<Path>, max_records: usize) -> io::Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.try_lock()?;
        let manifest = matching_persistence::ManifestManager::<
            matching_persistence::FileManifestStorage,
        >::open_recovery(manifest_dir(path))?;
        if manifest.state().generation < 2 {
            return Err(invalid("审计清单未初始化"));
        }
        let count = usize::try_from(manifest.state().local_durable_seq)
            .map_err(|_| invalid("审计记录过多"))?;
        if max_records == 0 || count > max_records {
            return Err(invalid("审计容量无效"));
        }
        let expected = count
            .checked_mul(RECORD_LEN)
            .and_then(|v| v.checked_add(4))
            .ok_or_else(|| invalid("审计长度溢出"))?;
        let len = file.metadata()?.len();
        let expected_u64 = u64::try_from(expected).map_err(|_| invalid("审计长度溢出"))?;
        if len < expected_u64
            || len
                > expected_u64
                    .checked_add(RECORD_LEN as u64)
                    .ok_or_else(|| invalid("审计长度溢出"))?
        {
            return Err(invalid("审计日志长度无效"));
        }
        let mut header = [0; 4];
        file.read_exact(&mut header)?;
        if &header != HEADER {
            return Err(invalid("审计头无效"));
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(count)
            .map_err(|_| io::Error::other("审计内存不足"))?;
        for _ in 0..count {
            let mut bytes = [0; RECORD_LEN];
            file.read_exact(&mut bytes)?;
            records.push(decode(&bytes)?);
        }
        if len > expected_u64 {
            file.set_len(expected_u64)?;
            file.sync_all()?;
        }
        Ok(Self {
            file,
            manifest,
            records,
            max_records,
            failed: false,
        })
    }

    /// 追加并同步单条记录；失败后本句柄永久停止写入。
    pub fn append(&mut self, record: &AuditRecord) -> io::Result<()> {
        if self.failed || self.records.len() >= self.max_records {
            return Err(io::Error::other("审计日志不可写或已满"));
        }
        self.records
            .try_reserve(1)
            .map_err(|_| io::Error::other("审计内存不足"))?;
        let through_seq = self
            .manifest
            .state()
            .local_durable_seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("审计序号耗尽"))?;
        let bytes = encode(record);
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&bytes))
            .and_then(|()| self.file.sync_all());
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        if let Err(error) =
            self.manifest
                .submit(matching_persistence::ManifestIntent::LocalDurableAdvanced {
                    through_seq,
                    first_seq: 1,
                })
        {
            self.failed = true;
            return Err(error);
        }
        self.records.push(record.clone());
        Ok(())
    }

    /// 读取已校验记录，顺序与写入顺序一致。
    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }
}

/// 清单存于日志旁的独立目录，调用方须保护整个目录及其身份。
fn manifest_dir(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".state");
    name.into()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn encode(record: &AuditRecord) -> [u8; RECORD_LEN] {
    let mut b = [0; RECORD_LEN];
    b[..8].copy_from_slice(&record.actor_id.to_be_bytes());
    b[8..24].copy_from_slice(&record.command_id.to_be_bytes());
    b[24] = record.action as u8;
    b[25] = record.outcome as u8;
    if let Some(value) = record.market_id {
        b[26] = 1;
        b[27..31].copy_from_slice(&value.to_be_bytes());
    }
    if let Some(value) = record.route_epoch {
        b[31] = 1;
        b[32..40].copy_from_slice(&value.to_be_bytes());
    }
    if let Some(value) = record.primary_epoch {
        b[40] = 1;
        b[41..49].copy_from_slice(&value.to_be_bytes());
    }
    if let Some((cluster, shard)) = record.scope {
        b[49] = 1;
        b[50..52].copy_from_slice(&cluster.to_be_bytes());
        b[52..54].copy_from_slice(&shard.to_be_bytes());
    }
    b[54..86].copy_from_slice(&record.command_digest);
    let crc = matching_persistence::crc32c(&b[..86]);
    b[86..].copy_from_slice(&crc.to_be_bytes());
    b
}

fn decode(b: &[u8; RECORD_LEN]) -> io::Result<AuditRecord> {
    if matching_persistence::crc32c(&b[..86]).to_be_bytes() != b[86..] {
        return Err(invalid("审计校验失败"));
    }
    let action = match b[24] {
        1 => AuditAction::MarketCreate,
        2 => AuditAction::MarketUpdate,
        3 => AuditAction::MarketState,
        4 => AuditAction::KillSwitch,
        5 => AuditAction::FeeUpdate,
        6 => AuditAction::Control,
        7 => AuditAction::Snapshot,
        8 => AuditAction::AuthenticationFailure,
        9 => AuditAction::ReferenceUpdate,
        10 => AuditAction::MassCancel,
        _ => return Err(invalid("审计操作无效")),
    };
    let outcome = match b[25] {
        1 => AuditOutcome::Attempt,
        2 => AuditOutcome::Succeeded,
        3 => AuditOutcome::Rejected,
        4 => AuditOutcome::Unknown,
        _ => return Err(invalid("审计结果无效")),
    };
    let market_id = match b[26] {
        0 if b[27..31] == [0; 4] => None,
        1 => Some(u32::from_be_bytes(b[27..31].try_into().expect("定长字段"))),
        _ => return Err(invalid("审计市场字段无效")),
    };
    let route_epoch = match b[31] {
        0 if b[32..40] == [0; 8] => None,
        1 => Some(u64::from_be_bytes(b[32..40].try_into().expect("定长字段"))),
        _ => return Err(invalid("审计路由字段无效")),
    };
    let primary_epoch = match b[40] {
        0 if b[41..49] == [0; 8] => None,
        1 => Some(u64::from_be_bytes(b[41..49].try_into().expect("定长字段"))),
        _ => return Err(invalid("审计主节点纪元字段无效")),
    };
    let scope = match b[49] {
        0 if b[50..54] == [0; 4] => None,
        1 => Some((
            u16::from_be_bytes(b[50..52].try_into().expect("定长字段")),
            u16::from_be_bytes(b[52..54].try_into().expect("定长字段")),
        )),
        _ => return Err(invalid("审计分片字段无效")),
    };
    Ok(AuditRecord {
        actor_id: u64::from_be_bytes(b[..8].try_into().expect("定长字段")),
        command_id: u128::from_be_bytes(b[8..24].try_into().expect("定长字段")),
        action,
        outcome,
        market_id,
        route_epoch,
        primary_epoch,
        scope,
        command_digest: b[54..86].try_into().expect("定长字段"),
    })
}
