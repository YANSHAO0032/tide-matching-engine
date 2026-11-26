//! 单机会话断连意图日志；调用方保证目录独占且会话身份不复用。
use matching_domain::{
    Command, CommandEnvelope, CommandId, MarketId, MassCancelShard, OriginSessionId,
    ShardCancelScope, UserId,
};
use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

const MAX_TARGETS: usize = 4096;
const HEADER: usize = 33;
const TARGET_BYTES: usize = 28;
const RECORD: usize = 9;

/// 会话日志中持久登记的撤单市场、初始路由纪元和命令身份。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTarget {
    /// 目标市场。
    pub market: MarketId,
    /// 会话登记时的路由纪元。派发撤单时以此为下限，使用当前活动所有者的纪元。
    pub route_epoch: u64,
    /// 在所有重试中复用的分片命令身份。
    pub command_id: CommandId,
}

/// 独占一个已同步会话日志；不可跨主机充当租约。
///
/// 会话启用前持久登记全部可能的目标；重启后旧连接必须不可再提交命令，
/// 会话身份和子命令身份不得复用。每个目标只能在收到持久成功结果后确认。
/// 目录、文件身份和调用方授权必须由外围环境保护。
pub struct SessionControlJournal {
    /// 独占并持久追加会话状态的文件。
    file: File,
    /// 断连撤单作用的用户。
    user: UserId,
    /// 订单持久保存的来源会话。
    session: OriginSessionId,
    /// 不与客户端会话复用的控制命令会话。
    control_session: u64,
    /// 会话建立时持久固定的断连撤单开关。
    cancel_on_disconnect: bool,
    /// 已冻结且按市场排序的撤单目标。
    targets: Vec<SessionTarget>,
    /// 与目标一一对应的持久确认状态。
    acknowledged: Vec<bool>,
    /// 是否仍允许关联的网关会话接收新命令。
    open: bool,
    /// 追加或同步失败后禁止继续使用。
    failed: bool,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn lock(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|e| match e {
        TryLockError::WouldBlock => io::Error::new(io::ErrorKind::WouldBlock, "会话日志已被占用"),
        TryLockError::Error(error) => error,
    })
}
fn u32_at(bytes: &[u8]) -> io::Result<u32> {
    Ok(u32::from_be_bytes(
        bytes.try_into().map_err(|_| invalid("日志截断"))?,
    ))
}
fn u64_at(bytes: &[u8]) -> io::Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| invalid("日志截断"))?,
    ))
}
fn u128_at(bytes: &[u8]) -> io::Result<u128> {
    Ok(u128::from_be_bytes(
        bytes.try_into().map_err(|_| invalid("日志截断"))?,
    ))
}
fn valid_targets(targets: &[SessionTarget], max: usize) -> bool {
    !targets.is_empty()
        && targets.len() <= max.min(MAX_TARGETS)
        && targets.windows(2).all(|w| w[0].market < w[1].market)
        && targets
            .iter()
            .enumerate()
            .all(|(i, t)| targets[..i].iter().all(|p| p.command_id != t.command_id))
}
impl SessionControlJournal {
    /// 独占新文件并同步完整目标清单，成功后才允许启用对应会话。
    /// `control_session` 必须由调用方分配且不与任何客户端或其他控制会话复用。
    pub fn create(
        path: impl AsRef<Path>,
        user: UserId,
        session: OriginSessionId,
        control_session: u64,
        targets: Vec<SessionTarget>,
        max_targets: usize,
    ) -> io::Result<Self> {
        Self::create_with_policy(
            path,
            user,
            session,
            control_session,
            targets,
            max_targets,
            true,
        )
    }

    /// 同步登记会话和断连撤单配置；关闭时仍排空，禁用撤单时不派发撤单。
    pub fn create_with_policy(
        path: impl AsRef<Path>,
        user: UserId,
        session: OriginSessionId,
        control_session: u64,
        targets: Vec<SessionTarget>,
        max_targets: usize,
        cancel_on_disconnect: bool,
    ) -> io::Result<Self> {
        if !valid_targets(&targets, max_targets) {
            return Err(invalid("会话目标无效或超限"));
        }
        let path = path.as_ref();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(HEADER + TARGET_BYTES * targets.len() + 4)
            .map_err(|_| io::Error::other("会话日志内存不足"))?;
        bytes.extend_from_slice(b"SCT1");
        bytes.extend_from_slice(&user.0.to_be_bytes());
        bytes.extend_from_slice(&session.0.to_be_bytes());
        bytes.extend_from_slice(&control_session.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(targets.len())
                .map_err(|_| invalid("目标超限"))?
                .to_be_bytes(),
        );
        bytes.push(u8::from(cancel_on_disconnect));
        for t in &targets {
            bytes.extend_from_slice(&t.market.0.to_be_bytes());
            bytes.extend_from_slice(&t.route_epoch.to_be_bytes());
            bytes.extend_from_slice(&t.command_id.0.to_be_bytes());
        }
        bytes.extend_from_slice(&matching_persistence::crc32c(&bytes).to_be_bytes());
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        lock(&file)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        matching_persistence::sync_directory(parent)?;
        let acknowledged = vec![false; targets.len()];
        Ok(Self {
            file,
            user,
            session,
            control_session,
            cancel_on_disconnect,
            targets,
            acknowledged,
            open: true,
            failed: false,
        })
    }

    /// 校验并独占已有日志；重启时持久关闭任何尚未关闭的旧会话。
    pub fn open(path: impl AsRef<Path>, max_targets: usize) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        lock(&file)?;
        let max = max_targets.min(MAX_TARGETS);
        let max_size = HEADER + TARGET_BYTES * max + 4 + RECORD * (max + 1);
        let size = usize::try_from(file.metadata()?.len()).map_err(|_| invalid("日志过大"))?;
        if size < HEADER + 4 || size > max_size {
            return Err(invalid("日志长度无效"));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| io::Error::other("会话日志内存不足"))?;
        std::io::Read::by_ref(&mut file)
            .take(u64::try_from(max_size + 1).map_err(|_| invalid("日志长度超限"))?)
            .read_to_end(&mut bytes)?;
        if bytes.len() != size || &bytes[..4] != b"SCT1" {
            return Err(invalid("日志头无效"));
        }
        let count =
            usize::try_from(u32_at(&bytes[28..32])?).map_err(|_| invalid("目标数量无效"))?;
        if count == 0 || count > max {
            return Err(invalid("目标数量超限"));
        }
        let cancel_on_disconnect = match bytes[32] {
            0 => false,
            1 => true,
            _ => return Err(invalid("断连撤单配置无效")),
        };
        let end = HEADER + count * TARGET_BYTES;
        if bytes.len() < end + 4
            || matching_persistence::crc32c(&bytes[..end]) != u32_at(&bytes[end..end + 4])?
        {
            return Err(invalid("目标清单损坏"));
        }
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(count)
            .map_err(|_| io::Error::other("目标内存不足"))?;
        for chunk in bytes[HEADER..end].as_chunks::<TARGET_BYTES>().0 {
            targets.push(SessionTarget {
                market: MarketId(u32_at(&chunk[..4])?),
                route_epoch: u64_at(&chunk[4..12])?,
                command_id: CommandId(u128_at(&chunk[12..28])?),
            });
        }
        if !valid_targets(&targets, max) {
            return Err(invalid("目标清单无效"));
        }
        let records = &bytes[end + 4..];
        if records.len() % RECORD != 0 {
            return Err(invalid("日志尾部截断"));
        }
        let mut journal = Self {
            file,
            user: UserId(u64_at(&bytes[4..12])?),
            session: OriginSessionId(u64_at(&bytes[12..20])?),
            control_session: u64_at(&bytes[20..28])?,
            cancel_on_disconnect,
            acknowledged: vec![false; count],
            targets,
            open: true,
            failed: false,
        };
        for record in records.as_chunks::<RECORD>().0 {
            if matching_persistence::crc32c(&record[..5]) != u32_at(&record[5..])? {
                return Err(invalid("日志记录损坏"));
            }
            let index =
                usize::try_from(u32_at(&record[1..5])?).map_err(|_| invalid("日志索引无效"))?;
            match record[0] {
                0 if journal.open && index == 0 => journal.open = false,
                1 if !journal.open
                    && journal.cancel_on_disconnect
                    && index < count
                    && !journal.acknowledged[index] =>
                {
                    journal.acknowledged[index] = true
                }
                _ => return Err(invalid("日志状态转换无效")),
            }
        }
        if journal.open {
            journal.disconnect()?;
        }
        Ok(journal)
    }

    fn append(&mut self, kind: u8, index: u32) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("会话日志已失效"));
        }
        let mut record = [0_u8; RECORD];
        record[0] = kind;
        record[1..5].copy_from_slice(&index.to_be_bytes());
        let crc = matching_persistence::crc32c(&record[..5]);
        record[5..].copy_from_slice(&crc.to_be_bytes());
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|()| self.file.sync_all());
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// 持久关闭会话；调用方须先关闭接入并排空旧会话中所有已准入命令，随后才可派发撤单。
    pub fn disconnect(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("会话日志已失效"));
        }
        if self.open {
            self.append(0, 0)?;
            self.open = false;
        }
        Ok(())
    }
    /// 持久记录已收到持久成功结果的目标；重复确认不追加记录。
    pub fn acknowledge(&mut self, market: MarketId) -> io::Result<()> {
        if self.failed || self.open || !self.cancel_on_disconnect {
            return Err(io::Error::other("会话尚未关闭或日志失效"));
        }
        let i = self
            .targets
            .iter()
            .position(|t| t.market == market)
            .ok_or_else(|| invalid("未知市场"))?;
        if !self.acknowledged[i] {
            self.append(1, u32::try_from(i).map_err(|_| invalid("目标索引超限"))?)?;
            self.acknowledged[i] = true;
        }
        Ok(())
    }
    /// 获取待执行的稳定目标；会话尚开放时为空。
    pub fn pending(&self) -> io::Result<Vec<SessionTarget>> {
        if self.failed {
            return Err(io::Error::other("会话日志已失效"));
        }
        if self.open || !self.cancel_on_disconnect {
            return Ok(Vec::new());
        }
        Ok(self
            .targets
            .iter()
            .zip(&self.acknowledged)
            .filter_map(|(t, done)| (!done).then_some(*t))
            .collect())
    }
    /// 为尚未确认的目标构造稳定的正式分片消息；调用方必须路由到持久入口。
    pub fn command(&self, market: MarketId) -> io::Result<CommandEnvelope> {
        if self.failed || self.open || !self.cancel_on_disconnect {
            return Err(io::Error::other("会话尚未关闭或日志失效"));
        }
        let index = self
            .targets
            .iter()
            .position(|target| target.market == market)
            .ok_or_else(|| invalid("未知市场"))?;
        if self.acknowledged[index] {
            return Err(invalid("目标已确认"));
        }
        let target = self.targets[index];
        Ok(CommandEnvelope {
            command_id: target.command_id,
            session_id: self.control_session,
            client_seq: u64::try_from(index).map_err(|_| invalid("目标索引超限"))?,
            market_id: Some(target.market),
            user_id: None,
            route_epoch: Some(target.route_epoch),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Session {
                    user_id: self.user,
                    session_id: self.session,
                },
            }),
        })
    }
    /// 会话是否仍可由网关接收新命令。
    pub fn is_open(&self) -> bool {
        self.open && !self.failed
    }
    /// 日志绑定的用户。
    pub fn user(&self) -> UserId {
        self.user
    }
    /// 日志绑定的原始接入会话。
    pub fn session(&self) -> OriginSessionId {
        self.session
    }
    /// 返回启用前冻结的完整市场目标。
    pub(crate) fn targets(&self) -> &[SessionTarget] {
        &self.targets
    }
}

/// 派发会话撤单时的错误；超时不代表分片没有执行。
#[derive(Debug)]
pub enum SessionDispatchError {
    /// 未找到目标对应的持久分片或路由已失效。
    Route,
    /// 分片句柄不提供正式业务持久提交。
    NonDurable,
    /// 日志不可用、目标无效或确认同步失败。
    Journal(io::Error),
    /// 有界入口拒绝或已关闭；请求未被接收。
    Ingress(crate::ShardError),
    /// 分片前置检查或身份检查拒绝。
    Formal(crate::FormalError),
    /// 业务执行拒绝或返回的结果与请求不一致。
    Rejected,
    /// 请求可能已执行，须以同一命令身份重试。
    OutcomeUnknown,
}

/// 依照持久目标顺序派发全部未确认市场；部分成功后失败不得回滚已确认目标。
pub fn dispatch_session_cancels(
    journal: &mut SessionControlJournal,
    shards: &[crate::ShardHandle],
    timeout: std::time::Duration,
) -> Result<(), SessionDispatchError> {
    if journal.is_open() {
        return Err(SessionDispatchError::Journal(io::Error::other(
            "会话尚未关闭",
        )));
    }
    let end = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(SessionDispatchError::OutcomeUnknown)?;
    for target in journal.pending().map_err(SessionDispatchError::Journal)? {
        let mut selected = None;
        for shard in shards {
            if shard.has_durable_formal()
                && shard
                    .routes()
                    .map_err(SessionDispatchError::Ingress)?
                    .get(target.market)
                    .is_some_and(|route| {
                        route.shard_id == shard.shard_id()
                            && route.state == crate::RouteState::Active
                            && route.route_epoch >= target.route_epoch
                    })
            {
                selected = Some(shard);
                break;
            }
        }
        let shard = selected.ok_or(SessionDispatchError::Route)?;
        let remaining = end
            .checked_duration_since(std::time::Instant::now())
            .ok_or(SessionDispatchError::OutcomeUnknown)?;
        dispatch_session_cancel(journal, target.market, shard, remaining)?;
    }
    Ok(())
}

/// 投递一个待处理目标，仅在持久分片成功回复后同步确认。
///
/// 超时、失败和拒绝均保留目标。调用方负责会话入口关闭、旧请求排空及目标路由。
pub fn dispatch_session_cancel(
    journal: &mut SessionControlJournal,
    market: MarketId,
    shard: &crate::ShardHandle,
    timeout: std::time::Duration,
) -> Result<matching_domain::ShardMassCancelResult, SessionDispatchError> {
    if !shard.has_durable_formal() {
        return Err(SessionDispatchError::NonDurable);
    }
    let mut command = journal
        .command(market)
        .map_err(SessionDispatchError::Journal)?;
    let routes = shard.routes().map_err(SessionDispatchError::Ingress)?;
    let route = routes.get(market).ok_or(SessionDispatchError::Route)?;
    if route.shard_id != shard.shard_id()
        || route.state != crate::RouteState::Active
        || command
            .route_epoch
            .is_none_or(|epoch| route.route_epoch < epoch)
    {
        return Err(SessionDispatchError::Route);
    }
    command.route_epoch = Some(route.route_epoch);
    let id = command.command_id;
    // 恢复后旧会话的已准入消息也必须先于撤单执行。
    let deadline = std::time::Instant::now()
        .checked_add(timeout)
        .ok_or(SessionDispatchError::OutcomeUnknown)?;
    let barrier = shard.barrier(id.0).map_err(SessionDispatchError::Ingress)?;
    let remaining = deadline
        .checked_duration_since(std::time::Instant::now())
        .ok_or(SessionDispatchError::OutcomeUnknown)?;
    let (observed, _) = barrier
        .recv_timeout(remaining)
        .map_err(|_| SessionDispatchError::OutcomeUnknown)?;
    if observed != id.0 {
        return Err(SessionDispatchError::Rejected);
    }
    let reply = shard
        .submit_command(command)
        .map_err(SessionDispatchError::Ingress)?;
    let remaining = deadline
        .checked_duration_since(std::time::Instant::now())
        .ok_or(SessionDispatchError::OutcomeUnknown)?;
    let result = reply
        .recv_timeout(remaining)
        .map_err(|_| SessionDispatchError::OutcomeUnknown)?
        .map_err(SessionDispatchError::Formal)?;
    if result.command_id != id {
        return Err(SessionDispatchError::Rejected);
    }
    let crate::FormalOutcome::MassCancel(summary) = result.outcome else {
        return Err(SessionDispatchError::Rejected);
    };
    journal
        .acknowledge(market)
        .map_err(SessionDispatchError::Journal)?;
    Ok(summary)
}
