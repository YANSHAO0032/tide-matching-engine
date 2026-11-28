//! 本地 WAL 与外围复制线程之间的有界同步交接。
use crate::replication::{send_replication_batch, validate_limit};
use matching_persistence::{DurableAppender, ReplicaDurableAck, WalRecord};
use std::io::{self, Read, Write};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// 已由外围认证的复制连接，可为底层读写设置超时。
pub trait ReplicationStream: Read + Write + Send + 'static {
    /// 设置底层读写超时；失败时不能启动复制线程。
    fn set_io_timeout(&mut self, timeout: Duration) -> io::Result<()>;
}

/// 在备节点持久确认后，以当前写权推进权威连续前缀。
///
/// 实现者须保证比较写权与推进前缀属于同一原子操作；错误或结果未知不得视为成功。
pub trait ReplicaCommitFence: Send + 'static {
    /// 在分配分片序号前核验已认证主写者的写权。提交复制前缀时仍须重新比较写权。
    fn verify_control_owner(&mut self, _epoch: u64, _deadline: Instant) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "复制权威未提供控制命令凭据",
        ))
    }
    /// 在发送备节点之前持久登记完整复制意图；它不是提交确认。
    fn prepare(&mut self, records: &[WalRecord], deadline: Instant) -> io::Result<()>;
    /// 比较写权并提交一条连续记录，返回其已确认序号。
    /// 超过截止时间不得发起新请求；已发出的请求可能在超时后提交。
    fn commit(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<u64>;
    /// 在已提交水位登记可信业务摘要。
    fn checkpoint_digest(
        &mut self,
        _seq: u64,
        _state: [u8; 32],
        _events: [u8; 32],
        _deadline: Instant,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "权威不支持摘要检查点",
        ))
    }
    /// 返回需周期续持的间隔；无到期时间的权威可返回空值。
    fn renewal_interval(&self) -> Option<Duration> {
        None
    }
    /// 刷新写权；不需要续持的权威可使用默认实现。
    fn renew(&mut self) -> io::Result<()> {
        Ok(())
    }
    /// 在剩余总期限内续持写权；已发出的请求可能在超时后完成。
    fn renew_until(&mut self, deadline: Instant) -> io::Result<()> {
        remaining(deadline)?;
        self.renew()
    }
}

impl<F> ReplicaCommitFence for F
where
    F: FnMut(&WalRecord, Instant) -> io::Result<u64> + Send + 'static,
{
    fn prepare(&mut self, _: &[WalRecord], deadline: Instant) -> io::Result<()> {
        remaining(deadline)?;
        Ok(())
    }
    fn commit(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<u64> {
        self(record, deadline)
    }
}

impl ReplicaCommitFence for crate::FencedLogWriter {
    fn verify_control_owner(&mut self, epoch: u64, deadline: Instant) -> io::Result<()> {
        if self.epoch() != epoch {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "主写者纪元不一致",
            ));
        }
        self.verify_bound_lease(deadline)
    }
    fn prepare(&mut self, records: &[WalRecord], deadline: Instant) -> io::Result<()> {
        for record in records {
            self.prepare_until(record, deadline)?;
        }
        Ok(())
    }
    fn commit(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<u64> {
        self.commit_prepared_until(record, deadline)
    }
    fn checkpoint_digest(
        &mut self,
        seq: u64,
        state: [u8; 32],
        events: [u8; 32],
        deadline: Instant,
    ) -> io::Result<()> {
        crate::FencedLogWriter::checkpoint_digest(self, seq, state, events, deadline).map(|_| ())
    }
    fn renewal_interval(&self) -> Option<Duration> {
        Some(crate::FencedLogWriter::renewal_interval(self))
    }
    fn renew(&mut self) -> io::Result<()> {
        self.keep_alive()
    }
    fn renew_until(&mut self, deadline: Instant) -> io::Result<()> {
        self.keep_alive_until(deadline)
    }
}

impl ReplicationStream for std::net::TcpStream {
    fn set_io_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(timeout))?;
        self.set_write_timeout(Some(timeout))
    }
}

/// 交给外围线程的写权查验、复制批次或摘要登记请求。
enum Request {
    /// 在序号分配前查验主写者的实时凭据。
    Verify {
        /// 子命令要求的主节点纪元。
        epoch: u64,
        /// 读取控制存储的截止时间。
        deadline: Instant,
        /// 单次核验的有界回复。
        reply: SyncSender<io::Result<()>>,
    },
    /// 顺序复制完整有界 WAL 批次。
    Batch {
        /// 已在主节点同步的连续 WAL 记录。
        records: Vec<WalRecord>,
        /// 本批次复制及权威提交共用的截止时间。
        deadline: Instant,
        /// 接收备节点持久确认或复制错误的单次回复端。
        reply: SyncSender<io::Result<ReplicaDurableAck>>,
    },
    /// 在当前提交顺序点登记状态和事件摘要。
    Digest {
        /// 待登记摘要对应的已提交分片序号。
        seq: u64,
        /// 该序号处的业务状态摘要。
        state: [u8; 32],
        /// 从创世起累计到该序号的有序事件摘要。
        events: [u8; 32],
        /// 摘要登记操作的截止时间。
        deadline: Instant,
        /// 接收登记结果的单次回复端。
        reply: SyncSender<io::Result<()>>,
    },
}

/// 每次底层读写都使用剩余总期限，超期后不再启动新操作。
pub(crate) struct DeadlineStream<'a, S> {
    /// 已认证的底层复制流。
    pub(crate) inner: &'a mut S,
    /// 当前批次的截止时间。
    pub(crate) deadline: Instant,
}
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "复制批次期限已过"))
}
impl<S: ReplicationStream> Read for DeadlineStream<'_, S> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.set_io_timeout(remaining(self.deadline)?)?;
        let count = self.inner.read(bytes)?;
        remaining(self.deadline)?;
        Ok(count)
    }
}
impl<S: ReplicationStream> Write for DeadlineStream<'_, S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.set_io_timeout(remaining(self.deadline)?)?;
        let count = self.inner.write(bytes)?;
        remaining(self.deadline)?;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.set_io_timeout(remaining(self.deadline)?)?;
        self.inner.flush()?;
        remaining(self.deadline)?;
        Ok(())
    }
}

/// 复制提交链的本地健康观测；不证明远端租约必然有效。
#[derive(Clone)]
pub struct ReplicaHealth(Arc<AtomicBool>);
impl ReplicaHealth {
    /// 返回复制线程是否仍处于本地可用状态。
    pub fn ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// 保证复制线程退出或异常展开时发布失效状态。
struct WorkerHealth(Arc<AtomicBool>);
impl Drop for WorkerHealth {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// 本地同步后由独立线程发送备节点并取得确认的写入器。
///
/// 确认须经过调用方提供的写权事务；本写入器不独立执行接任恢复或备节点尾部协调，
/// 这些步骤须在启用复制前完成。调用方必须提供已认证且读写均有期限的流，
/// 并在任何错误后停止使用实例。
pub struct ReplicatedAppender<A> {
    /// 本机独占的 WAL 和清单写入器。
    local: A,
    /// 尚待同步与复制的有界记录。
    pending: Vec<WalRecord>,
    /// 当前复制帧包含的字节数。
    pending_bytes: usize,
    /// 单帧允许的最大字节数。
    max_frame: usize,
    /// 等待备节点回复的期限。
    timeout: Duration,
    /// 容量为一的外围复制请求入口。
    sender: Option<SyncSender<Request>>,
    /// 独占网络流的外围线程。
    worker: Option<JoinHandle<()>>,
    /// 最近一次同步得到的备节点确认。
    ack: Option<ReplicaDurableAck>,
    /// 同步失败或结果未知后永久停止。
    halted: bool,
    /// 对外只读的复制链本地健康状态。
    health: ReplicaHealth,
}

impl<A: DurableAppender> ReplicatedAppender<A> {
    /// 使用证书绑定写者和已认证 TLS 流创建复制线程。
    ///
    /// 调用方仍须先完成接任协调、主节点恢复和备节点恢复确认。
    pub fn new_authenticated(
        local: A,
        stream: crate::TlsReplicaStream,
        mut fence: crate::FencedLogWriter,
        max_frame: usize,
        timeout: Duration,
    ) -> io::Result<Self> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .filter(|_| !timeout.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "复制等待期限无效"))?;
        fence.verify_bound_lease(deadline)?;
        fence.initialize_genesis_if_empty(deadline)?;
        Self::new(local, stream, fence, max_frame, timeout)
    }
    /// 创建外围线程；流的身份认证及底层读写超时由调用方事先配置。
    pub fn new<S: ReplicationStream, F: ReplicaCommitFence>(
        local: A,
        mut stream: S,
        mut fence: F,
        max_frame: usize,
        timeout: Duration,
    ) -> io::Result<Self> {
        validate_limit(max_frame)?;
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制等待期限为零",
            ));
        }
        stream.set_io_timeout(timeout)?;
        let renewal_interval = fence.renewal_interval();
        if renewal_interval.is_some_and(|interval| interval.is_zero()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "写权续租间隔为零",
            ));
        }
        fence.renew()?;
        let (sender, receiver) = mpsc::sync_channel::<Request>(1);
        let health = ReplicaHealth(Arc::new(AtomicBool::new(true)));
        let worker_health = Arc::clone(&health.0);
        let worker = std::thread::Builder::new()
            .name("wal-replication".into())
            .spawn(move || {
                let _worker_health = WorkerHealth(worker_health);
                loop {
                    let request = if let Some(interval) = renewal_interval {
                        match receiver.recv_timeout(interval) {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => {
                                if fence.renew().is_err() {
                                    break;
                                }
                                continue;
                            }
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    } else {
                        match receiver.recv() {
                            Ok(request) => request,
                            Err(_) => break,
                        }
                    };
                    let failed = match request {
                        Request::Verify {
                            epoch,
                            deadline,
                            reply,
                        } => {
                            // 连续核验会一直占用收件循环，须在同一期限内续持写权。
                            let result = remaining(deadline)
                                .and_then(|_| fence.verify_control_owner(epoch, deadline))
                                .and_then(|()| fence.renew_until(deadline));
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            failed
                        }
                        Request::Batch {
                            records,
                            deadline,
                            reply,
                        } => {
                            let mut bounded = DeadlineStream {
                                inner: &mut stream,
                                deadline,
                            };
                            let result = fence
                                .prepare(&records, deadline)
                                .and_then(|()| {
                                    send_replication_batch(&mut bounded, &records, max_frame)
                                })
                                .and_then(|ack| {
                                    for record in &records {
                                        remaining(deadline)?;
                                        if fence.commit(record, deadline)? != record.shard_seq {
                                            return Err(io::Error::other("权威提交水位不一致"));
                                        }
                                    }
                                    remaining(deadline)?;
                                    fence.renew_until(deadline)?;
                                    remaining(deadline)?;
                                    Ok(ack)
                                });
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            failed
                        }
                        Request::Digest {
                            seq,
                            state,
                            events,
                            deadline,
                            reply,
                        } => {
                            let result = remaining(deadline)
                                .and_then(|_| fence.checkpoint_digest(seq, state, events, deadline))
                                .and_then(|()| remaining(deadline).map(|_| ()));
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            failed
                        }
                    };
                    if failed {
                        break;
                    }
                }
            })?;
        Ok(Self {
            local,
            pending: Vec::new(),
            pending_bytes: 16,
            max_frame,
            timeout,
            sender: Some(sender),
            worker: Some(worker),
            ack: None,
            halted: false,
            health,
        })
    }
    /// 获取只读的本地复制健康句柄。
    pub fn health_handle(&self) -> ReplicaHealth {
        self.health.clone()
    }
    fn record_size(record: &WalRecord) -> io::Result<usize> {
        if record.flags != 0 || record.shard_seq == 0 || record.payload.len() > 16 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制 WAL 记录无效",
            ));
        }
        68_usize
            .checked_add(record.payload.len())
            .ok_or_else(|| io::Error::other("复制记录长度耗尽"))
    }
}

impl<A: DurableAppender> DurableAppender for ReplicatedAppender<A> {
    fn verify_control_owner(&mut self, epoch: u64) -> io::Result<()> {
        if self.halted || !self.health.ready() {
            return Err(io::Error::other("复制写入器已停止"));
        }
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "写权核验期限越界"))?;
        let (reply, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| io::Error::other("复制线程不可用"))?
            .try_send(Request::Verify {
                epoch,
                deadline,
                reply,
            })
            .map_err(|e| match e {
                TrySendError::Full(_) => io::Error::new(io::ErrorKind::WouldBlock, "复制队列已满"),
                TrySendError::Disconnected(_) => io::Error::other("复制线程已停止"),
            })?;
        receiver
            .recv_timeout(remaining(deadline)?)
            .map_err(|e| match e {
                mpsc::RecvTimeoutError::Timeout => {
                    io::Error::new(io::ErrorKind::TimedOut, "写权核验超时")
                }
                mpsc::RecvTimeoutError::Disconnected => io::Error::other("写权核验线程停止"),
            })??;
        remaining(deadline).map(|_| ())
    }
    fn would_rotate(&self, record: &WalRecord) -> io::Result<bool> {
        if self.halted || !self.health.ready() {
            return Err(io::Error::other("复制写入器已停止"));
        }
        let size = Self::record_size(record)?;
        let total = self
            .pending_bytes
            .checked_add(size)
            .ok_or_else(|| io::Error::other("复制批次长度耗尽"))?;
        if total > self.max_frame {
            if self.pending.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "单条记录超过复制帧容量",
                ));
            }
            return Ok(true);
        }
        self.local.would_rotate(record)
    }
    fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        if self.halted || !self.health.ready() {
            return Err(io::Error::other("复制写入器已停止"));
        }
        let size = Self::record_size(record)?;
        let total = self
            .pending_bytes
            .checked_add(size)
            .ok_or_else(|| io::Error::other("复制批次长度耗尽"))?;
        if total > self.max_frame {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "复制批次超限"));
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(record.payload.len())
            .map_err(|_| io::Error::other("复制载荷内存不足"))?;
        payload.extend_from_slice(&record.payload);
        self.pending
            .try_reserve(1)
            .map_err(|_| io::Error::other("复制批次内存不足"))?;
        let copy = WalRecord {
            cluster_id: record.cluster_id,
            shard_id: record.shard_id,
            flags: record.flags,
            epoch: record.epoch,
            shard_seq: record.shard_seq,
            engine_time: record.engine_time,
            command_id: record.command_id,
            payload,
        };
        if let Err(error) = self.local.append(record) {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
            return Err(error);
        }
        self.pending.push(copy);
        self.pending_bytes = total;
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        if self.halted || !self.health.ready() || self.pending.is_empty() || self.ack.is_some() {
            return Err(io::Error::other("复制写入器状态无效"));
        }
        if let Err(error) = self.local.sync() {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
            return Err(error);
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        let Some(deadline) = Instant::now().checked_add(self.timeout) else {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制截止时间越界",
            ));
        };
        let records = std::mem::take(&mut self.pending);
        self.pending_bytes = 16;
        let request = Request::Batch {
            records,
            reply,
            deadline,
        };
        let Some(sender) = self.sender.as_ref() else {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
            return Err(io::Error::other("复制线程不可用"));
        };
        if let Err(error) = sender.try_send(request) {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
            return Err(match error {
                TrySendError::Full(_) => io::Error::new(io::ErrorKind::WouldBlock, "复制队列已满"),
                TrySendError::Disconnected(_) => io::Error::other("复制线程已停止"),
            });
        }
        let wait = match remaining(deadline) {
            Ok(wait) => wait,
            Err(error) => {
                self.halted = true;
                self.health.0.store(false, Ordering::Release);
                return Err(error);
            }
        };
        match receiver.recv_timeout(wait) {
            Ok(Ok(ack)) => {
                if let Err(error) = remaining(deadline) {
                    self.halted = true;
                    self.health.0.store(false, Ordering::Release);
                    return Err(error);
                }
                self.ack = Some(ack);
                Ok(())
            }
            Ok(Err(error)) => {
                self.halted = true;
                self.health.0.store(false, Ordering::Release);
                Err(error)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.halted = true;
                self.health.0.store(false, Ordering::Release);
                Err(io::Error::new(io::ErrorKind::TimedOut, "复制确认结果未知"))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.halted = true;
                self.health.0.store(false, Ordering::Release);
                Err(io::Error::other("复制线程停止，确认结果未知"))
            }
        }
    }
    fn take_replica_ack(&mut self) -> Option<ReplicaDurableAck> {
        self.ack.take()
    }
    fn replication_ready(&self) -> Option<bool> {
        Some(self.health.ready())
    }
    fn digest_checkpoint_required(&self) -> bool {
        true
    }
    fn checkpoint_digest(&mut self, seq: u64, state: [u8; 32], events: [u8; 32]) -> io::Result<()> {
        if self.halted
            || !self.health.ready()
            || !self.pending.is_empty()
            || self.ack.is_some()
            || seq == 0
        {
            return Err(io::Error::other("复制写入器未处于摘要登记顺序点"));
        }
        let result = (|| {
            let deadline = Instant::now()
                .checked_add(self.timeout)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "摘要登记期限越界"))?;
            let (reply, receiver) = mpsc::sync_channel(1);
            let sender = self
                .sender
                .as_ref()
                .ok_or_else(|| io::Error::other("复制线程不可用"))?;
            sender
                .try_send(Request::Digest {
                    seq,
                    state,
                    events,
                    deadline,
                    reply,
                })
                .map_err(|error| match error {
                    TrySendError::Full(_) => {
                        io::Error::new(io::ErrorKind::WouldBlock, "复制队列已满")
                    }
                    TrySendError::Disconnected(_) => io::Error::other("复制线程已停止"),
                })?;
            let result =
                receiver
                    .recv_timeout(remaining(deadline)?)
                    .map_err(|error| match error {
                        mpsc::RecvTimeoutError::Timeout => {
                            io::Error::new(io::ErrorKind::TimedOut, "摘要登记结果未知")
                        }
                        mpsc::RecvTimeoutError::Disconnected => {
                            io::Error::other("复制线程停止，摘要登记结果未知")
                        }
                    })?;
            result?;
            remaining(deadline)?;
            Ok(())
        })();
        if result.is_err() {
            self.halted = true;
            self.health.0.store(false, Ordering::Release);
        }
        result
    }
    fn commit_snapshot(&mut self, snapshot_id: u64, snapshot_seq: u64) -> io::Result<()> {
        if self.halted || !self.pending.is_empty() || self.ack.is_some() {
            return Err(io::Error::other("复制写入器尚未确认前缀"));
        }
        self.local.commit_snapshot(snapshot_id, snapshot_seq)
    }
}

impl<A> Drop for ReplicatedAppender<A> {
    fn drop(&mut self) {
        self.health.0.store(false, Ordering::Release);
        drop(self.sender.take());
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            eprintln!("复制线程异常退出");
        }
    }
}
