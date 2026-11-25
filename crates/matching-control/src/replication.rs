//! 在已认证、有期限的外部流上交换有界 WAL 批次。
use matching_persistence::{
    DurableAppender, FencedStandbyReplica, ReplicaDurableAck, StandbyReplica, WalRecord,
};
use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

/// 单条最大 WAL 记录、长度与帧字段所需空间。
const MIN_FRAME: usize = 16 * 1024 * 1024 + 84;
/// 单个复制帧的最大空间；较大的批次须按记录边界拆分。
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// 在调用方完成主节点恢复后，请求已认证备节点核验并恢复复制。
pub fn send_replica_resume(
    stream: &mut impl crate::ReplicationStream,
    identity: crate::ReplicaWriterIdentity,
    durable: u64,
    deadline: Instant,
) -> io::Result<()> {
    if identity.epoch == 0 || identity.lease_id <= 0 {
        return Err(invalid("写者身份无效"));
    }
    let request = resume_frame(b"RRC1", identity, durable);
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    stream.write_all(&request)?;
    stream.flush()?;
    let mut response = [0_u8; 36];
    stream.read_exact(&mut response)?;
    if response != resume_frame(b"RRA1", identity, durable) {
        return Err(invalid("备节点恢复确认无效"));
    }
    Ok(())
}

/// 校验认证连接、备节点水位及权威记录，然后确认恢复复制。
pub fn receive_replica_resume(
    stream: &mut impl crate::ReplicationStream,
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<
        matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
    >,
    authenticated: crate::ReplicaWriterIdentity,
    snapshot_seq: u64,
    deadline: Instant,
) -> io::Result<u64> {
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    let mut request = [0_u8; 36];
    stream.read_exact(&mut request)?;
    let durable = u64_at(&request[24..32])?;
    if request != resume_frame(b"RRC1", authenticated, durable) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "恢复请求身份或校验无效",
        ));
    }
    log.verify_replica_writer(authenticated, deadline)?;
    standby.suspend();
    let records = standby.fenced_records(snapshot_seq)?;
    if records
        .last()
        .map_or(snapshot_seq, |record| record.shard_seq)
        != durable
    {
        return Err(invalid("恢复请求水位不一致"));
    }
    let confirmed =
        confirm_authoritative_replica(log, standby, authenticated, snapshot_seq, deadline)?;
    if let Err(error) = stream
        .write_all(&resume_frame(b"RRA1", authenticated, confirmed))
        .and_then(|()| stream.flush())
    {
        standby.suspend();
        return Err(error);
    }
    Ok(confirmed)
}

/// 使用实际 TLS 对端证书验证租约绑定后处理备节点恢复确认。
///
/// 调用方须使用双向证书验证配置，并在发送请求前完成主节点恢复。
pub fn receive_authenticated_replica_resume(
    stream: &mut crate::TlsReplicaStream,
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<
        matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
    >,
    identity: crate::ReplicaWriterIdentity,
    snapshot_seq: u64,
    deadline: Instant,
) -> io::Result<u64> {
    log.verify_bound_replica_writer(identity, stream.peer_certificate(), deadline)?;
    receive_replica_resume(stream, log, standby, identity, snapshot_seq, deadline)
}

/// 生成绑定写者与持久水位的固定长度帧。
fn resume_frame(magic: &[u8; 4], identity: crate::ReplicaWriterIdentity, durable: u64) -> [u8; 36] {
    let mut frame = [0_u8; 36];
    frame[..4].copy_from_slice(magic);
    frame[4..6].copy_from_slice(&identity.cluster_id.to_be_bytes());
    frame[6..8].copy_from_slice(&identity.shard_id.to_be_bytes());
    frame[8..16].copy_from_slice(&identity.epoch.to_be_bytes());
    frame[16..24].copy_from_slice(&identity.lease_id.to_be_bytes());
    frame[24..32].copy_from_slice(&durable.to_be_bytes());
    let crc = matching_persistence::crc32c(&frame[..32]);
    frame[32..].copy_from_slice(&crc.to_be_bytes());
    frame
}

/// 已认证备节点栅栏给出的前缀边界。
#[derive(Clone, Copy, Debug)]
pub struct ReplicaPrefixBoundary {
    /// 集群身份。
    pub cluster_id: u16,
    /// 分片身份。
    pub shard_id: u16,
    /// 调用方已验证的快照水位。
    pub snapshot_seq: u64,
    /// 栅栏确认的备节点持久水位。
    pub durable_seq: u64,
    /// 当前写者纪元，历史记录必须早于此纪元。
    pub writer_epoch: u64,
}

/// 在同一备节点独占顺序点安装栅栏并发送真实持久前缀。
///
/// 调用方须认证连接、验证快照和独占 WAL 目录；出错后关闭连接并重新协调。
/// 发送成功不开放复制，调用方仍须完成权威前缀协调。
pub fn serve_fenced_prefix(
    stream: &mut impl crate::ReplicationStream,
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<
        matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
    >,
    authenticated: crate::ReplicaWriterIdentity,
    snapshot_seq: u64,
    deadline: Instant,
) -> io::Result<u64> {
    let durable = receive_replica_fence(stream, log, standby, authenticated, deadline)?;
    let records = standby.fenced_records(snapshot_seq)?;
    log.verify_replica_writer(authenticated, deadline)?;
    let boundary = ReplicaPrefixBoundary {
        cluster_id: authenticated.cluster_id,
        shard_id: authenticated.shard_id,
        snapshot_seq,
        durable_seq: durable,
        writer_epoch: authenticated.epoch,
    };
    send_replica_prefix(stream, boundary, &records, deadline)?;
    Ok(durable)
}

/// 使用真实 TLS 对端证书验证租约绑定后服务备节点持久前缀。
///
/// 调用方仍须使用强制双向认证的配置，并保持备节点单写者及快照校验。
pub fn serve_authenticated_fenced_prefix(
    stream: &mut crate::TlsReplicaStream,
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<
        matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
    >,
    identity: crate::ReplicaWriterIdentity,
    snapshot_seq: u64,
    deadline: Instant,
) -> io::Result<u64> {
    log.verify_bound_replica_writer(identity, stream.peer_certificate(), deadline)?;
    serve_fenced_prefix(stream, log, standby, identity, snapshot_seq, deadline)
}

/// 对备节点实际持久前缀逐条核验权威记录后开放复制。
///
/// 调用方须认证请求者、验证快照并确认主节点恢复；失败后必须保持接任流程关闭。
pub fn confirm_authoritative_replica(
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<
        matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
    >,
    authenticated: crate::ReplicaWriterIdentity,
    snapshot_seq: u64,
    deadline: Instant,
) -> io::Result<u64> {
    log.verify_replica_writer(authenticated, deadline)?;
    let records = standby.fenced_records(snapshot_seq)?;
    let durable = records
        .last()
        .map_or(snapshot_seq, |record| record.shard_seq);
    if log.committed_until(deadline)? != durable {
        return Err(invalid("备节点与权威水位不一致"));
    }
    for record in &records {
        fence_deadline(deadline)?;
        if record.cluster_id != authenticated.cluster_id
            || record.shard_id != authenticated.shard_id
            || log.read_until(record.shard_seq, deadline)?.as_ref() != Some(record)
        {
            return Err(invalid("备节点与权威记录不一致"));
        }
    }
    standby.confirm_reconciled(durable, |cluster, shard, epoch| {
        if (cluster, shard, epoch)
            != (
                authenticated.cluster_id,
                authenticated.shard_id,
                authenticated.epoch,
            )
        {
            return Err(invalid("备节点与写者身份不一致"));
        }
        log.verify_replica_writer(authenticated, deadline)
    })?;
    Ok(durable)
}

/// 按记录帧发送已经从独占栅栏读取的备节点持久前缀。
pub fn send_replica_prefix(
    stream: &mut impl crate::ReplicationStream,
    boundary: ReplicaPrefixBoundary,
    records: &[WalRecord],
    deadline: Instant,
) -> io::Result<()> {
    let mut seq = boundary.snapshot_seq;
    for record in records {
        validate_prefix_record(boundary, seq, record)?;
        seq = record.shard_seq;
    }
    if seq != boundary.durable_seq || boundary.writer_epoch == 0 {
        return Err(invalid("备节点前缀不完整"));
    }
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    for record in records {
        let bytes = record.encode().map_err(|_| invalid("备节点记录编码无效"))?;
        let len = u32::try_from(bytes.len()).map_err(|_| invalid("备节点记录长度越界"))?;
        stream.write_all(b"RFR1")?;
        stream.write_all(&len.to_be_bytes())?;
        stream.write_all(&bytes)?;
        stream.write_all(&matching_persistence::crc32c(&bytes).to_be_bytes())?;
    }
    stream.write_all(b"RFT1")?;
    stream.write_all(&boundary.durable_seq.to_be_bytes())?;
    stream.flush()
}

/// 逐条接收并检查前缀；回调不得在完整前缀和权威意图核对前提交记录。
pub fn read_replica_prefix(
    stream: &mut impl crate::ReplicationStream,
    boundary: ReplicaPrefixBoundary,
    deadline: Instant,
    mut visit: impl FnMut(WalRecord) -> io::Result<()>,
) -> io::Result<()> {
    if boundary.writer_epoch == 0 || boundary.snapshot_seq > boundary.durable_seq {
        return Err(invalid("备节点前缀边界无效"));
    }
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    let mut seq = boundary.snapshot_seq;
    while seq < boundary.durable_seq {
        let mut header = [0_u8; 8];
        stream.read_exact(&mut header)?;
        if &header[..4] != b"RFR1" {
            return Err(invalid("备节点记录帧无效"));
        }
        let len = usize::try_from(u32_at(&header[4..])?).map_err(|_| invalid("记录长度越界"))?;
        if !(64..=16 * 1024 * 1024 + 64).contains(&len) {
            return Err(invalid("备节点记录长度无效"));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| io::Error::other("备节点记录内存不足"))?;
        bytes.resize(len, 0);
        stream.read_exact(&mut bytes)?;
        let mut checksum = [0_u8; 4];
        stream.read_exact(&mut checksum)?;
        if matching_persistence::crc32c(&bytes).to_be_bytes() != checksum {
            return Err(invalid("备节点记录校验失败"));
        }
        let (record, used) = WalRecord::decode(&bytes).map_err(|_| invalid("备节点 WAL 损坏"))?;
        if used != len {
            return Err(invalid("备节点 WAL 长度不一致"));
        }
        validate_prefix_record(boundary, seq, &record)?;
        seq = record.shard_seq;
        visit(record)?;
    }
    let mut footer = [0_u8; 12];
    stream.read_exact(&mut footer)?;
    if &footer[..4] != b"RFT1" || u64_at(&footer[4..])? != boundary.durable_seq {
        return Err(invalid("备节点前缀结束标识无效"));
    }
    Ok(())
}

/// 检查记录属于指定分片、纪元和连续水位。
fn validate_prefix_record(
    boundary: ReplicaPrefixBoundary,
    previous: u64,
    record: &WalRecord,
) -> io::Result<()> {
    if record.cluster_id != boundary.cluster_id
        || record.shard_id != boundary.shard_id
        || previous.checked_add(1) != Some(record.shard_seq)
        || record.shard_seq > boundary.durable_seq
        || record.epoch == 0
        || record.epoch >= boundary.writer_epoch
    {
        return Err(invalid("备节点记录身份、纪元或序号无效"));
    }
    Ok(())
}

/// 通过已认证并配置读写期限的流请求备节点持久栅栏。
///
/// 连接失败或响应丢失表示结果未知；调用方须关闭连接并重新协调。
pub fn send_replica_fence(
    stream: &mut impl crate::ReplicationStream,
    identity: crate::ReplicaWriterIdentity,
    deadline: Instant,
) -> io::Result<u64> {
    fence_deadline(deadline)?;
    if identity.epoch == 0 || identity.lease_id <= 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "写者身份无效"));
    }
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    let mut request = [0_u8; 28];
    request[..4].copy_from_slice(b"RFE1");
    request[4..6].copy_from_slice(&identity.cluster_id.to_be_bytes());
    request[6..8].copy_from_slice(&identity.shard_id.to_be_bytes());
    request[8..16].copy_from_slice(&identity.epoch.to_be_bytes());
    request[16..24].copy_from_slice(&identity.lease_id.to_be_bytes());
    let checksum = matching_persistence::crc32c(&request[..24]);
    request[24..].copy_from_slice(&checksum.to_be_bytes());
    stream.write_all(&request)?;
    stream.flush()?;
    fence_deadline(deadline)?;
    let mut response = [0_u8; 36];
    stream.read_exact(&mut response)?;
    fence_deadline(deadline)?;
    if &response[..4] != b"RFA1"
        || response[4..24] != request[4..24]
        || matching_persistence::crc32c(&response[..32]).to_be_bytes() != response[32..]
    {
        return Err(invalid("备节点栅栏确认身份或校验值无效"));
    }
    u64_at(&response[24..32])
}

/// 在备节点单写者顺序点处理已认证连接上的持久栅栏请求。
///
/// `authenticated` 须由调用方认证并绑定当前连接；失败后关闭连接，且不得并发借用备节点。
pub fn receive_replica_fence<A: DurableAppender>(
    stream: &mut impl crate::ReplicationStream,
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<A>,
    authenticated: crate::ReplicaWriterIdentity,
    deadline: Instant,
) -> io::Result<u64> {
    fence_deadline(deadline)?;
    let mut stream = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    let mut request = [0_u8; 28];
    stream.read_exact(&mut request)?;
    fence_deadline(deadline)?;
    if &request[..4] != b"RFE1"
        || matching_persistence::crc32c(&request[..24]).to_be_bytes() != request[24..]
    {
        return Err(invalid("备节点栅栏请求损坏"));
    }
    let identity = crate::ReplicaWriterIdentity {
        cluster_id: u16::from_be_bytes([request[4], request[5]]),
        shard_id: u16::from_be_bytes([request[6], request[7]]),
        epoch: u64_at(&request[8..16])?,
        lease_id: i64::from_be_bytes(
            request[16..24]
                .try_into()
                .map_err(|_| invalid("租约编号无效"))?,
        ),
    };
    if identity != authenticated {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "栅栏请求身份与连接不符",
        ));
    }
    let durable = activate_replica_writer(log, standby, identity, deadline)?;
    fence_deadline(deadline)?;
    let mut response = [0_u8; 36];
    response[..4].copy_from_slice(b"RFA1");
    response[4..24].copy_from_slice(&request[4..24]);
    response[24..32].copy_from_slice(&durable.to_be_bytes());
    let checksum = matching_persistence::crc32c(&response[..32]);
    response[32..].copy_from_slice(&checksum.to_be_bytes());
    stream.write_all(&response)?;
    stream.flush()?;
    fence_deadline(deadline)?;
    Ok(durable)
}

/// 在每次状态变化或新 I/O 前检查本次请求的总期限。
fn fence_deadline(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "备节点栅栏期限已过",
        ))
    } else {
        Ok(())
    }
}

/// 在备节点单写者顺序点核验权威并同步安装写者纪元，返回栅栏时持久水位。
///
/// 连接和请求身份须由调用方认证；返回的水位尚须与权威日志协调后才能接收业务流量。
pub fn activate_replica_writer<A: DurableAppender>(
    log: &crate::FencedLog,
    standby: &mut FencedStandbyReplica<A>,
    identity: crate::ReplicaWriterIdentity,
    deadline: Instant,
) -> io::Result<u64> {
    standby.activate(identity.epoch, |cluster, shard, epoch| {
        if (cluster, shard, epoch) != (identity.cluster_id, identity.shard_id, identity.epoch) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "备节点和写者身份不一致",
            ));
        }
        log.verify_replica_writer(identity, deadline)
    })
}

/// 持有备节点单写者的复制批次接收端。
pub trait ReplicaBatchSink {
    /// 同步一批经过意图验证的记录。
    fn append_verified(
        &mut self,
        records: &[WalRecord],
        now: Instant,
    ) -> io::Result<ReplicaDurableAck>;
}

impl<A: DurableAppender> ReplicaBatchSink for StandbyReplica<A> {
    fn append_verified(
        &mut self,
        records: &[WalRecord],
        now: Instant,
    ) -> io::Result<ReplicaDurableAck> {
        self.append_batch(records, now)
    }
}

impl<A: DurableAppender> ReplicaBatchSink for FencedStandbyReplica<A> {
    fn append_verified(
        &mut self,
        records: &[WalRecord],
        now: Instant,
    ) -> io::Result<ReplicaDurableAck> {
        self.append_batch(records, now)
    }
}

/// 核对一条复制记录是否对应可信权威中的完整意图。
pub trait ReplicaIntentVerifier {
    /// 在截止时间内核对记录；失败或结果未知不得写入备节点 WAL。
    fn verify(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<()>;
}

/// 在双向 TLS 连接上核验对端证书、写者租约和逐条复制意图，再同步备节点 WAL。
///
/// 调用方须使用强制双向证书验证的配置，并在失败后关闭连接。
pub fn receive_authenticated_replication_batch(
    stream: &mut crate::TlsReplicaStream,
    log: &crate::FencedLog,
    standby: &mut impl ReplicaBatchSink,
    identity: crate::ReplicaWriterIdentity,
    max_batch: usize,
    max_frame: usize,
    timeout: Duration,
) -> io::Result<ReplicaDurableAck> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .filter(|_| !timeout.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "复制期限无效"))?;
    let peer = stream.peer_certificate().to_vec();
    log.verify_bound_replica_writer(identity, &peer, deadline)?;
    let mut bounded = crate::replicated_appender::DeadlineStream {
        inner: stream,
        deadline,
    };
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "复制期限已过"))?;
    receive_replication_batch(
        &mut bounded,
        standby,
        &mut |record: &WalRecord, record_deadline: Instant| {
            if (record.cluster_id, record.shard_id, record.epoch)
                != (identity.cluster_id, identity.shard_id, identity.epoch)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "复制记录与认证写者不一致",
                ));
            }
            let limit = record_deadline.min(deadline);
            log.verify_bound_replica_writer(identity, &peer, limit)?;
            log.verify_active_prepared(record, limit)
        },
        max_batch,
        max_frame,
        remaining,
    )
}

impl<F> ReplicaIntentVerifier for F
where
    F: FnMut(&WalRecord, Instant) -> io::Result<()>,
{
    fn verify(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<()> {
        self(record, deadline)
    }
}

impl ReplicaIntentVerifier for crate::FencedLog {
    fn verify(&mut self, record: &WalRecord, deadline: Instant) -> io::Result<()> {
        self.verify_active_prepared(record, deadline)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(crate) fn validate_limit(max_frame: usize) -> io::Result<()> {
    if !(MIN_FRAME..=MAX_FRAME).contains(&max_frame) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "复制帧容量无效",
        ));
    }
    Ok(())
}
fn u32_at(bytes: &[u8]) -> io::Result<u32> {
    Ok(u32::from_be_bytes(
        bytes.try_into().map_err(|_| invalid("复制帧整数无效"))?,
    ))
}
fn u64_at(bytes: &[u8]) -> io::Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| invalid("复制确认整数无效"))?,
    ))
}

/// 发送一批连续记录并验证备节点返回的持久确认。
///
/// 调用方必须提供已认证的双向流并设置读写期限；此函数不得在撮合线程调用。
/// 出错或结果未知后应关闭连接，并通过已验证的连续前缀恢复。
pub fn send_replication_batch(
    stream: &mut (impl Read + Write),
    records: &[WalRecord],
    max_frame: usize,
) -> io::Result<ReplicaDurableAck> {
    validate_limit(max_frame)?;
    let Some(first) = records.first() else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "复制批次为空"));
    };
    if first.epoch == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "复制纪元为零"));
    }
    let count = u32::try_from(records.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "复制批次过大"))?;
    let mut total = 16_usize;
    let mut expected = first.shard_seq;
    for (index, record) in records.iter().enumerate() {
        if record.cluster_id != first.cluster_id
            || record.shard_id != first.shard_id
            || record.epoch != first.epoch
            || record.shard_seq != expected
            || record.flags != 0
            || record.shard_seq == 0
            || record.payload.len() > 16 * 1024 * 1024
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制批次身份或序号无效",
            ));
        }
        total = total
            .checked_add(68)
            .and_then(|size| size.checked_add(record.payload.len()))
            .ok_or_else(|| invalid("复制帧长度溢出"))?;
        if total > max_frame {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "复制帧超限"));
        }
        if index + 1 < records.len() {
            expected = expected
                .checked_add(1)
                .ok_or_else(|| invalid("复制序号耗尽"))?;
        }
    }
    let body_len = u32::try_from(total - 8).map_err(|_| invalid("复制帧长度越界"))?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(total)
        .map_err(|_| io::Error::other("复制帧内存不足"))?;
    frame.extend_from_slice(b"RPL1");
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(&count.to_be_bytes());
    for record in records {
        let bytes = record.encode().map_err(|_| invalid("WAL 记录编码失败"))?;
        frame.extend_from_slice(
            &u32::try_from(bytes.len())
                .map_err(|_| invalid("WAL 长度越界"))?
                .to_be_bytes(),
        );
        frame.extend_from_slice(&bytes);
    }
    let checksum = matching_persistence::crc32c(&frame);
    frame.extend_from_slice(&checksum.to_be_bytes());
    if frame.len() != total {
        return Err(invalid("复制帧长度不一致"));
    }
    stream.write_all(&frame)?;
    stream.flush()?;
    let mut ack = [0_u8; 36];
    stream.read_exact(&mut ack)?;
    if &ack[..4] != b"RPA1" || matching_persistence::crc32c(&ack[..32]).to_be_bytes() != ack[32..] {
        return Err(invalid("复制确认损坏"));
    }
    let result = ReplicaDurableAck {
        cluster_id: u16::from_be_bytes([ack[4], ack[5]]),
        shard_id: u16::from_be_bytes([ack[6], ack[7]]),
        epoch: u64_at(&ack[8..16])?,
        first_seq: u64_at(&ack[16..24])?,
        through_seq: u64_at(&ack[24..32])?,
    };
    if result.cluster_id != first.cluster_id
        || result.shard_id != first.shard_id
        || result.epoch != first.epoch
        || result.first_seq != first.shard_seq
        || result.through_seq != expected
    {
        return Err(invalid("复制确认身份或范围不一致"));
    }
    Ok(result)
}

/// 读取并验证一批记录，在备节点实际同步后写回连续前缀确认。
///
/// 调用方负责认证连接、限制并发和设置读写期限；错误后必须关闭该连接。
pub fn receive_replication_batch(
    stream: &mut (impl Read + Write),
    standby: &mut impl ReplicaBatchSink,
    verifier: &mut impl ReplicaIntentVerifier,
    max_batch: usize,
    max_frame: usize,
    timeout: Duration,
) -> io::Result<ReplicaDurableAck> {
    validate_limit(max_frame)?;
    if max_batch == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "复制批次上限为零",
        ));
    }
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "复制接收期限为零",
        ));
    }
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "复制接收期限越界"))?;
    let mut header = [0_u8; 8];
    stream.read_exact(&mut header)?;
    if &header[..4] != b"RPL1" {
        return Err(invalid("复制帧标识无效"));
    }
    let body_len = usize::try_from(u32_at(&header[4..])?).map_err(|_| invalid("复制帧长度越界"))?;
    let total = body_len
        .checked_add(header.len())
        .ok_or_else(|| invalid("复制帧长度溢出"))?;
    if body_len < 8 || total > max_frame {
        return Err(invalid("复制帧长度无效"));
    }
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(total)
        .map_err(|_| io::Error::other("复制帧内存不足"))?;
    frame.extend_from_slice(&header);
    frame.resize(total, 0);
    stream.read_exact(&mut frame[8..])?;
    let checksum_offset = total - 4;
    if matching_persistence::crc32c(&frame[..checksum_offset]).to_be_bytes()
        != frame[checksum_offset..]
    {
        return Err(invalid("复制帧校验失败"));
    }
    let count = usize::try_from(u32_at(&frame[8..12])?).map_err(|_| invalid("复制数量越界"))?;
    if count == 0 || count > max_batch || count > (checksum_offset - 12) / 68 {
        return Err(invalid("复制批次数量无效"));
    }
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| io::Error::other("复制记录内存不足"))?;
    let mut position = 12_usize;
    for _ in 0..count {
        let end = position
            .checked_add(4)
            .ok_or_else(|| invalid("复制偏移溢出"))?;
        if end > checksum_offset {
            return Err(invalid("复制记录长度缺失"));
        }
        let len =
            usize::try_from(u32_at(&frame[position..end])?).map_err(|_| invalid("WAL 长度越界"))?;
        position = end;
        let end = position
            .checked_add(len)
            .ok_or_else(|| invalid("WAL 偏移溢出"))?;
        if end > checksum_offset {
            return Err(invalid("复制记录截断"));
        }
        let (record, used) =
            WalRecord::decode(&frame[position..end]).map_err(|_| invalid("复制 WAL 记录损坏"))?;
        if used != len {
            return Err(invalid("复制 WAL 记录长度不一致"));
        }
        records.push(record);
        position = end;
    }
    if position != checksum_offset {
        return Err(invalid("复制帧含多余字节"));
    }
    for record in &records {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "复制意图验证期限已过",
            ));
        }
        verifier.verify(record, deadline)?;
    }
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "复制意图验证期限已过",
        ));
    }
    let ack = standby.append_verified(&records, Instant::now())?;
    let mut reply = [0_u8; 36];
    reply[..4].copy_from_slice(b"RPA1");
    reply[4..6].copy_from_slice(&ack.cluster_id.to_be_bytes());
    reply[6..8].copy_from_slice(&ack.shard_id.to_be_bytes());
    reply[8..16].copy_from_slice(&ack.epoch.to_be_bytes());
    reply[16..24].copy_from_slice(&ack.first_seq.to_be_bytes());
    reply[24..32].copy_from_slice(&ack.through_seq.to_be_bytes());
    let checksum = matching_persistence::crc32c(&reply[..32]);
    reply[32..].copy_from_slice(&checksum.to_be_bytes());
    stream.write_all(&reply)?;
    stream.flush()?;
    Ok(ack)
}
