//! 以租约和纪元事务保护跨主机连续日志的外围存储。
use crate::EtcdClient;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use matching_domain::{ClusterId, ShardId};
use matching_persistence::WalRecord;
use serde_json::{Value, json};
use std::io;

/// 每个分块的最大字节数，小于控制存储单值上限。
const CHUNK: usize = 192 * 1024;
/// WAL 编码允许的最大记录长度。
const MAX_RECORD: usize = 16 * 1024 * 1024 + 64;

/// 按集群和分片隔离的线性一致连续日志入口。
#[derive(Clone)]
pub struct FencedLog {
    /// 线性一致事务客户端。
    client: EtcdClient,
    /// 独立日志命名空间。
    namespace: String,
    /// 必须匹配的集群身份。
    cluster: ClusterId,
    /// 必须匹配的分片身份。
    shard: ShardId,
}

/// 经认证连接携带的分片写者身份；租约编号本身不构成身份认证。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaWriterIdentity {
    /// 所属集群。
    pub cluster_id: u16,
    /// 所属分片。
    pub shard_id: u16,
    /// 已分配的持久写者纪元。
    pub epoch: u64,
    /// 绑定写权的控制存储租约。
    pub lease_id: i64,
}

/// 某个权威提交水位对应的持久状态和有序事件摘要。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DigestCheckpoint {
    /// 已验证的分片提交序号。
    pub shard_seq: u64,
    /// 登记摘要的写者纪元。
    pub epoch: u64,
    /// 该序号执行后的状态摘要。
    pub state_digest: [u8; 32],
    /// 从创世至该序号的有序事件摘要。
    pub event_hash: [u8; 32],
}

fn encode_digest(value: DigestCheckpoint) -> [u8; 88] {
    let mut bytes = [0; 88];
    bytes[..4].copy_from_slice(b"FDC1");
    bytes[4..12].copy_from_slice(&value.shard_seq.to_be_bytes());
    bytes[12..20].copy_from_slice(&value.epoch.to_be_bytes());
    bytes[20..52].copy_from_slice(&value.state_digest);
    bytes[52..84].copy_from_slice(&value.event_hash);
    let crc = matching_persistence::crc32c(&bytes[..84]);
    bytes[84..].copy_from_slice(&crc.to_be_bytes());
    bytes
}
fn decode_digest(bytes: &[u8], seq: u64) -> io::Result<DigestCheckpoint> {
    if bytes.len() != 88
        || &bytes[..4] != b"FDC1"
        || matching_persistence::crc32c(&bytes[..84]).to_be_bytes() != bytes[84..]
    {
        return Err(corrupt("摘要检查点损坏"));
    }
    let shard_seq = u64::from_be_bytes(
        bytes[4..12]
            .try_into()
            .map_err(|_| corrupt("摘要序号无效"))?,
    );
    let epoch = u64::from_be_bytes(
        bytes[12..20]
            .try_into()
            .map_err(|_| corrupt("摘要纪元无效"))?,
    );
    if shard_seq == 0 || shard_seq != seq || epoch == 0 {
        return Err(corrupt("摘要身份无效"));
    }
    Ok(DigestCheckpoint {
        shard_seq,
        epoch,
        state_digest: bytes[20..52]
            .try_into()
            .map_err(|_| corrupt("状态摘要无效"))?,
        event_hash: bytes[52..84]
            .try_into()
            .map_err(|_| corrupt("事件摘要无效"))?,
    })
}

/// 独占租约和纪元；失权或写入结果未知后不得继续使用。
pub struct FencedLogWriter {
    /// 此写者所属日志。
    log: FencedLog,
    /// 控制存储租约身份。
    lease: i64,
    /// 获取租约时申请的有效期。
    ttl: std::time::Duration,
    /// 已持久分配的单调写者纪元。
    epoch: u64,
    /// 随租约绑定的写者证书；旧接口不绑定证书。
    peer_certificate: Option<Vec<u8>>,
    /// 本写者已登记的连续复制意图末尾。
    prepared_through: u64,
    /// 本实例是否已经失效或结果未知。
    invalidated: bool,
    /// 测试事务已提交但响应丢失的恢复路径。
    #[cfg(test)]
    simulate_lost_commit: bool,
    /// 在本地补齐预检后模拟远端撤销租约。
    #[cfg(test)]
    simulate_revoke_during_extend: bool,
}

fn corrupt(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn compare_value(key: &str, value: &[u8]) -> Value {
    json!({"key": STANDARD.encode(key), "target":"VALUE", "value":STANDARD.encode(value), "result":"EQUAL"})
}
fn compare_revision(key: &str, revision: Option<u64>) -> Value {
    match revision {
        Some(revision) => {
            json!({"key":STANDARD.encode(key), "target":"MOD", "mod_revision":revision.to_string(), "result":"EQUAL"})
        }
        None => {
            json!({"key":STANDARD.encode(key), "target":"VERSION", "version":"0", "result":"EQUAL"})
        }
    }
}
fn put(key: &str, value: &[u8], lease: Option<i64>) -> Value {
    let mut request = json!({"key":STANDARD.encode(key), "value":STANDARD.encode(value)});
    if let Some(lease) = lease {
        request["lease"] = Value::String(lease.to_string());
    }
    json!({"request_put":request})
}
fn checked_u64(bytes: &[u8], magic: &[u8; 4]) -> io::Result<u64> {
    if bytes.len() != 16
        || &bytes[..4] != magic
        || matching_persistence::crc32c(&bytes[..12]).to_be_bytes() != bytes[12..]
    {
        return Err(corrupt("日志水位或纪元损坏"));
    }
    let value = u64::from_be_bytes(
        bytes[4..12]
            .try_into()
            .map_err(|_| corrupt("日志数值无效"))?,
    );
    if value == 0 {
        return Err(corrupt("日志水位或纪元为零"));
    }
    Ok(value)
}
fn encode_u64(magic: &[u8; 4], value: u64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..4].copy_from_slice(magic);
    bytes[4..12].copy_from_slice(&value.to_be_bytes());
    let checksum = matching_persistence::crc32c(&bytes[..12]);
    bytes[12..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

impl FencedLog {
    /// 建立受限命名空间；调用方负责通过访问策略保护该命名空间。
    pub fn new(
        client: EtcdClient,
        prefix: &str,
        cluster: ClusterId,
        shard: ShardId,
    ) -> io::Result<Self> {
        if prefix.is_empty()
            || prefix.len() > 256
            || !prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains("//")
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "日志命名空间无效",
            ));
        }
        Ok(Self {
            client,
            namespace: format!(
                "{prefix}/cluster/{:04x}/shard/{:04x}/fenced-log",
                cluster.get(),
                shard.get()
            ),
            cluster,
            shard,
        })
    }
    fn owner(&self) -> String {
        format!("{}/owner", self.namespace)
    }
    /// 随写者租约失效的证书绑定键。
    fn owner_certificate(&self) -> String {
        format!("{}/owner-certificate", self.namespace)
    }
    fn epoch_key(&self) -> String {
        format!("{}/epoch", self.namespace)
    }
    fn head(&self) -> String {
        format!("{}/head", self.namespace)
    }
    fn record(&self, seq: u64) -> String {
        format!("{}/record/{seq:016x}", self.namespace)
    }
    fn digest_checkpoint(&self, seq: u64) -> String {
        format!("{}/digest/{seq:016x}", self.namespace)
    }
    fn digest_head(&self) -> String {
        format!("{}/digest-head", self.namespace)
    }
    fn genesis_protocol(&self) -> String {
        format!("{}/genesis-protocol", self.namespace)
    }
    /// 读取指定序号的持久摘要；调用方须另核验其与实际业务状态的关系。
    pub fn read_digest_checkpoint(&self, seq: u64) -> io::Result<Option<DigestCheckpoint>> {
        if seq == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "摘要序号为零"));
        }
        self.client
            .get(self.digest_checkpoint(seq).as_bytes())?
            .map(|(_, bytes)| decode_digest(&bytes, seq))
            .transpose()
    }
    fn intent(&self, epoch: u64, seq: u64) -> String {
        format!("{}/intent/{epoch:016x}/{seq:016x}", self.namespace)
    }
    fn chunk(&self, epoch: u64, seq: u64, index: usize) -> String {
        format!(
            "{}/stage/{epoch:016x}/{seq:016x}/{index:08x}",
            self.namespace
        )
    }
    fn read_head(&self) -> io::Result<(Option<u64>, u64)> {
        match self.client.get(self.head().as_bytes())? {
            Some((revision, bytes)) => Ok((Some(revision), checked_u64(&bytes, b"FLH1")?)),
            None => Ok((None, 0)),
        }
    }
    /// 返回控制存储确认的连续前缀末尾；空日志返回零。
    pub fn committed(&self) -> io::Result<u64> {
        Ok(self.read_head()?.1)
    }
    /// 在共同截止时间内读取权威连续水位。
    pub(crate) fn committed_until(&self, deadline: std::time::Instant) -> io::Result<u64> {
        check_deadline(Some(deadline))?;
        let head = self
            .client
            .constrained_by(deadline)?
            .get(self.head().as_bytes())?;
        check_deadline(Some(deadline))?;
        match head {
            Some((_, bytes)) => checked_u64(&bytes, b"FLH1"),
            None => Ok(0),
        }
    }

    /// 在共同截止时间内读取并校验已确认记录。
    pub(crate) fn read_until(
        &self,
        seq: u64,
        deadline: std::time::Instant,
    ) -> io::Result<Option<WalRecord>> {
        if seq == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "日志序号为零"));
        }
        if seq > self.committed_until(deadline)? {
            return Ok(None);
        }
        let (_, meta) = self
            .client
            .constrained_by(deadline)?
            .get(self.record(seq).as_bytes())?
            .ok_or_else(|| corrupt("已确认日志记录缺失"))?;
        let record = self.decode_meta(seq, &meta, Some(deadline))?;
        check_deadline(Some(deadline))?;
        Ok(Some(record))
    }
    /// 以线性一致事务核验写者身份；调用方还须认证连接并串行安装备节点栅栏。
    pub fn verify_replica_writer(
        &self,
        identity: ReplicaWriterIdentity,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        if identity.cluster_id != self.cluster.get()
            || identity.shard_id != self.shard.get()
            || identity.epoch == 0
            || identity.lease_id <= 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制写者身份无效",
            ));
        }
        check_deadline(Some(deadline))?;
        if self
            .client
            .constrained_by(deadline)?
            .transaction(
                vec![
                    compare_value(&self.owner(), &identity.lease_id.to_be_bytes()),
                    compare_value(&self.epoch_key(), &encode_u64(b"FLE1", identity.epoch)),
                ],
                vec![],
            )?
            .is_none()
        {
            return Err(io::Error::other("复制写者已失权"));
        }
        check_deadline(Some(deadline))
    }

    /// 在同一事务中核验写者租约、纪元和绑定的对端证书。
    pub fn verify_bound_replica_writer(
        &self,
        identity: ReplicaWriterIdentity,
        certificate: &[u8],
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        if identity.cluster_id != self.cluster.get()
            || identity.shard_id != self.shard.get()
            || identity.epoch == 0
            || identity.lease_id <= 0
            || certificate.is_empty()
            || certificate.len() > 16 * 1024
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "绑定写者身份无效",
            ));
        }
        check_deadline(Some(deadline))?;
        if self
            .client
            .constrained_by(deadline)?
            .transaction(
                vec![
                    compare_value(&self.owner(), &identity.lease_id.to_be_bytes()),
                    compare_value(&self.epoch_key(), &encode_u64(b"FLE1", identity.epoch)),
                    compare_value(&self.owner_certificate(), certificate),
                ],
                vec![],
            )?
            .is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "写者证书或租约无效",
            ));
        }
        check_deadline(Some(deadline))
    }
    /// 申请写者租约并通过事务持久递增纪元。
    pub fn acquire(&self, ttl_seconds: u64) -> io::Result<FencedLogWriter> {
        self.acquire_inner(ttl_seconds, None)
    }

    /// 以原子事务将非空证书绑定到新写者租约。
    pub fn acquire_bound(
        &self,
        ttl_seconds: u64,
        certificate: &[u8],
    ) -> io::Result<FencedLogWriter> {
        if certificate.is_empty() || certificate.len() > 16 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "写者证书长度无效",
            ));
        }
        self.acquire_inner(ttl_seconds, Some(certificate))
    }

    /// 申请租约并根据需要原子登记对端证书。
    fn acquire_inner(
        &self,
        ttl_seconds: u64,
        certificate: Option<&[u8]>,
    ) -> io::Result<FencedLogWriter> {
        let lease = self.client.grant_lease(ttl_seconds)?;
        let result = (|| {
            let previous = self.client.get(self.epoch_key().as_bytes())?;
            let (revision, old) = match previous {
                Some((revision, bytes)) => (Some(revision), checked_u64(&bytes, b"FLE1")?),
                None => (None, 0),
            };
            let epoch = old.checked_add(1).ok_or_else(|| corrupt("写者纪元耗尽"))?;
            let comparisons = vec![
                compare_revision(&self.owner(), None),
                compare_revision(&self.owner_certificate(), None),
                compare_revision(&self.epoch_key(), revision),
            ];
            let mut writes = vec![
                put(&self.epoch_key(), &encode_u64(b"FLE1", epoch), None),
                put(&self.owner(), &lease.to_be_bytes(), Some(lease)),
            ];
            if let Some(cert) = certificate {
                writes.push(put(&self.owner_certificate(), cert, Some(lease)));
            }
            if self.client.transaction(comparisons, writes)?.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "写者租约已被占用或纪元发生变化",
                ));
            }
            let (head_revision, head) = self.read_head()?;
            let mut previous_epoch = 0;
            for seq in 1..=head {
                let record = self
                    .read(seq)?
                    .ok_or_else(|| corrupt("已确认日志记录缺失"))?;
                if record.epoch < previous_epoch || record.epoch >= epoch {
                    return Err(corrupt("已确认日志纪元顺序无效"));
                }
                previous_epoch = record.epoch;
                self.client.keep_lease(lease)?;
            }
            self.client.keep_lease(lease)?;
            let mut final_checks = vec![
                compare_value(&self.owner(), &lease.to_be_bytes()),
                compare_value(&self.epoch_key(), &encode_u64(b"FLE1", epoch)),
                compare_revision(&self.head(), head_revision),
            ];
            if let Some(cert) = certificate {
                final_checks.push(compare_value(&self.owner_certificate(), cert));
            }
            if self.client.transaction(final_checks, vec![])?.is_none() {
                return Err(io::Error::other("检查前缀期间写权或水位发生变化"));
            }
            Ok(FencedLogWriter {
                log: self.clone(),
                lease,
                ttl: std::time::Duration::from_secs(ttl_seconds),
                epoch,
                peer_certificate: certificate.map(<[u8]>::to_vec),
                prepared_through: head,
                invalidated: false,
                #[cfg(test)]
                simulate_lost_commit: false,
                #[cfg(test)]
                simulate_revoke_during_extend: false,
            })
        })();
        match result {
            Ok(writer) => Ok(writer),
            Err(error) => match self.client.revoke_lease(lease) {
                Ok(()) => Err(error),
                Err(_) => Err(io::Error::other("写者租约申请失败且撤销结果未知")),
            },
        }
    }
    /// 读取已确认记录；确认范围内缺块、损坏或身份错误均失败关闭。
    pub fn read(&self, seq: u64) -> io::Result<Option<WalRecord>> {
        if seq == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "日志序号为零"));
        }
        if seq > self.committed()? {
            return Ok(None);
        }
        let (_, meta) = self
            .client
            .get(self.record(seq).as_bytes())?
            .ok_or_else(|| corrupt("已确认日志记录缺失"))?;
        Ok(Some(self.decode_meta(seq, &meta, None)?))
    }
    /// 读取尚未提交的指定纪元复制意图；不存在时返回空值。
    pub fn read_prepared(&self, epoch: u64, seq: u64) -> io::Result<Option<WalRecord>> {
        self.read_prepared_with_deadline(epoch, seq, None)
    }
    /// 验证活动 owner、纪元与完整复制意图；同步过程仍需接任栅栏保护。
    pub(crate) fn verify_active_prepared(
        &self,
        record: &WalRecord,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        if record.cluster_id != self.cluster.get()
            || record.shard_id != self.shard.get()
            || record.epoch == 0
            || record.shard_seq == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制记录身份无效",
            ));
        }
        let owner_key = self.owner();
        let epoch_key = self.epoch_key();
        let intent_key = self.intent(record.epoch, record.shard_seq);
        let (_, owner) = self
            .client
            .constrained_by(deadline)?
            .get(owner_key.as_bytes())?
            .ok_or_else(|| io::Error::other("复制写权不存在"))?;
        let lease = i64::from_be_bytes(
            owner
                .as_slice()
                .try_into()
                .map_err(|_| corrupt("复制租约身份无效"))?,
        );
        if lease <= 0 {
            return Err(corrupt("复制租约身份无效"));
        }
        let epoch = encode_u64(b"FLE1", record.epoch);
        let (_, meta) = self
            .client
            .constrained_by(deadline)?
            .get_if_values(
                &[
                    (owner_key.as_bytes(), &owner),
                    (epoch_key.as_bytes(), &epoch),
                ],
                intent_key.as_bytes(),
            )?
            .ok_or_else(|| io::Error::other("复制意图不存在"))?;
        if self.decode_meta(record.shard_seq, &meta, Some(deadline))? != *record {
            return Err(corrupt("复制意图与记录不一致"));
        }
        if self
            .client
            .constrained_by(deadline)?
            .transaction(
                vec![
                    compare_value(&owner_key, &owner),
                    compare_value(&epoch_key, &epoch),
                ],
                vec![],
            )?
            .is_none()
        {
            return Err(io::Error::other("复制意图检查期间写权已失效"));
        }
        Ok(())
    }
    pub(crate) fn read_prepared_with_deadline(
        &self,
        epoch: u64,
        seq: u64,
        deadline: Option<std::time::Instant>,
    ) -> io::Result<Option<WalRecord>> {
        if epoch == 0 || seq == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "复制意图身份无效",
            ));
        }
        let Some((_, meta)) = self
            .client_for_deadline(deadline)?
            .get(self.intent(epoch, seq).as_bytes())?
        else {
            return Ok(None);
        };
        let record = self.decode_meta(seq, &meta, deadline)?;
        if record.epoch != epoch {
            return Err(corrupt("复制意图纪元不一致"));
        }
        Ok(Some(record))
    }
    fn client_for_deadline(&self, deadline: Option<std::time::Instant>) -> io::Result<EtcdClient> {
        match deadline {
            Some(deadline) => self.client.constrained_by(deadline),
            None => Ok(self.client.clone()),
        }
    }
    fn decode_meta(
        &self,
        seq: u64,
        meta: &[u8],
        deadline: Option<std::time::Instant>,
    ) -> io::Result<WalRecord> {
        if meta.len() != 24
            || &meta[..4] != b"FLR1"
            || matching_persistence::crc32c(&meta[..20]).to_be_bytes() != meta[20..]
        {
            return Err(corrupt("已确认日志元数据损坏"));
        }
        let epoch = u64::from_be_bytes(
            meta[4..12]
                .try_into()
                .map_err(|_| corrupt("记录纪元无效"))?,
        );
        let len = usize::try_from(u32::from_be_bytes(
            meta[12..16]
                .try_into()
                .map_err(|_| corrupt("记录长度无效"))?,
        ))
        .map_err(|_| corrupt("记录长度越界"))?;
        let checksum = u32::from_be_bytes(
            meta[16..20]
                .try_into()
                .map_err(|_| corrupt("记录校验值无效"))?,
        );
        if epoch == 0 || len == 0 || len > MAX_RECORD {
            return Err(corrupt("记录身份或长度无效"));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| io::Error::other("日志恢复内存不足"))?;
        for index in 0..len.div_ceil(CHUNK) {
            let (_, part) = self
                .client_for_deadline(deadline)?
                .get(self.chunk(epoch, seq, index).as_bytes())?
                .ok_or_else(|| corrupt("已确认日志分块缺失"))?;
            let expected = (len - bytes.len()).min(CHUNK);
            if part.len() != expected {
                return Err(corrupt("已确认日志分块长度无效"));
            }
            bytes.extend_from_slice(&part);
        }
        if matching_persistence::crc32c(&bytes) != checksum {
            return Err(corrupt("已确认日志校验失败"));
        }
        let (record, used) = WalRecord::decode(&bytes).map_err(|_| corrupt("WAL 记录无效"))?;
        if used != len
            || record.cluster_id != self.cluster.get()
            || record.shard_id != self.shard.get()
            || record.shard_seq != seq
            || record.epoch != epoch
        {
            return Err(corrupt("WAL 身份或序号不一致"));
        }
        Ok(record)
    }
}

impl FencedLogWriter {
    /// 以证书绑定写权核验完整权威前缀，并将缺失后缀补齐到独占本地 WAL。
    ///
    /// 调用方须先完成备节点前缀协调并保持目录锁；本方法不代表业务状态已重放。
    pub fn extend_verified_local_prefix(
        &mut self,
        wal: impl AsRef<std::path::Path>,
        manager: &mut matching_persistence::ManifestManager<
            matching_persistence::FileManifestStorage,
        >,
        max_segment_size: u64,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            let head = self.log.committed_until(deadline)?;
            let local = manager.state().local_durable_seq;
            if self.prepared_through != head || local >= head {
                return Err(corrupt("本地不是落后的权威前缀"));
            }
            let mut suffix = Vec::new();
            let mut seq = local;
            while seq < head {
                seq = seq.checked_add(1).ok_or_else(|| corrupt("补齐序号溢出"))?;
                let record = self
                    .log
                    .read_until(seq, deadline)?
                    .ok_or_else(|| corrupt("权威后缀缺失"))?;
                suffix
                    .try_reserve(1)
                    .map_err(|_| io::Error::other("权威后缀内存不足"))?;
                suffix.push(record);
            }
            matching_persistence::extend_local_prefix(
                wal,
                manager,
                max_segment_size,
                self.log.cluster.get(),
                self.log.shard.get(),
                &suffix,
                |prefix, suffix| {
                    self.verify_bound_lease(deadline)?;
                    let mut expected = 0_u64;
                    for record in prefix.iter().chain(suffix) {
                        expected = expected
                            .checked_add(1)
                            .ok_or_else(|| corrupt("补齐序号溢出"))?;
                        if record.shard_seq != expected
                            || self.log.read_until(expected, deadline)?.as_ref() != Some(record)
                        {
                            return Err(corrupt("本地或补齐记录与权威日志不一致"));
                        }
                    }
                    if expected != head || self.log.committed_until(deadline)? != head {
                        return Err(corrupt("补齐期间权威水位变化"));
                    }
                    self.verify_bound_lease(deadline)?;
                    #[cfg(test)]
                    if self.simulate_revoke_during_extend {
                        self.log.client.revoke_lease(self.lease)?;
                    }
                    Ok(())
                },
            )?;
            if self.log.committed_until(deadline)? != head {
                return Err(corrupt("补齐后权威水位变化"));
            }
            self.verify_bound_lease(deadline)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 核验协调前缀与当前权威 head、完整 WAL 记录及证书绑定写权。
    pub fn verify_reconciliation_prefix(
        &mut self,
        records: &[WalRecord],
        target: u64,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            if self.log.committed_until(deadline)? != target
                || self.prepared_through != target
                || u64::try_from(records.len()).ok() != Some(target)
            {
                return Err(corrupt("协调前缀水位不一致"));
            }
            for (index, record) in records.iter().enumerate() {
                let seq = u64::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| corrupt("协调序号溢出"))?;
                if record.shard_seq != seq
                    || self.log.read_until(seq, deadline)?.as_ref() != Some(record)
                {
                    return Err(corrupt("本地与权威前缀不一致"));
                }
            }
            if self.log.committed_until(deadline)? != target {
                return Err(corrupt("协调期间权威水位变化"));
            }
            self.verify_bound_lease(deadline)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在空权威日志中登记要求回复前摘要的创世协议；只能由证书绑定写者调用。
    ///
    /// 相同创世纪元的重复登记可重试；日志已有提交后不得补写此标记。
    pub fn initialize_genesis_protocol(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            let client = self.client_for_deadline(Some(deadline))?;
            if client.get(self.log.head().as_bytes())?.is_some() || self.prepared_through != 0 {
                return Err(corrupt("非空日志不能初始化创世协议"));
            }
            let key = self.log.genesis_protocol();
            let expected = encode_u64(b"FGP1", self.epoch);
            let existing = client.get(key.as_bytes())?;
            if let Some((_, value)) = &existing
                && checked_u64(value, b"FGP1")? > self.epoch
            {
                return Err(corrupt("创世协议标记冲突"));
            }
            let mut checks = self.comparisons();
            checks.push(compare_revision(&self.log.head(), None));
            checks.push(compare_revision(
                &key,
                existing.as_ref().map(|(revision, _)| *revision),
            ));
            let writes = if existing.is_none() {
                vec![put(&key, &expected, None)]
            } else {
                Vec::new()
            };
            if client.transaction(checks, writes)?.is_none() {
                return Err(io::Error::other("创世协议登记时写权或水位变化"));
            }
            check_deadline(Some(deadline))?;
            Ok(())
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 空日志建立创世协议，已有前缀维持原有认证恢复要求。
    pub(crate) fn initialize_genesis_if_empty(
        &mut self,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        if self.log.committed_until(deadline)? == 0 {
            self.initialize_genesis_protocol(deadline)
        } else {
            self.verify_bound_lease(deadline)
        }
    }

    /// 在有效证书绑定写权下验证此前空日志中登记的创世协议标记。
    pub fn verify_genesis_protocol(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            let client = self.client_for_deadline(Some(deadline))?;
            let (_, bytes) = client
                .get(self.log.genesis_protocol().as_bytes())?
                .ok_or_else(|| corrupt("缺少创世协议标记"))?;
            if checked_u64(&bytes, b"FGP1")? > self.epoch {
                return Err(corrupt("创世协议纪元无效"));
            }
            self.verify_bound_lease(deadline)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 读取不超过当前权威水位的最新持久摘要；空日志或尚无检查点时返回空。
    pub fn load_latest_digest(
        &mut self,
        deadline: std::time::Instant,
    ) -> io::Result<Option<DigestCheckpoint>> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            let head = self.log.committed_until(deadline)?;
            if self.prepared_through != head {
                return Err(corrupt("写者前缀与权威水位不一致"));
            }
            let client = self.client_for_deadline(Some(deadline))?;
            let pointer = client.get(self.log.digest_head().as_bytes())?;
            let value = if let Some((_, bytes)) = pointer {
                let seq = checked_u64(&bytes, b"FDH1")?;
                if seq > head {
                    return Err(corrupt("摘要水位超过权威前缀"));
                }
                let (_, bytes) = client
                    .get(self.log.digest_checkpoint(seq).as_bytes())?
                    .ok_or_else(|| corrupt("摘要索引对应的检查点缺失"))?;
                let checkpoint = decode_digest(&bytes, seq)?;
                if self
                    .log
                    .read_until(seq, deadline)?
                    .is_none_or(|record| record.epoch != checkpoint.epoch)
                {
                    return Err(corrupt("摘要与提交记录纪元不一致"));
                }
                Some(checkpoint)
            } else {
                None
            };
            if self.log.committed_until(deadline)? != head {
                return Err(corrupt("读取摘要期间水位发生变化"));
            }
            self.verify_bound_lease(deadline)?;
            Ok(value)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在证书绑定写权下读取当前权威水位的摘要，不允许使用旧水位的摘要。
    ///
    /// 缺失、损坏、纪元不一致或写权变化均令实例失效；返回后仍须重新核验写权。
    pub fn load_current_digest(
        &mut self,
        deadline: std::time::Instant,
    ) -> io::Result<DigestCheckpoint> {
        let result = (|| {
            self.verify_bound_lease(deadline)?;
            let head = self.log.committed_until(deadline)?;
            if head == 0 || self.prepared_through != head {
                return Err(corrupt("摘要水位不是当前权威前缀"));
            }
            let client = self.client_for_deadline(Some(deadline))?;
            let (_, bytes) = client
                .get(self.log.digest_checkpoint(head).as_bytes())?
                .ok_or_else(|| corrupt("当前权威水位缺少摘要"))?;
            let checkpoint = decode_digest(&bytes, head)?;
            if self
                .log
                .read_until(head, deadline)?
                .is_none_or(|record| record.epoch != checkpoint.epoch)
            {
                return Err(corrupt("摘要与提交记录纪元不一致"));
            }
            if self.log.committed_until(deadline)? != head {
                return Err(corrupt("读取摘要期间水位发生变化"));
            }
            self.verify_bound_lease(deadline)?;
            Ok(checkpoint)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在同一事务中核验证书绑定写权、权威水位并登记不可变摘要。
    ///
    /// 调用方必须在对应分片顺序点计算真实状态和事件摘要；此接口不承担计算或回复屏障。
    /// 已存在相同内容时允许重试；冲突、失权或结果未知使写者永久失效。
    pub fn checkpoint_digest(
        &mut self,
        seq: u64,
        state_digest: [u8; 32],
        event_hash: [u8; 32],
        deadline: std::time::Instant,
    ) -> io::Result<DigestCheckpoint> {
        let result = (|| {
            if seq == 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "摘要序号为零"));
            }
            self.verify_bound_lease(deadline)?;
            let client = self.client_for_deadline(Some(deadline))?;
            let (head_revision, bytes) = client
                .get(self.log.head().as_bytes())?
                .ok_or_else(|| corrupt("权威水位缺失"))?;
            if checked_u64(&bytes, b"FLH1")? != seq || self.prepared_through != seq {
                return Err(corrupt("摘要不对应当前权威水位"));
            }
            if self
                .log
                .read_until(seq, deadline)?
                .is_none_or(|record| record.epoch != self.epoch)
            {
                return Err(corrupt("摘要纪元与最后提交记录不一致"));
            }
            let checkpoint = DigestCheckpoint {
                shard_seq: seq,
                epoch: self.epoch,
                state_digest,
                event_hash,
            };
            let encoded = encode_digest(checkpoint);
            let key = self.log.digest_checkpoint(seq);
            let existing = client.get(key.as_bytes())?;
            let pointer_key = self.log.digest_head();
            let pointer = client.get(pointer_key.as_bytes())?;
            if let Some((_, value)) = &pointer
                && checked_u64(value, b"FDH1")? > seq
            {
                return Err(corrupt("摘要索引不得回退"));
            }
            if let Some((_, bytes)) = &existing
                && bytes.as_slice() != encoded
            {
                return Err(corrupt("相同序号的摘要冲突"));
            }
            let mut checks = self.comparisons();
            checks.push(compare_revision(&self.log.head(), Some(head_revision)));
            checks.push(compare_revision(
                &key,
                existing.as_ref().map(|(revision, _)| *revision),
            ));
            checks.push(compare_revision(
                &pointer_key,
                pointer.as_ref().map(|(revision, _)| *revision),
            ));
            let mut writes = Vec::new();
            if existing.is_none() {
                writes.push(put(&key, &encoded, None));
            }
            if pointer
                .as_ref()
                .is_none_or(|(_, value)| value.as_slice() != encode_u64(b"FDH1", seq))
            {
                writes.push(put(&pointer_key, &encode_u64(b"FDH1", seq), None));
            }
            if client.transaction(checks, writes)?.is_none() {
                return Err(io::Error::other("摘要登记时写权或水位发生变化"));
            }
            check_deadline(Some(deadline))?;
            Ok(checkpoint)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在证书绑定写权下逐条核验完整创世 WAL，忽略未认证的本地快照。
    ///
    /// 调用方须独占目录并提供最初的可信市场策略；此入口不证明业务重放摘要。
    pub fn recover_verified_genesis_prefix(
        &mut self,
        wal: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &matching_runtime::SnapshotRestoreConfig,
        policies: &[(
            matching_domain::MarketId,
            matching_orderbook::QueuePriorityPolicy,
        )],
        deadline: std::time::Instant,
    ) -> io::Result<matching_runtime::SnapshotRecoveryRecords> {
        let result = (|| {
            if config.cluster != self.log.cluster
                || config.shard != self.log.shard
                || config.primary_epoch != self.epoch
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "恢复身份或写者纪元不一致",
                ));
            }
            self.verify_bound_lease(deadline)?;
            let head = self.log.committed_until(deadline)?;
            if head != self.prepared_through {
                return Err(corrupt("写者前缀与权威水位不一致"));
            }
            let recovered = matching_runtime::recover_verified_genesis_records(
                wal,
                manager,
                config,
                policies,
                head,
                |record| {
                    if self.log.read_until(record.shard_seq, deadline)?.as_ref() != Some(record) {
                        return Err(corrupt("本地记录与权威日志不一致"));
                    }
                    Ok(())
                },
            )?;
            if self.log.committed_until(deadline)? != head {
                return Err(corrupt("恢复期间权威水位发生变化"));
            }
            self.verify_bound_lease(deadline)?;
            Ok(recovered)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在证书绑定写权下，将严格清单恢复的本地命令与权威连续日志逐条核对。
    ///
    /// 调用方须独占 WAL、清单和快照目录，并独立验证快照及其状态摘要。
    /// 返回的候选尚未重放、修复本地尾部或确认备节点，不能单独用于开放业务。
    /// 任何核验错误都使写者失效；返回后仍须在开放业务前重新核验写权。
    pub fn recover_verified_local_prefix(
        &mut self,
        wal: impl AsRef<std::path::Path>,
        snapshot_dir: impl AsRef<std::path::Path>,
        manager: &matching_persistence::ManifestManager<matching_persistence::FileManifestStorage>,
        config: &matching_runtime::SnapshotRestoreConfig,
        policies: &[(
            matching_domain::MarketId,
            matching_orderbook::QueuePriorityPolicy,
        )],
        deadline: std::time::Instant,
    ) -> io::Result<Option<matching_runtime::SnapshotRecoveryRecords>> {
        let result = (|| {
            if config.cluster != self.log.cluster
                || config.shard != self.log.shard
                || config.primary_epoch != self.epoch
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "恢复身份或写者纪元不一致",
                ));
            }
            self.verify_bound_lease(deadline)?;
            let head = self.log.committed_until(deadline)?;
            if head != self.prepared_through {
                return Err(corrupt("写者前缀与权威水位不一致"));
            }
            let recovered = matching_runtime::recover_verified_snapshot_records_with_genesis(
                wal,
                snapshot_dir,
                manager,
                config,
                policies,
                head,
                |record| {
                    if self.log.read_until(record.shard_seq, deadline)?.as_ref() != Some(record) {
                        return Err(corrupt("本地记录与权威日志不一致"));
                    }
                    Ok(())
                },
            )?;
            self.verify_bound_lease(deadline)?;
            if self.log.committed_until(deadline)? != head {
                return Err(corrupt("恢复期间权威水位发生变化"));
            }
            self.verify_bound_lease(deadline)?;
            Ok(recovered)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 核验当前写者的证书绑定和租约；未绑定或结果未知时永久失效。
    pub(crate) fn verify_bound_lease(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        let result = self
            .peer_certificate
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "写者未绑定证书"))
            .and_then(|certificate| {
                self.log
                    .verify_bound_replica_writer(self.replica_identity(), certificate, deadline)
            });
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }
    /// 使用证书绑定写者和固定对端证书的双向 TLS 流协调远端前缀。
    ///
    /// 调用方须使用安全 TLS 配置并验证所选快照；返回不代表主节点状态已恢复。
    pub fn reconcile_authenticated_remote_prefix(
        &mut self,
        stream: &mut crate::TlsReplicaStream,
        snapshot_seq: u64,
        deadline: std::time::Instant,
    ) -> io::Result<u64> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        let check = self
            .peer_certificate
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "写者未绑定证书"))
            .and_then(|cert| {
                self.log
                    .verify_bound_replica_writer(self.replica_identity(), cert, deadline)
            });
        if let Err(error) = check {
            self.invalidated = true;
            return Err(error);
        }
        self.reconcile_remote_prefix(stream, snapshot_seq, deadline)
    }

    /// 从已认证备节点取得持久前缀，完整核对后采纳同步尾部。
    ///
    /// 调用方须验证快照与连接身份；返回后备节点仍暂停，主节点也须恢复状态才能开放流量。
    /// 接收、校验或事务结果未知时使当前写者失效；此入口暂不提供备节点恢复确认。
    pub fn reconcile_remote_prefix(
        &mut self,
        stream: &mut impl crate::ReplicationStream,
        snapshot_seq: u64,
        deadline: std::time::Instant,
    ) -> io::Result<u64> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        let result = (|| {
            let identity = self.replica_identity();
            let durable = crate::send_replica_fence(stream, identity, deadline)?;
            let boundary = crate::ReplicaPrefixBoundary {
                cluster_id: identity.cluster_id,
                shard_id: identity.shard_id,
                snapshot_seq,
                durable_seq: durable,
                writer_epoch: identity.epoch,
            };
            let mut records = Vec::new();
            crate::read_replica_prefix(stream, boundary, deadline, |record| {
                records
                    .try_reserve(1)
                    .map_err(|_| io::Error::other("前缀暂存内存不足"))?;
                records.push(record);
                Ok(())
            })?;
            self.reconcile_records(&records, snapshot_seq, durable, deadline)?;
            self.log.verify_replica_writer(identity, deadline)?;
            Ok(durable)
        })();
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 对同进程持有的真实备节点执行接任协调；调用方须先验证所选快照及独占目录。
    ///
    /// 先核对整个备节点前缀与已确认记录或旧纪元复制意图，再逐条事务采纳已同步尾部。
    /// 本入口不提供跨主机认证或网络传输，失败及结果未知后须重新获取写权并恢复。
    pub fn reconcile_local_standby(
        &mut self,
        standby: &mut matching_persistence::FencedStandbyReplica<
            matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
        >,
        snapshot_seq: u64,
        deadline: std::time::Instant,
    ) -> io::Result<u64> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        let result = self.reconcile_local_standby_inner(standby, snapshot_seq, deadline);
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 在写权有效且备节点栅栏持有期间校验并采纳连续前缀。
    fn reconcile_local_standby_inner(
        &mut self,
        standby: &mut matching_persistence::FencedStandbyReplica<
            matching_persistence::RegisteredAppender<matching_persistence::FileManifestStorage>,
        >,
        snapshot_seq: u64,
        deadline: std::time::Instant,
    ) -> io::Result<u64> {
        let identity = self.replica_identity();
        let durable = crate::activate_replica_writer(&self.log, standby, identity, deadline)?;
        let records = standby.fenced_records(snapshot_seq)?;
        self.reconcile_records(&records, snapshot_seq, durable, deadline)?;
        standby.confirm_reconciled(durable, |cluster, shard, epoch| {
            if (cluster, shard, epoch) != (identity.cluster_id, identity.shard_id, identity.epoch) {
                return Err(corrupt("备节点协调身份发生变化"));
            }
            self.log.verify_replica_writer(identity, deadline)
        })?;
        Ok(durable)
    }

    /// 核对完整连续记录后采纳持久尾部；记录必须来自已认证并保持栅栏的备节点。
    fn reconcile_records(
        &mut self,
        records: &[WalRecord],
        snapshot_seq: u64,
        durable: u64,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        check_deadline(Some(deadline))?;
        let mut bounded = self.log.clone();
        bounded.client = bounded.client.constrained_by(deadline)?;
        let head = bounded.committed()?;
        if head < snapshot_seq || head > durable || self.prepared_through != head {
            return Err(corrupt("备节点与权威提交水位不一致"));
        }
        let mut previous_epoch = if snapshot_seq == 0 {
            0
        } else {
            bounded
                .read(snapshot_seq)?
                .ok_or_else(|| corrupt("快照边界日志缺失"))?
                .epoch
        };
        let mut previous_seq = snapshot_seq;
        for record in records {
            check_deadline(Some(deadline))?;
            if record.cluster_id != self.log.cluster.get()
                || record.shard_id != self.log.shard.get()
                || previous_seq.checked_add(1) != Some(record.shard_seq)
                || record.shard_seq > durable
                || record.epoch < previous_epoch
                || record.epoch >= self.epoch
            {
                return Err(corrupt("备节点记录纪元顺序无效"));
            }
            previous_seq = record.shard_seq;
            previous_epoch = record.epoch;
            let authoritative = if record.shard_seq <= head {
                bounded.read(record.shard_seq)?
            } else {
                bounded.read_prepared_with_deadline(
                    record.epoch,
                    record.shard_seq,
                    Some(deadline),
                )?
            };
            if authoritative.as_ref() != Some(record) {
                return Err(corrupt("备节点记录与权威记录或复制意图不一致"));
            }
        }
        if previous_seq != durable {
            return Err(corrupt("备节点记录未覆盖持久前缀"));
        }
        for record in records.iter().filter(|record| record.shard_seq > head) {
            self.adopt_durable_intent(record, deadline)?;
            self.prepared_through = record.shard_seq;
        }
        check_deadline(Some(deadline))?;
        if bounded.committed()? != durable {
            return Err(corrupt("协调后权威水位不一致"));
        }
        Ok(())
    }

    /// 在新写权下以事务采纳一条已经核对且备节点同步的旧意图。
    fn adopt_durable_intent(
        &self,
        record: &WalRecord,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        check_deadline(Some(deadline))?;
        let client = self.client_for_deadline(Some(deadline))?;
        let (head_revision, head) = match client.get(self.log.head().as_bytes())? {
            Some((revision, bytes)) => (Some(revision), checked_u64(&bytes, b"FLH1")?),
            None => (None, 0),
        };
        if head.checked_add(1) != Some(record.shard_seq) || head != self.prepared_through {
            return Err(corrupt("采纳序号不连续"));
        }
        let intent = self.log.intent(record.epoch, record.shard_seq);
        let (revision, meta) = self
            .client_for_deadline(Some(deadline))?
            .get(intent.as_bytes())?
            .ok_or_else(|| corrupt("复制意图缺失"))?;
        if self
            .log
            .decode_meta(record.shard_seq, &meta, Some(deadline))?
            != *record
        {
            return Err(corrupt("复制意图与备节点记录不一致"));
        }
        let key = self.log.record(record.shard_seq);
        let mut comparisons = self.comparisons();
        comparisons.push(compare_revision(&self.log.head(), head_revision));
        comparisons.push(compare_revision(&key, None));
        comparisons.push(compare_revision(&intent, Some(revision)));
        if self
            .client_for_deadline(Some(deadline))?
            .transaction(
                comparisons,
                vec![
                    put(&key, &meta, None),
                    put(
                        &self.log.head(),
                        &encode_u64(b"FLH1", record.shard_seq),
                        None,
                    ),
                ],
            )?
            .is_none()
        {
            return Err(io::Error::other("采纳时写权或前缀发生变化"));
        }
        #[cfg(test)]
        if self.simulate_lost_commit {
            return Err(io::Error::other("模拟采纳响应丢失"));
        }
        check_deadline(Some(deadline))
    }
    /// 返回供经认证的备节点核验的当前写者身份。
    pub fn replica_identity(&self) -> ReplicaWriterIdentity {
        ReplicaWriterIdentity {
            cluster_id: self.log.cluster.get(),
            shard_id: self.log.shard.get(),
            epoch: self.epoch,
            lease_id: self.lease,
        }
    }
    /// 返回已经持久分配的写者纪元。
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// 返回本实例是否已发现失权；未失效不证明远端租约仍有效。
    pub fn is_locally_invalidated(&self) -> bool {
        self.invalidated
    }
    /// 返回外围续租循环的间隔；续租失败仍须立即停止写入。
    pub(crate) fn renewal_interval(&self) -> std::time::Duration {
        self.ttl / 3
    }
    fn comparisons(&self) -> Vec<Value> {
        let mut checks = vec![
            compare_value(&self.log.owner(), &self.lease.to_be_bytes()),
            compare_value(&self.log.epoch_key(), &encode_u64(b"FLE1", self.epoch)),
        ];
        if let Some(certificate) = &self.peer_certificate {
            checks.push(compare_value(&self.log.owner_certificate(), certificate));
        }
        checks
    }
    /// 刷新租约；失败或结果未知后此写者永久失效。
    pub fn keep_alive(&mut self) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        if let Err(error) = self.log.client.keep_lease(self.lease) {
            self.invalidated = true;
            return Err(error);
        }
        Ok(())
    }
    /// 在剩余总期限内刷新租约。
    pub(crate) fn keep_alive_until(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        let result = self
            .log
            .client
            .constrained_by(deadline)
            .and_then(|client| client.keep_lease(self.lease));
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }
    /// 撤销写权；响应丢失也立即使本实例失效。
    pub fn revoke(&mut self) -> io::Result<()> {
        self.invalidated = true;
        self.log.client.revoke_lease(self.lease)
    }
    /// 登记不可变复制意图；此操作不推进已提交前缀。
    pub fn prepare(&mut self, record: &WalRecord) -> io::Result<()> {
        self.prepare_with_deadline(record, None)
    }
    /// 在批次截止时间内登记复制意图。
    pub(crate) fn prepare_until(
        &mut self,
        record: &WalRecord,
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        self.prepare_with_deadline(record, Some(deadline))
    }
    fn prepare_with_deadline(
        &mut self,
        record: &WalRecord,
        deadline: Option<std::time::Instant>,
    ) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        if record.cluster_id != self.log.cluster.get()
            || record.shard_id != self.log.shard.get()
            || record.epoch != self.epoch
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL 写者身份不一致",
            ));
        }
        let bytes = record
            .encode()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL 记录无效"))?;
        let result = self.prepare_encoded(record.shard_seq, &bytes, deadline);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::InvalidInput)
        {
            self.invalidated = true;
        }
        if result.is_ok() {
            self.prepared_through = record.shard_seq;
        }
        result
    }
    fn prepare_encoded(
        &self,
        seq: u64,
        bytes: &[u8],
        deadline: Option<std::time::Instant>,
    ) -> io::Result<()> {
        check_deadline(deadline)?;
        let client = self.client_for_deadline(deadline)?;
        let head = client.get_if_value(
            self.log.owner().as_bytes(),
            &self.lease.to_be_bytes(),
            self.log.head().as_bytes(),
        )?;
        check_deadline(deadline)?;
        let (head_revision, previous) = match head {
            Some((revision, bytes)) => (Some(revision), checked_u64(&bytes, b"FLH1")?),
            None => (None, 0),
        };
        if previous > self.prepared_through || self.prepared_through.checked_add(1) != Some(seq) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "日志序号不连续",
            ));
        }
        for (index, part) in bytes.chunks(CHUNK).enumerate() {
            check_deadline(deadline)?;
            let key = self.log.chunk(self.epoch, seq, index);
            let mut comparisons = self.comparisons();
            comparisons.push(compare_revision(&key, None));
            if self
                .client_for_deadline(deadline)?
                .transaction(comparisons, vec![put(&key, part, None)])?
                .is_none()
            {
                return Err(io::Error::other("日志分块写权或版本失效"));
            }
        }
        let mut meta = [0; 24];
        meta[..4].copy_from_slice(b"FLR1");
        meta[4..12].copy_from_slice(&self.epoch.to_be_bytes());
        meta[12..16].copy_from_slice(
            &u32::try_from(bytes.len())
                .map_err(|_| corrupt("记录长度越界"))?
                .to_be_bytes(),
        );
        meta[16..20].copy_from_slice(&matching_persistence::crc32c(bytes).to_be_bytes());
        let checksum = matching_persistence::crc32c(&meta[..20]);
        meta[20..].copy_from_slice(&checksum.to_be_bytes());
        let key = self.log.intent(self.epoch, seq);
        let mut comparisons = self.comparisons();
        comparisons.push(compare_revision(&self.log.head(), head_revision));
        comparisons.push(compare_revision(&key, None));
        check_deadline(deadline)?;
        if self
            .client_for_deadline(deadline)?
            .transaction(comparisons, vec![put(&key, &meta, None)])?
            .is_none()
        {
            return Err(io::Error::other("复制意图写权或连续水位失效"));
        }
        check_deadline(deadline)?;
        Ok(())
    }
    /// 提交已登记且内容完全一致的下一条复制意图。
    pub fn commit_prepared(&mut self, record: &WalRecord) -> io::Result<u64> {
        self.commit_prepared_with_deadline(record, None)
    }
    /// 在批次截止时间内提交已登记的复制意图。
    pub(crate) fn commit_prepared_until(
        &mut self,
        record: &WalRecord,
        deadline: std::time::Instant,
    ) -> io::Result<u64> {
        self.commit_prepared_with_deadline(record, Some(deadline))
    }
    fn commit_prepared_with_deadline(
        &mut self,
        record: &WalRecord,
        deadline: Option<std::time::Instant>,
    ) -> io::Result<u64> {
        if self.invalidated {
            return Err(io::Error::other("写者已经失效"));
        }
        if record.cluster_id != self.log.cluster.get()
            || record.shard_id != self.log.shard.get()
            || record.epoch != self.epoch
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL 写者身份不一致",
            ));
        }
        let result = self.commit_prepared_inner(record, deadline);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::InvalidInput)
        {
            self.invalidated = true;
        }
        result
    }
    fn commit_prepared_inner(
        &self,
        record: &WalRecord,
        deadline: Option<std::time::Instant>,
    ) -> io::Result<u64> {
        check_deadline(deadline)?;
        let head = self.client_for_deadline(deadline)?.get_if_value(
            self.log.owner().as_bytes(),
            &self.lease.to_be_bytes(),
            self.log.head().as_bytes(),
        )?;
        let (head_revision, previous) = match head {
            Some((revision, bytes)) => (Some(revision), checked_u64(&bytes, b"FLH1")?),
            None => (None, 0),
        };
        if previous.checked_add(1) != Some(record.shard_seq)
            || record.shard_seq > self.prepared_through
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "已登记意图的提交序号不连续",
            ));
        }
        let intent = self.log.intent(self.epoch, record.shard_seq);
        let (intent_revision, meta) = self
            .client_for_deadline(deadline)?
            .get(intent.as_bytes())?
            .ok_or_else(|| corrupt("复制意图缺失"))?;
        if self.log.decode_meta(record.shard_seq, &meta, deadline)? != *record {
            return Err(corrupt("复制意图与 WAL 记录不一致"));
        }
        let key = self.log.record(record.shard_seq);
        let mut comparisons = self.comparisons();
        comparisons.push(compare_revision(&self.log.head(), head_revision));
        comparisons.push(compare_revision(&key, None));
        comparisons.push(compare_revision(&intent, Some(intent_revision)));
        check_deadline(deadline)?;
        if self
            .client_for_deadline(deadline)?
            .transaction(
                comparisons,
                vec![
                    put(&key, &meta, None),
                    put(
                        &self.log.head(),
                        &encode_u64(b"FLH1", record.shard_seq),
                        None,
                    ),
                ],
            )?
            .is_none()
        {
            return Err(io::Error::other("日志提交写权、意图或连续水位失效"));
        }
        check_deadline(deadline)?;
        #[cfg(test)]
        if self.simulate_lost_commit {
            return Err(io::Error::other("模拟提交响应丢失"));
        }
        Ok(record.shard_seq)
    }
    /// 按连续序号登记并提交完整 WAL 记录；结果未知时必须恢复。
    pub fn append(&mut self, record: &WalRecord) -> io::Result<u64> {
        self.prepare(record)?;
        self.commit_prepared(record)
    }
    fn client_for_deadline(&self, deadline: Option<std::time::Instant>) -> io::Result<EtcdClient> {
        match deadline {
            Some(deadline) => self.log.client.constrained_by(deadline),
            None => Ok(self.log.client.clone()),
        }
    }
}

/// 超过截止时间后不再发起新的控制存储请求。
fn check_deadline(deadline: Option<std::time::Instant>) -> io::Result<()> {
    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
        Err(io::Error::new(io::ErrorKind::TimedOut, "权威提交期限已过"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    #[ignore = "需要独立本地 etcd 测试集群"]
    fn lease_revoked_between_local_prefix_records_is_detected_and_reconciled() {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
            Profile, RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let client = EtcdClient::new(endpoints, Duration::from_secs(5), true).unwrap();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tide-extend-interleave-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir(&root).unwrap();
        let wal = root.join("wal");
        let manifest = root.join("manifest");
        std::fs::create_dir(&wal).unwrap();
        std::fs::create_dir(&manifest).unwrap();
        let log = FencedLog::new(
            client.clone(),
            &format!(
                "/tide-test/extend-interleave/{}-{stamp}",
                std::process::id()
            ),
            ClusterId(183),
            ShardId(184),
        )
        .unwrap();
        let mut writer = log
            .acquire_bound(120, include_bytes!("../tests/fixtures/test-client.der"))
            .unwrap();
        let records: Vec<_> = (1..=512)
            .map(|seq| WalRecord {
                cluster_id: 183,
                shard_id: 184,
                flags: 0,
                epoch: writer.epoch(),
                shard_seq: seq,
                engine_time: 0,
                command_id: u128::from(seq),
                payload: vec![7],
            })
            .collect();
        for record in &records {
            writer.append(record).unwrap();
        }
        let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
        let appender =
            RegisteredAppender::new(SegmentWriter::new(&wal, 1024 * 1024).unwrap(), manager)
                .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: true,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut batcher =
            DurableBatcher::new_with_config(appender, config, writer.epoch()).unwrap();
        batcher
            .append_at(&records[0], std::time::Instant::now())
            .unwrap();
        drop(batcher);
        let mut manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
        let before_a = std::fs::read(manifest.join("manifest-a")).unwrap();
        let before_b = std::fs::read(manifest.join("manifest-b")).unwrap();
        let wal_file = wal.join("00000000000000000001.wal");
        let initial_len = std::fs::metadata(&wal_file).unwrap().len();
        let full_len: u64 = records
            .iter()
            .map(|record| u64::try_from(record.encode().unwrap().len()).unwrap())
            .sum();
        let lease = writer.lease;
        let until = std::time::Instant::now() + Duration::from_secs(60);
        let done = std::sync::atomic::AtomicBool::new(false);
        let (result, observed_len) = std::thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                loop {
                    let a = std::fs::read(manifest.join("manifest-a"));
                    let b = std::fs::read(manifest.join("manifest-b"));
                    if a.as_ref().is_ok_and(|bytes| bytes != &before_a)
                        || b.as_ref().is_ok_and(|bytes| bytes != &before_b)
                    {
                        client.revoke_lease(lease).unwrap();
                        break std::fs::metadata(&wal_file).unwrap().len();
                    }
                    assert!(
                        !done.load(std::sync::atomic::Ordering::Acquire),
                        "补齐在撤销前结束"
                    );
                    assert!(std::time::Instant::now() < until, "等待清单更新超时");
                    std::thread::sleep(Duration::from_millis(1));
                }
            });
            let result =
                writer.extend_verified_local_prefix(&wal, &mut manager, 1024 * 1024, until);
            done.store(true, std::sync::atomic::Ordering::Release);
            (result, watcher.join().unwrap())
        });
        assert!(observed_len > initial_len, "撤销前没有同步后缀");
        assert!(observed_len < full_len, "撤销发生时补齐已经写完");
        let error = result.unwrap_err();
        assert_ne!(
            error.kind(),
            io::ErrorKind::TimedOut,
            "补齐因超时而不是失权停止"
        );
        eprintln!("撤销完成时 WAL 字节数 {observed_len}/{full_len}，初始字节数 {initial_len}");
        assert!(writer.is_locally_invalidated());
        drop(manager);
        let local = matching_persistence::recover_local(&wal, &manifest, 0, 183, 184).unwrap();
        assert_eq!(local, records);
        let mut successor = log
            .acquire_bound(30, include_bytes!("../tests/fixtures/test-client.der"))
            .unwrap();
        successor
            .verify_reconciliation_prefix(
                &local,
                512,
                std::time::Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
        let mut next_record = records.last().unwrap().clone();
        next_record.shard_seq = 513;
        next_record.command_id = 513;
        assert!(writer.append(&next_record).is_err());
        assert_eq!(log.committed().unwrap(), 512);
        next_record.epoch = successor.epoch();
        successor.append(&next_record).unwrap();
        assert_eq!(log.read(513).unwrap(), Some(next_record));
        successor.revoke().unwrap();
        let target = root.canonicalize().unwrap();
        assert_eq!(
            target.parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tide-extend-interleave-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }

    #[test]
    #[ignore = "需要独立本地 etcd 测试集群"]
    fn lease_lost_after_prefix_validation_closes_writer_and_allows_reconciliation() {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
            Profile, RegisteredAppender, ReplicationMode, SegmentWriter,
        };
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("tide-extend-fence-{}-{stamp}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("wal")).unwrap();
        std::fs::create_dir(root.join("manifest")).unwrap();
        let log = FencedLog::new(
            client,
            &format!("/tide-test/extend-fence/{}-{stamp}", std::process::id()),
            ClusterId(181),
            ShardId(182),
        )
        .unwrap();
        let mut writer = log
            .acquire_bound(30, include_bytes!("../tests/fixtures/test-client.der"))
            .unwrap();
        let records: Vec<_> = (1..=3)
            .map(|seq| WalRecord {
                cluster_id: 181,
                shard_id: 182,
                flags: 0,
                epoch: writer.epoch(),
                shard_seq: seq,
                engine_time: 0,
                command_id: u128::from(seq),
                payload: vec![7],
            })
            .collect();
        for record in &records {
            writer.append(record).unwrap();
        }
        let manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
        let appender =
            RegisteredAppender::new(SegmentWriter::new(root.join("wal"), 4096).unwrap(), manager)
                .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: true,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let mut batcher =
            DurableBatcher::new_with_config(appender, config, writer.epoch()).unwrap();
        batcher
            .append_at(&records[0], std::time::Instant::now())
            .unwrap();
        drop(batcher);
        let mut manager =
            ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
        writer.simulate_revoke_during_extend = true;
        let deadline = || std::time::Instant::now() + Duration::from_secs(15);
        assert!(
            writer
                .extend_verified_local_prefix(root.join("wal"), &mut manager, 4096, deadline())
                .is_err()
        );
        assert!(writer.is_locally_invalidated());
        assert_eq!(manager.state().local_durable_seq, 3);
        drop(manager);
        assert_eq!(
            matching_persistence::recover_local(
                root.join("wal"),
                root.join("manifest"),
                0,
                181,
                182
            )
            .unwrap(),
            records
        );
        let mut successor = log
            .acquire_bound(30, include_bytes!("../tests/fixtures/test-client.der"))
            .unwrap();
        successor
            .verify_reconciliation_prefix(&records, 3, deadline())
            .unwrap();
        successor.revoke().unwrap();
        let target = root.canonicalize().unwrap();
        assert_eq!(
            target.parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tide-extend-fence-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }

    #[test]
    #[ignore = "需要独立本地 etcd 测试集群"]
    fn lost_adoption_response_reconciles_partial_prefix_after_new_lease() {
        use matching_persistence::{
            DurabilityConfig, DurabilityMode, DurableBatcher, FencedStandbyReplica,
            FileManifestStorage, ManifestManager, Profile, RegisteredAppender, ReplicaEpochStore,
            ReplicationMode, SegmentWriter, StandbyReplica,
        };
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
        let prefix = format!("/tide-test/adopt-response/{}", std::process::id());
        let log = FencedLog::new(client, &prefix, ClusterId(73), ShardId(74)).unwrap();
        let mut first = log.acquire(10).unwrap();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tide-adopt-response-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("wal")).unwrap();
        std::fs::create_dir(root.join("manifest")).unwrap();
        let manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
        let appender =
            RegisteredAppender::new(SegmentWriter::new(root.join("wal"), 4096).unwrap(), manager)
                .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Development,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::LocalDurable,
            crash_recovery_required: true,
            ha_enabled: false,
            allow_data_loss_on_failover: false,
        };
        let batcher = DurableBatcher::new_with_config(appender, config, 1).unwrap();
        let replica = StandbyReplica::new(batcher, 73, 74, 1, 8).unwrap();
        let epochs = ReplicaEpochStore::create(root.join("epoch"), 73, 74, 1, 0).unwrap();
        let mut standby = FencedStandbyReplica::new(replica, epochs).unwrap();
        let deadline = || std::time::Instant::now() + Duration::from_secs(3);
        crate::activate_replica_writer(&log, &mut standby, first.replica_identity(), deadline())
            .unwrap();
        standby
            .confirm_reconciled(0, |_, _, _| {
                log.verify_replica_writer(first.replica_identity(), deadline())
            })
            .unwrap();
        let records: Vec<_> = (1..=2)
            .map(|seq| WalRecord {
                cluster_id: 73,
                shard_id: 74,
                flags: 0,
                epoch: 1,
                shard_seq: seq,
                engine_time: 0,
                command_id: u128::from(seq),
                payload: vec![7],
            })
            .collect();
        for record in &records {
            first.prepare(record).unwrap();
        }
        standby
            .append_batch(&records, std::time::Instant::now())
            .unwrap();
        first.revoke().unwrap();
        let mut second = log.acquire(10).unwrap();
        assert!(
            second
                .reconcile_records(&records[1..], 0, 2, deadline())
                .is_err()
        );
        assert_eq!(log.committed().unwrap(), 0);
        assert!(
            second
                .reconcile_records(&records[..1], 0, 2, deadline())
                .is_err()
        );
        assert_eq!(log.committed().unwrap(), 0);
        second.simulate_lost_commit = true;
        assert!(
            second
                .reconcile_local_standby(&mut standby, 0, deadline())
                .is_err()
        );
        assert!(second.is_locally_invalidated());
        assert_eq!(log.committed().unwrap(), 1);
        second.revoke().unwrap();
        let mut third = log.acquire(10).unwrap();
        assert_eq!(
            third
                .reconcile_local_standby(&mut standby, 0, deadline())
                .unwrap(),
            2
        );
        assert_eq!(log.read(1).unwrap().as_ref(), records.first());
        assert_eq!(log.read(2).unwrap().as_ref(), records.get(1));
        third.revoke().unwrap();
        drop(standby);
        let target = root.canonicalize().unwrap();
        assert_eq!(
            target.parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tide-adopt-response-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }

    #[test]
    #[ignore = "需要独立本地 etcd 测试集群"]
    fn lost_commit_response_is_recovered_from_authoritative_prefix() {
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("/tide-test/fenced-response/{}-{stamp}", std::process::id());
        let log = FencedLog::new(client, &prefix, ClusterId(71), ShardId(72)).unwrap();
        let expired = std::time::Instant::now();
        assert_eq!(
            log.committed_until(expired).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            log.read_until(1, expired).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        let mut first = log.acquire(10).unwrap();
        first.simulate_lost_commit = true;
        let make = |epoch, seq| WalRecord {
            cluster_id: 71,
            shard_id: 72,
            flags: 0,
            epoch,
            shard_seq: seq,
            engine_time: 0,
            command_id: u128::from(seq),
            payload: vec![7; 8],
        };
        let committed = make(1, 1);
        assert!(first.append(&committed).is_err());
        assert!(first.is_locally_invalidated());
        assert_eq!(log.committed().unwrap(), 1);
        assert_eq!(log.read(1).unwrap(), Some(committed));
        first.revoke().unwrap();
        let mut next = log.acquire(10).unwrap();
        assert_eq!(next.epoch(), 2);
        assert_eq!(next.append(&make(2, 2)).unwrap(), 2);
        next.revoke().unwrap();
        let mut unbound = log.acquire(10).unwrap();
        assert_eq!(
            unbound
                .verify_bound_lease(std::time::Instant::now() + Duration::from_secs(3))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(unbound.is_locally_invalidated());
        unbound.revoke().unwrap();
    }
}
