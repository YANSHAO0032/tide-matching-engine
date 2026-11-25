//! 按集群、分片和消费者隔离的持久游标与发布者围栏。
use crate::EtcdClient;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use matching_domain::{ClusterId, ShardId};
use serde_json::{Value, json};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// 一个分片内最后获得持久确认的事件位置。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CursorPosition {
    /// 事件所属的严格正分片序号。
    pub shard_seq: u64,
    /// 命令内事件索引。
    pub event_index: u32,
}

/// 单个消费者的持久确认位置和版本。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorCheckpoint {
    /// 最后持久确认的事件位置。
    pub position: CursorPosition,
    /// 最后推进该位置的发布者纪元。
    pub publisher_epoch: u64,
    /// 此消费者游标的严格递增版本。
    pub generation: u64,
}

/// 持久配置的必需消费者集合及其版本。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredSinks {
    /// 集合的严格递增版本。
    pub generation: u64,
    /// 严格递增且不重复的消费者身份。
    pub sinks: Vec<u64>,
}

const MAX_SINKS: usize = 4096;

/// 由控制存储授予的发布者身份；旧令牌仍须通过存储端原子校验。
#[derive(Clone, Debug)]
pub struct PublisherToken {
    /// 令牌绑定的集群分片命名空间。
    namespace: String,
    /// 控制存储授予的租约身份。
    lease_id: i64,
    /// 持久递增的发布者纪元。
    epoch: u64,
    /// 本进程中所有副本共享的不可恢复失效标记。
    invalidated: Arc<AtomicBool>,
}
impl PublisherToken {
    /// 返回令牌绑定的发布者纪元。
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// 返回本进程是否已发现失权；未失效不代表远端租约仍有效。
    pub fn is_locally_invalidated(&self) -> bool {
        self.invalidated.load(Ordering::Acquire)
    }
}

/// 持有的远程发布者租约；调用方须按时刷新并显式撤销。
pub struct PublisherLease {
    /// 执行租约操作的线性一致客户端。
    client: EtcdClient,
    /// 可供游标事务验证的身份。
    token: PublisherToken,
    /// 是否已经收到撤销成功响应。
    revoked: bool,
}

/// 单集群、单分片下所有消费者游标的存储入口。
pub struct CursorStore {
    /// 执行事务和一致性读取的客户端。
    client: EtcdClient,
    /// 精确绑定的集群分片命名空间。
    namespace: String,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn namespace(prefix: &str, cluster: ClusterId, shard: ShardId) -> io::Result<String> {
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
            "控制存储前缀无效",
        ));
    }
    Ok(format!(
        "{prefix}/cluster/{:04x}/shard/{:04x}",
        cluster.get(),
        shard.get()
    ))
}
fn owner_key(ns: &str) -> String {
    format!("{ns}/publisher/owner")
}
fn epoch_key(ns: &str) -> String {
    format!("{ns}/publisher/epoch")
}
fn cursor_key(ns: &str, sink: u64) -> String {
    format!("{ns}/cursor/{sink:016x}")
}
fn value_compare(key: &str, value: &[u8]) -> Value {
    json!({"key":STANDARD.encode(key),"target":"VALUE","value":STANDARD.encode(value),"result":"EQUAL"})
}
fn version_compare(key: &str, revision: Option<u64>) -> Value {
    match revision {
        None => {
            json!({"key":STANDARD.encode(key),"target":"VERSION","version":"0","result":"EQUAL"})
        }
        Some(revision) => {
            json!({"key":STANDARD.encode(key),"target":"MOD","mod_revision":revision.to_string(),"result":"EQUAL"})
        }
    }
}
fn put(key: &str, value: &[u8], lease: Option<i64>) -> Value {
    let mut request = json!({"key":STANDARD.encode(key),"value":STANDARD.encode(value)});
    if let Some(lease) = lease {
        request["lease"] = Value::String(lease.to_string());
    }
    json!({"request_put":request})
}
fn epoch_value(bytes: &[u8]) -> io::Result<u64> {
    let epoch = u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("发布者纪元格式无效"))?,
    );
    if epoch == 0 {
        return Err(invalid("发布者纪元为零"));
    }
    Ok(epoch)
}
fn abort(client: &EtcdClient, lease: i64, cause: io::Error) -> io::Error {
    match client.revoke_lease(lease) {
        Ok(()) => cause,
        Err(_) => io::Error::other("租约申请未完成且撤销结果未知"),
    }
}

impl PublisherLease {
    /// 获取独占租约并在同一事务中递增分片发布者纪元。
    pub fn acquire(
        client: EtcdClient,
        prefix: &str,
        cluster: ClusterId,
        shard: ShardId,
        ttl_seconds: u64,
    ) -> io::Result<Self> {
        let namespace = namespace(prefix, cluster, shard)?;
        let owner = owner_key(&namespace);
        let epoch = epoch_key(&namespace);
        let lease = client.grant_lease(ttl_seconds)?;
        let result = (|| {
            for _ in 0..8 {
                let current = client.get(epoch.as_bytes())?;
                let (revision, previous) = match current {
                    Some((revision, bytes)) => (Some(revision), epoch_value(&bytes)?),
                    None => (None, 0),
                };
                let next = previous
                    .checked_add(1)
                    .ok_or_else(|| invalid("发布者纪元耗尽"))?;
                let comparisons = vec![
                    version_compare(&owner, None),
                    version_compare(&epoch, revision),
                ];
                let writes = vec![
                    put(&epoch, &next.to_be_bytes(), None),
                    put(&owner, &lease.to_be_bytes(), Some(lease)),
                ];
                if client.transaction(comparisons, writes)?.is_some() {
                    return Ok(Self {
                        client: client.clone(),
                        token: PublisherToken {
                            namespace: namespace.clone(),
                            lease_id: lease,
                            epoch: next,
                            invalidated: Arc::new(AtomicBool::new(false)),
                        },
                        revoked: false,
                    });
                }
                if client.get(owner.as_bytes())?.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "已有发布者租约",
                    ));
                }
            }
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "发布者竞争次数超限",
            ))
        })();
        result.map_err(|error| abort(&client, lease, error))
    }
    /// 返回可用于原子校验的令牌副本；令牌本身不保证租约仍有效。
    pub fn token(&self) -> PublisherToken {
        self.token.clone()
    }
    /// 刷新远程租约；失败时调用方须停止发布。
    pub fn keep_alive(&self) -> io::Result<()> {
        if self.revoked || self.token.is_locally_invalidated() {
            return Err(io::Error::other("发布者租约已撤销"));
        }
        let result = self.client.keep_lease(self.token.lease_id);
        if result.is_err() {
            self.token.invalidated.store(true, Ordering::Release);
        }
        result
    }
    /// 显式撤销租约；仅收到远程成功响应后标记完成。
    pub fn revoke(&mut self) -> io::Result<()> {
        if self.revoked {
            return Ok(());
        }
        self.token.invalidated.store(true, Ordering::Release);
        self.client.revoke_lease(self.token.lease_id)?;
        self.revoked = true;
        Ok(())
    }
    /// 线性一致核验租约和发布者纪元；发布前仍须遵循外围 fencing 协议。
    pub fn verify(&self) -> io::Result<()> {
        if self.revoked || self.token.is_locally_invalidated() {
            return Err(io::Error::other("发布者租约已撤销"));
        }
        let comparisons = token_compares(&self.token);
        let result = self.client.transaction(comparisons, vec![]);
        if !matches!(result, Ok(Some(_))) {
            self.token.invalidated.store(true, Ordering::Release);
        }
        if result?.is_none() {
            return Err(io::Error::other("发布者租约已失效"));
        }
        Ok(())
    }
}

fn token_compares(token: &PublisherToken) -> Vec<Value> {
    vec![
        value_compare(&owner_key(&token.namespace), &token.lease_id.to_be_bytes()),
        value_compare(&epoch_key(&token.namespace), &token.epoch.to_be_bytes()),
    ]
}

impl CursorStore {
    /// 绑定一个集群与分片的持久消费者游标命名空间。
    pub fn new(
        client: EtcdClient,
        prefix: &str,
        cluster: ClusterId,
        shard: ShardId,
    ) -> io::Result<Self> {
        Ok(Self {
            client,
            namespace: namespace(prefix, cluster, shard)?,
        })
    }
    /// 线性一致读取消费者最近持久确认的位置。
    pub fn load(&self, sink: u64) -> io::Result<Option<CursorCheckpoint>> {
        self.client
            .get(cursor_key(&self.namespace, sink).as_bytes())?
            .map(|(_, bytes)| decode(&bytes))
            .transpose()
    }
    /// 线性一致读取管理员持久配置的必需消费者集合。
    pub fn load_required_sinks(&self) -> io::Result<Option<RequiredSinks>> {
        let key = format!("{}/required-sinks", self.namespace);
        self.client
            .get(key.as_bytes())?
            .map(|(_, bytes)| decode_sinks(&bytes))
            .transpose()
    }

    /// 以版本比较写入必需消费者集合；调用方负责管理员授权。
    pub fn set_required_sinks(
        &self,
        expected: Option<u64>,
        sinks: &[u64],
    ) -> io::Result<RequiredSinks> {
        if sinks.len() > MAX_SINKS || sinks.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "消费者集合无效",
            ));
        }
        let key = format!("{}/required-sinks", self.namespace);
        let current = self.client.get(key.as_bytes())?;
        let (version, generation) = match current {
            None if expected.is_none() => (crate::KvVersion::Absent, 1),
            Some((revision, bytes)) => {
                let current = decode_sinks(&bytes)?;
                if expected != Some(current.generation) {
                    return Err(io::Error::other("消费者集合版本冲突"));
                }
                (
                    crate::KvVersion::Revision(revision),
                    current
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| invalid("消费者集合版本耗尽"))?,
                )
            }
            None => return Err(io::Error::other("消费者集合版本冲突")),
        };
        let next = RequiredSinks {
            generation,
            sinks: sinks.to_vec(),
        };
        if self
            .client
            .compare_and_swap(key.as_bytes(), version, &encode_sinks(&next)?)?
            .is_none()
        {
            return Err(io::Error::other("消费者集合版本冲突"));
        }
        Ok(next)
    }
    /// 比较既有版本并推进游标，事务同时校验发布者租约与纪元。
    pub fn advance(
        &self,
        token: &PublisherToken,
        sink: u64,
        expected: Option<u64>,
        position: CursorPosition,
    ) -> io::Result<CursorCheckpoint> {
        if token.namespace != self.namespace
            || token.is_locally_invalidated()
            || position.shard_seq == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "游标身份或位置无效",
            ));
        }
        let key = cursor_key(&self.namespace, sink);
        let existing = self.client.get(key.as_bytes()).inspect_err(|_| {
            token.invalidated.store(true, Ordering::Release);
        })?;
        let (revision, generation) = match existing {
            Some((revision, bytes)) => {
                let current = decode(&bytes).inspect_err(|_| {
                    token.invalidated.store(true, Ordering::Release);
                })?;
                if current.publisher_epoch > token.epoch {
                    token.invalidated.store(true, Ordering::Release);
                    return Err(io::Error::other("发布者纪元已失效"));
                }
                if expected != Some(current.generation) || position < current.position {
                    return Err(io::Error::other("游标版本冲突或位置倒退"));
                }
                (
                    Some(revision),
                    current
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| invalid("游标版本耗尽"))?,
                )
            }
            None => {
                if expected.is_some() {
                    return Err(io::Error::other("游标版本冲突"));
                }
                (None, 1)
            }
        };
        let checkpoint = CursorCheckpoint {
            position,
            publisher_epoch: token.epoch,
            generation,
        };
        let mut comparisons = token_compares(token);
        comparisons.push(version_compare(&key, revision));
        let result = self
            .client
            .transaction(comparisons, vec![put(&key, &encode(checkpoint), None)]);
        if result.is_err() {
            token.invalidated.store(true, Ordering::Release);
        }
        if result?.is_none() {
            if !matches!(
                self.client.transaction(token_compares(token), vec![]),
                Ok(Some(_))
            ) {
                token.invalidated.store(true, Ordering::Release);
            }
            return Err(io::Error::other("游标冲突或发布者租约已失效"));
        }
        Ok(checkpoint)
    }
}

fn encode_sinks(value: &RequiredSinks) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::from(&b"RSC1"[..]);
    bytes.extend_from_slice(&value.generation.to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(value.sinks.len())
            .map_err(|_| invalid("消费者集合超限"))?
            .to_be_bytes(),
    );
    for sink in &value.sinks {
        bytes.extend_from_slice(&sink.to_be_bytes());
    }
    bytes.extend_from_slice(&matching_persistence::crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

fn decode_sinks(bytes: &[u8]) -> io::Result<RequiredSinks> {
    if bytes.len() < 20 || &bytes[..4] != b"RSC1" {
        return Err(invalid("消费者集合记录损坏"));
    }
    let count = u32::from_be_bytes(
        bytes[12..16]
            .try_into()
            .map_err(|_| invalid("消费者数量无效"))?,
    ) as usize;
    if count > MAX_SINKS
        || 20_usize.checked_add(
            count
                .checked_mul(8)
                .ok_or_else(|| invalid("消费者集合超限"))?,
        ) != Some(bytes.len())
    {
        return Err(invalid("消费者集合长度无效"));
    }
    let end = bytes.len() - 4;
    let crc = u32::from_be_bytes(
        bytes[end..]
            .try_into()
            .map_err(|_| invalid("消费者集合校验值无效"))?,
    );
    if matching_persistence::crc32c(&bytes[..end]) != crc {
        return Err(invalid("消费者集合校验失败"));
    }
    let generation = u64::from_be_bytes(
        bytes[4..12]
            .try_into()
            .map_err(|_| invalid("消费者集合版本无效"))?,
    );
    let mut sinks = Vec::with_capacity(count);
    for chunk in bytes[16..end].as_chunks::<8>().0 {
        sinks.push(u64::from_be_bytes(*chunk));
    }
    if generation == 0 || sinks.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("消费者集合记录无效"));
    }
    Ok(RequiredSinks { generation, sinks })
}

fn encode(checkpoint: CursorCheckpoint) -> [u8; 36] {
    let mut bytes = [0_u8; 36];
    bytes[..4].copy_from_slice(b"CPC1");
    bytes[4..12].copy_from_slice(&checkpoint.position.shard_seq.to_be_bytes());
    bytes[12..16].copy_from_slice(&checkpoint.position.event_index.to_be_bytes());
    bytes[16..24].copy_from_slice(&checkpoint.publisher_epoch.to_be_bytes());
    bytes[24..32].copy_from_slice(&checkpoint.generation.to_be_bytes());
    let crc = matching_persistence::crc32c(&bytes[..32]);
    bytes[32..].copy_from_slice(&crc.to_be_bytes());
    bytes
}
fn decode(bytes: &[u8]) -> io::Result<CursorCheckpoint> {
    if bytes.len() != 36
        || &bytes[..4] != b"CPC1"
        || matching_persistence::crc32c(&bytes[..32])
            != u32::from_be_bytes(
                bytes[32..36]
                    .try_into()
                    .map_err(|_| invalid("游标校验值无效"))?,
            )
    {
        return Err(invalid("游标记录损坏"));
    }
    let position = CursorPosition {
        shard_seq: u64::from_be_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| invalid("游标序号无效"))?,
        ),
        event_index: u32::from_be_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| invalid("游标索引无效"))?,
        ),
    };
    let publisher_epoch = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| invalid("发布者纪元无效"))?,
    );
    let generation = u64::from_be_bytes(
        bytes[24..32]
            .try_into()
            .map_err(|_| invalid("游标版本无效"))?,
    );
    if position.shard_seq == 0 || publisher_epoch == 0 || generation == 0 {
        return Err(invalid("游标记录字段无效"));
    }
    Ok(CursorCheckpoint {
        position,
        publisher_epoch,
        generation,
    })
}
