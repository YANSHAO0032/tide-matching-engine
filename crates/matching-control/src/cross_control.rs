//! 跨分片控制协调者的持久操作、租约和准入围栏。
mod codec;

use crate::EtcdClient;
use crate::etcd::{VersionedValue, put, value_compare, version_compare};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use codec::{Scope, decode_index, decode_record, encode_index, encode_record};
use matching_domain::{ClusterId, CommandId, RejectReason, ShardId, UserId};
use matching_runtime::cross_control::{
    ControlAck, ControlAdmission, ControlAuthority, ControlKind, ControlOperation, ControlPhase,
    ControlUpdate,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

const MAX_PENDING: usize = 4096;
const UNBOUND: &[u8] = b"CCS2";
const BOUND: &[u8] = b"CCS2M";

/// 返回命名空间标记的修改版本，以及是否已完成首次拓扑登记。
fn topology_marker(marker: &VersionedValue) -> io::Result<(u64, bool)> {
    match marker {
        Some((revision, value)) if value == UNBOUND => Ok((*revision, false)),
        Some((revision, value)) if value == BOUND => Ok((*revision, true)),
        _ => Err(invalid("控制命名空间的格式或拓扑标记无效")),
    }
}

/// 范围状态与见证键共享同次事务的值、修改版本。只有两者均未出现才能采用初始状态。
fn checked_scope(
    key: &str,
    state: VersionedValue,
    witness: VersionedValue,
) -> io::Result<(Option<u64>, Scope)> {
    if state != witness {
        return Err(invalid("控制范围与持久见证不一致"));
    }
    let scope = Scope::decode(key, state.as_ref().map(|(_, value)| value.as_slice()))?;
    Ok((state.map(|(revision, _)| revision), scope))
}

/// 将范围及其见证写入同一事务；两份字节都绑定原范围键。
fn scope_writes(key: &str, scope: &Scope) -> [Value; 2] {
    let bytes = scope.encode(key);
    [
        put(key, &bytes, None),
        put(&format!("{key}/witness"), &bytes, None),
    ]
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn conflict() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "协调者已失权或控制记录已变化")
}
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "当前客户端未持有协调者租约",
    )
}

/// 协调者租约和创建它的 etcd 修订，后者是严格递增的所有者纪元。
struct Owner {
    /// 服务端授予的租约身份，随 TTL 到期或显式撤销失效。
    lease: i64,
    /// 独占取得所有者键时的修改版本。
    epoch: u64,
}

/// 跨分片控制存储。同一部署的网关和协调者应绑定同一命名空间。
pub struct EtcdControlStore {
    /// 执行线性一致事务的外围客户端。
    client: EtcdClient,
    /// 部署配置提供的业务集群身份。
    cluster: ClusterId,
    /// 业务集群在该 etcd 集群上的键前缀。
    path: String,
    /// 同时绑定 etcd 集群及业务集群的准入身份。
    identity: String,
    /// 写者持有独占租约，网关读者为空。
    owner: Option<Owner>,
}

impl EtcdControlStore {
    fn scoped(client: EtcdClient, prefix: &str, cluster: ClusterId) -> io::Result<Self> {
        if prefix.len() > 256
            || !prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains("//")
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "无效的控制存储前缀",
            ));
        }
        let path = format!("{prefix}/cluster/{:04x}/cross-control", cluster.get());
        let identity = format!("etcd/{:016x}{path}", client.cluster_id()?);
        Ok(Self {
            client,
            cluster,
            path,
            identity,
            owner: None,
        })
    }

    /// 打开已有部署的准入读者。启动时核对格式和持久索引，范围在每次准入时核对。
    pub fn open(client: EtcdClient, prefix: &str, cluster: ClusterId) -> io::Result<Self> {
        let store = Self::scoped(client, prefix, cluster)?;
        let key = store.index_key();
        let [marker, index] = store.client.get_keys([&store.marker_key(), &key])?;
        topology_marker(&marker)?;
        let (_, bytes) = index.ok_or_else(|| invalid("未完成操作索引不存在"))?;
        decode_index(&key, &bytes)?;
        Ok(store)
    }

    /// 获取独占租约。首次创建命名空间时，未完成索引与所有者原子建立。
    pub fn acquire(
        client: EtcdClient,
        prefix: &str,
        cluster: ClusterId,
        ttl_seconds: u64,
    ) -> io::Result<Self> {
        let mut store = Self::scoped(client, prefix, cluster)?;
        let key = store.index_key();
        let marker_key = store.marker_key();
        let [marker, index] = store.client.get_keys([&marker_key, &key])?;
        let mut compares = Vec::new();
        if let Some((_, bytes)) = &index {
            decode_index(&key, bytes)?;
            let (revision, _) = topology_marker(&marker)?;
            compares.push(version_compare(&marker_key, Some(revision)));
        }
        let owner = store.owner_key();
        let lease = store.client.grant_lease(ttl_seconds)?;
        let mut writes = vec![put(&owner, &lease.to_be_bytes(), Some(lease))];
        compares.push(version_compare(&owner, None));
        if let Some((revision, _)) = index {
            compares.push(version_compare(&key, Some(revision)));
        } else {
            // 对整个键前缀比较 VERSION=0。索引意外丢失时，初始化标记、范围或历史操作任一存在即拒绝。
            let mut empty_namespace = version_compare(&format!("{}/", store.path), None);
            empty_namespace["range_end"] =
                Value::String(STANDARD.encode(format!("{}0", store.path)));
            compares.push(empty_namespace);
            writes.push(put(&marker_key, UNBOUND, None));
            writes.push(put(&key, &encode_index(&key, &BTreeSet::new()), None));
        }
        let result = store.client.transaction(compares, writes);
        match result {
            Ok(Some(epoch)) => {
                store.owner = Some(Owner { lease, epoch });
                Ok(store)
            }
            result => {
                store.client.revoke_lease(lease)?;
                result?;
                if store.client.get(owner.as_bytes())?.is_some() {
                    Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "已有协调者持有租约",
                    ))
                } else if store.client.get(key.as_bytes())?.is_none() {
                    Err(invalid("控制命名空间已有数据但恢复索引缺失"))
                } else {
                    Err(conflict())
                }
            }
        }
    }
    /// 应在服务器 TTL 到期前续租，失败后每次后续推进仍在事务内核对所有者。
    pub fn keep_alive(&self) -> io::Result<()> {
        self.client
            .keep_lease(self.owner.as_ref().ok_or_else(unavailable)?.lease)
    }
    /// 撤销自己的远端租约，保留所有范围围栏与未完成操作。
    pub fn revoke(&self) -> io::Result<()> {
        self.client
            .revoke_lease(self.owner.as_ref().ok_or_else(unavailable)?.lease)
    }
    /// 返回该写者获取租约时的纪元；读取纪元不保证当前仍有写权。
    pub fn owner_epoch(&self) -> Option<u64> {
        self.owner.as_ref().map(|o| o.epoch)
    }

    fn owner_key(&self) -> String {
        format!("{}/owner", self.path)
    }
    fn index_key(&self) -> String {
        format!("{}/pending", self.path)
    }
    fn marker_key(&self) -> String {
        format!("{}/initialized", self.path)
    }
    fn record_key(&self, parent: CommandId) -> String {
        format!("{}/op/{:032x}", self.path, parent.0)
    }
    fn scope_key(&self, user: Option<UserId>) -> String {
        match user {
            None => format!("{}/global", self.path),
            Some(user) => format!("{}/user/{:016x}", self.path, user.0),
        }
    }
    fn owner_compares(&self) -> io::Result<Vec<Value>> {
        let owner = self.owner.as_ref().ok_or_else(unavailable)?;
        let key = self.owner_key();
        Ok(vec![
            value_compare(&key, &owner.lease.to_be_bytes()),
            version_compare(&key, Some(owner.epoch)),
        ])
    }
    fn index(&self) -> io::Result<(u64, BTreeSet<CommandId>)> {
        let key = self.index_key();
        let (revision, bytes) = self
            .client
            .get(key.as_bytes())?
            .ok_or_else(|| invalid("未完成操作索引不存在"))?;
        Ok((revision, decode_index(&key, &bytes)?))
    }
    fn owned_value(&self, key: &str) -> io::Result<Option<(u64, Vec<u8>)>> {
        let owner = self.owner.as_ref().ok_or_else(unavailable)?;
        self.client.get_if_value(
            self.owner_key().as_bytes(),
            &owner.lease.to_be_bytes(),
            key.as_bytes(),
        )
    }
    fn scopes(&self, user: UserId) -> io::Result<[Scope; 2]> {
        let user_key = self.scope_key(Some(user));
        let global_key = self.scope_key(None);
        let [marker, local, local_witness, global, global_witness] = self.client.get_keys([
            &self.marker_key(),
            &user_key,
            &format!("{user_key}/witness"),
            &global_key,
            &format!("{global_key}/witness"),
        ])?;
        topology_marker(&marker)?;
        Ok([
            checked_scope(&user_key, local, local_witness)?.1,
            checked_scope(&global_key, global, global_witness)?.1,
        ])
    }
    /// 读取一次范围写入的旧状态，把格式、范围及见证的版本一并加入写入条件。
    fn scope_for_update(
        &self,
        key: &str,
        compares: &mut Vec<Value>,
    ) -> io::Result<(Option<u64>, Scope)> {
        let marker_key = self.marker_key();
        let witness_key = format!("{key}/witness");
        let [marker, state, witness] = self.client.get_keys([&marker_key, key, &witness_key])?;
        let (marker_revision, _) = topology_marker(&marker)?;
        let (revision, scope) = checked_scope(key, state, witness)?;
        compares.extend([
            version_compare(&marker_key, Some(marker_revision)),
            version_compare(key, revision),
            version_compare(&witness_key, revision),
        ]);
        Ok((revision, scope))
    }
    /// 在同一线性一致切面返回已发布的用户纪元及全局纪元；临时围栏由准入接口单独判定。
    pub fn current_epochs(&self, user: UserId) -> io::Result<(u64, u64)> {
        let [local, global] = self.scopes(user)?;
        Ok((local.epoch, global.epoch))
    }
    /// 在停写窗口导入该用户跨分片的正数纪元下限。范围未持栅栏且开关关闭时可重试相同值。
    pub fn import_user_epoch(&self, user: UserId, floor: u64) -> io::Result<()> {
        if floor == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "历史纪元必须为正",
            ));
        }
        let key = self.scope_key(Some(user));
        let mut compares = self.owner_compares()?;
        let (revision, mut scope) = self.scope_for_update(&key, &mut compares)?;
        let writes = if revision.is_some() {
            if scope.epoch != floor || scope.fence.is_some() || scope.killed {
                return Err(conflict());
            }
            Vec::new()
        } else {
            scope.epoch = floor;
            scope_writes(&key, &scope).into()
        };
        self.client
            .transaction(compares, writes)?
            .ok_or_else(conflict)?;
        Ok(())
    }

    fn confirm(
        expected: &ControlOperation,
        stored: ControlOperation,
    ) -> io::Result<ControlOperation> {
        if stored.kind == expected.kind && stored.targets == expected.targets {
            Ok(stored)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "重复父命令的参数不一致",
            ))
        }
    }
    fn confirm_existing(&self, expected: &ControlOperation) -> io::Result<ControlOperation> {
        let key = self.record_key(expected.parent);
        let (_, bytes) = self.owned_value(&key)?.ok_or_else(conflict)?;
        Self::confirm(expected, decode_record(&key, expected.parent, &bytes)?)
    }
    fn change_scope(
        &self,
        next: &mut ControlOperation,
        compares: &mut Vec<Value>,
        writes: &mut Vec<Value>,
    ) -> io::Result<()> {
        let user = match next.kind {
            ControlKind::UserMassCancel(user) | ControlKind::UserKill { user, .. } => Some(user),
            _ => None,
        };
        let key = self.scope_key(user);
        let (_, mut scope) = self.scope_for_update(&key, compares)?;
        let reserved = scope
            .epoch
            .checked_add(1)
            .ok_or_else(|| invalid("控制纪元已耗尽"))?;
        if next.phase == ControlPhase::Fencing {
            if scope.fence.is_some() {
                return Err(conflict());
            }
            scope.fence = Some(next.parent);
            next.epoch = reserved;
        } else {
            if scope.fence != Some(next.parent) || next.epoch != reserved {
                return Err(invalid("释放围栏的身份或纪元不符"));
            }
            scope.fence = None;
            if next.phase == ControlPhase::Released {
                scope.epoch = next.epoch;
                if let ControlKind::UserKill { active, .. }
                | ControlKind::GlobalKill { active, .. } = next.kind
                {
                    scope.killed = active;
                }
            }
        }
        writes.extend(scope_writes(&key, &scope));
        Ok(())
    }
}
impl ControlAdmission for EtcdControlStore {
    fn cluster_id(&self) -> ClusterId {
        self.cluster
    }
    fn namespace(&self) -> String {
        self.identity.clone()
    }
    fn check(&self, user: UserId) -> io::Result<(u64, u64, Option<RejectReason>)> {
        let [local, global] = self.scopes(user)?;
        let reason = if local.fence.is_some() || global.fence.is_some() {
            Some(RejectReason::ControlFenceActive)
        } else if local.killed || global.killed {
            Some(RejectReason::KillSwitchActive)
        } else {
            None
        };
        Ok((global.epoch, local.epoch, reason))
    }
}

impl ControlAuthority for EtcdControlStore {
    fn prepare(
        &self,
        parent: CommandId,
        kind: ControlKind,
        targets: &[ShardId],
    ) -> io::Result<ControlOperation> {
        let record = ControlOperation::prepared(parent, kind, targets)?;
        let key = self.record_key(parent);
        if let Some((_, bytes)) = self.owned_value(&key)? {
            return Self::confirm(&record, decode_record(&key, parent, &bytes)?);
        }
        let (revision, mut index) = self.index()?;
        if !index.insert(parent) {
            // 另一个相同父命令可能刚刚完成登记。重读时同时校验租约和意图。
            return self.confirm_existing(&record);
        }
        if index.len() > MAX_PENDING {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "未完成操作索引已满",
            ));
        }
        let index_key = self.index_key();
        let mut compares = self.owner_compares()?;
        compares.extend([
            version_compare(&key, None),
            version_compare(&index_key, Some(revision)),
        ]);
        let mut writes = vec![
            put(&key, &encode_record(&key, &record)?, None),
            put(&index_key, &encode_index(&index_key, &index), None),
        ];
        // 首次登记的完整部署拓扑与操作一起固定。离线目标也必须留在后续意图中。
        let members_key = format!("{}/members", self.path);
        let marker_key = self.marker_key();
        let members = codec::encode_members(&members_key, record.targets.keys().copied());
        let [marker, stored_members] = self.client.get_keys([&marker_key, &members_key])?;
        let (marker_revision, bound) = topology_marker(&marker)?;
        compares.push(version_compare(&marker_key, Some(marker_revision)));
        match (bound, stored_members) {
            (true, Some((revision, stored))) => {
                if stored != members {
                    return Err(invalid("目标分片与部署拓扑不一致"));
                }
                compares.push(version_compare(&members_key, Some(revision)));
            }
            (false, None) => {
                compares.push(version_compare(&members_key, None));
                writes.push(put(&members_key, &members, None));
                writes.push(put(&marker_key, BOUND, None));
            }
            _ => return Err(invalid("控制拓扑与首次登记标记不一致")),
        }
        if self.client.transaction(compares, writes)?.is_none() {
            return self.confirm_existing(&record);
        }
        Ok(record)
    }
    fn load(&self, parent: CommandId) -> io::Result<ControlOperation> {
        let key = self.record_key(parent);
        let (_, bytes) = self
            .owned_value(&key)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "控制操作不存在"))?;
        decode_record(&key, parent, &bytes)
    }
    fn pending(&self) -> io::Result<Vec<CommandId>> {
        let key = self.index_key();
        let (_, bytes) = self
            .owned_value(&key)?
            .ok_or_else(|| invalid("未完成操作索引不存在"))?;
        Ok(decode_index(&key, &bytes)?.into_iter().collect())
    }
    fn advance(
        &self,
        previous: &ControlOperation,
        update: ControlUpdate,
    ) -> io::Result<ControlOperation> {
        let key = self.record_key(previous.parent);
        let (revision, bytes) = self
            .owned_value(&key)?
            .ok_or_else(|| invalid("控制操作不存在"))?;
        let current = decode_record(&key, previous.parent, &bytes)?;
        if &current != previous {
            return Err(conflict());
        }
        let mut next = current.transition(update)?;
        let mut compares = self.owner_compares()?;
        compares.push(version_compare(&key, Some(revision)));
        let mut writes = Vec::new();
        match (current.phase, next.phase) {
            (_, ControlPhase::Fencing | ControlPhase::Released)
            | (ControlPhase::Fencing | ControlPhase::Fanout, ControlPhase::Aborted) => {
                self.change_scope(&mut next, &mut compares, &mut writes)?
            }
            _ => (),
        }
        if matches!(next.phase, ControlPhase::Done | ControlPhase::Aborted) {
            let index_key = self.index_key();
            let (version, mut index) = self.index()?;
            if !index.remove(&next.parent) {
                return Err(invalid("控制操作未登记在索引"));
            }
            compares.push(version_compare(&index_key, Some(version)));
            writes.push(put(&index_key, &encode_index(&index_key, &index), None));
        }
        writes.push(put(&key, &encode_record(&key, &next)?, None));
        self.client
            .transaction(compares, writes)?
            .ok_or_else(conflict)?;
        Ok(next)
    }
}
