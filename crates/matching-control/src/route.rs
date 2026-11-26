//! 路由快照的线性一致读取与比较写入。
use crate::{EtcdClient, KvVersion};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use matching_domain::ClusterId;
use matching_runtime::{RouteAuthority, RouteTable, decode_route_snapshot, encode_route_snapshot};
use serde_json::json;
use std::io;

const MAX_ROUTES: usize = 10_000;

fn validate_route_key(key: &str) -> io::Result<()> {
    if key.len() > 1018
        || !key.starts_with('/')
        || key.ends_with('/')
        || key.ends_with("/owner")
        || key.contains("//")
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "路由键无效或与租约命名空间冲突",
        ));
    }
    Ok(())
}

/// 通过 etcd 租约独占路由写权，按快照版本比较并提交更新。
pub struct EtcdRouteAuthority {
    /// 线性一致客户端。
    client: EtcdClient,
    /// 持久路由快照键。
    key: Vec<u8>,
    /// 快照绑定的集群身份。
    cluster: ClusterId,
    /// 由控制存储授予的写入租约；仅在明确获取后可提交路由。
    lease: Option<i64>,
    /// 申请租约时使用的保守时长。
    lease_ttl: Option<std::time::Duration>,
    /// 续租失败或显式撤销租约后置位，本实例不再获得写权。
    invalidated: bool,
}

fn encode(cluster: ClusterId, routes: &RouteTable) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::from(&b"ERC1"[..]);
    bytes.extend_from_slice(&cluster.get().to_be_bytes());
    bytes.extend_from_slice(&encode_route_snapshot(routes, MAX_ROUTES)?);
    bytes.extend_from_slice(&matching_persistence::crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

fn decode(cluster: ClusterId, bytes: &[u8]) -> io::Result<RouteTable> {
    if bytes.len() < 10 || &bytes[..4] != b"ERC1" || bytes[4..6] != cluster.get().to_be_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "路由快照身份无效",
        ));
    }
    let end = bytes.len() - 4;
    let crc = u32::from_be_bytes(
        bytes[end..]
            .try_into()
            .map_err(|_| io::Error::other("路由校验值无效"))?,
    );
    if matching_persistence::crc32c(&bytes[..end]) != crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "路由快照校验失败",
        ));
    }
    decode_route_snapshot(&bytes[6..end], MAX_ROUTES)
}

impl EtcdRouteAuthority {
    /// 当键不存在时创建绑定集群的初始路由；结果未知时不得假设失败。
    pub fn create(
        client: EtcdClient,
        key: &str,
        cluster: ClusterId,
        routes: RouteTable,
    ) -> io::Result<Self> {
        validate_route_key(key)?;
        let bytes = encode(cluster, &routes)?;
        if client
            .compare_and_swap(key.as_bytes(), KvVersion::Absent, &bytes)?
            .is_none()
        {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "路由已存在"));
        }
        Self::open(client, key, cluster)
    }

    /// 打开并校验已有快照；此操作不申请独占租约。
    pub fn open(client: EtcdClient, key: &str, cluster: ClusterId) -> io::Result<Self> {
        validate_route_key(key)?;
        let authority = Self {
            client,
            key: key.as_bytes().to_vec(),
            cluster,
            lease: None,
            lease_ttl: None,
            invalidated: false,
        };
        authority.read_snapshot()?;
        Ok(authority)
    }

    /// 只读查询已持久路由，不授予协调控制权。
    pub fn read_snapshot(&self) -> io::Result<(u64, RouteTable)> {
        let (revision, bytes) = self
            .client
            .get(&self.key)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "路由不存在"))?;
        Ok((revision, decode(self.cluster, &bytes)?))
    }

    /// 申请路由写入独占租约；租约竞争及存储错误均显式失败。
    pub fn acquire_lease(&mut self, ttl_seconds: u64) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("路由权威已失效"));
        }
        if self.lease.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "已持有路由租约",
            ));
        }
        let owner = [self.key.as_slice(), b"/owner"].concat();
        let lease = self.client.grant_lease(ttl_seconds)?;
        let key = STANDARD.encode(owner);
        let result = self.client.transaction(
            vec![json!({"key":key,"target":"VERSION","version":"0","result":"EQUAL"})],
            vec![json!({"request_put":{"key":key,"value":STANDARD.encode(lease.to_be_bytes()),"lease":lease.to_string()}})],
        );
        match result {
            Ok(Some(_)) => {
                self.lease = Some(lease);
                self.lease_ttl = Some(std::time::Duration::from_secs(ttl_seconds));
                Ok(())
            }
            result => {
                self.client
                    .revoke_lease(lease)
                    .map_err(|_| io::Error::other("路由租约撤销结果未知"))?;
                match result {
                    Ok(None) => Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "路由租约已被占用",
                    )),
                    Err(error) => Err(error),
                    Ok(Some(_)) => unreachable!(),
                }
            }
        }
    }

    /// 刷新路由租约；失败后调用方必须立即停止使用该权威。
    pub fn keep_alive(&mut self) -> io::Result<()> {
        if self.invalidated {
            return Err(io::Error::other("路由权威已失效"));
        }
        let result = self.client.keep_lease(
            self.lease
                .ok_or_else(|| io::Error::other("未持有路由租约"))?,
        );
        if result.is_err() {
            self.invalidated = true;
        }
        result
    }

    /// 返回本实例是否因续租失败或撤销租约而失效。远端租约状态仍须由存储事务核验。
    pub fn is_locally_invalidated(&self) -> bool {
        self.invalidated
    }

    /// 撤销路由租约；即使响应未知也不再允许本实例写入。
    pub fn revoke_lease(&mut self) -> io::Result<()> {
        self.invalidated = true;
        self.lease_ttl = None;
        let lease = self
            .lease
            .take()
            .ok_or_else(|| io::Error::other("未持有路由租约"))?;
        self.client.revoke_lease(lease)
    }
}

impl RouteAuthority for EtcdRouteAuthority {
    fn requires_monitor(&self) -> bool {
        true
    }
    fn validate_monitor_interval(&self, interval: std::time::Duration) -> io::Result<()> {
        if interval.is_zero() || self.lease_ttl.is_none_or(|ttl| interval >= ttl) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "监视间隔必须小于租约时长",
            ));
        }
        Ok(())
    }
    fn renew(&mut self) -> io::Result<()> {
        self.keep_alive()
    }
    fn cluster_id(&self) -> ClusterId {
        self.cluster
    }
    fn load(&self) -> io::Result<(u64, RouteTable)> {
        if self.invalidated {
            return Err(io::Error::other("路由权威已失效"));
        }
        let lease = self
            .lease
            .ok_or_else(|| io::Error::other("未持有路由租约"))?;
        let owner = [self.key.as_slice(), b"/owner"].concat();
        let (revision, bytes) = self
            .client
            .get_if_value(&owner, &lease.to_be_bytes(), &self.key)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "路由不存在"))?;
        Ok((revision, decode(self.cluster, &bytes)?))
    }
    fn compare_and_swap(&mut self, expected: u64, routes: RouteTable) -> io::Result<u64> {
        let (current_version, current) = self.load()?;
        if current_version != expected {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "路由版本冲突"));
        }
        if !current.allows_epoch_update(&routes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "所有权变更须由迁移流程提交",
            ));
        }
        self.commit_table(expected, &routes)
    }

    fn commit_migration(
        &mut self,
        change: matching_runtime::MigrationRouteChange<'_>,
    ) -> io::Result<u64> {
        let (expected, routes) = change.for_cluster(self.cluster_id())?;
        self.commit_table(expected, routes)
    }
}

impl EtcdRouteAuthority {
    /// 在同一事务中比较版本与租约，再写入绑定集群的完整快照。
    fn commit_table(&mut self, expected: u64, routes: &RouteTable) -> io::Result<u64> {
        if self.invalidated {
            return Err(io::Error::other("路由权威已失效"));
        }
        let lease = self
            .lease
            .ok_or_else(|| io::Error::other("未持有路由租约"))?;
        if expected == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "路由版本为零"));
        }
        let key = STANDARD.encode(&self.key);
        let owner = STANDARD.encode([self.key.as_slice(), b"/owner"].concat());
        self.client
            .transaction(
                vec![json!({"key":key,"target":"MOD","mod_revision":expected.to_string(),"result":"EQUAL"}), json!({"key":owner,"target":"VALUE","value":STANDARD.encode(lease.to_be_bytes()),"result":"EQUAL"})],
                vec![json!({"request_put":{"key":key,"value":STANDARD.encode(encode(self.cluster, routes)?)}})],
            )?
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "路由版本冲突"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_key_cannot_overlap_owner_namespace_or_exceed_limit() {
        assert!(validate_route_key("/routes/market").is_ok());
        assert!(validate_route_key("/routes/market/owner").is_err());
        assert!(validate_route_key(&format!("/{}", "x".repeat(1018))).is_err());
        assert!(validate_route_key("/routes/\0market").is_err());
    }
}
