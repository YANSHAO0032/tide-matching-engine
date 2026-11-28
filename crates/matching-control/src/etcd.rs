//! etcd 线性一致读取和原子比较写入。
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    io::{self, Read},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use url::{Host, Url};

const MAX_KEY: usize = 1024;
const MAX_VALUE: usize = 256 * 1024;
const MAX_RESPONSE: u64 = 1024 * 1024;

/// etcd 事务对目标键的前置条件。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvVersion {
    /// 键尚不存在。
    Absent,
    /// 键的 `mod_revision` 必须等于给定值。
    Revision(u64),
}

/// 有界同步外围客户端；不应在撮合线程中使用。
#[derive(Clone)]
pub struct EtcdClient {
    /// 已验证的服务端点，按配置顺序使用。
    endpoints: Arc<Vec<Url>>,
    /// 有超时且禁止重定向的网络客户端。
    agent: ureq::Agent,
    /// 首次响应绑定的 etcd 集群身份。
    cluster: Arc<Mutex<Option<u64>>>,
    /// 每次操作共享的总超时。
    timeout: Duration,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn number(value: &Value) -> io::Result<u64> {
    value
        .as_str()
        .ok_or_else(|| invalid("控制存储数字格式无效"))?
        .parse()
        .map_err(|_| invalid("控制存储数字越界"))
}

fn transaction_succeeded(response: &Value) -> io::Result<bool> {
    if !response.is_object() {
        return Err(invalid("事务响应格式无效"));
    }
    match response.get("succeeded") {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(invalid("事务结果格式无效")),
    }
}

/// 事务内比较键值。
pub(crate) fn value_compare(key: &str, value: &[u8]) -> Value {
    json!({"key":STANDARD.encode(key),"target":"VALUE","value":STANDARD.encode(value),"result":"EQUAL"})
}
/// 比较修改版本或要求键不存在。
pub(crate) fn version_compare(key: &str, revision: Option<u64>) -> Value {
    match revision {
        None => {
            json!({"key":STANDARD.encode(key),"target":"VERSION","version":"0","result":"EQUAL"})
        }
        Some(revision) => {
            json!({"key":STANDARD.encode(key),"target":"MOD","mod_revision":revision.to_string(),"result":"EQUAL"})
        }
    }
}
/// 构造持久或租约绑定的键写入。
pub(crate) fn put(key: &str, value: &[u8], lease: Option<i64>) -> Value {
    let mut request = json!({"key":STANDARD.encode(key),"value":STANDARD.encode(value)});
    if let Some(lease) = lease {
        request["lease"] = Value::String(lease.to_string());
    }
    json!({"request_put":request})
}
/// 单键读取的修改版本与原始字节。
pub(crate) type VersionedValue = Option<(u64, Vec<u8>)>;

impl EtcdClient {
    /// 返回构造客户端时经服务端响应核对的集群身份。
    pub(crate) fn cluster_id(&self) -> io::Result<u64> {
        self.cluster
            .lock()
            .map_err(|_| io::Error::other("控制存储身份锁不可用"))?
            .ok_or_else(|| invalid("控制存储缺少集群身份"))
    }

    /// 在一个线性一致事务中按输入顺序读取固定数量的键，各键保留自己的修改版本。
    pub(crate) fn get_keys<const N: usize>(
        &self,
        keys: [&str; N],
    ) -> io::Result<[VersionedValue; N]> {
        let mut ranges = Vec::with_capacity(N);
        for key in keys {
            validate(key.as_bytes(), None)?;
            ranges.push(json!({"request_range":{"key":STANDARD.encode(key)}}));
        }
        let response = self.read_rpc("v3/kv/txn", &json!({"success":ranges}))?;
        if !transaction_succeeded(&response)? {
            return Err(invalid("控制状态读取失败"));
        }
        let responses = response["responses"]
            .as_array()
            .filter(|a| a.len() == N)
            .ok_or_else(|| invalid("控制状态读取数量错误"))?;
        let mut values = std::array::from_fn(|_| None);
        for ((key, response), value) in keys.into_iter().zip(responses).zip(&mut values) {
            *value = Self::parse_range(key.as_bytes(), &response["response_range"])?;
        }
        Ok(values)
    }

    /// 保留同一集群身份与连接池，将单次操作限制在剩余期限内。
    pub(crate) fn constrained_by(&self, deadline: Instant) -> io::Result<Self> {
        let left = deadline
            .checked_duration_since(Instant::now())
            .filter(|value| !value.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "控制存储操作期限已过"))?;
        let mut client = self.clone();
        client.timeout = client.timeout.min(left);
        Ok(client)
    }

    /// 创建有界客户端；明文只允许显式开启的 IPv4 回环测试端点。
    pub fn new(
        endpoints: Vec<String>,
        timeout: Duration,
        allow_loopback_http: bool,
    ) -> io::Result<Self> {
        if endpoints.is_empty() || endpoints.len() > 16 || timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "端点或超时无效",
            ));
        }
        let mut parsed = Vec::new();
        for endpoint in endpoints {
            let url = Url::parse(&endpoint)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "控制存储地址无效"))?;
            let allowed = url.scheme() == "https"
                || (allow_loopback_http
                    && url.scheme() == "http"
                    && matches!(url.host(), Some(Host::Ipv4(ip)) if ip.is_loopback()));
            if !allowed
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "控制存储地址不符合安全约束",
                ));
            }
            parsed.push(url);
        }
        let client = Self {
            endpoints: Arc::new(parsed),
            agent: ureq::AgentBuilder::new()
                .timeout(timeout)
                .redirects(0)
                .build(),
            cluster: Arc::new(Mutex::new(None)),
            timeout,
        };
        // 核验所有当前可用端点属于同一集群，不可用端点稍后重新检查。
        let mut available = false;
        let mut last_error = None;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "超时时间越界"))?;
        for (index, endpoint) in client.endpoints.iter().enumerate() {
            let budget = match remaining(deadline, client.endpoints.len() - index) {
                Ok(budget) => budget,
                Err(_) if available => break,
                Err(error) => return Err(error),
            };
            match client.post_one(
                endpoint,
                "v3/kv/range",
                &json!({"key": STANDARD.encode([0_u8])}),
                budget,
            ) {
                Ok(_) => available = true,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    return Err(error);
                }
                Err(error) => last_error = Some(error),
            }
        }
        if !available {
            return Err(last_error.unwrap_or_else(|| io::Error::other("控制存储不可用")));
        }
        Ok(client)
    }

    /// 从一致性读返回键的修改版本与值；不存在返回空。
    pub fn get(&self, key: &[u8]) -> io::Result<Option<(u64, Vec<u8>)>> {
        validate(key, None)?;
        let request = json!({"key": STANDARD.encode(key)});
        let response = self.read_rpc("v3/kv/range", &request)?;
        Self::parse_range(key, &response)
    }

    /// 在同一线性一致事务中校验身份并读取另一键。
    pub(crate) fn get_if_value(
        &self,
        owner: &[u8],
        expected: &[u8],
        key: &[u8],
    ) -> io::Result<Option<(u64, Vec<u8>)>> {
        self.get_if_values(&[(owner, expected)], key)
    }

    /// 在同一线性一致事务中核对多个身份条件并读取目标键。
    pub(crate) fn get_if_values(
        &self,
        conditions: &[(&[u8], &[u8])],
        key: &[u8],
    ) -> io::Result<Option<(u64, Vec<u8>)>> {
        if conditions.is_empty() || conditions.len() > 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "控制身份条件数量无效",
            ));
        }
        for (owner, expected) in conditions {
            validate(owner, Some(expected))?;
        }
        validate(key, None)?;
        let comparisons: Vec<_> = conditions.iter().map(|(owner, expected)| json!({"key":STANDARD.encode(owner),"target":"VALUE","value":STANDARD.encode(expected),"result":"EQUAL"})).collect();
        let request = json!({"compare":comparisons,"success":[{"request_range":{"key":STANDARD.encode(key)}}]});
        let response = self.read_rpc("v3/kv/txn", &request)?;
        if !transaction_succeeded(&response)? {
            return Err(io::Error::other("控制存储身份已失效"));
        }
        let responses = response
            .get("responses")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("事务读取响应缺失"))?;
        if responses.len() != 1 {
            return Err(invalid("事务读取响应数量无效"));
        }
        let range = responses[0]
            .get("response_range")
            .ok_or_else(|| invalid("事务读取响应无效"))?;
        Self::parse_range(key, range)
    }

    fn parse_range(key: &[u8], response: &Value) -> io::Result<Option<(u64, Vec<u8>)>> {
        if !response.is_object() {
            return Err(invalid("控制存储响应格式无效"));
        }
        match response.get("more") {
            None | Some(Value::Bool(false)) => (),
            _ => return Err(invalid("控制存储返回截断记录")),
        }
        let count = response.get("count").map(number).transpose()?;
        let Some(entries) = response.get("kvs") else {
            if count.is_some_and(|count| count != 0) {
                return Err(invalid("控制存储记录计数不一致"));
            }
            return Ok(None);
        };
        let entries = entries
            .as_array()
            .ok_or_else(|| invalid("控制存储返回无效记录"))?;
        if entries.is_empty() && count.is_none_or(|count| count == 0) {
            return Ok(None);
        }
        if entries.len() != 1 || count.is_some_and(|count| count != 1) {
            return Err(invalid("控制存储返回记录数量无效"));
        }
        let entry = &entries[0];
        let actual_key = STANDARD
            .decode(
                entry
                    .get("key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("控制存储缺少键"))?,
            )
            .map_err(|_| invalid("控制存储键编码无效"))?;
        if actual_key != key {
            return Err(invalid("控制存储返回错误键"));
        }
        let value = match entry.get("value") {
            Some(value) => STANDARD
                .decode(value.as_str().ok_or_else(|| invalid("控制存储值无效"))?)
                .map_err(|_| invalid("控制存储值编码无效"))?,
            None => Vec::new(),
        };
        if value.len() > MAX_VALUE {
            return Err(invalid("控制存储值超限"));
        }
        let revision = number(&entry["mod_revision"])?;
        if revision == 0 {
            return Err(invalid("控制存储记录版本为零"));
        }
        Ok(Some((revision, value)))
    }

    /// 原子比较并写入；返回空表示比较失败，传输失败表示结果未知。
    pub fn compare_and_swap(
        &self,
        key: &[u8],
        expected: KvVersion,
        value: &[u8],
    ) -> io::Result<Option<u64>> {
        validate(key, Some(value))?;
        let key = STANDARD.encode(key);
        let compare = match expected {
            KvVersion::Absent => {
                json!({"key":key,"target":"VERSION","version":"0","result":"EQUAL"})
            }
            KvVersion::Revision(revision) if revision > 0 => {
                json!({"key":key,"target":"MOD","mod_revision":revision.to_string(),"result":"EQUAL"})
            }
            KvVersion::Revision(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "修改版本必须为正",
                ));
            }
        };
        self.transaction(
            vec![compare],
            vec![json!({"request_put":{"key":key,"value":STANDARD.encode(value)}})],
        )
    }

    /// 执行原子比较及写入；失败结果不得猜测为未提交。
    pub(crate) fn transaction(
        &self,
        compare: Vec<Value>,
        success: Vec<Value>,
    ) -> io::Result<Option<u64>> {
        let request = json!({"compare":compare,"success":success});
        // 写入结果未知时不得自动切换端点重试。
        let response = self.write_rpc("v3/kv/txn", &request)?;
        if !transaction_succeeded(&response)? {
            return Ok(None);
        }
        let revision = number(&response["header"]["revision"])?;
        if revision == 0 {
            return Err(invalid("控制存储返回零版本"));
        }
        Ok(Some(revision))
    }

    /// 创建由 etcd 管理的限时租约。
    pub(crate) fn grant_lease(&self, ttl: u64) -> io::Result<i64> {
        if ttl == 0 || i64::try_from(ttl).is_err() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "租约时长无效"));
        }
        let response = self.write_rpc("v3/lease/grant", &json!({"TTL": ttl.to_string()}))?;
        let id = response["ID"]
            .as_str()
            .ok_or_else(|| invalid("租约身份缺失"))?
            .parse::<i64>()
            .map_err(|_| invalid("租约身份无效"))?;
        if id <= 0 {
            return Err(invalid("租约身份无效"));
        }
        Ok(id)
    }

    /// 刷新租约并核对返回身份和剩余时长。
    pub(crate) fn keep_lease(&self, id: i64) -> io::Result<()> {
        let response = self.write_rpc("v3/lease/keepalive", &json!({"ID": id.to_string()}))?;
        let result = &response["result"];
        let actual = result["ID"]
            .as_str()
            .ok_or_else(|| invalid("租约刷新响应缺少身份"))?
            .parse::<i64>()
            .map_err(|_| invalid("租约身份无效"))?;
        if actual != id || number(&result["TTL"])? == 0 {
            return Err(invalid("租约已失效"));
        }
        Ok(())
    }

    /// 显式撤销租约；失败不得认定为撤销成功。
    pub(crate) fn revoke_lease(&self, id: i64) -> io::Result<()> {
        self.write_rpc("v3/lease/revoke", &json!({"ID":id.to_string()}))?;
        Ok(())
    }

    /// 只读操作在单个端点不可用时尝试其他端点。
    fn read_rpc(&self, path: &str, body: &Value) -> io::Result<Value> {
        let mut last = None;
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| invalid("超时时间越界"))?;
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            let budget = remaining(deadline, self.endpoints.len() - index)?;
            match self.post_one(endpoint, path, body, budget) {
                Ok(value) => return Ok(value),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    return Err(error);
                }
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| io::Error::other("控制存储不可用")))
    }

    /// 写前用一致性读选择可用端点；一旦发送写入，结果未知时不再重试。
    fn write_rpc(&self, path: &str, body: &Value) -> io::Result<Value> {
        let probe = json!({"key": STANDARD.encode([0_u8])});
        let mut last = None;
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| invalid("超时时间越界"))?;
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            let budget = remaining(deadline, self.endpoints.len() - index + 1)?;
            match self.post_one(endpoint, "v3/kv/range", &probe, budget) {
                Ok(_) => return self.post_one(endpoint, path, body, remaining(deadline, 1)?),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    return Err(error);
                }
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| io::Error::other("控制存储不可用")))
    }

    /// 限制响应大小并验证集群身份，避免跨集群静默回退。
    fn post_one(
        &self,
        endpoint: &Url,
        path: &str,
        body: &Value,
        timeout: Duration,
    ) -> io::Result<Value> {
        let url = endpoint
            .join(path)
            .map_err(|_| invalid("控制存储路径无效"))?;
        let response = self
            .agent
            .post(url.as_str())
            .timeout(timeout)
            .set("Content-Type", "application/json")
            .send_string(&body.to_string())
            .map_err(|error| match error {
                ureq::Error::Status(401 | 403, _) => {
                    io::Error::new(io::ErrorKind::PermissionDenied, "控制存储拒绝授权")
                }
                ureq::Error::Status(400..=499, _) => invalid("控制存储拒绝请求"),
                _ => io::Error::other("控制存储请求失败或结果未知"),
            })?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take(MAX_RESPONSE + 1)
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).map_err(|_| invalid("控制存储响应超限"))? > MAX_RESPONSE
        {
            return Err(invalid("控制存储响应超限"));
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| invalid("控制存储响应格式无效"))?;
        let header = value
            .get("header")
            .or_else(|| value.get("result").and_then(|result| result.get("header")))
            .ok_or_else(|| invalid("控制存储响应缺少身份"))?;
        let cluster_id = number(&header["cluster_id"])?;
        let mut cluster = self
            .cluster
            .lock()
            .map_err(|_| io::Error::other("控制存储身份锁不可用"))?;
        if cluster.is_some_and(|existing| existing != cluster_id) {
            return Err(invalid("控制存储集群身份不一致"));
        }
        *cluster = Some(cluster_id);
        Ok(value)
    }
}

fn remaining(deadline: Instant, slots: usize) -> io::Result<Duration> {
    let left = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "控制存储操作超时"))?;
    let budget = left
        .checked_div(u32::try_from(slots).map_err(|_| invalid("端点数量越界"))?)
        .ok_or_else(|| invalid("端点数量无效"))?;
    if budget.is_zero() {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "控制存储操作超时"));
    }
    Ok(budget)
}

fn validate(key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
    if key.is_empty() || key.len() > MAX_KEY || value.is_some_and(|v| v.len() > MAX_VALUE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "控制存储键或值超限",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constrained_client_uses_remaining_operation_timeout() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            fn consume_request(stream: &mut std::net::TcpStream) {
                let mut header = Vec::new();
                let mut byte = [0_u8; 1];
                while !header.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    header.push(byte[0]);
                    assert!(header.len() <= 4096);
                }
                let text = std::str::from_utf8(&header).unwrap();
                let length: usize = text
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .unwrap()
                    .1
                    .trim()
                    .parse()
                    .unwrap();
                assert!(length <= 4096);
                let mut body = vec![0_u8; length];
                stream.read_exact(&mut body).unwrap();
            }
            let (mut first, _) = listener.accept().unwrap();
            consume_request(&mut first);
            let body = b"{\"header\":{\"cluster_id\":\"1\",\"revision\":\"1\"}}";
            write!(
                first,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            first.write_all(body).unwrap();
            drop(first);
            let (mut second, _) = listener.accept().unwrap();
            second
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            consume_request(&mut second);
            std::thread::sleep(Duration::from_millis(700));
        });
        let client = EtcdClient::new(vec![endpoint], Duration::from_secs(2), true).unwrap();
        let deadline = Instant::now() + Duration::from_millis(150);
        let constrained = client.constrained_by(deadline).unwrap();
        let started = Instant::now();
        assert!(constrained.get(b"key").is_err());
        assert!(started.elapsed() < Duration::from_millis(500));
        server.join().unwrap();
    }

    #[test]
    fn malformed_range_metadata_is_rejected() {
        let key = b"key";
        assert!(EtcdClient::parse_range(key, &json!({"count":"1"})).is_err());
        assert!(EtcdClient::parse_range(key, &json!({"more":true})).is_err());
        assert!(
            EtcdClient::parse_range(key, &json!({"kvs":[{"key":"a2V5","mod_revision":"0"}]}))
                .is_err()
        );
        assert!(
            EtcdClient::parse_range(
                key,
                &json!({"kvs":[{"key":"a2V5","mod_revision":"1"}],"count":"2"})
            )
            .is_err()
        );
        assert_eq!(EtcdClient::parse_range(key, &json!({})).unwrap(), None);
    }

    #[test]
    fn malformed_transaction_success_field_is_not_a_compare_failure() {
        assert!(!transaction_succeeded(&json!({})).unwrap());
        assert!(transaction_succeeded(&json!([])).is_err());
        assert!(transaction_succeeded(&json!({"succeeded":"true"})).is_err());
        assert!(transaction_succeeded(&json!({"succeeded":null})).is_err());
        assert!(transaction_succeeded(&json!({"succeeded":true})).unwrap());
    }

    #[test]
    #[ignore = "需要独立本地 etcd 测试集群"]
    fn conditional_read_observes_compare_and_value_atomically() {
        let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
        let client = EtcdClient::new(
            endpoints.split(',').map(str::to_owned).collect(),
            Duration::from_secs(3),
            true,
        )
        .unwrap();
        let owner = format!("/tide-test/atomic/{}/owner", std::process::id());
        let data = format!("/tide-test/atomic/{}/data", std::process::id());
        client
            .compare_and_swap(owner.as_bytes(), KvVersion::Absent, b"owner")
            .unwrap()
            .unwrap();
        let revision = client
            .compare_and_swap(data.as_bytes(), KvVersion::Absent, b"route")
            .unwrap()
            .unwrap();
        assert_eq!(
            client
                .get_if_value(owner.as_bytes(), b"owner", data.as_bytes())
                .unwrap(),
            Some((revision, b"route".to_vec()))
        );
        assert!(
            client
                .get_if_value(owner.as_bytes(), b"stale", data.as_bytes())
                .is_err()
        );
    }
}
