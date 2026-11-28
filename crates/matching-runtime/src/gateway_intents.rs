//! 网关意图身份的单机持久登记，防止重连时更换命令和订单身份。
use matching_domain::{
    Command, CommandEnvelope, CommandId, OrderId, PlaceLimitOrder, ReservationToken, UserId,
};
use matching_persistence::{FileManifestStorage, ManifestIntent, ManifestManager};
use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

const MAX_ENTRIES: usize = 4096;
const MAX_WIRE: usize = 65_536;
const FIXED: usize = 60;
/// 记录长度的最高位标识后续载荷是已有意图的风险授权。
const AUTH_FLAG: u32 = 1 << 31;
/// 身份记录的此位表示规范请求长度之后还保存了八字节的全局纪元。
const EPOCH_FLAG: u32 = 1 << 30;

struct Entry {
    /// 首次准入时已发布的全局纪元；旧格式记录属于纪元零。
    global_epoch: u64,
    /// 认证用户身份。
    user: UserId,
    /// 用户域内幂等键。
    key: u128,
    /// 已持久绑定的命令身份。
    command: CommandId,
    /// 已持久绑定的订单身份。
    order: OrderId,
    /// 由持久日志位置派生的稳定会话别名序号。
    client_seq: u64,
    /// 不含请求关联身份的规范意图帧。
    wire: Vec<u8>,
    /// 已确认的原始风险令牌；重试必须逐字段复用。
    authorization: Option<ReservationToken>,
}

/// 独占本机目录中的持久意图表；不提供跨主机一致性或自动清理。
///
/// 真实部署必须保护目录身份并保证一个权威写者；表满时拒绝新意图。
pub struct GatewayIntentStore {
    /// 顺序追加且独占的意图记录文件。
    file: File,
    /// 记录已确认连续前缀的唯一清单管理器。
    manifest: ManifestManager<FileManifestStorage>,
    /// 已恢复或已确认的意图身份索引。
    entries: Vec<Entry>,
    /// 意图条数上限。
    capacity: usize,
    /// 单条规范意图的字节上限。
    max_wire: usize,
    /// 持久化错误后禁止继续分配。
    failed: bool,
}
fn manifest_dir(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".state");
    name.into()
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "网关意图日志损坏或配置无效")
}
fn lock(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|e| match e {
        TryLockError::WouldBlock => {
            io::Error::new(io::ErrorKind::WouldBlock, "网关意图日志已被占用")
        }
        TryLockError::Error(error) => error,
    })
}
fn read_u32(x: &[u8]) -> io::Result<u32> {
    Ok(u32::from_be_bytes(x.try_into().map_err(|_| invalid())?))
}
fn read_u64(x: &[u8]) -> io::Result<u64> {
    Ok(u64::from_be_bytes(x.try_into().map_err(|_| invalid())?))
}
fn read_u128(x: &[u8]) -> io::Result<u128> {
    Ok(u128::from_be_bytes(x.try_into().map_err(|_| invalid())?))
}
fn validate(capacity: usize, max_wire: usize) -> io::Result<()> {
    if capacity == 0 || capacity > MAX_ENTRIES || max_wire == 0 || max_wire > MAX_WIRE {
        return Err(invalid());
    }
    Ok(())
}
/// 验证授权记录仍对应先前持久登记的完整规范意图。
fn matches_intent(entry: &Entry, place: &crate::PlaceIntent) -> bool {
    let Ok((_, request)) = matching_protocol::decode_place_request(&entry.wire, MAX_WIRE, MAX_WIRE)
    else {
        return false;
    };
    let side = match place.order.side {
        matching_domain::Side::Buy => matching_protocol::PlaceSide::Buy,
        matching_domain::Side::Sell => matching_protocol::PlaceSide::Sell,
    };
    let stp = match place.order.stp_mode {
        matching_domain::StpMode::None => matching_protocol::StpPolicy::None,
        matching_domain::StpMode::CancelTaker => matching_protocol::StpPolicy::CancelTaker,
        matching_domain::StpMode::CancelMaker => matching_protocol::StpPolicy::CancelMaker,
        matching_domain::StpMode::CancelBoth => matching_protocol::StpPolicy::CancelBoth,
        matching_domain::StpMode::DecrementAndCancel => return false,
    };
    request.key == entry.key
        && request.market == place.market.0
        && request.price == place.order.price.get()
        && request.qty == place.order.qty.get()
        && request.stp_key == place.order.stp_key.get()
        && request.user_control_epoch == place.user_control_epoch
        && request.side == side
        && request.stp == stp
        && request.post_only == place.post_only
        && request.client_order_id == place.client_order_id
}
impl GatewayIntentStore {
    /// 只读查找并验证已保存的风险授权，不调用外部风险服务。
    pub(crate) fn existing_authorization(
        &self,
        prepared: &crate::PreparedPlace,
    ) -> Result<Option<ReservationToken>, crate::GatewayError> {
        use crate::GatewayError;
        if self.failed {
            return Err(GatewayError::Journal(io::Error::other("意图日志已失效")));
        }
        let entry = self
            .entries
            .iter()
            .find(|e| e.command == prepared.command_id())
            .ok_or(GatewayError::Conflict)?;
        let intent = prepared.intent();
        if !prepared.is_durable()
            || entry.order != intent.order.order_id
            || entry.user != intent.order.user_id
            || !matches_intent(entry, intent)
        {
            return Err(GatewayError::Conflict);
        }
        Ok(entry.authorization.clone())
    }
    /// 返回意图首次持久登记时分配的稳定别名序号。
    pub fn client_seq(&self, command: CommandId) -> Option<u64> {
        self.entries
            .iter()
            .find(|entry| entry.command == command)
            .map(|entry| entry.client_seq)
    }
    /// 先同步首次风险令牌，再供传输提交；同命令重试使用已保存的完整令牌。
    ///
    /// 风险服务仍须按 CommandId 持久幂等，覆盖授权成功后本地写入失败的窗口。
    pub fn authorize(
        &mut self,
        prepared: &crate::PreparedPlace,
        issue: impl FnOnce() -> Result<ReservationToken, crate::GatewayError>,
    ) -> Result<ReservationToken, crate::GatewayError> {
        use crate::GatewayError;
        if self.failed {
            return Err(GatewayError::Journal(io::Error::other("意图日志已失效")));
        }
        let index = self
            .entries
            .iter()
            .position(|e| e.command == prepared.command_id())
            .ok_or(GatewayError::Conflict)?;
        let intent = prepared.intent();
        if !prepared.is_durable()
            || self.entries[index].order != intent.order.order_id
            || self.entries[index].user != intent.order.user_id
            || !matches_intent(&self.entries[index], intent)
        {
            return Err(GatewayError::Conflict);
        }
        if let Some(token) = &self.entries[index].authorization {
            return Ok(token.clone());
        }
        let token = issue()?;
        if token.command_id != prepared.command_id()
            || token.order_id != intent.order.order_id
            || token.user_id != intent.order.user_id
            || token.market_id != intent.market
            || token.canonical_signing_payload().is_err()
        {
            return Err(GatewayError::Reservation);
        }
        let envelope = CommandEnvelope {
            command_id: prepared.command_id(),
            session_id: 0,
            client_seq: 0,
            market_id: Some(intent.market),
            user_id: Some(intent.order.user_id),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: intent.order.clone(),
                client_order_id: intent.client_order_id.clone(),
                user_control_epoch: intent.user_control_epoch,
                post_only: intent.post_only,
                reservation: token.clone(),
            })),
        };
        let bytes =
            matching_domain::encode_wal_command(&envelope).map_err(|_| GatewayError::Capacity)?;
        if bytes.len() > MAX_WIRE {
            return Err(GatewayError::Capacity);
        }
        let len = u32::try_from(bytes.len()).map_err(|_| GatewayError::Capacity)?;
        let mut record = Vec::new();
        record
            .try_reserve_exact(bytes.len().checked_add(8).ok_or(GatewayError::Capacity)?)
            .map_err(|_| GatewayError::Capacity)?;
        record.extend_from_slice(&(len | AUTH_FLAG).to_be_bytes());
        record.extend_from_slice(&bytes);
        record.extend_from_slice(&matching_persistence::crc32c(&record).to_be_bytes());
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|()| self.file.sync_all());
        if let Err(error) = result {
            self.failed = true;
            return Err(GatewayError::Journal(error));
        }
        let through_seq = self
            .manifest
            .state()
            .local_durable_seq
            .checked_add(1)
            .ok_or(GatewayError::Capacity)?;
        if let Err(error) = self.manifest.submit(ManifestIntent::LocalDurableAdvanced {
            through_seq,
            first_seq: 1,
        }) {
            self.failed = true;
            return Err(GatewayError::Journal(error));
        }
        self.entries[index].authorization = Some(token.clone());
        Ok(token)
    }
    /// 新建并同步空日志；调用方负责保留该文件并确保全局唯一写者。
    pub fn create(path: impl AsRef<Path>, capacity: usize, max_wire: usize) -> io::Result<Self> {
        validate(capacity, max_wire)?;
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        lock(&file)?;
        file.write_all(b"GIN1")?;
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        matching_persistence::sync_directory(parent)?;
        let state_dir = manifest_dir(path);
        std::fs::create_dir(&state_dir)?;
        matching_persistence::sync_directory(parent)?;
        let mut manifest = ManifestManager::<FileManifestStorage>::open(&state_dir)?;
        manifest.submit(ManifestIntent::Initialize)?;
        manifest.submit(ManifestIntent::RecoveryAnchor)?;
        Ok(Self {
            file,
            manifest,
            entries: Vec::new(),
            capacity,
            max_wire,
            failed: false,
        })
    }
    /// 只接受完整、校验正确、身份不冲突的持久前缀；不跳过损坏或截断记录。
    pub fn open(path: impl AsRef<Path>, capacity: usize, max_wire: usize) -> io::Result<Self> {
        validate(capacity, max_wire)?;
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        lock(&file)?;
        let manifest = ManifestManager::<FileManifestStorage>::open_recovery(manifest_dir(path))?;
        if manifest.state().generation < 2 {
            return Err(invalid());
        }
        let committed =
            usize::try_from(manifest.state().local_durable_seq).map_err(|_| invalid())?;
        if committed > capacity.checked_mul(2).ok_or_else(invalid)? {
            return Err(invalid());
        }
        let max_size = capacity
            .checked_mul(FIXED + 8 + max_wire + 8 + MAX_WIRE + 8)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(invalid)?;
        let size = usize::try_from(file.metadata()?.len()).map_err(|_| invalid())?;
        if size < 4 || size > max_size {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| io::Error::other("意图日志内存不足"))?;
        let read_limit = max_size.checked_add(1).ok_or_else(invalid)?;
        std::io::Read::by_ref(&mut file)
            .take(u64::try_from(read_limit).map_err(|_| invalid())?)
            .read_to_end(&mut bytes)?;
        if bytes.len() != size || &bytes[..4] != b"GIN1" {
            return Err(invalid());
        }
        let mut entries: Vec<Entry> = Vec::new();
        let mut pos = 4_usize;
        let mut record_count = 0_usize;
        while record_count < committed {
            let prefix_end = pos.checked_add(4).ok_or_else(invalid)?;
            let tagged = read_u32(bytes.get(pos..prefix_end).ok_or_else(invalid)?)?;
            let is_auth = tagged & AUTH_FLAG != 0;
            let has_epoch = tagged & EPOCH_FLAG != 0;
            let header = if has_epoch { FIXED + 8 } else { FIXED };
            let len = usize::try_from(tagged & !(AUTH_FLAG | EPOCH_FLAG)).map_err(|_| invalid())?;
            if (is_auth && (has_epoch || len == 0 || len > MAX_WIRE))
                || (!is_auth && (len < header || len > header + max_wire))
            {
                return Err(invalid());
            }
            let end = prefix_end.checked_add(len).ok_or_else(invalid)?;
            let crc_end = end.checked_add(4).ok_or_else(invalid)?;
            let body = bytes.get(prefix_end..end).ok_or_else(invalid)?;
            let crc = read_u32(bytes.get(end..crc_end).ok_or_else(invalid)?)?;
            if matching_persistence::crc32c(&bytes[pos..end]) != crc {
                return Err(invalid());
            }
            if is_auth {
                let envelope = matching_domain::decode_wal_command(body).map_err(|_| invalid())?;
                let Command::PlaceLimit(place) = envelope.payload else {
                    return Err(invalid());
                };
                let entry = entries
                    .iter_mut()
                    .find(|e| e.command == envelope.command_id)
                    .ok_or_else(invalid)?;
                if entry.authorization.is_some()
                    || place.order.order_id != entry.order
                    || place.order.user_id != entry.user
                    || place.reservation.command_id != entry.command
                    || place.reservation.order_id != entry.order
                    || place.reservation.user_id != entry.user
                    || envelope.user_id != Some(entry.user)
                    || envelope.market_id != Some(place.reservation.market_id)
                    || place.reservation.canonical_signing_payload().is_err()
                    || !matches_intent(
                        entry,
                        &crate::PlaceIntent {
                            market: place.reservation.market_id,
                            order: place.order.clone(),
                            client_order_id: place.client_order_id.clone(),
                            user_control_epoch: place.user_control_epoch,
                            post_only: place.post_only,
                        },
                    )
                {
                    return Err(invalid());
                }
                entry.authorization = Some(place.reservation);
                record_count = record_count.checked_add(1).ok_or_else(invalid)?;
                pos = crc_end;
                continue;
            }
            if entries.len() == capacity {
                return Err(invalid());
            }
            let user = UserId(read_u64(&body[..8])?);
            let key = read_u128(&body[8..24])?;
            let command = CommandId(read_u128(&body[24..40])?);
            let order = OrderId(read_u128(&body[40..56])?);
            let wire_len = usize::try_from(read_u32(&body[56..60])?).map_err(|_| invalid())?;
            let global_epoch = if has_epoch {
                read_u64(&body[FIXED..header])?
            } else {
                0
            };
            if header.checked_add(wire_len) != Some(len)
                || entries.iter().any(|e| {
                    (e.user == user && e.key == key) || e.command == command || e.order == order
                })
            {
                return Err(invalid());
            }
            entries
                .try_reserve(1)
                .map_err(|_| io::Error::other("意图表内存不足"))?;
            entries.push(Entry {
                global_epoch,
                user,
                key,
                command,
                order,
                client_seq: u64::try_from(record_count)
                    .map_err(|_| invalid())?
                    .checked_add(1)
                    .ok_or_else(invalid)?,
                wire: body[header..].to_vec(),
                authorization: None,
            });
            record_count = record_count.checked_add(1).ok_or_else(invalid)?;
            pos = crc_end;
        }
        if pos < bytes.len() {
            file.set_len(u64::try_from(pos).map_err(|_| invalid())?)?;
            file.sync_all()?;
        }
        Ok(Self {
            file,
            manifest,
            entries,
            capacity,
            max_wire,
            failed: false,
        })
    }
    /// 查找或同步插入固定意图；同键变更内容或身份冲突时拒绝。
    pub(crate) fn bind(
        &mut self,
        user: UserId,
        key: u128,
        wire: &[u8],
        global_epoch: u64,
        allocate: impl FnOnce() -> Result<(CommandId, OrderId), crate::GatewayError>,
    ) -> Result<(CommandId, OrderId), crate::GatewayError> {
        use crate::GatewayError;
        if self.failed {
            return Err(GatewayError::Journal(io::Error::other("意图日志已失效")));
        }
        if wire.len() > self.max_wire {
            return Err(GatewayError::Capacity);
        }
        if let Some(entry) = self.entries.iter().find(|e| e.user == user && e.key == key) {
            if entry.wire != wire {
                return Err(GatewayError::Conflict);
            }
            if entry.global_epoch != global_epoch {
                return Err(GatewayError::Rejected(
                    matching_domain::RejectReason::ControlFenceActive,
                ));
            }
            return Ok((entry.command, entry.order));
        }
        if self.entries.len() == self.capacity {
            return Err(GatewayError::Capacity);
        }
        self.entries
            .try_reserve(1)
            .map_err(|_| GatewayError::Capacity)?;
        let mut stored = Vec::new();
        stored
            .try_reserve_exact(wire.len())
            .map_err(|_| GatewayError::Capacity)?;
        stored.extend_from_slice(wire);
        let (command, order) = allocate()?;
        if self
            .entries
            .iter()
            .any(|e| e.command == command || e.order == order)
        {
            return Err(GatewayError::Conflict);
        }
        let header = if global_epoch == 0 { FIXED } else { FIXED + 8 };
        let len = header
            .checked_add(wire.len())
            .ok_or(GatewayError::Capacity)?;
        let mut record = Vec::new();
        record
            .try_reserve_exact(len + 8)
            .map_err(|_| GatewayError::Capacity)?;
        let length = u32::try_from(len).map_err(|_| GatewayError::Capacity)?;
        let tag = if global_epoch == 0 {
            length
        } else {
            length | EPOCH_FLAG
        };
        record.extend_from_slice(&tag.to_be_bytes());
        record.extend_from_slice(&user.0.to_be_bytes());
        record.extend_from_slice(&key.to_be_bytes());
        record.extend_from_slice(&command.0.to_be_bytes());
        record.extend_from_slice(&order.0.to_be_bytes());
        record.extend_from_slice(
            &u32::try_from(wire.len())
                .map_err(|_| GatewayError::Capacity)?
                .to_be_bytes(),
        );
        if global_epoch != 0 {
            record.extend_from_slice(&global_epoch.to_be_bytes());
        }
        record.extend_from_slice(wire);
        record.extend_from_slice(&matching_persistence::crc32c(&record).to_be_bytes());
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|()| self.file.sync_all());
        if let Err(error) = result {
            self.failed = true;
            return Err(GatewayError::Journal(error));
        }
        let through_seq = self
            .manifest
            .state()
            .local_durable_seq
            .checked_add(1)
            .ok_or(GatewayError::Capacity)?;
        if let Err(error) = self.manifest.submit(ManifestIntent::LocalDurableAdvanced {
            through_seq,
            first_seq: 1,
        }) {
            self.failed = true;
            return Err(GatewayError::Journal(error));
        }
        self.entries.push(Entry {
            global_epoch,
            user,
            key,
            command,
            order,
            client_seq: through_seq,
            wire: stored,
            authorization: None,
        });
        Ok((command, order))
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    #[test]
    fn reopened_intent_keeps_original_global_admission_epoch() {
        let dir = crate::test_directory::TestDirectory::new();
        let path = dir.0.join("gateway");
        let mut store = GatewayIntentStore::create(&path, 8, 128).unwrap();
        for epoch in [0, 9] {
            store
                .bind(UserId(1), u128::from(epoch), b"intent", epoch, || {
                    Ok((CommandId(u128::from(epoch)), OrderId(u128::from(epoch))))
                })
                .unwrap();
        }
        drop(store);
        let mut store = GatewayIntentStore::open(&path, 8, 128).unwrap();
        for epoch in [0, 9] {
            let old = store.bind(UserId(1), u128::from(epoch), b"intent", epoch, || {
                panic!("重试重新分配身份")
            });
            assert_eq!(old.unwrap().0, CommandId(u128::from(epoch)));
            assert!(matches!(
                store.bind(UserId(1), u128::from(epoch), b"intent", 10, || panic!(
                    "过期意图重新分配身份"
                )),
                Err(crate::GatewayError::Rejected(
                    matching_domain::RejectReason::ControlFenceActive
                ))
            ));
        }
    }
}
