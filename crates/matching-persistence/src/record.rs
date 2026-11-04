//! 按固定大端字段顺序编码并校验单条持久化记录。
use crate::crc32c;

/// 单条记录允许的最大载荷长度，限制解码内存需求。
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
/// 固定头部字节数。
const HEADER: usize = 60;
/// 格式识别值。
const MAGIC: u32 = 0x5741_4c31;
/// 当前格式版本。
const VERSION: u16 = 1;

/// 记录解析或编码失败分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalError {
    /// 输入未包含完整记录。
    Truncated,
    /// 识别值或版本不受支持。
    InvalidHeader,
    /// 载荷超出允许范围。
    PayloadTooLarge,
    /// 校验值不匹配。
    Checksum,
    /// 固定头部的独立校验值不匹配。
    HeaderChecksum,
}

/// 已分配分片序号的持久化变更及其不透明载荷。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    /// 集群身份。
    pub cluster_id: u16,
    /// 分片身份。
    pub shard_id: u16,
    /// 格式标志；当前版本仅接受零。
    pub flags: u16,
    /// 主节点纪元。
    pub epoch: u64,
    /// 分片业务序号。
    pub shard_seq: u64,
    /// 分片逻辑时间。
    pub engine_time: u64,
    /// 根命令身份。
    pub command_id: u128,
    /// 由调用方提供的规范命令字节。
    pub payload: Vec<u8>,
}
impl WalRecord {
    /// 编码固定字段、载荷及覆盖此前全部字节的 CRC32C。
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        if self.flags != 0 || self.shard_seq == 0 {
            return Err(WalError::InvalidHeader);
        }
        if self.payload.len() > MAX_PAYLOAD {
            return Err(WalError::PayloadTooLarge);
        }
        let mut out = Vec::with_capacity(HEADER + self.payload.len() + 4);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&self.cluster_id.to_be_bytes());
        out.extend_from_slice(&self.shard_id.to_be_bytes());
        out.extend_from_slice(&self.flags.to_be_bytes());
        out.extend_from_slice(&self.epoch.to_be_bytes());
        out.extend_from_slice(&self.shard_seq.to_be_bytes());
        out.extend_from_slice(&self.engine_time.to_be_bytes());
        out.extend_from_slice(&self.command_id.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&crc32c(&out).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&crc32c(&out).to_be_bytes());
        Ok(out)
    }
    /// 从输入起始位置解码一条记录；返回消耗字节数。
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), WalError> {
        if bytes.len() < HEADER {
            return Err(WalError::Truncated);
        }
        let u16_at = |i| u16::from_be_bytes([bytes[i], bytes[i + 1]]);
        let u32_at = |i| u32::from_be_bytes(bytes[i..i + 4].try_into().expect("固定长度"));
        let u64_at = |i| u64::from_be_bytes(bytes[i..i + 8].try_into().expect("固定长度"));
        if u32_at(0) != MAGIC || u16_at(4) != VERSION || u16_at(10) != 0 {
            return Err(WalError::InvalidHeader);
        }
        if crc32c(&bytes[..56]) != u32_at(56) {
            return Err(WalError::HeaderChecksum);
        }
        if u64_at(20) == 0 {
            return Err(WalError::InvalidHeader);
        }
        let len = u32_at(52) as usize;
        if len > MAX_PAYLOAD {
            return Err(WalError::PayloadTooLarge);
        }
        let total = HEADER
            .checked_add(len)
            .and_then(|n| n.checked_add(4))
            .ok_or(WalError::PayloadTooLarge)?;
        if bytes.len() < total {
            return Err(WalError::Truncated);
        }
        if crc32c(&bytes[..total - 4]) != u32_at(total - 4) {
            return Err(WalError::Checksum);
        }
        Ok((
            Self {
                cluster_id: u16_at(6),
                shard_id: u16_at(8),
                flags: u16_at(10),
                epoch: u64_at(12),
                shard_seq: u64_at(20),
                engine_time: u64_at(28),
                command_id: u128::from_be_bytes(bytes[36..52].try_into().expect("固定长度")),
                payload: bytes[HEADER..total - 4].to_vec(),
            },
            total,
        ))
    }
}
