//! 扫描单个 segment，识别有界尾部撕裂并拒绝中间损坏。
use crate::{WalError, WalRecord};

/// 扫描失败的稳定内部分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanError {
    /// 包含不可恢复的记录损坏。
    Corrupt,
    /// 分片身份或序号不连续。
    Sequence,
}

/// 扫描得到的完整记录与可保留长度。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanResult {
    /// 按文件顺序读取的完整记录。
    pub records: Vec<WalRecord>,
    /// 最后一条完整记录之后的字节偏移。
    pub valid_len: usize,
    /// 最后 segment 含未完成或校验失败的尾记录。
    pub torn_tail: bool,
}

/// 扫描一个 segment；只有最后 segment 的明确尾部可标记为撕裂。
pub fn scan_segment(bytes: &[u8], last: bool) -> Result<ScanResult, ScanError> {
    let mut records: Vec<WalRecord> = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        match WalRecord::decode(remaining) {
            Ok((record, len)) => {
                if let Some(previous) = records.last()
                    && (previous.cluster_id != record.cluster_id
                        || previous.shard_id != record.shard_id
                        || previous.shard_seq.checked_add(1) != Some(record.shard_seq))
                {
                    return Err(ScanError::Sequence);
                }
                offset += len;
                records.push(record);
            }
            Err(WalError::Truncated) if last => {
                check_tail_identity(records.last(), remaining)?;
                return Ok(ScanResult {
                    records,
                    valid_len: offset,
                    torn_tail: true,
                });
            }
            Err(WalError::Checksum) if last && remaining.len() >= 60 => {
                check_tail_identity(records.last(), remaining)?;
                let len =
                    u32::from_be_bytes(remaining[52..56].try_into().expect("固定长度")) as usize;
                if len.checked_add(64) == Some(remaining.len()) {
                    return Ok(ScanResult {
                        records,
                        valid_len: offset,
                        torn_tail: true,
                    });
                }
                return Err(ScanError::Corrupt);
            }
            Err(_) => return Err(ScanError::Corrupt),
        }
    }
    Ok(ScanResult {
        records,
        valid_len: offset,
        torn_tail: false,
    })
}

/// 完整且已验证的尾部头仍须满足分片身份与序号连续性。
fn check_tail_identity(previous: Option<&WalRecord>, bytes: &[u8]) -> Result<(), ScanError> {
    if bytes.len() < 60 {
        return Ok(());
    }
    let cluster = u16::from_be_bytes(bytes[6..8].try_into().expect("固定长度"));
    let shard = u16::from_be_bytes(bytes[8..10].try_into().expect("固定长度"));
    let seq = u64::from_be_bytes(bytes[20..28].try_into().expect("固定长度"));
    if let Some(previous) = previous
        && (previous.cluster_id != cluster
            || previous.shard_id != shard
            || previous.shard_seq.checked_add(1) != Some(seq))
    {
        return Err(ScanError::Sequence);
    }
    Ok(())
}
