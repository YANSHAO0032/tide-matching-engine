//! 按首序号命名的有限大小 segment 与顺序扫描。
use crate::{WalRecord, scan_segment};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// 文件扫描及写入允许的最大单个 segment 字节数。
const MAX_SEGMENT: u64 = 64 * 1024 * 1024;

/// 已扫描 segment 的实际范围与尾部状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    /// 文件名中声明的首序号。
    pub first_seq: u64,
    /// 最后完整记录的序号；空文件为空。
    pub last_seq: Option<u64>,
    /// 扫描时文件字节数。
    pub size: u64,
    /// 最后完整记录的结束偏移。
    pub valid_len: u64,
    /// 扫描所得整个文件的 CRC32C；未与持久化 manifest 比对。
    pub checksum: u32,
    /// 是否存在可识别的尾记录撕裂。
    pub torn_tail: bool,
}

/// 顺序扫描得到的记录和每个 segment 的元数据。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentScan {
    /// 原序完整记录。
    pub records: Vec<WalRecord>,
    /// 按首序号排列的文件信息。
    pub segments: Vec<SegmentMeta>,
}

/// 单写者文件追加器；调用方须确保目录仅由一个写入器使用。
///
/// 写入成功不代表数据已经持久化；该类型不提供进程间独占锁。
///
/// 低层写入器不能直接用于推进持久提交水位：
/// ```compile_fail
/// fn requires_registered<T: matching_persistence::DurableAppender>() {}
/// requires_registered::<matching_persistence::SegmentWriter>();
/// ```
pub struct SegmentWriter {
    /// 已存在的 segment 目录。
    directory: PathBuf,
    /// 允许的最大文件长度。
    max_size: u64,
    /// 当前追加文件。
    file: Option<File>,
    /// 当前 segment 的首序号。
    active_first: Option<u64>,
    /// 当前文件已写字节数。
    size: u64,
    /// 最近成功写入的分片身份和序号。
    last: Option<(u16, u16, u64)>,
    /// 序号或分片身份不连续、文件写入错误后禁止继续使用。
    failed: bool,
}
impl SegmentWriter {
    /// 仅对空的已存在目录建立新写入器。
    pub fn new(directory: impl AsRef<Path>, max_size: u64) -> io::Result<Self> {
        if !(64..=MAX_SEGMENT).contains(&max_size) {
            return Err(invalid("无效 segment 容量"));
        }
        let directory = directory.as_ref().to_path_buf();
        if fs::read_dir(&directory)?.next().transpose()?.is_some() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "目录非空"));
        }
        Ok(Self {
            directory,
            max_size,
            file: None,
            active_first: None,
            size: 0,
            last: None,
            failed: false,
        })
    }

    /// 按序号追加；文件达到上限时创建新 segment。
    pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("写入器已失败"));
        }
        if record.shard_seq == 0
            || self.last.is_some_and(|(cluster, shard, seq)| {
                cluster != record.cluster_id
                    || shard != record.shard_id
                    || seq.checked_add(1) != Some(record.shard_seq)
            })
        {
            self.failed = true;
            return Err(invalid("分片身份或序号不连续"));
        }
        let bytes = record.encode().map_err(|_| invalid("记录不能编码"))?;
        let len = u64::try_from(bytes.len()).map_err(|_| invalid("记录长度溢出"))?;
        if len > self.max_size {
            return Err(invalid("记录超过 segment 容量"));
        }
        if self.file.is_none()
            || self
                .size
                .checked_add(len)
                .is_none_or(|sum| sum > self.max_size)
        {
            let path = self.directory.join(format!("{:020}.wal", record.shard_seq));
            let file = match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(file) => file,
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            };
            self.file = Some(file);
            self.active_first = Some(record.shard_seq);
            self.size = 0;
        }
        if let Err(error) = self.file.as_mut().expect("文件已创建").write_all(&bytes) {
            self.failed = true;
            return Err(error);
        }
        self.size += len;
        self.last = Some((record.cluster_id, record.shard_id, record.shard_seq));
        Ok(())
    }

    /// 返回当前文件的首序号。
    pub(crate) const fn active_first(&self) -> Option<u64> {
        self.active_first
    }

    /// 检查该写入器尚未接收任何记录。
    pub(crate) const fn is_pristine(&self) -> bool {
        self.file.is_none() && self.last.is_none() && !self.failed
    }
}

impl SegmentWriter {
    /// 在改变文件之前判断是否需要轮转。
    pub(crate) fn durable_would_rotate(&self, record: &WalRecord) -> io::Result<bool> {
        if self.failed {
            return Err(io::Error::other("写入器已失败"));
        }
        let len = u64::try_from(record.encode().map_err(|_| invalid("记录不能编码"))?.len())
            .map_err(|_| invalid("记录长度溢出"))?;
        if len > self.max_size {
            return Err(invalid("记录超过 segment 容量"));
        }
        Ok(self.file.is_some()
            && self
                .size
                .checked_add(len)
                .is_none_or(|sum| sum > self.max_size))
    }
    /// 同步当前文件和目录；任一失败均停止后续写入。
    pub(crate) fn durable_sync(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("写入器已失败"));
        }
        let result = self
            .file
            .as_ref()
            .ok_or_else(|| invalid("没有可同步的文件"))?
            .sync_all()
            .and_then(|()| crate::manifest_file::sync_directory(&self.directory));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

/// 按文件名顺序扫描目录，拒绝无效名称、跨文件缺口和非尾文件损坏。
pub fn scan_segments(directory: impl AsRef<Path>) -> io::Result<SegmentScan> {
    let mut files = Vec::new();
    for item in fs::read_dir(directory)? {
        let item = item?;
        let path = item.path();
        if !item.file_type()?.is_file() || path.extension().is_none_or(|ext| ext != "wal") {
            return Err(invalid("segment 目录包含无效条目"));
        }
        let name = item.file_name();
        let name = name.to_str().ok_or_else(|| invalid("无效文件名"))?;
        let digits = name
            .strip_suffix(".wal")
            .ok_or_else(|| invalid("无效文件名"))?;
        if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid("无效文件名"));
        }
        let first: u64 = digits.parse().map_err(|_| invalid("首序号溢出"))?;
        if first == 0 {
            return Err(invalid("首序号为零"));
        }
        files.push((first, path));
    }
    files.sort_by_key(|item| item.0);
    let mut records: Vec<WalRecord> = Vec::new();
    let mut segments = Vec::new();
    for (index, (first, path)) in files.iter().enumerate() {
        let size = fs::metadata(path)?.len();
        if size > MAX_SEGMENT {
            return Err(invalid("segment 超出容量"));
        }
        let mut bytes = Vec::new();
        File::open(path)?
            .take(MAX_SEGMENT + 1)
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).ok() != Some(size) {
            return Err(invalid("扫描期间文件发生变化"));
        }
        let result =
            scan_segment(&bytes, index + 1 == files.len()).map_err(|_| invalid("segment 损坏"))?;
        if result.records.is_empty() && result.torn_tail && bytes.len() >= 60 {
            let seq = u64::from_be_bytes(bytes[20..28].try_into().expect("固定长度"));
            let cluster = u16::from_be_bytes(bytes[6..8].try_into().expect("固定长度"));
            let shard = u16::from_be_bytes(bytes[8..10].try_into().expect("固定长度"));
            if seq != *first
                || records.last().is_some_and(|previous| {
                    previous.cluster_id != cluster || previous.shard_id != shard
                })
            {
                return Err(invalid("尾部头部身份与首序号不匹配"));
            }
        }
        if result
            .records
            .first()
            .is_some_and(|r| r.shard_seq != *first)
            || (result.records.is_empty() && index + 1 != files.len())
        {
            return Err(invalid("segment 首序号不匹配"));
        }
        if let Some(previous) = records.last()
            && (previous.shard_seq.checked_add(1) != Some(*first)
                || result.records.first().is_some_and(|r| {
                    r.cluster_id != previous.cluster_id || r.shard_id != previous.shard_id
                }))
        {
            return Err(invalid("跨 segment 身份或序号不连续"));
        }
        segments.push(SegmentMeta {
            first_seq: *first,
            last_seq: result.records.last().map(|r| r.shard_seq),
            size,
            valid_len: u64::try_from(result.valid_len).map_err(|_| invalid("偏移溢出"))?,
            checksum: crate::crc32c(&bytes),
            torn_tail: result.torn_tail,
        });
        records.extend(result.records);
    }
    Ok(SegmentScan { records, segments })
}

/// 在调用方确保目录独占时截断最后 segment 的已识别撕裂尾部。
///
/// 该操作不执行 fsync，成功不表示截断结果已经持久化。
pub fn truncate_torn_tail(directory: impl AsRef<Path>) -> io::Result<bool> {
    let scan = scan_segments(directory.as_ref())?;
    let Some(last) = scan.segments.last() else {
        return Ok(false);
    };
    if !last.torn_tail {
        return Ok(false);
    }
    let path = directory
        .as_ref()
        .join(format!("{:020}.wal", last.first_seq));
    let file = OpenOptions::new().write(true).open(path)?;
    if file.metadata()?.len() != last.size {
        return Err(invalid("截断前文件发生变化"));
    }
    file.set_len(last.valid_len)?;
    Ok(true)
}

/// 构造拒绝损坏输入的错误。
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
