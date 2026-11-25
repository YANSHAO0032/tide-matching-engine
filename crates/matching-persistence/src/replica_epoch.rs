//! 备节点接任栅栏的单写者持久纪元日志。
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// 固定记录长度；包括身份、纪元、备节点水位和校验值。
const RECORD_LEN: usize = 28;
/// 单文件最多保留的栅栏记录数。
const MAX_RECORDS: u64 = 1_000_000;

/// 经持久确认的备节点栅栏状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaEpochState {
    /// 所属集群身份。
    pub cluster_id: u16,
    /// 所属分片身份。
    pub shard_id: u16,
    /// 已持久确认的主节点纪元。
    pub epoch: u64,
    /// 建立栅栏时实际持久的备节点前缀。
    pub durable_seq: u64,
}

/// 可信目录中的排他备节点栅栏日志。
///
/// 调用方须在同一备节点单写者顺序点校验远端写权，并以真实 WAL 水位调用推进；
/// 本地日志不自行证明远端租约有效，也不允许文件替换或不可靠的网络文件系统。
pub struct ReplicaEpochStore {
    /// 持有排他锁的持久文件。
    file: File,
    /// 最近成功同步的栅栏状态。
    state: ReplicaEpochState,
    /// 已确认的固定记录数量。
    records: u64,
    /// 写入结果未知后永久停止推进。
    halted: bool,
    /// 在记录写入后模拟同步结果未知。
    #[cfg(test)]
    fail_sync: bool,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn lock(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => {
            io::Error::new(io::ErrorKind::WouldBlock, "备节点纪元日志已被占用")
        }
        TryLockError::Error(error) => error,
    })
}
fn encode(state: ReplicaEpochState) -> [u8; RECORD_LEN] {
    let mut bytes = [0_u8; RECORD_LEN];
    bytes[..4].copy_from_slice(b"RFE1");
    bytes[4..6].copy_from_slice(&state.cluster_id.to_be_bytes());
    bytes[6..8].copy_from_slice(&state.shard_id.to_be_bytes());
    bytes[8..16].copy_from_slice(&state.epoch.to_be_bytes());
    bytes[16..24].copy_from_slice(&state.durable_seq.to_be_bytes());
    let checksum = crate::crc32c(&bytes[..24]);
    bytes[24..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}
fn decode(bytes: &[u8; RECORD_LEN], cluster: u16, shard: u16) -> io::Result<ReplicaEpochState> {
    if &bytes[..4] != b"RFE1" || crate::crc32c(&bytes[..24]).to_be_bytes() != bytes[24..] {
        return Err(invalid("备节点纪元记录损坏"));
    }
    let state = ReplicaEpochState {
        cluster_id: u16::from_be_bytes([bytes[4], bytes[5]]),
        shard_id: u16::from_be_bytes([bytes[6], bytes[7]]),
        epoch: u64::from_be_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| invalid("备节点纪元无效"))?,
        ),
        durable_seq: u64::from_be_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| invalid("备节点水位无效"))?,
        ),
    };
    if state.cluster_id != cluster || state.shard_id != shard || state.epoch == 0 {
        return Err(invalid("备节点纪元身份不一致"));
    }
    Ok(state)
}

impl ReplicaEpochStore {
    /// 在不存在的路径创建初始栅栏，并同步文件及父目录。
    pub fn create(
        path: impl AsRef<Path>,
        cluster: u16,
        shard: u16,
        epoch: u64,
        durable_seq: u64,
    ) -> io::Result<Self> {
        if epoch == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "备节点纪元为零",
            ));
        }
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        lock(&file)?;
        let state = ReplicaEpochState {
            cluster_id: cluster,
            shard_id: shard,
            epoch,
            durable_seq,
        };
        file.write_all(&encode(state))?;
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        crate::sync_directory(parent)?;
        Ok(Self {
            file,
            state,
            records: 1,
            halted: false,
            #[cfg(test)]
            fail_sync: false,
        })
    }
    /// 独占并完整校验既有日志；截断、损坏、身份或单调性错误失败关闭。
    pub fn open(path: impl AsRef<Path>, cluster: u16, shard: u16) -> io::Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        lock(&file)?;
        let length = file.metadata()?.len();
        let record_len = u64::try_from(RECORD_LEN).map_err(|_| invalid("记录长度越界"))?;
        if length == 0 || length % record_len != 0 || length / record_len > MAX_RECORDS {
            return Err(invalid("备节点纪元日志长度无效"));
        }
        let records = length / record_len;
        let mut previous: Option<ReplicaEpochState> = None;
        for _ in 0..records {
            let mut bytes = [0_u8; RECORD_LEN];
            file.read_exact(&mut bytes)?;
            let state = decode(&bytes, cluster, shard)?;
            if previous
                .is_some_and(|old| state.epoch <= old.epoch || state.durable_seq < old.durable_seq)
            {
                return Err(invalid("备节点纪元或水位回退"));
            }
            previous = Some(state);
        }
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        crate::sync_directory(parent)?;
        Ok(Self {
            file,
            state: previous.ok_or_else(|| invalid("备节点纪元日志为空"))?,
            records,
            halted: false,
            #[cfg(test)]
            fail_sync: false,
        })
    }
    /// 返回最近已持久确认的状态。
    pub fn state(&self) -> ReplicaEpochState {
        self.state
    }
    /// 使下一次同步返回测试用错误。
    #[cfg(test)]
    pub(crate) fn fail_next_sync(&mut self) {
        self.fail_sync = true;
    }
    /// 在调用方核验写权后同步推进纪元和真实备节点持久前缀。
    pub fn advance(
        &mut self,
        expected_epoch: u64,
        next_epoch: u64,
        durable_seq: u64,
    ) -> io::Result<()> {
        if self.halted {
            return Err(io::Error::other("备节点纪元日志已停止"));
        }
        if expected_epoch != self.state.epoch
            || next_epoch <= expected_epoch
            || durable_seq < self.state.durable_seq
            || self.records >= MAX_RECORDS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "备节点栅栏版本或水位无效",
            ));
        }
        let records = self
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("备节点纪元记录数耗尽"))?;
        let state = ReplicaEpochState {
            epoch: next_epoch,
            durable_seq,
            ..self.state
        };
        let expected_len = self
            .records
            .checked_mul(u64::try_from(RECORD_LEN).map_err(|_| invalid("记录长度越界"))?)
            .ok_or_else(|| invalid("备节点纪元日志长度耗尽"))?;
        let result = self
            .file
            .seek(SeekFrom::End(0))
            .and_then(|position| {
                if position != expected_len {
                    return Err(invalid("备节点纪元日志长度发生变化"));
                }
                self.file.write_all(&encode(state))
            })
            .and_then(|()| {
                #[cfg(test)]
                if self.fail_sync {
                    return Err(io::Error::other("模拟栅栏同步结果未知"));
                }
                self.file.sync_all()
            });
        if let Err(error) = result {
            self.halted = true;
            return Err(error);
        }
        self.state = state;
        self.records = records;
        Ok(())
    }
}
