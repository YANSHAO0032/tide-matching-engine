//! 单机可信目录中的排他主节点纪元日志。
use matching_domain::{ClusterId, ShardId};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// 主节点租约获取与校验失败。
#[derive(Debug)]
pub enum LeaseError {
    /// 其他句柄持有排他租约。
    Busy,
    /// 纪元日志截断、损坏或身份不一致。
    Corrupt,
    /// 纪元空间已耗尽。
    Exhausted,
    /// 调用方提供的身份或纪元已过期。
    Stale,
    /// 文件系统错误；不得忽略或降级。
    Io(std::io::Error),
}

/// 保存在可信目录中的分片排他纪元日志。
///
/// 所有参与者必须访问同一真实文件且遵守文件锁；不支持文件替换或不可靠的网络文件系统。
#[derive(Clone, Debug)]
pub struct ControlStore {
    /// 保存在可信目录中的纪元日志路径。
    path: PathBuf,
    /// 日志绑定的集群身份。
    cluster: ClusterId,
    /// 日志绑定的分片身份。
    shard: ShardId,
}

/// 持有操作系统排他锁的主节点租约；关闭句柄会释放锁。
#[derive(Debug)]
pub struct PrimaryLease {
    /// 保持排他锁存活的文件句柄。
    _file: File,
    /// 租约绑定的集群身份。
    cluster: ClusterId,
    /// 租约绑定的分片身份。
    shard: ShardId,
    /// 持久追加并由当前句柄持有的主节点纪元。
    epoch: u64,
}

/// 在单机租约持有者进程中派生稳定分片控制子命令。
pub struct ControlCoordinator;

impl ControlCoordinator {
    /// 从父操作、范围与动作派生身份；重试保留同一身份，策略冲突由去重窗口拒绝。
    pub fn shard_kill_subcommand(
        lease: &PrimaryLease,
        parent: matching_domain::CommandId,
        active: bool,
        cancel_all: bool,
    ) -> (matching_domain::CommandId, matching_domain::ShardKillSwitch) {
        let id = Self::derive_shard_kill_id(lease.cluster, lease.shard, parent, active);
        (
            id,
            matching_domain::ShardKillSwitch {
                target_shard_id: lease.shard,
                parent_command_id: parent,
                expected_primary_epoch: lease.epoch,
                active,
                cancel_all,
            },
        )
    }

    /// 使用固定域、集群、分片、父操作和动作计算稳定子命令身份。
    pub fn derive_shard_kill_id(
        cluster: ClusterId,
        shard: ShardId,
        parent: matching_domain::CommandId,
        active: bool,
    ) -> matching_domain::CommandId {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"tide/shard-kill/v1");
        hash.update(cluster.get().to_be_bytes());
        hash.update(shard.get().to_be_bytes());
        hash.update(parent.get().to_be_bytes());
        hash.update([u8::from(active)]);
        let digest = hash.finalize();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        matching_domain::CommandId::new(u128::from_be_bytes(id))
    }
}

impl ControlStore {
    /// 绑定可信目录中的租约文件与分片身份。
    pub fn new(path: impl AsRef<Path>, cluster: ClusterId, shard: ShardId) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            cluster,
            shard,
        }
    }

    /// 独占日志并持久追加下一纪元；损坏和不完整记录一律失败关闭。
    pub fn acquire_primary(&self) -> Result<PrimaryLease, LeaseError> {
        const RECORD: usize = 20;
        const MAX_BYTES: u64 = 20 * 1_000_000;
        let open = || OpenOptions::new().read(true).write(true).open(&self.path);
        let (mut file, created) = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)
        {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                (open().map_err(LeaseError::Io)?, false)
            }
            Err(error) => return Err(LeaseError::Io(error)),
        };
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => LeaseError::Busy,
            TryLockError::Error(error) => LeaseError::Io(error),
        })?;
        let length = file.metadata().map_err(LeaseError::Io)?.len();
        if length > MAX_BYTES || length % RECORD as u64 != 0 || (!created && length == 0) {
            return Err(LeaseError::Corrupt);
        }
        if length == MAX_BYTES {
            return Err(LeaseError::Exhausted);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(usize::try_from(length).map_err(|_| LeaseError::Corrupt)?)
            .map_err(|_| LeaseError::Io(std::io::Error::other("租约日志内存不足")))?;
        std::io::Read::by_ref(&mut file)
            .take(length + 1)
            .read_to_end(&mut bytes)
            .map_err(LeaseError::Io)?;
        if u64::try_from(bytes.len()).ok() != Some(length) {
            return Err(LeaseError::Corrupt);
        }
        let mut last = 0_u64;
        let (records, remainder) = bytes.as_chunks::<RECORD>();
        if !remainder.is_empty() {
            return Err(LeaseError::Corrupt);
        }
        for record in records {
            if &record[..4] != b"CLS1"
                || record[4..6] != self.cluster.get().to_be_bytes()
                || record[6..8] != self.shard.get().to_be_bytes()
                || matching_persistence::crc32c(&record[..16]).to_be_bytes() != record[16..20]
            {
                return Err(LeaseError::Corrupt);
            }
            let epoch =
                u64::from_be_bytes(record[8..16].try_into().map_err(|_| LeaseError::Corrupt)?);
            if last.checked_add(1) != Some(epoch) {
                return Err(LeaseError::Corrupt);
            }
            last = epoch;
        }
        let epoch = last.checked_add(1).ok_or(LeaseError::Exhausted)?;
        let mut record = [0_u8; RECORD];
        record[..4].copy_from_slice(b"CLS1");
        record[4..6].copy_from_slice(&self.cluster.get().to_be_bytes());
        record[6..8].copy_from_slice(&self.shard.get().to_be_bytes());
        record[8..16].copy_from_slice(&epoch.to_be_bytes());
        let crc = matching_persistence::crc32c(&record[..16]);
        record[16..].copy_from_slice(&crc.to_be_bytes());
        file.seek(SeekFrom::End(0)).map_err(LeaseError::Io)?;
        file.write_all(&record)
            .and_then(|()| file.sync_all())
            .map_err(LeaseError::Io)?;
        if created {
            let parent = self
                .path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            matching_persistence::sync_directory(parent).map_err(LeaseError::Io)?;
        }
        Ok(PrimaryLease {
            _file: file,
            cluster: self.cluster,
            shard: self.shard,
            epoch,
        })
    }
}

impl PrimaryLease {
    /// 返回持有的持久纪元。
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// 校验调用方身份与所持排他租约纪元。
    pub fn verify(&self, cluster: ClusterId, shard: ShardId, epoch: u64) -> Result<(), LeaseError> {
        if (self.cluster, self.shard, self.epoch) == (cluster, shard, epoch) {
            Ok(())
        } else {
            Err(LeaseError::Stale)
        }
    }
}
