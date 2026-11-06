//! 快照文件的有界校验和持久提交。
use crate::{ManifestIntent, ManifestManager, ManifestStorage, crc32c};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

/// 单个快照载荷的最大字节数。
const MAX_IMAGE: u64 = 256 * 1024 * 1024;
/// 固定头长度。
const HEADER: usize = 30;

/// 将不可变镜像写入新文件，并在目录同步后通过唯一清单管理器提交。
///
/// 调用方须保证快照目录独占、镜像来自分片一致性屏障且仅包含已经应用的状态。
/// 调用前清单必须已完成双槽初始化；本接口只检查清单已发布代际。
/// 文件持久化成功但清单更新失败时可能留下未提交文件，错误必须向调用方传播。
pub fn commit_snapshot<S: ManifestStorage>(
    directory: impl AsRef<Path>,
    manager: &mut ManifestManager<S>,
    snapshot_id: u64,
    snapshot_seq: u64,
    image: &[u8],
) -> io::Result<()> {
    let directory = directory.as_ref();
    let size = u64::try_from(image.len()).map_err(|_| invalid("镜像长度溢出"))?;
    if size > MAX_IMAGE
        || manager.state().generation < 2
        || snapshot_seq > manager.state().local_durable_seq
        || manager
            .state()
            .latest_snapshot
            .is_some_and(|(_, previous)| snapshot_seq <= previous)
    {
        return Err(invalid("清单未初始化、快照超出持久边界或序号未递增"));
    }
    write_snapshot_file(directory, snapshot_id, snapshot_seq, image)?;
    manager.submit(ManifestIntent::SnapshotCommitted {
        snapshot_id,
        snapshot_seq,
    })
}

/// 将不可变载荷写成已同步的快照文件；调用者仍须通过唯一清单写者提交。
///
/// 目录须由调用方独占，成功仅表示文件已同步，尚不表示快照已提交。
pub fn write_snapshot_file(
    directory: impl AsRef<Path>,
    snapshot_id: u64,
    snapshot_seq: u64,
    image: &[u8],
) -> io::Result<()> {
    write_snapshot_file_inner(directory.as_ref(), snapshot_id, snapshot_seq, image, |_| {
        Ok(())
    })
}

/// 允许测试在真实文件操作边界注入中断。
fn write_snapshot_file_inner(
    directory: &Path,
    snapshot_id: u64,
    snapshot_seq: u64,
    image: &[u8],
    mut after: impl FnMut(u8) -> io::Result<()>,
) -> io::Result<()> {
    let size = u64::try_from(image.len()).map_err(|_| invalid("镜像长度溢出"))?;
    if size > MAX_IMAGE {
        return Err(invalid("镜像超过长度上限"));
    }
    let tmp = directory.join(format!("snapshot-{snapshot_id}.tmp"));
    let target = directory.join(format!("snapshot-{snapshot_id}.bin"));
    if target.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "快照身份已存在",
        ));
    }
    let mut bytes = Vec::with_capacity(HEADER + image.len() + 4);
    bytes.extend_from_slice(b"SNP1");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&snapshot_id.to_be_bytes());
    bytes.extend_from_slice(&snapshot_seq.to_be_bytes());
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(image);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    file.write_all(&bytes)?;
    after(1)?;
    file.sync_all()?;
    after(2)?;
    drop(file);
    fs::rename(&tmp, &target)?;
    after(3)?;
    crate::manifest_file::sync_directory(directory)?;
    after(4)?;
    Ok(())
}

/// 读取并校验完整快照文件，返回身份、序号和不透明镜像。
pub fn read_snapshot(path: impl AsRef<Path>) -> io::Result<(u64, u64, Vec<u8>)> {
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    if !(34..=MAX_IMAGE + 34).contains(&size) {
        return Err(invalid("快照长度无效"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_IMAGE + 35).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).ok() != Some(size)
        || &bytes[..4] != b"SNP1"
        || bytes[4..6] != 1_u16.to_be_bytes()
    {
        return Err(invalid("快照头无效"));
    }
    let id = u64::from_be_bytes(bytes[6..14].try_into().map_err(|_| invalid("快照头截断"))?);
    let seq = u64::from_be_bytes(
        bytes[14..22]
            .try_into()
            .map_err(|_| invalid("快照头截断"))?,
    );
    let len = u64::from_be_bytes(
        bytes[22..30]
            .try_into()
            .map_err(|_| invalid("快照头截断"))?,
    );
    if len > MAX_IMAGE || len.checked_add(34) != Some(size) {
        return Err(invalid("快照载荷长度无效"));
    }
    let end = bytes.len() - 4;
    let expected = u32::from_be_bytes(
        bytes[end..]
            .try_into()
            .map_err(|_| invalid("快照校验值截断"))?,
    );
    if crc32c(&bytes[..end]) != expected {
        return Err(invalid("快照校验失败"));
    }
    Ok((id, seq, bytes[HEADER..end].to_vec()))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// 按已验证清单读取当前已提交快照，并核对文件中的身份与序号。
///
/// 清单没有快照时返回空；文件缺失、损坏或不一致均返回错误。
pub fn read_committed_snapshot<S: ManifestStorage>(
    directory: impl AsRef<Path>,
    manager: &ManifestManager<S>,
) -> io::Result<Option<(u64, u64, Vec<u8>)>> {
    let Some((id, seq)) = manager.state().latest_snapshot else {
        return Ok(None);
    };
    let snapshot = read_snapshot(directory.as_ref().join(format!("snapshot-{id}.bin")))?;
    if snapshot.0 != id || snapshot.1 != seq {
        return Err(invalid("快照与清单不一致"));
    }
    Ok(Some(snapshot))
}

/// 从严格恢复检查通过的清单读取上一份已提交快照；不执行后续 WAL 回放。
pub fn read_previous_snapshot(
    directory: impl AsRef<Path>,
    manager: &ManifestManager<crate::FileManifestStorage>,
) -> io::Result<Option<(u64, u64, Vec<u8>)>> {
    if !manager.recovery_checked() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "清单未经过严格恢复检查",
        ));
    }
    let Some((id, seq)) = manager.state().previous_snapshot else {
        return Ok(None);
    };
    let image = read_snapshot(directory.as_ref().join(format!("snapshot-{id}.bin")))?;
    if image.0 != id || image.1 != seq {
        return Err(invalid("上一快照与清单不一致"));
    }
    Ok(Some(image))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileManifestStorage, ManifestIntent};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn interruption_after_each_file_step_never_publishes_manifest() {
        for stop in 1..=4 {
            let root = std::env::temp_dir().join(format!(
                "snapshot-boundary-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            let manifest = root.join("manifest");
            let snapshots = root.join("snapshots");
            fs::create_dir(&manifest).unwrap();
            fs::create_dir(&snapshots).unwrap();
            let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
            manager.submit(ManifestIntent::Initialize).unwrap();
            manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
            let result = write_snapshot_file_inner(&snapshots, 7, 0, b"image", |stage| {
                if stage == stop {
                    Err(io::Error::other("注入边界中断"))
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err());
            drop(manager);
            let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
            assert_eq!(manager.state().latest_snapshot, None);
            if stop >= 3 {
                assert_eq!(
                    read_snapshot(snapshots.join("snapshot-7.bin")).unwrap(),
                    (7, 0, b"image".to_vec())
                );
            } else {
                assert!(!snapshots.join("snapshot-7.bin").exists());
            }
            drop(manager);
            fs::remove_dir_all(root).unwrap();
        }
    }
}
