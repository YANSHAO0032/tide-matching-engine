//! WAL 记录、持久提交、清单、本地恢复与安全回收。

mod durability;
mod manifest;
mod manifest_file;
mod record;
mod recovery;
mod scan;
mod segment;
mod snapshot;
mod wal_gc;
pub use durability::{
    CommitWatermark, DurabilityConfig, DurabilityError, DurabilityMode, DurableAppender,
    DurableBatcher, Profile, RegisteredAppender, ReplicationMode,
};
pub use manifest::{ManifestIntent, ManifestManager, ManifestState, ManifestStorage};
pub use manifest_file::FileManifestStorage;
pub use record::{WalError, WalRecord};
pub use recovery::{recover_committed, recover_local, repair_local_wal};
pub use scan::{ScanError, ScanResult, scan_segment};
pub use segment::{SegmentMeta, SegmentScan, SegmentWriter, scan_segments, truncate_torn_tail};
pub use snapshot::{
    commit_snapshot, read_committed_snapshot, read_previous_snapshot, read_snapshot,
    write_snapshot_file,
};
pub use wal_gc::{WalGcPolicy, run_wal_gc};

/// 计算 Castagnoli 多项式的 CRC32C，用于检测持久化记录损坏。
///
/// 按字节输入顺序计算，初始值与最终异或值均为全一。
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}
