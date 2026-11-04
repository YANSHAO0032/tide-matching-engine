//! WAL、快照、清单、恢复和持久性边界的规划位置。

mod record;
mod scan;
mod segment;
pub use record::{WalError, WalRecord};
pub use scan::{ScanError, ScanResult, scan_segment};
pub use segment::{SegmentMeta, SegmentScan, SegmentWriter, scan_segments, truncate_torn_tail};

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
