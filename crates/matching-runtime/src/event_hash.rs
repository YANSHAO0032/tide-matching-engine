//! 按业务原序累积的确定性事件摘要。
use matching_domain::{
    EventEnvelope, encode_event_for_hash,
    snapshot_wire::{SnapshotEncoder, SnapshotWireError},
};
use sha2::{Digest, Sha256};

/// 使用版本化有界事件编码计算连续 SHA-256 摘要。
pub struct EventHasher {
    /// 已消费的原序事件摘要状态。
    hash: Sha256,
}
impl Default for EventHasher {
    fn default() -> Self {
        Self::new()
    }
}
impl EventHasher {
    /// 创建空事件流摘要，并写入领域分隔前缀。
    pub fn new() -> Self {
        let mut hash = Sha256::new();
        hash.update(b"matching-event-hash-v1\0");
        Self { hash }
    }
    /// 追加一个完整事件；编码失败时不改变摘要。
    pub fn update(&mut self, event: &EventEnvelope) -> Result<(), SnapshotWireError> {
        let mut w = SnapshotEncoder::new(1024);
        encode_event_for_hash(&mut w, event)?;
        self.hash.update(w.finish());
        Ok(())
    }
    /// 结束摘要并返回固定长度结果。
    pub fn finish(self) -> [u8; 32] {
        self.hash.finalize().into()
    }
}
