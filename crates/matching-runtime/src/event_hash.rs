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
        // 与快照单载荷上限一致，编码器仅按实际追加量分配。
        let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
        encode_event_for_hash(&mut w, event)?;
        self.hash.update(w.finish());
        Ok(())
    }
    /// 结束摘要并返回固定长度结果。
    pub fn finish(self) -> [u8; 32] {
        self.hash.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::EventHasher;
    use matching_domain::{
        AssetId, AssetReserve, ClusterId, CommandId, EngineEvent, EventEnvelope, OrderId,
        ReservationId, ReservationReleaseReason, ShardId,
    };
    #[test]
    fn hashes_large_valid_release_without_truncation() {
        let event = EventEnvelope {
            cluster_id: ClusterId(1),
            shard_id: ShardId(2),
            shard_seq: 3,
            event_index: 4,
            command_id: CommandId(5),
            engine_time: 6,
            market_id: None,
            payload: EngineEvent::ReservationReleased {
                order_id: OrderId(8),
                reservation_id: ReservationId(9),
                reason: ReservationReleaseReason::MassCancel,
                released: Some(
                    (0..100)
                        .map(|asset| AssetReserve {
                            asset: AssetId(asset),
                            atoms: 1,
                        })
                        .collect(),
                ),
            },
        };
        let mut h = EventHasher::new();
        h.update(&event).unwrap();
        let mut changed = event;
        if let EngineEvent::ReservationReleased {
            released: Some(assets),
            ..
        } = &mut changed.payload
        {
            assets.last_mut().unwrap().atoms = 2;
        }
        let mut other = EventHasher::new();
        other.update(&changed).unwrap();
        assert_ne!(h.finish(), other.finish());
    }
}
