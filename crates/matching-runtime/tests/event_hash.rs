//! 验证事件序列摘要对顺序和身份的敏感性。
use matching_domain::{
    ClusterId, CommandId, EngineEvent, EventEnvelope, MarketId, OrderId, ShardId,
};
use matching_runtime::EventHasher;
fn event(index: u32, id: u128) -> EventEnvelope {
    EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(2),
        shard_seq: 3,
        event_index: index,
        command_id: CommandId(4),
        engine_time: 5,
        market_id: Some(MarketId(6)),
        payload: EngineEvent::OrderAccepted {
            order_id: OrderId(id),
        },
    }
}
#[test]
fn event_hash_preserves_original_order_and_all_envelope_fields() {
    let a = event(0, 7);
    let b = event(1, 8);
    let mut h = EventHasher::new();
    h.update(&a).unwrap();
    h.update(&b).unwrap();
    let digest = h.finish();
    let mut h = EventHasher::new();
    h.update(&a).unwrap();
    h.update(&b).unwrap();
    assert_eq!(digest, h.finish());
    let mut h = EventHasher::new();
    h.update(&b).unwrap();
    h.update(&a).unwrap();
    assert_ne!(digest, h.finish());
    let mut changed = a.clone();
    changed.engine_time += 1;
    let mut h = EventHasher::new();
    h.update(&changed).unwrap();
    h.update(&b).unwrap();
    assert_ne!(digest, h.finish());
}

#[test]
fn event_hash_matches_independent_sha256_vectors() {
    assert_eq!(
        EventHasher::new().finish(),
        [
            0x3e, 0x43, 0xd3, 0x51, 0xa5, 0x38, 0x45, 0xab, 0x6b, 0xf1, 0x03, 0x79, 0x1c, 0xb4,
            0xdc, 0x23, 0xbd, 0xaf, 0x07, 0x94, 0xce, 0x48, 0x15, 0x80, 0x10, 0x93, 0x04, 0xef,
            0x6e, 0x89, 0x28, 0x2d,
        ]
    );
    let mut hash = EventHasher::new();
    hash.update(&event(0, 7)).unwrap();
    assert_eq!(
        hash.finish(),
        [
            0x70, 0x38, 0xd1, 0x24, 0xf8, 0xbd, 0x38, 0x97, 0x01, 0x41, 0xaa, 0x97, 0xca, 0xa7,
            0x3e, 0xc2, 0x8a, 0x2d, 0x11, 0x16, 0x82, 0xab, 0x40, 0x80, 0x68, 0xb6, 0x9b, 0x0c,
            0xf7, 0x73, 0x7a, 0x2e,
        ]
    );
}
