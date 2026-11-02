//! 验证分片标识与入口拒绝原因的稳定协议边界。
use matching_domain::{RejectReason, ShardId};

#[test]
fn shard_id_preserves_16_bit_identity() {
    assert_eq!(std::mem::size_of::<ShardId>(), 2);
    assert_eq!(ShardId::new(u16::MAX).get(), u16::MAX);
}

#[test]
fn ingress_rejection_codes_are_stable() {
    assert_eq!(RejectReason::MigrationInProgress as u16, 1008);
    assert_eq!(RejectReason::StaleRouteEpoch as u16, 1009);
    assert_eq!(RejectReason::EngineBusy as u16, 5002);
}
