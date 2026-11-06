//! 单档先进先出、局部故障时停止处理预检与独立结构模型的验证。
//!
//! 验证器和性质测试模型只在测试中存在，不是生产订单簿的不变量入口。

use super::*;
pub(super) use matching_domain::{OrderId, Qty, QueuePriority, Side};
pub(super) use proptest::prelude::*;
pub(super) use std::panic::{AssertUnwindSafe, catch_unwind};

#[path = "tests/support.rs"]
mod support;
use support::*;

#[path = "tests/model.rs"]
mod model;
#[path = "tests/partial_fill.rs"]
mod partial_fill;
#[path = "tests/pop_front.rs"]
mod pop_front;
#[path = "tests/push_back.rs"]
mod push_back;
#[path = "tests/unlink.rs"]
mod unlink;

#[test]
fn snapshot_price_level_roundtrips_and_rejects_invalid_endpoints() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let level = PriceLevel {
        head: Some(OrderIndex::try_from_usize(2).unwrap()),
        tail: Some(OrderIndex::try_from_usize(5).unwrap()),
        count: 2,
        total_visible_qty: 8,
    };
    let mut writer = SnapshotEncoder::new(128);
    level.write_snapshot(&mut writer).unwrap();
    let bytes = writer.finish();
    let mut reader = SnapshotDecoder::new(&bytes, 128).unwrap();
    assert_eq!(PriceLevel::read_snapshot(&mut reader).unwrap(), level);
    reader.finish().unwrap();
    let mut invalid = bytes.clone();
    invalid[4] = 0;
    let mut reader = SnapshotDecoder::new(&invalid, 128).unwrap();
    assert!(PriceLevel::read_snapshot(&mut reader).is_err());
    for end in 0..bytes.len() {
        let mut reader = SnapshotDecoder::new(&bytes[..end], 128).unwrap();
        assert!(PriceLevel::read_snapshot(&mut reader).is_err());
    }
}
