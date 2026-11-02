use matching_domain::{ClusterId, RejectReason, ShardId, TradeIdCursor};

#[test]
fn cursor_encodes_each_field_and_indexes_only_when_called() {
    let mut cursor = TradeIdCursor::new(ClusterId(0x1234), ShardId(0xabcd), 0x0123_4567_89ab_cdef);
    let prefix = (0x1234u128 << 112) | (0xabcdu128 << 96) | (0x0123_4567_89ab_cdefu128 << 32);
    assert_eq!(cursor.next_trade_id().unwrap().0, prefix);
    assert_eq!(cursor.next_trade_id().unwrap().0, prefix | 1);
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn last_index_is_valid_then_exhaustion_is_atomic() {
    let mut cursor =
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u32::MAX as u64).unwrap();
    assert_eq!(
        cursor.next_trade_id().unwrap().0 & u128::from(u32::MAX),
        u128::from(u32::MAX)
    );
    let before = cursor.clone();
    assert_eq!(
        cursor.next_trade_id(),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(cursor, before);
    assert_eq!(
        cursor.next_trade_id(),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u64::MAX),
        Err(RejectReason::ArithmeticOverflow)
    );
}
