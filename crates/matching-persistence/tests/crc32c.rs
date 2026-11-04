//! 固定 CRC32C 的标准校验向量。

#[test]
fn crc32c_matches_standard_check_value() {
    assert_eq!(matching_persistence::crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(matching_persistence::crc32c(b""), 0);
}
