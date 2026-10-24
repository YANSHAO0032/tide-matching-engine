/// 订单到期的稳定业务原因。
///
/// 数值是公开协议的一部分；只能追加，不能重排或复用。
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireReason {
    /// 常规 GTD 订单达到其逻辑到期时间。
    GtdReached = 1201,
    /// 常规 DAY 订单因交易时段结束而到期。
    SessionEnded = 1202,
    /// 未触发 Stop 的 GTD 生命周期达到逻辑到期时间。
    StopTriggerGtdReached = 1203,
    /// 未触发 Stop 的 DAY 生命周期因交易时段结束而到期。
    StopTriggerSessionEnded = 1204,
    /// 市场关闭时的到期终结。
    MarketClosed = 1205,
}

#[cfg(test)]
mod tests {
    //! 固定到期原因的线协议编号，防止重排或复用。
    use super::ExpireReason;

    #[test]
    fn expiry_reason_codes_are_stable() {
        assert_eq!(ExpireReason::GtdReached as u16, 1201);
        assert_eq!(ExpireReason::SessionEnded as u16, 1202);
        assert_eq!(ExpireReason::StopTriggerGtdReached as u16, 1203);
        assert_eq!(ExpireReason::StopTriggerSessionEnded as u16, 1204);
        assert_eq!(ExpireReason::MarketClosed as u16, 1205);
    }
}
