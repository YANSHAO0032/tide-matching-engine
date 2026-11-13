//! 订单终结时释放本地预留的稳定业务原因。

/// `ReservationReleased` 事件载荷使用的稳定预留释放原因。
///
/// 数值是公开协议；只能追加，不能重排或复用。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationReleaseReason {
    /// 成交后经真实剩余预留计算确认的未使用部分。
    FilledRemainder = 1301,
    /// 用户主动取消订单；撤单替换的旧预留仅在替换下单事件链之后由真实 R2 释放。
    UserCancel = 1302,
    /// 批量撤单终结后的真实剩余预留释放原因；PendingR2 转换本身不发出该事件。
    MassCancel = 1303,
    /// 订单因逻辑到期或时段结束而终结。
    Expired = 1304,
    /// 触发后的 child 未通过 Trigger-R1。
    TriggerRejected = 1305,
    /// 市价保护终止后释放未使用预留。
    MarketProtection = 1306,
    /// 市场关闭导致订单终结。
    MarketClosed = 1307,
    /// 自成交防护终结订单后释放真实剩余预留。
    SelfTradePrevention = 1309,
    /// 立即执行的订单终结后释放未使用预留。
    IocRemainder = 1310,
}

#[cfg(test)]
mod tests {
    use super::ReservationReleaseReason;

    #[test]
    fn trigger_rejected_release_reason_code_is_stable() {
        assert_eq!(ReservationReleaseReason::TriggerRejected as u16, 1305);
        assert_eq!(ReservationReleaseReason::MarketProtection as u16, 1306);
        assert_eq!(ReservationReleaseReason::Expired as u16, 1304);
        assert_eq!(ReservationReleaseReason::MarketClosed as u16, 1307);
        assert_eq!(ReservationReleaseReason::SelfTradePrevention as u16, 1309);
        assert_eq!(ReservationReleaseReason::IocRemainder as u16, 1310);
    }
}
