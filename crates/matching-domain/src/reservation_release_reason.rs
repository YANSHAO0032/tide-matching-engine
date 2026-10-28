//! 订单终结时释放本地预留的稳定业务原因。

/// `ReservationReleased` 事件载荷使用的稳定预留释放原因。
///
/// 数值是公开协议；只能追加，不能重排或复用。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationReleaseReason {
    /// 成交后经真实剩余预留计算确认的未使用部分；当前尚无真实 R2 计算，故不发出此原因。
    FilledRemainder = 1301,
    /// 用户主动取消订单；撤单替换的旧预留仅在替换下单事件链之后由真实 R2 释放。
    UserCancel = 1302,
    /// 批量撤单终结后的真实剩余预留释放原因；PendingR2 转换本身不发出该事件。
    MassCancel = 1303,
    /// 触发后的 child 未通过 Trigger-R1。
    TriggerRejected = 1305,
}

#[cfg(test)]
mod tests {
    use super::ReservationReleaseReason;

    #[test]
    fn trigger_rejected_release_reason_code_is_stable() {
        assert_eq!(ReservationReleaseReason::TriggerRejected as u16, 1305);
    }
}
