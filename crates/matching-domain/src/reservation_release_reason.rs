/// 订单终结时释放本地预留的稳定原因。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationReleaseReason {
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
