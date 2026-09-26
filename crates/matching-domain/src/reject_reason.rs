/// 业务拒绝原因。
///
/// 判别值是固定的协议编号，不应随意修改
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    InvalidPrice = 2002,
    InvalidQty = 2003,
    DuplicateOrderId = 2008,
    OrderNotFound = 2010,
    InvalidExpireTime = 2013,
    ArithmeticOverflow = 5001,
    PrioritySpaceExhaustion = 5006,
}

#[cfg(test)]
mod tests {
    use crate::reject_reason::RejectReason;

    #[test]
    fn reason_code_golden_tests() {
        assert_eq!(RejectReason::InvalidPrice as u16, 2002);
        assert_eq!(RejectReason::InvalidQty as u16, 2003);
        assert_eq!(RejectReason::DuplicateOrderId as u16, 2008);
        assert_eq!(RejectReason::ArithmeticOverflow as u16, 5001);
        assert_eq!(RejectReason::InvalidExpireTime as u16, 2013);
        assert_eq!(RejectReason::PrioritySpaceExhaustion as u16, 5006);
        assert_eq!(RejectReason::OrderNotFound as u16, 2010);
    }
}
