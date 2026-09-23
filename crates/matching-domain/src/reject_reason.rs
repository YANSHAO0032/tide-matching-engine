/// 业务拒绝原因。
///
/// 判别值是固定的协议编号，不应随意修改
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    InvalidPrice = 2002,
    InvalidQty = 2003,
    ArithmeticOverflow = 5001,
    InvalidExpireTime = 2013,
}

#[cfg(test)]
mod tests {
    use crate::reject_reason::RejectReason;

    #[test]
    fn reason_code_golden_tests() {
        assert_eq!(RejectReason::InvalidPrice as u16, 2002);
        assert_eq!(RejectReason::InvalidQty as u16, 2003);
        assert_eq!(RejectReason::ArithmeticOverflow as u16, 5001);
        assert_eq!(RejectReason::InvalidExpireTime as u16, 2013);
    }
}
