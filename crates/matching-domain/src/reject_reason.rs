/// 业务拒绝原因。
///
/// 判别值是固定的协议编号，不应随意修改。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// 价格不是该字段所要求的严格正 ticks。
    InvalidPrice = 2002,
    /// 数量不是该字段所要求的严格正 lots。
    InvalidQty = 2003,
    /// 同一订单簿内已存在相同的订单身份。
    DuplicateOrderId = 2008,
    /// 请求引用的订单不在当前订单簿中。
    OrderNotFound = 2010,
    /// 到期时间不满足命令的逻辑时间约束。
    InvalidExpireTime = 2013,
    /// 所需 checked 算术无法以目标整数类型精确表示。
    ArithmeticOverflow = 5001,
    /// 市场本地 priority 已进入不可分配的保护区。
    PrioritySpaceExhaustion = 5006,
}

#[cfg(test)]
mod tests {
    //! 固定拒绝原因的 wire 编号，防止协议兼容性回退。
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
