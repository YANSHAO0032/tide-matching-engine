/// 业务拒绝原因。
///
/// 判别值是固定的协议编号，不应随意修改。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// 当前执行入口不支持该订单类型、有效期或标志组合。
    InvalidOrderCombination = 2001,
    /// 价格原始整数不是该字段要求的严格正值。
    InvalidPrice = 2002,
    /// 数量或初始 quote 预算不是该字段要求的严格正值。
    InvalidQty = 2003,
    /// 价格不符合市场 tick_size 的对齐要求。
    InvalidTick = 2004,
    /// 数量不符合市场 lot_size 的对齐要求。
    InvalidLot = 2005,
    /// 同一订单簿内已存在相同的订单身份。
    DuplicateOrderId = 2008,
    /// 请求引用的订单不在当前订单簿中。
    OrderNotFound = 2010,
    /// 到期时间不满足命令的逻辑时间约束。
    InvalidExpireTime = 2013,
    /// PostOnly 候选会立即作为 taker 成交，必须在改簿前拒绝。
    PostOnlyWouldCross = 2015,
    /// 限价和数量约束下的可执行流动性不足以完整成交 FOK。
    FokInsufficientLiquidity = 2016,
    /// 订单需要有效保护边界，但用户和市场均未提供可用边界。
    MarketProtectionRequired = 3005,
    /// 下一笔候选 maker 成交将越过有效保护边界。
    MarketProtectionExceeded = 3006,
    /// 市价滑点参数不在 0..=9_999 的稳定合法范围内。
    InvalidSlippageBps = 3007,
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
        assert_eq!(RejectReason::InvalidOrderCombination as u16, 2001);
        assert_eq!(RejectReason::FokInsufficientLiquidity as u16, 2016);
        assert_eq!(RejectReason::InvalidPrice as u16, 2002);
        assert_eq!(RejectReason::InvalidQty as u16, 2003);
        assert_eq!(RejectReason::InvalidTick as u16, 2004);
        assert_eq!(RejectReason::InvalidLot as u16, 2005);
        assert_eq!(RejectReason::DuplicateOrderId as u16, 2008);
        assert_eq!(RejectReason::ArithmeticOverflow as u16, 5001);
        assert_eq!(RejectReason::InvalidExpireTime as u16, 2013);
        assert_eq!(RejectReason::PostOnlyWouldCross as u16, 2015);
        assert_eq!(RejectReason::MarketProtectionRequired as u16, 3005);
        assert_eq!(RejectReason::MarketProtectionExceeded as u16, 3006);
        assert_eq!(RejectReason::InvalidSlippageBps as u16, 3007);
        assert_eq!(RejectReason::PrioritySpaceExhaustion as u16, 5006);
        assert_eq!(RejectReason::OrderNotFound as u16, 2010);
    }
}
