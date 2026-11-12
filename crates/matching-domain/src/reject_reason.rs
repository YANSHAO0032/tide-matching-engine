/// 业务拒绝原因。
///
/// 判别值是固定的协议编号，不应随意修改。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// 查询或命令指定的市场不存在于当前执行单元。
    MarketNotFound = 1001,
    /// 当前市场未开放所请求的业务操作。
    MarketNotOpen = 1002,
    /// 市场因风控或熔断停止新增及改单。
    MarketHalted = 1003,
    /// 市场因人工暂停停止新增及改单。
    MarketSuspended = 1004,
    /// 市场已关闭，不能接受该业务操作。
    MarketClosed = 1005,
    /// 开市前的限价订单将穿越对手最优价。
    PreOpenWouldCross = 1006,
    /// 市场正在迁移，入口暂不接收该市场的变更。
    MigrationInProgress = 1008,
    /// 请求携带的路由版本或分片归属已经失效。
    StaleRouteEpoch = 1009,
    /// 管理命令不符合当前市场状态转换规则。
    InvalidMarketStateTransition = 1010,
    /// 市场配置的价格步长无效。
    MarketConfigInvalidTick = 1011,
    /// 市场配置的数量步长无效。
    MarketConfigInvalidLot = 1012,
    /// 市场配置的精度因子不可表示。
    MarketConfigUnsupportedScale = 1013,
    /// 最小价格和数量步长的本金不是报价资产的整数原子。
    MarketConfigNonIntegralQuoteUnit = 1014,
    /// 计算市场配置的最小本金时发生溢出。
    MarketConfigArithmeticOverflow = 1015,
    /// 当前执行入口不支持该订单类型、有效期或标志组合。
    InvalidOrderCombination = 2001,
    /// 价格原始整数不是该字段要求的严格正值。
    InvalidPrice = 2002,
    /// 数量或初始报价资产预算不是该字段要求的严格正值。
    InvalidQty = 2003,
    /// 价格不符合市场 tick_size 的对齐要求。
    InvalidTick = 2004,
    /// 数量不符合市场 lot_size 的对齐要求。
    InvalidLot = 2005,
    /// 订单本金低于市场最小值。
    MinNotional = 2006,
    /// 订单本金超过市场最大值。
    MaxNotional = 2007,
    /// 同一订单簿内已存在相同的订单身份。
    DuplicateOrderId = 2008,
    /// 用户的客户端订单身份仍被活动订单或终结保留窗口占用。
    DuplicateClientOrderId = 2009,
    /// 请求引用的订单不在当前订单簿中。
    OrderNotFound = 2010,
    /// 冰山显示上限不满足严格小于订单总量的关系。
    InvalidIceberg = 2011,
    /// 到期时间不满足命令的逻辑时间约束。
    InvalidExpireTime = 2013,
    /// DAY 订单或会话终结命令引用的交易时段不是当前时段。
    InvalidSession = 2014,
    /// PostOnly 候选会立即作为吃单方成交，必须在改簿前拒绝。
    PostOnlyWouldCross = 2015,
    /// 限价和数量约束下的可执行流动性不足以完整成交 FOK。
    FokInsufficientLiquidity = 2016,
    /// 限价订单的价格超出市场价格带。
    PriceBandExceeded = 3001,
    /// 市场价格带引用的价格当前不可用。
    ReferencePriceUnavailable = 3002,
    /// 订单需要有效保护边界，但用户和市场均未提供可用边界。
    MarketProtectionRequired = 3005,
    /// 下一笔候选挂单成交将越过有效保护边界。
    MarketProtectionExceeded = 3006,
    /// 用户滑点或市场价格带偏离基点不在 0..=9_999 的稳定合法范围内。
    InvalidSlippageBps = 3007,
    /// 静态开市参考或不允许的来源被用来驱动止损触发。
    InvalidReferencePriceSource = 3003,
    /// 同一价格来源的更新序号没有严格递增。
    StaleReferencePriceUpdate = 3004,
    /// 已接受订单缺少有效或足额的本地预留。
    InsufficientReservation = 4001,
    /// 已接受订单引用的预留状态无效。
    InvalidReservation = 4002,
    /// 已接受订单固定引用的费率版本不存在。
    FeeScheduleNotFound = 4003,
    /// 已接受订单固定引用的费率版本或层级不再匹配。
    FeeScheduleMismatch = 4004,
    /// 用户控制版本低于分片当前接收下限。
    StaleUserControlEpoch = 4005,
    /// 所需带溢出检查的算术无法以目标整数类型精确表示。
    ArithmeticOverflow = 5001,
    /// 有界入口容量已满，当前请求未进入业务执行。
    EngineBusy = 5002,
    /// 主节点纪元与控制命令不一致。
    StalePrimaryEpoch = 5005,
    /// 市场本地优先级已进入不可分配的保护区。
    PrioritySpaceExhaustion = 5006,
}

#[cfg(test)]
mod tests {
    //! 固定拒绝原因的线协议编号，防止协议兼容性回退。
    use crate::reject_reason::RejectReason;

    #[test]
    fn reason_code_golden_tests() {
        assert_eq!(RejectReason::MarketNotFound as u16, 1001);
        assert_eq!(RejectReason::InvalidOrderCombination as u16, 2001);
        assert_eq!(RejectReason::FokInsufficientLiquidity as u16, 2016);
        assert_eq!(RejectReason::InvalidPrice as u16, 2002);
        assert_eq!(RejectReason::InvalidQty as u16, 2003);
        assert_eq!(RejectReason::InvalidTick as u16, 2004);
        assert_eq!(RejectReason::InvalidLot as u16, 2005);
        assert_eq!(RejectReason::DuplicateOrderId as u16, 2008);
        assert_eq!(RejectReason::DuplicateClientOrderId as u16, 2009);
        assert_eq!(RejectReason::StaleUserControlEpoch as u16, 4005);
        assert_eq!(RejectReason::ArithmeticOverflow as u16, 5001);
        assert_eq!(RejectReason::StalePrimaryEpoch as u16, 5005);
        assert_eq!(RejectReason::InvalidExpireTime as u16, 2013);
        assert_eq!(RejectReason::InvalidSession as u16, 2014);
        assert_eq!(RejectReason::PostOnlyWouldCross as u16, 2015);
        assert_eq!(RejectReason::MarketProtectionRequired as u16, 3005);
        assert_eq!(RejectReason::MarketProtectionExceeded as u16, 3006);
        assert_eq!(RejectReason::InvalidSlippageBps as u16, 3007);
        assert_eq!(RejectReason::InvalidReferencePriceSource as u16, 3003);
        assert_eq!(RejectReason::StaleReferencePriceUpdate as u16, 3004);
        assert_eq!(RejectReason::InsufficientReservation as u16, 4001);
        assert_eq!(RejectReason::InvalidReservation as u16, 4002);
        assert_eq!(RejectReason::FeeScheduleNotFound as u16, 4003);
        assert_eq!(RejectReason::FeeScheduleMismatch as u16, 4004);
        assert_eq!(RejectReason::PrioritySpaceExhaustion as u16, 5006);
        assert_eq!(RejectReason::OrderNotFound as u16, 2010);
    }
}
