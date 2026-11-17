//! Stop 级联预演使用的显式逻辑时间与市场输入。

use matching_domain::{LogicalTime, MarketMath, TradeId};

/// 触发预演的调用方输入，不读取系统时钟或访问外部资金服务。
pub struct TriggerRegistryContext<'a> {
    /// 当前逻辑时间。
    pub now: LogicalTime,
    /// 当前交易时段。
    pub current_session_id: u64,
    /// 当前市场的数值规则。
    pub math: &'a MarketMath,
    /// 切片入口由调用方提供的预分配且全局唯一成交 ID；游标入口不读取此字段。
    pub trade_ids: &'a [TradeId],
}
