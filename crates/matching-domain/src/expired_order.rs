use crate::{ExpireReason, OrderId};

/// 最小的订单到期业务结果。
///
/// 它不代表持久化事件封装，后续协议层再将其映射到完整事件载荷。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiredOrder {
    /// 已从活动状态删除的订单标识符。
    pub order_id: OrderId,
    /// 本次到期的稳定业务原因。
    pub reason: ExpireReason,
}
