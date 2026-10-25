use crate::{
    OrderId, Price, Qty, QuoteBudget, ReservationId, Side, TimeInForce, TriggerExpiry, UserId,
};

/// 止损订单触发后将形成的 child 类型。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggeredOrderKind {
    /// 限价 child。
    Limit = 1,
    /// 按基础资产数量执行的市价 child。
    MarketByBase = 2,
    /// 按报价资产预算执行的市价 child。
    MarketByQuote = 3,
}

/// StopMarket 触发后 child 使用的数量计价模式。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopMarketQuantity {
    /// child 按严格正基础资产数量立即执行。
    Base(Qty),
    /// child 按严格正报价资产预算立即执行。
    Quote(QuoteBudget),
}

/// 未触发的 StopLimit 订单载荷。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopLimitOrder {
    /// 决定本单触发集合的持久价格来源。
    pub trigger_reference: crate::PriceReferenceKind,
    /// 订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
    /// 接收阶段锁定、触发拒绝时必须释放的预留身份。
    pub reservation_id: ReservationId,
    /// 接收阶段固定的费率版本。
    pub fee_schedule_version: u64,
    /// 买入或卖出方向。
    pub side: Side,
    /// 决定进入可触发集合的严格正触发价格。
    pub stop_price: Price,
    /// 触发后 child 的严格正限价。
    pub price: Price,
    /// 触发后 child 的严格正基础资产数量。
    pub qty: Qty,
    /// 只描述触发后 child 的有效期策略。
    pub child_tif: TimeInForce,
    /// 只描述未触发 Stop 的存活期。
    pub trigger_expiry: TriggerExpiry,
}

/// 未触发的 StopMarket 订单载荷。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopMarketOrder {
    /// 决定本单触发集合的持久价格来源。
    pub trigger_reference: crate::PriceReferenceKind,
    /// 触发时以该来源价格冻结并执行的市价保护。
    pub market_protection: Option<crate::MarketProtection>,
    /// 订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
    /// 接收阶段锁定、触发拒绝时必须释放的预留身份。
    pub reservation_id: ReservationId,
    /// 接收阶段固定的费率版本。
    pub fee_schedule_version: u64,
    /// 买入或卖出方向。
    pub side: Side,
    /// 决定进入可触发集合的严格正触发价格。
    pub stop_price: Price,
    /// 触发后 child 的基础数量或报价预算模式。
    pub quantity: StopMarketQuantity,
    /// 只描述触发后 child 的有效期策略。
    pub child_tif: TimeInForce,
    /// 只描述未触发 Stop 的存活期。
    pub trigger_expiry: TriggerExpiry,
}

/// 尚未触发的 Stop 订单的统一领域表示。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopOrder {
    /// 触发后形成限价 child。
    Limit(StopLimitOrder),
    /// 触发后形成市价 child。
    Market(StopMarketOrder),
}

impl StopOrder {
    /// 返回本单所属的触发来源。
    pub fn trigger_reference(&self) -> crate::PriceReferenceKind {
        match self {
            Self::Limit(order) => order.trigger_reference,
            Self::Market(order) => order.trigger_reference,
        }
    }

    /// 返回市价 child 的实际保护模板，限价 child 没有市价保护。
    pub fn market_protection(&self) -> Option<&crate::MarketProtection> {
        match self {
            Self::Limit(_) => None,
            Self::Market(order) => order.market_protection.as_ref(),
        }
    }
    /// 返回不依赖容器位置的订单标识符。
    pub fn order_id(&self) -> OrderId {
        match self {
            Self::Limit(order) => order.order_id,
            Self::Market(order) => order.order_id,
        }
    }

    /// 返回决定 StopLevel 的方向。
    pub fn side(&self) -> Side {
        match self {
            Self::Limit(order) => order.side,
            Self::Market(order) => order.side,
        }
    }

    /// 返回决定 StopLevel 的触发价格。
    pub fn stop_price(&self) -> Price {
        match self {
            Self::Limit(order) => order.stop_price,
            Self::Market(order) => order.stop_price,
        }
    }

    /// 返回未触发生命期。
    pub fn trigger_expiry(&self) -> TriggerExpiry {
        match self {
            Self::Limit(order) => order.trigger_expiry,
            Self::Market(order) => order.trigger_expiry,
        }
    }

    /// 返回触发后 child 的稳定类型标签。
    pub fn child_kind(&self) -> TriggeredOrderKind {
        match self {
            Self::Limit(_) => TriggeredOrderKind::Limit,
            Self::Market(order) => match order.quantity {
                StopMarketQuantity::Base(_) => TriggeredOrderKind::MarketByBase,
                StopMarketQuantity::Quote(_) => TriggeredOrderKind::MarketByQuote,
            },
        }
    }

    /// 返回接收阶段锁定的预留身份。
    pub fn reservation_id(&self) -> ReservationId {
        match self {
            Self::Limit(order) => order.reservation_id,
            Self::Market(order) => order.reservation_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LogicalTime, OrderId, Price, QuoteBudget, ReservationId, Side, TimeInForce, UserId,
    };

    #[test]
    fn stop_order_exposes_location_fields_without_child_lifetime_confusion() {
        let order = StopOrder::Market(StopMarketOrder {
            trigger_reference: crate::PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: OrderId::new(7),
            user_id: UserId::new(8),
            reservation_id: ReservationId::new(9),
            fee_schedule_version: 2,
            side: Side::Buy,
            stop_price: Price::try_new(100).unwrap(),
            quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(9).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(10),
            },
        });
        assert_eq!(order.order_id(), OrderId::new(7));
        assert_eq!(order.side(), Side::Buy);
        assert_eq!(order.reservation_id(), ReservationId::new(9));
        assert_eq!(order.child_kind(), TriggeredOrderKind::MarketByQuote);
        assert_eq!(order.stop_price(), Price::try_new(100).unwrap());
        assert_eq!(
            order.trigger_expiry(),
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(10)
            }
        );
    }

    #[test]
    fn triggered_order_kind_codes_are_stable() {
        assert_eq!(TriggeredOrderKind::Limit as u8, 1);
        assert_eq!(TriggeredOrderKind::MarketByBase as u8, 2);
        assert_eq!(TriggeredOrderKind::MarketByQuote as u8, 3);
    }
}
