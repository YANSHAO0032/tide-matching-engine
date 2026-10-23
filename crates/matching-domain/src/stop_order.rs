use crate::{OrderId, Price, Qty, QuoteBudget, Side, TimeInForce, TriggerExpiry, UserId};

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
    /// 订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
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
    /// 订单唯一标识符。
    pub order_id: OrderId,
    /// 订单所属用户标识符。
    pub user_id: UserId,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LogicalTime, OrderId, Price, QuoteBudget, Side, TimeInForce, UserId};

    #[test]
    fn stop_order_exposes_location_fields_without_child_lifetime_confusion() {
        let order = StopOrder::Market(StopMarketOrder {
            order_id: OrderId::new(7),
            user_id: UserId::new(8),
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
        assert_eq!(order.stop_price(), Price::try_new(100).unwrap());
        assert_eq!(
            order.trigger_expiry(),
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(10)
            }
        );
    }
}
