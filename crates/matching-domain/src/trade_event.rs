use crate::{OrderId, Price, Qty, Side, TradeId};

/// 最小成交事件值对象。
///
/// 它不含费用、结算、持久化序列或公开 feed 元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEvent {
    /// 此笔实际成交的唯一身份。
    pub trade_id: TradeId,
    /// 提供 resting 流动性的 maker 订单身份。
    pub maker_order_id: OrderId,
    /// 主动消耗流动性的 taker 订单身份。
    pub taker_order_id: OrderId,
    /// 成交价格；在当前参考模型中取 maker 的限价。
    pub price: Price,
    /// 实际成交的严格正数量。
    pub qty: Qty,
    /// maker 订单的买卖方向，用于确定双方角色。
    pub maker_side: Side,
}

#[cfg(test)]
mod tests {
    //! 验证成交载荷各字段均参与值语义。
    use super::*;

    /// 构造可复用的成交载荷 fixture。
    fn sample_trade() -> TradeEvent {
        TradeEvent {
            trade_id: TradeId::new(100),
            maker_order_id: OrderId::new(200),
            taker_order_id: OrderId::new(300),
            price: Price::try_new(50_000).unwrap(),
            qty: Qty::try_new(10).unwrap(),
            maker_side: Side::Buy,
        }
    }

    #[test]
    fn identical_fields_are_equal() {
        let a = sample_trade();
        let b = sample_trade();

        assert_eq!(a, b);
    }

    #[test]
    fn changing_any_field_changes_equality() {
        let original = sample_trade();

        // 分别修改六个字段，验证每个字段都参与相等比较。
        let different_trade_id = TradeEvent {
            trade_id: TradeId::new(101),
            ..sample_trade()
        };

        let different_maker_order_id = TradeEvent {
            maker_order_id: OrderId::new(201),
            ..sample_trade()
        };

        let different_taker_order_id = TradeEvent {
            taker_order_id: OrderId::new(301),
            ..sample_trade()
        };

        let different_price = TradeEvent {
            price: Price::try_new(50_001).unwrap(),
            ..sample_trade()
        };

        let different_qty = TradeEvent {
            qty: Qty::try_new(11).unwrap(),
            ..sample_trade()
        };

        let different_maker_side = TradeEvent {
            maker_side: Side::Sell,
            ..sample_trade()
        };

        for changed in [
            different_trade_id,
            different_maker_order_id,
            different_taker_order_id,
            different_price,
            different_qty,
            different_maker_side,
        ] {
            assert_ne!(original, changed);
        }
    }

    #[test]
    fn supports_clone() {
        let original = sample_trade();
        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    #[test]
    fn supports_debug() {
        let trade = sample_trade();
        let debug = format!("{trade:?}");

        assert!(debug.contains("TradeEvent"));
        assert!(debug.contains("TradeId(100)"));
        assert!(debug.contains("OrderId(200)"));
        assert!(debug.contains("OrderId(300)"));
        assert!(debug.contains("Price(50000)"));
        assert!(debug.contains("Qty(10)"));
        assert!(debug.contains("Buy"));
    }
}
