use crate::{OrderId, Qty, RejectReason, TradeEvent};

/// 撮合引擎核心生命周期事件
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EngineEvent {
    // 命令被拒绝
    CommandRejected { reason: RejectReason },
    // 订单已接受
    OrderAccepted { order_id: OrderId },
    // 订单已进入订单簿
    OrderOpened { order_id: OrderId },
    // 订单部分成交
    OrderPartiallyFilled { order_id: OrderId, remaining: Qty },
    // 订单完全成交
    OrderFilled { order_id: OrderId },
    // 成交事件
    Trade(TradeEvent),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_rejected_value_semantics() {
        let a = EngineEvent::CommandRejected {
            reason: RejectReason::InvalidPrice,
        };
        let b = EngineEvent::CommandRejected {
            reason: RejectReason::InvalidPrice,
        };
        let c = EngineEvent::CommandRejected {
            reason: RejectReason::InvalidQty,
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn order_lifecycle_events_are_distinct() {
        let order_id = OrderId::new(42);

        let accepted = EngineEvent::OrderAccepted { order_id };
        let opened = EngineEvent::OrderOpened { order_id };
        let partially_filled = EngineEvent::OrderPartiallyFilled {
            order_id,
            remaining: Qty::try_new(1).unwrap(),
        };
        let filled = EngineEvent::OrderFilled { order_id };

        assert_ne!(accepted, opened);
        assert_ne!(accepted, partially_filled);
        assert_ne!(accepted, filled);
        assert_ne!(opened, partially_filled);
        assert_ne!(opened, filled);
        assert_ne!(partially_filled, filled);
    }

    #[test]
    fn order_id_participates_in_equality() {
        assert_eq!(
            EngineEvent::OrderAccepted {
                order_id: OrderId::new(1),
            },
            EngineEvent::OrderAccepted {
                order_id: OrderId::new(1),
            }
        );

        assert_ne!(
            EngineEvent::OrderAccepted {
                order_id: OrderId::new(1),
            },
            EngineEvent::OrderAccepted {
                order_id: OrderId::new(2),
            }
        );
    }

    #[test]
    fn partial_fill_payload_participates_in_equality() {
        let a = EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(1),
            remaining: Qty::try_new(10).unwrap(),
        };
        let same = EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(1),
            remaining: Qty::try_new(10).unwrap(),
        };
        let different_order = EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(2),
            remaining: Qty::try_new(10).unwrap(),
        };
        let different_qty = EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(1),
            remaining: Qty::try_new(11).unwrap(),
        };

        assert_eq!(a, same);
        assert_ne!(a, different_order);
        assert_ne!(a, different_qty);
    }

    #[test]
    fn supports_debug() {
        let event = EngineEvent::OrderAccepted {
            order_id: OrderId::new(42),
        };

        assert_eq!(
            format!("{event:?}"),
            "OrderAccepted { order_id: OrderId(42) }"
        );
    }

    fn sample_trade() -> TradeEvent {
        TradeEvent {
            trade_id: crate::TradeId::new(100),
            maker_order_id: OrderId::new(200),
            taker_order_id: OrderId::new(300),
            price: crate::Price::try_new(50_000).unwrap(),
            qty: Qty::try_new(10).unwrap(),
            maker_side: crate::Side::Buy,
        }
    }

    #[test]
    fn trade_events_with_identical_payloads_are_equal() {
        let a = EngineEvent::Trade(sample_trade());
        let b = EngineEvent::Trade(sample_trade());

        assert_eq!(a, b);
    }

    #[test]
    fn trade_events_with_different_payloads_are_not_equal() {
        let original = EngineEvent::Trade(sample_trade());

        let different_trade_id = EngineEvent::Trade(TradeEvent {
            trade_id: crate::TradeId::new(101),
            ..sample_trade()
        });

        let different_qty = EngineEvent::Trade(TradeEvent {
            qty: Qty::try_new(11).unwrap(),
            ..sample_trade()
        });

        let different_side = EngineEvent::Trade(TradeEvent {
            maker_side: crate::Side::Sell,
            ..sample_trade()
        });

        assert_ne!(original, different_trade_id);
        assert_ne!(original, different_qty);
        assert_ne!(original, different_side);
    }

    #[test]
    fn trade_is_distinct_from_lifecycle_events() {
        let trade = EngineEvent::Trade(sample_trade());
        let order_id = OrderId::new(200);

        let lifecycle_events = [
            EngineEvent::CommandRejected {
                reason: RejectReason::InvalidPrice,
            },
            EngineEvent::OrderAccepted { order_id },
            EngineEvent::OrderOpened { order_id },
            EngineEvent::OrderPartiallyFilled {
                order_id,
                remaining: Qty::try_new(10).unwrap(),
            },
            EngineEvent::OrderFilled { order_id },
        ];

        for event in lifecycle_events {
            assert_ne!(trade, event);
        }
    }

    #[test]
    fn trade_event_supports_clone_and_debug() {
        let original = EngineEvent::Trade(sample_trade());
        let cloned = original.clone();

        assert_eq!(original, cloned);

        let debug = format!("{original:?}");
        assert!(debug.contains("Trade"));
        assert!(debug.contains("TradeId(100)"));
    }
}
