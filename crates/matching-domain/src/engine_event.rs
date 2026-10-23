use crate::{CancelReason, OrderId, Price, ProtectionSource, Qty, RejectReason, TradeEvent};

/// 当前订单簿使用的最小订单生命周期事件载荷。
///
/// 它不是完整持久化事件封装：不携带市场事件序列、WAL 位置或发布元数据。
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EngineEvent {
    /// 命令在状态变更前被拒绝，并携带稳定的原因码。
    CommandRejected {
        /// 拒绝命令的稳定原因码。
        reason: RejectReason,
    },
    /// 订单已通过接收阶段。
    OrderAccepted {
        /// 已被接收的订单标识符。
        order_id: OrderId,
    },
    /// 订单已进入订单簿并成为挂单流动性。
    OrderOpened {
        /// 已成为挂单流动性的订单标识符。
        order_id: OrderId,
    },
    /// 订单成交后仍有严格正的总剩余量；冰山包含可见量 + 隐藏量。
    OrderPartiallyFilled {
        /// 发生部分成交的订单标识符。
        order_id: OrderId,
        /// 成交后仍严格为正的总基础资产余量。
        remaining: Qty,
    },
    /// 订单因成交完毕而结束。
    OrderFilled {
        /// 已全部成交的订单标识符。
        order_id: OrderId,
    },
    /// 未成交的严格正基础资产余量按指定原因终结；不表示取消了一张挂单订单。
    OrderCanceled {
        /// 被终结订单的标识符。
        order_id: OrderId,
        /// 被取消的严格正基础资产余量。
        remaining: Qty,
        /// 此次终结的稳定业务原因。
        reason: CancelReason,
    },
    /// 一笔已提交的成交及其挂单/吃单方载荷。
    Trade(TradeEvent),
    /// 冰山可见片耗尽后补量并取得新队列优先级；`visible` 是本次显示量。
    IcebergRefreshed {
        /// 发生补量的冰山订单标识符。
        order_id: OrderId,
        /// 本次重新展示的严格正可见数量。
        visible: Qty,
    },
    /// 下一候选挂单越过有效保护边界，市价订单据此终结。
    MarketProtectionTriggered {
        /// 因保护边界终结的市价订单标识符。
        order_id: OrderId,
        /// 首个不可继续成交的候选挂单价格。
        next_price: Price,
        /// 合并用户和市场约束后的最差可成交价格。
        effective_worst_price: Price,
        /// 形成有效保护边界的来源。
        source: ProtectionSource,
    },
}

#[cfg(test)]
mod tests {
    //! 验证生命周期事件及其载荷的值语义。
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

    /// 构造可复用的成交事件测试数据。
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
    #[test]
    fn market_protection_triggered_preserves_four_field_payload() {
        for (source, next, worst) in [
            (ProtectionSource::User, 110, 100),
            (ProtectionSource::MarketBand, 90, 100),
            (ProtectionSource::Both, 120, 110),
        ] {
            let order_id = OrderId::new(100);
            let next_price = Price::try_new(next).unwrap();
            let effective_worst_price = Price::try_new(worst).unwrap();

            let event = EngineEvent::MarketProtectionTriggered {
                order_id,
                next_price,
                effective_worst_price,
                source,
            };

            // 保持 EngineEvent 既有的值语义。
            assert_eq!(event.clone(), event);

            let debug = format!("{event:?}");
            assert!(debug.contains("MarketProtectionTriggered"));

            // 直接验证四个字段原样保留。
            let EngineEvent::MarketProtectionTriggered {
                order_id: actual_order_id,
                next_price: actual_next_price,
                effective_worst_price: actual_worst_price,
                source: actual_source,
            } = event
            else {
                panic!("expected MarketProtectionTriggered");
            };

            assert_eq!(actual_order_id, order_id);
            assert_eq!(actual_next_price, next_price);
            assert_eq!(actual_worst_price, effective_worst_price);
            assert_eq!(actual_source, source);
        }
    }
    #[test]
    fn market_protection_triggered_has_stable_value_semantics() {
        use crate::{ProtectionSource, Qty, Side, TradeEvent, TradeId};

        // 固定原始事件，作为所有比较的基准。
        let original = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(100),
            next_price: Price::try_new(110).unwrap(),
            effective_worst_price: Price::try_new(100).unwrap(),
            source: ProtectionSource::User,
        };

        // 1. 独立构造相同四字段，必须相等。
        let same = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(100),
            next_price: Price::try_new(110).unwrap(),
            effective_worst_price: Price::try_new(100).unwrap(),
            source: ProtectionSource::User,
        };

        assert_eq!(original, same);

        // 2. Clone 保留完整值语义。
        assert_eq!(original.clone(), original);

        // 3. Debug 可用，包含变体与字段信息。
        let debug = format!("{original:?}");
        assert!(debug.contains("MarketProtectionTriggered"));
        assert!(debug.contains("order_id"));
        assert!(debug.contains("next_price"));
        assert!(debug.contains("effective_worst_price"));
        assert!(debug.contains("User"));

        // 4. 仅改变 order_id。
        let changed_order_id = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(101),
            next_price: Price::try_new(110).unwrap(),
            effective_worst_price: Price::try_new(100).unwrap(),
            source: ProtectionSource::User,
        };
        assert_ne!(original, changed_order_id);

        // 5. 仅改变 next_price。
        let changed_next_price = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(100),
            next_price: Price::try_new(120).unwrap(),
            effective_worst_price: Price::try_new(100).unwrap(),
            source: ProtectionSource::User,
        };
        assert_ne!(original, changed_next_price);

        // 6. 仅改变 effective_worst_price。
        let changed_effective_worst_price = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(100),
            next_price: Price::try_new(110).unwrap(),
            effective_worst_price: Price::try_new(105).unwrap(),
            source: ProtectionSource::User,
        };
        assert_ne!(original, changed_effective_worst_price);

        // 7. 仅改变来源。
        let changed_source = EngineEvent::MarketProtectionTriggered {
            order_id: OrderId::new(100),
            next_price: Price::try_new(110).unwrap(),
            effective_worst_price: Price::try_new(100).unwrap(),
            source: ProtectionSource::MarketBand,
        };
        assert_ne!(original, changed_source);

        // 8. 与 CommandRejected 必须不相等。
        let rejected = EngineEvent::CommandRejected {
            reason: RejectReason::InvalidPrice,
        };
        assert_ne!(original, rejected);

        // 9. 与 Trade 必须不相等。
        let trade = EngineEvent::Trade(TradeEvent {
            trade_id: TradeId::new(1),
            maker_order_id: OrderId::new(200),
            taker_order_id: OrderId::new(100),
            price: Price::try_new(110).unwrap(),
            qty: Qty::try_new(1).unwrap(),
            maker_side: Side::Sell,
        });
        assert_ne!(original, trade);

        // 10. 与既有生命周期事件必须不相等。
        let accepted = EngineEvent::OrderAccepted {
            order_id: OrderId::new(100),
        };
        assert_ne!(original, accepted);
    }
}
