//! 改单游标与旧身份切片的双模型有序对照。
use matching_domain::{
    AmendOrder, ClusterId, IcebergOrder, LimitOrder, LogicalTime, MarketId, MarketMath, OrderId,
    OriginSessionId, Price, Qty, RejectReason, ReservationId, ShardId, Side, StpKey, StpMode,
    TradeIdCursor, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy, ReferenceOrderBook};
use matching_risk::{ReservationState, ReservationStatus};

fn order(id: u128, user: u64, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(user),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}
fn reservation(o: &LimitOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId(o.order_id.0),
        order_id: o.order_id,
        user_id: o.user_id,
        market_id: MarketId(7),
        origin_session_id: OriginSessionId(1),
        version: 1,
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
        reserves: vec![],
    }
}
fn amend(o: &LimitOrder) -> AmendOrder {
    AmendOrder {
        market_id: MarketId(7),
        user_id: o.user_id,
        order_id: o.order_id,
        new_price: Some(Price::try_new(100).unwrap()),
        new_qty: None,
        new_quote_budget: None,
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }
}
#[test]
fn amend_multifill_iceberg_and_stp_match_legacy_and_rollback_on_overflow() {
    for iceberg in [false, true] {
        for stp in [false, true] {
            let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
            let mut r = ReferenceOrderBook::new();
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            let mut target = order(1, 10, Side::Buy, 90, 2);
            if stp {
                target.stp_mode = StpMode::CancelMaker;
            }
            if iceberg {
                let ice = IcebergOrder::try_new(target.clone(), Qty::try_new(1).unwrap()).unwrap();
                p.place_iceberg_post_only_with_reservation(
                    ice.clone(),
                    &math,
                    reservation(&target),
                )
                .unwrap();
                r.place_iceberg_post_only_with_reservation(ice, &math, reservation(&target))
                    .unwrap();
            } else {
                p.rest_with_reservation(target.clone(), reservation(&target))
                    .unwrap();
                r.rest_with_reservation(target.clone(), reservation(&target))
                    .unwrap();
            }
            if stp {
                let maker = order(2, 10, Side::Sell, 100, 1);
                p.rest(maker.clone()).unwrap();
                r.rest(maker).unwrap();
            }
            for maker in [
                order(3, 20, Side::Sell, 100, 1),
                order(4, 30, Side::Sell, 100, 1),
            ] {
                p.rest(maker.clone()).unwrap();
                r.rest(maker).unwrap();
            }
            let change = amend(&target);
            let before_p = p.clone();
            let before_r = r.clone();
            let mut pc =
                TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 3, u32::MAX as u64)
                    .unwrap();
            let mut rc = pc.clone();
            let original = pc.clone();
            assert_eq!(
                p.amend_regular_with_cursor(&change, LogicalTime::new(1), 9, &mut pc),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(
                r.amend_regular_with_cursor(&change, LogicalTime::new(1), 9, &mut rc),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(p, before_p);
            assert_eq!(r, before_r);
            assert_eq!(pc, original);
            assert_eq!(rc, original);
            let mut pc = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
            let mut rc = pc.clone();
            let mut generator = pc.clone();
            let ids = [
                generator.next_trade_id().unwrap(),
                generator.next_trade_id().unwrap(),
            ];
            let mut old_p = p.clone();
            let mut old_r = r.clone();
            let pe = p
                .amend_regular_with_cursor(&change, LogicalTime::new(1), 9, &mut pc)
                .unwrap();
            let re = r
                .amend_regular_with_cursor(&change, LogicalTime::new(1), 9, &mut rc)
                .unwrap();
            assert_eq!(
                pe.iter()
                    .filter(|event| matches!(event, matching_domain::EngineEvent::Trade(_)))
                    .count(),
                2
            );
            assert_eq!(
                pe.iter()
                    .filter(|event| matches!(
                        event,
                        matching_domain::EngineEvent::OrderCanceled {
                            reason: matching_domain::CancelReason::SelfTradePreventionMaker,
                            ..
                        }
                    ))
                    .count(),
                usize::from(stp)
            );
            assert_eq!(pe, re);
            assert_eq!(
                pe,
                old_p
                    .amend_regular_with_events(&change, LogicalTime::new(1), 9, &ids)
                    .unwrap()
            );
            assert_eq!(
                re,
                old_r
                    .amend_regular_with_events(&change, LogicalTime::new(1), 9, &ids)
                    .unwrap()
            );
            assert_eq!(p, old_p);
            assert_eq!(r, old_r);
            assert_eq!(pc, generator);
            assert_eq!(rc, generator);
        }
    }
}
