//! 根命令游标与历史身份切片在双订单簿中的有序事件对照。

use matching_domain::{
    ClusterId, EngineEvent, LimitOrder, OrderId, Price, Qty, ShardId, Side, StpKey, StpMode,
    TradeIdCursor, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy, ReferenceOrderBook};

fn order(id: u128, user: u64, side: Side) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    }
}

#[test]
fn stp_cancellation_does_not_consume_trade_id_and_events_match_both_models_and_legacy() {
    let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut r = ReferenceOrderBook::new();
    for maker in [order(1, 10, Side::Sell), order(2, 20, Side::Sell)] {
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
    }
    let mut old_p = p.clone();
    let mut old_r = r.clone();
    let mut taker = order(3, 10, Side::Buy);
    taker.stp_mode = StpMode::CancelMaker;
    let mut pc = TradeIdCursor::new(ClusterId::new(7), ShardId::new(9), 12);
    let mut rc = pc.clone();
    let mut id_source = pc.clone();
    let expected_id = id_source.next_trade_id().unwrap();
    let prod = p
        .place_limit_gtc_with_cursor(taker.clone(), &mut pc)
        .unwrap();
    let reference = r
        .place_limit_gtc_with_cursor(taker.clone(), &mut rc)
        .unwrap();
    let legacy_prod = old_p
        .place_limit_gtc_with_events(taker.clone(), &[expected_id])
        .unwrap();
    let legacy_ref = old_r
        .place_limit_gtc_with_events(taker, &[expected_id])
        .unwrap();
    assert_eq!(prod, reference);
    assert_eq!(prod, legacy_prod);
    assert_eq!(prod, legacy_ref);
    assert!(matches!(
        prod.first(),
        Some(EngineEvent::OrderCanceled { .. })
    ));
    let ids: Vec<_> = prod
        .iter()
        .filter_map(|e| match e {
            EngineEvent::Trade(t) => Some(t.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![expected_id]);
    assert_eq!(
        expected_id.0,
        (7u128 << 112) | (9u128 << 96) | (12u128 << 32)
    );
    assert_eq!(pc.next_index(), 1);
    assert_eq!(rc.next_index(), 1);
}

#[test]
fn iceberg_replenishments_produce_consecutive_trade_ids_in_exact_event_order() {
    use matching_domain::{IcebergOrder, MarketMath};
    let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut r = ReferenceOrderBook::new();
    let mut maker = order(11, 20, Side::Sell);
    maker.qty = Qty::try_new(3).unwrap();
    let iceberg = IcebergOrder::try_new(maker, Qty::try_new(1).unwrap()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    p.place_iceberg_post_only(iceberg.clone(), &math).unwrap();
    r.place_iceberg_post_only(iceberg, &math).unwrap();
    let mut old_p = p.clone();
    let mut old_r = r.clone();
    let mut taker = order(12, 30, Side::Buy);
    taker.qty = Qty::try_new(3).unwrap();
    let mut pc = TradeIdCursor::new(ClusterId::new(7), ShardId::new(9), 13);
    let mut rc = pc.clone();
    let mut id_source = pc.clone();
    let ids: Vec<_> = (0..3).map(|_| id_source.next_trade_id().unwrap()).collect();
    let prod = p
        .place_limit_gtc_with_cursor(taker.clone(), &mut pc)
        .unwrap();
    let reference = r
        .place_limit_gtc_with_cursor(taker.clone(), &mut rc)
        .unwrap();
    let legacy_prod = old_p
        .place_limit_gtc_with_events(taker.clone(), &ids)
        .unwrap();
    let legacy_ref = old_r.place_limit_gtc_with_events(taker, &ids).unwrap();
    assert_eq!(prod, reference);
    assert_eq!(prod, legacy_prod);
    assert_eq!(prod, legacy_ref);
    let actual: Vec<_> = prod
        .iter()
        .filter_map(|e| match e {
            EngineEvent::Trade(t) => Some(t.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(actual, ids);
    assert_eq!(pc.next_index(), 3);
    assert_eq!(rc.next_index(), 3);
}

fn stop_market(id: u128, side: Side, price: i64) -> matching_domain::StopOrder {
    use matching_domain::{
        PriceReferenceKind, ReservationId, StopMarketOrder, StopMarketQuantity, StopOrder,
        TimeInForce, TriggerExpiry,
    };
    StopOrder::Market(StopMarketOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: ReservationId::new(id + 1000),
        fee_schedule_version: 7,
        trigger_reference: PriceReferenceKind::LastTrade,
        market_protection: None,
        side,
        stop_price: Price::try_new(price).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(1).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    })
}

fn reference(seq: u64, price: i64) -> matching_domain::PriceReference {
    matching_domain::PriceReference {
        kind: matching_domain::PriceReferenceKind::LastTrade,
        source_seq: seq,
        price: Price::try_new(price).unwrap(),
    }
}

#[test]
fn stop_descendant_shares_cursor_and_overflow_preserves_complete_state() {
    use matching_domain::{LogicalTime, MarketMath, RejectReason};
    let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
    let mut r = ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for (id, user, side, price) in [(9, 90, Side::Buy, 104), (10, 100, Side::Sell, 105)] {
        let mut maker = order(id, user, side);
        maker.price = Price::try_new(price).unwrap();
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
    }
    for stop in [
        stop_market(11, Side::Buy, 110),
        stop_market(12, Side::Sell, 108),
    ] {
        p.place_stop(stop.clone(), LogicalTime::new(1), 1).unwrap();
        r.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    }
    p.trigger_stops(
        reference(1, 100),
        LogicalTime::new(1),
        1,
        &math,
        &[],
        |_| Ok(()),
    )
    .unwrap();
    r.trigger_stops(
        reference(1, 100),
        LogicalTime::new(1),
        1,
        &math,
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let before_p = p.clone();
    let before_r = r.clone();
    let mut exhausted_p =
        TradeIdCursor::with_next_index(ClusterId(7), ShardId(9), 12, u64::from(u32::MAX)).unwrap();
    let mut exhausted_r = exhausted_p.clone();
    let before_cursor = exhausted_p.clone();
    assert_eq!(
        p.trigger_stops_with_cursor(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &mut exhausted_p,
            |_| Ok(())
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        r.trigger_stops_with_cursor(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &mut exhausted_r,
            |_| Ok(())
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(p, before_p);
    assert_eq!(r, before_r);
    assert_eq!(exhausted_p, before_cursor);
    assert_eq!(exhausted_r, before_cursor);
    let mut old_p = p.clone();
    let mut old_r = r.clone();
    let mut pc = TradeIdCursor::new(ClusterId(7), ShardId(9), 12);
    let mut rc = pc.clone();
    let mut generator = pc.clone();
    let ids: Vec<_> = (0..2).map(|_| generator.next_trade_id().unwrap()).collect();
    let actual_p = p
        .trigger_stops_with_cursor(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &mut pc,
            |_| Ok(()),
        )
        .unwrap();
    let actual_r = r
        .trigger_stops_with_cursor(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &mut rc,
            |_| Ok(()),
        )
        .unwrap();
    let legacy_p = old_p
        .trigger_stops(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &ids,
            |_| Ok(()),
        )
        .unwrap();
    let legacy_r = old_r
        .trigger_stops(
            reference(2, 110),
            LogicalTime::new(1),
            1,
            &math,
            &ids,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual_p, actual_r);
    assert_eq!(actual_p, legacy_p);
    assert_eq!(actual_p, legacy_r);
    let first_stop = actual_p.iter().position(|event| matches!(event, EngineEvent::StopTriggered { order_id, .. } if *order_id == OrderId::new(11))).unwrap();
    let second_stop = actual_p.iter().position(|event| matches!(event, EngineEvent::StopTriggered { order_id, .. } if *order_id == OrderId::new(12))).unwrap();
    let first_trade = actual_p
        .iter()
        .position(|event| matches!(event, EngineEvent::Trade(_)))
        .unwrap();
    assert!(first_stop < first_trade && first_trade < second_stop);
    let actual_ids: Vec<_> = actual_p
        .iter()
        .filter_map(|e| match e {
            EngineEvent::Trade(t) => Some(t.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(actual_ids, ids);
    assert_eq!(pc.next_index(), 2);
    assert_eq!(rc.next_index(), 2);
}

#[test]
fn immediate_cursor_and_legacy_match_for_sides_fok_ioc_iceberg_and_stp() {
    use matching_domain::{IcebergOrder, MarketMath, RejectReason};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        let maker_side = if side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        for fok in [false, true] {
            for scenario in 0..4 {
                let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
                let mut r = ReferenceOrderBook::new();
                if scenario == 1 {
                    let mut maker = order(1, 20, maker_side);
                    maker.qty = Qty::try_new(2).unwrap();
                    let iceberg = IcebergOrder::try_new(maker, Qty::try_new(1).unwrap()).unwrap();
                    p.place_iceberg_post_only(iceberg.clone(), &math).unwrap();
                    r.place_iceberg_post_only(iceberg, &math).unwrap();
                } else {
                    let count = if scenario == 2 {
                        1
                    } else if scenario == 3 {
                        3
                    } else {
                        2
                    };
                    for i in 0..count {
                        let maker = order(
                            i + 1,
                            if scenario == 3 && i == 0 {
                                10
                            } else {
                                20 + i as u64
                            },
                            maker_side,
                        );
                        p.rest(maker.clone()).unwrap();
                        r.rest(maker).unwrap();
                    }
                }
                let mut old_p = p.clone();
                let mut old_r = r.clone();
                let mut taker = order(10, 10, side);
                taker.qty = Qty::try_new(2).unwrap();
                if scenario == 3 {
                    taker.stp_mode = StpMode::CancelMaker;
                }
                let mut pc = TradeIdCursor::new(ClusterId(7), ShardId(9), 12);
                let mut rc = pc.clone();
                let mut generator = pc.clone();
                let ids: Vec<_> = (0..2).map(|_| generator.next_trade_id().unwrap()).collect();
                let actual_p =
                    p.place_limit_immediate_with_cursor(taker.clone(), &math, fok, &mut pc);
                let actual_r =
                    r.place_limit_immediate_with_cursor(taker.clone(), &math, fok, &mut rc);
                let old_result_p = if fok {
                    old_p.place_limit_fok(taker.clone(), &math, &ids)
                } else {
                    old_p.place_limit_ioc(taker.clone(), &math, &ids)
                };
                let old_result_r = if fok {
                    old_r.place_limit_fok(taker, &math, &ids)
                } else {
                    old_r.place_limit_ioc(taker, &math, &ids)
                };
                assert_eq!(
                    actual_p, actual_r,
                    "side={side:?} fok={fok} scenario={scenario}"
                );
                assert_eq!(actual_p, old_result_p);
                assert_eq!(actual_p, old_result_r);
                if fok && scenario == 2 {
                    assert_eq!(actual_p, Err(RejectReason::FokInsufficientLiquidity));
                    assert_eq!(pc.next_index(), 0);
                    assert_eq!(rc.next_index(), 0);
                }
                if scenario == 3 {
                    assert!(
                        actual_p
                            .as_ref()
                            .unwrap()
                            .events()
                            .iter()
                            .any(|e| matches!(e, EngineEvent::OrderCanceled { .. }))
                    );
                }
            }
        }
    }
}

#[test]
fn immediate_cursor_exhaustion_preserves_both_books_and_cursor() {
    use matching_domain::{MarketMath, RejectReason};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for fok in [false, true] {
        for side in [Side::Buy, Side::Sell] {
            let maker_side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
            let mut r = ReferenceOrderBook::new();
            for id in 1..=2 {
                let maker = order(id, id as u64 + 20, maker_side);
                p.rest(maker.clone()).unwrap();
                r.rest(maker).unwrap();
            }
            let before_p = p.clone();
            let before_r = r.clone();
            let mut taker = order(10, 10, side);
            taker.qty = Qty::try_new(2).unwrap();
            let mut pc =
                TradeIdCursor::with_next_index(ClusterId(7), ShardId(9), 12, u64::from(u32::MAX))
                    .unwrap();
            let mut rc = pc.clone();
            let before = pc.clone();
            assert_eq!(
                p.place_limit_immediate_with_cursor(taker.clone(), &math, fok, &mut pc),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(
                r.place_limit_immediate_with_cursor(taker, &math, fok, &mut rc),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(p, before_p);
            assert_eq!(r, before_r);
            assert_eq!(pc, before);
            assert_eq!(rc, before);
        }
    }
}

#[test]
fn market_cursor_matches_legacy_for_both_models() {
    use matching_domain::{MarketByBase, MarketByQuote, MarketMath, QuoteBudget};
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for quote in [false, true] {
        for side in [Side::Buy, Side::Sell] {
            let maker_side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            let mut p = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
            let mut r = ReferenceOrderBook::new();
            for maker in [order(1, 1, maker_side), order(2, 2, maker_side)] {
                p.rest(maker.clone()).unwrap();
                r.rest(maker).unwrap();
            }
            let mut old_p = p.clone();
            let mut old_r = r.clone();
            let mut pc = TradeIdCursor::new(ClusterId(1), ShardId(2), 3);
            let mut rc = pc.clone();
            let mut generator = pc.clone();
            let ids = [
                generator.next_trade_id().unwrap(),
                generator.next_trade_id().unwrap(),
            ];
            if quote {
                let request = MarketByQuote {
                    order_id: OrderId(3),
                    user_id: UserId(3),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side,
                    quote_budget: QuoteBudget::try_new(200).unwrap(),
                    market_protection: None,
                };
                let a = p
                    .execute_market_by_quote_with_cursor(request.clone(), &math, &mut pc)
                    .unwrap();
                let b = r
                    .execute_market_by_quote_with_cursor(request.clone(), &math, &mut rc)
                    .unwrap();
                assert_eq!(a, b);
                assert_eq!(
                    a,
                    old_p
                        .execute_market_by_quote(request.clone(), &math, &ids)
                        .unwrap()
                );
                assert_eq!(
                    b,
                    old_r.execute_market_by_quote(request, &math, &ids).unwrap()
                );
            } else {
                let request = MarketByBase {
                    order_id: OrderId(3),
                    user_id: UserId(3),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side,
                    qty: Qty::try_new(2).unwrap(),
                    market_protection: None,
                };
                let a = p
                    .execute_market_by_base_with_cursor(request.clone(), &math, &mut pc)
                    .unwrap();
                let b = r
                    .execute_market_by_base_with_cursor(request.clone(), &math, &mut rc)
                    .unwrap();
                assert_eq!(a, b);
                assert_eq!(
                    a,
                    old_p
                        .execute_market_by_base(request.clone(), &ids, &math)
                        .unwrap()
                );
                assert_eq!(
                    b,
                    old_r.execute_market_by_base(request, &ids, &math).unwrap()
                );
            }
            assert_eq!(pc.next_index(), 2);
            assert_eq!(rc.next_index(), 2);
        }
    }
}
