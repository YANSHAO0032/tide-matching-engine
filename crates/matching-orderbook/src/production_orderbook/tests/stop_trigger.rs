//! 止损触发、Trigger-R1 与内部级联的生产/参考一致性验证。

use super::*;
use crate::ReferenceOrderBook;
use matching_domain::{
    LogicalTime, PriceReference, PriceReferenceKind, ReservationId, ReservationReleaseReason,
    StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce, TradeId,
    TriggerExpiry,
};

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn stop_market(id: u128, side: Side, stop_price: i64) -> StopOrder {
    StopOrder::Market(StopMarketOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id + 1_000),
        fee_schedule_version: 7,
        side,
        stop_price: Price::try_new(stop_price).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(1).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    })
}

fn stop_limit(id: u128, side: Side, stop_price: i64, price: i64) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id + 1_000),
        fee_schedule_version: 7,
        side,
        stop_price: Price::try_new(stop_price).unwrap(),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    })
}

fn last_trade(source_seq: u64, price: i64) -> PriceReference {
    PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq,
        price: Price::try_new(price).unwrap(),
    }
}

#[test]
fn stop_quote_child_preserves_stp_identity_and_event_order_in_both_books() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = ReferenceOrderBook::new();
    let mut maker = order(10, Side::Sell, 100, 2);
    maker.stp_key = matching_domain::StpKey::new(7);
    production.rest(maker.clone()).unwrap();
    reference.rest(maker).unwrap();
    let mut stop = stop_market(11, Side::Buy, 105);
    let StopOrder::Market(ref mut child) = stop else {
        unreachable!()
    };
    child.stp_key = matching_domain::StpKey::new(7);
    child.stp_mode = matching_domain::StpMode::CancelTaker;
    child.quantity =
        StopMarketQuantity::Quote(matching_domain::QuoteBudget::try_new(1000).unwrap());
    production
        .place_stop(stop.clone(), LogicalTime::new(1), 1)
        .unwrap();
    reference.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    production
        .trigger_stops(
            last_trade(1, 100),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    reference
        .trigger_stops(
            last_trade(1, 100),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    let actual = production
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    let expected = reference
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.as_slice(), [EngineEvent::StopTriggered { order_id, .. }, EngineEvent::OrderCanceled { remaining: matching_domain::OrderRemaining::Quote(q), reason: matching_domain::CancelReason::SelfTradePreventionTaker, .. }] if *order_id == OrderId::new(11) && q.get() == 1000)
    );
}

#[test]
fn stop_base_and_limit_children_keep_stp_events_in_both_books() {
    for is_limit in [false, true] {
        let mut production = ProductionOrderBook::new(policy());
        let mut reference = ReferenceOrderBook::new();
        let mut maker = order(10, Side::Sell, 100, 2);
        maker.stp_key = matching_domain::StpKey::new(7);
        production.rest(maker.clone()).unwrap();
        reference.rest(maker).unwrap();
        let mut stop = if is_limit {
            stop_limit(11, Side::Buy, 105, 100)
        } else {
            stop_market(11, Side::Buy, 105)
        };
        match &mut stop {
            StopOrder::Limit(child) => {
                child.stp_key = matching_domain::StpKey::new(7);
                child.stp_mode = matching_domain::StpMode::CancelTaker;
            }
            StopOrder::Market(child) => {
                child.stp_key = matching_domain::StpKey::new(7);
                child.stp_mode = matching_domain::StpMode::CancelTaker;
            }
        }
        production
            .place_stop(stop.clone(), LogicalTime::new(1), 1)
            .unwrap();
        reference.place_stop(stop, LogicalTime::new(1), 1).unwrap();
        production
            .trigger_stops(
                last_trade(1, 100),
                LogicalTime::new(1),
                1,
                &math(),
                &[],
                |_| Ok(()),
            )
            .unwrap();
        reference
            .trigger_stops(
                last_trade(1, 100),
                LogicalTime::new(1),
                1,
                &math(),
                &[],
                |_| Ok(()),
            )
            .unwrap();
        let actual = production
            .trigger_stops(
                last_trade(2, 105),
                LogicalTime::new(1),
                1,
                &math(),
                &[],
                |_| Ok(()),
            )
            .unwrap();
        let expected = reference
            .trigger_stops(
                last_trade(2, 105),
                LogicalTime::new(1),
                1,
                &math(),
                &[],
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(actual, expected);
        assert!(
            matches!(actual.as_slice(), [EngineEvent::StopTriggered { order_id, .. }, EngineEvent::OrderCanceled { remaining: matching_domain::OrderRemaining::Base(q), reason: matching_domain::CancelReason::SelfTradePreventionTaker, .. }] if *order_id == OrderId::new(11) && q.get() == 1)
        );
    }
}

#[test]
fn trigger_r1_insufficient_reservation_has_zero_trades_and_removes_stop_first() {
    let mut book = ProductionOrderBook::new(policy());
    let stop = stop_market(1, Side::Buy, 105);
    book.place_stop(stop.clone(), LogicalTime::new(1), 1)
        .unwrap();
    assert_eq!(
        book.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );
    let priority_before = book.next_queue_priority();

    let events = book
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Err(RejectReason::InsufficientReservation),
        )
        .unwrap();

    assert_eq!(
        events,
        vec![
            EngineEvent::StopTriggered {
                order_id: OrderId::new(1),
                child_kind: matching_domain::TriggeredOrderKind::MarketByBase,
            },
            EngineEvent::TriggeredOrderRejected {
                order_id: OrderId::new(1),
                reason: RejectReason::InsufficientReservation,
            },
            EngineEvent::ReservationReleased {
                order_id: OrderId::new(1),
                reservation_id: ReservationId::new(1_001),
                reason: ReservationReleaseReason::TriggerRejected,
                released: None,
            },
        ]
    );
    assert!(book.stop_order(OrderId::new(1)).is_none());
    assert_eq!(book.next_queue_priority(), priority_before);
    assert!(book.validate_stop_book().is_ok());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::Trade(_)))
    );
    assert_eq!(
        book.trigger_stops(
            last_trade(3, 106),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(())
        ),
        Ok(Vec::new())
    );
}

#[test]
fn trigger_r1_fee_schedule_mismatch_has_zero_trades_and_releases_reservation() {
    let mut book = ProductionOrderBook::new(policy());
    book.place_stop(stop_market(1, Side::Buy, 105), LogicalTime::new(1), 1)
        .unwrap();
    assert_eq!(
        book.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );

    let events = book
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Err(RejectReason::FeeScheduleMismatch),
        )
        .unwrap();

    assert!(matches!(
        events.as_slice(),
        [
            EngineEvent::StopTriggered { order_id, .. },
            EngineEvent::TriggeredOrderRejected {
                order_id: rejected,
                reason: RejectReason::FeeScheduleMismatch,
            },
            EngineEvent::ReservationReleased {
                order_id: released,
                reservation_id,
                reason: ReservationReleaseReason::TriggerRejected,
                released: None,
            },
        ] if *order_id == OrderId::new(1)
            && *rejected == OrderId::new(1)
            && *released == OrderId::new(1)
            && *reservation_id == ReservationId::new(1_001)
    ));
    assert!(book.stop_order(OrderId::new(1)).is_none());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::Trade(_)))
    );
}

#[test]
fn static_source_and_stale_sequence_do_not_mutate_stop_book() {
    let mut book = ProductionOrderBook::new(policy());
    book.place_stop(stop_market(1, Side::Buy, 105), LogicalTime::new(1), 1)
        .unwrap();

    assert_eq!(
        book.collect_triggered_stops(PriceReference {
            kind: PriceReferenceKind::StaticOpening,
            source_seq: 1,
            price: Price::try_new(200).unwrap(),
        }),
        Err(RejectReason::InvalidReferencePriceSource)
    );
    assert_eq!(
        book.collect_triggered_stops(last_trade(2, 100)),
        Ok(Vec::new())
    );
    assert_eq!(
        book.collect_triggered_stops(last_trade(2, 106)),
        Err(RejectReason::StaleReferencePriceUpdate)
    );
    assert!(book.stop_order(OrderId::new(1)).is_some());
}

#[test]
fn collecting_stop_removes_stop_book_and_expiry_index_first() {
    let mut book = ProductionOrderBook::new(policy());
    book.place_stop(stop_market(1, Side::Buy, 105), LogicalTime::new(1), 1)
        .unwrap();
    assert_eq!(
        book.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );

    let collected = book.collect_triggered_stops(last_trade(2, 105)).unwrap();
    assert!(book.stop_order(OrderId::new(1)).is_none());
    assert!(book.validate_stop_book().is_ok());
    assert_eq!(collected.len(), 1);
    assert!(matches!(
        collected[0].2,
        EngineEvent::StopTriggered { order_id, .. } if order_id == OrderId::new(1)
    ));
}

#[test]
fn limit_child_allocates_queue_priority_only_when_it_rests() {
    let mut book = ProductionOrderBook::new(policy());
    book.place_stop(stop_limit(1, Side::Buy, 105, 90), LogicalTime::new(1), 1)
        .unwrap();
    assert_eq!(
        book.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );
    assert_eq!(book.next_queue_priority(), 0);

    let events = book
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    assert!(matches!(
        events.as_slice(),
        [EngineEvent::StopTriggered {
            order_id,
            child_kind: matching_domain::TriggeredOrderKind::Limit,
        }] if *order_id == OrderId::new(1)
    ));
    assert_eq!(book.order(OrderId::new(1)).unwrap().priority().get(), 0);
    assert_eq!(book.next_queue_priority(), 1);
}

fn run_cascade() -> (Vec<EngineEvent>, Vec<EngineEvent>) {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = ReferenceOrderBook::new();

    for (id, side, price) in [(10, Side::Sell, 100), (11, Side::Buy, 90)] {
        production
            .place_limit_gtc(order(id, side, price, 1), &[])
            .unwrap();
        reference.rest(order(id, side, price, 1)).unwrap();
    }
    for stop in [
        stop_market(1, Side::Buy, 105),
        stop_market(2, Side::Sell, 100),
    ] {
        production
            .place_stop(stop.clone(), LogicalTime::new(1), 1)
            .unwrap();
        reference.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    }
    assert_eq!(
        production.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );
    assert_eq!(
        reference.collect_triggered_stops(last_trade(1, 100)),
        Ok(Vec::new())
    );

    let trade_ids = [TradeId::new(1), TradeId::new(2), TradeId::new(3)];
    let production_events = production
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &trade_ids,
            |_| Ok(()),
        )
        .unwrap();
    let reference_events = reference
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &trade_ids,
            |_| Ok(()),
        )
        .unwrap();
    assert!(production.stop_order(OrderId::new(1)).is_none());
    assert!(production.stop_order(OrderId::new(2)).is_none());
    assert!(reference.stop_order(OrderId::new(1)).is_none());
    assert!(reference.stop_order(OrderId::new(2)).is_none());
    assert!(production.validate_stop_book().is_ok());
    (production_events, reference_events)
}

#[test]
fn child_trades_trigger_follow_up_stops_in_fifo_order_and_match_reference() {
    let (production, reference) = run_cascade();
    assert_eq!(production, reference);
    assert!(matches!(
        production.as_slice(),
        [
            EngineEvent::StopTriggered { order_id: first, .. },
            EngineEvent::Trade(_),
            EngineEvent::StopTriggered { order_id: second, .. },
            EngineEvent::Trade(_),
        ] if *first == OrderId::new(1) && *second == OrderId::new(2)
    ));
}

#[test]
fn repeated_input_produces_identical_cascade_event_order() {
    assert_eq!(run_cascade().0, run_cascade().0);
}

#[test]
fn triggered_market_uses_new_band_reference_in_both_books() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let maker = order(10, Side::Sell, 106, 1);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let stop = stop_market(11, Side::Buy, 105);
    p.place_stop(stop.clone(), LogicalTime::new(1), 1).unwrap();
    r.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    p.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    r.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let config = matching_domain::PriceBandConfig {
        reference_kind: PriceReferenceKind::LastTrade,
        max_deviation_bps: 0,
    };
    p.set_price_band_config(Some((config, math()))).unwrap();
    r.set_price_band_config(Some((config, math()))).unwrap();
    let pe = p
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    let re = r
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(pe, re);
    assert!(pe.iter().any(|event| matches!(event, EngineEvent::MarketProtectionTriggered { next_price, effective_worst_price, .. } if next_price.get() == 106 && effective_worst_price.get() == 105)));
}

#[test]
fn triggered_market_uses_other_configured_reference_in_both_books() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let maker = order(10, Side::Sell, 106, 1);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let stop = stop_market(11, Side::Buy, 105);
    p.place_stop(stop.clone(), LogicalTime::new(1), 1).unwrap();
    r.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    let mark = PriceReference {
        kind: PriceReferenceKind::MarkPrice,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    };
    p.trigger_stops(mark, LogicalTime::new(1), 1, &math(), &[], |_| Ok(()))
        .unwrap();
    r.trigger_stops(mark, LogicalTime::new(1), 1, &math(), &[], |_| Ok(()))
        .unwrap();
    p.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    r.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let config = matching_domain::PriceBandConfig {
        reference_kind: PriceReferenceKind::MarkPrice,
        max_deviation_bps: 0,
    };
    p.set_price_band_config(Some((config, math()))).unwrap();
    r.set_price_band_config(Some((config, math()))).unwrap();
    let pe = p
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    let re = r
        .trigger_stops(
            last_trade(2, 105),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(pe, re);
    assert!(pe.iter().any(|event| matches!(event, EngineEvent::MarketProtectionTriggered { next_price, effective_worst_price, .. } if next_price.get() == 106 && effective_worst_price.get() == 100)));
}

#[test]
fn stop_limit_descendant_uses_frozen_band_after_last_trade_changes() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let maker = order(10, Side::Sell, 105, 1);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    for stop in [
        stop_market(11, Side::Buy, 110),
        stop_limit(12, Side::Sell, 108, 115),
    ] {
        p.place_stop(stop.clone(), LogicalTime::new(1), 1).unwrap();
        r.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    }
    p.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    r.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let config = matching_domain::PriceBandConfig {
        reference_kind: PriceReferenceKind::LastTrade,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((config, math()))).unwrap();
    r.set_price_band_config(Some((config, math()))).unwrap();
    let pe = p
        .trigger_stops(
            last_trade(2, 110),
            LogicalTime::new(1),
            1,
            &math(),
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    let re = r
        .trigger_stops(
            last_trade(2, 110),
            LogicalTime::new(1),
            1,
            &math(),
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(pe, re);
    assert!(!pe.iter().any(|event| matches!(event, EngineEvent::TriggeredOrderRejected { order_id, .. } if *order_id == OrderId::new(12))));
    assert!(p.order(OrderId::new(12)).is_some());
}

#[test]
fn stop_market_descendant_uses_frozen_band_after_last_trade_changes() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    for maker in [order(9, Side::Buy, 104, 1), order(10, Side::Sell, 105, 1)] {
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
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    r.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let config = matching_domain::PriceBandConfig {
        reference_kind: PriceReferenceKind::LastTrade,
        max_deviation_bps: 500,
    };
    p.set_price_band_config(Some((config, math()))).unwrap();
    r.set_price_band_config(Some((config, math()))).unwrap();
    let pe = p
        .trigger_stops(
            last_trade(2, 110),
            LogicalTime::new(1),
            1,
            &math(),
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    let re = r
        .trigger_stops(
            last_trade(2, 110),
            LogicalTime::new(1),
            1,
            &math(),
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(pe, re);
    assert!(pe.iter().any(|event| matches!(event, EngineEvent::MarketProtectionTriggered { order_id, next_price, effective_worst_price, .. } if *order_id == OrderId::new(12) && next_price.get() == 104 && effective_worst_price.get() == 105)));
    assert!(p.order(OrderId::new(9)).is_some());
}

#[test]
fn stop_cursor_overflow_discards_complete_root_and_keeps_cursor() {
    use matching_domain::{ClusterId, ShardId, TradeIdCursor};
    let mut p = ProductionOrderBook::new(policy());
    p.rest(order(10, Side::Sell, 105, 2)).unwrap();
    p.place_stop(stop_market(11, Side::Buy, 102), LogicalTime::new(1), 1)
        .unwrap();
    p.place_stop(stop_market(12, Side::Buy, 103), LogicalTime::new(1), 1)
        .unwrap();
    p.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let before = p.clone();
    let mut cursor =
        TradeIdCursor::with_next_index(ClusterId(1), ShardId(2), 5, u64::from(u32::MAX)).unwrap();
    let saved = cursor.clone();
    assert_eq!(
        p.trigger_stops_with_cursor(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &mut cursor,
            |_| Ok(())
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(p, before);
    assert_eq!(cursor, saved);
}

#[test]
fn stop_cursor_cascade_matches_slice_and_keeps_root_trade_indices() {
    use matching_domain::{ClusterId, ShardId, TradeIdCursor};
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(10, Side::Sell, 105, 2)).unwrap();
    book.place_stop(stop_market(11, Side::Buy, 102), LogicalTime::new(1), 1)
        .unwrap();
    book.place_stop(stop_market(12, Side::Buy, 105), LogicalTime::new(1), 1)
        .unwrap();
    book.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let mut direct = book.clone();
    let mut id_generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let ids = [
        id_generator.next_trade_id().unwrap(),
        id_generator.next_trade_id().unwrap(),
    ];
    let expected = direct
        .trigger_stops(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &ids,
            |_| Ok(()),
        )
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let actual = book
        .trigger_stops_with_cursor(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &mut cursor,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book.arena.state_snapshot(), direct.arena.state_snapshot());
    assert_eq!(book.bids, direct.bids);
    assert_eq!(book.asks, direct.asks);
    assert_eq!(book.orders, direct.orders);
    assert_eq!(book.stop_book, direct.stop_book);
    assert_eq!(book.reference_prices, direct.reference_prices);
    assert_eq!(book.next_queue_priority(), direct.next_queue_priority());
    assert_eq!(cursor.next_index(), 2);
}

#[test]
fn stop_cursor_limit_and_quote_match_slice_events() {
    use matching_domain::{ClusterId, ShardId, TradeIdCursor};
    for quote in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(10, Side::Sell, 105, 2)).unwrap();
        let mut child = if quote {
            stop_market(11, Side::Buy, 102)
        } else {
            stop_limit(11, Side::Buy, 102, 105)
        };
        if let StopOrder::Market(ref mut order) = child {
            order.quantity =
                StopMarketQuantity::Quote(matching_domain::QuoteBudget::try_new(105).unwrap());
        }
        book.place_stop(child, LogicalTime::new(1), 1).unwrap();
        book.trigger_stops(
            last_trade(1, 100),
            LogicalTime::new(1),
            1,
            &math(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
        let mut direct = book.clone();
        let mut id_generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
        let ids = [id_generator.next_trade_id().unwrap()];
        let expected = direct
            .trigger_stops(
                last_trade(2, 104),
                LogicalTime::new(1),
                1,
                &math(),
                &ids,
                |_| Ok(()),
            )
            .unwrap();
        let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
        let actual = book
            .trigger_stops_with_cursor(
                last_trade(2, 104),
                LogicalTime::new(1),
                1,
                &math(),
                &mut cursor,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(book.arena.state_snapshot(), direct.arena.state_snapshot());
        assert_eq!(book.stop_book, direct.stop_book);
        assert_eq!(cursor.next_index(), 1);
    }
}

#[test]
fn stop_quote_cursor_iceberg_events_match_slice() {
    use matching_domain::{ClusterId, IcebergOrder, QuoteBudget, ShardId, TradeIdCursor};
    let mut book = ProductionOrderBook::new(policy());
    let ice =
        IcebergOrder::try_new(order(10, Side::Sell, 105, 3), Qty::try_new(1).unwrap()).unwrap();
    book.place_iceberg_post_only(ice, &math()).unwrap();
    let mut stop = stop_market(11, Side::Buy, 102);
    if let StopOrder::Market(ref mut order) = stop {
        order.quantity = StopMarketQuantity::Quote(QuoteBudget::try_new(315).unwrap());
    }
    book.place_stop(stop, LogicalTime::new(1), 1).unwrap();
    book.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let mut direct = book.clone();
    let mut id_generator = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let ids = [
        id_generator.next_trade_id().unwrap(),
        id_generator.next_trade_id().unwrap(),
        id_generator.next_trade_id().unwrap(),
    ];
    let expected = direct
        .trigger_stops(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &ids,
            |_| Ok(()),
        )
        .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let actual = book
        .trigger_stops_with_cursor(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &mut cursor,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn failed_stop_child_does_not_consume_trade_identity() {
    use matching_domain::{ClusterId, ShardId, TradeIdCursor};
    let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(2)).unwrap();
    let mut book = ProductionOrderBook::new(policy);
    book.rest(order(10, Side::Sell, 105, 1)).unwrap();
    book.rest(order(13, Side::Sell, 105, 1)).unwrap();
    let mut first = stop_limit(11, Side::Buy, 102, 105);
    if let StopOrder::Limit(ref mut stop) = first {
        stop.qty = Qty::try_new(3).unwrap();
    }
    book.place_stop(first, LogicalTime::new(1), 1).unwrap();
    book.place_stop(stop_market(12, Side::Buy, 103), LogicalTime::new(1), 1)
        .unwrap();
    book.trigger_stops(
        last_trade(1, 100),
        LogicalTime::new(1),
        1,
        &math(),
        &[],
        |_| Ok(()),
    )
    .unwrap();
    let mut cursor = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    let events = book
        .trigger_stops_with_cursor(
            last_trade(2, 104),
            LogicalTime::new(1),
            1,
            &math(),
            &mut cursor,
            |_| Ok(()),
        )
        .unwrap();
    let ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::Trade(trade) => Some(trade.trade_id),
            _ => None,
        })
        .collect();
    let mut expected = TradeIdCursor::new(ClusterId(1), ShardId(2), 5);
    assert_eq!(ids, vec![expected.next_trade_id().unwrap()]);
    assert_eq!(cursor.next_index(), 1);
}
