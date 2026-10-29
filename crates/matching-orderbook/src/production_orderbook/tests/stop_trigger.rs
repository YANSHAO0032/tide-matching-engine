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
