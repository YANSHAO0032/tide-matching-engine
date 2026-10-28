//! 逻辑时间固定 tape 的独立参考 / 生产差分验证。

use super::*;
use matching_domain::{
    ExpireReason, ExpiredOrder, LogicalClock, LogicalTime, ReservationId, StopLimitOrder,
    StopOrder, TimeInForce, TriggerExpiry,
};

fn timed_order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn timed_stop(id: u128, expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        reservation_id: ReservationId::new(id + 1_000),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(110).unwrap(),
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: expiry,
    })
}

#[test]
fn logical_time_fixed_tape_keeps_reference_and_production_identical() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let mut clock = LogicalClock::new(LogicalTime::new(0));
    let session = 7;

    for (order, tif) in [
        (timed_order(1, Side::Buy, 90, 3), TimeInForce::Gtc),
        (
            timed_order(2, Side::Buy, 89, 3),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(100),
            },
        ),
        (
            timed_order(3, Side::Buy, 88, 3),
            TimeInForce::Day {
                session_id: session,
            },
        ),
        (
            timed_order(4, Side::Buy, 87, 3),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(90),
            },
        ),
    ] {
        assert_eq!(
            reference.rest_timed(order.clone(), tif, clock.now(), session),
            production.rest_timed(order, tif, clock.now(), session)
        );
        assert_eq!(
            observe_reference(&reference),
            observe_production(&production)
        );
        assert_eq!(validate_book(&production), Ok(()));
    }
    for stop in [
        timed_stop(
            5,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(100),
            },
        ),
        timed_stop(
            6,
            TriggerExpiry::Day {
                session_id: session,
            },
        ),
    ] {
        assert_eq!(
            reference.place_stop(stop.clone(), clock.now(), session),
            production.place_stop(stop, clock.now(), session)
        );
    }

    clock.advance_to(LogicalTime::new(89)).unwrap();
    assert_eq!(
        reference.expire_gtd_through(clock.now()),
        Vec::<ExpiredOrder>::new()
    );
    assert_eq!(
        production.expire_gtd_through(clock.now()),
        Vec::<ExpiredOrder>::new()
    );

    clock.advance_to(LogicalTime::new(90)).unwrap();
    let expected_ninety = vec![ExpiredOrder {
        order_id: OrderId::new(4),
        reason: ExpireReason::GtdReached,
    }];
    assert_eq!(reference.expire_gtd_through(clock.now()), expected_ninety);
    assert_eq!(production.expire_gtd_through(clock.now()), expected_ninety);
    assert_eq!(
        observe_reference(&reference),
        observe_production(&production)
    );

    clock.advance_to(LogicalTime::new(100)).unwrap();
    let expected_hundred = vec![
        ExpiredOrder {
            order_id: OrderId::new(2),
            reason: ExpireReason::GtdReached,
        },
        ExpiredOrder {
            order_id: OrderId::new(5),
            reason: ExpireReason::StopTriggerGtdReached,
        },
    ];
    assert_eq!(reference.expire_gtd_through(clock.now()), expected_hundred);
    assert_eq!(production.expire_gtd_through(clock.now()), expected_hundred);

    let expected_session = vec![
        ExpiredOrder {
            order_id: OrderId::new(3),
            reason: ExpireReason::SessionEnded,
        },
        ExpiredOrder {
            order_id: OrderId::new(6),
            reason: ExpireReason::StopTriggerSessionEnded,
        },
    ];
    assert_eq!(
        reference.end_session(session, session),
        Ok(expected_session.clone())
    );
    assert_eq!(
        production.end_session(session, session),
        Ok(expected_session)
    );
    assert_eq!(
        observe_reference(&reference),
        observe_production(&production)
    );
    assert_eq!(validate_book(&production), Ok(()));
    assert_eq!(
        reference.orders(Side::Buy)[0].original_order().order_id,
        OrderId::new(1)
    );
}

#[test]
fn timed_limit_places_only_its_residual_and_that_residual_expires() {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let maker = timed_order(1, Side::Sell, 100, 2);
    reference.rest(maker.clone()).unwrap();
    production.rest(maker).unwrap();
    let incoming = timed_order(2, Side::Buy, 100, 5);
    let tif = TimeInForce::Gtd {
        expire_at: LogicalTime::new(10),
    };

    assert_eq!(
        reference.place_limit_timed_with_events(
            incoming.clone(),
            tif,
            LogicalTime::new(0),
            1,
            &[TradeId::new(1)],
        ),
        production.place_limit_timed_with_events(
            incoming,
            tif,
            LogicalTime::new(0),
            1,
            &[TradeId::new(1)],
        )
    );
    assert_eq!(
        observe_reference(&reference),
        observe_production(&production)
    );
    assert_eq!(
        reference.expire_gtd_through(LogicalTime::new(10)),
        vec![ExpiredOrder {
            order_id: OrderId::new(2),
            reason: ExpireReason::GtdReached,
        }]
    );
    assert_eq!(
        production.expire_gtd_through(LogicalTime::new(10)),
        vec![ExpiredOrder {
            order_id: OrderId::new(2),
            reason: ExpireReason::GtdReached,
        }]
    );
    assert_eq!(
        observe_reference(&reference),
        observe_production(&production)
    );
    assert_eq!(validate_book(&production), Ok(()));
}

#[test]
fn repeating_the_same_logical_time_tape_keeps_the_transcript_identical() {
    let mut transcripts = Vec::new();
    for _ in 0..2 {
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());
        let mut clock = LogicalClock::new(LogicalTime::new(0));
        let order = timed_order(1, Side::Buy, 90, 1);
        let tif = TimeInForce::Gtd {
            expire_at: LogicalTime::new(5),
        };
        let reference_result = reference.rest_timed(order.clone(), tif, clock.now(), 1);
        let production_result = production.rest_timed(order, tif, clock.now(), 1);
        clock.advance_to(LogicalTime::new(5)).unwrap();
        let reference_expired = reference.expire_gtd_through(clock.now());
        let production_expired = production.expire_gtd_through(clock.now());
        transcripts.push((
            reference_result,
            production_result,
            reference_expired,
            production_expired,
            observe_reference(&reference),
            observe_production(&production),
        ));
    }
    assert_eq!(transcripts[0], transcripts[1]);
}

proptest! {
    #[test]
    fn generated_gtd_expiry_tape_keeps_independent_models_equal(
        candidates in proptest::collection::vec((1u128..80, 1u64..200), 1..32),
        canceled_position in 0usize..32,
        advance_to in 1u64..200,
    ) {
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());
        let now = LogicalTime::new(0);

        for (id, expire_at) in &candidates {
            let order = timed_order(*id, Side::Buy, 100, 1);
            let tif = TimeInForce::Gtd {
                expire_at: LogicalTime::new(*expire_at),
            };
            prop_assert_eq!(
                reference.rest_timed(order.clone(), tif, now, 1),
                production.rest_timed(order, tif, now, 1),
            );
            prop_assert_eq!(observe_reference(&reference), observe_production(&production));
            prop_assert_eq!(validate_book(&production), Ok(()));
        }

        if let Some((order_id, _)) = candidates.get(canceled_position % candidates.len()) {
            let reference_cancel = reference.cancel(OrderId::new(*order_id)).map(|_| ());
            let production_cancel = production.cancel(OrderId::new(*order_id)).map(|_| ());
            prop_assert_eq!(reference_cancel, production_cancel);
        }

        let at = LogicalTime::new(advance_to);
        prop_assert_eq!(reference.expire_gtd_through(at), production.expire_gtd_through(at));
        prop_assert_eq!(observe_reference(&reference), observe_production(&production));
        prop_assert_eq!(validate_book(&production), Ok(()));
    }
}
