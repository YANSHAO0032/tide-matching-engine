//! 广泛审查发现的接收总量、预留身份、费率及参考序号边界回归。
use super::amend_reservation::{command, context, setup, sign, state, token};
use super::amend_stop::production_snapshot;
use super::*;
use crate::ReferenceOrderBook;
use ed25519_dalek::SigningKey;
use matching_domain::{
    AmendedQuantity, AssetReserve, IcebergOrder, LogicalTime, PriceReference, PriceReferenceKind,
    ReservationId, StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce,
    TriggerExpiry,
};
use matching_risk::RiskKeySet;

#[test]
fn broad_review_partial_placement_preserves_absolute_total_for_later_amend() {
    for side in [Side::Buy, Side::Sell] {
        for iceberg_maker in [false, true] {
            let mut p = ProductionOrderBook::new(policy());
            let mut r = ReferenceOrderBook::new();
            let opposite = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            let maker = order(1, opposite, 100, 3);
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            if iceberg_maker {
                let ice = IcebergOrder::try_new(maker, Qty::try_new(2).unwrap()).unwrap();
                p.place_iceberg_post_only(ice.clone(), &math).unwrap();
                r.place_iceberg_post_only(ice, &math).unwrap();
            } else {
                p.rest(maker.clone()).unwrap();
                r.rest(maker).unwrap();
            }
            let input = order(2, side, 100, 10);
            let trades = [TradeId::new(1), TradeId::new(2)];
            assert_eq!(
                p.place_limit_gtc_with_reservation(input.clone(), state(&input), &trades),
                r.place_limit_gtc_with_reservation(input.clone(), state(&input), &trades)
            );
            assert_eq!(
                (
                    p.order(input.order_id).unwrap().original_order().qty.get(),
                    r.orders(side)[0].original_order().qty.get()
                ),
                (10, 10)
            );
            let priority = p.order(input.order_id).unwrap().priority();
            let next = p.next_queue_priority();
            let mut amend = command(&input);
            amend.new_qty = Some(Qty::try_new(9).unwrap());
            let keys = RiskKeySet::new(2, []).unwrap();
            let mut ctx = context(&keys);
            ctx.required_reserves = vec![AssetReserve {
                asset: state(&input).reserves[0].asset,
                atoms: 900,
            }];
            let expected = vec![EngineEvent::OrderAmended {
                order_id: input.order_id,
                old_price: Some(input.price),
                new_price: Some(input.price),
                old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(7).unwrap()),
                new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(6).unwrap()),
                priority_retained: true,
            }];
            assert_eq!(
                p.amend_with_context(&amend, LogicalTime::new(1), 9, &[], &ctx),
                Ok(expected.clone())
            );
            assert_eq!(
                r.amend_with_context(&amend, LogicalTime::new(1), 9, &[], &ctx),
                Ok(expected)
            );
            assert_eq!(p.order(input.order_id).unwrap().total_remaining(), 6);
            assert_eq!(r.orders(side)[0].total_remaining(), 6);
            assert_eq!(p.order(input.order_id).unwrap().priority(), priority);
            assert_eq!(r.orders(side)[0].priority(), priority);
            assert_eq!(p.next_queue_priority(), next);
            assert_eq!(r.test_next_queue_priority(), next);
            assert_eq!(validate_book(&p), Ok(()));
        }
    }
}

#[test]
fn broad_review_all_regular_initializers_reject_wrong_user_atomically() {
    for route in 0..4 {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = ReferenceOrderBook::new();
        let input = order(1, Side::Buy, 100, 10);
        let mut shadow = state(&input);
        shadow.user_id = UserId::new(99);
        let maker = order(2, Side::Sell, if route == 2 { 100 } else { 110 }, 3);
        p.rest_with_reservation(maker.clone(), state(&maker))
            .unwrap();
        r.rest_with_reservation(maker.clone(), state(&maker))
            .unwrap();
        let before_p = production_snapshot(&p);
        let before_r = r.test_deterministic_state();
        let actual = match route {
            0 => (
                p.rest_with_reservation(input.clone(), shadow.clone()),
                r.rest_with_reservation(input, shadow),
            ),
            1 => (
                p.rest_timed_with_reservation(
                    input.clone(),
                    TimeInForce::Gtc,
                    LogicalTime::new(1),
                    9,
                    shadow.clone(),
                ),
                r.rest_timed_with_reservation(
                    input,
                    TimeInForce::Gtc,
                    LogicalTime::new(1),
                    9,
                    shadow,
                ),
            ),
            2 => (
                p.place_limit_gtc_with_reservation(
                    input.clone(),
                    shadow.clone(),
                    &[TradeId::new(1)],
                )
                .map(|_| ()),
                r.place_limit_gtc_with_reservation(input, shadow, &[TradeId::new(1)])
                    .map(|_| ()),
            ),
            _ => {
                let ice = IcebergOrder::try_new(input, Qty::try_new(3).unwrap()).unwrap();
                let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
                (
                    p.place_iceberg_post_only_with_reservation(ice.clone(), &math, shadow.clone()),
                    r.place_iceberg_post_only_with_reservation(ice, &math, shadow),
                )
            }
        };
        assert_eq!(
            actual,
            (
                Err(RejectReason::InvalidReservation),
                Err(RejectReason::InvalidReservation)
            ),
            "route={route}"
        );
        assert_eq!(production_snapshot(&p), before_p);
        assert_eq!(r.test_deterministic_state(), before_r);
    }
}

fn stop(input: &LimitGtcOrder, market: bool) -> StopOrder {
    if market {
        StopOrder::Market(StopMarketOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: input.order_id,
            user_id: input.user_id,
            reservation_id: state(input).reservation_id,
            fee_schedule_version: 5,
            side: input.side,
            stop_price: Price::try_new(110).unwrap(),
            quantity: StopMarketQuantity::Base(input.qty),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        })
    } else {
        StopOrder::Limit(StopLimitOrder {
            trigger_reference: PriceReferenceKind::LastTrade,
            order_id: input.order_id,
            user_id: input.user_id,
            reservation_id: state(input).reservation_id,
            fee_schedule_version: 5,
            side: input.side,
            stop_price: Price::try_new(110).unwrap(),
            price: input.price,
            qty: input.qty,
            child_tif: TimeInForce::Gtc,
            trigger_expiry: TriggerExpiry::Gtc,
        })
    }
}

#[test]
fn broad_review_stop_initializers_bind_user_and_payload_reservation_atomically() {
    for market in [false, true] {
        for wrong_user in [false, true] {
            let mut p = ProductionOrderBook::new(policy());
            let mut r = ReferenceOrderBook::new();
            let input = order(1, Side::Buy, 100, 10);
            let stop = stop(&input, market);
            let mut shadow = state(&input);
            if wrong_user {
                shadow.user_id = UserId::new(99);
            } else {
                shadow.reservation_id = ReservationId::new(99);
            }
            let before_p = production_snapshot(&p);
            let before_r = r.test_deterministic_state();
            let actual = (
                p.place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 9, shadow.clone()),
                r.place_stop_with_reservation(stop, LogicalTime::new(1), 9, shadow),
            );
            assert_eq!(
                actual,
                (
                    Err(RejectReason::InvalidReservation),
                    Err(RejectReason::InvalidReservation)
                )
            );
            assert_eq!(production_snapshot(&p), before_p);
            assert_eq!(r.test_deterministic_state(), before_r);
        }
    }
}

#[test]
fn broad_review_stop_fee_context_and_signed_token_must_match_pinned_policy() {
    let signing = SigningKey::from_bytes(&[8; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 2..5 {
        for signed in [false, true] {
            let (mut p, mut r, input, mut amend) = setup(kind);
            let mut ctx = context(&keys);
            ctx.fee_schedule_version = 6;
            ctx.required_reserves[0].atoms = 900;
            if signed {
                let mut authorization = token(&input, &signing);
                authorization.fee_schedule_version = 6;
                sign(&signing, &mut authorization);
                amend.reservation_adjustment = Some(authorization);
            }
            let before_p = production_snapshot(&p);
            let before_r = r.test_deterministic_state();
            let actual = (
                p.amend_with_context(&amend, LogicalTime::new(1), 9, &[], &ctx),
                r.amend_with_context(&amend, LogicalTime::new(1), 9, &[], &ctx),
            );
            assert_eq!(
                actual,
                (
                    Err(RejectReason::InvalidReservation),
                    Err(RejectReason::InvalidReservation)
                )
            );
            assert_eq!(production_snapshot(&p), before_p);
            assert_eq!(r.test_deterministic_state(), before_r);
        }
    }
}

#[test]
fn broad_review_root_last_trade_sequence_overflow_rejects_before_any_mutation() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let input = order(1, Side::Buy, 100, 10);
    let stop = stop(&input, false);
    p.place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 9, state(&input))
        .unwrap();
    r.place_stop_with_reservation(stop, LogicalTime::new(1), 9, state(&input))
        .unwrap();
    let maker = order(2, Side::Sell, 100, 3);
    p.rest_with_reservation(maker.clone(), state(&maker))
        .unwrap();
    r.rest_with_reservation(maker.clone(), state(&maker))
        .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let reference = PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(100).unwrap(),
    };
    p.trigger_stops(reference, LogicalTime::new(1), 9, &math, &[], |_| Ok(()))
        .unwrap();
    r.trigger_stops(reference, LogicalTime::new(1), 9, &math, &[], |_| Ok(()))
        .unwrap();
    let before_p = production_snapshot(&p);
    let before_r = r.test_deterministic_state();
    let reference = PriceReference {
        source_seq: u64::MAX,
        price: Price::try_new(110).unwrap(),
        ..reference
    };
    assert_eq!(
        p.trigger_stops(
            reference,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(())
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        r.trigger_stops(
            reference,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(())
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(production_snapshot(&p), before_p);
    assert_eq!(r.test_deterministic_state(), before_r);
    // 恰好预留一个后代成交序号仍可成功；不能把合法上界一并拒绝。
    let reference = PriceReference {
        source_seq: u64::MAX - 1,
        ..reference
    };
    let actual = p
        .trigger_stops(
            reference,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    let expected = r
        .trigger_stops(
            reference,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual
            .iter()
            .filter(|event| matches!(event, EngineEvent::Trade(_)))
            .count(),
        1
    );
    assert_eq!(
        p.reference_prices
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .source_seq,
        u64::MAX
    );
    assert_eq!(
        r.test_reference_state()
            .latest(PriceReferenceKind::LastTrade)
            .unwrap()
            .source_seq,
        u64::MAX
    );
    assert_eq!(p.order(input.order_id).unwrap().total_remaining(), 7);
    assert_eq!(r.orders(Side::Buy)[0].total_remaining(), 7);
    assert_eq!(validate_book(&p), Ok(()));
}
