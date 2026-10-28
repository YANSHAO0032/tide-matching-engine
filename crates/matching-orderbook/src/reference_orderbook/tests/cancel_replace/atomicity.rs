//! 独立参考状态的提交漂移屏障与调用方容量契约。

use super::*;
use proptest::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn setup(side: Side, qty: u64) -> (ReferenceOrderBook, ReservationRegistry) {
    let mut book = ReferenceOrderBook::new();
    let mut old = order(1, side, qty);
    old.price = Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap();
    book.rest_timed_with_reservation(
        old.clone(),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        },
        LogicalTime::new(1),
        7,
        state(1),
    )
    .unwrap();
    old.order_id = OrderId::new(2);
    book.rest_with_reservation(old, state(2)).unwrap();
    book.place_stop_with_reservation(
        StopOrder::Limit(StopLimitOrder {
            order_id: OrderId::new(3),
            user_id: UserId::new(5),
            reservation_id: state(3).reservation_id,
            fee_schedule_version: 1,
            side,
            stop_price: Price::try_new(120).unwrap(),
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_reference: matching_domain::PriceReferenceKind::MarkPrice,
            trigger_expiry: TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(100),
            },
        }),
        LogicalTime::new(1),
        7,
        state(3),
    )
    .unwrap();
    (
        book,
        ReservationRegistry::new([state(1), state(2), state(3)]).unwrap(),
    )
}

fn drift(case: u8, side: Side, qty: u64, new: bool) {
    let (mut book, mut registry) = setup(side, qty);
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let plan = book
        .plan_cancel_replace(
            &command(new, side, qty, TimeInForce::Gtc),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[]),
        )
        .unwrap();
    match case {
        0 => book.next_priority += 1,
        1 => book.next_stop_priority += 1,
        2 => book.reservations.get_mut(&OrderId::new(2)).unwrap().version += 1,
        3 => {
            book.reference_prices
                .advance(matching_domain::PriceReference {
                    kind: matching_domain::PriceReferenceKind::MarkPrice,
                    source_seq: 1,
                    price: Price::try_new(100).unwrap(),
                })
                .unwrap();
        }
        4 => book.orders.swap(0, 1),
        5 => {
            let old = &book.orders[0];
            book.orders[0] = ReferenceOrder::new_timed(
                old.original_order().clone(),
                old.priority(),
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(101),
                },
            );
        }
        6 => {
            if let StopOrder::Limit(old) = &mut book.stop_orders[0].order {
                old.trigger_expiry = TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(101),
                };
            }
        }
        7 => book.stop_orders[0].priority = matching_domain::StopPriority::new(10),
        8 => {
            let change = registry.plan_binding_transitions([&state(2)], []).unwrap();
            registry.apply_binding_transition(change).unwrap();
        }
        9 => {
            let mut history = state(99);
            history.status = ReservationStatus::Released;
            registry = ReservationRegistry::new([state(1), state(2), state(3), history]).unwrap();
        }
        10 => {
            book.apply_cancel_replace(&mut registry, plan.clone())
                .unwrap();
        }
        _ => unreachable!(),
    }
    let before = (book.clone(), registry.clone());
    assert_eq!(
        book.apply_cancel_replace(&mut registry, plan.clone()),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!((book.clone(), registry.clone()), before);
    assert_eq!(
        book.apply_cancel_replace(&mut registry, plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!((book, registry), before);
}

#[test]
fn cancel_replace_atomicity_reference_apply_checks_every_state_component_and_history() {
    for case in 0..11 {
        for side in [Side::Buy, Side::Sell] {
            for new in [false, true] {
                drift(case, side, 5, new);
            }
        }
    }
}

#[test]
fn cancel_replace_atomicity_reference_trade_id_shortage_panics_before_any_candidate_commit() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for new in [false, true] {
            let (mut book, registry) = setup(side, 5);
            let opposite = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            book.rest_with_reservation(order(4, opposite, 5), state(4))
                .unwrap();
            let registry = ReservationRegistry::new([
                registry.get(state(1).reservation_id).unwrap().clone(),
                state(2),
                state(3),
                state(4),
            ])
            .unwrap();
            let before = (book.clone(), registry.clone());
            for _ in 0..2 {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| book.plan_cancel_replace(
                        &command(new, side, 5, TimeInForce::Fok),
                        &expected(),
                        &registry,
                        &keys(),
                        context(&math, &[])
                    )))
                    .is_err()
                );
                assert_eq!((book.clone(), registry.clone()), before);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn cancel_replace_atomicity_generated_reference_drift_is_fail_closed(case in 0u8..11,buy in any::<bool>(),qty in 2u64..32,new in any::<bool>()) {
        drift(case,if buy {Side::Buy} else {Side::Sell},qty,new);
    }

    #[test]
    fn cancel_replace_atomicity_reference_priority_high_water_reject_is_pure(buy in any::<bool>(),qty in 2u64..32,new in any::<bool>()) {
        let side=if buy {Side::Buy} else {Side::Sell};
        let (mut book,registry)=setup(side,qty);
        book.next_priority=u128::MAX-PRIORITY_GUARD;
        let before=(book.clone(),registry.clone());
        let math=MarketMath::try_new(0,0,0,1,1).unwrap();
        let request=command(new,side,qty,TimeInForce::Gtc);
        let first=book.plan_cancel_replace(&request,&expected(),&registry,&keys(),context(&math,&[]));
        assert_eq!(first,book.plan_cancel_replace(&request,&expected(),&registry,&keys(),context(&math,&[])));
        assert_eq!(first.unwrap_err(),request.rejection(RejectReason::PrioritySpaceExhaustion));
        assert_eq!((book,registry),before);
    }
}
