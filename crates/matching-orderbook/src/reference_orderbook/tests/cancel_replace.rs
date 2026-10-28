//! 撤单替换必须先完成全量预演，并将订单终结与金融释放分离。

mod atomicity;

use super::*;
use crate::reference_orderbook::ReferencePlaceContext;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, CancelReason, CancelReplaceOrder, CommandId, IcebergOrder, LogicalTime,
    MarketId, OrderFlags, ReplacementOrder, ReplacementOrderKind, ReservationId, ReservationToken,
    ReservationTransition, StopLimitOrder, StopOrder, TimeInForce, TriggerExpiry, UserId,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationApplyError, ReservationRegistry, ReservationState,
    ReservationStatus, RiskKeySet,
};

fn order(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn state(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(100 + id),
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 3,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 10_000,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 2,
    }
}

fn expected() -> ExpectedReplacementReservation {
    ExpectedReplacementReservation {
        command_id: CommandId::new(41),
        old_reservation_id: state(1).reservation_id,
        old_order_id: OrderId::new(1),
        new_order_id: OrderId::new(9),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
        expected_version: 3,
        fee_schedule_version: 1,
        fee_tier_id: 2,
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 500,
        }],
    }
}

fn keys() -> RiskKeySet {
    RiskKeySet::new(1, [(31, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap()
}

fn command(new_reservation: bool, side: Side, qty: u64, tif: TimeInForce) -> CancelReplaceOrder {
    let expected = expected();
    let transition = if new_reservation {
        let mut token = ReservationToken {
            token_id: 8,
            command_id: expected.command_id,
            reservation_id: ReservationId::new(109),
            order_id: expected.new_order_id,
            user_id: expected.user_id,
            market_id: expected.market_id,
            reserves: expected.required_reserves.clone().into(),
            fee_schedule_version: 1,
            fee_tier_id: 2,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = SigningKey::from_bytes(&[7; 32])
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        ReservationTransition::ReplaceWithNew {
            new_reservation: token,
        }
    } else {
        ReservationTransition::ReuseOld { adjustment: None }
    };
    CancelReplaceOrder {
        market_id: expected.market_id,
        user_id: expected.user_id,
        old_order_id: expected.old_order_id,
        replacement: ReplacementOrder {
            market_id: expected.market_id,
            user_id: expected.user_id,
            order_id: expected.new_order_id,
            side,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
            kind: ReplacementOrderKind::Limit {
                tif,
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            },
        },
        reservation_transition: transition,
    }
}

fn context<'a>(math: &'a MarketMath, ids: &'a [TradeId]) -> ReferencePlaceContext<'a> {
    ReferencePlaceContext {
        math,
        now: LogicalTime::new(10),
        current_session_id: 7,
        trade_ids: ids,
    }
}

fn assert_parity(book: &ReferenceOrderBook, registry: &ReservationRegistry) {
    registry
        .validate_order_bindings(book.reservations.iter().map(|(id, state)| (*id, state)))
        .unwrap();
}

#[test]
fn cancel_replace_plan_is_pure_and_rested_new_identity_loses_old_priority() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new_reservation in [false, true] {
        let mut book = ReferenceOrderBook::new();
        for id in [1, 2] {
            book.rest_with_reservation(order(id, Side::Buy, 5), state(id))
                .unwrap();
        }
        let mut registry = ReservationRegistry::new([state(1), state(2)]).unwrap();
        let before = (book.clone(), registry.clone());
        let request = command(new_reservation, Side::Buy, 5, TimeInForce::Gtc);
        let first = book
            .plan_cancel_replace(
                &request,
                &expected(),
                &registry,
                &keys(),
                context(&math, &[]),
            )
            .unwrap();
        let second = book
            .plan_cancel_replace(
                &request,
                &expected(),
                &registry,
                &keys(),
                context(&math, &[]),
            )
            .unwrap();
        assert_eq!(first, second);
        assert_eq!((book.clone(), registry.clone()), before);
        let events = book.apply_cancel_replace(&mut registry, first).unwrap();
        assert_eq!(
            events,
            vec![EngineEvent::OrderCanceled {
                order_id: OrderId::new(1),
                remaining: matching_domain::OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: CancelReason::CancelReplace,
            }]
        );
        assert_eq!(
            book.orders(Side::Buy)
                .iter()
                .map(|o| (o.original_order().order_id, o.priority()))
                .collect::<Vec<_>>(),
            vec![
                (OrderId::new(2), QueuePriority::new(1)),
                (OrderId::new(9), QueuePriority::new(2))
            ]
        );
        let active = book.reservation_state(OrderId::new(9)).unwrap();
        assert_eq!(
            active.reservation_id,
            ReservationId::new(if new_reservation { 109 } else { 101 })
        );
        assert_eq!(active.version, if new_reservation { 0 } else { 4 });
        assert_eq!(
            registry.is_pending_r2(state(1).reservation_id),
            new_reservation
        );
        if new_reservation {
            assert_eq!(registry.get(state(1).reservation_id), Some(&state(1)));
        }
        assert_parity(&book, &registry);
    }
}

#[test]
fn cancel_replace_orders_old_cancel_before_normal_place_and_terminal_batch() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let ids = [TradeId::new(1), TradeId::new(2)];
    for new_reservation in [false, true] {
        for tif in [TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Fok] {
            let mut book = ReferenceOrderBook::new();
            book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
                .unwrap();
            for (id, qty) in [(2, 2), (3, 3)] {
                book.rest_with_reservation(order(id, Side::Sell, qty), state(id))
                    .unwrap();
            }
            let mut registry = ReservationRegistry::new([state(1), state(2), state(3)]).unwrap();
            let request = command(new_reservation, Side::Buy, 5, tif);
            let plan = book
                .plan_cancel_replace(
                    &request,
                    &expected(),
                    &registry,
                    &keys(),
                    context(&math, &ids),
                )
                .unwrap();
            let events = book.apply_cancel_replace(&mut registry, plan).unwrap();
            assert!(
                matches!(events.first(), Some(EngineEvent::OrderCanceled {order_id, reason: CancelReason::CancelReplace, ..}) if *order_id == OrderId::new(1))
            );
            let makers = events
                .iter()
                .filter_map(|event| {
                    if let EngineEvent::Trade(t) = event {
                        Some(t.maker_order_id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(makers, vec![OrderId::new(2), OrderId::new(3)]);
            assert!(!events.iter().any(|event| matches!(
                event,
                EngineEvent::ReservationReleased { .. } | EngineEvent::CommandRejected { .. }
            )));
            for id in [101, 102, 103] {
                assert!(registry.is_pending_r2(ReservationId::new(id)));
            }
            if new_reservation {
                assert!(registry.is_pending_r2(ReservationId::new(109)));
            }
            assert_eq!(registry.get(state(2).reservation_id), Some(&state(2)));
            assert_eq!(book.next_priority, 3);
            assert_parity(&book, &registry);
        }
    }
}

#[test]
fn cancel_replace_fok_excludes_entire_old_regular_and_iceberg_identity() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for iceberg in [false, true] {
        let mut book = ReferenceOrderBook::new();
        if iceberg {
            book.place_iceberg_post_only_with_reservation(
                IcebergOrder::try_new(order(1, Side::Sell, 10), Qty::try_new(2).unwrap()).unwrap(),
                &math,
                state(1),
            )
            .unwrap();
        } else {
            book.rest_with_reservation(order(1, Side::Sell, 5), state(1))
                .unwrap();
        }
        book.rest_with_reservation(order(2, Side::Sell, 5), state(2))
            .unwrap();
        let registry = ReservationRegistry::new([state(1), state(2)]).unwrap();
        let before = (book.clone(), registry.clone());
        let error = book
            .plan_cancel_replace(
                &command(false, Side::Buy, 10, TimeInForce::Fok),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[TradeId::new(1)]),
            )
            .unwrap_err();
        assert_eq!(error.reason, RejectReason::FokInsufficientLiquidity);
        assert_eq!((book, registry), before);
    }
}

#[test]
fn cancel_replace_post_only_uses_best_after_excluding_old() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 5), state(1))
        .unwrap();
    let mut registry = ReservationRegistry::new([state(1)]).unwrap();
    let mut request = command(false, Side::Buy, 5, TimeInForce::Gtc);
    request.replacement.kind = ReplacementOrderKind::Limit {
        tif: TimeInForce::Gtc,
        flags: OrderFlags {
            post_only: true,
            reduce_only: false,
        },
    };
    let plan = book
        .plan_cancel_replace(
            &request,
            &expected(),
            &registry,
            &keys(),
            context(&math, &[]),
        )
        .unwrap();
    book.apply_cancel_replace(&mut registry, plan).unwrap();
    assert_eq!(
        book.best(Side::Buy).unwrap().priority(),
        QueuePriority::new(1)
    );
    assert_parity(&book, &registry);
}

#[test]
fn cancel_replace_identity_structure_duplicate_and_token_rejections_are_atomic() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for scenario in 0..9 {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
            .unwrap();
        book.rest_with_reservation(order(2, Side::Buy, 5), state(2))
            .unwrap();
        let registry = ReservationRegistry::new([state(1), state(2)]).unwrap();
        let mut request = command(true, Side::Buy, 5, TimeInForce::Gtc);
        let mut e = expected();
        let reason = match scenario {
            0 => {
                request.old_order_id = OrderId::new(88);
                RejectReason::OrderNotFound
            }
            1 => {
                request.user_id = UserId::new(88);
                RejectReason::OrderNotFound
            }
            2 => {
                request.market_id = MarketId::new(88);
                RejectReason::OrderNotFound
            }
            3 => {
                request.replacement.user_id = UserId::new(88);
                RejectReason::InvalidOrderCombination
            }
            4 => {
                request.replacement.order_id = OrderId::new(1);
                RejectReason::DuplicateOrderId
            }
            5 => {
                request.replacement.order_id = OrderId::new(2);
                RejectReason::DuplicateOrderId
            }
            6 => {
                request.replacement.kind = ReplacementOrderKind::Limit {
                    tif: TimeInForce::Ioc,
                    flags: OrderFlags {
                        post_only: true,
                        reduce_only: false,
                    },
                };
                RejectReason::InvalidOrderCombination
            }
            7 => {
                if let ReservationTransition::ReplaceWithNew { new_reservation } =
                    &mut request.reservation_transition
                {
                    new_reservation.signature[0] ^= 1;
                }
                RejectReason::InvalidReservation
            }
            8 => {
                e.expected_version += 1;
                RejectReason::InvalidReservation
            }
            _ => unreachable!(),
        };
        let before = (book.clone(), registry.clone());
        let error = book
            .plan_cancel_replace(&request, &e, &registry, &keys(), context(&math, &[]))
            .unwrap_err();
        assert_eq!(error.reason, reason, "scenario {scenario}");
        assert_eq!(
            error.unadopted_reservation.unwrap().reservation_id,
            ReservationId::new(109)
        );
        assert_eq!((book, registry), before, "scenario {scenario}");
    }
}

#[test]
fn cancel_replace_apply_rejects_whole_book_or_registry_drift_before_writes() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for drift_registry in [false, true] {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
            .unwrap();
        let mut registry = ReservationRegistry::new([state(1)]).unwrap();
        let plan = book
            .plan_cancel_replace(
                &command(false, Side::Buy, 5, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[]),
            )
            .unwrap();
        if drift_registry {
            let change = registry.plan_binding_transitions([&state(1)], []).unwrap();
            registry.apply_binding_transition(change).unwrap();
        } else {
            book.next_stop_priority += 1;
        }
        let before = (book.clone(), registry.clone());
        assert_eq!(
            book.apply_cancel_replace(&mut registry, plan),
            Err(ReservationApplyError::StateChanged)
        );
        assert_eq!((book, registry), before);
    }
}

#[test]
fn cancel_replace_stop_limit_loses_stop_expiry_and_allocates_only_regular_priority() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.place_stop_with_reservation(
        StopOrder::Limit(StopLimitOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(5),
            reservation_id: state(1).reservation_id,
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(110).unwrap(),
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(5).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
            trigger_expiry: TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(30),
            },
        }),
        LogicalTime::new(1),
        7,
        state(1),
    )
    .unwrap();
    let mut registry = ReservationRegistry::new([state(1)]).unwrap();
    let plan = book
        .plan_cancel_replace(
            &command(false, Side::Buy, 5, TimeInForce::Gtc),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[]),
        )
        .unwrap();
    book.apply_cancel_replace(&mut registry, plan).unwrap();
    assert!(book.stop_order(OrderId::new(1)).is_none());
    assert_eq!(book.next_stop_priority, 1);
    assert_eq!(
        book.best(Side::Buy).unwrap().priority(),
        QueuePriority::new(0)
    );
    assert_parity(&book, &registry);
}

#[test]
fn cancel_replace_partial_and_empty_ioc_keep_exact_event_order_and_pending_financial_snapshots() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for maker_qty in [0, 2, 8] {
            let mut book = ReferenceOrderBook::new();
            book.rest_with_reservation(order(1, side, 5), state(1))
                .unwrap();
            let mut states = vec![state(1)];
            if maker_qty != 0 {
                let opposite = if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                };
                book.rest_with_reservation(order(2, opposite, maker_qty), state(2))
                    .unwrap();
                states.push(state(2));
            }
            let mut registry = ReservationRegistry::new(states).unwrap();
            let plan = book
                .plan_cancel_replace(
                    &command(true, side, 5, TimeInForce::Ioc),
                    &expected(),
                    &registry,
                    &keys(),
                    context(&math, &[TradeId::new(71)]),
                )
                .unwrap();
            let events = book.apply_cancel_replace(&mut registry, plan).unwrap();
            let mut expected_events = vec![EngineEvent::OrderCanceled {
                order_id: OrderId::new(1),
                remaining: matching_domain::OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: CancelReason::CancelReplace,
            }];
            if maker_qty != 0 {
                expected_events.push(EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(71),
                    maker_order_id: OrderId::new(2),
                    taker_order_id: OrderId::new(9),
                    maker_side: if side == Side::Buy {
                        Side::Sell
                    } else {
                        Side::Buy
                    },
                    price: Price::try_new(100).unwrap(),
                    qty: Qty::try_new(maker_qty.min(5)).unwrap(),
                }));
            }
            if maker_qty < 5 {
                expected_events.push(EngineEvent::OrderCanceled {
                    order_id: OrderId::new(9),
                    remaining: matching_domain::OrderRemaining::Base(
                        Qty::try_new(5 - maker_qty).unwrap(),
                    ),
                    reason: CancelReason::IocRemainder,
                });
            }
            assert_eq!(events, expected_events);
            assert!(registry.is_pending_r2(ReservationId::new(101)));
            assert!(registry.is_pending_r2(ReservationId::new(109)));
            assert_eq!(registry.get(ReservationId::new(101)), Some(&state(1)));
            assert_eq!(
                registry.is_pending_r2(ReservationId::new(102)),
                maker_qty == 2
            );
            if maker_qty != 0 {
                assert_eq!(registry.get(ReservationId::new(102)), Some(&state(2)));
            }
            assert_eq!(book.next_priority, if maker_qty == 0 { 1 } else { 2 });
            assert_parity(&book, &registry);
        }
    }
}

#[test]
fn cancel_replace_gtc_partial_rest_and_iceberg_replacement_preserve_real_normal_place_state() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for iceberg in [false, true] {
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Day { session_id: 7 },
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(20),
            },
        ] {
            if iceberg && tif != TimeInForce::Gtc {
                continue;
            }
            let mut book = ReferenceOrderBook::new();
            book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
                .unwrap();
            book.rest_with_reservation(order(2, Side::Sell, 2), state(2))
                .unwrap();
            let mut registry = ReservationRegistry::new([state(1), state(2)]).unwrap();
            let mut request = command(false, Side::Buy, 5, tif);
            if iceberg {
                request.replacement.kind = ReplacementOrderKind::Iceberg {
                    peak_size: Qty::try_new(2).unwrap(),
                    tif,
                    flags: OrderFlags {
                        post_only: false,
                        reduce_only: false,
                    },
                };
            }
            let plan = book
                .plan_cancel_replace(
                    &request,
                    &expected(),
                    &registry,
                    &keys(),
                    context(&math, &[TradeId::new(1)]),
                )
                .unwrap();
            let events = book.apply_cancel_replace(&mut registry, plan).unwrap();
            assert!(matches!(
                events.first(),
                Some(EngineEvent::OrderCanceled {
                    reason: CancelReason::CancelReplace,
                    ..
                })
            ));
            let residual = book.best(Side::Buy).unwrap();
            assert_eq!(residual.original_order().qty.get(), 5);
            assert_eq!(residual.total_remaining(), 3);
            assert_eq!(residual.priority(), QueuePriority::new(2));
            assert_eq!(residual.time_in_force(), tif);
            if iceberg {
                assert_eq!(residual.remaining(), 2);
                assert_eq!(residual.hidden_remaining(), 1);
            }
            assert!(registry.is_pending_r2(ReservationId::new(102)));
            assert!(!registry.is_pending_r2(ReservationId::new(101)));
            assert_parity(&book, &registry);
        }
    }
}

#[test]
fn cancel_replace_iceberg_old_cancel_reports_total_and_excludes_hidden_and_expiry() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg_post_only_with_reservation(
        IcebergOrder::try_new(order(1, Side::Sell, 10), Qty::try_new(2).unwrap()).unwrap(),
        &math,
        state(1),
    )
    .unwrap();
    let mut registry = ReservationRegistry::new([state(1)]).unwrap();
    let plan = book
        .plan_cancel_replace(
            &command(true, Side::Buy, 5, TimeInForce::Ioc),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[]),
        )
        .unwrap();
    let events = book.apply_cancel_replace(&mut registry, plan).unwrap();
    assert_eq!(
        events,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(1),
                remaining: matching_domain::OrderRemaining::Base(Qty::try_new(10).unwrap()),
                reason: CancelReason::CancelReplace
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(9),
                remaining: matching_domain::OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: CancelReason::IocRemainder
            },
        ]
    );
    assert_eq!(book.next_priority, 1);
    assert!(book.orders.is_empty());
    assert_parity(&book, &registry);
}

#[test]
fn cancel_replace_registry_parity_and_shadow_fail_closed_without_any_candidate_commit() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for scenario in 0..5 {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
            .unwrap();
        let registry = ReservationRegistry::new(if scenario == 0 {
            vec![state(1), state(2)]
        } else {
            vec![state(1)]
        })
        .unwrap();
        match scenario {
            0 => {}
            1 => {
                book.reservations.remove(&OrderId::new(1));
            }
            2 => {
                book.reservations.get_mut(&OrderId::new(1)).unwrap().status =
                    ReservationStatus::Released;
            }
            3 => {
                book.reservations
                    .get_mut(&OrderId::new(1))
                    .unwrap()
                    .market_id = MarketId::new(999);
            }
            4 => {
                book.reservations
                    .get_mut(&OrderId::new(1))
                    .unwrap()
                    .reserves[0]
                    .atoms += 1;
            }
            _ => unreachable!(),
        }
        let before = (book.clone(), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(
                &command(false, Side::Buy, 5, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[])
            )
            .unwrap_err()
            .reason,
            RejectReason::InvalidReservation
        );
        assert_eq!((book, registry), before);
    }
}

#[test]
fn cancel_replace_maker_missing_active_binding_is_rejected_before_old_removal() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
        .unwrap();
    book.rest(order(2, Side::Sell, 5)).unwrap();
    let registry = ReservationRegistry::new([state(1)]).unwrap();
    let before = (book.clone(), registry.clone());
    assert_eq!(
        book.plan_cancel_replace(
            &command(false, Side::Buy, 5, TimeInForce::Fok),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[TradeId::new(1)])
        )
        .unwrap_err()
        .reason,
        RejectReason::InvalidReservation
    );
    assert_eq!((book, registry), before);
}

#[test]
fn cancel_replace_expiry_math_priority_and_insufficient_reserves_reject_atomically() {
    for scenario in 0..7 {
        let mut book = ReferenceOrderBook::new();
        book.rest_timed_with_reservation(
            order(1, Side::Buy, 5),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(30),
            },
            LogicalTime::new(1),
            7,
            state(1),
        )
        .unwrap();
        let registry = ReservationRegistry::new([state(1)]).unwrap();
        let mut request = command(false, Side::Buy, 5, TimeInForce::Gtc);
        let mut e = expected();
        let mut tick = 1;
        let mut lot = 1;
        let reason = match scenario {
            0 => {
                request.replacement.kind = ReplacementOrderKind::Limit {
                    tif: TimeInForce::Gtd {
                        expire_at: LogicalTime::new(10),
                    },
                    flags: OrderFlags {
                        post_only: false,
                        reduce_only: false,
                    },
                };
                RejectReason::InvalidExpireTime
            }
            1 => {
                request.replacement.kind = ReplacementOrderKind::Limit {
                    tif: TimeInForce::Day { session_id: 8 },
                    flags: OrderFlags {
                        post_only: false,
                        reduce_only: false,
                    },
                };
                RejectReason::InvalidSession
            }
            2 => {
                tick = 3;
                RejectReason::InvalidTick
            }
            3 => {
                lot = 2;
                RejectReason::InvalidLot
            }
            4 => {
                book.next_priority = u128::MAX - PRIORITY_GUARD;
                RejectReason::PrioritySpaceExhaustion
            }
            5 => {
                e.required_reserves[0].atoms = 10_001;
                RejectReason::InsufficientReservation
            }
            6 => {
                e.expected_version = u64::MAX;
                book.reservations.get_mut(&OrderId::new(1)).unwrap().version = u64::MAX;
                RejectReason::ArithmeticOverflow
            }
            _ => unreachable!(),
        };
        let registry = if scenario == 6 {
            ReservationRegistry::new([book.reservations[&OrderId::new(1)].clone()]).unwrap()
        } else {
            registry
        };
        let math = MarketMath::try_new(0, 0, 0, tick, lot).unwrap();
        let before = (book.clone(), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(&request, &e, &registry, &keys(), context(&math, &[]))
                .unwrap_err()
                .reason,
            reason,
            "scenario {scenario}"
        );
        assert_eq!((book, registry), before);
    }
}

#[test]
fn cancel_replace_book_dependent_reject_precedes_transition_verification() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 5), state(1))
        .unwrap();
    let registry = ReservationRegistry::new([state(1)]).unwrap();
    let mut request = command(true, Side::Buy, 5, TimeInForce::Fok);
    if let ReservationTransition::ReplaceWithNew { new_reservation } =
        &mut request.reservation_transition
    {
        new_reservation.signature[0] ^= 1;
    }
    let before = (book.clone(), registry.clone());
    assert_eq!(
        book.plan_cancel_replace(
            &request,
            &expected(),
            &registry,
            &keys(),
            context(&math, &[])
        )
        .unwrap_err()
        .reason,
        RejectReason::FokInsufficientLiquidity
    );
    assert_eq!((book, registry), before);
}

#[test]
fn cancel_replace_signed_reuse_adjustment_is_old_bound_and_terminal_replacement_is_pending() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for valid in [false, true] {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
            .unwrap();
        let mut registry = ReservationRegistry::new([state(1)]).unwrap();
        let mut e = expected();
        e.required_reserves[0].atoms = 15_000;
        let mut token = matching_domain::ReservationAdjustmentToken {
            token_id: 50,
            command_id: e.command_id,
            reservation_id: e.old_reservation_id,
            order_id: e.old_order_id,
            user_id: e.user_id,
            market_id: e.market_id,
            expected_reservation_version: e.expected_version,
            target_reserves: vec![AssetReserve {
                asset: AssetId::new(1),
                atoms: 20_000,
            }]
            .into(),
            fee_schedule_version: e.fee_schedule_version,
            fee_tier_id: e.fee_tier_id,
            risk_key_id: 31,
            signature: [0; 64],
        };
        if !valid {
            token.order_id = OrderId::new(9);
        }
        token.signature = SigningKey::from_bytes(&[7; 32])
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let mut request = command(false, Side::Buy, 5, TimeInForce::Ioc);
        request.reservation_transition = ReservationTransition::ReuseOld {
            adjustment: Some(token),
        };
        let before = (book.clone(), registry.clone());
        let result =
            book.plan_cancel_replace(&request, &e, &registry, &keys(), context(&math, &[]));
        if valid {
            let events = book
                .apply_cancel_replace(&mut registry, result.unwrap())
                .unwrap();
            assert_eq!(events.len(), 2);
            assert!(
                events
                    .iter()
                    .all(|event| matches!(event, EngineEvent::OrderCanceled { .. }))
            );
            let target = registry.get(e.old_reservation_id).unwrap();
            assert_eq!(target.order_id, e.new_order_id);
            assert_eq!(target.version, 4);
            assert_eq!(target.reserves[0].atoms, 20_000);
            assert_eq!(target.status, ReservationStatus::Active);
            assert!(registry.is_pending_r2(e.old_reservation_id));
            assert_parity(&book, &registry);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.reason, RejectReason::InvalidReservation);
            assert!(error.unadopted_reservation.is_none());
            assert_eq!((book, registry), before);
        }
    }
}

#[test]
fn cancel_replace_new_reservation_keeps_max_version_old_financial_state_and_reapply_halts() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut old = state(1);
    old.version = u64::MAX;
    let mut e = expected();
    e.expected_version = u64::MAX;
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Buy, 5), old.clone())
        .unwrap();
    let mut registry = ReservationRegistry::new([old.clone()]).unwrap();
    let plan = book
        .plan_cancel_replace(
            &command(true, Side::Buy, 5, TimeInForce::Gtc),
            &e,
            &registry,
            &keys(),
            context(&math, &[]),
        )
        .unwrap();
    book.apply_cancel_replace(&mut registry, plan.clone())
        .unwrap();
    assert_eq!(registry.get(old.reservation_id), Some(&old));
    assert!(registry.is_pending_r2(old.reservation_id));
    let before_reapply = (book.clone(), registry.clone());
    assert_eq!(
        book.apply_cancel_replace(&mut registry, plan),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!((book, registry), before_reapply);
}

#[test]
fn cancel_replace_rejects_both_stop_market_old_modes_without_changing_stop_or_expiry() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for quantity in [
        matching_domain::StopMarketQuantity::Base(Qty::try_new(5).unwrap()),
        matching_domain::StopMarketQuantity::Quote(
            matching_domain::QuoteBudget::try_new(500).unwrap(),
        ),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.place_stop_with_reservation(
            StopOrder::Market(matching_domain::StopMarketOrder {
                order_id: OrderId::new(1),
                user_id: UserId::new(5),
                reservation_id: state(1).reservation_id,
                fee_schedule_version: 1,
                side: Side::Buy,
                stop_price: Price::try_new(110).unwrap(),
                quantity,
                child_tif: TimeInForce::Ioc,
                market_protection: None,
                trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
                trigger_expiry: TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(30),
                },
            }),
            LogicalTime::new(1),
            7,
            state(1),
        )
        .unwrap();
        let registry = ReservationRegistry::new([state(1)]).unwrap();
        let before = (book.clone(), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(
                &command(false, Side::Buy, 5, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[])
            )
            .unwrap_err()
            .reason,
            RejectReason::InvalidOrderCombination
        );
        assert_eq!((book, registry), before);
    }
}

fn assert_schema_precedes_transition(scenario: usize) {
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Buy, 5), state(1))
        .unwrap();
    book.rest_with_reservation(order(2, Side::Sell, 5), state(2))
        .unwrap();
    let registry = ReservationRegistry::new([state(1), state(2)]).unwrap();
    let mut request = command(true, Side::Buy, 6, TimeInForce::Gtc);
    if let ReservationTransition::ReplaceWithNew { new_reservation } =
        &mut request.reservation_transition
    {
        new_reservation.signature[0] ^= 1;
    }
    let mut math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let reason = match scenario {
        0 => {
            request.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Gtd {
                    expire_at: LogicalTime::new(10),
                },
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidExpireTime
        }
        1 => {
            request.replacement.kind = ReplacementOrderKind::Limit {
                tif: TimeInForce::Day { session_id: 8 },
                flags: OrderFlags {
                    post_only: false,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidSession
        }
        2 => {
            math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
            request.replacement.kind = ReplacementOrderKind::Iceberg {
                peak_size: Qty::try_new(3).unwrap(),
                tif: TimeInForce::Gtc,
                flags: OrderFlags {
                    post_only: true,
                    reduce_only: false,
                },
            };
            RejectReason::InvalidLot
        }
        3 | 4 => {
            math = MarketMath::try_new(0, 0, 36, 1, 1).unwrap();
            request.replacement.price = Price::try_new(10_000).unwrap();
            if scenario == 4 {
                request.replacement.kind = ReplacementOrderKind::Limit {
                    tif: TimeInForce::Gtc,
                    flags: OrderFlags {
                        post_only: true,
                        reduce_only: false,
                    },
                };
            }
            RejectReason::ArithmeticOverflow
        }
        _ => unreachable!(),
    };
    let before = (book.clone(), registry.clone());
    assert_eq!(
        book.plan_cancel_replace(
            &request,
            &expected(),
            &registry,
            &keys(),
            context(&math, &[TradeId::new(1)])
        )
        .unwrap_err()
        .reason,
        reason,
        "scenario {scenario}"
    );
    assert_eq!((book, registry), before, "scenario {scenario}");
}

#[test]
fn cancel_replace_schema_preflight_expired_gtd_precedes_invalid_token() {
    assert_schema_precedes_transition(0);
}

#[test]
fn cancel_replace_schema_preflight_invalid_day_precedes_invalid_token() {
    assert_schema_precedes_transition(1);
}

#[test]
fn cancel_replace_schema_preflight_peak_lot_precedes_post_only_cross() {
    assert_schema_precedes_transition(2);
}

#[test]
fn cancel_replace_schema_preflight_notional_precedes_invalid_token() {
    assert_schema_precedes_transition(3);
}

#[test]
fn cancel_replace_schema_preflight_notional_precedes_post_only_cross() {
    assert_schema_precedes_transition(4);
}
