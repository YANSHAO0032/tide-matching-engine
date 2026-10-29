//! 固定支持矩阵和敏感边界先于长磁带执行。
use super::*;

fn initialize(h: &mut Harness, kind: u8, side: Side) {
    let old = input(1, side, if side == Side::Buy { 90 } else { 110 }, 12);
    let command = match kind {
        0 => Command::Rest(old, gtd(100)),
        1 => Command::Iceberg(IcebergOrder::try_new(old, Qty::try_new(3).unwrap()).unwrap()),
        2 => Command::Stop(stop(1, side, 100)),
        _ => unreachable!(),
    };
    assert!(!h.run(&command).outcome.failed());
}

pub(super) fn stop(id: u128, side: Side, at: u64) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id),
        fee_schedule_version: 5,
        trigger_reference: PriceReferenceKind::LastTrade,
        side,
        stop_price: Price::try_new(if side == Side::Buy { 120 } else { 80 }).unwrap(),
        price: Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap(),
        qty: Qty::try_new(12).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(at),
        },
    })
}

#[test]
fn cancel_replace_differential_fixed_supported_matrix() {
    for old in 0..3 {
        for transition in 0..3 {
            for side in [Side::Buy, Side::Sell] {
                for kind in 0..8 {
                    let mut h = Harness::new();
                    initialize(&mut h, old, side);
                    let maker_side = opposite_side(side);
                    for (id, price) in [(2, 100), (3, if side == Side::Buy { 101 } else { 99 })] {
                        h.run(&Command::Rest(
                            input(id, maker_side, price, 6),
                            TimeInForce::Gtc,
                        ));
                    }
                    let cross = matches!(kind, 1 | 2);
                    let price = if cross {
                        if side == Side::Buy { 101 } else { 99 }
                    } else if side == Side::Buy {
                        90
                    } else {
                        110
                    };
                    let strategy = match kind {
                        0 => limit(TimeInForce::Gtc, false),
                        1 => limit(TimeInForce::Ioc, false),
                        2 => limit(TimeInForce::Fok, false),
                        3 => limit(TimeInForce::Gtc, true),
                        4 | 5 => ReplacementOrderKind::Iceberg {
                            peak_size: Qty::try_new(3).unwrap(),
                            tif: TimeInForce::Gtc,
                            flags: flags(kind == 5),
                        },
                        6 => limit(
                            TimeInForce::Day {
                                session_id: SESSION,
                            },
                            false,
                        ),
                        7 => limit(gtd(50), false),
                        _ => unreachable!(),
                    };
                    let record = h.run(&replacement(
                        (1, 9, 1, 3),
                        41,
                        transition,
                        side,
                        price,
                        12,
                        strategy,
                    ));
                    assert!(!record.outcome.failed());
                    assert!(
                        matches!(record.outcome.events().first(), Some(EngineEvent::OrderCanceled { order_id, .. }) if *order_id == OrderId::new(1))
                    );
                    assert!(
                        !record
                            .outcome
                            .events()
                            .iter()
                            .any(|e| matches!(e, EngineEvent::ReservationReleased { .. }))
                    );
                }
            }
        }
    }
}

#[test]
fn cancel_replace_differential_fixed_rejects_keep_full_registry_and_old() {
    for old in 0..3 {
        for transition in 0..3 {
            for side in [Side::Buy, Side::Sell] {
                for failure in 0..8 {
                    let mut h = Harness::new();
                    initialize(&mut h, old, side);
                    let mut command = replacement(
                        (1, 9, 1, 3),
                        41,
                        transition,
                        side,
                        if side == Side::Buy { 90 } else { 110 },
                        12,
                        limit(TimeInForce::Gtc, false),
                    );
                    let Command::CancelReplace { request, expected } = &mut command else {
                        unreachable!()
                    };
                    let reason = match failure {
                        0 => {
                            request.replacement.order_id = OrderId::new(1);
                            RejectReason::DuplicateOrderId
                        }
                        1 => {
                            request.replacement.kind = limit(gtd(1), false);
                            RejectReason::InvalidExpireTime
                        }
                        2 => {
                            request.replacement.kind =
                                limit(TimeInForce::Day { session_id: 8 }, false);
                            RejectReason::InvalidSession
                        }
                        3 => {
                            request.user_id = UserId::new(6);
                            RejectReason::OrderNotFound
                        }
                        4 => {
                            expected.expected_version += 1;
                            RejectReason::InvalidReservation
                        }
                        5 => {
                            request.replacement.kind = limit(TimeInForce::Ioc, true);
                            RejectReason::InvalidOrderCombination
                        }
                        6 => {
                            expected.required_reserves[0].atoms = 20_000;
                            RejectReason::InsufficientReservation
                        }
                        7 => {
                            request.replacement.side = opposite_side(side);
                            request.replacement.kind = limit(TimeInForce::Fok, false);
                            RejectReason::FokInsufficientLiquidity
                        }
                        _ => unreachable!(),
                    };
                    let rejection = request.rejection(reason);
                    assert_eq!(
                        h.run(&command).outcome,
                        Outcome::CancelReplace(Err(rejection))
                    );
                }
            }
        }
    }
}

#[test]
fn cancel_replace_differential_fixed_old_only_fok_and_post_only_filtered_best() {
    for side in [Side::Buy, Side::Sell] {
        for old in 0..3 {
            let mut h = Harness::new();
            initialize(&mut h, old, side);
            let price = if side == Side::Buy { 90 } else { 110 };
            h.run(&Command::Rest(input(2, side, price, 6), TimeInForce::Gtc));
            let command = replacement(
                (1, 9, 1, 3),
                41,
                0,
                opposite_side(side),
                price,
                18,
                limit(TimeInForce::Fok, false),
            );
            let Command::CancelReplace { request, .. } = &command else {
                unreachable!()
            };
            assert_eq!(
                h.run(&command).outcome,
                Outcome::CancelReplace(Err(
                    request.rejection(RejectReason::FokInsufficientLiquidity)
                ))
            );
        }
        for old in 0..2 {
            for crossing in [false, true] {
                let mut h = Harness::new();
                initialize(&mut h, old, side);
                let next_price = if side == Side::Buy { 89 } else { 111 };
                h.run(&Command::Rest(
                    input(2, side, next_price, 6),
                    TimeInForce::Gtc,
                ));
                let price = if crossing {
                    next_price
                } else if side == Side::Buy {
                    90
                } else {
                    110
                };
                let command = replacement(
                    (1, 9, 1, 3),
                    41,
                    2,
                    opposite_side(side),
                    price,
                    12,
                    limit(TimeInForce::Gtc, true),
                );
                let Command::CancelReplace { request, .. } = &command else {
                    unreachable!()
                };
                let record = h.run(&command);
                if crossing {
                    assert_eq!(
                        record.outcome,
                        Outcome::CancelReplace(Err(
                            request.rejection(RejectReason::PostOnlyWouldCross)
                        ))
                    );
                } else {
                    assert!(!record.outcome.failed());
                }
            }
        }
    }
}

#[test]
fn cancel_replace_differential_fixed_signed_rejection_preserves_pending_cleanup_identity() {
    for transition in [1, 2] {
        for side in [Side::Buy, Side::Sell] {
            let mut h = Harness::new();
            initialize(&mut h, 0, side);
            let mut command = replacement(
                (1, 9, 1, 3),
                41,
                transition,
                side,
                if side == Side::Buy { 90 } else { 110 },
                12,
                limit(TimeInForce::Gtc, false),
            );
            let Command::CancelReplace { request, .. } = &mut command else {
                unreachable!()
            };
            match &mut request.reservation_transition {
                ReservationTransition::ReuseOld {
                    adjustment: Some(token),
                } => token.signature[0] ^= 1,
                ReservationTransition::ReplaceWithNew { new_reservation } => {
                    new_reservation.signature[0] ^= 1
                }
                _ => unreachable!(),
            }
            let rejection = request.rejection(RejectReason::InvalidReservation);
            assert_eq!(
                h.run(&command).outcome,
                Outcome::CancelReplace(Err(rejection))
            );
        }
    }
}

#[test]
fn cancel_replace_differential_fixed_true_high_water_requires_priority_only_for_actual_rest() {
    for transition in 0..3 {
        for side in [Side::Buy, Side::Sell] {
            for rest in [false, true] {
                let mut h = Harness::new();
                initialize(&mut h, 0, side);
                let ceiling = h.r.test_exhaust_queue_priority();
                h.p.queue_priority_allocator =
                    crate::QueuePriorityAllocator::try_restore(policy(), ceiling).unwrap();
                let command = replacement(
                    (1, 9, 1, 3),
                    41,
                    transition,
                    side,
                    if side == Side::Buy { 90 } else { 110 },
                    12,
                    limit(
                        if rest {
                            TimeInForce::Gtc
                        } else {
                            TimeInForce::Ioc
                        },
                        false,
                    ),
                );
                let Command::CancelReplace { request, .. } = &command else {
                    unreachable!()
                };
                let record = h.run(&command);
                if rest {
                    assert_eq!(
                        record.outcome,
                        Outcome::CancelReplace(Err(
                            request.rejection(RejectReason::PrioritySpaceExhaustion)
                        ))
                    );
                } else {
                    assert!(!record.outcome.failed());
                }
                assert_eq!(h.p.next_queue_priority(), ceiling);
            }
        }
    }
}

#[test]
fn cancel_replace_differential_fixed_partial_regular_and_replenished_iceberg_old() {
    for old in 0..2 {
        for transition in 0..3 {
            for side in [Side::Buy, Side::Sell] {
                let mut h = Harness::new();
                initialize(&mut h, old, side);
                let price = if side == Side::Buy { 90 } else { 110 };
                assert!(
                    !h.run(&Command::Place(input(2, opposite_side(side), price, 4)))
                        .outcome
                        .failed()
                );
                assert_eq!(h.p.order(OrderId::new(1)).unwrap().total_remaining(), 8);
                let record = h.run(&replacement(
                    (1, 9, 1, 3),
                    41,
                    transition,
                    side,
                    price,
                    12,
                    limit(TimeInForce::Gtc, false),
                ));
                assert!(!record.outcome.failed());
                assert!(
                    matches!(record.outcome.events().first(), Some(EngineEvent::OrderCanceled { order_id, remaining: matching_domain::OrderRemaining::Base(remaining), .. }) if *order_id == OrderId::new(1) && remaining.get() == 8)
                );
            }
        }
    }
}
