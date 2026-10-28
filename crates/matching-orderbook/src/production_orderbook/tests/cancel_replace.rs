//! 生产撤单替换的物理状态、事件原序与终结归属回归。

mod atomicity;
mod constraints;

use super::amend_stop::production_snapshot;
use super::*;
use crate::production_orderbook::ProductionCancelReplaceContext;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, CancelReason, CancelReplaceOrder, CommandId, IcebergOrder, LogicalTime,
    MarketId, OrderFlags, ReplacementOrder, ReplacementOrderKind, ReservationId, ReservationToken,
    ReservationTransition, StopLimitOrder, StopOrder, TimeInForce, TriggerExpiry,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationApplyError, ReservationRegistry, ReservationState,
    ReservationStatus, RiskKeySet,
};

fn input(id: u128, side: Side, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn reserve(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(100 + id),
        order_id: OrderId::new(id),
        user_id: UserId::new(5),
        market_id: MarketId::new(7),
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
        old_reservation_id: reserve(1).reservation_id,
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

fn request(new: bool, side: Side, qty: u64, tif: TimeInForce) -> CancelReplaceOrder {
    let e = expected();
    let reservation_transition = if new {
        let mut token = ReservationToken {
            token_id: 8,
            command_id: e.command_id,
            reservation_id: ReservationId::new(109),
            order_id: e.new_order_id,
            user_id: e.user_id,
            market_id: e.market_id,
            reserves: e.required_reserves.clone().into(),
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
        market_id: e.market_id,
        user_id: e.user_id,
        old_order_id: e.old_order_id,
        replacement: ReplacementOrder {
            market_id: e.market_id,
            user_id: e.user_id,
            order_id: e.new_order_id,
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
        reservation_transition,
    }
}

fn context<'a>(
    math: &'a MarketMath,
    trade_ids: &'a [TradeId],
) -> ProductionCancelReplaceContext<'a> {
    ProductionCancelReplaceContext {
        math,
        now: LogicalTime::new(10),
        current_session_id: 7,
        trade_ids,
    }
}

fn parity(book: &ProductionOrderBook, registry: &ReservationRegistry) {
    registry
        .validate_order_bindings(book.reservations.iter().map(|(id, state)| (*id, state)))
        .unwrap();
    book.validate_regular_expiry().unwrap();
    book.validate_stop_book().unwrap();
}

#[test]
fn production_cancel_replace_plan_is_pure_and_new_identity_joins_fifo_tail() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new in [false, true] {
        for old_position in [0, 1, 2] {
            let mut book = ProductionOrderBook::new(policy());
            let mut ids = vec![2, 3];
            ids.insert(old_position, 1);
            for id in ids {
                book.rest_with_reservation(input(id, Side::Buy, 5), reserve(id))
                    .unwrap();
            }
            let mut registry =
                ReservationRegistry::new([reserve(1), reserve(2), reserve(3)]).unwrap();
            let before = (production_snapshot(&book), registry.clone());
            let r = request(new, Side::Buy, 5, TimeInForce::Gtc);
            let first = book
                .plan_cancel_replace(&r, &expected(), &registry, &keys(), context(&math, &[]))
                .unwrap();
            let second = book
                .plan_cancel_replace(&r, &expected(), &registry, &keys(), context(&math, &[]))
                .unwrap();
            assert_eq!(first, second);
            assert_eq!((production_snapshot(&book), registry.clone()), before);
            assert_eq!(
                book.apply_cancel_replace(&mut registry, first).unwrap(),
                vec![EngineEvent::OrderCanceled {
                    order_id: OrderId::new(1),
                    remaining: Qty::try_new(5).unwrap(),
                    reason: CancelReason::CancelReplace
                }]
            );
            let nodes = ordered_nodes(&book, Side::Buy);
            assert_eq!(
                nodes
                    .iter()
                    .map(|node| node.original_order().order_id)
                    .collect::<Vec<_>>(),
                vec![OrderId::new(2), OrderId::new(3), OrderId::new(9)]
            );
            assert_eq!(
                book.order(OrderId::new(9)).unwrap().priority(),
                QueuePriority::new(3)
            );
            assert_eq!(registry.is_pending_r2(ReservationId::new(101)), new);
            let state = book.reservation_state(OrderId::new(9)).unwrap();
            assert_eq!(
                state.reservation_id,
                ReservationId::new(if new { 109 } else { 101 })
            );
            assert_eq!(state.version, if new { 0 } else { 4 });
            if new {
                assert_eq!(registry.get(ReservationId::new(101)), Some(&reserve(1)));
            }
            parity(&book, &registry);
        }
    }
}

fn ordered_nodes(book: &ProductionOrderBook, side: Side) -> Vec<&OrderNode> {
    let levels = if side == Side::Buy {
        &book.bids
    } else {
        &book.asks
    };
    let mut nodes = Vec::new();
    for level in levels.values() {
        let mut cursor = level.head();
        while let Some(index) = cursor {
            let node = book.arena.get(index).unwrap();
            nodes.push(node);
            cursor = node.next();
        }
    }
    nodes
}

#[test]
fn production_cancel_replace_fok_excludes_whole_old_regular_or_iceberg() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for iceberg in [false, true] {
        for side in [Side::Buy, Side::Sell] {
            let mut book = ProductionOrderBook::new(policy());
            if iceberg {
                book.place_iceberg_post_only_with_reservation(
                    IcebergOrder::try_new(input(1, side, 10), Qty::try_new(2).unwrap()).unwrap(),
                    &math,
                    reserve(1),
                )
                .unwrap();
            } else {
                book.rest_with_reservation(input(1, side, 5), reserve(1))
                    .unwrap();
            }
            book.rest_with_reservation(input(2, side, 5), reserve(2))
                .unwrap();
            let registry = ReservationRegistry::new([reserve(1), reserve(2)]).unwrap();
            let before = (production_snapshot(&book), registry.clone());
            let opposite = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            assert_eq!(
                book.plan_cancel_replace(
                    &request(false, opposite, 10, TimeInForce::Fok),
                    &expected(),
                    &registry,
                    &keys(),
                    context(&math, &[TradeId::new(1); 10])
                )
                .unwrap_err()
                .reason,
                RejectReason::FokInsufficientLiquidity
            );
            assert_eq!((production_snapshot(&book), registry), before);
        }
    }
}

#[test]
fn production_cancel_replace_post_only_filters_exclusive_old_level() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for iceberg in [false, true] {
            let mut book = ProductionOrderBook::new(policy());
            let opposite = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            book.rest_with_reservation(input(1, opposite, 5), reserve(1))
                .unwrap();
            let mut registry = ReservationRegistry::new([reserve(1)]).unwrap();
            let mut r = request(false, side, 5, TimeInForce::Gtc);
            if let ReplacementOrderKind::Limit { flags, .. } = &mut r.replacement.kind {
                flags.post_only = true;
            }
            if iceberg {
                r.replacement.kind = ReplacementOrderKind::Iceberg {
                    peak_size: Qty::try_new(2).unwrap(),
                    tif: TimeInForce::Gtc,
                    flags: OrderFlags {
                        post_only: true,
                        reduce_only: false,
                    },
                };
            }
            let p = book
                .plan_cancel_replace(&r, &expected(), &registry, &keys(), context(&math, &[]))
                .unwrap();
            book.apply_cancel_replace(&mut registry, p).unwrap();
            assert_eq!(
                book.order(OrderId::new(9)).unwrap().priority(),
                QueuePriority::new(1)
            );
            assert!(book.order(OrderId::new(1)).is_none());
            parity(&book, &registry);
        }
    }
}

#[test]
fn production_cancel_replace_old_cancel_precedes_every_normal_event_and_terminal_batch() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let ids = [TradeId::new(71), TradeId::new(72)];
    for new in [false, true] {
        for tif in [TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Fok] {
            for side in [Side::Buy, Side::Sell] {
                let maker_side = if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                };
                let mut book = ProductionOrderBook::new(policy());
                for (id, order_side, qty) in [(1, side, 5), (2, maker_side, 2), (3, maker_side, 3)]
                {
                    let mut order = input(id, order_side, qty);
                    if id == 1 {
                        order.price =
                            Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap();
                    }
                    book.rest_with_reservation(order, reserve(id)).unwrap();
                }
                let mut registry =
                    ReservationRegistry::new([reserve(1), reserve(2), reserve(3)]).unwrap();
                let p = book
                    .plan_cancel_replace(
                        &request(new, side, 5, tif),
                        &expected(),
                        &registry,
                        &keys(),
                        context(&math, &ids),
                    )
                    .unwrap();
                let events = book.apply_cancel_replace(&mut registry, p).unwrap();
                assert_eq!(
                    events,
                    vec![
                        EngineEvent::OrderCanceled {
                            order_id: OrderId::new(1),
                            remaining: Qty::try_new(5).unwrap(),
                            reason: CancelReason::CancelReplace
                        },
                        EngineEvent::Trade(TradeEvent {
                            trade_id: ids[0],
                            maker_order_id: OrderId::new(2),
                            taker_order_id: OrderId::new(9),
                            price: Price::try_new(100).unwrap(),
                            qty: Qty::try_new(2).unwrap(),
                            maker_side
                        }),
                        EngineEvent::Trade(TradeEvent {
                            trade_id: ids[1],
                            maker_order_id: OrderId::new(3),
                            taker_order_id: OrderId::new(9),
                            price: Price::try_new(100).unwrap(),
                            qty: Qty::try_new(3).unwrap(),
                            maker_side
                        })
                    ]
                );
                for id in [101, 102, 103] {
                    assert!(registry.is_pending_r2(ReservationId::new(id)));
                }
                if new {
                    assert!(registry.is_pending_r2(ReservationId::new(109)));
                }
                for id in [2, 3] {
                    assert_eq!(registry.get(reserve(id).reservation_id), Some(&reserve(id)));
                }
                assert_eq!(book.next_queue_priority(), 3);
                parity(&book, &registry);
            }
        }
    }
}

#[test]
fn production_cancel_replace_stop_old_cleans_expiry_without_transferring_priority() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new in [false, true] {
        for maker_qty in [0, 2] {
            let mut book = ProductionOrderBook::new(policy());
            book.place_stop_with_reservation(
                StopOrder::Limit(StopLimitOrder {
                    order_id: OrderId::new(1),
                    user_id: UserId::new(5),
                    reservation_id: reserve(1).reservation_id,
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
                reserve(1),
            )
            .unwrap();
            let mut states = vec![reserve(1)];
            if maker_qty != 0 {
                book.rest_with_reservation(input(2, Side::Sell, maker_qty), reserve(2))
                    .unwrap();
                states.push(reserve(2));
            }
            let mut registry = ReservationRegistry::new(states).unwrap();
            let p = book
                .plan_cancel_replace(
                    &request(
                        new,
                        Side::Buy,
                        5,
                        TimeInForce::Gtd {
                            expire_at: LogicalTime::new(40),
                        },
                    ),
                    &expected(),
                    &registry,
                    &keys(),
                    context(&math, &[TradeId::new(1)]),
                )
                .unwrap();
            book.apply_cancel_replace(&mut registry, p).unwrap();
            assert!(book.stop_order(OrderId::new(1)).is_none());
            assert_eq!(book.next_stop_priority(), 1);
            assert_eq!(
                book.order(OrderId::new(9)).unwrap().priority(),
                QueuePriority::new(if maker_qty == 0 { 0 } else { 1 })
            );
            assert!(book.stop_book.test_expiry_entries().is_empty());
            assert_eq!(
                book.regular_expiry.test_entries(),
                vec![(
                    OrderId::new(9),
                    TimeInForce::Gtd {
                        expire_at: LogicalTime::new(40)
                    }
                )]
            );
            parity(&book, &registry);
        }
    }
}

#[test]
fn production_cancel_replace_identity_numeric_time_risk_and_registry_failures_are_atomic() {
    for scenario in 0..16 {
        let mut book = ProductionOrderBook::new(policy());
        book.rest_timed_with_reservation(
            input(1, Side::Buy, 5),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(30),
            },
            LogicalTime::new(1),
            7,
            reserve(1),
        )
        .unwrap();
        let registry = ReservationRegistry::new([reserve(1)]).unwrap();
        let mut r = request(true, Side::Buy, 5, TimeInForce::Gtc);
        let mut e = expected();
        let mut math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let reason = match scenario {
            0 => {
                r.old_order_id = OrderId::new(99);
                RejectReason::OrderNotFound
            }
            1 => {
                r.user_id = UserId::new(99);
                RejectReason::OrderNotFound
            }
            2 => {
                r.market_id = MarketId::new(99);
                RejectReason::OrderNotFound
            }
            3 => {
                r.replacement.user_id = UserId::new(99);
                RejectReason::InvalidOrderCombination
            }
            4 => {
                r.replacement.order_id = r.old_order_id;
                RejectReason::DuplicateOrderId
            }
            5 => {
                e.expected_version += 1;
                RejectReason::InvalidReservation
            }
            6 => {
                book.reservations.remove(&r.old_order_id);
                RejectReason::InvalidReservation
            }
            7 => {
                book.reservations.get_mut(&r.old_order_id).unwrap().reserves[0].atoms += 1;
                RejectReason::InvalidReservation
            }
            8 => {
                math = MarketMath::try_new(0, 0, 0, 3, 1).unwrap();
                RejectReason::InvalidTick
            }
            9 => {
                math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
                RejectReason::InvalidLot
            }
            10 => {
                r.replacement.kind = ReplacementOrderKind::Limit {
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
            11 => {
                r.replacement.kind = ReplacementOrderKind::Limit {
                    tif: TimeInForce::Day { session_id: 8 },
                    flags: OrderFlags {
                        post_only: false,
                        reduce_only: false,
                    },
                };
                RejectReason::InvalidSession
            }
            12 => {
                math = MarketMath::try_new(0, 0, 36, 1, 1).unwrap();
                r.replacement.price = Price::try_new(10_000).unwrap();
                RejectReason::ArithmeticOverflow
            }
            13 => {
                r.replacement.kind = ReplacementOrderKind::Iceberg {
                    peak_size: Qty::try_new(3).unwrap(),
                    tif: TimeInForce::Gtc,
                    flags: OrderFlags {
                        post_only: false,
                        reduce_only: false,
                    },
                };
                r.replacement.qty = Qty::try_new(6).unwrap();
                math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
                RejectReason::InvalidLot
            }
            14 => {
                e.required_reserves[0].atoms = 20_000;
                RejectReason::InsufficientReservation
            }
            15 => {
                e.command_id = CommandId::new(99);
                RejectReason::InvalidReservation
            }
            _ => unreachable!(),
        };
        if scenario < 14
            && let ReservationTransition::ReplaceWithNew { new_reservation } =
                &mut r.reservation_transition
        {
            new_reservation.signature[0] ^= 1;
        }
        let before = (production_snapshot(&book), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(&r, &e, &registry, &keys(), context(&math, &[]))
                .unwrap_err()
                .reason,
            reason,
            "scenario {scenario}"
        );
        assert_eq!(
            (production_snapshot(&book), registry),
            before,
            "scenario {scenario}"
        );
    }
}

#[test]
fn production_cancel_replace_apply_checks_complete_physical_book_and_registry_before_writes() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for scenario in 0..10 {
        let mut book = ProductionOrderBook::new(policy());
        book.rest_with_reservation(input(1, Side::Buy, 5), reserve(1))
            .unwrap();
        let mut registry = ReservationRegistry::new([reserve(1)]).unwrap();
        let p = book
            .plan_cancel_replace(
                &request(false, Side::Buy, 5, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[]),
            )
            .unwrap();
        match scenario {
            0 => {
                book.stop_priority_allocator.allocate().unwrap();
            }
            1 => {
                book.queue_priority_allocator.allocate().unwrap();
            }
            2 => {
                let change = registry
                    .plan_binding_transitions([&reserve(1)], [])
                    .unwrap();
                registry.apply_binding_transition(change).unwrap();
            }
            3 => {
                book.regular_expiry
                    .insert(OrderId::new(1), TimeInForce::Day { session_id: 7 });
            }
            4 => {
                book.rest(input(88, Side::Buy, 1)).unwrap();
                book.cancel(OrderId::new(88)).unwrap();
            }
            5 => {
                book.reservations.get_mut(&OrderId::new(1)).unwrap().version += 1;
            }
            6 => {
                book.cancel(OrderId::new(1)).unwrap();
            }
            7 => {
                book.apply_cancel_replace(&mut registry, p.clone()).unwrap();
            }
            8 => {
                book.reference_prices
                    .advance(matching_domain::PriceReference {
                        kind: matching_domain::PriceReferenceKind::MarkPrice,
                        source_seq: 1,
                        price: Price::try_new(99).unwrap(),
                    })
                    .unwrap();
            }
            9 => {
                book.place_stop(
                    StopOrder::Limit(StopLimitOrder {
                        order_id: OrderId::new(88),
                        user_id: UserId::new(5),
                        reservation_id: ReservationId::new(188),
                        fee_schedule_version: 1,
                        side: Side::Sell,
                        stop_price: Price::try_new(90).unwrap(),
                        price: Price::try_new(100).unwrap(),
                        qty: Qty::try_new(5).unwrap(),
                        child_tif: TimeInForce::Gtc,
                        trigger_reference: matching_domain::PriceReferenceKind::MarkPrice,
                        trigger_expiry: TriggerExpiry::Gtd {
                            expire_at: LogicalTime::new(30),
                        },
                    }),
                    LogicalTime::new(1),
                    7,
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = (production_snapshot(&book), registry.clone());
        assert_eq!(
            book.apply_cancel_replace(&mut registry, p),
            Err(ReservationApplyError::StateChanged)
        );
        assert_eq!((production_snapshot(&book), registry), before);
    }
}

#[test]
fn production_cancel_replace_residual_and_ioc_lifecycle_keep_finances_and_original_total() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new in [false, true] {
        for iceberg in [false, true] {
            for maker_qty in [0, 2, 8] {
                let mut book = ProductionOrderBook::new(policy());
                let mut old = input(1, Side::Buy, 5);
                old.price = Price::try_new(90).unwrap();
                book.rest_with_reservation(old, reserve(1)).unwrap();
                let mut states = vec![reserve(1)];
                if maker_qty != 0 {
                    book.rest_with_reservation(input(2, Side::Sell, maker_qty), reserve(2))
                        .unwrap();
                    states.push(reserve(2));
                }
                let mut registry = ReservationRegistry::new(states).unwrap();
                let mut r = request(
                    new,
                    Side::Buy,
                    5,
                    if iceberg {
                        TimeInForce::Gtc
                    } else {
                        TimeInForce::Ioc
                    },
                );
                if iceberg {
                    r.replacement.kind = ReplacementOrderKind::Iceberg {
                        peak_size: Qty::try_new(2).unwrap(),
                        tif: TimeInForce::Gtc,
                        flags: OrderFlags {
                            post_only: false,
                            reduce_only: false,
                        },
                    };
                }
                let p = book
                    .plan_cancel_replace(
                        &r,
                        &expected(),
                        &registry,
                        &keys(),
                        context(&math, &[TradeId::new(71)]),
                    )
                    .unwrap();
                let events = book.apply_cancel_replace(&mut registry, p).unwrap();
                assert!(
                    matches!(events[0], EngineEvent::OrderCanceled {order_id, remaining, reason: CancelReason::CancelReplace} if order_id == OrderId::new(1) && remaining.get() == 5)
                );
                assert!(!events.iter().any(|e| matches!(
                    e,
                    EngineEvent::ReservationReleased { .. }
                        | EngineEvent::CommandRejected { .. }
                        | EngineEvent::OrderAccepted { .. }
                        | EngineEvent::OrderOpened { .. }
                )));
                let active = iceberg && maker_qty < 5;
                assert_eq!(book.order(OrderId::new(9)).is_some(), active);
                assert_eq!(
                    registry.is_pending_r2(ReservationId::new(if new { 109 } else { 101 })),
                    !active
                );
                if active {
                    let node = book.order(OrderId::new(9)).unwrap();
                    assert_eq!(node.original_order().qty.get(), 5);
                    assert_eq!(node.total_remaining(), 5 - maker_qty);
                }
                if new {
                    assert_eq!(registry.get(ReservationId::new(101)), Some(&reserve(1)));
                }
                if maker_qty != 0 {
                    assert_eq!(registry.get(ReservationId::new(102)), Some(&reserve(2)));
                    assert_eq!(
                        registry.is_pending_r2(ReservationId::new(102)),
                        maker_qty == 2
                    );
                }
                parity(&book, &registry);
            }
        }
    }
}

#[test]
fn production_cancel_replace_priority_exhaustion_only_blocks_actual_rest_or_refresh() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for tif in [TimeInForce::Gtc, TimeInForce::Ioc] {
        for iceberg_maker in [false, true] {
            let mut book = ProductionOrderBook::new(
                QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(2)).unwrap(),
            );
            let mut old = input(1, Side::Buy, 5);
            old.price = Price::try_new(90).unwrap();
            book.rest_with_reservation(old, reserve(1)).unwrap();
            if iceberg_maker {
                book.place_iceberg_post_only_with_reservation(
                    IcebergOrder::try_new(input(2, Side::Sell, 5), Qty::try_new(2).unwrap())
                        .unwrap(),
                    &math,
                    reserve(2),
                )
                .unwrap();
            } else {
                book.rest_with_reservation(input(2, Side::Sell, 5), reserve(2))
                    .unwrap();
            }
            let mut registry = ReservationRegistry::new([reserve(1), reserve(2)]).unwrap();
            let before = (production_snapshot(&book), registry.clone());
            let result = book.plan_cancel_replace(
                &request(false, Side::Buy, 10, tif),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[TradeId::new(1), TradeId::new(2), TradeId::new(3)]),
            );
            if iceberg_maker || tif == TimeInForce::Gtc {
                assert_eq!(
                    result.unwrap_err().reason,
                    RejectReason::PrioritySpaceExhaustion
                );
                assert_eq!((production_snapshot(&book), registry), before);
            } else {
                book.apply_cancel_replace(&mut registry, result.unwrap())
                    .unwrap();
                assert_eq!(book.next_queue_priority(), 2);
                parity(&book, &registry);
            }
        }
    }
}

#[test]
fn production_cancel_replace_same_side_aggregate_overflow_is_atomic() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest_with_reservation(input(1, side, 1), reserve(1))
            .unwrap();
        let mut large = input(2, side, 1);
        large.qty = Qty::try_new(u64::MAX - 1).unwrap();
        book.rest_with_reservation(large, reserve(2)).unwrap();
        let registry = ReservationRegistry::new([reserve(1), reserve(2)]).unwrap();
        let before = (production_snapshot(&book), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(
                &request(false, side, 2, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[])
            )
            .unwrap_err()
            .reason,
            RejectReason::ArithmeticOverflow
        );
        assert_eq!((production_snapshot(&book), registry), before);
    }
}

#[test]
fn production_cancel_replace_checked_cumulative_notional_failure_preserves_every_state() {
    let math = MarketMath::try_new(0, 0, 18, 1, 1).unwrap();
    let mut book = ProductionOrderBook::new(policy());
    let mut old = input(1, Side::Sell, 5);
    old.price = Price::try_new(i64::MAX).unwrap();
    book.rest_with_reservation(old, reserve(1)).unwrap();
    for id in [2, 3] {
        let mut maker = input(id, Side::Buy, 20);
        maker.price = Price::try_new(9_000_000_000_000_000_000).unwrap();
        book.rest_with_reservation(maker, reserve(id)).unwrap();
    }
    let registry = ReservationRegistry::new([reserve(1), reserve(2), reserve(3)]).unwrap();
    let mut r = request(false, Side::Sell, 40, TimeInForce::Ioc);
    r.replacement.price = Price::try_new(1).unwrap();
    let before = (production_snapshot(&book), registry.clone());
    assert_eq!(
        book.plan_cancel_replace(
            &r,
            &expected(),
            &registry,
            &keys(),
            context(&math, &[TradeId::new(1), TradeId::new(2)])
        )
        .unwrap_err()
        .reason,
        RejectReason::ArithmeticOverflow
    );
    assert_eq!((production_snapshot(&book), registry), before);
}

#[test]
fn production_cancel_replace_trade_id_capacity_fails_closed_before_old_unlink() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ProductionOrderBook::new(policy());
    let mut old = input(1, Side::Buy, 5);
    old.price = Price::try_new(90).unwrap();
    book.rest_with_reservation(old, reserve(1)).unwrap();
    book.rest_with_reservation(input(2, Side::Sell, 5), reserve(2))
        .unwrap();
    let registry = ReservationRegistry::new([reserve(1), reserve(2)]).unwrap();
    let before = (production_snapshot(&book), registry.clone());
    assert!(
        catch_unwind(AssertUnwindSafe(|| book.plan_cancel_replace(
            &request(false, Side::Buy, 5, TimeInForce::Fok),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[])
        )))
        .is_err()
    );
    assert_eq!((production_snapshot(&book), registry), before);
}

#[test]
fn production_cancel_replace_partial_gtc_preserves_total_and_replaces_real_expiry() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for tif in [
        TimeInForce::Gtc,
        TimeInForce::Day { session_id: 7 },
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(40),
        },
    ] {
        let mut book = ProductionOrderBook::new(policy());
        let mut old = input(1, Side::Buy, 5);
        old.price = Price::try_new(90).unwrap();
        book.rest_timed_with_reservation(
            old,
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(30),
            },
            LogicalTime::new(1),
            7,
            reserve(1),
        )
        .unwrap();
        book.rest_with_reservation(input(2, Side::Sell, 2), reserve(2))
            .unwrap();
        let old_slot = book.orders[&OrderId::new(1)];
        let maker_slot = book.orders[&OrderId::new(2)];
        let mut registry = ReservationRegistry::new([reserve(1), reserve(2)]).unwrap();
        let p = book
            .plan_cancel_replace(
                &request(false, Side::Buy, 5, tif),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[TradeId::new(1)]),
            )
            .unwrap();
        book.apply_cancel_replace(&mut registry, p).unwrap();
        let node = book.order(OrderId::new(9)).unwrap();
        assert_eq!(node.original_order().qty.get(), 5);
        assert_eq!(node.total_remaining(), 3);
        assert_eq!(node.time_in_force(), tif);
        assert_eq!(node.priority(), QueuePriority::new(2));
        assert_eq!(book.orders[&OrderId::new(9)], maker_slot);
        assert_eq!(book.arena.state_snapshot().1, vec![old_slot]);
        assert_eq!(
            book.regular_expiry.test_entries(),
            if tif == TimeInForce::Gtc {
                vec![]
            } else {
                vec![(OrderId::new(9), tif)]
            }
        );
        parity(&book, &registry);
    }
}

#[test]
fn production_cancel_replace_iceberg_makers_refresh_fifo_and_terminalize_once() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ProductionOrderBook::new(policy());
    let mut old = input(1, Side::Buy, 5);
    old.price = Price::try_new(90).unwrap();
    book.rest_with_reservation(old, reserve(1)).unwrap();
    book.place_iceberg_post_only_with_reservation(
        IcebergOrder::try_new(input(2, Side::Sell, 4), Qty::try_new(2).unwrap()).unwrap(),
        &math,
        reserve(2),
    )
    .unwrap();
    book.rest_with_reservation(input(3, Side::Sell, 1), reserve(3))
        .unwrap();
    let mut registry = ReservationRegistry::new([reserve(1), reserve(2), reserve(3)]).unwrap();
    let ids = [TradeId::new(81), TradeId::new(82), TradeId::new(83)];
    let p = book
        .plan_cancel_replace(
            &request(true, Side::Buy, 6, TimeInForce::Gtc),
            &expected(),
            &registry,
            &keys(),
            context(&math, &ids),
        )
        .unwrap();
    let events = book.apply_cancel_replace(&mut registry, p).unwrap();
    assert_eq!(
        events,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(1),
                remaining: Qty::try_new(5).unwrap(),
                reason: CancelReason::CancelReplace
            },
            EngineEvent::Trade(TradeEvent {
                trade_id: ids[0],
                maker_order_id: OrderId::new(2),
                taker_order_id: OrderId::new(9),
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(2).unwrap(),
                maker_side: Side::Sell
            }),
            EngineEvent::OrderPartiallyFilled {
                order_id: OrderId::new(2),
                remaining: Qty::try_new(2).unwrap()
            },
            EngineEvent::IcebergRefreshed {
                order_id: OrderId::new(2),
                visible: Qty::try_new(2).unwrap()
            },
            EngineEvent::Trade(TradeEvent {
                trade_id: ids[1],
                maker_order_id: OrderId::new(3),
                taker_order_id: OrderId::new(9),
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                maker_side: Side::Sell
            }),
            EngineEvent::Trade(TradeEvent {
                trade_id: ids[2],
                maker_order_id: OrderId::new(2),
                taker_order_id: OrderId::new(9),
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(2).unwrap(),
                maker_side: Side::Sell
            }),
            EngineEvent::OrderFilled {
                order_id: OrderId::new(2)
            }
        ]
    );
    assert_eq!(
        book.order(OrderId::new(9)).unwrap().priority(),
        QueuePriority::new(4)
    );
    for id in [101, 102, 103] {
        assert!(registry.is_pending_r2(ReservationId::new(id)));
    }
    assert!(!registry.is_pending_r2(ReservationId::new(109)));
    parity(&book, &registry);
}

#[test]
fn production_cancel_replace_signed_adjustment_is_old_bound_and_only_apply_installs_target() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for valid in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest_with_reservation(input(1, Side::Buy, 5), reserve(1))
            .unwrap();
        let mut registry = ReservationRegistry::new([reserve(1)]).unwrap();
        let mut e = expected();
        e.required_reserves[0].atoms = 20_000;
        let mut token = matching_domain::ReservationAdjustmentToken {
            token_id: 91,
            command_id: e.command_id,
            reservation_id: e.old_reservation_id,
            order_id: if valid {
                e.old_order_id
            } else {
                e.new_order_id
            },
            user_id: e.user_id,
            market_id: e.market_id,
            expected_reservation_version: 3,
            target_reserves: e.required_reserves.clone().into(),
            fee_schedule_version: 1,
            fee_tier_id: 2,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = SigningKey::from_bytes(&[7; 32])
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let mut r = request(false, Side::Buy, 5, TimeInForce::Gtc);
        r.reservation_transition = ReservationTransition::ReuseOld {
            adjustment: Some(token),
        };
        let before = (production_snapshot(&book), registry.clone());
        let result = book.plan_cancel_replace(&r, &e, &registry, &keys(), context(&math, &[]));
        assert_eq!((production_snapshot(&book), registry.clone()), before);
        if valid {
            book.apply_cancel_replace(&mut registry, result.unwrap())
                .unwrap();
            let active = book.reservation_state(OrderId::new(9)).unwrap();
            assert_eq!(active.reservation_id, ReservationId::new(101));
            assert_eq!(active.version, 4);
            assert_eq!(active.reserves, e.required_reserves);
            parity(&book, &registry);
        } else {
            assert_eq!(result.unwrap_err().reason, RejectReason::InvalidReservation);
        }
    }
}

#[test]
fn production_cancel_replace_old_max_version_is_retained_for_new_and_checked_for_reuse() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new in [false, true] {
        let mut old = reserve(1);
        old.version = u64::MAX;
        let mut book = ProductionOrderBook::new(policy());
        book.rest_with_reservation(input(1, Side::Buy, 5), old.clone())
            .unwrap();
        let mut registry = ReservationRegistry::new([old.clone()]).unwrap();
        let mut e = expected();
        e.expected_version = u64::MAX;
        let before = (production_snapshot(&book), registry.clone());
        let result = book.plan_cancel_replace(
            &request(new, Side::Buy, 5, TimeInForce::Gtc),
            &e,
            &registry,
            &keys(),
            context(&math, &[]),
        );
        if new {
            book.apply_cancel_replace(&mut registry, result.unwrap())
                .unwrap();
            assert_eq!(registry.get(old.reservation_id), Some(&old));
            assert!(registry.is_pending_r2(old.reservation_id));
            parity(&book, &registry);
        } else {
            assert_eq!(result.unwrap_err().reason, RejectReason::ArithmeticOverflow);
            assert_eq!((production_snapshot(&book), registry), before);
        }
    }
}

#[test]
fn production_cancel_replace_terminal_maker_without_financial_binding_is_atomic_rejection() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ProductionOrderBook::new(policy());
    let mut old = input(1, Side::Buy, 5);
    old.price = Price::try_new(90).unwrap();
    book.rest_with_reservation(old, reserve(1)).unwrap();
    book.rest(input(2, Side::Sell, 5)).unwrap();
    let registry = ReservationRegistry::new([reserve(1)]).unwrap();
    let before = (production_snapshot(&book), registry.clone());
    assert_eq!(
        book.plan_cancel_replace(
            &request(false, Side::Buy, 5, TimeInForce::Fok),
            &expected(),
            &registry,
            &keys(),
            context(&math, &[TradeId::new(1)])
        )
        .unwrap_err()
        .reason,
        RejectReason::InvalidReservation
    );
    assert_eq!((production_snapshot(&book), registry), before);
}

#[test]
fn production_cancel_replace_terminal_maker_validation_precedes_insufficient_reuse_reserve() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for registered_maker in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        let mut old = input(1, Side::Buy, 5);
        old.price = Price::try_new(90).unwrap();
        book.rest_with_reservation(old, reserve(1)).unwrap();
        let mut states = vec![reserve(1)];
        if registered_maker {
            book.rest_with_reservation(input(2, Side::Sell, 5), reserve(2))
                .unwrap();
            states.push(reserve(2));
        } else {
            book.rest(input(2, Side::Sell, 5)).unwrap();
        }
        let registry = ReservationRegistry::new(states).unwrap();
        let mut requirements = expected();
        requirements.required_reserves[0].atoms = 10_001;
        let before = (production_snapshot(&book), registry.clone());
        let reason = book
            .plan_cancel_replace(
                &request(false, Side::Buy, 5, TimeInForce::Fok),
                &requirements,
                &registry,
                &keys(),
                context(&math, &[TradeId::new(1)]),
            )
            .unwrap_err()
            .reason;
        assert_eq!(
            reason,
            if registered_maker {
                RejectReason::InsufficientReservation
            } else {
                RejectReason::InvalidReservation
            },
            "registered_maker={registered_maker}"
        );
        assert_eq!((production_snapshot(&book), registry), before);
    }
}

#[test]
fn production_cancel_replace_stop_market_old_modes_are_unsupported_and_never_mutated() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for quantity in [
        matching_domain::StopMarketQuantity::Base(Qty::try_new(5).unwrap()),
        matching_domain::StopMarketQuantity::Quote(
            matching_domain::QuoteBudget::try_new(500).unwrap(),
        ),
    ] {
        let mut book = ProductionOrderBook::new(policy());
        book.place_stop_with_reservation(
            StopOrder::Market(matching_domain::StopMarketOrder {
                order_id: OrderId::new(1),
                user_id: UserId::new(5),
                reservation_id: reserve(1).reservation_id,
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
            reserve(1),
        )
        .unwrap();
        let registry = ReservationRegistry::new([reserve(1)]).unwrap();
        let before = (production_snapshot(&book), registry.clone());
        assert_eq!(
            book.plan_cancel_replace(
                &request(false, Side::Buy, 5, TimeInForce::Gtc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[])
            )
            .unwrap_err()
            .reason,
            RejectReason::InvalidOrderCombination
        );
        assert_eq!((production_snapshot(&book), registry), before);
    }
}

#[test]
fn production_cancel_replace_iceberg_old_cancel_uses_total_and_terminal_new_is_pending_r2() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for new in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.place_iceberg_post_only_with_reservation(
            IcebergOrder::try_new(input(1, Side::Sell, 10), Qty::try_new(2).unwrap()).unwrap(),
            &math,
            reserve(1),
        )
        .unwrap();
        let mut registry = ReservationRegistry::new([reserve(1)]).unwrap();
        let p = book
            .plan_cancel_replace(
                &request(new, Side::Buy, 5, TimeInForce::Ioc),
                &expected(),
                &registry,
                &keys(),
                context(&math, &[]),
            )
            .unwrap();
        assert_eq!(
            book.apply_cancel_replace(&mut registry, p).unwrap(),
            vec![
                EngineEvent::OrderCanceled {
                    order_id: OrderId::new(1),
                    remaining: Qty::try_new(10).unwrap(),
                    reason: CancelReason::CancelReplace
                },
                EngineEvent::OrderCanceled {
                    order_id: OrderId::new(9),
                    remaining: Qty::try_new(5).unwrap(),
                    reason: CancelReason::IocRemainder
                }
            ]
        );
        assert!(book.orders.is_empty());
        assert_eq!(book.next_queue_priority(), 1);
        assert!(registry.is_pending_r2(ReservationId::new(101)));
        if new {
            assert!(registry.is_pending_r2(ReservationId::new(109)));
            assert_eq!(registry.get(ReservationId::new(101)), Some(&reserve(1)));
        }
        parity(&book, &registry);
    }
}
