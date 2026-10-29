//! 普通下单计划必须同时维护真实预留归属，且不能提前释放金融状态。

use super::*;
use crate::reference_orderbook::{ReferencePlaceContext, ReferencePlaceOrder};
use matching_domain::{
    AssetId, AssetReserve, IcebergOrder, LogicalTime, MarketId, ReservationId, TimeInForce, UserId,
};
use matching_risk::{
    ReservationApplyError, ReservationRegistry, ReservationState, ReservationStatus,
};

fn order(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn state(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id + 100),
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 3,
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 2,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 500,
        }],
    }
}

fn limit(id: u128, side: Side, qty: u64, tif: TimeInForce) -> ReferencePlaceOrder {
    ReferencePlaceOrder::Limit {
        order: order(id, side, qty),
        time_in_force: tif,
        post_only: false,
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
fn pending_r2_full_incoming_and_multiple_terminal_makers_share_one_pure_plan() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let ids = [TradeId::new(11), TradeId::new(12)];
    for tif in [TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Fok] {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Sell, 2), state(1))
            .unwrap();
        book.rest_with_reservation(order(2, Side::Sell, 3), state(2))
            .unwrap();
        let mut registry = ReservationRegistry::new([state(1), state(2), state(9)]).unwrap();
        let before_book = book.clone();
        let before_registry = registry.clone();
        let request = limit(9, Side::Buy, 5, tif);
        let first = book
            .plan_place_with_registry(request.clone(), state(9), &registry, context(&math, &ids))
            .unwrap();
        let second = book
            .plan_place_with_registry(request, state(9), &registry, context(&math, &ids))
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(book, before_book);
        assert_eq!(registry, before_registry);
        let events = book
            .apply_place_with_registry(&mut registry, first)
            .unwrap();
        let trades = events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::Trade(trade) => {
                    Some((trade.trade_id, trade.maker_order_id, trade.qty.get()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            trades,
            vec![(ids[0], OrderId::new(1), 2), (ids[1], OrderId::new(2), 3)]
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
        );
        assert!(book.orders(Side::Buy).is_empty());
        assert!(book.orders(Side::Sell).is_empty());
        assert_eq!(book.next_priority, 2);
        for id in [1, 2, 9] {
            assert!(
                registry.is_pending_r2(state(id).reservation_id),
                "terminal {id}, tif {tif:?}"
            );
            assert_eq!(registry.get(state(id).reservation_id), Some(&state(id)));
        }
        assert_parity(&book, &registry);
    }
}

#[test]
fn pending_r2_gtc_residual_keeps_binding_and_original_total_for_later_amend() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 3), state(1))
        .unwrap();
    let mut registry = ReservationRegistry::new([state(1), state(9)]).unwrap();
    let plan = book
        .plan_place_with_registry(
            limit(9, Side::Buy, 10, TimeInForce::Gtc),
            state(9),
            &registry,
            context(&math, &[TradeId::new(1)]),
        )
        .unwrap();
    book.apply_place_with_registry(&mut registry, plan).unwrap();
    let resting = book.best(Side::Buy).unwrap();
    assert_eq!(resting.total_remaining(), 7);
    assert_eq!(resting.original_order().qty.get(), 10);
    assert_eq!(resting.priority(), QueuePriority::new(1));
    assert_eq!(book.reservation_state(OrderId::new(9)), Some(&state(9)));
    assert!(!registry.is_pending_r2(state(9).reservation_id));
    assert!(registry.is_pending_r2(state(1).reservation_id));
    assert_parity(&book, &registry);
}

#[test]
fn pending_r2_partial_maker_stays_active_and_empty_or_partial_ioc_is_terminal() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for (maker_qty, incoming_qty, expected_maker) in [(0, 4, 0), (2, 4, 0), (5, 2, 3)] {
        let mut book = ReferenceOrderBook::new();
        let mut states = vec![state(9)];
        if maker_qty != 0 {
            book.rest_with_reservation(order(1, Side::Sell, maker_qty), state(1))
                .unwrap();
            states.push(state(1));
        }
        let mut registry = ReservationRegistry::new(states).unwrap();
        let plan = book
            .plan_place_with_registry(
                limit(9, Side::Buy, incoming_qty, TimeInForce::Ioc),
                state(9),
                &registry,
                context(&math, &[TradeId::new(1)]),
            )
            .unwrap();
        let events = book.apply_place_with_registry(&mut registry, plan).unwrap();
        assert!(registry.is_pending_r2(state(9).reservation_id));
        assert_eq!(registry.get(state(9).reservation_id), Some(&state(9)));
        assert_eq!(
            book.best(Side::Sell).map_or(0, |maker| maker.remaining()),
            expected_maker
        );
        if expected_maker != 0 {
            assert!(!registry.is_pending_r2(state(1).reservation_id));
            assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
        } else if maker_qty != 0 {
            assert!(registry.is_pending_r2(state(1).reservation_id));
        }
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
        );
        assert_parity(&book, &registry);
    }
}

#[test]
fn pending_r2_timed_post_only_and_iceberg_use_the_same_normal_place_boundary() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let requests = [
        limit(9, Side::Buy, 5, TimeInForce::Day { session_id: 7 }),
        limit(
            9,
            Side::Buy,
            5,
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(20),
            },
        ),
        ReferencePlaceOrder::Limit {
            order: order(9, Side::Buy, 5),
            time_in_force: TimeInForce::Gtc,
            post_only: true,
        },
        ReferencePlaceOrder::Iceberg {
            order: IcebergOrder::try_new(order(9, Side::Buy, 5), Qty::try_new(2).unwrap()).unwrap(),
            post_only: false,
        },
        ReferencePlaceOrder::Iceberg {
            order: IcebergOrder::try_new(order(9, Side::Buy, 5), Qty::try_new(2).unwrap()).unwrap(),
            post_only: true,
        },
    ];
    for request in requests {
        let mut book = ReferenceOrderBook::new();
        let mut registry = ReservationRegistry::new([state(9)]).unwrap();
        let plan = book
            .plan_place_with_registry(request, state(9), &registry, context(&math, &[]))
            .unwrap();
        assert_eq!(book.next_priority, 0);
        book.apply_place_with_registry(&mut registry, plan).unwrap();
        assert_eq!(book.best(Side::Buy).unwrap().total_remaining(), 5);
        assert_eq!(book.next_priority, 1);
        assert!(!registry.is_pending_r2(state(9).reservation_id));
        assert_parity(&book, &registry);
    }
}

#[test]
fn pending_r2_missing_or_mismatched_terminal_reservation_rejects_without_writes() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for path in 0..4 {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, Side::Sell, 2)).unwrap();
        if path != 0 {
            book.reservations.insert(OrderId::new(1), state(1));
        }
        let mut maker = state(1);
        if path == 2 {
            maker.version = 44;
        }
        let registry = match path {
            0 | 1 => ReservationRegistry::new([state(9)]).unwrap(),
            2 => ReservationRegistry::new([maker, state(9)]).unwrap(),
            3 => ReservationRegistry::new_with_pending_r2(
                [maker, state(9)],
                [state(1).reservation_id],
            )
            .unwrap(),
            _ => unreachable!(),
        };
        let before_book = book.clone();
        let before_registry = registry.clone();
        assert_eq!(
            book.plan_place_with_registry(
                limit(9, Side::Buy, 2, TimeInForce::Gtc),
                state(9),
                &registry,
                context(&math, &[TradeId::new(1)])
            ),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(book, before_book);
        assert_eq!(registry, before_registry);
    }
    let book = ReferenceOrderBook::new();
    for registry in [
        ReservationRegistry::new([]).unwrap(),
        ReservationRegistry::new_with_pending_r2([state(9)], [state(9).reservation_id]).unwrap(),
    ] {
        let before = registry.clone();
        assert_eq!(
            book.plan_place_with_registry(
                limit(9, Side::Buy, 2, TimeInForce::Ioc),
                state(9),
                &registry,
                context(&math, &[])
            ),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(registry, before);
    }
}

#[test]
fn pending_r2_business_rejections_preserve_book_and_registry() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for path in 0..5 {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Sell, 2), state(1))
            .unwrap();
        let registry = ReservationRegistry::new([state(1), state(9)]).unwrap();
        let (request, expected) = match path {
            0 => (
                limit(9, Side::Buy, 3, TimeInForce::Fok),
                RejectReason::FokInsufficientLiquidity,
            ),
            1 => (
                ReferencePlaceOrder::Limit {
                    order: order(9, Side::Buy, 2),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                },
                RejectReason::PostOnlyWouldCross,
            ),
            2 => (
                limit(
                    9,
                    Side::Buy,
                    3,
                    TimeInForce::Gtd {
                        expire_at: LogicalTime::new(10),
                    },
                ),
                RejectReason::InvalidExpireTime,
            ),
            3 => {
                book.next_priority = u128::MAX - PRIORITY_GUARD;
                (
                    limit(9, Side::Buy, 3, TimeInForce::Gtc),
                    RejectReason::PrioritySpaceExhaustion,
                )
            }
            4 => (
                ReferencePlaceOrder::Limit {
                    order: order(9, Side::Buy, 2),
                    time_in_force: TimeInForce::Ioc,
                    post_only: true,
                },
                RejectReason::InvalidOrderCombination,
            ),
            _ => unreachable!(),
        };
        let before_book = book.clone();
        let before_registry = registry.clone();
        assert_eq!(
            book.plan_place_with_registry(
                request,
                state(9),
                &registry,
                context(&math, &[TradeId::new(1)])
            ),
            Err(expected)
        );
        assert_eq!(book, before_book);
        assert_eq!(registry, before_registry);
    }
}

#[test]
fn pending_r2_apply_drift_never_partially_changes_book_or_registry() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for drift_book in [false, true] {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Sell, 2), state(1))
            .unwrap();
        let mut registry = ReservationRegistry::new([state(1), state(9)]).unwrap();
        let plan = book
            .plan_place_with_registry(
                limit(9, Side::Buy, 2, TimeInForce::Gtc),
                state(9),
                &registry,
                context(&math, &[TradeId::new(1)]),
            )
            .unwrap();
        if drift_book {
            book.next_stop_priority += 1;
        } else {
            let other = registry.plan_binding_transitions([&state(9)], []).unwrap();
            registry.apply_binding_transition(other).unwrap();
        }
        let before_book = book.clone();
        let before_registry = registry.clone();
        assert_eq!(
            book.apply_place_with_registry(&mut registry, plan),
            Err(ReservationApplyError::StateChanged)
        );
        assert_eq!(book, before_book);
        assert_eq!(registry, before_registry);
    }
}

#[test]
fn pending_r2_iceberg_refresh_retains_binding_until_total_is_consumed() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg_post_only_with_reservation(
        IcebergOrder::try_new(order(1, Side::Sell, 5), Qty::try_new(2).unwrap()).unwrap(),
        &math,
        state(1),
    )
    .unwrap();
    let mut registry = ReservationRegistry::new([state(1), state(8), state(9)]).unwrap();
    // 未执行的新到身份不能提前出现在本次完整绑定集合中。
    let before = registry.clone();
    assert_eq!(
        book.plan_place_with_registry(
            limit(8, Side::Buy, 2, TimeInForce::Ioc),
            state(8),
            &registry,
            context(&math, &[TradeId::new(1)])
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(registry, before);
    registry = ReservationRegistry::new([state(1), state(8)]).unwrap();
    let plan = book
        .plan_place_with_registry(
            limit(8, Side::Buy, 2, TimeInForce::Ioc),
            state(8),
            &registry,
            context(&math, &[TradeId::new(1)]),
        )
        .unwrap();
    let events = book.apply_place_with_registry(&mut registry, plan).unwrap();
    assert_eq!(book.best(Side::Sell).unwrap().total_remaining(), 3);
    assert_eq!(
        book.best(Side::Sell).unwrap().priority(),
        QueuePriority::new(1)
    );
    assert!(!registry.is_pending_r2(state(1).reservation_id));
    assert!(registry.is_pending_r2(state(8).reservation_id));
    assert!(events.iter().any(|event| matches!(event, EngineEvent::IcebergRefreshed { order_id, .. } if *order_id == OrderId::new(1))));
    assert_parity(&book, &registry);
    // 新到金融状态由外层接管；恢复真实历史时保留已终结身份。
    registry = ReservationRegistry::new_with_pending_r2(
        [state(1), state(8), state(9)],
        [state(8).reservation_id],
    )
    .unwrap();
    let plan = book
        .plan_place_with_registry(
            limit(9, Side::Buy, 3, TimeInForce::Fok),
            state(9),
            &registry,
            context(&math, &[TradeId::new(2), TradeId::new(3)]),
        )
        .unwrap();
    let repeated = plan.clone();
    book.apply_place_with_registry(&mut registry, plan).unwrap();
    assert_eq!(book.next_priority, 3);
    assert!(book.orders(Side::Sell).is_empty());
    for id in [1, 8, 9] {
        assert!(registry.is_pending_r2(state(id).reservation_id));
        assert_eq!(registry.get(state(id).reservation_id), Some(&state(id)));
    }
    let before_book = book.clone();
    let before_registry = registry.clone();
    assert_eq!(
        book.apply_place_with_registry(&mut registry, repeated),
        Err(ReservationApplyError::StateChanged)
    );
    assert_eq!(book, before_book);
    assert_eq!(registry, before_registry);
    assert_parity(&book, &registry);
}

#[test]
fn pending_r2_iceberg_and_timed_incoming_terminal_or_residual_are_not_special_cases() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for incoming_qty in [4, 6] {
        for iceberg in [false, true] {
            let mut book = ReferenceOrderBook::new();
            book.rest_with_reservation(order(1, Side::Buy, 4), state(1))
                .unwrap();
            let mut registry = ReservationRegistry::new([state(1), state(9)]).unwrap();
            let request = if iceberg {
                ReferencePlaceOrder::Iceberg {
                    order: IcebergOrder::try_new(
                        order(9, Side::Sell, incoming_qty),
                        Qty::try_new(2).unwrap(),
                    )
                    .unwrap(),
                    post_only: false,
                }
            } else {
                limit(
                    9,
                    Side::Sell,
                    incoming_qty,
                    TimeInForce::Gtd {
                        expire_at: LogicalTime::new(20),
                    },
                )
            };
            let plan = book
                .plan_place_with_registry(
                    request,
                    state(9),
                    &registry,
                    context(&math, &[TradeId::new(1)]),
                )
                .unwrap();
            let events = book.apply_place_with_registry(&mut registry, plan).unwrap();
            assert!(registry.is_pending_r2(state(1).reservation_id));
            assert_eq!(
                registry.is_pending_r2(state(9).reservation_id),
                incoming_qty == 4
            );
            assert_eq!(registry.get(state(9).reservation_id), Some(&state(9)));
            assert_eq!(
                book.best(Side::Sell)
                    .map_or(0, |maker| maker.total_remaining()),
                incoming_qty - 4
            );
            if incoming_qty == 6 {
                let residual = book.best(Side::Sell).unwrap();
                assert_eq!(residual.original_order().qty.get(), 6);
                if !iceberg {
                    assert_eq!(
                        residual.time_in_force(),
                        TimeInForce::Gtd {
                            expire_at: LogicalTime::new(20)
                        }
                    );
                }
            }
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, EngineEvent::ReservationReleased { .. }))
            );
            assert_parity(&book, &registry);
        }
    }
}

#[test]
fn pending_r2_gtc_trade_math_failure_precedes_every_binding_or_book_write() {
    let math = MarketMath::try_new(0, 0, 1, 1, 1).unwrap();
    for maker_count in [1, 2] {
        let mut book = ReferenceOrderBook::new();
        let maker_qty = if maker_count == 1 {
            u64::MAX
        } else {
            u64::MAX / 8
        };
        for id in 1..=maker_count {
            let mut maker = order(id, Side::Buy, maker_qty);
            maker.price = Price::try_new(i64::MAX).unwrap();
            book.rest_with_reservation(maker, state(id)).unwrap();
        }
        let registry = ReservationRegistry::new(
            (1..=maker_count)
                .map(state)
                .chain(std::iter::once(state(9))),
        )
        .unwrap();
        let incoming_qty = if maker_count == 1 {
            u64::MAX
        } else {
            (u64::MAX / 8) * 2
        };
        let mut incoming = order(9, Side::Sell, incoming_qty);
        incoming.price = Price::try_new(1).unwrap();
        let before_book = book.clone();
        let before_registry = registry.clone();
        assert_eq!(
            book.plan_place_with_registry(
                ReferencePlaceOrder::Limit {
                    order: incoming,
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
                },
                state(9),
                &registry,
                context(&math, &[TradeId::new(1), TradeId::new(2)]),
            ),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(book, before_book);
        assert_eq!(registry, before_registry);
    }
}
