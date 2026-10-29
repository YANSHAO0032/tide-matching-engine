//! 验证参考簿独立管理预留影子状态。

use super::*;
use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{
    AssetId, AssetReserve, IcebergOrder, LogicalTime, MarketByBase, MarketByQuote, MarketId,
    MarketMath, QuoteBudget, ReservationId, TimeInForce, TradeId, UserId,
};
use matching_risk::ReservationState;

fn order(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(if side == Side::Sell { 100 } else { 90 }).unwrap(),
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
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 0,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 500,
        }],
    }
}

#[test]
fn released_reservation_cannot_be_adopted_by_any_reservation_constructor() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for path in 0..5 {
        let mut book = ReferenceOrderBook::new();
        book.rest_timed_with_reservation(
            order(1, Side::Sell, 2),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(20),
            },
            LogicalTime::new(1),
            7,
            state(1),
        )
        .unwrap();
        let mut released = state(9);
        released.status = matching_risk::ReservationStatus::Released;
        let before = book.test_deterministic_state();
        let mut candidate = order(9, Side::Buy, 4);
        if path == 2 {
            candidate.price = Price::try_new(100).unwrap();
        }
        let result = match path {
            0 => book.rest_with_reservation(candidate, released),
            1 => book.rest_timed_with_reservation(
                candidate,
                TimeInForce::Day { session_id: 7 },
                LogicalTime::new(1),
                7,
                released,
            ),
            2 => book
                .place_limit_gtc_with_reservation(candidate, released, &[TradeId::new(1)])
                .map(|_| ()),
            3 => book.place_iceberg_post_only_with_reservation(
                IcebergOrder::try_new(candidate, Qty::try_new(2).unwrap()).unwrap(),
                &math,
                released,
            ),
            4 => book
                .place_stop_with_reservation(
                    matching_domain::StopOrder::Limit(matching_domain::StopLimitOrder {
                        order_id: candidate.order_id,
                        user_id: candidate.user_id,
                        stp_key: matching_domain::StpKey::new(0),
                        stp_mode: matching_domain::StpMode::None,
                        reservation_id: released.reservation_id,
                        fee_schedule_version: 1,
                        side: candidate.side,
                        stop_price: Price::try_new(110).unwrap(),
                        price: candidate.price,
                        qty: candidate.qty,
                        child_tif: TimeInForce::Gtc,
                        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
                        trigger_expiry: matching_domain::TriggerExpiry::Gtd {
                            expire_at: LogicalTime::new(30),
                        },
                    }),
                    LogicalTime::new(1),
                    7,
                    released,
                )
                .map(|_| ()),
            _ => unreachable!(),
        };
        assert_eq!(result, Err(RejectReason::InvalidReservation), "path {path}");
        assert_eq!(book.test_deterministic_state(), before, "path {path}");
    }
}

#[test]
fn atomic_rest_with_reservation_initializes_open_order() {
    let mut book = ReferenceOrderBook::new();
    assert_eq!(
        book.rest_with_reservation(order(1, Side::Buy, 2), state(1)),
        Ok(())
    );
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
}

#[test]
fn atomic_gtc_rejects_bad_identity_before_trade_and_attaches_remainder() {
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 2), state(1))
        .unwrap();
    let before = book
        .orders(Side::Sell)
        .iter()
        .map(|o| (o.original_order().order_id, o.remaining(), o.priority()))
        .collect::<Vec<_>>();
    let next = book.next_priority;
    let mut candidate = order(2, Side::Buy, 4);
    candidate.price = Price::try_new(100).unwrap();
    assert_eq!(
        book.place_limit_gtc_with_reservation(candidate.clone(), state(9), &[TradeId::new(1)]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        book.orders(Side::Sell)
            .iter()
            .map(|o| (o.original_order().order_id, o.remaining(), o.priority()))
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(book.next_priority, next);
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    let events = book
        .place_limit_gtc_with_reservation(candidate, state(2), &[TradeId::new(1)])
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    assert_eq!(book.orders(Side::Buy)[0].remaining(), 2);
}

#[test]
fn atomic_rest_rejections_preserve_vec_order_and_shadow_state() {
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 4), state(1))
        .unwrap();
    book.rest(order(2, Side::Sell, 4)).unwrap();
    let before: Vec<_> = book
        .orders(Side::Sell)
        .iter()
        .map(|o| (o.original_order().order_id, o.priority(), o.remaining()))
        .collect();
    let next = book.next_priority;
    let stop_next = book.next_stop_priority;
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    assert_eq!(
        book.rest_with_reservation(order(3, Side::Sell, 4), state(9)),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        book.rest_with_reservation(order(2, Side::Sell, 4), state(2)),
        Err(RejectReason::DuplicateOrderId)
    );
    let mut replacement = state(1);
    replacement.version = 99;
    assert_eq!(
        book.rest_with_reservation(order(1, Side::Sell, 4), replacement),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(
        book.orders(Side::Sell)
            .iter()
            .map(|o| (o.original_order().order_id, o.priority(), o.remaining()))
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(book.next_priority, next);
    assert_eq!(book.next_stop_priority, stop_next);
}

#[test]
fn cancel_fill_and_expiry_clear_only_terminal_reservations() {
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 5), state(1))
        .unwrap();
    book.rest_with_reservation(order(2, Side::Sell, 5), state(2))
        .unwrap();
    book.rest_timed_with_reservation(
        order(3, Side::Buy, 2),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(10),
        1,
        state(3),
    )
    .unwrap();
    let next = book.next_priority;
    let mut first_taker = order(9, Side::Buy, 2);
    first_taker.price = Price::try_new(100).unwrap();
    let mut incoming = IncomingOrder::new(first_taker);
    assert!(book.execute_next_fill(&mut incoming).unwrap().is_some());
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    let mut second_taker = order(10, Side::Buy, 3);
    second_taker.price = Price::try_new(100).unwrap();
    let mut incoming = IncomingOrder::new(second_taker);
    assert!(book.execute_next_fill(&mut incoming).unwrap().is_some());
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    book.cancel(OrderId::new(2)).unwrap();
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(
        book.rest_with_reservation(order(2, Side::Sell, 5), state(1)),
        Err(RejectReason::InvalidReservation)
    );
    book.expire_gtd_through(LogicalTime::new(20));
    assert_eq!(book.reservation_state(OrderId::new(3)), None);
    assert_eq!(book.next_priority, next);
}

#[test]
fn iceberg_refresh_keeps_state_until_full_fill_and_day_expiry_cleans_state() {
    let mut book = ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let iceberg = IcebergOrder::try_new(order(1, Side::Sell, 5), Qty::try_new(2).unwrap()).unwrap();
    book.place_iceberg_post_only_with_reservation(iceberg, &math, state(1))
        .unwrap();
    book.rest_timed_with_reservation(
        order(2, Side::Buy, 1),
        TimeInForce::Day { session_id: 7 },
        LogicalTime::new(10),
        7,
        state(2),
    )
    .unwrap();
    let stop_next = book.next_stop_priority;
    let mut first_taker = order(3, Side::Buy, 2);
    first_taker.price = Price::try_new(100).unwrap();
    book.place_limit_ioc(first_taker, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    let mut second_taker = order(4, Side::Buy, 3);
    second_taker.price = Price::try_new(100).unwrap();
    book.place_limit_fok(second_taker, &math, &[TradeId::new(2), TradeId::new(3)])
        .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    book.end_session(7, 7).unwrap();
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(book.next_stop_priority, stop_next);
}

#[test]
fn timed_or_priority_rejection_does_not_create_order_or_shadow_state() {
    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(order(1, Side::Sell, 1), state(1))
        .unwrap();
    let before = book
        .orders(Side::Sell)
        .iter()
        .map(|o| (o.original_order().order_id, o.remaining(), o.priority()))
        .collect::<Vec<_>>();
    assert_eq!(
        book.rest_timed_with_reservation(
            order(2, Side::Buy, 1),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(10)
            },
            LogicalTime::new(10),
            7,
            state(2)
        ),
        Err(RejectReason::InvalidExpireTime)
    );
    book.next_priority = u128::MAX - super::super::PRIORITY_GUARD;
    let next = book.next_priority;
    assert_eq!(
        book.rest_with_reservation(order(3, Side::Buy, 1), state(3)),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(
        book.orders(Side::Sell)
            .iter()
            .map(|o| (o.original_order().order_id, o.remaining(), o.priority()))
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(book.next_priority, next);
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(book.reservation_state(OrderId::new(3)), None);
    assert!(book.expire_gtd_through(LogicalTime::new(10)).is_empty());
}

#[test]
fn market_base_quote_and_bulk_immediate_full_fills_clear_shadow_state() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    for path in 0..5 {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, Side::Sell, 2), state(1))
            .unwrap();
        let next = book.next_priority;
        match path {
            0 => {
                let taker = MarketByBase {
                    order_id: OrderId::new(9),
                    user_id: UserId::new(9),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Buy,
                    qty: Qty::try_new(2).unwrap(),
                    market_protection: None,
                };
                assert_eq!(
                    book.execute_market_by_base(taker, &[TradeId::new(1)])
                        .unwrap()
                        .len(),
                    1
                );
            }
            1 => {
                let taker = MarketByQuote {
                    order_id: OrderId::new(9),
                    user_id: UserId::new(9),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Buy,
                    quote_budget: QuoteBudget::try_new(200).unwrap(),
                    market_protection: None,
                };
                assert_eq!(
                    book.execute_market_by_quote(taker, &math, &[TradeId::new(1)])
                        .unwrap()
                        .trades()
                        .len(),
                    1
                );
            }
            2 => {
                let taker = MarketByBase {
                    order_id: OrderId::new(9),
                    user_id: UserId::new(9),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Buy,
                    qty: Qty::try_new(2).unwrap(),
                    market_protection: None,
                };
                assert_eq!(
                    book.execute_market_by_base_with_protection(
                        taker,
                        &context,
                        &math,
                        &[TradeId::new(1)]
                    )
                    .unwrap()
                    .iter()
                    .filter(|event| matches!(event, EngineEvent::Trade(_)))
                    .count(),
                    1
                );
            }
            3 => {
                let taker = MarketByQuote {
                    order_id: OrderId::new(9),
                    user_id: UserId::new(9),
                    stp_key: matching_domain::StpKey::new(0),
                    stp_mode: matching_domain::StpMode::None,
                    side: Side::Buy,
                    quote_budget: QuoteBudget::try_new(200).unwrap(),
                    market_protection: None,
                };
                assert_eq!(
                    book.execute_market_by_quote_with_protection(
                        taker,
                        &context,
                        &math,
                        &[TradeId::new(1)]
                    )
                    .unwrap()
                    .events()
                    .iter()
                    .filter(|event| matches!(event, EngineEvent::Trade(_)))
                    .count(),
                    1
                );
            }
            _ => {
                let mut taker = order(9, Side::Buy, 2);
                taker.price = Price::try_new(100).unwrap();
                assert_eq!(
                    book.place_limit_ioc(taker, &math, &[TradeId::new(1)])
                        .unwrap()
                        .events()
                        .len(),
                    1
                );
            }
        }
        assert_eq!(book.reservation_state(OrderId::new(1)), None);
        assert!(book.orders(Side::Sell).is_empty());
        assert_eq!(book.next_priority, next);
    }
}
