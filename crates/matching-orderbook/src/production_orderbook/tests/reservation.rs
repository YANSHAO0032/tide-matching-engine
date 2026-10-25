//! 验证生产簿预留影子状态的登记与终结清理。

use super::*;
use matching_domain::market_order::CommandStartProtectionContext;
use matching_domain::{
    AssetId, AssetReserve, IcebergOrder, LogicalTime, MarketByBase, MarketByQuote, MarketId,
    MarketMath, QuoteBudget, ReservationId, TimeInForce,
};
use matching_risk::ReservationState;

fn state(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id + 100),
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        market_id: MarketId::new(7),
        version: 3,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 500,
        }],
    }
}

#[test]
fn atomic_rest_with_reservation_initializes_open_order() {
    let mut book = ProductionOrderBook::new(policy());
    assert_eq!(
        book.rest_with_reservation(order(1, Side::Buy, 90, 2), state(1)),
        Ok(())
    );
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn atomic_gtc_rejects_bad_identity_before_trade_and_attaches_remainder() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest_with_reservation(order(1, Side::Sell, 100, 2), state(1))
        .unwrap();
    let before = node_snapshot(&book, &[OrderId::new(1)]);
    let depth = level_snapshot(&book.asks);
    let next = book.next_queue_priority();
    assert_eq!(
        book.place_limit_gtc_with_reservation(
            order(2, Side::Buy, 100, 4),
            state(9),
            &[TradeId::new(1)]
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(node_snapshot(&book, &[OrderId::new(1)]), before);
    assert_eq!(level_snapshot(&book.asks), depth);
    assert_eq!(book.next_queue_priority(), next);
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    let events = book
        .place_limit_gtc_with_reservation(order(2, Side::Buy, 100, 4), state(2), &[TradeId::new(1)])
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 2);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn atomic_rest_rejections_preserve_book_and_shadow_state() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest_with_reservation(order(1, Side::Sell, 100, 4), state(1))
        .unwrap();
    book.rest(order(2, Side::Sell, 100, 4)).unwrap();
    let before = node_snapshot(&book, &[OrderId::new(1), OrderId::new(2)]);
    let depth = level_snapshot(&book.asks);
    let next = book.next_queue_priority();
    let stop_next = book.next_stop_priority();
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    assert_eq!(
        book.rest_with_reservation(order(3, Side::Sell, 100, 4), state(9)),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        book.rest_with_reservation(order(2, Side::Sell, 100, 4), state(2)),
        Err(RejectReason::DuplicateOrderId)
    );
    let mut replacement = state(1);
    replacement.version = 99;
    assert_eq!(
        book.rest_with_reservation(order(1, Side::Sell, 100, 4), replacement),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(
        node_snapshot(&book, &[OrderId::new(1), OrderId::new(2)]),
        before
    );
    assert_eq!(level_snapshot(&book.asks), depth);
    assert_eq!(book.next_queue_priority(), next);
    assert_eq!(book.next_stop_priority(), stop_next);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn cancel_fill_and_expiry_clear_only_terminal_reservations() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest_with_reservation(order(1, Side::Sell, 100, 5), state(1))
        .unwrap();
    book.rest_with_reservation(order(2, Side::Sell, 100, 5), state(2))
        .unwrap();
    book.rest_timed_with_reservation(
        order(3, Side::Buy, 90, 2),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(10),
        1,
        state(3),
    )
    .unwrap();
    let next = book.next_queue_priority();
    let mut incoming = IncomingOrder::new(order(9, Side::Buy, 100, 2));
    book.execute_next_fill_trade(&mut incoming, TradeId::new(1))
        .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    assert_eq!(validate_book(&book), Ok(()));
    let mut incoming = IncomingOrder::new(order(10, Side::Buy, 100, 3));
    book.execute_next_fill_trade(&mut incoming, TradeId::new(2))
        .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    assert_eq!(validate_book(&book), Ok(()));
    book.cancel(OrderId::new(2)).unwrap();
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(
        book.rest_with_reservation(order(2, Side::Sell, 100, 5), state(1)),
        Err(RejectReason::InvalidReservation)
    );
    book.expire_gtd_through(LogicalTime::new(20));
    assert_eq!(book.reservation_state(OrderId::new(3)), None);
    assert_eq!(book.next_queue_priority(), next);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn iceberg_refresh_keeps_state_until_full_fill_and_day_expiry_cleans_state() {
    let mut book = ProductionOrderBook::new(policy());
    let iceberg =
        IcebergOrder::try_new(order(1, Side::Sell, 100, 5), Qty::try_new(2).unwrap()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.place_iceberg_post_only_with_reservation(iceberg, &math, state(1))
        .unwrap();
    book.rest_timed_with_reservation(
        order(2, Side::Buy, 90, 1),
        TimeInForce::Day { session_id: 7 },
        LogicalTime::new(10),
        7,
        state(2),
    )
    .unwrap();
    let stop_next = book.next_stop_priority();
    book.place_limit_ioc(order(3, Side::Buy, 100, 2), &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), Some(&state(1)));
    assert_eq!(validate_book(&book), Ok(()));
    book.place_limit_fok(
        order(4, Side::Buy, 100, 3),
        &math,
        &[TradeId::new(2), TradeId::new(3)],
    )
    .unwrap();
    assert_eq!(book.reservation_state(OrderId::new(1)), None);
    assert_eq!(book.reservation_state(OrderId::new(2)), Some(&state(2)));
    assert_eq!(validate_book(&book), Ok(()));
    book.end_session(7, 7).unwrap();
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(book.next_stop_priority(), stop_next);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validator_rejects_orphaned_or_mismatched_reservation_identity() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Buy, 90, 1)).unwrap();
    book.reservations.insert(OrderId::new(9), state(9));
    assert!(validate_book(&book).is_err());
    book.reservations.clear();
    book.reservations.insert(OrderId::new(1), state(2));
    assert!(validate_book(&book).is_err());
}

#[test]
fn timed_or_priority_rejection_does_not_create_order_or_shadow_state() {
    let mut book = ProductionOrderBook::new(policy_with_ceiling(1));
    book.rest_with_reservation(order(1, Side::Sell, 100, 1), state(1))
        .unwrap();
    let before = node_snapshot(&book, &[OrderId::new(1)]);
    let depth = level_snapshot(&book.asks);
    let next = book.next_queue_priority();
    assert_eq!(
        book.rest_timed_with_reservation(
            order(2, Side::Buy, 90, 1),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(10)
            },
            LogicalTime::new(10),
            7,
            state(2)
        ),
        Err(RejectReason::InvalidExpireTime)
    );
    assert_eq!(
        book.rest_with_reservation(order(3, Side::Buy, 90, 1), state(3)),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(node_snapshot(&book, &[OrderId::new(1)]), before);
    assert_eq!(level_snapshot(&book.asks), depth);
    assert_eq!(book.next_queue_priority(), next);
    assert_eq!(book.reservation_state(OrderId::new(2)), None);
    assert_eq!(book.reservation_state(OrderId::new(3)), None);
    assert!(book.expire_gtd_through(LogicalTime::new(10)).is_empty());
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn market_base_and_quote_full_fills_clear_shadow_state() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    for path in 0..4 {
        let mut book = ProductionOrderBook::new(policy());
        book.rest_with_reservation(order(1, Side::Sell, 100, 2), state(1))
            .unwrap();
        let next = book.next_queue_priority();
        let base = MarketByBase {
            order_id: OrderId::new(9),
            user_id: UserId::new(9),
            side: Side::Buy,
            qty: Qty::try_new(2).unwrap(),
            market_protection: None,
        };
        let quote = MarketByQuote {
            order_id: OrderId::new(9),
            user_id: UserId::new(9),
            side: Side::Buy,
            quote_budget: QuoteBudget::try_new(200).unwrap(),
            market_protection: None,
        };
        match path {
            0 => assert_eq!(
                book.execute_market_by_base(base, &[TradeId::new(1)])
                    .unwrap()
                    .len(),
                1
            ),
            1 => assert_eq!(
                book.execute_market_by_quote(quote, &math, &[TradeId::new(1)])
                    .unwrap()
                    .trades()
                    .len(),
                1
            ),
            2 => assert_eq!(
                book.execute_market_by_base_with_protection(
                    base,
                    &context,
                    &math,
                    &[TradeId::new(1)]
                )
                .unwrap()
                .iter()
                .filter(|event| matches!(event, EngineEvent::Trade(_)))
                .count(),
                1
            ),
            _ => assert_eq!(
                book.execute_market_by_quote_with_protection(
                    quote,
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
            ),
        }
        assert_eq!(book.reservation_state(OrderId::new(1)), None);
        assert!(book.order(OrderId::new(1)).is_none());
        assert_eq!(book.next_queue_priority(), next);
        assert_eq!(validate_book(&book), Ok(()));
    }
}
