use super::*;
use matching_domain::{CancelReason, LimitImmediateExecution};

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn assert_atomic(book: &mut ProductionOrderBook, action: impl FnOnce(&mut ProductionOrderBook)) {
    assert_eq!(validate_book(book), Ok(()));
    let bids = level_snapshot(&book.bids);
    let asks = level_snapshot(&book.asks);
    let orders = book.orders.clone();
    let arena = book.arena.test_snapshot();
    let next = book.next_queue_priority();
    let policy = *book.queue_priority_allocator.policy();
    action(book);
    assert_eq!(level_snapshot(&book.bids), bids);
    assert_eq!(level_snapshot(&book.asks), asks);
    assert_eq!(book.orders, orders);
    assert_eq!(book.arena.test_snapshot(), arena);
    assert_eq!(book.next_queue_priority(), next);
    assert_eq!(*book.queue_priority_allocator.policy(), policy);
    assert_eq!(validate_book(book), Ok(()));
}

fn execute(
    book: &mut ProductionOrderBook,
    fok: bool,
    order: LimitGtcOrder,
    math: &MarketMath,
    ids: &[TradeId],
) -> Result<LimitImmediateExecution, RejectReason> {
    if fok {
        book.place_limit_fok(order, math, ids)
    } else {
        book.place_limit_ioc(order, math, ids)
    }
}

fn trade(id: u128, maker: u128, side: Side, price: i64, qty: u64) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(id),
        maker_order_id: OrderId::new(maker),
        taker_order_id: OrderId::new(100),
        maker_side: side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    })
}

fn canceled(qty: u64) -> EngineEvent {
    EngineEvent::OrderCanceled {
        order_id: OrderId::new(100),
        remaining: Qty::try_new(qty).unwrap(),
        reason: CancelReason::IocRemainder,
    }
}

#[test]
fn post_only_rejects_equal_and_crossing_prices_before_any_write() {
    for (side, opposite, crossing) in [(Side::Buy, Side::Sell, 101), (Side::Sell, Side::Buy, 99)] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, opposite, 100, 2)).unwrap();
        for price in [100, crossing] {
            assert_atomic(&mut book, |book| {
                assert_eq!(
                    book.place_limit_post_only(order(100, side, price, 5)),
                    Err(RejectReason::PostOnlyWouldCross)
                );
            });
        }
    }
}

#[test]
fn post_only_rest_preserves_fifo_and_consumes_priority_once() {
    for (side, opposite, away) in [(Side::Buy, Side::Sell, 99), (Side::Sell, Side::Buy, 101)] {
        let mut book = ProductionOrderBook::new(policy());
        book.place_limit_post_only(order(1, side, away, 2)).unwrap();
        book.rest(order(2, opposite, 100, 2)).unwrap();
        let next = book.next_queue_priority();
        book.place_limit_post_only(order(3, side, away, 3)).unwrap();
        assert_eq!(book.next_queue_priority(), next.checked_add(1).unwrap());
        let first = *book.orders.get(&OrderId::new(1)).unwrap();
        let second = *book.orders.get(&OrderId::new(3)).unwrap();
        assert_eq!(book.arena.get(first).unwrap().next(), Some(second));
        assert_eq!(
            book.arena.get(second).unwrap().priority(),
            QueuePriority::new(next)
        );
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn post_only_duplicate_crossing_and_capacity_failures_are_atomic() {
    for (side, opposite, away) in [(Side::Buy, Side::Sell, 99), (Side::Sell, Side::Buy, 101)] {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));
        book.rest(order(1, opposite, 100, 2)).unwrap();
        for (id, price, expected) in [
            (1, 100, RejectReason::DuplicateOrderId),
            (100, 100, RejectReason::PostOnlyWouldCross),
            (100, away, RejectReason::PrioritySpaceExhaustion),
        ] {
            assert_atomic(&mut book, |book| {
                assert_eq!(
                    book.place_limit_post_only(order(id, side, price, 1)),
                    Err(expected)
                )
            });
        }
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, side, away, u64::MAX)).unwrap();
        assert_atomic(&mut book, |book| {
            assert_eq!(
                book.place_limit_post_only(order(100, side, away, 1)),
                Err(RejectReason::ArithmeticOverflow)
            )
        });
    }
}

#[test]
fn ioc_empty_and_non_crossing_cancel_all_without_rest_or_priority() {
    for (side, opposite, outside) in [(Side::Buy, Side::Sell, 101), (Side::Sell, Side::Buy, 99)] {
        let mut book = ProductionOrderBook::new(policy());
        for empty in [true, false] {
            if !empty {
                book.rest(order(1, opposite, outside, 5)).unwrap();
            }
            assert_atomic(&mut book, |book| {
                let result = book
                    .place_limit_ioc(order(100, side, 100, 3), &math(), &[])
                    .unwrap();
                assert_eq!(result.events(), &[canceled(3)]);
                assert_eq!(result.canceled_remaining(), Some(Qty::try_new(3).unwrap()));
            });
        }
    }
}

#[test]
fn immediate_orders_match_both_directions_by_price_then_fifo() {
    for fok in [false, true] {
        for (side, opposite, better, outside) in [
            (Side::Buy, Side::Sell, 90, 101),
            (Side::Sell, Side::Buy, 110, 99),
        ] {
            let mut book = ProductionOrderBook::new(policy());
            for (id, price, qty) in [
                (40, 100, 3),
                (90, outside, 4),
                (30, better, 2),
                (10, 100, 5),
            ] {
                book.rest(order(id, opposite, price, qty)).unwrap();
            }
            let slot = *book.orders.get(&OrderId::new(10)).unwrap();
            let priority = book.order(OrderId::new(10)).unwrap().priority();
            let next = book.next_queue_priority();
            let ids = [TradeId::new(7), TradeId::new(8), TradeId::new(9)];
            let result = execute(&mut book, fok, order(100, side, 100, 7), &math(), &ids).unwrap();
            assert_eq!(
                result.events(),
                &[
                    trade(7, 30, opposite, better, 2),
                    trade(8, 40, opposite, 100, 3),
                    trade(9, 10, opposite, 100, 2)
                ]
            );
            assert_eq!(result.canceled_remaining(), None);
            assert_eq!(book.order(OrderId::new(10)).unwrap().remaining(), 3);
            assert_eq!(book.orders.get(&OrderId::new(10)), Some(&slot));
            assert_eq!(book.order(OrderId::new(10)).unwrap().priority(), priority);
            assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 4);
            assert!(book.order(OrderId::new(100)).is_none());
            assert_eq!(book.next_queue_priority(), next);
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
}

#[test]
fn ioc_partial_finishes_with_cancellation_after_trades() {
    for (side, opposite, outside) in [(Side::Buy, Side::Sell, 101), (Side::Sell, Side::Buy, 99)] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, opposite, 100, 2)).unwrap();
        book.rest(order(2, opposite, outside, 10)).unwrap();
        let next = book.next_queue_priority();
        let result = book
            .place_limit_ioc(order(100, side, 100, 5), &math(), &[TradeId::new(7)])
            .unwrap();
        assert_eq!(
            result.events(),
            &[trade(7, 1, opposite, 100, 2), canceled(3)]
        );
        assert!(book.order(OrderId::new(1)).is_none());
        assert!(book.order(OrderId::new(100)).is_none());
        assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 10);
        assert_eq!(book.next_queue_priority(), next);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn fok_rejects_empty_insufficient_and_price_excluded_liquidity_atomically() {
    for (side, opposite, outside) in [(Side::Buy, Side::Sell, 101), (Side::Sell, Side::Buy, 99)] {
        let mut book = ProductionOrderBook::new(policy());
        for state in 0..3 {
            if state == 1 {
                book.rest(order(1, opposite, 100, 2)).unwrap();
            }
            if state == 2 {
                book.rest(order(2, opposite, outside, 10)).unwrap();
            }
            assert_atomic(&mut book, |book| {
                assert_eq!(
                    book.place_limit_fok(order(100, side, 100, 3), &math(), &[]),
                    Err(RejectReason::FokInsufficientLiquidity)
                )
            });
        }
    }
}

#[test]
fn immediate_exact_fill_clears_levels_even_when_priority_is_exhausted() {
    for fok in [false, true] {
        for (side, opposite) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy_with_ceiling(1));
            book.rest(order(1, opposite, 100, 2)).unwrap();
            let next = book.next_queue_priority();
            let result = execute(
                &mut book,
                fok,
                order(100, side, 100, 2),
                &math(),
                &[TradeId::new(7)],
            )
            .unwrap();
            assert_eq!(result.events(), &[trade(7, 1, opposite, 100, 2)]);
            assert_eq!(result.canceled_remaining(), None);
            assert!(book.orders.is_empty());
            assert!(book.bids.is_empty() && book.asks.is_empty());
            assert_eq!(book.next_queue_priority(), next);
            assert!(book.queue_priority_allocator.is_exhausted());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
}

#[test]
fn immediate_duplicate_precedes_invalid_math_and_input_validation_is_atomic() {
    for fok in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Sell, 100, 2)).unwrap();
        let tick_lot = MarketMath::try_new(0, 0, 0, 10, 2).unwrap();
        for (id, price, qty, expected) in [
            (1, 101, 3, RejectReason::DuplicateOrderId),
            (100, 101, 2, RejectReason::InvalidTick),
            (100, 100, 3, RejectReason::InvalidLot),
        ] {
            assert_atomic(&mut book, |book| {
                assert_eq!(
                    execute(book, fok, order(id, Side::Buy, price, qty), &tick_lot, &[]),
                    Err(expected)
                )
            });
        }
        let huge = MarketMath::try_new(0, 0, 38, 1, 1).unwrap();
        assert_atomic(&mut book, |book| {
            assert_eq!(
                execute(book, fok, order(100, Side::Buy, 100, 1), &huge, &[]),
                Err(RejectReason::ArithmeticOverflow)
            )
        });
    }
}

#[test]
fn immediate_late_maker_math_errors_precede_first_fill() {
    for fok in [false, true] {
        for (side, opposite, first, second, limit) in [
            (Side::Buy, Side::Sell, 90, 101, 110),
            (Side::Sell, Side::Buy, 110, 101, 100),
        ] {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, opposite, first, 2)).unwrap();
            book.rest(order(2, opposite, second, 2)).unwrap();
            let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
            assert_atomic(&mut book, |book| {
                assert_eq!(
                    execute(
                        book,
                        fok,
                        order(100, side, limit, 4),
                        &math,
                        &[TradeId::new(7), TradeId::new(8)]
                    ),
                    Err(RejectReason::InvalidTick)
                )
            });
        }
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Buy, 3, 1)).unwrap();
        book.rest(order(2, Side::Buy, 2, 2)).unwrap();
        let huge = MarketMath::try_new(0, 0, 38, 1, 1).unwrap();
        assert_atomic(&mut book, |book| {
            assert_eq!(
                execute(
                    book,
                    fok,
                    order(100, Side::Sell, 1, 3),
                    &huge,
                    &[TradeId::new(7), TradeId::new(8)]
                ),
                Err(RejectReason::ArithmeticOverflow)
            )
        });
    }
}

#[test]
fn immediate_aggregate_quote_overflow_precedes_first_fill() {
    for fok in [false, true] {
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Buy, 2, 1)).unwrap();
        book.rest(order(2, Side::Buy, 2, 1)).unwrap();
        let huge = MarketMath::try_new(0, 0, 38, 1, 1).unwrap();
        assert_atomic(&mut book, |book| {
            assert_eq!(
                execute(
                    book,
                    fok,
                    order(100, Side::Sell, 1, 2),
                    &huge,
                    &[TradeId::new(7), TradeId::new(8)]
                ),
                Err(RejectReason::ArithmeticOverflow)
            )
        });
    }
}

#[test]
fn immediate_insufficient_trade_ids_panic_before_first_fill() {
    for fok in [false, true] {
        for (side, opposite) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, opposite, 100, 1)).unwrap();
            book.rest(order(2, opposite, 100, 1)).unwrap();
            assert_atomic(&mut book, |book| {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| execute(
                        book,
                        fok,
                        order(100, side, 100, 2),
                        &math(),
                        &[TradeId::new(7)]
                    )))
                    .is_err()
                );
            });
        }
    }
}
