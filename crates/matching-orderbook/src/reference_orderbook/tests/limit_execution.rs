//! 限价 IOC、FOK 与 PostOnly 的成交、拒绝及失败原子性测试。

use super::*;
use matching_domain::{
    CancelReason, EngineEvent, LimitImmediateExecution, OrderId, Price, Qty, QueuePriority, Side,
    TradeEvent, UserId,
};

fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitOrder, u64, QueuePriority)>, u128) {
    (
        book.orders
            .iter()
            .map(|o| (o.original_order().clone(), o.remaining(), o.priority()))
            .collect(),
        book.next_priority,
    )
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

#[test]
fn post_only_checks_level_aggregate_after_crossing_and_priority() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, side, 100, u64::MAX)).unwrap();
        let before = snapshot(&book);
        assert_eq!(
            book.place_limit_post_only(order(2, side, 100, 1)),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(snapshot(&book), before);
        // 价档容量取决于当前剩余量，而不是原始下单量。
        book.orders[0].apply_fill(Qty::try_new(2).unwrap()).unwrap();
        book.place_limit_post_only(order(2, side, 100, 2)).unwrap();
        assert_eq!(book.next_priority, 2);
        book.next_priority = u128::MAX;
        let before = snapshot(&book);
        assert_eq!(
            book.place_limit_post_only(order(3, side, 100, 1)),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(snapshot(&book), before);
    }
}

fn execute(
    book: &mut ReferenceOrderBook,
    fok: bool,
    incoming: LimitOrder,
    math: &MarketMath,
    ids: &[TradeId],
) -> Result<LimitImmediateExecution, RejectReason> {
    if fok {
        book.place_limit_fok(incoming, math, ids)
    } else {
        book.place_limit_ioc(incoming, math, ids)
    }
}

fn trade(id: u128, maker: u128, side: Side, price: i64, qty: u64) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(id),
        maker_order_id: OrderId::new(maker),
        taker_order_id: OrderId::new(99),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        maker_side: opposite(side),
    })
}

#[test]
fn post_only_rejects_inclusive_crossing_and_rests_non_crossing_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, opposite(side), 100, 3)).unwrap();
        let before = snapshot(&book);
        let crossing = match side {
            Side::Buy => 101,
            Side::Sell => 99,
        };
        for price in [100, crossing] {
            assert_eq!(
                book.place_limit_post_only(order(99, side, price, 2)),
                Err(RejectReason::PostOnlyWouldCross)
            );
            assert_eq!(snapshot(&book), before);
        }
        let passive = match side {
            Side::Buy => 99,
            Side::Sell => 101,
        };
        book.place_limit_post_only(order(99, side, passive, 2))
            .unwrap();
        assert_eq!(book.orders.len(), 2);
        assert_eq!(book.orders[1].priority(), QueuePriority::new(1));
        assert_eq!(book.orders[0].remaining(), 3);
        assert_eq!(book.next_priority, 2);
    }
}

#[test]
fn post_only_empty_same_side_duplicate_and_priority_failure_are_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.place_limit_post_only(order(1, Side::Buy, 100, 3))
        .unwrap();
    book.place_limit_post_only(order(2, Side::Buy, 101, 3))
        .unwrap();
    book.next_priority = u128::MAX;
    let before = snapshot(&book);
    assert_eq!(
        book.place_limit_post_only(order(1, Side::Sell, 100, 3)),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        book.place_limit_post_only(order(3, Side::Sell, 100, 3)),
        Err(RejectReason::PostOnlyWouldCross)
    );
    assert_eq!(
        book.place_limit_post_only(order(3, Side::Sell, 102, 3)),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(snapshot(&book), before);
}

#[test]
fn immediate_empty_book_cancels_ioc_and_rejects_fok_without_priority() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        book.next_priority = u128::MAX;
        let before = snapshot(&book);
        let result = book
            .place_limit_ioc(order(99, side, 100, 3), &math(), &[])
            .unwrap();
        assert_eq!(
            result.events(),
            &[EngineEvent::OrderCanceled {
                order_id: OrderId::new(99),
                remaining: Qty::try_new(3).unwrap(),
                reason: CancelReason::IocRemainder
            }]
        );
        assert_eq!(result.canceled_remaining(), Some(Qty::try_new(3).unwrap()));
        assert_eq!(
            book.place_limit_fok(order(99, side, 100, 3), &math(), &[]),
            Err(RejectReason::FokInsufficientLiquidity)
        );
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn immediate_exact_and_partial_maker_fills_never_allocate_priority() {
    for side in [Side::Buy, Side::Sell] {
        for fok in [false, true] {
            for qty in [2, 3] {
                let mut book = ReferenceOrderBook::new();
                book.rest(order(1, opposite(side), 100, 3)).unwrap();
                book.next_priority = u128::MAX;
                let result = execute(
                    &mut book,
                    fok,
                    order(99, side, 100, qty),
                    &math(),
                    &[TradeId::new(10)],
                )
                .unwrap();
                assert_eq!(result.events(), &[trade(10, 1, side, 100, qty)]);
                assert_eq!(result.canceled_remaining(), None);
                assert_eq!(book.next_priority, u128::MAX);
                if qty == 3 {
                    assert!(book.orders.is_empty());
                } else {
                    assert_eq!(book.orders[0].remaining(), 1);
                    assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
                }
            }
        }
    }
}

#[test]
fn immediate_multilevel_follows_price_then_fifo_and_limit_boundary() {
    for side in [Side::Buy, Side::Sell] {
        for fok in [false, true] {
            let (best, outside) = match side {
                Side::Buy => (99, 101),
                Side::Sell => (101, 99),
            };
            let mut book = ReferenceOrderBook::new();
            for (id, price, qty) in [(1, 100, 2), (2, best, 1), (3, 100, 4), (4, outside, 8)] {
                book.rest(order(id, opposite(side), price, qty)).unwrap();
            }
            let result = execute(
                &mut book,
                fok,
                order(99, side, 100, 5),
                &math(),
                &[TradeId::new(10), TradeId::new(11), TradeId::new(12)],
            )
            .unwrap();
            assert_eq!(
                result.events(),
                &[
                    trade(10, 2, side, best, 1),
                    trade(11, 1, side, 100, 2),
                    trade(12, 3, side, 100, 2)
                ]
            );
            assert_eq!(book.orders.len(), 2);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(3));
            assert_eq!(book.orders[0].remaining(), 2);
            assert_eq!(book.orders[0].priority(), QueuePriority::new(2));
            assert_eq!(book.orders[1].remaining(), 8);
            assert_eq!(book.next_priority, 4);
        }
    }
}

#[test]
fn ioc_partial_liquidity_emits_cancel_after_trades_and_never_rests() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, opposite(side), 100, 2)).unwrap();
        book.rest(order(
            2,
            opposite(side),
            if side == Side::Buy { 101 } else { 99 },
            20,
        ))
        .unwrap();
        let result = book
            .place_limit_ioc(order(99, side, 100, 5), &math(), &[TradeId::new(10)])
            .unwrap();
        assert_eq!(
            result.events(),
            &[
                trade(10, 1, side, 100, 2),
                EngineEvent::OrderCanceled {
                    order_id: OrderId::new(99),
                    remaining: Qty::try_new(3).unwrap(),
                    reason: CancelReason::IocRemainder
                }
            ]
        );
        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].remaining(), 20);
        assert_eq!(book.next_priority, 2);
    }
}

#[test]
fn fok_insufficient_or_non_crossing_liquidity_is_atomic_before_trade_ids() {
    for side in [Side::Buy, Side::Sell] {
        for executable in [0, 2] {
            let mut book = ReferenceOrderBook::new();
            if executable > 0 {
                book.rest(order(1, opposite(side), 100, executable))
                    .unwrap();
            }
            book.rest(order(
                2,
                opposite(side),
                if side == Side::Buy { 101 } else { 99 },
                20,
            ))
            .unwrap();
            let before = snapshot(&book);
            assert_eq!(
                book.place_limit_fok(order(99, side, 100, 3), &math(), &[]),
                Err(RejectReason::FokInsufficientLiquidity)
            );
            assert_eq!(snapshot(&book), before);
        }
    }
}

#[test]
fn immediate_duplicate_wins_before_math_and_liquidity_validation() {
    for fok in [false, true] {
        for side in [Side::Buy, Side::Sell] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(99, side, 100, 2)).unwrap();
            let before = snapshot(&book);
            assert_eq!(
                execute(
                    &mut book,
                    fok,
                    order(99, Side::Buy, 101, 3),
                    &MarketMath::try_new(0, 0, 0, 2, 2).unwrap(),
                    &[]
                ),
                Err(RejectReason::DuplicateOrderId)
            );
            assert_eq!(snapshot(&book), before);
        }
    }
}

#[test]
fn immediate_validates_full_input_even_when_empty_or_only_partially_executable() {
    for fok in [false, true] {
        for has_maker in [false, true] {
            for (incoming, config, reason) in [
                (
                    order(99, Side::Buy, 101, 2),
                    MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
                    RejectReason::InvalidTick,
                ),
                (
                    order(99, Side::Buy, 100, 3),
                    MarketMath::try_new(0, 0, 0, 1, 2).unwrap(),
                    RejectReason::InvalidLot,
                ),
                (
                    order(99, Side::Buy, i64::MAX, u64::MAX),
                    MarketMath::try_new(0, 0, 1, 1, 1).unwrap(),
                    RejectReason::ArithmeticOverflow,
                ),
            ] {
                let mut book = ReferenceOrderBook::new();
                if has_maker {
                    book.rest(order(1, Side::Sell, 100, 2)).unwrap();
                }
                let before = snapshot(&book);
                assert_eq!(execute(&mut book, fok, incoming, &config, &[]), Err(reason));
                assert_eq!(snapshot(&book), before);
            }
        }
    }
}

#[test]
fn immediate_late_fill_tick_lot_and_notional_failures_are_atomic() {
    for fok in [false, true] {
        for (first, second, incoming, config, reason) in [
            (
                order(1, Side::Sell, 100, 2),
                order(2, Side::Sell, 101, 2),
                order(99, Side::Buy, 102, 4),
                MarketMath::try_new(0, 0, 0, 2, 1).unwrap(),
                RejectReason::InvalidTick,
            ),
            (
                order(1, Side::Sell, 100, 2),
                order(2, Side::Sell, 100, 1),
                order(99, Side::Buy, 100, 4),
                MarketMath::try_new(0, 0, 0, 1, 2).unwrap(),
                RejectReason::InvalidLot,
            ),
            (
                order(1, Side::Buy, i64::MAX, 1),
                order(2, Side::Buy, i64::MAX - 1, u64::MAX),
                order(99, Side::Sell, 1, u64::MAX),
                MarketMath::try_new(0, 0, 1, 1, 1).unwrap(),
                RejectReason::ArithmeticOverflow,
            ),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(first).unwrap();
            book.rest(second).unwrap();
            let before = snapshot(&book);
            assert_eq!(
                execute(
                    &mut book,
                    fok,
                    incoming,
                    &config,
                    &[TradeId::new(10), TradeId::new(11)]
                ),
                Err(reason)
            );
            assert_eq!(snapshot(&book), before);
        }
    }
}

#[test]
fn immediate_aggregate_notional_overflow_is_atomic() {
    for fok in [false, true] {
        let mut book = ReferenceOrderBook::new();
        for id in [1, 2] {
            book.rest(order(id, Side::Buy, i64::MAX, 2_000_000_000_000_000_000))
                .unwrap();
        }
        let before = snapshot(&book);
        assert_eq!(
            execute(
                &mut book,
                fok,
                order(99, Side::Sell, 1, 4_000_000_000_000_000_000),
                &MarketMath::try_new(0, 0, 1, 1, 1).unwrap(),
                &[TradeId::new(10), TradeId::new(11)]
            ),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn immediate_trade_id_shortage_panics_before_first_mutation() {
    for fok in [false, true] {
        let mut book = ReferenceOrderBook::new();
        for id in [1, 2] {
            book.rest(order(id, Side::Sell, 100, 2)).unwrap();
        }
        let before = snapshot(&book);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = execute(
                &mut book,
                fok,
                order(99, Side::Buy, 100, 4),
                &math(),
                &[TradeId::new(10)],
            );
        }));
        assert!(result.is_err());
        assert_eq!(snapshot(&book), before);
    }
}
