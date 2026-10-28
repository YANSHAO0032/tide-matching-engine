//! 生产验证视图必须排除整个旧身份，且不改动任何真实容器。

use super::amend_reservation::state;
use super::amend_stop::production_snapshot;
use super::*;
use matching_domain::{
    IcebergOrder, LogicalTime, ReservationId, StopLimitOrder, StopOrder, TimeInForce, TriggerExpiry,
};

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn stop(id: u128, expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        reservation_id: ReservationId::new(id),
        fee_schedule_version: 5,
        side: Side::Sell,
        stop_price: Price::try_new(100).unwrap(),
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(10).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: expiry,
    })
}

#[test]
fn production_book_without_regular_fok_never_counts_old_five() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ProductionOrderBook::new(policy());
        for id in [1, 2] {
            let input = order(id, side, 100, 5);
            book.rest_with_reservation(input.clone(), state(&input))
                .unwrap();
        }
        let before = production_snapshot(&book);
        let view = book.book_without(OrderId::new(1)).unwrap();
        assert_eq!(
            view.preflight_limit_fok(&order(3, opposite(side), 100, 10), &math()),
            Err(RejectReason::FokInsufficientLiquidity)
        );
        assert_eq!(
            view.preflight_limit_fok(&order(3, opposite(side), 100, 5), &math()),
            Ok(())
        );
        assert_eq!(
            view.orders(side)
                .iter()
                .map(|n| n.original_order().order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(2)]
        );
        assert_eq!(view, book.book_without(OrderId::new(1)).unwrap());
        assert_eq!(production_snapshot(&book), before);
    }
}

#[test]
fn production_book_without_iceberg_excludes_all_future_liquidity() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Sell, 100, 10);
    let iceberg = IcebergOrder::try_new(input.clone(), Qty::try_new(2).unwrap()).unwrap();
    book.place_iceberg_post_only_with_reservation(iceberg, &math(), state(&input))
        .unwrap();
    book.rest(order(2, Side::Sell, 100, 5)).unwrap();
    let before = production_snapshot(&book);
    let view = book.book_without(input.order_id).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 5), &math()),
        Ok(())
    );
    assert_eq!(
        view.depth(Side::Sell),
        vec![(Price::try_new(100).unwrap(), 5)]
    );
    assert_eq!(view, book.book_without(input.order_id).unwrap());
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_post_only_changes_best_and_preserves_fifo() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ProductionOrderBook::new(policy());
        let next = if side == Side::Sell { 110 } else { 90 };
        book.rest(order(1, side, 100, 5)).unwrap();
        for id in [2, 3] {
            book.rest(order(id, side, next, 5)).unwrap();
        }
        let before = production_snapshot(&book);
        let candidate = order(4, opposite(side), 100, 1);
        assert_eq!(
            book.place_limit_post_only(candidate.clone()),
            Err(RejectReason::PostOnlyWouldCross)
        );
        let view = book.book_without(OrderId::new(1)).unwrap();
        assert_eq!(view.best_price(side), Some(Price::try_new(next).unwrap()));
        assert_eq!(view.preflight_post_only(&candidate), Ok(()));
        assert_eq!(
            view.orders(side)
                .iter()
                .map(|n| n.original_order().order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(2), OrderId::new(3)]
        );
        assert_eq!(
            view.preflight_post_only(&order(4, opposite(side), next, 1)),
            Err(RejectReason::PostOnlyWouldCross)
        );
        assert_eq!(production_snapshot(&book), before);
    }
}

#[test]
fn production_book_without_stop_is_never_executable_liquidity() {
    let mut book = ProductionOrderBook::new(policy());
    for id in [1, 2] {
        book.place_stop(
            stop(
                id,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(30),
                },
            ),
            LogicalTime::new(1),
            9,
        )
        .unwrap();
    }
    book.rest(order(3, Side::Sell, 100, 5)).unwrap();
    let before = production_snapshot(&book);
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert!(view.stop_order(OrderId::new(1)).is_none());
    assert!(view.stop_order(OrderId::new(2)).is_some());
    assert_eq!(
        view.preflight_limit_fok(&order(4, Side::Buy, 100, 10), &math()),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        view.preflight_limit_fok(&order(2, Side::Buy, 100, 5), &math()),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(view.stop_orders(Side::Sell).len(), 1);
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_rest_capacity_subtracts_old_and_keeps_reservation() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 100, u64::MAX);
    book.rest_with_reservation(input.clone(), state(&input))
        .unwrap();
    let before = production_snapshot(&book);
    let view = book.book_without(input.order_id).unwrap();
    assert_eq!(
        view.preflight_rest(
            &order(2, Side::Buy, 100, u64::MAX),
            view.next_queue_priority()
        ),
        Ok(())
    );
    assert_eq!(view.active_regular_count(), 0);
    assert_eq!(
        view.reservation_state(input.order_id),
        book.reservation_state(input.order_id)
    );
    assert_eq!(view.depth(Side::Buy), vec![]);
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_other_iceberg_still_needs_replenish_priority() {
    let low = QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(3)).unwrap();
    let mut book = ProductionOrderBook::new(low);
    book.rest(order(1, Side::Buy, 90, 1)).unwrap();
    book.place_iceberg_post_only(
        IcebergOrder::try_new(order(2, Side::Sell, 100, 10), Qty::try_new(2).unwrap()).unwrap(),
        &math(),
    )
    .unwrap();
    let before = production_snapshot(&book);
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_repeated_view_preserves_populated_physical_state() {
    use matching_domain::{AssetId, AssetReserve, PriceReference, PriceReferenceKind};
    let mut book = ProductionOrderBook::new(policy());
    for (id, side, price, tif) in [
        (
            1,
            Side::Buy,
            90,
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(20),
            },
        ),
        (2, Side::Buy, 90, TimeInForce::Day { session_id: 9 }),
        (
            3,
            Side::Sell,
            110,
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(30),
            },
        ),
    ] {
        let input = order(id, side, price, 5);
        let mut reservation = state(&input);
        reservation.reserves.push(AssetReserve {
            asset: AssetId::new(8),
            atoms: 37,
        });
        book.rest_timed_with_reservation(input, tif, LogicalTime::new(1), 9, reservation)
            .unwrap();
    }
    for (id, expiry) in [
        (
            4,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(40),
            },
        ),
        (5, TriggerExpiry::Day { session_id: 9 }),
    ] {
        book.place_stop_with_reservation(
            stop(id, expiry),
            LogicalTime::new(1),
            9,
            state(&order(id, Side::Sell, 100, 10)),
        )
        .unwrap();
    }
    book.rest(order(6, Side::Buy, 80, 1)).unwrap();
    book.cancel(OrderId::new(6)).unwrap();
    for (kind, source_seq) in [
        (PriceReferenceKind::LastTrade, 3),
        (PriceReferenceKind::MarkPrice, 5),
        (PriceReferenceKind::IndexPrice, 7),
    ] {
        book.reference_prices
            .advance(PriceReference {
                kind,
                source_seq,
                price: Price::try_new(100).unwrap(),
            })
            .unwrap();
    }
    let before = production_snapshot(&book);
    assert!(!book.arena.test_snapshot().1.is_empty());
    for old in [OrderId::new(1), OrderId::new(4)] {
        let first = book.book_without(old).unwrap();
        let second = book.book_without(old).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.regular_expiry(old), None);
        assert_eq!(first.stop_expiry(old), None);
        assert_eq!(
            first.regular_expiry(OrderId::new(2)),
            Some(TimeInForce::Day { session_id: 9 })
        );
        assert_eq!(
            first.stop_expiry(OrderId::new(5)),
            Some(TriggerExpiry::Day { session_id: 9 })
        );
        assert!(first.reservation_state(old).is_some());
        assert_eq!(
            first.preflight_post_only(&order(8, Side::Buy, 95, 2)),
            Ok(())
        );
        assert_eq!(
            first.preflight_rest(&order(8, Side::Buy, 95, 2), first.next_queue_priority()),
            Ok(())
        );
        assert_eq!(
            first.preflight_limit_fok(&order(8, Side::Buy, 110, 5), &math()),
            Ok(())
        );
        assert_eq!(production_snapshot(&book), before);
    }
    assert_eq!(
        book.book_without(OrderId::new(99)).unwrap_err(),
        RejectReason::OrderNotFound
    );
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_replenish_aggregate_excludes_old_visible() {
    let mut book = ProductionOrderBook::new(policy());
    book.rest(order(1, Side::Sell, 100, u64::MAX - 2)).unwrap();
    book.place_iceberg_post_only(
        IcebergOrder::try_new(order(2, Side::Sell, 100, 10), Qty::try_new(2).unwrap()).unwrap(),
        &math(),
    )
    .unwrap();
    let before = production_snapshot(&book);
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Ok(())
    );
    assert_eq!(
        view.preflight_rest(
            &order(3, Side::Sell, 100, u64::MAX),
            view.next_queue_priority()
        ),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        view.preflight_rest(
            &order(3, Side::Sell, 100, u64::MAX - 2),
            view.next_queue_priority()
        ),
        Ok(())
    );
    assert_eq!(production_snapshot(&book), before);
}

#[test]
fn production_book_without_rejects_both_stop_market_old_modes() {
    use matching_domain::{QuoteBudget, StopMarketOrder, StopMarketQuantity};
    for quantity in [
        StopMarketQuantity::Base(Qty::try_new(10).unwrap()),
        StopMarketQuantity::Quote(QuoteBudget::try_new(1000).unwrap()),
    ] {
        let mut book = ProductionOrderBook::new(policy());
        let StopOrder::Limit(base) = stop(1, TriggerExpiry::Gtc) else {
            unreachable!()
        };
        book.place_stop(
            StopOrder::Market(StopMarketOrder {
                trigger_reference: base.trigger_reference,
                market_protection: None,
                order_id: base.order_id,
                user_id: base.user_id,
                reservation_id: base.reservation_id,
                fee_schedule_version: base.fee_schedule_version,
                side: base.side,
                stop_price: base.stop_price,
                quantity,
                child_tif: TimeInForce::Ioc,
                trigger_expiry: base.trigger_expiry,
            }),
            LogicalTime::new(1),
            9,
        )
        .unwrap();
        let before = production_snapshot(&book);
        assert_eq!(
            book.book_without(OrderId::new(1)).unwrap_err(),
            RejectReason::InvalidOrderCombination
        );
        assert_eq!(production_snapshot(&book), before);
    }
}
