//! 撤单替换验证视图完整排除旧身份，并保持参考簿全部状态不变。

use super::*;
use matching_domain::{
    AssetId, AssetReserve, IcebergOrder, LogicalTime, MarketId, PriceReference, PriceReferenceKind,
    ReservationId, StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce,
    TriggerExpiry, UserId,
};
use matching_risk::{ReservationState, ReservationStatus};

fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn state(id: u128) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id + 100),
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 3,
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 0,
        reserves: vec![
            AssetReserve {
                asset: AssetId::new(1),
                atoms: 500,
            },
            AssetReserve {
                asset: AssetId::new(2),
                atoms: 900,
            },
        ],
    }
}

fn stop(id: u128, expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        reservation_id: ReservationId::new(id + 100),
        fee_schedule_version: 1,
        side: Side::Sell,
        stop_price: Price::try_new(90).unwrap(),
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(10).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_reference: PriceReferenceKind::LastTrade,
        trigger_expiry: expiry,
    })
}

#[test]
fn book_without_regular_fok_never_counts_old_five() {
    for (maker_side, taker_side) in [(Side::Sell, Side::Buy), (Side::Buy, Side::Sell)] {
        let mut book = ReferenceOrderBook::new();
        book.rest_with_reservation(order(1, maker_side, 100, 5), state(1))
            .unwrap();
        book.rest(order(2, maker_side, 100, 5)).unwrap();
        let before = book.test_deterministic_state();
        let view = book.book_without(OrderId::new(1)).unwrap();
        assert_eq!(
            view.preflight_limit_fok(&order(3, taker_side, 100, 10), &math()),
            Err(RejectReason::FokInsufficientLiquidity)
        );
        assert_eq!(view.orders(maker_side).len(), 1);
        assert_eq!(view.orders(maker_side)[0].total_remaining(), 5);
        assert_eq!(view, book.book_without(OrderId::new(1)).unwrap());
        assert_eq!(
            view.preflight_limit_fok(&order(3, taker_side, 100, 5), &math()),
            Ok(())
        );
        assert_eq!(book.test_deterministic_state(), before);
    }
}

#[test]
fn book_without_iceberg_excludes_visible_hidden_and_future_slices() {
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg_post_only_with_reservation(
        IcebergOrder::try_new(order(1, Side::Sell, 100, 10), Qty::try_new(2).unwrap()).unwrap(),
        &math(),
        state(1),
    )
    .unwrap();
    book.rest(order(2, Side::Sell, 100, 5)).unwrap();
    assert_eq!(
        (
            book.orders[0].remaining(),
            book.orders[0].hidden_remaining()
        ),
        (2, 8)
    );
    let before = book.test_deterministic_state();
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        view.orders(Side::Sell)
            .iter()
            .map(|o| o.total_remaining())
            .sum::<u64>(),
        5
    );
    assert_eq!(view, book.book_without(OrderId::new(1)).unwrap());
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 5), &math()),
        Ok(())
    );
    assert_eq!(book.test_deterministic_state(), before);
}

#[test]
fn book_without_post_only_uses_changed_best_and_preserves_fifo() {
    for (maker_side, taker_side, next_price) in
        [(Side::Sell, Side::Buy, 110), (Side::Buy, Side::Sell, 90)]
    {
        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, maker_side, 100, 5)).unwrap();
        book.rest(order(2, maker_side, next_price, 3)).unwrap();
        book.rest(order(3, maker_side, next_price, 4)).unwrap();
        let candidate = order(9, taker_side, 100, 1);
        assert_eq!(
            book.preflight_post_only(&candidate),
            Err(RejectReason::PostOnlyWouldCross)
        );
        let before = book.test_deterministic_state();
        let view = book.book_without(OrderId::new(1)).unwrap();
        assert_eq!(
            view.best(maker_side).unwrap().original_order().price,
            Price::try_new(next_price).unwrap()
        );
        assert_eq!(
            view.orders(maker_side)
                .iter()
                .map(|o| o.original_order().order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(2), OrderId::new(3)]
        );
        assert_eq!(view.preflight_post_only(&candidate), Ok(()));
        assert_eq!(
            view.preflight_post_only(&order(9, taker_side, next_price, 1)),
            Err(RejectReason::PostOnlyWouldCross)
        );
        assert_eq!(view, book.book_without(OrderId::new(1)).unwrap());
        assert_eq!(book.test_deterministic_state(), before);
    }
}

#[test]
fn book_without_stop_limit_removes_only_stop_identity_never_regular_liquidity() {
    let mut book = ReferenceOrderBook::new();
    book.place_stop_with_reservation(
        stop(
            1,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(30),
            },
        ),
        LogicalTime::new(1),
        7,
        state(1),
    )
    .unwrap();
    book.place_stop(
        stop(2, TriggerExpiry::Day { session_id: 7 }),
        LogicalTime::new(1),
        7,
    )
    .unwrap();
    book.rest(order(3, Side::Sell, 100, 5)).unwrap();
    let before = book.test_deterministic_state();
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert!(view.stop_order(OrderId::new(1)).is_none());
    assert!(view.stop_order(OrderId::new(2)).is_some());
    assert_eq!(view.stop_orders(Side::Sell).len(), 1);
    assert_eq!(view.orders(Side::Sell).len(), 1);
    assert_eq!(view, book.book_without(OrderId::new(1)).unwrap());
    assert_eq!(
        view.preflight_limit_fok(&order(2, Side::Buy, 100, 5), &math()),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(
        view.preflight_limit_fok(&order(9, Side::Buy, 100, 10), &math()),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    assert_eq!(
        view.preflight_limit_fok(&order(9, Side::Buy, 100, 5), &math()),
        Ok(())
    );
    assert_eq!(book.test_deterministic_state(), before);
}

#[test]
fn book_without_repeatable_view_preserves_full_deterministic_state() {
    let mut book = ReferenceOrderBook::new();
    book.rest_timed_with_reservation(
        order(1, Side::Sell, 100, 5),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(1),
        7,
        state(1),
    )
    .unwrap();
    book.rest_timed_with_reservation(
        order(2, Side::Sell, 110, 5),
        TimeInForce::Day { session_id: 7 },
        LogicalTime::new(1),
        7,
        state(2),
    )
    .unwrap();
    book.place_stop_with_reservation(
        stop(
            3,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(40),
            },
        ),
        LogicalTime::new(1),
        7,
        state(3),
    )
    .unwrap();
    for (kind, source_seq) in [
        (PriceReferenceKind::LastTrade, 5),
        (PriceReferenceKind::MarkPrice, 9),
        (PriceReferenceKind::IndexPrice, 20),
    ] {
        book.reference_prices
            .advance(PriceReference {
                kind,
                source_seq,
                price: Price::try_new(105).unwrap(),
            })
            .unwrap();
    }
    let before = book.test_deterministic_state();
    let first = book.book_without(OrderId::new(1)).unwrap();
    let second = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.orders(Side::Sell).len(), 1);
    assert_eq!(
        first.orders(Side::Sell)[0].time_in_force(),
        TimeInForce::Day { session_id: 7 }
    );
    assert_eq!(book.test_deterministic_state(), before);
    assert!(matches!(
        book.book_without(OrderId::new(999)),
        Err(RejectReason::OrderNotFound)
    ));
    assert_eq!(book.test_deterministic_state(), before);
}

#[test]
fn book_without_other_iceberg_still_preflights_replenishment_and_priority() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, Side::Buy, 80, 5)).unwrap();
    book.place_iceberg_post_only(
        IcebergOrder::try_new(order(2, Side::Sell, 100, 10), Qty::try_new(2).unwrap()).unwrap(),
        &math(),
    )
    .unwrap();
    let before = book.test_deterministic_state();
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Ok(())
    );
    assert_eq!(book.test_deterministic_state(), before);
    book.next_priority = u128::MAX;
    let before = book.test_deterministic_state();
    let view = book.book_without(OrderId::new(1)).unwrap();
    assert_eq!(
        view.preflight_limit_fok(&order(3, Side::Buy, 100, 10), &math()),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(book.test_deterministic_state(), before);
}

#[test]
fn book_without_rejects_unsupported_stop_market_without_mutation() {
    for quantity in [
        StopMarketQuantity::Base(Qty::try_new(10).unwrap()),
        StopMarketQuantity::Quote(matching_domain::QuoteBudget::try_new(100).unwrap()),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.place_stop(
            StopOrder::Market(StopMarketOrder {
                order_id: OrderId::new(1),
                user_id: UserId::new(1),
                reservation_id: ReservationId::new(101),
                fee_schedule_version: 1,
                side: Side::Buy,
                stop_price: Price::try_new(110).unwrap(),
                quantity,
                child_tif: TimeInForce::Ioc,
                trigger_reference: PriceReferenceKind::LastTrade,
                trigger_expiry: TriggerExpiry::Gtc,
                market_protection: None,
            }),
            LogicalTime::new(1),
            7,
        )
        .unwrap();
        let before = book.test_deterministic_state();
        assert!(matches!(
            book.book_without(OrderId::new(1)),
            Err(RejectReason::InvalidOrderCombination)
        ));
        assert_eq!(book.test_deterministic_state(), before);
    }
}
