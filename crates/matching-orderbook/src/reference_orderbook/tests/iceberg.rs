//! 冰山订单的队尾补量、事件、兼容入口及失败原子性测试。

use super::*;
use matching_domain::{IcebergOrder, MarketByBase, QuoteBudget, UserId};
use std::panic::{AssertUnwindSafe, catch_unwind};
fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}
fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}
fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}
fn iceberg(id: u128, side: Side, total: u64, peak: u64) -> IcebergOrder {
    IcebergOrder::try_new(order(id, side, 100, total), Qty::try_new(peak).unwrap()).unwrap()
}
fn seeded(side: Side) -> ReferenceOrderBook {
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg(iceberg(1, side, 8, 3), &math(), &[])
        .unwrap();
    book.rest(order(2, side, 100, 2)).unwrap();
    book
}
fn ids() -> Vec<TradeId> {
    (1..20).map(TradeId::new).collect()
}
fn trades(events: &[EngineEvent]) -> Vec<(OrderId, u64)> {
    events
        .iter()
        .filter_map(|e| {
            if let EngineEvent::Trade(t) = e {
                Some((t.maker_order_id, t.qty.get()))
            } else {
                None
            }
        })
        .collect()
}
type Snapshot = (
    Vec<(LimitOrder, u64, u64, Option<Qty>, QueuePriority)>,
    u128,
);
fn snapshot(book: &ReferenceOrderBook) -> Snapshot {
    (
        book.orders
            .iter()
            .map(|o| {
                (
                    o.original_order().clone(),
                    o.remaining(),
                    o.hidden_remaining(),
                    o.peak_size(),
                    o.priority(),
                )
            })
            .collect(),
        book.next_priority,
    )
}
fn base(side: Side, qty: u64) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(50),
        user_id: UserId::new(1),
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection: None,
    }
}
fn quote(side: Side, budget: u128) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(50),
        user_id: UserId::new(1),
        side,
        quote_budget: QuoteBudget::try_new(budget).unwrap(),
        market_protection: None,
    }
}
fn context() -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    }
}
#[test]
fn reference_iceberg_repeated_refresh_exact_fok_both_sides() {
    for maker_side in [Side::Buy, Side::Sell] {
        let mut book = seeded(maker_side);
        let result = book
            .place_limit_fok(order(3, opposite(maker_side), 100, 10), &math(), &ids())
            .unwrap();
        assert_eq!(
            trades(result.events()),
            vec![
                (OrderId::new(1), 3),
                (OrderId::new(2), 2),
                (OrderId::new(1), 3),
                (OrderId::new(1), 2)
            ]
        );
        assert_eq!(
            result
                .events()
                .iter()
                .filter(|e| matches!(e, EngineEvent::IcebergRefreshed { .. }))
                .count(),
            2
        );
        assert!(result.events().contains(&EngineEvent::OrderFilled {
            order_id: OrderId::new(1)
        }));
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, 4);
    }
}
#[test]
fn reference_iceberg_ioc_remainder_cancels_without_rest() {
    let mut book = seeded(Side::Sell);
    let result = book
        .place_limit_ioc(order(3, Side::Buy, 100, 12), &math(), &ids())
        .unwrap();
    assert_eq!(result.canceled_remaining(), Some(Qty::try_new(2).unwrap()));
    assert!(book.orders.is_empty());
    assert_eq!(book.next_priority, 4);
}
#[test]
fn reference_iceberg_fok_insufficient_preserves_all_runtime() {
    for maker_side in [Side::Buy, Side::Sell] {
        let mut book = seeded(maker_side);
        let before = snapshot(&book);
        assert_eq!(
            book.place_limit_fok(order(3, opposite(maker_side), 100, 11), &math(), &[]),
            Err(RejectReason::FokInsufficientLiquidity)
        );
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn reference_iceberg_last_incoming_fill_still_refreshes() {
    let mut book = seeded(Side::Sell);
    book.place_limit_ioc(order(3, Side::Buy, 100, 3), &math(), &ids())
        .unwrap();
    assert_eq!(
        book.orders(Side::Sell)
            .iter()
            .map(|o| o.original_order().order_id)
            .collect::<Vec<_>>(),
        vec![OrderId::new(2), OrderId::new(1)]
    );
    let maker = book.orders.last().unwrap();
    assert_eq!(
        (
            maker.remaining(),
            maker.hidden_remaining(),
            maker.total_remaining()
        ),
        (3, 2, 5)
    );
    assert_eq!(maker.priority(), QueuePriority::new(2));
}
#[test]
fn reference_iceberg_cancel_removes_total_without_consuming_priority() {
    let mut book = seeded(Side::Sell);
    let removed = book.cancel(OrderId::new(1)).unwrap();
    assert_eq!(
        (
            removed.remaining(),
            removed.hidden_remaining(),
            removed.total_remaining()
        ),
        (3, 5, 8)
    );
    assert_eq!(book.next_priority, 2);
    assert_eq!(book.orders.len(), 1);
}
#[test]
fn reference_iceberg_incoming_takes_full_total_then_slices_remainder() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(10, Side::Sell, 90, 5)).unwrap();
    let events = book
        .place_iceberg(iceberg(1, Side::Buy, 10, 2), &math(), &ids())
        .unwrap();
    assert_eq!(trades(&events), vec![(OrderId::new(10), 5)]);
    let maker = book.best(Side::Buy).unwrap();
    assert_eq!(
        (
            maker.remaining(),
            maker.hidden_remaining(),
            maker.total_remaining()
        ),
        (2, 3, 5)
    );
    assert_eq!(maker.original_order().qty.get(), 10);
    assert_eq!(
        events.last(),
        Some(&EngineEvent::OrderPartiallyFilled {
            order_id: OrderId::new(1),
            remaining: Qty::try_new(5).unwrap()
        })
    );
}
#[test]
fn reference_iceberg_ordinary_gtc_matches_iceberg_and_rests_remainder() {
    let mut book = seeded(Side::Sell);
    let events = book
        .place_limit_gtc_with_events(order(3, Side::Buy, 100, 12), &ids())
        .unwrap();
    assert_eq!(trades(&events).len(), 4);
    let maker = book.best(Side::Buy).unwrap();
    assert_eq!(maker.remaining(), 2);
    assert_eq!(maker.peak_size(), None);
    assert_eq!(book.next_priority, 5);
}
#[test]
fn reference_iceberg_postonly_cross_reject_and_noncross_rest_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = seeded(opposite(side));
        let before = snapshot(&book);
        assert_eq!(
            book.place_iceberg_post_only(iceberg(3, side, 8, 3), &math()),
            Err(RejectReason::PostOnlyWouldCross)
        );
        assert_eq!(snapshot(&book), before);
        let price = if side == Side::Buy { 99 } else { 101 };
        let candidate =
            IcebergOrder::try_new(order(3, side, price, 8), Qty::try_new(3).unwrap()).unwrap();
        book.place_iceberg_post_only(candidate, &math()).unwrap();
        assert_eq!(book.best(side).unwrap().total_remaining(), 8);
    }
}
#[test]
fn reference_iceberg_invalid_lot_rejects_before_taker_mutation() {
    let mut book = seeded(Side::Sell);
    let before = snapshot(&book);
    let math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
    assert_eq!(
        book.place_iceberg(iceberg(3, Side::Buy, 8, 3), &math, &ids()),
        Err(RejectReason::InvalidLot)
    );
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_legacy_single_and_multi_fill_refresh() {
    for single in [true, false] {
        let mut book = seeded(Side::Sell);
        let mut incoming = IncomingOrder::new(order(3, Side::Buy, 100, 6));
        if single {
            let fill = book.execute_next_fill(&mut incoming).unwrap().unwrap();
            assert_eq!(fill.2.get(), 3);
            assert_eq!(incoming.remaining(), 3);
            assert_eq!(book.next_priority, 3);
        } else {
            let fills = book.execute_fills(&mut incoming).unwrap();
            assert_eq!(
                fills.iter().map(|x| (x.0, x.2.get())).collect::<Vec<_>>(),
                vec![
                    (OrderId::new(1), 3),
                    (OrderId::new(2), 2),
                    (OrderId::new(1), 1)
                ]
            );
            assert_eq!(incoming.remaining(), 0);
        }
    }
}
#[test]
fn reference_iceberg_all_market_paths_use_replenished_fifo_liquidity() {
    for side in [Side::Buy, Side::Sell] {
        for path in 0..4 {
            let mut book = seeded(opposite(side));
            let actual = match path {
                0 => book
                    .execute_market_by_base(base(side, 10), &ids())
                    .unwrap()
                    .into_iter()
                    .map(EngineEvent::Trade)
                    .collect(),
                1 => book
                    .execute_market_by_base_with_protection(
                        base(side, 10),
                        &context(),
                        &math(),
                        &ids(),
                    )
                    .unwrap(),
                2 => book
                    .execute_market_by_quote(quote(side, 1000), &math(), &ids())
                    .unwrap()
                    .trades()
                    .iter()
                    .cloned()
                    .map(EngineEvent::Trade)
                    .collect(),
                _ => book
                    .execute_market_by_quote_with_protection(
                        quote(side, 1000),
                        &context(),
                        &math(),
                        &ids(),
                    )
                    .unwrap()
                    .events()
                    .to_vec(),
            };
            assert_eq!(
                trades(&actual),
                vec![
                    (OrderId::new(1), 3),
                    (OrderId::new(2), 2),
                    (OrderId::new(1), 3),
                    (OrderId::new(1), 2)
                ]
            );
            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, 4);
        }
    }
}
#[test]
fn reference_iceberg_quote_dust_and_boundary_keep_hidden() {
    let mut book = seeded(Side::Sell);
    let before = snapshot(&book);
    let result = book
        .execute_market_by_quote(quote(Side::Buy, 99), &math(), &[])
        .unwrap();
    assert!(result.trades().is_empty());
    assert_eq!(result.unused_quote().get(), 99);
    assert_eq!(snapshot(&book), before);
    let mut ctx = context();
    ctx.market_band_worst = Some(Price::try_new(99).unwrap());
    let result = book
        .execute_market_by_quote_with_protection(quote(Side::Buy, 1), &ctx, &math(), &[])
        .unwrap();
    assert!(matches!(
        result.events(),
        [EngineEvent::MarketProtectionTriggered { .. }]
    ));
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_trade_id_shortage_is_atomic_after_multiple_refresh_plans() {
    for path in 0..5 {
        let mut book = seeded(Side::Sell);
        let before = snapshot(&book);
        let result = catch_unwind(AssertUnwindSafe(|| match path {
            0 => {
                let _ =
                    book.place_limit_ioc(order(3, Side::Buy, 100, 10), &math(), &[TradeId::new(1)]);
            }
            1 => {
                let _ = book
                    .place_limit_gtc_with_events(order(3, Side::Buy, 100, 10), &[TradeId::new(1)]);
            }
            2 => {
                let _ = book.execute_market_by_base(base(Side::Buy, 10), &[TradeId::new(1)]);
            }
            3 => {
                let _ = book.execute_market_by_quote(
                    quote(Side::Buy, 1000),
                    &math(),
                    &[TradeId::new(1)],
                );
            }
            _ => {
                let _ = book.execute_market_by_quote_with_protection(
                    quote(Side::Buy, 1000),
                    &context(),
                    &math(),
                    &[TradeId::new(1)],
                );
            }
        }));
        assert!(result.is_err());
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn reference_iceberg_priority_failure_is_atomic_across_full_command() {
    for path in 0..6 {
        let mut book = seeded(Side::Sell);
        book.next_priority = u128::MAX - super::super::PRIORITY_GUARD - 1;
        let before = snapshot(&book);
        let result = match path {
            0 => book
                .place_limit_ioc(order(3, Side::Buy, 100, 10), &math(), &ids())
                .map(|_| ()),
            1 => book
                .place_limit_fok(order(3, Side::Buy, 100, 10), &math(), &ids())
                .map(|_| ()),
            2 => book
                .execute_market_by_base(base(Side::Buy, 10), &ids())
                .map(|_| ()),
            3 => book
                .execute_market_by_quote(quote(Side::Buy, 1000), &math(), &ids())
                .map(|_| ()),
            4 => book
                .place_limit_gtc_with_events(order(3, Side::Buy, 100, 10), &ids())
                .map(|_| ()),
            _ => {
                let mut incoming = IncomingOrder::new(order(3, Side::Buy, 100, 10));
                let r = book.execute_fills(&mut incoming).map(|_| ());
                assert_eq!(incoming.remaining(), 10);
                r
            }
        };
        assert_eq!(result, Err(RejectReason::PrioritySpaceExhaustion));
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn reference_iceberg_refresh_level_aggregate_overflow_is_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg(iceberg(1, Side::Sell, 8, 3), &math(), &[])
        .unwrap();
    // 后续部分成交留下较小的当前可见片，而下一次补量片更大。
    book.place_limit_ioc(order(3, Side::Buy, 100, 2), &math(), &ids())
        .unwrap();
    book.rest(order(2, Side::Sell, 100, u64::MAX - 1)).unwrap();
    let before = snapshot(&book);
    assert_eq!(
        book.place_limit_ioc(order(4, Side::Buy, 100, 1), &math(), &ids()),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_last_safe_refresh_succeeds_then_single_fill_guard_rejects() {
    let mut book = seeded(Side::Sell);
    book.next_priority = u128::MAX - super::super::PRIORITY_GUARD - 1;
    let mut incoming = IncomingOrder::new(order(3, Side::Buy, 100, 3));
    book.execute_next_fill(&mut incoming).unwrap();
    assert_eq!(incoming.remaining(), 0);
    book.cancel(OrderId::new(2)).unwrap();
    let before = snapshot(&book);
    let mut incoming = IncomingOrder::new(order(4, Side::Buy, 100, 3));
    assert_eq!(
        book.execute_next_fill(&mut incoming),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(incoming.remaining(), 3);
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_incoming_visible_aggregate_overflow_is_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, Side::Sell, 100, u64::MAX - 2)).unwrap();
    let before = snapshot(&book);
    assert_eq!(
        book.place_iceberg(iceberg(3, Side::Sell, 8, 3), &math(), &[]),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_later_math_rejection_keeps_all_prior_virtual_fills() {
    for path in 0..3 {
        let mut book = seeded(Side::Sell);
        book.rest(order(7, Side::Sell, 101, 2)).unwrap();
        let before = snapshot(&book);
        let aligned = MarketMath::try_new(0, 0, 0, 2, 1).unwrap();
        let result = match path {
            0 => book
                .place_limit_ioc(order(3, Side::Buy, 102, 12), &aligned, &ids())
                .map(|_| ()),
            1 => book
                .execute_market_by_quote(quote(Side::Buy, 2000), &aligned, &ids())
                .map(|_| ()),
            _ => book
                .execute_market_by_quote_with_protection(
                    quote(Side::Buy, 2000),
                    &context(),
                    &aligned,
                    &ids(),
                )
                .map(|_| ()),
        };
        assert_eq!(result, Err(RejectReason::InvalidTick));
        assert_eq!(snapshot(&book), before);
    }
}
#[test]
fn reference_iceberg_full_total_math_checked_with_empty_book() {
    let mut book = ReferenceOrderBook::new();
    let before = snapshot(&book);
    let huge = MarketMath::try_new(0, 0, 19, 1, 1).unwrap();
    let candidate = IcebergOrder::try_new(
        order(1, Side::Sell, i64::MAX, u64::MAX),
        Qty::try_new(1).unwrap(),
    )
    .unwrap();
    assert_eq!(
        book.place_iceberg(candidate, &huge, &[]),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(snapshot(&book), before);
}
#[test]
fn reference_iceberg_protected_boundary_after_refresh_has_raw_event_order() {
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg(iceberg(1, Side::Sell, 5, 2), &math(), &[])
        .unwrap();
    book.rest(order(2, Side::Sell, 110, 2)).unwrap();
    let mut ctx = context();
    ctx.market_band_worst = Some(Price::try_new(100).unwrap());
    let events = book
        .execute_market_by_base_with_protection(base(Side::Buy, 9), &ctx, &math(), &ids())
        .unwrap();
    assert_eq!(
        trades(&events),
        vec![
            (OrderId::new(1), 2),
            (OrderId::new(1), 2),
            (OrderId::new(1), 1)
        ]
    );
    assert!(
        matches!(events.last(),Some(EngineEvent::MarketProtectionTriggered{next_price,..}) if next_price.get()==110)
    );
    assert_eq!(book.orders.len(), 1);
    assert_eq!(book.next_priority, 4);
}

fn order_at_100(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}
#[test]
fn reference_iceberg_refreshes_to_tail_and_reports_total() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ReferenceOrderBook::new();
    book.place_iceberg(
        IcebergOrder::try_new(order_at_100(1, Side::Sell, 8), Qty::try_new(3).unwrap()).unwrap(),
        &math,
        &[],
    )
    .unwrap();
    book.rest(order_at_100(2, Side::Sell, 2)).unwrap();
    let result = book
        .place_limit_ioc(
            order_at_100(3, Side::Buy, 6),
            &math,
            &[TradeId::new(1), TradeId::new(2), TradeId::new(3)],
        )
        .unwrap();
    let trades: Vec<_> = result
        .events()
        .iter()
        .filter_map(|e| {
            if let EngineEvent::Trade(t) = e {
                Some((t.maker_order_id, t.qty.get()))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        trades,
        vec![
            (OrderId::new(1), 3),
            (OrderId::new(2), 2),
            (OrderId::new(1), 1)
        ]
    );
    let maker = book.best(Side::Sell).unwrap();
    assert_eq!(
        (
            maker.visible_remaining(),
            maker.hidden_remaining(),
            maker.total_remaining()
        ),
        (2, 2, 4)
    );
    assert!(result.events().contains(&EngineEvent::IcebergRefreshed {
        order_id: OrderId::new(1),
        visible: Qty::try_new(3).unwrap()
    }));
    assert!(
        result
            .events()
            .contains(&EngineEvent::OrderPartiallyFilled {
                order_id: OrderId::new(1),
                remaining: Qty::try_new(5).unwrap()
            })
    );
}
