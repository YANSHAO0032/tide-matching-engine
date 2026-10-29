//! 参考订单簿自成交防护的事件和连续遍历契约。

use super::*;
use matching_domain::{CancelReason, OrderRemaining, StpKey, StpMode, UserId};

fn order(id: u128, user: u64, key: u64, mode: StpMode, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        stp_key: StpKey::new(key),
        stp_mode: mode,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

#[test]
fn cancel_maker_removes_consecutive_self_makers_then_trades_external_maker() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 7, StpMode::None, Side::Sell, 2))
        .unwrap();
    book.rest(order(2, 2, 7, StpMode::None, Side::Sell, 3))
        .unwrap();
    book.rest(order(3, 3, 9, StpMode::None, Side::Sell, 4))
        .unwrap();

    let events = book
        .place_limit_gtc_with_events(
            order(4, 4, 7, StpMode::CancelMaker, Side::Buy, 4),
            &[TradeId::new(10)],
        )
        .unwrap();

    assert_eq!(
        events,
        vec![
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(1),
                remaining: OrderRemaining::Base(Qty::try_new(2).unwrap()),
                reason: CancelReason::SelfTradePreventionMaker,
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(2),
                remaining: OrderRemaining::Base(Qty::try_new(3).unwrap()),
                reason: CancelReason::SelfTradePreventionMaker,
            },
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(10),
                maker_order_id: OrderId::new(3),
                taker_order_id: OrderId::new(4),
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(4).unwrap(),
                maker_side: Side::Sell,
            }),
        ]
    );
    assert!(!book.contains_active_order(OrderId::new(1)));
    assert!(!book.contains_active_order(OrderId::new(2)));
    assert!(!book.contains_active_order(OrderId::new(3)));
}

#[test]
fn cancel_taker_keeps_maker_and_never_rests_taker_remainder() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 0, StpMode::None, Side::Sell, 10))
        .unwrap();

    let events = book
        .place_limit_gtc_with_events(order(2, 1, 0, StpMode::CancelTaker, Side::Buy, 5), &[])
        .unwrap();

    assert_eq!(
        events,
        vec![EngineEvent::OrderCanceled {
            order_id: OrderId::new(2),
            remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
            reason: CancelReason::SelfTradePreventionTaker,
        }]
    );
    assert_eq!(book.orders(Side::Sell)[0].remaining(), 10);
    assert!(!book.contains_active_order(OrderId::new(2)));
}

#[test]
fn fok_cancel_maker_insufficient_external_liquidity_is_atomic() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 7, StpMode::None, Side::Sell, 5))
        .unwrap();
    book.rest(order(2, 2, 8, StpMode::None, Side::Sell, 5))
        .unwrap();
    let before = book.clone();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book.place_limit_fok(
        order(3, 3, 7, StpMode::CancelMaker, Side::Buy, 10),
        &math,
        &[TradeId::new(1), TradeId::new(2)],
    );
    assert_eq!(result, Err(RejectReason::FokInsufficientLiquidity));
    assert_eq!(book, before);
}

#[test]
fn fok_cancel_maker_sufficient_external_liquidity_commits_in_encounter_order() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 7, StpMode::None, Side::Sell, 5))
        .unwrap();
    book.rest(order(2, 2, 8, StpMode::None, Side::Sell, 10))
        .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book
        .place_limit_fok(
            order(3, 3, 7, StpMode::CancelMaker, Side::Buy, 10),
            &math,
            &[TradeId::new(1)],
        )
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, reason: CancelReason::SelfTradePreventionMaker, .. }, EngineEvent::Trade(trade)] if *order_id == OrderId::new(1) && trade.maker_order_id == OrderId::new(2) && trade.qty.get() == 10)
    );
    assert!(!book.contains_active_order(OrderId::new(1)));
    assert!(!book.contains_active_order(OrderId::new(2)));
}

#[test]
fn fok_cancel_taker_does_not_commit_preceding_external_fills() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 8, StpMode::None, Side::Sell, 2))
        .unwrap();
    book.rest(order(2, 2, 7, StpMode::None, Side::Sell, 5))
        .unwrap();
    let before = book.clone();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book
        .place_limit_fok(
            order(3, 3, 7, StpMode::CancelTaker, Side::Buy, 5),
            &math,
            &[TradeId::new(1)],
        )
        .unwrap();
    assert_eq!(
        result.events(),
        &[EngineEvent::OrderCanceled {
            order_id: OrderId::new(3),
            remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
            reason: CancelReason::SelfTradePreventionTaker
        }]
    );
    assert_eq!(book, before);
}

#[test]
fn fok_cancel_both_discards_simulated_fills_and_cancels_only_conflicting_maker() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 8, StpMode::None, Side::Sell, 2))
        .unwrap();
    book.rest(order(2, 2, 7, StpMode::None, Side::Sell, 5))
        .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book
        .place_limit_fok(
            order(3, 3, 7, StpMode::CancelBoth, Side::Buy, 5),
            &math,
            &[TradeId::new(1)],
        )
        .unwrap();
    assert_eq!(
        result.events(),
        &[
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(2),
                remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: CancelReason::SelfTradePreventionMaker
            },
            EngineEvent::OrderCanceled {
                order_id: OrderId::new(3),
                remaining: OrderRemaining::Base(Qty::try_new(5).unwrap()),
                reason: CancelReason::SelfTradePreventionBoth
            },
        ]
    );
    assert_eq!(book.orders(Side::Sell)[0].remaining(), 2);
    assert!(!book.contains_active_order(OrderId::new(2)));
}

#[test]
fn market_quote_cancel_taker_reports_unspent_quote() {
    let mut book = ReferenceOrderBook::new();
    book.rest(order(1, 1, 7, StpMode::None, Side::Sell, 2))
        .unwrap();
    let taker = MarketByQuote {
        order_id: OrderId::new(2),
        user_id: UserId::new(2),
        stp_key: StpKey::new(7),
        stp_mode: StpMode::CancelTaker,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(1000).unwrap(),
        market_protection: None,
    };
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book
        .execute_market_by_quote_with_protection(taker, &context, &math, &[])
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Quote(q), reason: CancelReason::SelfTradePreventionTaker }] if *order_id == OrderId::new(2) && q.get() == 1000)
    );
    assert_eq!(result.spent_quote().get(), 0);
    assert_eq!(result.unused_quote().get(), 1000);
}
