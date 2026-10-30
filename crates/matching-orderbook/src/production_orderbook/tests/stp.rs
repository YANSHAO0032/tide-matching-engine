//! Production Arena/PriceLevel 自成交防护计划的基本事件顺序验证。

use super::*;
use matching_domain::{CancelReason, OrderRemaining, StpKey, StpMode};
use proptest::prelude::*;

#[test]
fn quote_budget_below_one_lot_does_not_trigger_stp() {
    for mode in [
        StpMode::CancelMaker,
        StpMode::CancelTaker,
        StpMode::CancelBoth,
    ] {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        let mut maker = order(1, Side::Sell, 100, 2);
        maker.stp_key = StpKey::new(7);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
        let taker = MarketByQuote {
            order_id: OrderId::new(2),
            user_id: UserId::new(2),
            stp_key: StpKey::new(7),
            stp_mode: mode,
            side: Side::Buy,
            quote_budget: matching_domain::QuoteBudget::try_new(99).unwrap(),
            market_protection: None,
        };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let actual = p
            .execute_market_by_quote(taker.clone(), &math, &[])
            .unwrap();
        let expected = r.execute_market_by_quote(taker, &math, &[]).unwrap();
        assert_eq!(actual, expected);
        assert!(actual.events().is_empty());
        assert_eq!(actual.unused_quote().get(), 99);
        assert_eq!(p.order(OrderId::new(1)).unwrap().total_remaining(), 2);
        assert_eq!(r.orders(Side::Sell)[0].total_remaining(), 2);
    }
}

#[test]
fn quote_budget_after_trade_below_next_lot_does_not_trigger_stp() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    for (id, key, price) in [(1, 8, 100), (2, 7, 101)] {
        let mut maker = order(id, Side::Sell, price, 1);
        maker.user_id = UserId::new(id as u64);
        maker.stp_key = StpKey::new(key);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
    }
    let taker = MarketByQuote {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: StpKey::new(7),
        stp_mode: StpMode::CancelMaker,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(200).unwrap(),
        market_protection: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let actual = p
        .execute_market_by_quote(taker.clone(), &math, &[TradeId::new(1)])
        .unwrap();
    let expected = r
        .execute_market_by_quote(taker, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.events(), [EngineEvent::Trade(trade)] if trade.maker_order_id == OrderId::new(1))
    );
    assert_eq!(actual.unused_quote().get(), 100);
    assert!(p.order(OrderId::new(2)).is_some());
}

proptest! {
    #[test]
    fn generated_stp_pair_matches_independent_models(
        maker_user in 1u64..4, taker_user in 1u64..4,
        maker_key in 0u64..3, taker_key in 0u64..3,
        mode in 0u8..4, maker_qty in 1u64..20, taker_qty in 1u64..20,
    ) {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        let mut maker = order(1, Side::Sell, 100, maker_qty);
        maker.user_id = UserId::new(maker_user);
        maker.stp_key = StpKey::new(maker_key);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
        let mut taker = order(2, Side::Buy, 100, taker_qty);
        taker.user_id = UserId::new(taker_user);
        taker.stp_key = StpKey::new(taker_key);
        taker.stp_mode = match mode { 0 => StpMode::None, 1 => StpMode::CancelMaker, 2 => StpMode::CancelTaker, _ => StpMode::CancelBoth };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let ids = [TradeId::new(1)];
        let priority_before = p.next_queue_priority();
        let actual = p.place_limit_ioc(taker.clone(), &math, &ids).unwrap();
        let expected = r.place_limit_ioc(taker, &math, &ids).unwrap();
        prop_assert_eq!(&actual, &expected);
        prop_assert_eq!(p.next_queue_priority(), priority_before);
        let conflict = mode != 0 && (maker_user == taker_user || (maker_key != 0 && maker_key == taker_key));
        if conflict {
            prop_assert!(!actual.events().iter().any(|e| matches!(e, EngineEvent::Trade(_))));
            prop_assert_eq!(p.order(OrderId::new(1)).is_some(), mode == 2);
            if mode == 3 {
                prop_assert!(matches!(actual.events(), [EngineEvent::OrderCanceled { order_id: maker_id, .. }, EngineEvent::OrderCanceled { order_id: taker_id, .. }] if *maker_id == OrderId::new(1) && *taker_id == OrderId::new(2)), "双方撤单顺序错误");
            }
        } else {
            prop_assert!(actual.events().iter().any(|e| matches!(e, EngineEvent::Trade(_))));
        }
    }

    #[test]
    fn generated_fok_self_liquidity_is_excluded_atomically(own_qty in 1u64..20, external_qty in 1u64..20, extra in 1u64..10) {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        for (id, key, qty) in [(1, 7, own_qty), (2, 8, external_qty)] {
            let mut maker = order(id, Side::Sell, 100, qty);
            maker.user_id = UserId::new(id as u64);
            maker.stp_key = StpKey::new(key);
            p.rest(maker.clone()).unwrap();
            r.rest(maker).unwrap();
        }
        let mut taker = order(3, Side::Buy, 100, external_qty + extra);
        taker.user_id = UserId::new(3);
        taker.stp_key = StpKey::new(7);
        taker.stp_mode = StpMode::CancelMaker;
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        p.arena.reset_slot_accesses();
        let before_p = p.clone();
        let before_r = r.clone();
        let ids = [TradeId::new(1), TradeId::new(2)];
        prop_assert_eq!(p.place_limit_fok(taker.clone(), &math, &ids), Err(RejectReason::FokInsufficientLiquidity));
        prop_assert_eq!(r.place_limit_fok(taker, &math, &ids), Err(RejectReason::FokInsufficientLiquidity));
        p.arena.reset_slot_accesses();
        prop_assert_eq!(p, before_p);
        prop_assert_eq!(r, before_r);
    }

    #[test]
    fn generated_quote_stp_spends_only_real_trades(budget in 1u128..2000, own_qty in 1u64..10, external_qty in 1u64..10) {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        for (id, key, qty) in [(1, 7, own_qty), (2, 8, external_qty)] {
            let mut maker = order(id, Side::Sell, 100, qty);
            maker.user_id = UserId::new(id as u64);
            maker.stp_key = StpKey::new(key);
            p.rest(maker.clone()).unwrap();
            r.rest(maker).unwrap();
        }
        let taker = MarketByQuote {
            order_id: OrderId::new(3), user_id: UserId::new(3),
            stp_key: StpKey::new(7), stp_mode: StpMode::CancelMaker,
            side: Side::Buy, quote_budget: matching_domain::QuoteBudget::try_new(budget).unwrap(),
            market_protection: None,
        };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let ids = [TradeId::new(1)];
        let actual = p.execute_market_by_quote(taker.clone(), &math, &ids).unwrap();
        let expected = r.execute_market_by_quote(taker, &math, &ids).unwrap();
        prop_assert_eq!(&actual, &expected);
        let spent = actual.events().iter().filter_map(|event| match event {
            EngineEvent::Trade(trade) => Some(math.trade_notional(trade.price, trade.qty).unwrap().get()),
            _ => None,
        }).sum::<u128>();
        prop_assert_eq!(actual.spent_quote().get(), spent);
        prop_assert!(spent <= budget);
    }
}

#[test]
fn unsupported_mode_cannot_rest_in_either_book() {
    let mut input = order(55, Side::Buy, 100, 1);
    input.stp_mode = StpMode::DecrementAndCancel;
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    let production_before = production.clone();
    let reference_before = reference.clone();
    assert_eq!(
        production.rest(input.clone()),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        reference.rest(input),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(production, production_before);
    assert_eq!(reference, reference_before);
}

#[test]
fn unsupported_ioc_is_rejected_before_crossing_maker_changes() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    let maker = order(1, Side::Sell, 100, 2);
    production.rest(maker.clone()).unwrap();
    reference.rest(maker).unwrap();
    let mut taker = order(2, Side::Buy, 100, 1);
    taker.stp_mode = StpMode::DecrementAndCancel;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let production_before = production.clone();
    let reference_before = reference.clone();
    assert_eq!(
        production.place_limit_ioc(taker.clone(), &math, &[TradeId::new(1)]),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        reference.place_limit_ioc(taker, &math, &[TradeId::new(1)]),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(production, production_before);
    assert_eq!(reference, reference_before);
}

#[test]
fn cancel_maker_keeps_traversing_to_external_fifo_head() {
    let mut book = ProductionOrderBook::new(policy());
    let mut first = order(1, Side::Sell, 100, 2);
    first.user_id = UserId::new(1);
    first.stp_key = StpKey::new(7);
    rest_fixture(&mut book, first);
    let mut second = order(2, Side::Sell, 100, 3);
    second.user_id = UserId::new(2);
    second.stp_key = StpKey::new(7);
    rest_fixture(&mut book, second);
    let mut external = order(3, Side::Sell, 100, 4);
    external.user_id = UserId::new(3);
    external.stp_key = StpKey::new(9);
    rest_fixture(&mut book, external);

    let mut taker = order(4, Side::Buy, 100, 4);
    taker.user_id = UserId::new(4);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let events = book
        .place_limit_gtc_with_events(taker, &[TradeId::new(10)])
        .unwrap();

    assert!(
        matches!(events[0], EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionMaker } if order_id == OrderId::new(1) && qty.get() == 2)
    );
    assert!(
        matches!(events[1], EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionMaker } if order_id == OrderId::new(2) && qty.get() == 3)
    );
    assert!(
        matches!(events[2], EngineEvent::Trade(TradeEvent { maker_order_id, taker_order_id, qty, .. }) if maker_order_id == OrderId::new(3) && taker_order_id == OrderId::new(4) && qty.get() == 4)
    );
    assert!(book.order(OrderId::new(1)).is_none());
    assert!(book.order(OrderId::new(2)).is_none());
    assert!(book.order(OrderId::new(3)).is_none());
}

#[test]
fn fok_cancel_maker_insufficient_external_liquidity_preserves_production_book() {
    let mut book = ProductionOrderBook::new(policy());
    let mut own = order(1, Side::Sell, 100, 5);
    own.stp_key = StpKey::new(7);
    book.rest(own).unwrap();
    let mut external = order(2, Side::Sell, 100, 5);
    external.stp_key = StpKey::new(8);
    book.rest(external).unwrap();
    book.arena.reset_slot_accesses();
    let before = book.clone();
    let mut taker = order(3, Side::Buy, 100, 10);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    assert_eq!(
        book.place_limit_fok(taker, &math, &[TradeId::new(1), TradeId::new(2)]),
        Err(RejectReason::FokInsufficientLiquidity)
    );
    book.arena.reset_slot_accesses();
    assert_eq!(book, before);
}

#[test]
fn fok_cancel_maker_sufficient_liquidity_commits_cancel_then_external_trade() {
    let mut book = ProductionOrderBook::new(policy());
    let mut own = order(1, Side::Sell, 100, 5);
    own.stp_key = StpKey::new(7);
    book.rest(own).unwrap();
    let mut external = order(2, Side::Sell, 100, 10);
    external.stp_key = StpKey::new(8);
    book.rest(external).unwrap();
    let mut taker = order(3, Side::Buy, 100, 10);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book
        .place_limit_fok(taker, &math, &[TradeId::new(1)])
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, reason: CancelReason::SelfTradePreventionMaker, .. }, EngineEvent::Trade(trade)] if *order_id == OrderId::new(1) && trade.maker_order_id == OrderId::new(2) && trade.qty.get() == 10)
    );
    assert!(book.order(OrderId::new(1)).is_none());
    assert!(book.order(OrderId::new(2)).is_none());
}

#[test]
fn fok_cancel_both_discards_prior_simulated_trades() {
    let mut book = ProductionOrderBook::new(policy());
    let mut external = order(1, Side::Sell, 100, 2);
    external.stp_key = StpKey::new(8);
    book.rest(external).unwrap();
    let mut own = order(2, Side::Sell, 100, 5);
    own.stp_key = StpKey::new(7);
    book.rest(own).unwrap();
    let mut taker = order(3, Side::Buy, 100, 5);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelBoth;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book.place_limit_fok(taker, &math, &[]).unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id: maker, reason: CancelReason::SelfTradePreventionMaker, .. }, EngineEvent::OrderCanceled { order_id: taker, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionBoth }] if *maker == OrderId::new(2) && *taker == OrderId::new(3) && qty.get() == 5)
    );
    assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 2);
    assert!(book.order(OrderId::new(2)).is_none());
}

#[test]
fn fok_cancel_taker_discards_prior_simulated_fills_in_both_models() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    for (id, key) in [(1, 8), (2, 7)] {
        let mut maker = order(id, Side::Sell, 100, 2);
        maker.user_id = UserId::new(id as u64);
        maker.stp_key = StpKey::new(key);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
    }
    let mut taker = order(3, Side::Buy, 100, 4);
    taker.user_id = UserId::new(3);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelTaker;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let actual = p
        .place_limit_fok(taker.clone(), &math, &[TradeId::new(1)])
        .unwrap();
    let expected = r.place_limit_fok(taker, &math, &[TradeId::new(1)]).unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.events(), [EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionTaker }] if *order_id == OrderId::new(3) && qty.get() == 4)
    );
    assert_eq!(p.order(OrderId::new(1)).unwrap().total_remaining(), 2);
    assert_eq!(p.order(OrderId::new(2)).unwrap().total_remaining(), 2);
}

#[test]
fn fok_cancel_maker_removes_entire_iceberg_without_refresh() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut own = order(1, Side::Sell, 100, 10);
    own.stp_key = StpKey::new(7);
    let iceberg = matching_domain::IcebergOrder::try_new(own, Qty::try_new(2).unwrap()).unwrap();
    book.place_iceberg_post_only(iceberg, &math).unwrap();
    let mut external = order(2, Side::Sell, 100, 3);
    external.stp_key = StpKey::new(8);
    book.rest(external).unwrap();
    let mut taker = order(3, Side::Buy, 100, 3);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let next = book.next_queue_priority();
    let result = book
        .place_limit_fok(taker, &math, &[TradeId::new(1)])
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionMaker }, EngineEvent::Trade(_)] if *order_id == OrderId::new(1) && qty.get() == 10)
    );
    assert!(book.order(OrderId::new(1)).is_none());
    assert_eq!(book.next_queue_priority(), next);
}

#[test]
fn gtc_cancel_maker_removes_iceberg_parent_and_continues() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut own = order(1, Side::Sell, 100, 10);
    own.stp_key = StpKey::new(7);
    let iceberg = matching_domain::IcebergOrder::try_new(own, Qty::try_new(2).unwrap()).unwrap();
    book.place_iceberg_post_only(iceberg, &math).unwrap();
    let mut external = order(2, Side::Sell, 100, 3);
    external.stp_key = StpKey::new(8);
    book.rest(external).unwrap();
    let mut taker = order(3, Side::Buy, 100, 3);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let result = book
        .place_limit_gtc_with_events(taker, &[TradeId::new(1)])
        .unwrap();
    assert!(
        matches!(result.as_slice(), [EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionMaker }, EngineEvent::Trade(_)] if *order_id == OrderId::new(1) && qty.get() == 10)
    );
    assert!(book.order(OrderId::new(1)).is_none());
}

#[test]
fn ioc_cancel_taker_keeps_maker_and_uses_stp_reason() {
    let mut book = ProductionOrderBook::new(policy());
    let mut maker = order(1, Side::Sell, 100, 5);
    maker.stp_key = StpKey::new(7);
    book.rest(maker).unwrap();
    let mut taker = order(2, Side::Buy, 100, 3);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelTaker;
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let result = book.place_limit_ioc(taker, &math, &[]).unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, remaining: OrderRemaining::Base(qty), reason: CancelReason::SelfTradePreventionTaker }] if *order_id == OrderId::new(2) && qty.get() == 3)
    );
    assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 5);
}

#[test]
fn fok_stp_external_iceberg_replenishes_in_fifo_order() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut own = order(1, Side::Sell, 100, 1);
    own.stp_key = StpKey::new(7);
    book.rest(own).unwrap();
    let mut external = order(2, Side::Sell, 100, 4);
    external.stp_key = StpKey::new(8);
    book.place_iceberg_post_only(
        matching_domain::IcebergOrder::try_new(external, Qty::try_new(2).unwrap()).unwrap(),
        &math,
    )
    .unwrap();
    let mut taker = order(3, Side::Buy, 100, 4);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let result = book
        .place_limit_fok(taker, &math, &[TradeId::new(1), TradeId::new(2)])
        .unwrap();
    assert!(
        matches!(result.events(), [EngineEvent::OrderCanceled { order_id, .. }, EngineEvent::Trade(first), EngineEvent::OrderPartiallyFilled { .. }, EngineEvent::IcebergRefreshed { .. }, EngineEvent::Trade(second), EngineEvent::OrderFilled { .. }] if *order_id == OrderId::new(1) && first.maker_order_id == OrderId::new(2) && second.maker_order_id == OrderId::new(2))
    );
    assert!(book.order(OrderId::new(2)).is_none());
}

#[test]
fn market_quote_cancel_taker_reports_quote_remainder() {
    let mut book = ProductionOrderBook::new(policy());
    let mut maker = order(1, Side::Sell, 100, 2);
    maker.stp_key = StpKey::new(7);
    book.rest(maker).unwrap();
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
    assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 2);
}

#[test]
fn market_base_cancel_maker_continues_and_matches_reference_events() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    for (id, key) in [(1, 7), (2, 8)] {
        let mut maker = order(id, Side::Sell, 100, 2);
        maker.stp_key = StpKey::new(key);
        production.rest(maker.clone()).unwrap();
        reference.rest(maker).unwrap();
    }
    let taker = MarketByBase {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: StpKey::new(7),
        stp_mode: StpMode::CancelMaker,
        side: Side::Buy,
        qty: Qty::try_new(2).unwrap(),
        market_protection: None,
    };
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let actual = production
        .execute_market_by_base_with_protection(taker.clone(), &context, &math, &[TradeId::new(1)])
        .unwrap();
    let expected = reference
        .execute_market_by_base_with_protection(taker, &context, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.as_slice(), [EngineEvent::OrderCanceled { order_id, reason: CancelReason::SelfTradePreventionMaker, .. }, EngineEvent::Trade(trade)] if *order_id == OrderId::new(1) && trade.maker_order_id == OrderId::new(2))
    );
}

#[test]
fn market_quote_cancel_maker_preserves_budget_and_exact_model_parity() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    for (id, key) in [(1, 7), (2, 8)] {
        let mut maker = order(id, Side::Sell, 100, 2);
        maker.stp_key = StpKey::new(key);
        production.rest(maker.clone()).unwrap();
        reference.rest(maker).unwrap();
    }
    let taker = MarketByQuote {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        stp_key: StpKey::new(7),
        stp_mode: StpMode::CancelMaker,
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(1000).unwrap(),
        market_protection: None,
    };
    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let actual = production
        .execute_market_by_quote_with_protection(taker.clone(), &context, &math, &[TradeId::new(1)])
        .unwrap();
    let expected = reference
        .execute_market_by_quote_with_protection(taker, &context, &math, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.spent_quote().get(), 200);
    assert_eq!(actual.unused_quote().get(), 800);
    assert!(
        matches!(actual.events(), [EngineEvent::OrderCanceled { order_id, reason: CancelReason::SelfTradePreventionMaker, .. }, EngineEvent::Trade(trade)] if *order_id == OrderId::new(1) && trade.maker_order_id == OrderId::new(2))
    );
}

#[test]
fn unprotected_market_orders_preserve_stp_events_and_reference_parity() {
    for quote in [false, true] {
        let mut p = ProductionOrderBook::new(policy());
        let mut r = crate::ReferenceOrderBook::new();
        let mut maker = order(1, Side::Sell, 100, 2);
        maker.stp_key = StpKey::new(7);
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
        if quote {
            let taker = MarketByQuote {
                order_id: OrderId::new(2),
                user_id: UserId::new(2),
                stp_key: StpKey::new(7),
                stp_mode: StpMode::CancelTaker,
                side: Side::Buy,
                quote_budget: matching_domain::QuoteBudget::try_new(1000).unwrap(),
                market_protection: None,
            };
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            let pe = p
                .execute_market_by_quote(taker.clone(), &math, &[])
                .unwrap();
            let re = r.execute_market_by_quote(taker, &math, &[]).unwrap();
            assert_eq!(pe, re);
            assert!(
                matches!(pe.events(), [EngineEvent::OrderCanceled { remaining: OrderRemaining::Quote(q), reason: CancelReason::SelfTradePreventionTaker, .. }] if q.get() == 1000)
            );
        } else {
            let taker = MarketByBase {
                order_id: OrderId::new(2),
                user_id: UserId::new(2),
                stp_key: StpKey::new(7),
                stp_mode: StpMode::CancelTaker,
                side: Side::Buy,
                qty: Qty::try_new(3).unwrap(),
                market_protection: None,
            };
            let pe = p
                .execute_market_by_base(
                    taker.clone(),
                    &[],
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                )
                .unwrap();
            let re = r
                .execute_market_by_base(
                    taker,
                    &[],
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                )
                .unwrap();
            assert_eq!(pe, re);
            assert!(
                matches!(pe.events(), [EngineEvent::OrderCanceled { remaining: OrderRemaining::Base(q), reason: CancelReason::SelfTradePreventionTaker, .. }] if q.get() == 3)
            );
        }
    }
}

#[test]
fn gtc_public_entry_retains_stp_cancel_event() {
    let mut book = ProductionOrderBook::new(policy());
    let mut maker = order(1, Side::Sell, 100, 2);
    maker.stp_key = StpKey::new(7);
    book.rest(maker).unwrap();
    let mut taker = order(2, Side::Buy, 100, 3);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelTaker;
    let result = book.place_limit_gtc(taker, &[]).unwrap();
    assert!(matches!(
        result.events(),
        [EngineEvent::OrderCanceled {
            reason: CancelReason::SelfTradePreventionTaker,
            ..
        }]
    ));
    assert!(result.trades().is_empty());
}

#[test]
fn trade_only_primitives_reject_enabled_stp_before_mutation() {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = crate::ReferenceOrderBook::new();
    let maker = order(1, Side::Sell, 100, 2);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let mut taker = order(2, Side::Buy, 100, 2);
    taker.stp_mode = StpMode::CancelTaker;
    let mut pi = IncomingOrder::new(taker.clone());
    let mut ri = IncomingOrder::new(taker);
    let ps = p.arena.test_snapshot();
    let rs = r.clone();
    assert_eq!(
        p.execute_next_fill_trade(&mut pi, TradeId::new(1)),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(
        r.execute_next_fill_trade(&mut ri, TradeId::new(1)),
        Err(RejectReason::InvalidOrderCombination)
    );
    assert_eq!(p.arena.test_snapshot(), ps);
    assert_eq!(r, rs);
    assert_eq!(pi.remaining(), 2);
    assert_eq!(ri.remaining(), 2);
}

#[test]
fn iceberg_taker_cancel_maker_rests_only_remaining_iceberg() {
    let mut book = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut own = order(1, Side::Sell, 100, 1);
    own.stp_key = StpKey::new(7);
    let mut external = order(2, Side::Sell, 100, 2);
    external.stp_key = StpKey::new(8);
    book.rest(own).unwrap();
    book.rest(external).unwrap();
    let mut taker = order(3, Side::Buy, 100, 5);
    taker.stp_key = StpKey::new(7);
    taker.stp_mode = StpMode::CancelMaker;
    let input = matching_domain::IcebergOrder::try_new(taker, Qty::try_new(2).unwrap()).unwrap();
    let events = book
        .place_iceberg(input, &math, &[TradeId::new(1)])
        .unwrap();
    assert!(
        matches!(events.as_slice(), [EngineEvent::OrderCanceled { order_id, .. }, EngineEvent::Trade(trade), EngineEvent::OrderPartiallyFilled { remaining, .. }] if *order_id == OrderId::new(1) && trade.maker_order_id == OrderId::new(2) && remaining.get() == 3)
    );
    let resting = book.order(OrderId::new(3)).unwrap();
    assert_eq!(resting.total_remaining(), 3);
    assert_eq!(resting.visible_remaining(), 2);
}
