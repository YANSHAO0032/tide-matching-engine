use super::*;
use matching_domain::{QuoteBudget, UserId};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn market(id: u128, side: Side, budget: u128) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        quote_budget: QuoteBudget::try_new(budget).unwrap(),
        market_protection: None,
    }
}

fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitOrder, u64, QueuePriority)>, u128) {
    (
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect(),
        book.next_priority,
    )
}

#[test]
fn market_by_quote_execution_empty_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        let before = snapshot(&book);

        let result = book
            .execute_market_by_quote(market(100, side, 1000), &math(), &[])
            .unwrap();

        assert!(result.trades().is_empty());
        assert_eq!(result.spent_quote().get(), 0);
        assert_eq!(result.unused_quote().get(), 1000);
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn market_by_quote_execution_partial_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, 100, 10)).unwrap();
        book.rest(limit(2, maker_side, 100, 5)).unwrap();

        let next_before = book.next_priority;
        let priority_before = book.orders[0].priority();

        let result = book
            .execute_market_by_quote(market(100, taker_side, 350), &math(), &[TradeId::new(500)])
            .unwrap();

        assert_eq!(result.trades().len(), 1);

        let trade = &result.trades()[0];

        assert_eq!(trade.trade_id, TradeId::new(500));
        assert_eq!(trade.maker_order_id, OrderId::new(1));
        assert_eq!(trade.taker_order_id, OrderId::new(100));
        assert_eq!(trade.maker_side, maker_side);
        assert_eq!(trade.price.get(), 100);
        assert_eq!(trade.qty.get(), 3);

        assert_eq!(result.spent_quote().get(), 300);
        assert_eq!(result.unused_quote().get(), 50);

        // 部分成交挂单不删除、不移动。
        assert_eq!(book.orders.len(), 2);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].priority(), priority_before);
        assert_eq!(book.orders[0].original_order().order_id, OrderId::new(1),);

        assert_eq!(book.orders[1].remaining(), 5);
        assert_eq!(book.next_priority, next_before);

        assert!(
            !book
                .orders
                .iter()
                .any(|maker| { maker.original_order().order_id == OrderId::new(100) })
        );
    }
}

#[test]
fn market_by_quote_execution_multi_level_fifo_both_sides() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = ReferenceOrderBook::new();

        // 标识符数值顺序与实际先进先出顺序不同。
        book.rest(limit(20, maker_side, worse, 4)).unwrap();
        book.rest(limit(30, maker_side, better, 3)).unwrap();
        book.rest(limit(10, maker_side, better, 5)).unwrap();

        let next_before = book.next_priority;

        let expected_spent = 8 * better as u128 + 4 * worse as u128;

        let result = book
            .execute_market_by_quote(
                market(100, taker_side, 2000),
                &math(),
                &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
            )
            .unwrap();

        let trades = result.trades();

        assert_eq!(trades.len(), 3);

        assert_eq!(
            trades
                .iter()
                .map(|trade| trade.maker_order_id)
                .collect::<Vec<_>>(),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20),],
        );

        assert_eq!(
            trades
                .iter()
                .map(|trade| trade.price.get())
                .collect::<Vec<_>>(),
            vec![better, better, worse],
        );

        assert_eq!(
            trades
                .iter()
                .map(|trade| trade.qty.get())
                .collect::<Vec<_>>(),
            vec![3, 5, 4],
        );

        assert_eq!(
            trades
                .iter()
                .map(|trade| trade.trade_id)
                .collect::<Vec<_>>(),
            vec![TradeId::new(501), TradeId::new(502), TradeId::new(503),],
        );

        for trade in trades {
            assert_eq!(trade.maker_side, maker_side);
            assert_eq!(trade.taker_order_id, OrderId::new(100));
        }

        assert_eq!(result.spent_quote().get(), expected_spent);
        assert_eq!(result.unused_quote().get(), 2000 - expected_spent,);

        // 所有挂单已耗尽，市价吃单方不挂簿。
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, next_before);
    }
}

#[test]
fn market_by_quote_execution_dust_and_no_rest() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, 100, 10)).unwrap();

        let before = snapshot(&book);

        // 99 报价资产不足成交 1 手。
        let result = book
            .execute_market_by_quote(market(100, taker_side, 99), &math(), &[])
            .unwrap();

        assert!(result.trades().is_empty());
        assert_eq!(result.spent_quote().get(), 0);
        assert_eq!(result.unused_quote().get(), 99);
        assert_eq!(snapshot(&book), before);

        // 能先成交、之后产生不足一手的预算的情况。
        let result = book
            .execute_market_by_quote(market(101, taker_side, 350), &math(), &[TradeId::new(600)])
            .unwrap();

        assert_eq!(result.trades().len(), 1);
        assert_eq!(result.trades()[0].qty.get(), 3);
        assert_eq!(result.spent_quote().get(), 300);
        assert_eq!(result.unused_quote().get(), 50);

        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.next_priority, before.1);

        assert!(
            !book
                .orders
                .iter()
                .any(|maker| { matches!(maker.original_order().order_id.get(), 100 | 101) })
        );
    }
}

#[test]
fn market_by_quote_execution_duplicate_is_atomic() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, 100, 10)).unwrap();

        let before = snapshot(&book);

        // TradeId 为空，但重复标识符必须优先于足量检查。
        let result = book.execute_market_by_quote(market(1, taker_side, 1000), &math(), &[]);

        assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn market_by_quote_execution_insufficient_trade_ids_is_atomic() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(30, maker_side, better, 3)).unwrap();
        book.rest(limit(10, maker_side, worse, 5)).unwrap();

        let before = snapshot(&book);

        // 两张挂单均可成交，需要两个 TradeId。
        // 仅提供一个，必须在第一笔状态变更前触发不变量断言。
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.execute_market_by_quote(
                market(100, taker_side, 2000),
                &math(),
                &[TradeId::new(700)],
            );
        }));

        assert!(result.is_err());

        // 包含原始订单、剩余量、优先级、源 Vec 顺序及计数器。
        assert_eq!(snapshot(&book), before);
    }
}
