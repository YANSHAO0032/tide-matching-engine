use super::*;
use matching_domain::market_order::ProtectedMarketByQuoteExecution;
use matching_domain::{MarketProtection, QuoteBudget, UserId};
use std::panic::{AssertUnwindSafe, catch_unwind};

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
fn protected_quote_execution_multilevel_sources() {
    let p = |v| Price::try_new(v).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    let cases = [
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            110,
            ProtectionSource::User,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            110,
            100,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Buy,
            Side::Sell,
            90,
            110,
            120,
            100,
            100,
            ProtectionSource::Both,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            90,
            ProtectionSource::User,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            90,
            100,
            ProtectionSource::MarketBand,
        ),
        (
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            100,
            100,
            ProtectionSource::Both,
        ),
    ];

    for (side, maker_side, better, outside, farther, user, band, source) in cases {
        let mut book = ReferenceOrderBook::new();

        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        let mut incoming = market(100, side, 2000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(user)),
        });

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(band)),
        };

        let priority_before = book.next_priority;

        let actual = book
            .execute_market_by_quote_with_protection(
                incoming,
                &context,
                &math,
                &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
            )
            .unwrap();

        let trade = |tid, maker_id, price, qty| {
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(tid),
                maker_order_id: OrderId::new(maker_id),
                taker_order_id: OrderId::new(100),
                price: p(price),
                qty: Qty::try_new(qty).unwrap(),
                maker_side,
            })
        };

        let expected_spent = if side == Side::Buy { 880 } else { 920 };

        let expected = ProtectedMarketByQuoteExecution::new(
            vec![
                trade(701, 30, better, 2),
                trade(702, 40, 100, 4),
                trade(703, 10, 100, 3),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source,
                },
            ],
            QuoteAmount::new(expected_spent),
            QuoteAmount::new(2000 - expected_spent),
        );

        assert_eq!(actual, expected);

        // 已成交挂单删除，越界与更远挂单完整保留。
        assert_eq!(
            snapshot(&book),
            (
                vec![
                    (limit(90, maker_side, farther, 2), 2, QueuePriority::new(0)),
                    (limit(50, maker_side, outside, 1), 1, QueuePriority::new(4)),
                ],
                priority_before,
            )
        );

        assert!(
            !book
                .orders
                .iter()
                .any(|maker| maker.original_order().order_id == OrderId::new(100))
        );
    }
}
#[test]
fn protected_quote_execution_boundary_dust_and_immediate_stop() {
    let p = |v| Price::try_new(v).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, outside) in [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)] {
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(100)),
        };

        let execute = |book: &mut ReferenceOrderBook, budget, ids: &[TradeId]| {
            book.execute_market_by_quote_with_protection(
                market(100, side, budget),
                &context,
                &math,
                ids,
            )
            .unwrap()
        };

        // 1. 空簿：事件为空、全部预算未使用。
        let mut book = ReferenceOrderBook::new();
        let before = snapshot(&book);
        let actual = execute(&mut book, 1000, &[]);

        assert_eq!(
            actual,
            ProtectedMarketByQuoteExecution::new(
                vec![],
                QuoteAmount::new(0),
                QuoteAmount::new(1000),
            )
        );
        assert_eq!(snapshot(&book), before);

        // 2. 首档越界，且预算不足一手：
        // 必须先触发保护，而不是按不足一手的预算停止。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, outside, 5)).unwrap();
        let before = snapshot(&book);

        let actual = execute(&mut book, 1, &[]);

        assert_eq!(
            actual,
            ProtectedMarketByQuoteExecution::new(
                vec![EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::MarketBand,
                }],
                QuoteAmount::new(0),
                QuoteAmount::new(1),
            )
        );
        assert_eq!(snapshot(&book), before);

        // 3. 挂单正好在边界上；预算完全耗尽，
        // 后面即使越界，也不能产生触发。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 2)).unwrap();
        book.rest(limit(2, maker_side, outside, 4)).unwrap();

        let actual = execute(&mut book, 200, &[TradeId::new(700)]);

        assert_eq!(
            actual,
            ProtectedMarketByQuoteExecution::new(
                vec![EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(700),
                    maker_order_id: OrderId::new(1),
                    taker_order_id: OrderId::new(100),
                    price: p(100),
                    qty: Qty::try_new(2).unwrap(),
                    maker_side,
                })],
                QuoteAmount::new(200),
                QuoteAmount::new(0),
            )
        );

        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].original_order().order_id, OrderId::new(2));

        // 4. 部分挂单成交：剩余量=7，
        // 原优先级保留；剩余 50 形成不足一手的预算。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 10)).unwrap();
        let priority_before = book.orders[0].priority();

        let actual = execute(&mut book, 350, &[TradeId::new(701)]);

        assert_eq!(actual.events().len(), 1);
        assert!(matches!(
            &actual.events()[0],
            EngineEvent::Trade(trade)
                if trade.maker_order_id == OrderId::new(1)
                    && trade.qty == Qty::try_new(3).unwrap()
        ));
        assert_eq!(actual.spent_quote(), QuoteAmount::new(300));
        assert_eq!(actual.unused_quote(), QuoteAmount::new(50));
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].priority(), priority_before);
        assert_eq!(book.next_priority, 1);

        // 5. 首档在边界内，但初始预算不足一手。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 5)).unwrap();
        let before = snapshot(&book);

        let actual = execute(&mut book, 99, &[]);

        assert_eq!(actual.events(), &[]);
        assert_eq!(actual.spent_quote(), QuoteAmount::new(0));
        assert_eq!(actual.unused_quote(), QuoteAmount::new(99));
        assert_eq!(snapshot(&book), before);

        // 6. 成交价 100 用掉 200，还剩 5。
        // 下一挂单越界：优先触发保护，而不是不足一手的预算。
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100, 2)).unwrap();
        book.rest(limit(2, maker_side, outside, 4)).unwrap();

        let actual = execute(&mut book, 205, &[TradeId::new(702)]);

        assert_eq!(actual.events().len(), 2);
        assert!(matches!(actual.events()[0], EngineEvent::Trade(_)));
        assert_eq!(
            actual.events()[1],
            EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            }
        );
        assert_eq!(actual.spent_quote(), QuoteAmount::new(200));
        assert_eq!(actual.unused_quote(), QuoteAmount::new(5));
        assert_eq!(book.orders.len(), 1);
        assert_eq!(book.orders[0].original_order().order_id, OrderId::new(2));
    }
}
#[test]
fn protected_quote_execution_without_boundary_scans_all() {
    let p = |v| Price::try_new(v).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, better, outside, farther, spent) in [
        (Side::Buy, Side::Sell, 90, 110, 120, 1230),
        (Side::Sell, Side::Buy, 110, 90, 80, 1170),
    ] {
        let mut book = ReferenceOrderBook::new();

        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: None,
        };

        let before_priority = book.next_priority;

        let trade_ids = [
            TradeId::new(701),
            TradeId::new(702),
            TradeId::new(703),
            TradeId::new(704),
            TradeId::new(705),
        ];

        let actual = book
            .execute_market_by_quote_with_protection(
                market(100, side, 2000),
                &context,
                &math,
                &trade_ids,
            )
            .unwrap();

        let expected_makers = [
            (30, better, 2),
            (40, 100, 4),
            (10, 100, 3),
            (50, outside, 1),
            (90, farther, 2),
        ];

        let expected_events = expected_makers
            .iter()
            .enumerate()
            .map(|(index, &(maker_id, price, qty))| {
                EngineEvent::Trade(TradeEvent {
                    trade_id: trade_ids[index],
                    maker_order_id: OrderId::new(maker_id),
                    taker_order_id: OrderId::new(100),
                    price: p(price),
                    qty: Qty::try_new(qty).unwrap(),
                    maker_side,
                })
            })
            .collect();

        assert_eq!(
            actual,
            ProtectedMarketByQuoteExecution::new(
                expected_events,
                QuoteAmount::new(spent),
                QuoteAmount::new(2000 - spent),
            )
        );

        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, before_priority);
    }
}
fn assert_protected_quote_rejected_atomically(
    book: &mut ReferenceOrderBook,
    incoming: MarketByQuote,
    context: &CommandStartProtectionContext,
    math: &MarketMath,
    expected: RejectReason,
) {
    let before = snapshot(book);

    assert_eq!(
        book.execute_market_by_quote_with_protection(incoming, context, math, &[],),
        Err(expected)
    );

    assert_eq!(snapshot(book), before);
}
#[test]
fn protected_quote_execution_rejections_are_atomic() {
    let p = |v| Price::try_new(v).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(100_000)),
        };

        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        // 重复标识符优先于非法基点。
        let mut incoming = market(1, side, 1000);
        incoming.market_protection = Some(invalid_bps.clone());

        assert_protected_quote_rejected_atomically(
            &mut book,
            incoming,
            &context,
            &math,
            RejectReason::DuplicateOrderId,
        );

        // 非重复订单：3007。
        let mut incoming = market(100, side, 1000);
        incoming.market_protection = Some(invalid_bps);

        assert_protected_quote_rejected_atomically(
            &mut book,
            incoming,
            &context,
            &math,
            RejectReason::InvalidSlippageBps,
        );

        // 显式 worst_price 未按价格步长=10 对齐。
        let mut incoming = market(100, side, 1000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(100_003)),
        });

        assert_protected_quote_rejected_atomically(
            &mut book,
            incoming,
            &context,
            &math,
            RejectReason::InvalidTick,
        );
    }

    // 命令开始时计算买入滑点边界溢出。
    let mut book = ReferenceOrderBook::new();
    book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

    let context = CommandStartProtectionContext {
        reference_price_at_command_start: p(i64::MAX),
        market_band_worst: Some(p(100_000)),
    };

    let mut incoming = market(100, Side::Buy, 1000);
    incoming.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: None,
    });

    assert_protected_quote_rejected_atomically(
        &mut book,
        incoming,
        &context,
        &math,
        RejectReason::ArithmeticOverflow,
    );

    // 数学错误发生在已经规划出前序成交之后：
    // 仍然不得提交任何挂单状态变更。
    for (side, maker_side, better) in [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, better, 1)).unwrap();
        book.rest(limit(2, maker_side, 101, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: None,
        };

        assert_protected_quote_rejected_atomically(
            &mut book,
            market(100, side, 1000),
            &context,
            &math,
            RejectReason::InvalidTick,
        );
    }
}

#[test]
fn protected_quote_execution_invalid_lot_is_atomic() {
    let mut book = ReferenceOrderBook::new();

    book.rest(limit(1, Side::Sell, 5, 11)).unwrap();

    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100).unwrap(),
        market_band_worst: None,
    };

    let math = MarketMath::try_new(0, 0, 0, 1, 10).unwrap();

    assert_protected_quote_rejected_atomically(
        &mut book,
        market(100, Side::Buy, 1000),
        &context,
        &math,
        RejectReason::InvalidLot,
    );
}

#[test]
fn protected_quote_execution_insufficient_ids_is_atomic() {
    let p = |v| Price::try_new(v).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, better) in [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)] {
        let mut book = ReferenceOrderBook::new();

        book.rest(limit(1, maker_side, better, 2)).unwrap();
        book.rest(limit(2, maker_side, 100, 3)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_003),
            market_band_worst: Some(p(100)),
        };

        let before = snapshot(&book);

        // 需要两个 TradeId，只提供一个。
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.execute_market_by_quote_with_protection(
                market(100, side, 1000),
                &context,
                &math,
                &[TradeId::new(700)],
            );
        }));

        assert!(result.is_err());
        assert_eq!(snapshot(&book), before);
    }
}
