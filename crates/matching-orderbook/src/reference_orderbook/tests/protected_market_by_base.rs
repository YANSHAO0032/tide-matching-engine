use super::*;
use matching_domain::{MarketByBase, MarketProtection, UserId};

fn market(id: u128, side: Side, qty: u64) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection: None,
    }
}
fn limit(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
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
fn protected_market_by_base_preflight_respects_fifo_and_boundary() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (taker_side, maker_side, better, outside, farther) in [
        (Side::Buy, Side::Sell, 90, 110, 120),
        (Side::Sell, Side::Buy, 110, 90, 80),
    ] {
        let mut book = ReferenceOrderBook::new();

        // 故意打乱价格与标识符的顺序。
        // 标识符=40 先于标识符=10 挂在边界价 100。
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
            reference_price_at_command_start: Price::try_new(100_003).unwrap(),
            market_band_worst: Some(Price::try_new(100).unwrap()),
        };

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        // 数量足够扫过全部挂单。
        // 只能规划更优价格 + 两张边界价订单；
        // 首张边界外价格挂单必须终止扫描。
        let plan = book
            .preflight_market_by_base_with_protection(&market(100, taker_side, 20), &context, &math)
            .unwrap();

        assert_eq!(
            plan,
            vec![
                (OrderId::new(30), Qty::try_new(2).unwrap()),
                (OrderId::new(40), Qty::try_new(4).unwrap()),
                (OrderId::new(10), Qty::try_new(3).unwrap()),
            ]
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);

        // 数量不足：第二张订单仅部分计划成交。
        // 同价先进先出必须先选择标识符=40，而不是标识符=10。
        let plan = book
            .preflight_market_by_base_with_protection(&market(101, taker_side, 5), &context, &math)
            .unwrap();

        assert_eq!(
            plan,
            vec![
                (OrderId::new(30), Qty::try_new(2).unwrap()),
                (OrderId::new(40), Qty::try_new(3).unwrap()),
            ]
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
    }
}

#[test]
fn protected_market_by_base_preflight_without_boundary_scans_all_fifo_levels() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (taker_side, maker_side, better, outside, farther) in [
        (Side::Buy, Side::Sell, 90, 110, 120),
        (Side::Sell, Side::Buy, 110, 90, 80),
    ] {
        let mut book = ReferenceOrderBook::new();

        // 与已有保护边界测试使用相同的多档、同价先进先出数据。
        // 源 Vec 顺序故意不同于实际撮合顺序。
        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        // 没有用户保护，也没有市场价格区间。
        let incoming = market(100, taker_side, 20);
        assert_eq!(incoming.market_protection, None);

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100_003).unwrap(),
            market_band_worst: None,
        };

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        let plan = book
            .preflight_market_by_base_with_protection(&incoming, &context, &math)
            .unwrap();

        // 无有效边界时，不能在 100 价位终止。
        // 必须继续纳入此前因保护边界而被排除的更远挂单。
        //
        // 买入:  90 -> 100 -> 100 -> 110 -> 120
        // 卖出: 110 -> 100 -> 100 -> 90 -> 80
        assert_eq!(
            plan,
            vec![
                (OrderId::new(30), Qty::try_new(2).unwrap()),
                (OrderId::new(40), Qty::try_new(4).unwrap()),
                (OrderId::new(10), Qty::try_new(3).unwrap()),
                (OrderId::new(50), Qty::try_new(1).unwrap()),
                (OrderId::new(90), Qty::try_new(2).unwrap()),
            ]
        );

        // 预检不得修改订单内容、剩余量、先进先出或优先级。
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
    }
}

#[test]
fn protected_market_by_base_preflight_propagates_errors_atomically() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: Price::try_new(100_003).unwrap(),
            market_band_worst: Some(Price::try_new(100_000).unwrap()),
        };

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        // 1. 非法基点：即使市场价格区间有效，仍必须返回 3007。
        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        let expected = invalid_bps
            .user_protection_price(
                context.reference_price_at_command_start,
                taker_side,
                math.tick_size(),
            )
            .unwrap_err();

        assert_eq!(expected as u16, 3007);

        let mut incoming = market(100, taker_side, 5);
        incoming.market_protection = Some(invalid_bps);

        assert_eq!(
            book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
            Err(expected)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);

        // 2. 显式 worst_price 未按价格步长=10 对齐。
        let invalid_price = MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(Price::try_new(100_003).unwrap()),
        };

        let expected = invalid_price
            .user_protection_price(
                context.reference_price_at_command_start,
                taker_side,
                math.tick_size(),
            )
            .unwrap_err();

        let mut incoming = market(100, taker_side, 5);
        incoming.market_protection = Some(invalid_price);

        assert_eq!(
            book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
            Err(expected)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
    }

    // 3. 买入的滑点边界超过 i64 正价格值域：
    //    既有领域层带溢出检查的算术错误必须原样传播。
    let mut book = ReferenceOrderBook::new();
    book.rest(limit(1, Side::Sell, 100_000, 5)).unwrap();

    let context = CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(i64::MAX).unwrap(),
        market_band_worst: Some(Price::try_new(100_000).unwrap()),
    };

    let protection = MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: None,
    };

    let expected = protection
        .user_protection_price(
            context.reference_price_at_command_start,
            Side::Buy,
            math.tick_size(),
        )
        .unwrap_err();

    assert_eq!(expected, RejectReason::ArithmeticOverflow);

    let mut incoming = market(100, Side::Buy, 5);
    incoming.market_protection = Some(protection);

    let before = snapshot(&book);
    let priority_before = book.next_priority;

    assert_eq!(
        book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
        Err(expected)
    );

    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, priority_before);
}
#[test]
fn protected_market_by_base_failures_preserve_book() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    let mut book = ReferenceOrderBook::new();

    book.rest(limit(1, Side::Sell, 90, 2)).unwrap();
    book.rest(limit(2, Side::Sell, 100, 4)).unwrap();
    book.rest(limit(3, Side::Sell, 110, 3)).unwrap();

    let context = CommandStartProtectionContext {
        reference_price_at_command_start: p(100_000),
        market_band_worst: Some(p(100)),
    };

    let before = snapshot(&book);

    // 1. 重复标识符必须优先于非法保护。
    let mut duplicate = market(1, Side::Buy, 10);
    duplicate.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(u32::MAX),
        worst_price: None,
    });

    assert_eq!(
        book.execute_market_by_base_with_protection(duplicate, &context, &math, &[],),
        Err(RejectReason::DuplicateOrderId)
    );
    assert_eq!(snapshot(&book), before);

    // 2. 非法基点必须原样传播。
    let mut invalid = market(100, Side::Buy, 10);
    invalid.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(u32::MAX),
        worst_price: None,
    });

    let expected = invalid
        .market_protection
        .as_ref()
        .unwrap()
        .user_protection_price(
            context.reference_price_at_command_start,
            Side::Buy,
            math.tick_size(),
        )
        .unwrap_err();

    assert_eq!(expected as u16, 3007);

    assert_eq!(
        book.execute_market_by_base_with_protection(invalid, &context, &math, &[],),
        Err(expected)
    );
    assert_eq!(snapshot(&book), before);

    // 3. TradeId 不足：需要两个成交标识符，却只提供一个。
    let mut incoming = market(100, Side::Buy, 10);
    incoming.market_protection = Some(MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(p(100)),
    });

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.execute_market_by_base_with_protection(
            incoming,
            &context,
            &math,
            &[TradeId::new(700)],
        );
    }));

    assert!(result.is_err());

    // 包括挂单载荷、剩余量、先进先出和 next_priority。
    assert_eq!(snapshot(&book), before);
}
#[test]
fn protected_entry_without_effective_boundary_matches_all_available() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, first, second) in [
        (Side::Buy, Side::Sell, 90, 110),
        (Side::Sell, Side::Buy, 110, 90),
    ] {
        let mut book = ReferenceOrderBook::new();
        book.rest(limit(1, maker_side, second, 2)).unwrap();
        book.rest(limit(2, maker_side, first, 3)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: None,
        };

        let events = book
            .execute_market_by_base_with_protection(
                market(100, side, 10),
                &context,
                &math,
                &[TradeId::new(701), TradeId::new(702)],
            )
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            EngineEvent::Trade(t)
                if t.maker_order_id == OrderId::new(2)
        ));
        assert!(matches!(
            &events[1],
            EngineEvent::Trade(t)
                if t.maker_order_id == OrderId::new(1)
        ));

        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, 2);
    }
}

#[test]
fn protected_market_by_base_emits_trades_before_protection_stop() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, better, outside, farther, user, band, source) in [
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
            Side::Sell,
            Side::Buy,
            110,
            90,
            80,
            90,
            100,
            ProtectionSource::MarketBand,
        ),
    ] {
        let mut book = ReferenceOrderBook::new();

        // 故意打乱源顺序，验证价格优先与同价先进先出。
        for (id, price, qty) in [
            (90, farther, 2),
            (40, 100, 4),
            (30, better, 2),
            (10, 100, 3),
            (50, outside, 1),
        ] {
            book.rest(limit(id, maker_side, price, qty)).unwrap();
        }

        let mut incoming = market(100, side, 20);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(user)),
        });

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: Some(p(band)),
        };

        let priority_before = book.next_priority;

        let events = book
            .execute_market_by_base_with_protection(
                incoming,
                &context,
                &math,
                &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
            )
            .unwrap();

        let expected_trade = |trade_id, maker_id, price, qty| {
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(trade_id),
                maker_order_id: OrderId::new(maker_id),
                taker_order_id: OrderId::new(100),
                price: p(price),
                qty: Qty::try_new(qty).unwrap(),
                maker_side,
            })
        };

        assert_eq!(
            events,
            vec![
                expected_trade(701, 30, better, 2),
                expected_trade(702, 40, 100, 4),
                expected_trade(703, 10, 100, 3),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source,
                },
            ]
        );

        // 已成交的三个挂单被移除。
        // 越界挂单及更远挂单完整保留。
        assert_eq!(
            snapshot(&book),
            (
                vec![
                    (limit(90, maker_side, farther, 2), 2, QueuePriority::new(0),),
                    (limit(50, maker_side, outside, 1), 1, QueuePriority::new(4),),
                ],
                priority_before,
            )
        );

        // 市价单剩余 11 手不得挂簿。
        assert!(
            book.orders
                .iter()
                .all(|maker| maker.original_order().order_id != OrderId::new(100))
        );
    }
}

#[test]
fn protected_market_by_base_boundary_equal_is_tradable() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, outside) in [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)] {
        for (incoming_qty, should_stop) in [(2, false), (3, true)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(limit(1, maker_side, 100, 2)).unwrap();
            book.rest(limit(2, maker_side, outside, 4)).unwrap();

            let mut incoming = market(100, side, incoming_qty);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100)),
            });

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(100)),
            };

            let events = book
                .execute_market_by_base_with_protection(
                    incoming,
                    &context,
                    &math,
                    &[TradeId::new(700)],
                )
                .unwrap();

            assert_eq!(
                events[0],
                EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(700),
                    maker_order_id: OrderId::new(1),
                    taker_order_id: OrderId::new(100),
                    price: p(100),
                    qty: Qty::try_new(2).unwrap(),
                    maker_side,
                })
            );

            if should_stop {
                assert_eq!(
                    events[1],
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source: ProtectionSource::Both,
                    }
                );
                assert_eq!(events.len(), 2);
            } else {
                // 吃单方恰好成交完，不能继续探测越界挂单。
                assert_eq!(events.len(), 1);
            }

            assert_eq!(
                snapshot(&book),
                (
                    vec![(limit(2, maker_side, outside, 4), 4, QueuePriority::new(1),)],
                    2,
                )
            );
        }
    }
}
#[test]
fn protected_market_by_base_empty_and_immediate_stop() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, outside) in [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)] {
        let mut book = ReferenceOrderBook::new();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(100_000),
            market_band_worst: Some(p(100)),
        };

        let before = snapshot(&book);

        assert_eq!(
            book.execute_market_by_base_with_protection(
                market(100, side, 10),
                &context,
                &math,
                &[],
            ),
            Ok(vec![])
        );

        assert_eq!(snapshot(&book), before);

        book.rest(limit(1, maker_side, outside, 5)).unwrap();

        let before = snapshot(&book);

        // 首个挂单就越界，不需要任何 TradeId。
        let events = book
            .execute_market_by_base_with_protection(market(100, side, 10), &context, &math, &[])
            .unwrap();

        assert_eq!(
            events,
            vec![EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            }]
        );

        assert_eq!(snapshot(&book), before);
    }
}
