use super::*;
use matching_domain::{EngineEvent, MarketByBase, MarketProtection, UserId};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn market(id: u128, side: Side, qty: u64) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection: None,
    }
}

fn book() -> ProductionOrderBook {
    ProductionOrderBook::new(policy())
}

/// 比较完整生产簿状态。
///
/// 包括：
/// - 双侧 BTreeMap 档位
/// - OrderId 索引
/// - Arena 槽位/空闲链表
/// - QueuePriority 分配器
///
/// 只用于失败路径的原子性验证。
fn assert_failure_atomic<F>(book: &mut ProductionOrderBook, action: F)
where
    F: FnOnce(&mut ProductionOrderBook),
{
    assert_eq!(validate_book(book), Ok(()));

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();
    let next_before = book.next_queue_priority();
    let policy_before = *book.queue_priority_allocator.policy();

    action(book);

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);
    assert_eq!(book.orders, orders_before);
    assert_eq!(book.arena.test_snapshot(), arena_before);
    assert_eq!(book.next_queue_priority(), next_before);
    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

    assert_eq!(validate_book(book), Ok(()));
}

#[test]
fn market_by_base_empty_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = book();

        assert_failure_atomic(&mut book, |book| {
            let trades = book
                .execute_market_by_base(
                    market(100, side, 10),
                    &[],
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                )
                .unwrap();

            assert!(trades.is_empty());
        });

        assert_eq!(book.order(OrderId::new(100)), None);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_base_partial_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();

        book.rest(order(1, maker_side, 100, 10)).unwrap();
        book.rest(order(2, maker_side, 100, 5)).unwrap();

        let maker_id = OrderId::new(1);
        let slot_before = *book.orders.get(&maker_id).unwrap();
        let priority_before = book.order(maker_id).unwrap().priority();
        let next_before = book.next_queue_priority();

        let levels_before = match maker_side {
            Side::Buy => level_snapshot(&book.bids),
            Side::Sell => level_snapshot(&book.asks),
        };

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 3),
                &[TradeId::new(500)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 1);

        assert_eq!(trades[0].trade_id, TradeId::new(500));
        assert_eq!(trades[0].maker_order_id, maker_id);
        assert_eq!(trades[0].taker_order_id, OrderId::new(100));
        assert_eq!(trades[0].maker_side, maker_side);
        assert_eq!(trades[0].price, Price::try_new(100).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(3).unwrap());

        let maker = book.order(maker_id).unwrap();

        assert_eq!(maker.remaining(), 7);
        assert_eq!(maker.priority(), priority_before);
        assert_eq!(maker.prev(), None);
        assert_eq!(maker.next(), book.orders.get(&OrderId::new(2)).copied());

        assert_eq!(book.orders.get(&maker_id), Some(&slot_before));

        // 原价档仍有两张订单，只有聚合数量从 15 变成 12。
        let levels_after = match maker_side {
            Side::Buy => level_snapshot(&book.bids),
            Side::Sell => level_snapshot(&book.asks),
        };

        assert_eq!(levels_after.len(), levels_before.len());
        assert_eq!(levels_after[0].head, levels_before[0].head);
        assert_eq!(levels_after[0].tail, levels_before[0].tail);
        assert_eq!(levels_after[0].count, 2);
        assert_eq!(levels_after[0].total_visible_qty, 12);

        assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 5);
        assert!(book.order(OrderId::new(100)).is_none());
        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_base_multi_level_fifo_both_sides() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = book();

        // 故意让 OrderId 数值顺序与先进先出顺序不同。
        book.rest(order(20, maker_side, worse, 4)).unwrap();
        book.rest(order(30, maker_side, better, 3)).unwrap();
        book.rest(order(10, maker_side, better, 5)).unwrap();

        let next_before = book.next_queue_priority();

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 10),
                &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 3);

        assert_eq!(
            trades.iter().map(|t| t.maker_order_id).collect::<Vec<_>>(),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20),]
        );

        assert_eq!(
            trades.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
            vec![better, better, worse]
        );

        assert_eq!(
            trades.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
            vec![3, 5, 2]
        );

        assert_eq!(
            trades.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
            vec![TradeId::new(501), TradeId::new(502), TradeId::new(503),]
        );

        for trade in &trades {
            assert_eq!(trade.maker_side, maker_side);
            assert_eq!(trade.taker_order_id, OrderId::new(100));
            assert!(trade.qty.get() > 0);
        }

        // 前两张挂单完全成交。
        assert!(book.order(OrderId::new(30)).is_none());
        assert!(book.order(OrderId::new(10)).is_none());

        // 最后一张挂单部分成交。
        let remaining = book.order(OrderId::new(20)).unwrap();
        assert_eq!(remaining.remaining(), 2);
        assert_eq!(remaining.original_order().qty.get(), 4);

        // 更优价格档已被完全删除。
        let maker_levels = match maker_side {
            Side::Buy => &book.bids,
            Side::Sell => &book.asks,
        };

        assert!(!maker_levels.contains_key(&Price::try_new(better).unwrap()));
        assert_eq!(maker_levels.len(), 1);

        assert!(book.order(OrderId::new(100)).is_none());
        assert_eq!(book.next_queue_priority(), next_before);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_base_discards_remainder() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();

        book.rest(order(1, maker_side, 100, 3)).unwrap();

        let next_before = book.next_queue_priority();

        // 需要 10，但盘口仅有 3。
        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 10),
                &[TradeId::new(600)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].qty.get(), 3);

        // 挂单完全成交，市价余量 7 不允许挂簿。
        assert!(book.orders.is_empty());
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());

        assert!(book.order(OrderId::new(100)).is_none());
        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_base_duplicate_is_atomic() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();

        book.rest(order(1, maker_side, 100, 10)).unwrap();

        assert_failure_atomic(&mut book, |book| {
            // 重复标识符与 TradeId 不足同时成立。
            // DuplicateOrderId 必须优先返回。
            let result = book.execute_market_by_base(
                market(1, taker_side, 5),
                &[],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            );

            assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        });
    }
}

#[test]
fn market_by_base_insufficient_trade_ids_is_atomic() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();

        book.rest(order(30, maker_side, 105, 3)).unwrap();
        book.rest(order(10, maker_side, 110, 7)).unwrap();

        // 需要两笔成交，但只提供一个 TradeId。
        assert_failure_atomic(&mut book, |book| {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = book.execute_market_by_base(
                    market(100, taker_side, 10),
                    &[TradeId::new(700)],
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                );
            }));

            assert!(result.is_err());
        });
    }
}

#[test]
fn market_by_base_ignores_extra_trade_ids() {
    let mut book = book();

    book.rest(order(1, Side::Sell, 105, 10)).unwrap();

    let next_before = book.next_queue_priority();

    let trades = book
        .execute_market_by_base(
            market(100, Side::Buy, 3),
            &[TradeId::new(800), TradeId::new(801), TradeId::new(802)],
            &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        )
        .unwrap();

    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].trade_id, TradeId::new(800));
    assert_eq!(trades[0].qty.get(), 3);

    assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 7);
    assert_eq!(book.next_queue_priority(), next_before);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn market_by_base_works_with_exhausted_priority_allocator() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        // 消耗唯一一次允许的 QueuePriority 分配。
        book.rest(order(1, maker_side, 100, 5)).unwrap();

        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(book.next_queue_priority(), 1);

        let trades = book
            .execute_market_by_base(
                market(100, taker_side, 5),
                &[TradeId::new(900)],
                &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].qty.get(), 5);
        assert_eq!(trades[0].maker_order_id, OrderId::new(1));

        assert!(book.orders.is_empty());
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());

        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(book.next_queue_priority(), 1);

        assert_eq!(validate_book(&book), Ok(()));
    }
}

/// 受保护按基础数量市价单的预检与执行验证。
mod protected_market_by_base {
    use super::*;

    #[test]
    fn protected_market_by_base_preflight_price_fifo_and_boundary() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (taker_side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let mut book = book();

            // 插入顺序刻意不同于价格和标识符顺序。
            // 边界价 100：标识符=40 必须先于标识符=10。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100)),
            };

            // 完整扫描：允许更优价格和两个等于边界的挂单。
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_base_with_protection(
                        &market(100, taker_side, 20),
                        &context,
                        &math,
                    ),
                    Ok(vec![
                        (OrderId::new(30), Qty::try_new(2).unwrap()),
                        (OrderId::new(40), Qty::try_new(4).unwrap()),
                        (OrderId::new(10), Qty::try_new(3).unwrap()),
                    ])
                );
            });

            // 数量不足以耗尽第二张挂单：仍必须遵循同价先进先出。
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_base_with_protection(
                        &market(101, taker_side, 5),
                        &context,
                        &math,
                    ),
                    Ok(vec![
                        (OrderId::new(30), Qty::try_new(2).unwrap()),
                        (OrderId::new(40), Qty::try_new(3).unwrap()),
                    ])
                );
            });
        }
    }
    #[test]
    fn protected_market_by_base_preflight_empty_outside_and_unbounded() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let mut book = book();

            let bounded = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100)),
            };

            // 空簿：零计划。
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_base_with_protection(
                        &market(100, side, 20),
                        &bounded,
                        &math,
                    ),
                    Ok(vec![])
                );
            });

            // 首档直接越界：不能跳过它寻找其他挂单。
            book.rest(order(50, maker_side, outside, 1)).unwrap();

            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_base_with_protection(
                        &market(100, side, 20),
                        &bounded,
                        &math,
                    ),
                    Ok(vec![])
                );
            });

            // 再添加多档，其中有同价先进先出。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }

            let unbounded = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: None,
            };

            // 无用户/市场价格区间：必须扫描全部五个挂单。
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_base_with_protection(
                        &market(100, side, 20),
                        &unbounded,
                        &math,
                    ),
                    Ok(vec![
                        (OrderId::new(30), Qty::try_new(2).unwrap()),
                        (OrderId::new(40), Qty::try_new(4).unwrap()),
                        (OrderId::new(10), Qty::try_new(3).unwrap()),
                        (OrderId::new(50), Qty::try_new(1).unwrap()),
                        (OrderId::new(90), Qty::try_new(2).unwrap()),
                    ])
                );
            });
        }
    }
    #[test]
    fn protected_market_by_base_preflight_errors_are_atomic() {
        let p = |value| Price::try_new(value).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = book();

            book.rest(order(1, maker_side, 100_000, 5)).unwrap();

            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100_000)),
            };

            // 重复标识符优先于非法基点。
            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(1, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: Some(u32::MAX),
                    worst_price: None,
                });

                assert_eq!(
                    book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
                    Err(RejectReason::DuplicateOrderId)
                );
            });

            // 非法基点：固定线协议代码 3007。
            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(100, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: Some(u32::MAX),
                    worst_price: None,
                });

                assert!(matches!(
                    book.preflight_market_by_base_with_protection(
                        &incoming, &context, &math,
                    ),
                    Err(reason) if reason as u16 == 3007
                ));
            });

            // 显式 worst_price 未对齐价格步长=10。
            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(100, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100_003)),
                });

                assert_eq!(
                    book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
                    Err(RejectReason::InvalidTick)
                );
            });
        }

        // 买入向上扩张滑点边界，超过 `i64::MAX`。
        // 即使市场价格区间有效，也必须先传播算术错误。
        let mut book = book();
        book.rest(order(1, Side::Sell, 100_000, 5)).unwrap();

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };

        let mut incoming = market(100, Side::Buy, 5);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });

        assert_failure_atomic(&mut book, |book| {
            assert_eq!(
                book.preflight_market_by_base_with_protection(&incoming, &context, &math,),
                Err(RejectReason::ArithmeticOverflow)
            );
        });
    }
    #[test]
    fn protected_market_by_base_execution_orders_trades_then_trigger() {
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
            let mut book = book();

            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
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

            let priority_before = book.next_queue_priority();

            let events = book
                .execute_market_by_base_with_protection(
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

            assert_eq!(
                events,
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
                ]
            );

            // 已成交三张挂单被移除，越界及更远挂单保留。
            for id in [30, 40, 10] {
                assert!(book.order(OrderId::new(id)).is_none());
            }

            assert_eq!(book.order(OrderId::new(50)).unwrap().remaining(), 1);
            assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 2);

            assert_eq!(book.best_price(maker_side), Some(p(outside)));
            assert_eq!(book.next_queue_priority(), priority_before);
            assert!(book.order(OrderId::new(100)).is_none());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }

    #[test]
    fn protected_market_by_base_execution_equal_boundary_and_partial_maker() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)]
        {
            for (taker_qty, triggered) in [(2, false), (3, true)] {
                let mut book = book();
                book.rest(order(1, maker_side, 100, 2)).unwrap();
                book.rest(order(2, maker_side, outside, 4)).unwrap();

                let mut incoming = market(100, side, taker_qty);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100)),
                });
                let context = CommandStartProtectionContext {
                    reference_price_at_command_start: p(100_000),
                    market_band_worst: Some(p(100)),
                };
                let before_priority = book.next_queue_priority();
                let mut expected = vec![EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(700),
                    maker_order_id: OrderId::new(1),
                    taker_order_id: OrderId::new(100),
                    price: p(100),
                    qty: Qty::try_new(2).unwrap(),
                    maker_side,
                })];
                if triggered {
                    expected.push(EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source: ProtectionSource::Both,
                    });
                }
                let actual = book
                    .execute_market_by_base_with_protection(
                        incoming,
                        &context,
                        &math,
                        &[TradeId::new(700)],
                    )
                    .unwrap();
                assert_eq!(actual, expected);
                assert!(book.order(OrderId::new(1)).is_none());
                assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 4);
                assert_eq!(book.next_queue_priority(), before_priority);
                assert!(!book.orders.contains_key(&OrderId::new(100)));
                assert_eq!(validate_book(&book), Ok(()));
            }

            // 挂单严格部分成交：保留槽位、优先级和先进先出后继。
            let mut book = book();
            book.rest(order(1, maker_side, 100, 10)).unwrap();
            book.rest(order(2, maker_side, outside, 4)).unwrap();
            let slot = *book.orders.get(&OrderId::new(1)).unwrap();
            let priority = book.order(OrderId::new(1)).unwrap().priority();
            let next = book.next_queue_priority();
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(100)),
            };
            let events = book
                .execute_market_by_base_with_protection(
                    market(100, side, 3),
                    &context,
                    &math,
                    &[TradeId::new(701)],
                )
                .unwrap();
            assert_eq!(
                events,
                vec![EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(701),
                    maker_order_id: OrderId::new(1),
                    taker_order_id: OrderId::new(100),
                    price: p(100),
                    qty: Qty::try_new(3).unwrap(),
                    maker_side,
                })]
            );
            let maker = book.order(OrderId::new(1)).unwrap();
            assert_eq!(maker.remaining(), 7);
            assert_eq!(maker.priority(), priority);
            assert_eq!(book.orders.get(&OrderId::new(1)), Some(&slot));
            assert_eq!(book.next_queue_priority(), next);
            assert_eq!(validate_book(&book), Ok(()));
        }
    }

    #[test]
    fn protected_market_by_base_execution_empty_immediate_stop_and_unbounded() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side, better, outside, farther) in [
            (Side::Buy, Side::Sell, 90, 110, 120),
            (Side::Sell, Side::Buy, 110, 90, 80),
        ] {
            let mut book = book();
            let bounded = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(100)),
            };
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.execute_market_by_base_with_protection(
                        market(100, side, 20),
                        &bounded,
                        &math,
                        &[],
                    ),
                    Ok(vec![])
                );
            });

            // 首个挂单越界时，仅产生保护事件、不消耗 TradeId。
            book.rest(order(50, maker_side, outside, 1)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.execute_market_by_base_with_protection(
                        market(100, side, 20),
                        &bounded,
                        &math,
                        &[],
                    ),
                    Ok(vec![EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source: ProtectionSource::MarketBand,
                    }])
                );
            });

            // 没有任一保护候选时，扫完价格/先进先出的完整前缀。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }
            let no_boundary = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: None,
            };
            let before_priority = book.next_queue_priority();
            let trade_ids = [
                TradeId::new(701),
                TradeId::new(702),
                TradeId::new(703),
                TradeId::new(704),
                TradeId::new(705),
                TradeId::new(706),
            ];
            let events = book
                .execute_market_by_base_with_protection(
                    market(100, side, 20),
                    &no_boundary,
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
            let expected: Vec<_> = expected_makers
                .iter()
                .enumerate()
                .map(|(i, &(maker_id, price, qty))| {
                    EngineEvent::Trade(TradeEvent {
                        trade_id: trade_ids[i],
                        maker_order_id: OrderId::new(maker_id),
                        taker_order_id: OrderId::new(100),
                        price: p(price),
                        qty: Qty::try_new(qty).unwrap(),
                        maker_side,
                    })
                })
                .collect();
            assert_eq!(events, expected);
            assert!(book.orders.is_empty());
            assert!(book.bids.is_empty() && book.asks.is_empty());
            assert_eq!(book.next_queue_priority(), before_priority);
            assert_eq!(validate_book(&book), Ok(()));
        }
    }

    #[test]
    fn protected_market_by_base_execution_failures_are_atomic() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = book();
            book.rest(order(1, maker_side, 100_000, 5)).unwrap();
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_003),
                market_band_worst: Some(p(100_000)),
            };

            // 重复标识符的优先级高于非法基点。
            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(1, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: Some(u32::MAX),
                    worst_price: None,
                });
                assert_eq!(
                    book.execute_market_by_base_with_protection(incoming, &context, &math, &[],),
                    Err(RejectReason::DuplicateOrderId)
                );
            });

            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(100, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: Some(u32::MAX),
                    worst_price: None,
                });
                assert!(matches!(book.execute_market_by_base_with_protection(
                incoming, &context, &math, &[],
            ), Err(reason) if reason as u16 == 3007));
            });

            assert_failure_atomic(&mut book, |book| {
                let mut incoming = market(100, side, 5);
                incoming.market_protection = Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100_003)),
                });
                assert_eq!(
                    book.execute_market_by_base_with_protection(incoming, &context, &math, &[],),
                    Err(RejectReason::InvalidTick)
                );
            });

            // 无保护：两张挂单两笔成交，单个 TradeId 必须在修改前触发不变量断言。
            book.rest(order(2, maker_side, 100_000, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let _ = book.execute_market_by_base_with_protection(
                        market(100, side, 8),
                        &context,
                        &math,
                        &[TradeId::new(700)],
                    );
                }));
                assert!(result.is_err());
            });
        }

        let mut book = book();
        book.rest(order(1, Side::Sell, 100_000, 5)).unwrap();
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };
        let mut incoming = market(100, Side::Buy, 5);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });
        assert_failure_atomic(&mut book, |book| {
            assert_eq!(
                book.execute_market_by_base_with_protection(incoming, &context, &math, &[],),
                Err(RejectReason::ArithmeticOverflow)
            );
        });
    }

    #[test]
    fn protected_market_by_base_execution_ignores_priority_exhaustion() {
        let p = |v| Price::try_new(v).unwrap();
        let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy_with_ceiling(1));
            book.rest(order(1, maker_side, 100, 5)).unwrap();
            assert!(book.queue_priority_allocator.is_exhausted());
            let next = book.next_queue_priority();
            let context = CommandStartProtectionContext {
                reference_price_at_command_start: p(100_000),
                market_band_worst: Some(p(100)),
            };
            let events = book
                .execute_market_by_base_with_protection(
                    market(100, side, 5),
                    &context,
                    &math,
                    &[TradeId::new(701)],
                )
                .unwrap();
            assert_eq!(
                events,
                vec![EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId::new(701),
                    maker_order_id: OrderId::new(1),
                    taker_order_id: OrderId::new(100),
                    price: p(100),
                    qty: Qty::try_new(5).unwrap(),
                    maker_side,
                })]
            );
            assert_eq!(book.next_queue_priority(), next);
            assert!(book.queue_priority_allocator.is_exhausted());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
}
