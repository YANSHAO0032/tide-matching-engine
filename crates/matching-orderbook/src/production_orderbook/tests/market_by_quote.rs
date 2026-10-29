use super::*;
use matching_domain::{MarketByQuote, MarketProtection, QuoteBudget, UserId};
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

fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}

fn book() -> ProductionOrderBook {
    ProductionOrderBook::new(policy())
}

fn protected_math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 10, 1).unwrap()
}

fn protection_context(band: Option<i64>) -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100_003).unwrap(),
        market_band_worst: band.map(|v| Price::try_new(v).unwrap()),
    }
}

fn expected_protected_quote_plan(
    fills: &[(u128, i64, u64, u128)],
    spent: u128,
    unused: u128,
    stop: Option<(i64, i64, ProtectionSource)>,
) -> ProtectedMarketByQuotePlan {
    let p = |v| Price::try_new(v).unwrap();

    ProtectedMarketByQuotePlan {
        fills: fills
            .iter()
            .map(|&(id, price, qty, notional)| QuoteFillPlan {
                maker_order_id: OrderId::new(id),
                maker_price: p(price),
                qty: Qty::try_new(qty).unwrap(),
                notional: QuoteAmount::new(notional),
            })
            .collect(),
        spent_quote: QuoteAmount::new(spent),
        unused_quote: QuoteAmount::new(unused),
        stop: stop.map(|(next, worst, source)| ProtectionBoundaryStop {
            next_price: p(next),
            effective_worst_price: p(worst),
            source,
        }),
    }
}

// 对比全部生产簿内部状态，而不仅是活动订单。
fn assert_failure_atomic<F>(book: &mut ProductionOrderBook, action: F)
where
    F: FnOnce(&mut ProductionOrderBook),
{
    assert_eq!(validate_book(book), Ok(()));

    let bids = level_snapshot(&book.bids);
    let asks = level_snapshot(&book.asks);
    let orders = book.orders.clone();
    let arena = book.arena.test_snapshot();
    let priority = book.next_queue_priority();
    let policy = *book.queue_priority_allocator.policy();

    action(book);

    assert_eq!(level_snapshot(&book.bids), bids);
    assert_eq!(level_snapshot(&book.asks), asks);
    assert_eq!(book.orders, orders);
    assert_eq!(book.arena.test_snapshot(), arena);
    assert_eq!(book.next_queue_priority(), priority);
    assert_eq!(book.queue_priority_allocator.policy(), &policy);

    assert_eq!(validate_book(book), Ok(()));
}

#[test]
fn market_by_quote_empty_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = book();

        assert_failure_atomic(&mut book, |book| {
            let result = book
                .execute_market_by_quote(market(100, side, 1000), &math(), &[])
                .unwrap();

            assert!(result.trades().is_empty());
            assert_eq!(result.spent_quote().get(), 0);
            assert_eq!(result.unused_quote().get(), 1000);
        });
    }
}

#[test]
fn market_by_quote_partial_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();

        book.rest(order(1, maker_side, 100, 10)).unwrap();
        book.rest(order(2, maker_side, 100, 5)).unwrap();

        let index = *book.orders.get(&OrderId::new(1)).unwrap();
        let priority = book.order(OrderId::new(1)).unwrap().priority();
        let next = book.next_queue_priority();

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

        let maker = book.order(OrderId::new(1)).unwrap();
        assert_eq!(maker.remaining(), 7);
        assert_eq!(maker.priority(), priority);
        assert_eq!(book.orders.get(&OrderId::new(1)), Some(&index));

        assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 5);
        assert!(book.order(OrderId::new(100)).is_none());
        assert_eq!(book.next_queue_priority(), next);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_quote_multi_level_fifo_both_sides() {
    for (taker_side, maker_side, worse, better) in [
        (Side::Buy, Side::Sell, 110, 105),
        (Side::Sell, Side::Buy, 90, 95),
    ] {
        let mut book = book();

        book.rest(order(20, maker_side, worse, 4)).unwrap();
        book.rest(order(30, maker_side, better, 3)).unwrap();
        book.rest(order(10, maker_side, better, 5)).unwrap();

        let next = book.next_queue_priority();

        let result = book
            .execute_market_by_quote(
                market(100, taker_side, 2000),
                &math(),
                &[TradeId::new(501), TradeId::new(502), TradeId::new(503)],
            )
            .unwrap();

        let trades = result.trades();

        assert_eq!(
            trades.iter().map(|t| t.maker_order_id).collect::<Vec<_>>(),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );

        assert_eq!(
            trades.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
            vec![better, better, worse]
        );

        assert_eq!(
            trades.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
            vec![3, 5, 4]
        );

        assert_eq!(
            trades.iter().map(|t| t.trade_id).collect::<Vec<_>>(),
            vec![TradeId::new(501), TradeId::new(502), TradeId::new(503),]
        );

        for trade in trades {
            assert_eq!(trade.maker_side, maker_side);
            assert_eq!(trade.taker_order_id, OrderId::new(100));
        }

        let spent = 8 * better as u128 + 4 * worse as u128;
        assert_eq!(result.spent_quote().get(), spent);
        assert_eq!(result.unused_quote().get(), 2000 - spent);

        assert!(book.orders.is_empty());
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());

        assert_eq!(book.next_queue_priority(), next);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_quote_dust_both_sides() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();
        book.rest(order(1, maker_side, 100, 10)).unwrap();

        assert_failure_atomic(&mut book, |book| {
            let result = book
                .execute_market_by_quote(market(100, taker_side, 99), &math(), &[])
                .unwrap();

            assert!(result.trades().is_empty());
            assert_eq!(result.spent_quote().get(), 0);
            assert_eq!(result.unused_quote().get(), 99);
        });

        let next = book.next_queue_priority();

        // 先成交 3 手，再因剩余 50 不足 1 手停止。
        let result = book
            .execute_market_by_quote(market(101, taker_side, 350), &math(), &[TradeId::new(600)])
            .unwrap();

        assert_eq!(result.trades().len(), 1);
        assert_eq!(result.trades()[0].qty.get(), 3);
        assert_eq!(result.spent_quote().get(), 300);
        assert_eq!(result.unused_quote().get(), 50);
        assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 7);

        assert!(book.order(OrderId::new(101)).is_none());
        assert_eq!(book.next_queue_priority(), next);
        assert_eq!(validate_book(&book), Ok(()));
    }
}

#[test]
fn market_by_quote_duplicate_and_math_error_are_atomic() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = book();
        book.rest(order(1, maker_side, 6, 10)).unwrap();

        let invalid_tick_math = MarketMath::try_new(0, 0, 0, 5, 1).unwrap();

        assert_failure_atomic(&mut book, |book| {
            assert!(matches!(
                book.execute_market_by_quote(market(1, taker_side, 1000), &invalid_tick_math, &[],),
                Err(RejectReason::DuplicateOrderId)
            ));
        });

        assert_failure_atomic(&mut book, |book| {
            assert!(matches!(
                book.execute_market_by_quote(
                    market(100, taker_side, 1000),
                    &invalid_tick_math,
                    &[TradeId::new(700)],
                ),
                Err(RejectReason::InvalidTick)
            ));
        });
    }
}

#[test]
fn market_by_quote_insufficient_trade_ids_is_atomic() {
    for (taker_side, maker_side, better, worse) in [
        (Side::Buy, Side::Sell, 105, 110),
        (Side::Sell, Side::Buy, 95, 90),
    ] {
        let mut book = book();

        book.rest(order(30, maker_side, better, 3)).unwrap();
        book.rest(order(10, maker_side, worse, 5)).unwrap();

        assert_failure_atomic(&mut book, |book| {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = book.execute_market_by_quote(
                    market(100, taker_side, 2000),
                    &math(),
                    &[TradeId::new(800)],
                );
            }));

            assert!(result.is_err());
        });
    }
}

#[test]
fn market_by_quote_priority_exhaustion_does_not_block_execution() {
    for (taker_side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        book.rest(order(1, maker_side, 100, 5)).unwrap();

        assert!(book.queue_priority_allocator.is_exhausted());
        let next = book.next_queue_priority();

        let result = book
            .execute_market_by_quote(market(100, taker_side, 500), &math(), &[TradeId::new(900)])
            .unwrap();

        assert_eq!(result.trades().len(), 1);
        assert_eq!(result.trades()[0].qty.get(), 5);
        assert_eq!(result.spent_quote().get(), 500);
        assert_eq!(result.unused_quote().get(), 0);

        assert!(book.orders.is_empty());
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());

        assert_eq!(book.next_queue_priority(), next);
        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(validate_book(&book), Ok(()));
    }
}
/// 受保护按报价数量市价单的预检与执行验证。
mod protected_market_by_quote {
    use super::*;

    fn assert_protected_quote_rejected(
        book: &mut ProductionOrderBook,
        incoming: MarketByQuote,
        context: CommandStartProtectionContext,
        math: &MarketMath,
        expected: RejectReason,
    ) {
        assert_failure_atomic(book, |book| {
            assert_eq!(
                book.preflight_market_by_quote_with_protection(&incoming, &context, math,),
                Err(expected)
            );
        });
    }
    #[test]
    fn protected_quote_preflight_rejections_are_atomic() {
        let p = |v| Price::try_new(v).unwrap();
        let math = protected_math();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100_000, 5)).unwrap();

            let invalid_bps = MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            };

            // 重复标识符必须优先于非法基点。
            let mut incoming = market(1, side, 1000);
            incoming.market_protection = Some(invalid_bps.clone());

            assert_protected_quote_rejected(
                &mut book,
                incoming,
                protection_context(Some(100_000)),
                &math,
                RejectReason::DuplicateOrderId,
            );

            // 标识符不重复，应返回 3007。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(invalid_bps);

            assert_protected_quote_rejected(
                &mut book,
                incoming,
                protection_context(Some(100_000)),
                &math,
                RejectReason::InvalidSlippageBps,
            );

            // 显式保护价格未对齐价格步长=10。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100_003)),
            });

            assert_protected_quote_rejected(
                &mut book,
                incoming,
                protection_context(Some(100_000)),
                &math,
                RejectReason::InvalidTick,
            );
        }

        // 保护边界计算溢出。
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Sell, 100_000, 5)).unwrap();

        let mut incoming = market(100, Side::Buy, 1000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };

        assert_protected_quote_rejected(
            &mut book,
            incoming,
            context,
            &math,
            RejectReason::ArithmeticOverflow,
        );

        // 前序成交已规划成功，后续挂单的价格步长错误
        // 不得导致部分状态变更。
        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, better, 1)).unwrap();
            book.rest(order(2, maker_side, 101, 5)).unwrap();

            assert_protected_quote_rejected(
                &mut book,
                market(100, side, 2000),
                protection_context(None),
                &math,
                RejectReason::InvalidTick,
            );
        }
    }
    #[test]
    fn protected_quote_preflight_multilevel_fifo_and_sources() {
        let p = |v| Price::try_new(v).unwrap();
        let math = protected_math();

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
            let mut book = ProductionOrderBook::new(policy());

            // 标识符数值顺序故意不同于先进先出。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }

            let mut incoming = market(100, side, 2000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            });

            let spent = if side == Side::Buy { 880 } else { 920 };

            let expected = expected_protected_quote_plan(
                &[
                    (30, better, 2, (2 * better) as u128),
                    (40, 100, 4, 400),
                    (10, 100, 3, 300),
                ],
                spent,
                2000 - spent,
                Some((outside, 100, source)),
            );

            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &incoming,
                        &protection_context(Some(band)),
                        &math,
                    ),
                    Ok(expected)
                );
            });
        }
    }

    #[test]
    fn protected_quote_preflight_boundary_dust_and_unbounded() {
        let math = protected_math();

        for (side, maker_side, better, outside, farther, full_spent) in [
            (Side::Buy, Side::Sell, 90, 110, 120, 1230u128),
            (Side::Sell, Side::Buy, 110, 90, 80, 1170u128),
        ] {
            let bounded = protection_context(Some(100));

            // 空簿，不生成终止，预算全部未使用。
            let mut book = book();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 1000),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(&[], 0, 1000, None)),
                );
            });

            // 首档越界：即使预算不足一个手，仍优先触发保护。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, outside, 5)).unwrap();
            book.rest(order(2, maker_side, farther, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 1),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[],
                        0,
                        1,
                        Some((outside, 100, ProtectionSource::MarketBand)),
                    )),
                );
            });

            // 边界相等可成交；预算恰好耗尽后不能再探测越界档。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 2)).unwrap();
            book.rest(order(2, maker_side, outside, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 200),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[(1, 100, 2, 200)],
                        200,
                        0,
                        None,
                    )),
                );
            });

            // 首档在保护范围内，余额不足 1 手，正常不足一手的预算，无触发。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 99),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(&[], 0, 99, None)),
                );
            });

            // 挂单部分成交：只规划 3 手，余 50 报价资产，原簿只读不变。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 10)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 350),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[(1, 100, 3, 300)],
                        300,
                        50,
                        None,
                    )),
                );
            });

            // 前序成交之后的不足一手的预算：下一挂单在保护范围内，无终止。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, better, 1)).unwrap();
            book.rest(order(2, maker_side, 100, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, better as u128 + 5),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[(1, better, 1, better as u128)],
                        better as u128,
                        5,
                        None,
                    )),
                );
            });

            // 前序成交后仅余 5 报价资产：越界优先于不足一手的预算。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, better, 1)).unwrap();
            book.rest(order(2, maker_side, outside, 5)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, better as u128 + 5),
                        &bounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[(1, better, 1, better as u128)],
                        better as u128,
                        5,
                        Some((outside, 100, ProtectionSource::MarketBand)),
                    )),
                );
            });

            // 无用户保护、无市场价格区间，按 BTreeMap + 先进先出扫完五个挂单。
            let mut book = ProductionOrderBook::new(policy());
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }
            let unbounded = protection_context(None);
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 2000),
                        &unbounded,
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[
                            (30, better, 2, (2 * better) as u128),
                            (40, 100, 4, 400),
                            (10, 100, 3, 300),
                            (50, outside, 1, outside as u128),
                            (90, farther, 2, (2 * farther) as u128),
                        ],
                        full_spent,
                        2000 - full_spent,
                        None,
                    )),
                );
            });
        }
    }

    #[test]
    fn protected_quote_preflight_outside_price_skips_invalid_tick() {
        let math = protected_math();
        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 111), (Side::Sell, Side::Buy, 99)]
        {
            let mut book = book();
            // 价格不满足价格步长=10，但已越界，必须先终止而非报数学错误。
            book.rest(order(1, maker_side, outside, 2)).unwrap();
            assert_failure_atomic(&mut book, |book| {
                assert_eq!(
                    book.preflight_market_by_quote_with_protection(
                        &market(100, side, 1),
                        &protection_context(Some(100)),
                        &math,
                    ),
                    Ok(expected_protected_quote_plan(
                        &[],
                        0,
                        1,
                        Some((outside, 100, ProtectionSource::MarketBand)),
                    )),
                );
            });
        }
    }

    #[test]
    fn protected_quote_preflight_invalid_lot_is_atomic() {
        let math = MarketMath::try_new(0, 0, 0, 1, 10).unwrap();
        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = book();
            book.rest(order(1, maker_side, 100, 11)).unwrap();
            assert_protected_quote_rejected(
                &mut book,
                market(100, side, 2000),
                protection_context(None),
                &math,
                RejectReason::InvalidLot,
            );
        }
    }

    fn protected_quote_trade(
        tid: u128,
        maker_id: u128,
        price: i64,
        qty: u64,
        maker_side: Side,
    ) -> EngineEvent {
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId::new(tid),
            maker_order_id: OrderId::new(maker_id),
            taker_order_id: OrderId::new(100),
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
            maker_side,
        })
    }
    #[test]
    fn protected_quote_execution_multilevel_sources() {
        let p = |v| Price::try_new(v).unwrap();
        let math = protected_math();

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
            let mut book = ProductionOrderBook::new(policy());

            // 挂单源顺序故意与价格优先和先进先出顺序不同。
            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }

            let mut incoming = market(100, side, 2000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            });

            let priority_before = book.next_queue_priority();

            let actual = book
                .execute_market_by_quote_with_protection(
                    incoming,
                    &protection_context(Some(band)),
                    &math,
                    &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
                )
                .unwrap();

            let spent = if side == Side::Buy { 880 } else { 920 };

            let expected = ProtectedMarketByQuoteExecution::new(
                vec![
                    protected_quote_trade(701, 30, better, 2, maker_side),
                    protected_quote_trade(702, 40, 100, 4, maker_side),
                    protected_quote_trade(703, 10, 100, 3, maker_side),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(100),
                        source,
                    },
                ],
                QuoteAmount::new(spent),
                QuoteAmount::new(2000 - spent),
            );

            // 完整事件原序和金额比较。
            assert_eq!(actual, expected);

            // 已成交挂单删除。
            for id in [30, 40, 10] {
                assert!(book.order(OrderId::new(id)).is_none());
            }

            // 越界挂单和更远挂单均未改变。
            assert_eq!(book.order(OrderId::new(50)).unwrap().remaining(), 1);
            assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 2);

            assert_eq!(book.best_price(maker_side), Some(p(outside)));
            assert_eq!(book.next_queue_priority(), priority_before);
            assert!(book.order(OrderId::new(100)).is_none());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn protected_quote_execution_boundary_and_dust() {
        let p = |v| Price::try_new(v).unwrap();
        let math = protected_math();

        for (side, maker_side, outside) in
            [(Side::Buy, Side::Sell, 110), (Side::Sell, Side::Buy, 90)]
        {
            let context = protection_context(Some(100));

            // 空簿：预算全部未使用。
            let mut book = ProductionOrderBook::new(policy());

            assert_failure_atomic(&mut book, |book| {
                let actual = book
                    .execute_market_by_quote_with_protection(
                        market(100, side, 1000),
                        &context,
                        &math,
                        &[],
                    )
                    .unwrap();

                assert_eq!(
                    actual,
                    ProtectedMarketByQuoteExecution::new(
                        vec![],
                        QuoteAmount::new(0),
                        QuoteAmount::new(1000),
                    )
                );
            });

            // 首档越界，即使预算不足一手，仍先触发保护。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, outside, 5)).unwrap();

            assert_failure_atomic(&mut book, |book| {
                let actual = book
                    .execute_market_by_quote_with_protection(
                        market(100, side, 1),
                        &context,
                        &math,
                        &[],
                    )
                    .unwrap();

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
            });

            // 边界相等可成交；预算恰好耗尽，不产生触发。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 2)).unwrap();
            book.rest(order(2, maker_side, outside, 4)).unwrap();

            let actual = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 200),
                    &context,
                    &math,
                    &[TradeId::new(700)],
                )
                .unwrap();

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![protected_quote_trade(700, 1, 100, 2, maker_side)],
                    QuoteAmount::new(200),
                    QuoteAmount::new(0),
                )
            );
            assert!(book.order(OrderId::new(1)).is_none());
            assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 4);
            assert_eq!(validate_book(&book), Ok(()));

            // 部分挂单成交，保留原 Arena 槽位和优先级。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 10)).unwrap();

            let slot_before = *book.orders.get(&OrderId::new(1)).unwrap();
            let priority_before = book.order(OrderId::new(1)).unwrap().priority();
            let next_before = book.next_queue_priority();

            let actual = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 350),
                    &context,
                    &math,
                    &[TradeId::new(701)],
                )
                .unwrap();

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![protected_quote_trade(701, 1, 100, 3, maker_side)],
                    QuoteAmount::new(300),
                    QuoteAmount::new(50),
                )
            );
            assert_eq!(book.orders.get(&OrderId::new(1)), Some(&slot_before));
            assert_eq!(book.order(OrderId::new(1)).unwrap().remaining(), 7);
            assert_eq!(
                book.order(OrderId::new(1)).unwrap().priority(),
                priority_before,
            );
            assert_eq!(book.next_queue_priority(), next_before);
            assert_eq!(validate_book(&book), Ok(()));

            // 首档在保护内，但预算不足一手：不足一手的预算，无事件。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 5)).unwrap();

            assert_failure_atomic(&mut book, |book| {
                let actual = book
                    .execute_market_by_quote_with_protection(
                        market(100, side, 99),
                        &context,
                        &math,
                        &[],
                    )
                    .unwrap();

                assert_eq!(
                    actual,
                    ProtectedMarketByQuoteExecution::new(
                        vec![],
                        QuoteAmount::new(0),
                        QuoteAmount::new(99),
                    )
                );
            });

            // 成交后剩 5 报价资产，下一档越界：
            // 保护必须先于不足一手的预算，且触发不需要第二个 TradeId。
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 2)).unwrap();
            book.rest(order(2, maker_side, outside, 4)).unwrap();

            let actual = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 205),
                    &context,
                    &math,
                    &[TradeId::new(702)],
                )
                .unwrap();

            assert_eq!(
                actual,
                ProtectedMarketByQuoteExecution::new(
                    vec![
                        protected_quote_trade(702, 1, 100, 2, maker_side),
                        EngineEvent::MarketProtectionTriggered {
                            order_id: OrderId::new(100),
                            next_price: p(outside),
                            effective_worst_price: p(100),
                            source: ProtectionSource::MarketBand,
                        },
                    ],
                    QuoteAmount::new(200),
                    QuoteAmount::new(5),
                )
            );
            assert!(book.order(OrderId::new(1)).is_none());
            assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 4);
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn protected_quote_execution_without_boundary_scans_all() {
        let math = protected_math();

        for (side, maker_side, better, outside, farther, spent) in [
            (Side::Buy, Side::Sell, 90, 110, 120, 1230),
            (Side::Sell, Side::Buy, 110, 90, 80, 1170),
        ] {
            let mut book = ProductionOrderBook::new(policy());

            for (id, price, qty) in [
                (90, farther, 2),
                (40, 100, 4),
                (30, better, 2),
                (10, 100, 3),
                (50, outside, 1),
            ] {
                book.rest(order(id, maker_side, price, qty)).unwrap();
            }

            let priority_before = book.next_queue_priority();

            let ids = [
                TradeId::new(701),
                TradeId::new(702),
                TradeId::new(703),
                TradeId::new(704),
                TradeId::new(705),
            ];

            let actual = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 2000),
                    &protection_context(None),
                    &math,
                    &ids,
                )
                .unwrap();

            let expected = ProtectedMarketByQuoteExecution::new(
                vec![
                    protected_quote_trade(701, 30, better, 2, maker_side),
                    protected_quote_trade(702, 40, 100, 4, maker_side),
                    protected_quote_trade(703, 10, 100, 3, maker_side),
                    protected_quote_trade(704, 50, outside, 1, maker_side),
                    protected_quote_trade(705, 90, farther, 2, maker_side),
                ],
                QuoteAmount::new(spent),
                QuoteAmount::new(2000 - spent),
            );

            assert_eq!(actual, expected);
            assert!(book.orders.is_empty());
            assert!(book.bids.is_empty());
            assert!(book.asks.is_empty());
            assert_eq!(book.next_queue_priority(), priority_before);
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn protected_quote_execution_insufficient_ids_is_atomic() {
        let math = protected_math();

        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, better, 2)).unwrap();
            book.rest(order(2, maker_side, 100, 3)).unwrap();

            assert_failure_atomic(&mut book, |book| {
                // 预检得到 2 笔 Trade，但只提供一个 TradeId。
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let _ = book.execute_market_by_quote_with_protection(
                        market(100, side, 2000),
                        &protection_context(Some(100)),
                        &math,
                        &[TradeId::new(700)],
                    );
                }));

                assert!(result.is_err());
            });
        }
    }

    // 公共执行入口的业务拒绝必须在任何 BTreeMap/Arena/标识符/优先级改动之前发生。
    fn assert_protected_quote_execution_rejected(
        book: &mut ProductionOrderBook,
        incoming: MarketByQuote,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        expected: RejectReason,
    ) {
        assert_failure_atomic(book, |book| {
            assert_eq!(
                book.execute_market_by_quote_with_protection(incoming, context, math, &[],),
                Err(expected),
            );
        });
    }

    #[test]
    fn protected_quote_execution_rejections_are_atomic() {
        let p = |value| Price::try_new(value).unwrap();
        let math = protected_math();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100_000, 5)).unwrap();
            let context = protection_context(Some(100_000));
            let invalid_bps = MarketProtection {
                max_slippage_bps: Some(u32::MAX),
                worst_price: None,
            };

            // 重复标识符优先于非法基点；吃单方标识符必须确实等于挂单标识符。
            let mut incoming = market(1, side, 1000);
            incoming.market_protection = Some(invalid_bps.clone());
            assert_protected_quote_execution_rejected(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::DuplicateOrderId,
            );

            // 不重复的标识符=100：返回稳定 3007，而非重复标识符。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(invalid_bps);
            assert_protected_quote_execution_rejected(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::InvalidSlippageBps,
            );
            assert_eq!(RejectReason::InvalidSlippageBps as u16, 3007);

            // 显式用户 worst_price 不满足价格步长=10。
            let mut incoming = market(100, side, 1000);
            incoming.market_protection = Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(100_003)),
            });
            assert_protected_quote_execution_rejected(
                &mut book,
                incoming,
                &context,
                &math,
                RejectReason::InvalidTick,
            );
        }

        // 买入的滑点价格向上扩张溢出，即使市场价格区间有效也不能提交。
        let mut book = ProductionOrderBook::new(policy());
        book.rest(order(1, Side::Sell, 100_000, 5)).unwrap();
        let mut incoming = market(100, Side::Buy, 1000);
        incoming.market_protection = Some(MarketProtection {
            max_slippage_bps: Some(100),
            worst_price: None,
        });
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(i64::MAX),
            market_band_worst: Some(p(100_000)),
        };
        assert_protected_quote_execution_rejected(
            &mut book,
            incoming,
            &context,
            &math,
            RejectReason::ArithmeticOverflow,
        );

        // 进入 MarketMath 后的手错误仍保持完整状态不变。
        let lot_math = MarketMath::try_new(0, 0, 0, 1, 10).unwrap();
        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, 100, 11)).unwrap();
            assert_protected_quote_execution_rejected(
                &mut book,
                market(100, side, 2000),
                &protection_context(None),
                &lot_math,
                RejectReason::InvalidLot,
            );
        }

        // 第一个挂单已被纳入只读计划，第二个挂单的价格步长错误：
        // 整个执行入口必须拒绝且不可提前提交第一笔成交。
        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, better, 1)).unwrap();
            book.rest(order(2, maker_side, 101, 5)).unwrap();
            assert_protected_quote_execution_rejected(
                &mut book,
                market(100, side, 2000),
                &protection_context(None),
                &math,
                RejectReason::InvalidTick,
            );
        }
    }

    #[test]
    fn protected_quote_execution_dust_after_trade_does_not_trigger() {
        let math = protected_math();

        for (side, maker_side, better) in
            [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)]
        {
            let mut book = ProductionOrderBook::new(policy());
            book.rest(order(1, maker_side, better, 1)).unwrap();
            book.rest(order(2, maker_side, 100, 5)).unwrap();
            let priority_before = book.next_queue_priority();
            let second_slot = *book.orders.get(&OrderId::new(2)).unwrap();

            let result = book
                .execute_market_by_quote_with_protection(
                    market(100, side, better as u128 + 5),
                    &protection_context(Some(100)),
                    &math,
                    &[TradeId::new(700)],
                )
                .unwrap();

            assert_eq!(
                result,
                ProtectedMarketByQuoteExecution::new(
                    vec![protected_quote_trade(700, 1, better, 1, maker_side)],
                    QuoteAmount::new(better as u128),
                    QuoteAmount::new(5),
                ),
            );
            assert!(book.order(OrderId::new(1)).is_none());
            assert_eq!(book.orders.get(&OrderId::new(2)), Some(&second_slot));
            assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 5);
            assert_eq!(book.next_queue_priority(), priority_before);
            assert!(book.order(OrderId::new(100)).is_none());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }

    #[test]
    fn protected_quote_execution_ignores_priority_exhaustion() {
        let math = protected_math();

        for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
            let mut book = ProductionOrderBook::new(policy_with_ceiling(1));
            book.rest(order(1, maker_side, 100, 5)).unwrap();
            assert!(book.queue_priority_allocator.is_exhausted());
            let next_before = book.next_queue_priority();

            let result = book
                .execute_market_by_quote_with_protection(
                    market(100, side, 500),
                    &protection_context(None),
                    &math,
                    &[TradeId::new(701)],
                )
                .unwrap();

            assert_eq!(
                result,
                ProtectedMarketByQuoteExecution::new(
                    vec![protected_quote_trade(701, 1, 100, 5, maker_side)],
                    QuoteAmount::new(500),
                    QuoteAmount::new(0),
                ),
            );
            assert!(book.orders.is_empty());
            assert_eq!(book.next_queue_priority(), next_before);
            assert!(book.queue_priority_allocator.is_exhausted());
            assert_eq!(validate_book(&book), Ok(()));
        }
    }
}
