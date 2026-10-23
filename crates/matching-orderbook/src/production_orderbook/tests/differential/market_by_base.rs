//! 基础数量市价单的双向差分、价格保护边界与失败原子性。
use super::*;

/// 固定 MarketByBase 输入只包含正 Qty；TradeId 序列直接记录在命令序列中。
fn market_command(id: u128, side: Side, qty: u64, ids: &[u128]) -> Command {
    Command::MarketByBase(
        MarketByBase {
            order_id: OrderId::new(id),
            user_id: matching_domain::UserId::new(id as u64),
            side,
            qty: Qty::try_new(qty).unwrap(),
            market_protection: None,
        },
        ids.iter().copied().map(TradeId::new).collect(),
    )
}

fn fixed_market_by_base_tape(taker_side: Side) -> Vec<Command> {
    let maker_side = opposite_side(taker_side);
    let (better, worse) = match taker_side {
        Side::Buy => (105, 110),
        Side::Sell => (95, 90),
    };
    vec![
        // 空对手侧，MarketByBase 不得挂簿。
        market_command(90, taker_side, 5, &[]),
        // 单档部分成交：挂单方30 剩余 4。
        Command::Place(order(30, maker_side, better, 7)),
        market_command(91, taker_side, 3, &[9001]),
        // 第二笔买/卖入场：原有挂单方30 优先，随后同价挂单方10，再到更差价格挂单方20。
        Command::Place(order(20, maker_side, worse, 4)),
        Command::Place(order(10, maker_side, better, 5)),
        market_command(92, taker_side, 11, &[9002, 9003, 9004]),
        // 此时只余更差价格挂单方20 的 2 手；输入大于流动性，余量终结。
        market_command(93, taker_side, 9, &[9005]),
        // 当前对手侧已空；再次空。
        market_command(94, taker_side, 1, &[]),
        // 活动重复标识符优先返回 DuplicateOrderId，提供足量 TradeId（空即可）。
        Command::Place(order(50, maker_side, worse, 2)),
        market_command(50, taker_side, 1, &[]),
    ]
}

#[test]
fn fixed_market_by_base_buy_and_sell_tapes_are_differential_and_reproducible() {
    for side in [Side::Buy, Side::Sell] {
        let tape = fixed_market_by_base_tape(side);
        let first = run_tape(&tape);
        let second = run_tape(&tape);
        assert_eq!(
            first, second,
            "MarketByBase fixed tape not reproducible: {side:?}"
        );
        assert_eq!(first.len(), 10);

        for &index in &[0usize, 2, 5, 6, 7] {
            assert!(matches!(
                &first[index].outcome,
                CommandOutcome::MarketByBase(Ok(_))
            ));
            let taker_id = match &tape[index] {
                Command::MarketByBase(order, _) => order.order_id,
                _ => unreachable!(),
            };
            assert!(
                first[index]
                    .book
                    .bids
                    .orders
                    .iter()
                    .chain(&first[index].book.asks.orders)
                    .all(|entry| entry.original.order_id != taker_id)
            );
            let previous_priority = if index == 0 {
                0
            } else {
                first[index - 1].production_next_priority
            };
            assert_eq!(first[index].production_next_priority, previous_priority);
        }
        let CommandOutcome::MarketByBase(Ok(partial)) = &first[2].outcome else {
            panic!("partial")
        };
        assert_eq!(partial.len(), 1);
        assert_eq!(partial[0].qty.get(), 3);
        assert_eq!(partial[0].trade_id, TradeId::new(9001));

        let CommandOutcome::MarketByBase(Ok(multi)) = &first[5].outcome else {
            panic!("multi")
        };
        assert_eq!(
            multi
                .iter()
                .map(|t| t.maker_order_id.get())
                .collect::<Vec<_>>(),
            vec![30, 10, 20]
        );
        assert_eq!(
            multi.iter().map(|t| t.qty.get()).collect::<Vec<_>>(),
            vec![4, 5, 2]
        );
        assert_eq!(
            multi.iter().map(|t| t.trade_id.get()).collect::<Vec<_>>(),
            vec![9002, 9003, 9004]
        );
        let (better, worse) = match side {
            Side::Buy => (105, 110),
            Side::Sell => (95, 90),
        };
        assert_eq!(
            multi.iter().map(|t| t.price.get()).collect::<Vec<_>>(),
            vec![better, better, worse]
        );
        assert!(
            multi.iter().all(
                |t| t.taker_order_id == OrderId::new(92) && t.maker_side == opposite_side(side)
            )
        );

        // 说明: 挂单方20 原本 4，多个吃掉 2，余 2；下一笔全部吃掉。
        let CommandOutcome::MarketByBase(Ok(exhaust)) = &first[6].outcome else {
            panic!("remainder")
        };
        assert_eq!(exhaust.len(), 1);
        assert_eq!(exhaust[0].qty.get(), 2);
        assert!(first[6].book.bids.orders.is_empty() && first[6].book.asks.orders.is_empty());
        assert!(
            matches!(&first[7].outcome, CommandOutcome::MarketByBase(Ok(trades)) if trades.is_empty())
        );
        assert_eq!(
            first[9].outcome,
            CommandOutcome::MarketByBase(Err(RejectReason::DuplicateOrderId))
        );
        assert_eq!(first[9].book, first[8].book);
        assert_eq!(
            first[9].production_next_priority,
            first[8].production_next_priority
        );
    }
}

fn t0632_market(
    id: u128,
    side: Side,
    qty: u64,
    market_protection: Option<MarketProtection>,
) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        qty: Qty::try_new(qty).unwrap(),
        market_protection,
    }
}

fn t0632_context(reference: i64, band: Option<i64>) -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(reference).unwrap(),
        market_band_worst: band.map(|v| Price::try_new(v).unwrap()),
    }
}

fn t0632_trade(
    trade_id: u128,
    maker_id: u128,
    taker_id: u128,
    price: i64,
    qty: u64,
    maker_side: Side,
) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(trade_id),
        maker_order_id: OrderId::new(maker_id),
        taker_order_id: OrderId::new(taker_id),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        maker_side,
    })
}

fn t0632_assert_protected_case(
    makers: &[(u128, Side, i64, u64)],
    incoming: MarketByBase,
    context: CommandStartProtectionContext,
    trade_ids: &[TradeId],
    expected: Result<Vec<EngineEvent>, RejectReason>,
) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    // 两套订单簿从同一组挂单独立初始化。
    for &(id, side, price, qty) in makers {
        let maker = order(id, side, price, qty);
        reference.rest(maker.clone()).unwrap();
        production.rest(maker).unwrap();
    }

    let reference_before = observe_reference(&reference);
    let production_before = observe_production(&production);
    assert_eq!(reference_before, production_before);

    // 复用已有完整生产簿失败快照体系。
    let bids_before = level_snapshot(&production.bids);
    let asks_before = level_snapshot(&production.asks);
    let orders_before = production.orders.clone();
    let arena_before = production.arena.test_snapshot();
    let priority_before = production.next_queue_priority();
    let policy_before = *production.queue_priority_allocator.policy();

    // 必须独立调用两个公开入口。
    let reference_result = reference.execute_market_by_base_with_protection(
        incoming.clone(),
        &context,
        &math,
        trade_ids,
    );

    let production_result = production.execute_market_by_base_with_protection(
        incoming.clone(),
        &context,
        &math,
        trade_ids,
    );

    // 完整 Result 比较：包含原序 Vec<EngineEvent> 和精确 RejectReason。
    assert_eq!(reference_result, production_result);

    // 不能只依靠两套实现互相对照。
    assert_eq!(reference_result, expected);

    // 对返回的事件再次检查排序和成交标识符。
    if let Ok(events) = &reference_result {
        let mut triggered = false;
        let mut seen_trade_ids = BTreeSet::new();

        for event in events {
            match event {
                EngineEvent::Trade(trade) => {
                    assert!(!triggered, "Trade appeared after protection event");
                    assert!(trade.qty.get() > 0);
                    assert!(seen_trade_ids.insert(trade.trade_id.get()));
                }
                EngineEvent::MarketProtectionTriggered { .. } => {
                    assert!(!triggered, "more than one protection event");
                    triggered = true;
                }
                other => panic!("unexpected protected event: {other:?}"),
            }
        }
    }

    // 事件比较之后，才观察并比较执行后订单簿。
    let reference_after = observe_reference(&reference);
    let production_after = observe_production(&production);

    assert_eq!(reference_after, production_after);

    assert_regular_book_invariants(&production_after, production.next_queue_priority(), 0);

    if expected.is_err() {
        // 参考簿：完整业务状态必须不变。
        assert_eq!(reference_after, reference_before);

        // 生产簿：包括 BTreeMap、Arena、链表和分配器。
        assert_eq!(production_after, production_before);

        assert_production_failure_state_unchanged(
            &production,
            &bids_before,
            &asks_before,
            &orders_before,
            &arena_before,
            priority_before,
            policy_before,
        );
    } else {
        // 市价吃单方不得挂簿。
        assert!(!reference_contains_order(&reference, incoming.order_id));
        assert!(production.order(incoming.order_id).is_none());
    }

    // 成功/拒绝均不得分配 QueuePriority。
    assert_eq!(production.next_queue_priority(), priority_before);
    assert_eq!(validate_book(&production), Ok(()));
}

#[test]
fn t0632_protected_multilevel_sources_are_differential() {
    let p = |v| Price::try_new(v).unwrap();

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
        let makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        let incoming = t0632_market(
            100,
            side,
            20,
            Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            }),
        );

        let expected = vec![
            t0632_trade(701, 30, 100, better, 2, maker_side),
            t0632_trade(702, 40, 100, 100, 4, maker_side),
            t0632_trade(703, 10, 100, 100, 3, maker_side),
            EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source,
            },
        ];

        t0632_assert_protected_case(
            &makers,
            incoming,
            t0632_context(100_000, Some(band)),
            &[TradeId::new(701), TradeId::new(702), TradeId::new(703)],
            Ok(expected),
        );
    }
}

#[test]
fn t0632_protected_execution_boundaries_are_differential() {
    let p = |v| Price::try_new(v).unwrap();

    for (side, maker_side, better, outside, farther) in [
        (Side::Buy, Side::Sell, 90, 110, 120),
        (Side::Sell, Side::Buy, 110, 90, 80),
    ] {
        let bounded = Some(MarketProtection {
            max_slippage_bps: None,
            worst_price: Some(p(100)),
        });

        let boundary_makers = [(1, maker_side, 100, 2), (2, maker_side, outside, 4)];

        // 1. 边界相等可成交；吃单方恰好耗尽，不得触发保护。
        t0632_assert_protected_case(
            &boundary_makers,
            t0632_market(100, side, 2, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![t0632_trade(700, 1, 100, 100, 2, maker_side)]),
        );

        // 2. 边界成交后仍有余量，遇到下一档才触发保护。
        t0632_assert_protected_case(
            &boundary_makers,
            t0632_market(100, side, 3, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![
                t0632_trade(700, 1, 100, 100, 2, maker_side),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source: ProtectionSource::Both,
                },
            ]),
        );

        // 3. 单挂单部分成交，吃单方耗尽，无触发。
        t0632_assert_protected_case(
            &[(1, maker_side, 100, 5)],
            t0632_market(100, side, 2, bounded.clone()),
            t0632_context(100_000, Some(100)),
            &[TradeId::new(700)],
            Ok(vec![t0632_trade(700, 1, 100, 100, 2, maker_side)]),
        );

        // 4. 首档越界：零笔 Trade，零 TradeId。
        t0632_assert_protected_case(
            &[(1, maker_side, outside, 5)],
            t0632_market(100, side, 10, None),
            t0632_context(100_000, Some(100)),
            &[],
            Ok(vec![EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(100),
                next_price: p(outside),
                effective_worst_price: p(100),
                source: ProtectionSource::MarketBand,
            }]),
        );

        // 5. 空簿：无 Trade，也无触发事件。
        t0632_assert_protected_case(
            &[],
            t0632_market(100, side, 10, bounded),
            t0632_context(100_000, Some(100)),
            &[],
            Ok(vec![]),
        );

        // 6. 无有效保护：扫描全部多档，包括边界以外的挂单。
        let all_makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        t0632_assert_protected_case(
            &all_makers,
            t0632_market(100, side, 20, None),
            t0632_context(100_000, None),
            &[
                TradeId::new(701),
                TradeId::new(702),
                TradeId::new(703),
                TradeId::new(704),
                TradeId::new(705),
            ],
            Ok(vec![
                t0632_trade(701, 30, 100, better, 2, maker_side),
                t0632_trade(702, 40, 100, 100, 4, maker_side),
                t0632_trade(703, 10, 100, 100, 3, maker_side),
                t0632_trade(704, 50, 100, outside, 1, maker_side),
                t0632_trade(705, 90, 100, farther, 2, maker_side),
            ]),
        );
    }
}

#[test]
fn t0632_protected_rejections_are_differential_and_atomic() {
    let p = |v| Price::try_new(v).unwrap();

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let makers = [(1, maker_side, 100_000, 5)];

        let invalid_bps = MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        };

        // 重复标识符优先于非法基点。
        t0632_assert_protected_case(
            &makers,
            t0632_market(1, side, 5, Some(invalid_bps.clone())),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::DuplicateOrderId),
        );

        // 3007：即使市场价格区间可用，非法基点也必须拒绝。
        t0632_assert_protected_case(
            &makers,
            t0632_market(100, side, 5, Some(invalid_bps)),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::InvalidSlippageBps),
        );

        // 显式最差价格未按价格步长=10 对齐。
        t0632_assert_protected_case(
            &makers,
            t0632_market(
                100,
                side,
                5,
                Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(p(100_003)),
                }),
            ),
            t0632_context(100_003, Some(100_000)),
            &[],
            Err(RejectReason::InvalidTick),
        );
    }

    // 买入参考值=`i64::MAX`，100 个基点 向上扩张，算术溢出。
    t0632_assert_protected_case(
        &[(1, Side::Sell, 100_000, 5)],
        t0632_market(
            100,
            Side::Buy,
            5,
            Some(MarketProtection {
                max_slippage_bps: Some(100),
                worst_price: None,
            }),
        ),
        t0632_context(i64::MAX, Some(100_000)),
        &[],
        Err(RejectReason::ArithmeticOverflow),
    );
}
