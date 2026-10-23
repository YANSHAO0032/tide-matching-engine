//! 保护报价市价单的多档先进先出、保护来源与定向取整差分。
use super::*;
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
};
use matching_domain::{EngineEvent, MarketProtection, ProtectionSource, QuoteAmount};
fn quote(
    id: u128,
    side: Side,
    budget: u128,
    protection: Option<MarketProtection>,
) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        quote_budget: QuoteBudget::try_new(budget).unwrap(),
        market_protection: protection,
    }
}

fn context(band: Option<i64>) -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(100_003).unwrap(),
        market_band_worst: band.map(|p| Price::try_new(p).unwrap()),
    }
}

fn trade(tid: u128, maker_id: u128, price: i64, qty: u64, maker_side: Side) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(tid),
        maker_order_id: OrderId::new(maker_id),
        taker_order_id: OrderId::new(100),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        maker_side,
    })
}
fn run_case(
    makers: &[(u128, Side, i64, u64)],
    incoming: &MarketByQuote,
    context: &CommandStartProtectionContext,
    math: &MarketMath,
    trade_ids: &[TradeId],
) -> (
    Result<ProtectedMarketByQuoteExecution, RejectReason>,
    ObservedBook,
) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());

    // ============================================================
    // 1. 分别构建参考簿 / 生产簿
    // ============================================================

    for &(id, side, price, qty) in makers {
        let maker = order(id, side, price, qty);

        assert_eq!(reference.rest(maker.clone()), Ok(()));
        assert_eq!(production.rest(maker), Ok(()));
    }

    // ============================================================
    // 2. 执行前的完整业务状态
    // ============================================================

    let reference_before = observe_reference(&reference);
    let production_before = observe_production(&production);

    assert_eq!(
        reference_before, production_before,
        "initial protected Quote book state diverged"
    );

    assert_eq!(validate_book(&production), Ok(()));

    // ============================================================
    // 3. 保存生产簿完整物理状态
    // ============================================================

    let bids_before = level_snapshot(&production.bids);
    let asks_before = level_snapshot(&production.asks);

    let orders_before = production.orders.clone();
    let arena_before = production.arena.test_snapshot();

    let next_before = production.next_queue_priority();

    let policy_before = *production.queue_priority_allocator.policy();

    // ============================================================
    // 4. 两套引擎独立执行公开受保护报价资产入口
    // ============================================================

    let reference_result = reference.execute_market_by_quote_with_protection(
        incoming.clone(),
        context,
        math,
        trade_ids,
    );

    let production_result = production.execute_market_by_quote_with_protection(
        incoming.clone(),
        context,
        math,
        trade_ids,
    );

    // 完整比较：
    // - 结果 `Result` / 拒绝原因 `RejectReason`
    // - Vec<EngineEvent> 原始事件顺序
    // - 已用预算 `spent_quote`
    // - 未用预算 `unused_quote`
    assert_eq!(
        reference_result, production_result,
        "protected MarketByQuote result diverged"
    );

    // ============================================================
    // 5. 执行后完整业务观察
    // ============================================================

    let reference_after = observe_reference(&reference);
    let production_after = observe_production(&production);

    // 订单、剩余量、先进先出、深度、最优价格、优先级
    // 必须完全一致。
    assert_eq!(
        reference_after, production_after,
        "protected MarketByQuote book state diverged"
    );

    // 市价订单不能分配新的 QueuePriority。
    assert_eq!(
        production.next_queue_priority(),
        next_before,
        "protected MarketByQuote consumed QueuePriority"
    );

    assert_regular_book_invariants(&production_after, production.next_queue_priority(), 0);

    // ============================================================
    // 6. 按参考簿的真实执行结果分支
    // ============================================================

    match reference_result.as_ref() {
        // --------------------------------------------------------
        // Err：两套订单簿必须保持失败原子性
        // --------------------------------------------------------
        Err(_) => {
            // 参考簿完整业务状态不变。
            assert_eq!(
                reference_after, reference_before,
                "failed protected Quote mutated Reference"
            );

            // 生产簿完整业务状态不变。
            assert_eq!(
                production_after, production_before,
                "failed protected Quote mutated Production"
            );

            // 生产簿物理状态不变：
            // - 买卖两侧价格索引 `bids` / `asks`（BTreeMap）
            // - PriceLevel 链表和聚合
            // - OrderId 索引
            // - Arena 槽位 / 空闲链表
            // - 下一队列优先级 `next`（QueuePriority）
            // - 分配器策略
            assert_production_failure_state_unchanged(
                &production,
                &bids_before,
                &asks_before,
                &orders_before,
                &arena_before,
                next_before,
                policy_before,
            );
        }

        // --------------------------------------------------------
        // Ok：直接使用模式匹配绑定的执行
        // --------------------------------------------------------
        Ok(execution) => {
            // 市价吃单方不允许挂簿。
            assert!(
                !reference_contains_order(&reference, incoming.order_id,),
                "protected Quote taker rested in Reference"
            );

            assert!(
                production.order(incoming.order_id).is_none(),
                "protected Quote taker rested in Production"
            );

            // 验证 TradeId 唯一性及保护终结事件顺序。
            let mut seen_trade_ids = BTreeSet::new();
            let mut seen_trigger = false;

            for (index, event) in execution.events().iter().enumerate() {
                match event {
                    EngineEvent::Trade(fill) => {
                        // 触发出现后不允许继续产生成交。
                        assert!(!seen_trigger, "Trade emitted after protection trigger");

                        // 成交数量必须严格正值。
                        assert!(
                            fill.qty.get() > 0,
                            "protected Quote emitted zero-quantity Trade"
                        );

                        // TradeId 在本次执行中不得重复。
                        assert!(
                            seen_trade_ids.insert(fill.trade_id.get()),
                            "protected Quote emitted duplicate TradeId"
                        );
                    }

                    EngineEvent::MarketProtectionTriggered { .. } => {
                        // 同一命令最多只能有一个触发。
                        assert!(!seen_trigger, "protected Quote emitted multiple triggers");

                        // 触发必须是最后一个事件。
                        assert_eq!(
                            index + 1,
                            execution.events().len(),
                            "protection trigger must be terminal"
                        );

                        seen_trigger = true;
                    }

                    other => {
                        panic!("unexpected protected Quote event: {other:?}");
                    }
                }
            }
        }
    }

    // ============================================================
    // 7. 最终生产簿整簿验证
    // ============================================================

    assert_eq!(
        validate_book(&production),
        Ok(()),
        "Production validator failed after protected Quote"
    );

    // Result 在前面的 assert_eq! 和 `match` 中均通过引用访问，
    // 因此仍然可以将 production_result 原样返回。
    (production_result, production_after)
}
#[test]
fn t0638_multilevel_fifo_and_protection_sources() {
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
        let makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        let incoming = quote(
            100,
            side,
            2000,
            Some(MarketProtection {
                max_slippage_bps: None,
                worst_price: Some(p(user)),
            }),
        );

        let context = context(Some(band));
        let ids = [TradeId::new(701), TradeId::new(702), TradeId::new(703)];

        let spent = if side == Side::Buy { 880 } else { 920 };

        let expected = Ok(ProtectedMarketByQuoteExecution::new(
            vec![
                trade(701, 30, better, 2, maker_side),
                trade(702, 40, 100, 4, maker_side),
                trade(703, 10, 100, 3, maker_side),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: p(outside),
                    effective_worst_price: p(100),
                    source,
                },
            ],
            QuoteAmount::new(spent),
            QuoteAmount::new(2000 - spent),
        ));

        let first = run_case(&makers, &incoming, &context, &math, &ids);
        let second = run_case(&makers, &incoming, &context, &math, &ids);

        // 完整结果和最终盘口必须可重复。
        assert_eq!(first, second);
        assert_eq!(first.0, expected);

        let maker_book = if maker_side == Side::Buy {
            &first.1.bids
        } else {
            &first.1.asks
        };

        // 越界挂单和更远挂单保留在盘口，
        // 其先进先出优先级也不发生重新分配。
        assert_eq!(
            maker_book
                .orders
                .iter()
                .map(|o| (o.original.order_id, o.remaining, o.priority,))
                .collect::<Vec<_>>(),
            vec![
                (OrderId::new(50), 1, QueuePriority::new(4)),
                (OrderId::new(90), 2, QueuePriority::new(0)),
            ],
        );

        assert_eq!(maker_book.best_price, Some(p(outside)));
    }
}
fn assert_expected(
    makers: &[(u128, Side, i64, u64)],
    incoming: &MarketByQuote,
    context: &CommandStartProtectionContext,
    math: &MarketMath,
    ids: &[TradeId],
    expected: &Result<ProtectedMarketByQuoteExecution, RejectReason>,
) -> ObservedBook {
    let first = run_case(makers, incoming, context, math, ids);
    let second = run_case(makers, incoming, context, math, ids);

    assert_eq!(first, second, "protected Quote not reproducible");
    assert_eq!(&first.0, expected);

    first.1
}
#[test]
fn t0638_protection_precedes_quote_dust() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for (side, maker_side, better, outside) in [
        (Side::Buy, Side::Sell, 90, 110),
        (Side::Sell, Side::Buy, 110, 90),
    ] {
        let makers = [(1, maker_side, better, 1), (2, maker_side, outside, 5)];

        let incoming = quote(100, side, better as u128 + 5, None);

        let expected = Ok(ProtectedMarketByQuoteExecution::new(
            vec![
                trade(701, 1, better, 1, maker_side),
                EngineEvent::MarketProtectionTriggered {
                    order_id: OrderId::new(100),
                    next_price: Price::try_new(outside).unwrap(),
                    effective_worst_price: Price::try_new(100).unwrap(),
                    source: ProtectionSource::MarketBand,
                },
            ],
            QuoteAmount::new(better as u128),
            QuoteAmount::new(5),
        ));

        let after = assert_expected(
            &makers,
            &incoming,
            &context(Some(100)),
            &math,
            &[TradeId::new(701)],
            &expected,
        );

        let side_view = if maker_side == Side::Buy {
            &after.bids
        } else {
            &after.asks
        };

        assert_eq!(side_view.orders.len(), 1);
        assert_eq!(side_view.orders[0].original.order_id, OrderId::new(2),);
        assert_eq!(side_view.orders[0].remaining, 5);
        assert_eq!(side_view.orders[0].priority, QueuePriority::new(1));
    }
}
#[test]
fn t0638_duplicate_and_invalid_bps_are_atomic() {
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for side in [Side::Buy, Side::Sell] {
        let maker_side = opposite_side(side);
        let makers = [(1, maker_side, 100_000, 5)];

        let invalid_bps = Some(MarketProtection {
            max_slippage_bps: Some(u32::MAX),
            worst_price: None,
        });

        let ctx = context(Some(100_000));

        let duplicate = quote(1, side, 1000, invalid_bps.clone());

        assert_expected(
            &makers,
            &duplicate,
            &ctx,
            &math,
            &[],
            &Err(RejectReason::DuplicateOrderId),
        );

        let nonduplicate = quote(100, side, 1000, invalid_bps);

        assert_expected(
            &makers,
            &nonduplicate,
            &ctx,
            &math,
            &[],
            &Err(RejectReason::InvalidSlippageBps),
        );
    }
}

#[test]
fn t0639_protected_quote_bps_boundary_execution_differential() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    const BUDGET: u128 = 2_000_000;

    // 固定参考值=200_010，价格步长=10。
    //
    // 0 基点:
    //   买入 / 卖出 = 200_010
    //
    // 9_999 基点:
    //   买入 = `floor_to_tick(399_999.999)` = 399_990
    //   卖出 = `ceil_to_tick(20.001)`       = 30
    //
    // 10_000 基点:
    //   非法滑点基点 `InvalidSlippageBps`（3007）
    //
    // 最后一列为手写的三笔预期成交本金之和。
    let cases: [(Side, u32, i64, i64, i64, u128); 6] = [
        (Side::Buy, 0, 200_000, 200_010, 200_020, 600_020),
        (Side::Sell, 0, 200_020, 200_010, 200_000, 600_040),
        (Side::Buy, 9_999, 399_980, 399_990, 400_000, 1_199_960),
        (Side::Sell, 9_999, 40, 30, 20, 100),
        (Side::Buy, 10_000, 200_000, 200_010, 200_020, 0),
        (Side::Sell, 10_000, 200_020, 200_010, 200_000, 0),
    ];

    for (side, bps, better, boundary, outside, spent) in cases {
        let maker_side = opposite_side(side);

        // OrderId、插入顺序与最终撮合顺序故意不同。
        //
        // 优先级:
        //   挂单 40 -> 0，越界挂单
        //   挂单 20 -> 1，边界相等
        //   挂单 30 -> 2，内侧价格
        //   挂单 10 -> 3，边界相等、先进先出第二张
        //
        // 正确成交原序：
        //   挂单 30 -> 挂单 20 -> 挂单 10
        //
        // 挂单 40 不允许成交。
        let makers = [
            (40, maker_side, outside, 2),
            (20, maker_side, boundary, 1),
            (30, maker_side, better, 1),
            (10, maker_side, boundary, 1),
        ];

        let incoming = quote(
            100,
            side,
            BUDGET,
            Some(MarketProtection {
                max_slippage_bps: Some(bps),
                worst_price: None,
            }),
        );

        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(200_010),
            market_band_worst: None,
        };

        // 只提供三笔 TradeId。
        // 保护触发不能消耗 TradeId。
        let trade_ids = [TradeId::new(701), TradeId::new(702), TradeId::new(703)];

        // 预期由测试数据手写，不读取任何簿的私有计划。
        let expected = if bps == 10_000 {
            Err(RejectReason::InvalidSlippageBps)
        } else {
            Ok(ProtectedMarketByQuoteExecution::new(
                vec![
                    trade(701, 30, better, 1, maker_side),
                    trade(702, 20, boundary, 1, maker_side),
                    trade(703, 10, boundary, 1, maker_side),
                    EngineEvent::MarketProtectionTriggered {
                        order_id: OrderId::new(100),
                        next_price: p(outside),
                        effective_worst_price: p(boundary),
                        source: ProtectionSource::User,
                    },
                ],
                QuoteAmount::new(spent),
                QuoteAmount::new(BUDGET - spent),
            ))
        };

        // 两套模型各自独立执行。
        //
        // 现有 run_case 应保持以下完整检查：
        // - 参考簿 / 生产簿 Result 完全相等
        // - 原序 EngineEvent
        // - 已用预算 `spent_quote` / 未用预算 `unused_quote`
        // - 订单和先进先出投影
        // - 深度 / 最优价格 / 优先级
        // - 吃单方不挂单
        // - Trade 数量严格正
        // - 生产簿 validate_book
        // - Err 时两模型失败原子性
        // - Err 时生产簿完整物理快照不变
        let first = run_case(&makers, &incoming, &context, &math, &trade_ids);

        let second = run_case(&makers, &incoming, &context, &math, &trade_ids);

        // 固定命令序列运行两次，结果及最终可观察状态一致。
        assert_eq!(
            first, second,
            "T06.39 nondeterministic: side={side:?}, bps={bps}"
        );

        // 不仅比较双模型，也比较测试独立手写的固定预期。
        assert_eq!(
            first.0, expected,
            "T06.39 unexpected execution: side={side:?}, bps={bps}"
        );

        if bps == 10_000 {
            // run_case 的 Err 分支已经验证完整失败原子性。
            // 非法基点必须在首笔状态变更前被拒绝。
            continue;
        }

        // --------------------------------------------------------
        // 成功场景：越界挂单不成交、不删除、不改变优先级。
        // --------------------------------------------------------

        let after = &first.1;

        let maker_book = match maker_side {
            Side::Buy => &after.bids,
            Side::Sell => &after.asks,
        };

        // 只有越界挂单 40 仍在簿中。
        // 内侧挂单和两个边界挂单已完全成交。
        assert_eq!(
            maker_book.orders.len(),
            1,
            "unexpected resting maker count: side={side:?}, bps={bps}"
        );

        let remaining_maker = &maker_book.orders[0];

        assert_eq!(remaining_maker.original.order_id, OrderId::new(40),);

        assert_eq!(remaining_maker.original.price, p(outside),);

        // 未成交挂单原始数量完整保留。
        assert_eq!(remaining_maker.remaining, 2,);

        // 挂单 40 最先插入，优先级=0。
        // 市价成交和保护触发均不能重新分配优先级。
        assert_eq!(remaining_maker.priority, QueuePriority::new(0),);

        // 越界价格成为对手侧最新最优价格。
        assert_eq!(maker_book.best_price, Some(p(outside)),);

        // 原有成交档位已经移除，只剩一个越界价档。
        assert_eq!(maker_book.depth.len(), 1);

        assert_eq!(maker_book.depth[0].price, p(outside),);

        assert_eq!(maker_book.depth[0].order_count, 1,);

        assert_eq!(maker_book.depth[0].total_qty, 2,);

        // 吃单方不挂单；当前另一侧必须为空。
        let taker_book = match side {
            Side::Buy => &after.bids,
            Side::Sell => &after.asks,
        };

        assert!(taker_book.orders.is_empty());
        assert!(taker_book.depth.is_empty());
        assert_eq!(taker_book.best_price, None);
    }
}

#[test]
fn t0639_protected_quote_unaligned_reference_directional_rounding_execution() {
    let p = |value| Price::try_new(value).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 10, 1).unwrap();

    for side in [Side::Buy, Side::Sell] {
        let maker_side = opposite_side(side);

        // 挂单价格合法，且按价格步长=10 对齐。
        // 买入：200_010 位于有效保护边界上，允许成交。
        // 卖出：200_010 低于有效保护边界，必须停止。
        let makers = [(40, maker_side, 200_010, 2)];

        let incoming = quote(
            100,
            side,
            2_000_000,
            Some(MarketProtection {
                max_slippage_bps: Some(0),
                worst_price: None,
            }),
        );

        // 命令开始时参考值故意不按价格步长对齐。
        //
        // 参考值 = 200_013
        // 价格步长      = 10
        // 基点       = 0
        //
        // 方向性取整规则：
        // 买入  -> `floor_to_tick(200_013)` = 200_010
        // 卖出 -> `ceil_to_tick(200_013)`  = 200_020
        //
        // 必须将原始参考值交给领域层计算，
        // 测试中不能提前进行对齐。
        let context = CommandStartProtectionContext {
            reference_price_at_command_start: p(200_013),
            market_band_worst: None,
        };

        // 固定预期独立手写，不读取任何一侧的私有计划。
        let (trade_ids, expected) = match side {
            Side::Buy => {
                // 买入有效边界：200_010。
                // 挂单@200_010 恰好等于边界，允许成交。
                //
                // 数量 = 2
                // 已用预算 = 200_010 * 2 = 400_020
                // 未用预算 = 2_000_000 - 400_020 = 1_599_980
                //
                // 必须提供一枚 TradeId。
                (
                    vec![TradeId::new(701)],
                    Ok(ProtectedMarketByQuoteExecution::new(
                        vec![trade(701, 40, 200_010, 2, maker_side)],
                        QuoteAmount::new(400_020),
                        QuoteAmount::new(1_599_980),
                    )),
                )
            }

            Side::Sell => {
                // 卖出有效边界：200_020。
                // 挂单@200_010 低于边界，首档即越界。
                //
                // 不发生任何成交，不消耗 TradeId。
                // 返回一个终结保护触发。
                //
                // 已用预算 = 0
                // 未用预算 = 2_000_000
                (
                    vec![],
                    Ok(ProtectedMarketByQuoteExecution::new(
                        vec![EngineEvent::MarketProtectionTriggered {
                            order_id: OrderId::new(100),
                            next_price: p(200_010),
                            effective_worst_price: p(200_020),
                            source: ProtectionSource::User,
                        }],
                        QuoteAmount::new(0),
                        QuoteAmount::new(2_000_000),
                    )),
                )
            }
        };

        // ========================================================
        // 1. 参考簿 / 生产簿独立执行第一次
        // ========================================================

        let first = run_case(&makers, &incoming, &context, &math, &trade_ids);

        // ========================================================
        // 2. 参考簿 / 生产簿独立执行第二次
        // ========================================================

        let second = run_case(&makers, &incoming, &context, &math, &trade_ids);

        // 两次完整结果与最终盘口必须一致。
        //
        // run_case 已包含：
        // - 参考簿 / 生产簿完整结果比较
        // - 原序 EngineEvent
        // - 已用预算 / 未用预算
        // - 订单、剩余量、先进先出、深度、最优价格、优先级
        // - 吃单方不挂单
        // - Trade 数量严格正
        // - 生产簿 validate_book
        assert_eq!(
            first, second,
            "T06.39 directional rounding is nondeterministic: \
         side={side:?}"
        );

        // 两模型结果还必须符合独立手写的固定预期。
        assert_eq!(
            first.0, expected,
            "T06.39 unexpected directional rounding result: \
         side={side:?}"
        );

        // ========================================================
        // 3. 精确检查最终盘口
        // ========================================================

        let after = &first.1;

        let maker_book = match maker_side {
            Side::Buy => &after.bids,
            Side::Sell => &after.asks,
        };

        match side {
            Side::Buy => {
                // 挂单@200_010 完全成交，订单和价档均移除。
                assert!(maker_book.orders.is_empty());
                assert!(maker_book.depth.is_empty());
                assert_eq!(maker_book.best_price, None);
            }

            Side::Sell => {
                // 首档越界，挂单不成交、不删除。
                assert_eq!(maker_book.orders.len(), 1);

                let remaining_maker = &maker_book.orders[0];

                assert_eq!(remaining_maker.original.order_id, OrderId::new(40),);

                assert_eq!(remaining_maker.original.price, p(200_010),);

                assert_eq!(remaining_maker.remaining, 2);

                // 原始优先级保持不变。
                assert_eq!(remaining_maker.priority, QueuePriority::new(0),);

                // 价档保持完整。
                assert_eq!(maker_book.depth.len(), 1);

                assert_eq!(maker_book.depth[0].price, p(200_010),);

                assert_eq!(maker_book.depth[0].order_count, 1);
                assert_eq!(maker_book.depth[0].total_qty, 2);

                assert_eq!(maker_book.best_price, Some(p(200_010)),);
            }
        }

        // 两种方向均不允许吃单方挂簿。
        let taker_book = match side {
            Side::Buy => &after.bids,
            Side::Sell => &after.asks,
        };

        assert!(taker_book.orders.is_empty());
        assert!(taker_book.depth.is_empty());
        assert_eq!(taker_book.best_price, None);
    }
}
