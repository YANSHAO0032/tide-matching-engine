//! 保护报价市价单的固定边界场景、失败原子性与成交标识符契约。
// 受保护的 MarketByQuote 固定差分场景补充。
//
// 独立构建两套订单簿，逐项验证受保护报价执行结果与状态。
//
// 验证：
// 1. 参考簿 / 生产簿独立执行公开入口；
// 2. 完整比较原序事件、已用预算、未用预算；
// 3. 比较双侧订单、先进先出、深度、最优价格、优先级；
// 4. 失败时验证参考簿业务状态与生产簿完整物理状态不变；
// 5. TradeId 不足保留触发不变量断言并停止处理契约。

use super::{
    ObservedBook, ProductionOrderBook, ReferenceOrderBook,
    assert_production_failure_state_unchanged, assert_regular_book_invariants, level_snapshot,
    observe_production, observe_reference, order, policy, reference_contains_order, validate_book,
};

use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
};

use matching_domain::{
    EngineEvent, MarketByQuote, MarketMath, MarketProtection, OrderId, Price, Qty, QuoteAmount,
    QuoteBudget, RejectReason, Side, TradeEvent, TradeId, UserId,
};

use std::panic::{AssertUnwindSafe, catch_unwind};

type Expected = Result<ProtectedMarketByQuoteExecution, RejectReason>;

type Maker = (u128, Side, i64, u64);

// ============================================================
// 测试数据
// ============================================================

fn math(tick: i64, lot: u64) -> MarketMath {
    MarketMath::try_new(0, 0, 0, tick, lot).unwrap()
}

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

fn ctx(reference: i64, band: Option<i64>) -> CommandStartProtectionContext {
    CommandStartProtectionContext {
        reference_price_at_command_start: Price::try_new(reference).unwrap(),

        market_band_worst: band.map(|value| Price::try_new(value).unwrap()),
    }
}

fn trade(id: u128, maker: u128, price: i64, qty: u64, maker_side: Side) -> EngineEvent {
    EngineEvent::Trade(TradeEvent {
        trade_id: TradeId::new(id),
        maker_order_id: OrderId::new(maker),
        taker_order_id: OrderId::new(100),
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
        maker_side,
    })
}

fn expected(events: Vec<EngineEvent>, spent: u128, unused: u128) -> Expected {
    Ok(ProtectedMarketByQuoteExecution::new(
        events,
        QuoteAmount::new(spent),
        QuoteAmount::new(unused),
    ))
}

// ============================================================
// 双模型构建
// ============================================================

fn build(makers: &[Maker]) -> (ReferenceOrderBook, ProductionOrderBook) {
    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());

    for &(id, side, price, qty) in makers {
        let resting = order(id, side, price, qty);

        reference.rest(resting.clone()).unwrap();
        production.rest(resting).unwrap();
    }

    (reference, production)
}

// ============================================================
// 差分执行与观察
// ============================================================

/// 两套引擎分别调用自己的公开受保护报价资产执行入口。
///
/// 预期由测试手写固定预期构成，而不是从任意一侧结果推导。
///
/// stable_maker 用于验证部分成交挂单的 Arena 槽位
/// 和 QueuePriority 在成交前后不变。
fn execute_once(
    makers: &[Maker],
    incoming: &MarketByQuote,
    context: &CommandStartProtectionContext,
    math: &MarketMath,
    trade_ids: &[TradeId],
    wanted: &Expected,
    stable_maker: Option<u128>,
) -> ObservedBook {
    let (mut reference, mut production) = build(makers);

    assert_eq!(validate_book(&production), Ok(()));

    // 执行前业务状态。
    let reference_before = observe_reference(&reference);
    let production_before = observe_production(&production);

    assert_eq!(reference_before, production_before);

    // 生产簿执行前物理状态。
    let bids_before = level_snapshot(&production.bids);
    let asks_before = level_snapshot(&production.asks);

    let orders_before = production.orders.clone();
    let arena_before = production.arena.test_snapshot();

    let next_before = production.next_queue_priority();

    let policy_before = *production.queue_priority_allocator.policy();

    // 部分成交挂单的原始槽位和优先级。
    let stable_before = stable_maker.map(|id| {
        let id = OrderId::new(id);

        let slot = *production.orders.get(&id).unwrap();
        let priority = production.order(id).unwrap().priority();

        (id, slot, priority)
    });

    // --------------------------------------------------------
    // 两套模型独立执行
    // --------------------------------------------------------

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

    // 双方必须分别符合独立固定预期。
    assert_eq!(
        &reference_result, wanted,
        "Reference differs from independent fixture"
    );

    assert_eq!(
        &production_result, wanted,
        "Production differs from independent fixture"
    );

    // Result 包含：
    // Vec<EngineEvent> 完整原序、已用预算、未用预算或 RejectReason。
    assert_eq!(
        reference_result, production_result,
        "protected Quote models differ"
    );

    // --------------------------------------------------------
    // 执行后完整业务观察
    // --------------------------------------------------------

    let reference_after = observe_reference(&reference);
    let production_after = observe_production(&production);

    assert_eq!(reference_after, production_after);

    // 市价单不能消耗 QueuePriority。
    assert_eq!(production.next_queue_priority(), next_before);

    assert_eq!(validate_book(&production), Ok(()));

    assert_regular_book_invariants(&production_after, next_before, 0);

    // --------------------------------------------------------
    // 拒绝原子性
    // --------------------------------------------------------

    if wanted.is_err() {
        // 参考簿业务状态不变。
        assert_eq!(reference_after, reference_before);

        // 生产簿业务状态不变。
        assert_eq!(production_after, production_before);

        // 生产簿物理状态不变。
        assert_production_failure_state_unchanged(
            &production,
            &bids_before,
            &asks_before,
            &orders_before,
            &arena_before,
            next_before,
            policy_before,
        );
    } else {
        // ----------------------------------------------------
        // 成功路径检查
        // ----------------------------------------------------

        // 吃单方不应挂簿。
        assert!(!reference_contains_order(&reference, incoming.order_id));

        assert!(production.order(incoming.order_id).is_none());

        let execution = wanted.as_ref().unwrap();

        // 报价资产本金守恒。
        assert_eq!(
            execution
                .spent_quote()
                .checked_add(execution.unused_quote())
                .unwrap()
                .get(),
            incoming.quote_budget.get(),
        );

        // Trade* -> 最多一个终结保护事件。
        let mut terminal_seen = false;

        for (index, event) in execution.events().iter().enumerate() {
            match event {
                EngineEvent::Trade(fill) => {
                    assert!(!terminal_seen, "Trade emitted after protection trigger");

                    assert!(fill.qty.get() > 0);
                }

                EngineEvent::MarketProtectionTriggered { .. } => {
                    assert!(!terminal_seen);

                    assert_eq!(index + 1, execution.events().len());

                    terminal_seen = true;
                }

                other => {
                    panic!("unexpected protected Quote event: {other:?}");
                }
            }
        }

        // 如果指定部分成交挂单，必须保留槽位/优先级。
        if let Some((id, slot, priority)) = stable_before {
            assert_eq!(production.orders.get(&id), Some(&slot));

            assert_eq!(production.order(id).unwrap().priority(), priority);
        }
    }

    production_after
}

/// 同一固定场景完整重复两次，保证确定性。
fn assert_case(
    makers: &[Maker],
    incoming: &MarketByQuote,
    context: &CommandStartProtectionContext,
    math: &MarketMath,
    ids: &[TradeId],
    wanted: Expected,
    stable_maker: Option<u128>,
) -> ObservedBook {
    let first = execute_once(makers, incoming, context, math, ids, &wanted, stable_maker);

    let second = execute_once(makers, incoming, context, math, ids, &wanted, stable_maker);

    assert_eq!(
        first, second,
        "fixed protected Quote scenario is non-deterministic"
    );

    first
}

// ============================================================
// 测试一：空簿、边界相等、预算耗尽、两类不足一手的预算
// ============================================================

#[test]
fn t0638_empty_equal_budget_initial_and_after_dust() {
    let math = math(10, 1);

    for (side, maker_side, better, outside) in [
        (Side::Buy, Side::Sell, 90, 110),
        (Side::Sell, Side::Buy, 110, 90),
    ] {
        let bounded = ctx(100_003, Some(100));

        // 1. 空簿
        assert_case(
            &[],
            &quote(100, side, 1000, None),
            &bounded,
            &math,
            &[],
            expected(vec![], 0, 1000),
            None,
        );

        // 2. 边界相等、预算恰好耗尽。
        // 不允许继续读取下一越界挂单。
        let after = assert_case(
            &[(1, maker_side, 100, 2), (2, maker_side, outside, 4)],
            &quote(100, side, 200, None),
            &bounded,
            &math,
            &[TradeId::new(701)],
            expected(vec![trade(701, 1, 100, 2, maker_side)], 200, 0),
            None,
        );

        let view = if maker_side == Side::Buy {
            &after.bids
        } else {
            &after.asks
        };

        assert_eq!(view.orders.len(), 1);

        assert_eq!(view.orders[0].original.order_id, OrderId::new(2));

        assert_eq!(view.depth[0].price, Price::try_new(outside).unwrap());

        assert_eq!(view.best_price, Some(Price::try_new(outside).unwrap()));

        // 3. 初始不足一手的预算。
        // 99 报价资产不能支付 100 * 1 手。
        assert_case(
            &[(1, maker_side, 100, 5)],
            &quote(100, side, 99, None),
            &bounded,
            &math,
            &[],
            expected(vec![], 0, 99),
            None,
        );

        // 4. 成交后不足一手的预算。
        // 更优价格档成交 1 手后只剩 5 报价资产。
        // 后续档位价格 100 在保护范围内，
        // 不能因为不足一手的预算产生触发。
        let budget = better as u128 + 5;

        let after = assert_case(
            &[(1, maker_side, better, 1), (2, maker_side, 100, 5)],
            &quote(100, side, budget, None),
            &bounded,
            &math,
            &[TradeId::new(702)],
            expected(
                vec![trade(702, 1, better, 1, maker_side)],
                better as u128,
                5,
            ),
            None,
        );

        let view = if maker_side == Side::Buy {
            &after.bids
        } else {
            &after.asks
        };

        assert_eq!(view.orders.len(), 1);

        assert_eq!(view.orders[0].original.order_id, OrderId::new(2));

        assert_eq!(view.orders[0].remaining, 5);
        assert_eq!(view.depth[0].total_qty, 5);
    }
}

// ============================================================
// 测试二：无边界时扫描全部多档和先进先出
// ============================================================

#[test]
fn t0638_no_boundary_sweeps_all_price_levels_and_fifo() {
    let math = math(10, 1);

    for (side, maker_side, better, outside, farther, spent) in [
        (Side::Buy, Side::Sell, 90, 110, 120, 1230),
        (Side::Sell, Side::Buy, 110, 90, 80, 1170),
    ] {
        // 标识符顺序刻意不等于价格优先/先进先出顺序。
        let makers = [
            (90, maker_side, farther, 2),
            (40, maker_side, 100, 4),
            (30, maker_side, better, 2),
            (10, maker_side, 100, 3),
            (50, maker_side, outside, 1),
        ];

        let ids = [
            TradeId::new(701),
            TradeId::new(702),
            TradeId::new(703),
            TradeId::new(704),
            TradeId::new(705),
        ];

        let after = assert_case(
            &makers,
            &quote(100, side, 2000, None),
            &ctx(100_003, None),
            &math,
            &ids,
            expected(
                vec![
                    trade(701, 30, better, 2, maker_side),
                    trade(702, 40, 100, 4, maker_side),
                    trade(703, 10, 100, 3, maker_side),
                    trade(704, 50, outside, 1, maker_side),
                    trade(705, 90, farther, 2, maker_side),
                ],
                spent,
                2000 - spent,
            ),
            None,
        );

        // 五张挂单均已完全成交。
        assert!(after.bids.orders.is_empty());
        assert!(after.asks.orders.is_empty());

        assert!(after.bids.depth.is_empty());
        assert!(after.asks.depth.is_empty());

        assert_eq!(after.bids.best_price, None);
        assert_eq!(after.asks.best_price, None);
    }
}

// ============================================================
// 测试三：部分成交保留槽位/优先级，全部成交删除订单和档位
// ============================================================

#[test]
fn t0638_partial_preserves_slot_fifo_priority_and_full_removes_levels() {
    let math = math(10, 1);

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let bounded = ctx(100_003, Some(100));

        // ----------------------------------------------------
        // 部分成交挂单
        // ----------------------------------------------------

        let partial = assert_case(
            &[(1, maker_side, 100, 10), (2, maker_side, 100, 5)],
            &quote(100, side, 350, None),
            &bounded,
            &math,
            &[TradeId::new(701)],
            expected(vec![trade(701, 1, 100, 3, maker_side)], 300, 50),
            Some(1),
        );

        let view = if maker_side == Side::Buy {
            &partial.bids
        } else {
            &partial.asks
        };

        assert_eq!(view.orders.len(), 2);

        // 先进先出第一张挂单部分成交。
        assert_eq!(view.orders[0].original.order_id, OrderId::new(1));

        assert_eq!(view.orders[0].remaining, 7);
        assert_eq!(view.orders[0].priority.get(), 0);

        // 先进先出第二张挂单不变。
        assert_eq!(view.orders[1].original.order_id, OrderId::new(2));

        assert_eq!(view.orders[1].remaining, 5);
        assert_eq!(view.orders[1].priority.get(), 1);

        // 档位聚合：7 + 5 = 12。
        assert_eq!(view.depth[0].order_count, 2);
        assert_eq!(view.depth[0].total_qty, 12);

        assert_eq!(view.best_price, Some(Price::try_new(100).unwrap()));

        // ----------------------------------------------------
        // 全部成交挂单
        // ----------------------------------------------------

        let full = assert_case(
            &[(1, maker_side, 100, 2)],
            &quote(100, side, 200, None),
            &bounded,
            &math,
            &[TradeId::new(702)],
            expected(vec![trade(702, 1, 100, 2, maker_side)], 200, 0),
            None,
        );

        assert!(full.bids.orders.is_empty());
        assert!(full.asks.orders.is_empty());

        assert!(full.bids.depth.is_empty());
        assert!(full.asks.depth.is_empty());

        assert_eq!(full.bids.best_price, None);
        assert_eq!(full.asks.best_price, None);
    }
}

// ============================================================
// 测试四：InvalidTick / ArithmeticOverflow / InvalidLot
// ============================================================

#[test]
fn t0638_invalid_tick_overflow_and_invalid_lot_are_atomic() {
    let fixed_math = math(10, 1);

    for (side, maker_side) in [(Side::Buy, Side::Sell), (Side::Sell, Side::Buy)] {
        let makers = [(1, maker_side, 100_000, 5)];

        // ----------------------------------------------------
        // InvalidTick：用户显式 worst_price 不满足价格步长
        // ----------------------------------------------------

        assert_case(
            &makers,
            &quote(
                100,
                side,
                1000,
                Some(MarketProtection {
                    max_slippage_bps: None,
                    worst_price: Some(Price::try_new(100_003).unwrap()),
                }),
            ),
            &ctx(100_003, Some(100_000)),
            &fixed_math,
            &[],
            Err(RejectReason::InvalidTick),
            None,
        );

        // ----------------------------------------------------
        // InvalidLot：Qty=11 合法，但 lot_size=10
        // ----------------------------------------------------

        assert_case(
            &[(1, maker_side, 100, 11)],
            &quote(100, side, 2000, None),
            &ctx(100_003, None),
            &math(1, 10),
            &[],
            Err(RejectReason::InvalidLot),
            None,
        );
    }

    // --------------------------------------------------------
    // ArithmeticOverflow：买入基点边界计算溢出
    // --------------------------------------------------------

    assert_case(
        &[(1, Side::Sell, 100_000, 5)],
        &quote(
            100,
            Side::Buy,
            1000,
            Some(MarketProtection {
                max_slippage_bps: Some(100),
                worst_price: None,
            }),
        ),
        &ctx(i64::MAX, Some(100_000)),
        &fixed_math,
        &[],
        Err(RejectReason::ArithmeticOverflow),
        None,
    );

    // 保留重复标识符优先于滑点基点校验、拒绝码为 3007 的既有测试。
}

// ============================================================
// 测试五：TradeId 不足，独立触发不变量断言 + 完整原子性
// ============================================================

#[test]
fn t0638_insufficient_trade_ids_panic_atomically_on_both_books() {
    let math = math(10, 1);

    for (side, maker_side, better) in [(Side::Buy, Side::Sell, 90), (Side::Sell, Side::Buy, 110)] {
        // 两个可成交挂单，需要两个 TradeId。
        let makers = [(1, maker_side, better, 2), (2, maker_side, 100, 3)];

        let (mut reference, mut production) = build(&makers);

        let incoming = quote(100, side, 2000, None);

        let context = ctx(100_003, Some(100));

        // 故意只提供一个。
        let ids = [TradeId::new(701)];

        // ----------------------------------------------------
        // 1. 执行前业务观察
        // ----------------------------------------------------

        let reference_before = observe_reference(&reference);
        let production_before = observe_production(&production);

        assert_eq!(reference_before, production_before);
        assert_eq!(validate_book(&production), Ok(()));

        // ----------------------------------------------------
        // 2. 生产簿完整物理快照
        // ----------------------------------------------------

        let bids_before = level_snapshot(&production.bids);
        let asks_before = level_snapshot(&production.asks);

        let orders_before = production.orders.clone();
        let arena_before = production.arena.test_snapshot();

        let next_before = production.next_queue_priority();

        let policy_before = *production.queue_priority_allocator.policy();

        // ----------------------------------------------------
        // 3. 参考簿独立 catch_unwind
        // ----------------------------------------------------

        let reference_panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = reference.execute_market_by_quote_with_protection(
                incoming.clone(),
                &context,
                &math,
                &ids,
            );
        }));

        // ----------------------------------------------------
        // 4. 生产簿独立 catch_unwind
        // ----------------------------------------------------

        let production_panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = production.execute_market_by_quote_with_protection(
                incoming.clone(),
                &context,
                &math,
                &ids,
            );
        }));

        assert!(
            reference_panic.is_err(),
            "Reference must panic before first mutation"
        );

        assert!(
            production_panic.is_err(),
            "Production must panic before first mutation"
        );

        // ----------------------------------------------------
        // 5. 参考簿业务状态不得改变
        // ----------------------------------------------------

        assert_eq!(observe_reference(&reference), reference_before);

        // ----------------------------------------------------
        // 6. 生产簿业务状态不得改变
        // ----------------------------------------------------

        assert_eq!(observe_production(&production), production_before);

        // ----------------------------------------------------
        // 7. 生产簿完整物理状态不得改变
        // ----------------------------------------------------

        assert_production_failure_state_unchanged(
            &production,
            &bids_before,
            &asks_before,
            &orders_before,
            &arena_before,
            next_before,
            policy_before,
        );

        assert_eq!(validate_book(&production), Ok(()));

        assert_regular_book_invariants(&observe_production(&production), next_before, 0);
    }
}
