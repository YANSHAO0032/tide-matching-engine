//! 报价预算市价单的双向差分与预算守恒性质。
use super::*;

fn quote_market_command(id: u128, side: Side, budget: u128, ids: &[u128]) -> Command {
    Command::MarketByQuote(
        MarketByQuote {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            quote_budget: QuoteBudget::try_new(budget).unwrap(),
            market_protection: None,
        },
        ids.iter().copied().map(TradeId::new).collect(),
    )
}

fn fixed_market_by_quote_tape(taker_side: Side) -> Vec<Command> {
    let maker_side = opposite_side(taker_side);

    let (better, worse) = match taker_side {
        Side::Buy => (105i64, 110i64),
        Side::Sell => (95i64, 90i64),
    };

    let partial_budget = (3 * better + 1) as u128;
    let multi_budget = (9 * better + 2 * worse) as u128;

    vec![
        // 0. 空对手盘，不挂单，不消耗优先级。
        quote_market_command(90, taker_side, 1000, &[]),
        // 1. 建立第一张挂单。
        Command::Place(order(30, maker_side, better, 7)),
        // 2. 小于当前最优价格，直接不足一手的预算。
        quote_market_command(91, taker_side, (better - 1) as u128, &[]),
        // 3. 单挂单部分成交：成交 3，挂单方30 剩余 4。
        //    未用预算=1。
        quote_market_command(92, taker_side, partial_budget, &[9101]),
        // 4. 先挂较差档。
        Command::Place(order(20, maker_side, worse, 4)),
        // 5. 后挂同价挂单。
        Command::Place(order(10, maker_side, better, 5)),
        // 6. 价格优先 + 同价先进先出：
        // 挂单方30: 4
        // 挂单方10: 5
        // 挂单方20: 2
        // 挂单方20 留下 2。
        quote_market_command(93, taker_side, multi_budget, &[9102, 9103, 9104]),
        // 7. 耗尽最后 2 手，报价预算余量终结。
        quote_market_command(94, taker_side, 1000, &[9105]),
        // 8. 再次空簿。
        quote_market_command(95, taker_side, 100, &[]),
        // 9. 为重复标识符准备活动挂单。
        Command::Place(order(50, maker_side, worse, 2)),
        // 10. 重复标识符优先；没有 TradeId 也必须先返回业务拒绝。
        quote_market_command(50, taker_side, 1000, &[]),
    ]
}

#[test]
fn fixed_market_by_quote_buy_and_sell_tapes_are_differential_and_reproducible() {
    for side in [Side::Buy, Side::Sell] {
        let tape = fixed_market_by_quote_tape(side);

        // 每次 run_tape 都重新构造独立的两个空订单簿。
        let first = run_tape(&tape);
        let second = run_tape(&tape);

        // 比较完整执行记录，包括结果、整簿和优先级。
        assert_eq!(
            first, second,
            "MarketByQuote fixed tape not reproducible: {side:?}"
        );

        assert_eq!(first.len(), 11);

        // 所有成功的报价资产命令均不得挂单吃单方，
        // 也不得消耗优先级。
        for &index in &[0usize, 2, 3, 6, 7, 8] {
            let CommandOutcome::MarketByQuote(Ok(_)) = &first[index].outcome else {
                panic!(
                    "MarketByQuote expected success: \
                     step={index}, side={side:?}"
                );
            };

            let taker_id = match &tape[index] {
                Command::MarketByQuote(order, _) => order.order_id,
                _ => unreachable!(),
            };

            assert!(
                first[index]
                    .book
                    .bids
                    .orders
                    .iter()
                    .chain(&first[index].book.asks.orders)
                    .all(|entry| entry.original.order_id != taker_id),
            );

            let priority_before = if index == 0 {
                0
            } else {
                first[index - 1].production_next_priority
            };

            assert_eq!(first[index].production_next_priority, priority_before,);
        }

        let (better, worse) = match side {
            Side::Buy => (105i64, 110i64),
            Side::Sell => (95i64, 90i64),
        };

        // 空簿 + 初始预算不足一手。
        for (index, budget) in [(0usize, 1000u128), (2usize, (better - 1) as u128)] {
            let CommandOutcome::MarketByQuote(Ok(execution)) = &first[index].outcome else {
                panic!("expected empty/dust quote result");
            };

            assert!(execution.trades().is_empty());
            assert_eq!(execution.spent_quote().get(), 0);
            assert_eq!(execution.unused_quote().get(), budget);
        }

        // 部分成交挂单。
        let CommandOutcome::MarketByQuote(Ok(partial)) = &first[3].outcome else {
            panic!("MarketByQuote partial expected");
        };

        assert_eq!(partial.trades().len(), 1);
        assert_eq!(partial.trades()[0].maker_order_id, OrderId::new(30));
        assert_eq!(partial.trades()[0].trade_id, TradeId::new(9101));
        assert_eq!(partial.trades()[0].qty.get(), 3);
        assert_eq!(partial.trades()[0].price.get(), better);
        assert_eq!(partial.spent_quote().get(), (3 * better) as u128);
        assert_eq!(partial.unused_quote().get(), 1);

        let remaining_side = match side {
            Side::Buy => &first[3].book.asks,
            Side::Sell => &first[3].book.bids,
        };

        assert_eq!(remaining_side.orders.len(), 1);
        assert_eq!(remaining_side.orders[0].remaining, 4);

        // 同价先进先出 + 多档成交。
        let CommandOutcome::MarketByQuote(Ok(multi)) = &first[6].outcome else {
            panic!("MarketByQuote multi-level expected");
        };

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.maker_order_id.get())
                .collect::<Vec<_>>(),
            vec![30, 10, 20],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.qty.get())
                .collect::<Vec<_>>(),
            vec![4, 5, 2],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.price.get())
                .collect::<Vec<_>>(),
            vec![better, better, worse],
        );

        assert_eq!(
            multi
                .trades()
                .iter()
                .map(|trade| trade.trade_id.get())
                .collect::<Vec<_>>(),
            vec![9102, 9103, 9104],
        );

        assert!(multi.trades().iter().all(|trade| {
            trade.taker_order_id == OrderId::new(93) && trade.maker_side == opposite_side(side)
        }));

        let expected_multi_spent = (9 * better + 2 * worse) as u128;

        assert_eq!(multi.spent_quote().get(), expected_multi_spent);
        assert_eq!(multi.unused_quote().get(), 0);

        let remaining_side = match side {
            Side::Buy => &first[6].book.asks,
            Side::Sell => &first[6].book.bids,
        };

        assert_eq!(remaining_side.orders.len(), 1);
        assert_eq!(remaining_side.orders[0].original.order_id, OrderId::new(20),);
        assert_eq!(remaining_side.orders[0].remaining, 2);

        // 最后两个手全部成交，额外预算终结。
        let CommandOutcome::MarketByQuote(Ok(exhaust)) = &first[7].outcome else {
            panic!("MarketByQuote liquidity exhaustion expected");
        };

        assert_eq!(exhaust.trades().len(), 1);
        assert_eq!(exhaust.trades()[0].maker_order_id, OrderId::new(20));
        assert_eq!(exhaust.trades()[0].qty.get(), 2);
        assert_eq!(exhaust.trades()[0].trade_id, TradeId::new(9105));
        assert_eq!(exhaust.spent_quote().get(), (2 * worse) as u128);
        assert_eq!(exhaust.unused_quote().get(), 1000 - (2 * worse) as u128,);

        assert!(first[7].book.bids.orders.is_empty());
        assert!(first[7].book.asks.orders.is_empty());

        // 流动性耗尽后再次发单：空成交。
        let CommandOutcome::MarketByQuote(Ok(empty)) = &first[8].outcome else {
            panic!("MarketByQuote empty after exhaustion expected");
        };

        assert!(empty.trades().is_empty());
        assert_eq!(empty.spent_quote().get(), 0);
        assert_eq!(empty.unused_quote().get(), 100);

        // 重复标识符拒绝且不修改语义簿和优先级。
        assert_eq!(
            first[10].outcome,
            CommandOutcome::MarketByQuote(Err(RejectReason::DuplicateOrderId)),
        );

        assert_eq!(first[10].book, first[9].book);
        assert_eq!(
            first[10].production_next_priority,
            first[9].production_next_priority,
        );
    }
}

// MarketByQuote 跨价格步长/数量步长/价格/数量/预算的预算守恒差分。
//
// 仅生成成功路径：
// - 合法且对齐的挂单价格/数量；
// - 正 QuoteBudget；
// - 无重复 OrderId；
// - 三个预分配 TradeId；
// - 参考簿 / 生产簿独立构造与执行。
//
// 不依赖固定命令序列的逐笔预期表。
proptest! {
    #[test]
    fn market_by_quote_budget_conservation_property(
        is_buy in any::<bool>(),
        tick_size in 1i64..=5,
        lot_size in 1u64..=10,
        price_unit in 2i64..=100,
        maker30_lots in 2u64..=10,
        maker10_lots in 1u64..=10,
        maker20_lots in 1u64..=10,
        budget_case in 0u8..6,
        random_budget in 1u128..=100_000,
    ) {
        // 1. 构造对齐的价格和数量。
        let taker_side = if is_buy {
            Side::Buy
        } else {
            Side::Sell
        };
        let maker_side = opposite_side(taker_side);

        let (better_units, worse_units) = match taker_side {
            Side::Buy => (price_unit, price_unit + 1),
            Side::Sell => (price_unit + 1, price_unit),
        };

        let better_price = better_units
            .checked_mul(tick_size)
            .expect("better price overflow");
        let worse_price = worse_units
            .checked_mul(tick_size)
            .expect("worse price overflow");

        let qty30 = maker30_lots
            .checked_mul(lot_size)
            .expect("maker30 qty overflow");
        let qty10 = maker10_lots
            .checked_mul(lot_size)
            .expect("maker10 qty overflow");
        let qty20 = maker20_lots
            .checked_mul(lot_size)
            .expect("maker20 qty overflow");

        // 2. 用对齐的价格和手推导预算边界。
        // 本性质测试固定零小数位数，使用整数边界；
        // 不复制 MarketMath 的通用预算反推数量实现。
        let best_one_lot = (better_price as u128)
            .checked_mul(u128::from(lot_size))
            .expect("best one-lot notional overflow");

        let worse_one_lot = (worse_price as u128)
            .checked_mul(u128::from(lot_size))
            .expect("worse one-lot notional overflow");

        let better_total_lots = maker30_lots
            .checked_add(maker10_lots)
            .expect("better total lots overflow");

        let better_total = best_one_lot
            .checked_mul(u128::from(better_total_lots))
            .expect("better total notional overflow");

        let worse_total = worse_one_lot
            .checked_mul(u128::from(maker20_lots))
            .expect("worse total notional overflow");

        let total_notional = better_total
            .checked_add(worse_total)
            .expect("total notional overflow");

        let budget = match budget_case {
            0 => best_one_lot - 1,
            1 => best_one_lot,
            2 => best_one_lot
                .checked_mul(u128::from(maker30_lots - 1))
                .and_then(|value| value.checked_add(1))
                .expect("partial budget overflow"),
            3 => better_total
                .checked_add(worse_one_lot)
                .expect("multi-level budget overflow"),
            4 => total_notional
                .checked_add(1)
                .expect("exhaustion budget overflow"),
            _ => random_budget,
        };

        prop_assert!(budget > 0);

        // 3. 独立创建两套订单簿。
        let mut reference = ReferenceOrderBook::new();
        let mut production = ProductionOrderBook::new(policy());

        // 挂单顺序：
        // 挂单方20：较差档
        // 挂单方30：较优档先进先出头节点
        // 挂单方10：较优档先进先出尾节点
        let makers = [
            order(20, maker_side, worse_price, qty20),
            order(30, maker_side, better_price, qty30),
            order(10, maker_side, better_price, qty10),
        ];

        for maker in makers {
            reference
                .rest(maker.clone())
                .expect("valid Reference maker rest");

            production
                .rest(maker)
                .expect("valid Production maker rest");
        }

        // 4. 构造相同的 MarketMath 和 MarketByQuote。
        let math = MarketMath::try_new(
            0,
            0,
            0,
            tick_size,
            lot_size,
        )
        .expect("generated MarketMath must be valid");

        let quote_budget = QuoteBudget::try_new(budget)
            .expect("generated budget must be positive");

        let taker = MarketByQuote {
            order_id: OrderId::new(100),
            user_id: UserId::new(100),
            side: taker_side,
            quote_budget,
            market_protection:None
        };

        // 每个挂单最多产生一笔成交。
        let trade_ids = [
            TradeId::new(9001),
            TradeId::new(9002),
            TradeId::new(9003),
        ];

        // 5. 执行前检查两套簿一致，并记录优先级。
        prop_assert_eq!(
            observe_reference(&reference),
            observe_production(&production),
        );

        let priority_before = production.next_queue_priority();

        // 6. 两套公开入口独立执行。
        let reference_execution = reference
            .execute_market_by_quote(
                taker.clone(),
                &math,
                &trade_ids,
            )
            .expect("valid Reference MarketByQuote must succeed");

        let production_execution = production
            .execute_market_by_quote(
                taker.clone(),
                &math,
                &trade_ids,
            )
            .expect("valid Production MarketByQuote must succeed");

        // 7. 完整执行对比。
        // 包含原序成交、spent_quote、unused_quote。
        prop_assert_eq!(
            &reference_execution,
            &production_execution,
        );

        // 8. 预算守恒。
        let spent = production_execution.spent_quote().get();
        let unused = production_execution.unused_quote().get();

        prop_assert!(spent <= budget);

        let recovered_budget = spent
            .checked_add(unused)
            .expect("spent + unused must not overflow");

        prop_assert_eq!(recovered_budget, budget);

        // 9. 成交必须严格正量。
        for trade in production_execution.trades() {
            prop_assert!(trade.qty.get() > 0);
        }

        // 10. 无成交时预算全部返还。
        if production_execution.trades().is_empty() {
            prop_assert_eq!(spent, 0);
            prop_assert_eq!(unused, budget);
        }

        // 11. 参考簿 / 生产簿整簿有序观察一致。
        // 同时比较订单集合、剩余量、先进先出、深度、best_price。
        prop_assert_eq!(
            observe_reference(&reference),
            observe_production(&production),
        );

        // 12. 市价吃单方不得挂簿。
        prop_assert!(
            !reference_contains_order(&reference, taker.order_id)
        );

        prop_assert!(
            production.order(taker.order_id).is_none()
        );

        // 13. 报价资产市价成交不得分配 QueuePriority。
        prop_assert_eq!(
            production.next_queue_priority(),
            priority_before,
        );

        // 14. 生产簿内部结构完整性。
        prop_assert_eq!(validate_book(&production), Ok(()));
    }
}
