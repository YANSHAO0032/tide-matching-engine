//! 用生成式输入覆盖先进先出、数量守恒、取消和失败原子性。
use super::*;
use matching_domain::UserId;
use proptest::prelude::*;

/// 构造生成式测试使用的合法订单测试数据。
fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 保存源 `Vec` 顺序、原始订单、剩余量和优先级。
fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
    book.orders
        .iter()
        .map(|maker| {
            (
                maker.original_order().clone(),
                maker.remaining(),
                maker.priority(),
            )
        })
        .collect()
}

/// 将指定订单包装为尚未成交的新到订单运行态。
fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
    IncomingOrder::new(order(id, side, price, qty))
}

proptest! {
    // 性质一：成交守恒、正成交量、同价先进先出前缀。
    #[test]
    fn prop_same_price_fifo_and_quantity_conservation(
        maker_qtys in prop::collection::vec(1u64..=32, 1..=8),
        taker_qty in 1u64..=256,
        maker_is_sell in any::<bool>(),
    ) {
        let maker_side = if maker_is_sell {
            Side::Sell
        } else {
            Side::Buy
        };
        let taker_side = if maker_is_sell {
            Side::Buy
        } else {
            Side::Sell
        };
        let maker_price = if maker_is_sell { 105 } else { 95 };
        let taker_price = if maker_is_sell { 110 } else { 90 };

        let mut book = ReferenceOrderBook::new();

        for (i, &qty) in maker_qtys.iter().enumerate() {
            book.rest(order(
                i as u128 + 1,
                maker_side,
                maker_price,
                qty,
            )).unwrap();
        }

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let total_maker: u64 = maker_qtys.iter().sum();

        let mut taker = incoming(
            100,
            taker_side,
            taker_price,
            taker_qty,
        );

        let fills = book.execute_fills(&mut taker).unwrap();

        // 期望成交量由生成的数据直接计算，
        // 不调用计划生成器或其他撮合算法。
        let expected_total = taker_qty.min(total_maker);
        let actual_total: u64 =
            fills.iter().map(|(_, _, qty)| qty.get()).sum();

        prop_assert_eq!(actual_total, expected_total);
        prop_assert_eq!(
            actual_total + taker.remaining(),
            taker_qty
        );
        prop_assert_eq!(
            taker.original_order().qty.get(),
            taker_qty
        );

        // 手工构建先进先出前缀及每张挂单的期望成交量。
        let mut unfilled = taker_qty;
        let mut expected_fills = Vec::new();
        let mut expected_book = Vec::new();

        for (i, &(ref original, remaining, priority))
            in before.iter().enumerate()
        {
            let filled = remaining.min(unfilled);

            if filled > 0 {
                expected_fills.push((
                    original.order_id,
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(filled).unwrap(),
                ));
                unfilled -= filled;
            }

            let maker_remaining = remaining - filled;
            if maker_remaining > 0 {
                expected_book.push((
                    original.clone(),
                    maker_remaining,
                    priority,
                ));
            }

            prop_assert_eq!(
                original.order_id,
                OrderId::new(i as u128 + 1)
            );
        }

        // 同时验证先进先出前缀、每笔数量及挂单-价格。
        prop_assert_eq!(fills, expected_fills);

        // 完全成交的挂单被移除；
        // 未耗尽的挂单保持来源顺序及优先级。
        prop_assert_eq!(snapshot(&book), expected_book);
        prop_assert_eq!(book.next_priority, priority_before);
    }

    // 性质二：任意有效位置取消后的完整状态。
    #[test]
    fn prop_cancel_valid_position_preserves_remaining_book(
        quantities in prop::collection::vec(1u64..=100, 1..=8),
        index_seed in any::<usize>(),
        is_sell in any::<bool>(),
    ) {
        let side = if is_sell {
            Side::Sell
        } else {
            Side::Buy
        };

        let mut book = ReferenceOrderBook::new();

        for (i, &qty) in quantities.iter().enumerate() {
            book.rest(order(
                i as u128 + 1,
                side,
                100,
                qty,
            )).unwrap();

            // 预先制造不同的挂单剩余量，
            // 同时保留 Qty 严格正值约束。
            let filled = qty / 2;
            if filled > 0 {
                book.orders[i]
                    .apply_fill(Qty::try_new(filled).unwrap())
                    .unwrap();
            }
        }

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let target_index = index_seed % quantities.len();
        let target_id = before[target_index].0.order_id;

        // 期望状态仅通过过滤取消前的快照生成。
        let expected_remaining: Vec<_> = before
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != target_index)
            .map(|(_, state)| state.clone())
            .collect();

        let expected_canceled = before[target_index].clone();

        let canceled = book.cancel(target_id).unwrap();

        prop_assert_eq!(
            canceled.original_order(),
            &expected_canceled.0
        );
        prop_assert_eq!(
            canceled.remaining(),
            expected_canceled.1
        );
        prop_assert_eq!(
            canceled.priority(),
            expected_canceled.2
        );

        // 完整快照同时验证来源/先进先出顺序、
        // 原始订单、剩余量和 QueuePriority。
        prop_assert_eq!(snapshot(&book), expected_remaining);
            let target_removed = book
        .orders
        .iter()
        .all(|resting| resting.original_order().order_id != target_id);

        prop_assert!(
            target_removed,
            "canceled order must not remain in the book"
        );

        prop_assert_eq!(
            book.orders.len(),
            quantities.len() - 1
        );
        prop_assert_eq!(
            book.next_priority,
            priority_before
        );
    }
}
