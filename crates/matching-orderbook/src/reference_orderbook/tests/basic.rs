//! 覆盖参考模型的排序、成交、取消和优先级边界。
use super::*;
use matching_domain::{LimitOrder, OrderId, Price, Qty, UserId};

/// 构造带指定业务字段的挂单订单测试数据。
fn order(id: u128, side: Side, price: i64, qty: u64) -> ReferenceOrder {
    ReferenceOrder::new(
        LimitOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        },
        QueuePriority::new(0),
    )
}

/// 直接构造非空参考簿，仅用于表达需要特定源顺序的测试前置条件。
fn book(orders: Vec<ReferenceOrder>) -> ReferenceOrderBook {
    ReferenceOrderBook {
        orders,
        reservations: Default::default(),
        next_priority: 0,
        stop_orders: Vec::new(),
        next_stop_priority: 0,
        reference_prices: Default::default(),
    }
}

/// 提取只读视图中的订单标识符，便于断言显示顺序。
fn ids(orders: &[&ReferenceOrder]) -> Vec<OrderId> {
    orders
        .iter()
        .map(|order| order.original_order().order_id)
        .collect()
}

#[test]
fn empty_book_and_empty_side() {
    let empty = ReferenceOrderBook::new();

    assert!(empty.orders(Side::Buy).is_empty());
    assert!(empty.orders(Side::Sell).is_empty());
    assert!(empty.best(Side::Buy).is_none());
    assert!(empty.best(Side::Sell).is_none());

    let buy_only = book(vec![order(1, Side::Buy, 90, 10)]);
    assert!(buy_only.orders(Side::Sell).is_empty());
    assert!(buy_only.best(Side::Sell).is_none());

    let sell_only = book(vec![order(2, Side::Sell, 110, 10)]);
    assert!(sell_only.orders(Side::Buy).is_empty());
    assert!(sell_only.best(Side::Buy).is_none());
}

#[test]
fn buy_and_sell_views_are_isolated_and_price_sorted() {
    // 最高买价=95，最低卖价=101，不构成交叉盘口。
    let book = book(vec![
        order(1, Side::Buy, 90, 10),
        order(2, Side::Sell, 105, 10),
        order(3, Side::Buy, 95, 10),
        order(4, Side::Sell, 101, 10),
        order(5, Side::Buy, 92, 10),
        order(6, Side::Sell, 103, 10),
    ]);

    assert_eq!(
        ids(&book.orders(Side::Buy)),
        vec![OrderId::new(3), OrderId::new(5), OrderId::new(1)]
    );
    assert_eq!(
        ids(&book.orders(Side::Sell)),
        vec![OrderId::new(4), OrderId::new(6), OrderId::new(2)]
    );
}

#[test]
fn buy_same_price_preserves_fifo_not_id_order() {
    let book = book(vec![
        order(30, Side::Buy, 90, 10),
        order(10, Side::Buy, 90, 10),
        order(20, Side::Buy, 90, 10),
    ]);

    assert_eq!(
        ids(&book.orders(Side::Buy)),
        vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
    );
}

#[test]
fn sell_same_price_preserves_fifo_not_id_order() {
    let book = book(vec![
        order(30, Side::Sell, 110, 10),
        order(10, Side::Sell, 110, 10),
        order(20, Side::Sell, 110, 10),
    ]);

    assert_eq!(
        ids(&book.orders(Side::Sell)),
        vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
    );
}

#[test]
fn later_better_price_precedes_earlier_worse_price() {
    let book = book(vec![
        order(1, Side::Buy, 90, 10),
        order(2, Side::Sell, 110, 10),
        order(3, Side::Buy, 95, 10),
        order(4, Side::Sell, 105, 10),
    ]);

    assert_eq!(
        ids(&book.orders(Side::Buy)),
        vec![OrderId::new(3), OrderId::new(1)]
    );
    assert_eq!(
        ids(&book.orders(Side::Sell)),
        vec![OrderId::new(4), OrderId::new(2)]
    );
}

#[test]
fn price_boundaries_sort_correctly() {
    // 分别测试两个单侧簿，避免构造交叉盘口。
    let buys = book(vec![
        order(1, Side::Buy, 1, 10),
        order(2, Side::Buy, i64::MAX, 10),
    ]);
    assert_eq!(
        ids(&buys.orders(Side::Buy)),
        vec![OrderId::new(2), OrderId::new(1)]
    );

    let sells = book(vec![
        order(3, Side::Sell, i64::MAX, 10),
        order(4, Side::Sell, 1, 10),
    ]);
    assert_eq!(
        ids(&sells.orders(Side::Sell)),
        vec![OrderId::new(4), OrderId::new(3)]
    );
}

#[test]
fn repeated_queries_do_not_mutate_source_or_remaining() {
    let mut first = order(30, Side::Buy, 90, 10);
    first.apply_fill(Qty::try_new(3).unwrap()).unwrap();

    let book = book(vec![
        first,
        order(10, Side::Sell, 110, 20),
        order(20, Side::Buy, 95, 30),
        order(40, Side::Sell, 105, 40),
    ]);

    // 记录源 Vec 的顺序、完整原始订单及运行态剩余量。
    let snapshot: Vec<_> = book
        .orders
        .iter()
        .map(|order| (order.original_order().clone(), order.remaining()))
        .collect();

    let first_buy = ids(&book.orders(Side::Buy));
    let first_sell = ids(&book.orders(Side::Sell));

    for _ in 0..3 {
        assert_eq!(ids(&book.orders(Side::Buy)), first_buy);
        assert_eq!(ids(&book.orders(Side::Sell)), first_sell);

        assert!(book.best(Side::Buy).is_some());
        assert!(book.best(Side::Sell).is_some());
    }

    let after: Vec<_> = book
        .orders
        .iter()
        .map(|order| (order.original_order().clone(), order.remaining()))
        .collect();

    assert_eq!(snapshot, after);
    assert_eq!(book.orders[0].remaining(), 7);
    assert_eq!(book.orders[0].original_order().qty.get(), 10);
}

/// 构造经公开 `rest` 路径写入参考簿的合法订单测试数据。
fn candidate(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 保存完整可观察挂单状态，用于失败原子性断言。
fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitOrder, u64, QueuePriority)> {
    book.orders
        .iter()
        .map(|order| {
            (
                order.original_order().clone(),
                order.remaining(),
                order.priority(),
            )
        })
        .collect()
}

#[test]
fn rest_assigns_monotonically_increasing_priorities() {
    let mut book = ReferenceOrderBook::new();

    for id in [30, 10, 20] {
        book.rest(candidate(id, Side::Buy, 90, 10)).unwrap();
    }

    let view = book.orders(Side::Buy);

    let actual: Vec<_> = view
        .iter()
        .map(|order| (order.original_order().order_id, order.priority().get()))
        .collect();

    assert_eq!(
        actual,
        vec![
            (OrderId::new(30), 0),
            (OrderId::new(10), 1),
            (OrderId::new(20), 2),
        ]
    );

    assert_eq!(book.next_priority, 3);
}

#[test]
fn real_rest_preserves_price_priority_and_both_sides_fifo() {
    let mut book = ReferenceOrderBook::new();

    // 双侧盘口不交叉：最高买价=95，最低卖价=105。
    for order in [
        candidate(30, Side::Buy, 90, 10),
        candidate(40, Side::Sell, 110, 10),
        candidate(10, Side::Buy, 90, 10),
        candidate(50, Side::Sell, 110, 10),
        candidate(20, Side::Buy, 90, 10),
        candidate(60, Side::Sell, 110, 10),
        // 后到的更优价格必须排在前面。
        candidate(70, Side::Buy, 95, 10),
        candidate(80, Side::Sell, 105, 10),
    ] {
        book.rest(order).unwrap();
    }

    assert_eq!(
        ids(&book.orders(Side::Buy)),
        vec![
            OrderId::new(70),
            OrderId::new(30),
            OrderId::new(10),
            OrderId::new(20),
        ]
    );

    assert_eq!(
        ids(&book.orders(Side::Sell)),
        vec![
            OrderId::new(80),
            OrderId::new(40),
            OrderId::new(50),
            OrderId::new(60),
        ]
    );

    assert_eq!(book.best(Side::Buy).unwrap().priority().get(), 6);
    assert_eq!(book.best(Side::Sell).unwrap().priority().get(), 7);
}

#[test]
fn duplicate_order_id_does_not_mutate_book() {
    let mut book = ReferenceOrderBook::new();

    let original = candidate(1, Side::Buy, 90, 10);
    book.rest(original.clone()).unwrap();

    let before = snapshot(&book);
    let next_before = book.next_priority;

    // 同一订单标识符，即使方向、价格、数量不同也必须拒绝。
    let result = book.rest(candidate(1, Side::Sell, 110, 20));

    assert_eq!(result, Err(RejectReason::DuplicateOrderId));
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, next_before);
    assert_eq!(book.orders[0].original_order(), &original);
    assert_eq!(book.orders[0].remaining(), 10);

    // 失败不消耗优先级，下一张合法订单获得紧接着的值。
    book.rest(candidate(2, Side::Buy, 90, 10)).unwrap();

    assert_eq!(book.orders[1].priority().get(), 1);
    assert_eq!(book.next_priority, 2);
}

#[test]
fn priority_guard_allows_last_safe_rest_then_rejects() {
    let threshold = u128::MAX - PRIORITY_GUARD;

    // 只在测试模块中初始化高水位计数器。
    let mut book = ReferenceOrderBook {
        orders: Vec::new(),
        reservations: Default::default(),
        next_priority: threshold - 1,
        stop_orders: Vec::new(),
        next_stop_priority: 0,
        reference_prices: Default::default(),
    };

    // 临界点前的最后一次成功挂单。
    book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

    assert_eq!(book.orders[0].priority().get(), threshold - 1);
    assert_eq!(book.next_priority, threshold);

    let before = snapshot(&book);

    // 达到阈值后，不能再消耗优先级。
    assert_eq!(
        book.rest(candidate(2, Side::Buy, 90, 10)),
        Err(RejectReason::PrioritySpaceExhaustion)
    );

    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, threshold);
}

#[test]
fn duplicate_id_takes_precedence_over_priority_exhaustion() {
    let threshold = u128::MAX - PRIORITY_GUARD;

    let mut book = ReferenceOrderBook {
        orders: Vec::new(),
        reservations: Default::default(),
        next_priority: threshold - 1,
        stop_orders: Vec::new(),
        next_stop_priority: 0,
        reference_prices: Default::default(),
    };

    book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

    let before = snapshot(&book);

    // 同时满足重复标识符和高水位耗尽时，必须先返回 2008。
    assert_eq!(
        book.rest(candidate(1, Side::Buy, 90, 10)),
        Err(RejectReason::DuplicateOrderId)
    );

    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, threshold);
}

#[test]
fn failed_rest_preserves_existing_partial_fill() {
    let mut book = ReferenceOrderBook::new();

    book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    let before = snapshot(&book);
    let next_before = book.next_priority;

    assert_eq!(
        book.rest(candidate(1, Side::Buy, 90, 10)),
        Err(RejectReason::DuplicateOrderId)
    );

    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, next_before);
    assert_eq!(book.orders[0].remaining(), 7);
    assert_eq!(book.orders[0].original_order().qty.get(), 10);
}
#[test]
fn cancel_existing_order_preserves_other_orders() {
    let mut book = ReferenceOrderBook::new();

    for id in [30, 10, 20] {
        book.rest(candidate(id, Side::Buy, 95, 10)).unwrap();
    }

    // 预先部分成交待撤订单。
    book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    let before = snapshot(&book);
    let priority_before = book.next_priority;

    let canceled = book.cancel(OrderId::new(10)).unwrap();

    assert_eq!(canceled.original_order().order_id, OrderId::new(10));
    assert_eq!(canceled.remaining(), 7);
    assert_eq!(canceled.priority(), before[1].2);

    // 其余订单保持原有先进先出顺序及运行态。
    assert_eq!(snapshot(&book), vec![before[0].clone(), before[2].clone()]);
    assert_eq!(book.next_priority, priority_before);

    // 撤销后允许相同 OrderId 再次挂入。
    book.rest(candidate(10, Side::Buy, 95, 10)).unwrap();

    assert_eq!(book.orders[2].priority().get(), priority_before);
    assert_eq!(book.next_priority, priority_before + 1);
}

#[test]
fn cancel_missing_order_is_failure_atomic() {
    let mut book = ReferenceOrderBook::new();

    book.rest(candidate(1, Side::Sell, 105, 10)).unwrap();
    book.rest(candidate(2, Side::Sell, 105, 10)).unwrap();

    // 包含部分成交挂单，防止只验证原始订单。
    book.orders[0].apply_fill(Qty::try_new(4).unwrap()).unwrap();

    let before = snapshot(&book);
    let priority_before = book.next_priority;

    assert!(matches!(
        book.cancel(OrderId::new(999)),
        Err(RejectReason::OrderNotFound)
    ));

    // 完整原始订单、剩余量、优先级、源顺序不变。
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, priority_before);

    // 失败后仍能按原有先进先出选择挂单。
    let best = book.best(Side::Sell).unwrap();
    assert_eq!(best.original_order().order_id, OrderId::new(1));
    assert_eq!(best.remaining(), 6);
}

#[test]
fn cancel_empty_book_is_failure_atomic() {
    let mut book = ReferenceOrderBook::new();

    let before = snapshot(&book);
    let priority_before = book.next_priority;

    assert!(matches!(
        book.cancel(OrderId::new(1)),
        Err(RejectReason::OrderNotFound)
    ));

    assert_eq!(snapshot(&book), before);
    assert!(book.orders.is_empty());
    assert_eq!(book.next_priority, priority_before);
}
#[test]
fn cancel_preserves_fifo_and_remaining_on_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        // 分别取消同价先进先出队列的首项、中间项和尾项。
        for cancel_index in 0..3 {
            let mut book = ReferenceOrderBook::new();

            // 标识符顺序故意不同于挂单顺序。
            for id in [30, 10, 20] {
                book.rest(candidate(id, side, 100, 10)).unwrap();
            }

            // 制造不同的运行态剩余量：
            // 标识符=30 -> 9
            // 标识符=10 -> 7
            // 标识符=20 -> 5
            for (index, filled) in [1, 3, 5].into_iter().enumerate() {
                book.orders[index]
                    .apply_fill(Qty::try_new(filled).unwrap())
                    .unwrap();
            }

            let before = snapshot(&book);
            let priority_before = book.next_priority;

            let canceled_id = book.orders[cancel_index].original_order().order_id;

            let expected_canceled = before[cancel_index].clone();

            let expected_remaining: Vec<_> = before
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != cancel_index)
                .map(|(_, state)| state.clone())
                .collect();

            let expected_fifo: Vec<_> = expected_remaining
                .iter()
                .map(|(original, _, _)| original.order_id)
                .collect();

            // 执行取消。
            let canceled = book.cancel(canceled_id).unwrap();

            // 返回值必须保留完整原始订单、剩余量和优先级。
            assert_eq!(canceled.original_order(), &expected_canceled.0);
            assert_eq!(canceled.remaining(), expected_canceled.1);
            assert_eq!(canceled.priority(), expected_canceled.2);

            // 原始委托数量不能被之前的部分成交修改。
            assert_eq!(canceled.original_order().qty.get(), 10);

            // 来源 Vec 的相对顺序、完整原始订单、
            // 剩余量和 QueuePriority 均保持不变。
            assert_eq!(snapshot(&book), expected_remaining);

            // 同价订单仍保持原来的先进先出相对顺序。
            assert_eq!(ids(&book.orders(side)), expected_fifo);

            // 取消不分配新的 QueuePriority。
            assert_eq!(book.next_priority, priority_before);

            // 只移除目标订单。
            assert_eq!(book.orders.len(), 2);
            assert!(
                book.orders
                    .iter()
                    .all(|order| { order.original_order().order_id != canceled_id })
            );
        }
    }
}
