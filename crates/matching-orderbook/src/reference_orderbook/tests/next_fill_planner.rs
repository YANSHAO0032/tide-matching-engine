//! 验证只读 `next`-成交规划的挂单、数量和状态保持性质。
use super::ReferenceOrderBook;
use crate::IncomingOrder;
use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};

/// 构造指定方向、价格和数量的订单测试数据。
fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 将指定订单包装为尚未成交的新到订单运行态。
fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
    IncomingOrder::new(order(id, side, price, qty))
}

/// 构造最高买价为 95、最低卖价为 105 的不交叉盘口。
fn two_sided_book() -> ReferenceOrderBook {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 10)).unwrap();
    book.rest(order(2, Side::Buy, 95, 10)).unwrap();

    book
}

/// 验证公开规划结果、挂单对象身份与成交数量。
fn assert_plan(
    book: &ReferenceOrderBook,
    incoming: &IncomingOrder,
    expected: Option<(usize, u64)>,
) {
    let actual = book.next_fill_plan(incoming).unwrap();

    match (actual, expected) {
        (None, None) => {
            assert!(
                book.best_crossing_maker(incoming.original_order())
                    .is_none()
            );
        }

        (Some((maker, qty)), Some((index, lots))) => {
            // 必须是源 Vec 中的同一个 ReferenceOrder。
            assert!(std::ptr::eq(maker, &book.orders[index]));

            // 必须与已有的挂单选择逻辑保持一致。
            let selected = book.best_crossing_maker(incoming.original_order()).unwrap();

            assert!(std::ptr::eq(maker, selected));

            // 计划成交量始终是合法的正 Qty。
            assert_eq!(qty.get(), lots);
            assert!(qty.get() > 0);
        }

        (actual, expected) => {
            panic!(
                "unexpected plan: actual={actual:?}, \
                     expected={expected:?}"
            );
        }
    }
}

/// 保存完整订单、运行态剩余量和先进先出优先级。
fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
    book.orders
        .iter()
        .map(|resting| {
            (
                resting.original_order().clone(),
                resting.remaining(),
                resting.priority(),
            )
        })
        .collect()
}

#[test]
fn buy_and_sell_exact_fill() {
    let book = two_sided_book();

    // 买入 @105 = 卖价 @105。
    assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 10)));

    // 卖出 @95 = 买价 @95。
    assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 10)));

    // 查询不会执行成交。
    assert_eq!(book.orders[0].remaining(), 10);
    assert_eq!(book.orders[1].remaining(), 10);
}

#[test]
fn buy_and_sell_cross_at_better_prices() {
    let book = two_sided_book();

    assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((0, 10)));

    assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((1, 10)));
}

#[test]
fn maker_partial_fill_on_both_sides() {
    let book = two_sided_book();

    // 新到订单比挂单小，挂单只会部分成交。
    assert_plan(&book, &incoming(100, Side::Buy, 105, 4), Some((0, 4)));

    assert_plan(&book, &incoming(101, Side::Sell, 95, 6), Some((1, 6)));

    // 这里只生成计划，没有实际扣减。
    assert_eq!(book.orders[0].remaining(), 10);
    assert_eq!(book.orders[1].remaining(), 10);
}

#[test]
fn taker_partial_fill_on_both_sides() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 3)).unwrap();

    book.rest(order(2, Side::Buy, 95, 4)).unwrap();

    // 新到订单比挂单大，只能成交挂单的剩余量。
    assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 3)));

    assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 4)));

    assert_eq!(book.orders[0].remaining(), 3);
    assert_eq!(book.orders[1].remaining(), 4);
}

#[test]
fn empty_book_returns_no_plan() {
    let book = ReferenceOrderBook::new();

    assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

    assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
}

#[test]
fn non_crossing_prices_return_no_plan() {
    let book = two_sided_book();

    // 买入 @104 < 卖价 @105。
    assert_plan(&book, &incoming(100, Side::Buy, 104, 10), None);

    // 卖出 @96 > 买价 @95。
    assert_plan(&book, &incoming(101, Side::Sell, 96, 10), None);
}

#[test]
fn empty_opposite_side_returns_no_plan() {
    let mut buy_only = ReferenceOrderBook::new();

    buy_only.rest(order(1, Side::Buy, 95, 10)).unwrap();

    // 新到订单也是买入，没有卖盘。
    assert_plan(&buy_only, &incoming(100, Side::Buy, 200, 10), None);

    let mut sell_only = ReferenceOrderBook::new();

    sell_only.rest(order(2, Side::Sell, 105, 10)).unwrap();

    // 新到订单也是卖出，没有买盘。
    assert_plan(&sell_only, &incoming(101, Side::Sell, 1, 10), None);
}

#[test]
fn best_price_takes_precedence_over_earlier_worse_price() {
    let mut book = ReferenceOrderBook::new();

    // 更优价格的挂单后挂入。
    for resting in [
        order(11, Side::Sell, 110, 10),
        order(21, Side::Buy, 90, 10),
        order(12, Side::Sell, 105, 10),
        order(22, Side::Buy, 95, 10),
    ] {
        book.rest(resting).unwrap();
    }

    assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((2, 10)));

    assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((3, 10)));
}

#[test]
fn same_price_preserves_fifo_on_both_sides() {
    let mut book = ReferenceOrderBook::new();

    // 同价标识符顺序故意与数值顺序不同。
    for resting in [
        order(30, Side::Sell, 105, 3),
        order(130, Side::Buy, 95, 3),
        order(10, Side::Sell, 105, 7),
        order(110, Side::Buy, 95, 8),
        order(20, Side::Sell, 105, 9),
        order(120, Side::Buy, 95, 10),
    ] {
        book.rest(resting).unwrap();
    }

    let buy = incoming(200, Side::Buy, 105, 10);
    let sell = incoming(201, Side::Sell, 95, 10);

    // 最早挂入的同价挂单获得优先级。
    assert_plan(&book, &buy, Some((0, 3)));
    assert_plan(&book, &sell, Some((1, 3)));

    assert_eq!(
        book.best_crossing_maker(buy.original_order())
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(30),
    );

    assert_eq!(
        book.best_crossing_maker(sell.original_order())
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(130),
    );
}

#[test]
fn exhausted_makers_are_skipped_and_fifo_is_preserved() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(30, Side::Sell, 105, 3),
        order(130, Side::Buy, 95, 3),
        order(10, Side::Sell, 105, 7),
        order(110, Side::Buy, 95, 8),
    ] {
        book.rest(resting).unwrap();
    }

    let buy = incoming(200, Side::Buy, 105, 10);
    let sell = incoming(201, Side::Sell, 95, 10);

    // 耗尽之前，先选择最早挂入的订单。
    assert_plan(&book, &buy, Some((0, 3)));
    assert_plan(&book, &sell, Some((1, 3)));

    // 模拟已有成交，耗尽最早的两张挂单。
    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    assert_eq!(book.orders[0].remaining(), 0);
    assert_eq!(book.orders[1].remaining(), 0);

    // 跳过耗尽订单，选择下一张同价挂单。
    assert_plan(&book, &buy, Some((2, 7)));
    assert_plan(&book, &sell, Some((3, 8)));
}

#[test]
fn all_exhausted_makers_produce_no_plan() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 3)).unwrap();

    book.rest(order(2, Side::Buy, 95, 4)).unwrap();

    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    book.orders[1].apply_fill(Qty::try_new(4).unwrap()).unwrap();

    assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

    assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
}

#[test]
fn maximum_quantity_is_preserved_in_plan() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, u64::MAX)).unwrap();

    assert_plan(
        &book,
        &incoming(100, Side::Buy, 105, u64::MAX),
        Some((0, u64::MAX)),
    );

    assert_eq!(book.orders[0].remaining(), u64::MAX);
}

#[test]
fn repeated_planning_does_not_mutate_book_or_incoming() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(30, Side::Sell, 105, 10),
        order(20, Side::Buy, 95, 8),
        order(10, Side::Sell, 105, 12),
    ] {
        book.rest(resting).unwrap();
    }

    // 预先制造部分成交的挂单。
    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    let buy = incoming(100, Side::Buy, 105, 9);
    let sell = incoming(101, Side::Sell, 95, 3);

    let book_before = snapshot(&book);
    let next_priority_before = book.next_priority;

    let buy_original = buy.original_order().clone();
    let sell_original = sell.original_order().clone();

    let buy_remaining = buy.remaining();
    let sell_remaining = sell.remaining();

    for _ in 0..10 {
        assert_plan(&book, &buy, Some((0, 7)));
        assert_plan(&book, &sell, Some((1, 3)));
    }

    // 订单簿的全部运行态保持不变。
    assert_eq!(snapshot(&book), book_before);
    assert_eq!(book.next_priority, next_priority_before);
    assert_eq!(book.orders.len(), 3);

    // 新到订单的原始订单和运行态也保持不变。
    assert_eq!(buy.original_order(), &buy_original);
    assert_eq!(sell.original_order(), &sell_original);

    assert_eq!(buy.remaining(), buy_remaining);
    assert_eq!(sell.remaining(), sell_remaining);

    // 原始订单数量不受规划影响。
    assert_eq!(book.orders[0].remaining(), 7);
    assert_eq!(book.orders[0].original_order().qty.get(), 10);
}
#[test]
fn partially_filled_buy_uses_current_remaining() {
    let mut book = two_sided_book();

    // 卖出挂单: 原始量=10, 剩余量=8。
    book.orders[0].apply_fill(Qty::try_new(2).unwrap()).unwrap();

    // 买入新到订单: 原始量=10, 剩余量=3。
    let mut buy = incoming(100, Side::Buy, 105, 10);
    buy.apply_fill(Qty::try_new(7).unwrap()).unwrap();

    let book_before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = buy.original_order().clone();
    let remaining_before = buy.remaining();

    // 取新到订单余量 3 与挂单余量 8 的较小值，成交量为 3。
    for _ in 0..3 {
        assert_plan(&book, &buy, Some((0, 3)));
    }

    assert_eq!(snapshot(&book), book_before);
    assert_eq!(book.next_priority, priority_before);
    assert_eq!(book.orders[0].remaining(), 8);
    assert_eq!(book.orders[0].priority(), book_before[0].2);

    assert_eq!(buy.original_order(), &original_before);
    assert_eq!(buy.original_order().qty.get(), 10);
    assert_eq!(buy.remaining(), remaining_before);
    assert_eq!(buy.remaining(), 3);
}

#[test]
fn partially_filled_sell_uses_current_remaining() {
    let mut book = two_sided_book();

    // 买入挂单: 原始量=10, 剩余量=4。
    book.orders[1].apply_fill(Qty::try_new(6).unwrap()).unwrap();

    // 卖出新到订单: 原始量=10, 剩余量=7。
    let mut sell = incoming(101, Side::Sell, 95, 10);
    sell.apply_fill(Qty::try_new(3).unwrap()).unwrap();

    let book_before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = sell.original_order().clone();
    let remaining_before = sell.remaining();

    // 取新到订单余量 7 与挂单余量 4 的较小值，成交量为 4。
    for _ in 0..3 {
        assert_plan(&book, &sell, Some((1, 4)));
    }

    assert_eq!(snapshot(&book), book_before);
    assert_eq!(book.next_priority, priority_before);
    assert_eq!(book.orders[1].remaining(), 4);
    assert_eq!(book.orders[1].priority(), book_before[1].2);

    assert_eq!(sell.original_order(), &original_before);
    assert_eq!(sell.original_order().qty.get(), 10);
    assert_eq!(sell.remaining(), remaining_before);
    assert_eq!(sell.remaining(), 7);
}

#[test]
fn exhausted_buy_returns_none_with_crossing_maker() {
    let book = two_sided_book();

    let mut buy = incoming(100, Side::Buy, 105, 10);
    buy.apply_fill(Qty::try_new(10).unwrap()).unwrap();

    let book_before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = buy.original_order().clone();
    let remaining_before = buy.remaining();

    assert_eq!(buy.remaining(), 0);

    // 确认存在价格交叉挂单，避免测试因无对手盘而通过。
    assert!(book.best_crossing_maker(buy.original_order()).is_some());

    for _ in 0..3 {
        assert!(matches!(book.next_fill_plan(&buy), Ok(None)));
    }

    assert_eq!(snapshot(&book), book_before);
    assert_eq!(book.next_priority, priority_before);
    assert_eq!(book.orders[0].remaining(), 10);
    assert_eq!(book.orders[0].priority(), book_before[0].2);

    assert_eq!(buy.original_order(), &original_before);
    assert_eq!(buy.original_order().qty.get(), 10);
    assert_eq!(buy.remaining(), remaining_before);
    assert_eq!(buy.remaining(), 0);
}

#[test]
fn exhausted_sell_returns_none_with_crossing_maker() {
    let book = two_sided_book();

    let mut sell = incoming(101, Side::Sell, 95, 10);
    sell.apply_fill(Qty::try_new(10).unwrap()).unwrap();

    let book_before = snapshot(&book);
    let priority_before = book.next_priority;
    let original_before = sell.original_order().clone();
    let remaining_before = sell.remaining();

    assert_eq!(sell.remaining(), 0);

    // 确认存在价格交叉挂单。
    assert!(book.best_crossing_maker(sell.original_order()).is_some());

    for _ in 0..3 {
        assert!(matches!(book.next_fill_plan(&sell), Ok(None)));
    }

    assert_eq!(snapshot(&book), book_before);
    assert_eq!(book.next_priority, priority_before);
    assert_eq!(book.orders[1].remaining(), 10);
    assert_eq!(book.orders[1].priority(), book_before[1].2);

    assert_eq!(sell.original_order(), &original_before);
    assert_eq!(sell.original_order().qty.get(), 10);
    assert_eq!(sell.remaining(), remaining_before);
    assert_eq!(sell.remaining(), 0);
}
