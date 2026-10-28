//! 验证挂单查询的价格交叉、先进先出与只读性质。
use super::*;
use matching_domain::{OrderId, Price, Qty, UserId};

/// 构造指定方向、价格和数量的订单测试数据。
fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 将挂单查询结果投影为订单身份，便于断言选择结果。
fn maker_id(maker: Option<&ReferenceOrder>) -> Option<OrderId> {
    maker.map(|maker| maker.original_order().order_id)
}

/// 构造不交叉的双侧盘口，其中后挂入订单拥有更优价格。
fn two_sided_book() -> ReferenceOrderBook {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(11, Side::Sell, 110, 10),
        order(21, Side::Buy, 90, 10),
        order(12, Side::Sell, 105, 10),
        order(22, Side::Buy, 95, 10),
    ] {
        book.rest(resting).unwrap();
    }

    book
}

#[test]
fn buy_crosses_best_ask_and_sell_crosses_best_bid() {
    let book = two_sided_book();

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(100, Side::Buy, 108, 10))),
        Some(OrderId::new(12))
    );

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(101, Side::Sell, 92, 10))),
        Some(OrderId::new(22))
    );
}

#[test]
fn equal_price_is_crossing_on_both_sides() {
    let book = two_sided_book();

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 10))),
        Some(OrderId::new(12))
    );

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(101, Side::Sell, 95, 10))),
        Some(OrderId::new(22))
    );
}

#[test]
fn non_crossing_orders_return_none_on_both_sides() {
    let book = two_sided_book();

    // 买入价格低于最优卖价。
    assert!(
        book.best_crossing_maker(&order(100, Side::Buy, 104, 10))
            .is_none()
    );

    // 卖出价格高于最优买价。
    assert!(
        book.best_crossing_maker(&order(101, Side::Sell, 96, 10))
            .is_none()
    );
}

#[test]
fn empty_opposite_side_returns_none() {
    let empty = ReferenceOrderBook::new();

    assert!(
        empty
            .best_crossing_maker(&order(100, Side::Buy, 100, 10))
            .is_none()
    );

    assert!(
        empty
            .best_crossing_maker(&order(101, Side::Sell, 100, 10))
            .is_none()
    );

    let mut buy_only = ReferenceOrderBook::new();
    buy_only.rest(order(1, Side::Buy, 90, 10)).unwrap();

    assert!(
        buy_only
            .best_crossing_maker(&order(100, Side::Buy, 100, 10))
            .is_none()
    );

    let mut sell_only = ReferenceOrderBook::new();
    sell_only.rest(order(2, Side::Sell, 110, 10)).unwrap();

    assert!(
        sell_only
            .best_crossing_maker(&order(101, Side::Sell, 100, 10))
            .is_none()
    );
}

#[test]
fn same_price_makers_preserve_fifo_on_both_sides() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(30, Side::Sell, 105, 10),
        order(130, Side::Buy, 95, 10),
        order(10, Side::Sell, 105, 10),
        order(110, Side::Buy, 95, 10),
        order(20, Side::Sell, 105, 10),
        order(120, Side::Buy, 95, 10),
    ] {
        book.rest(resting).unwrap();
    }

    // 标识符的数值顺序故意不同于挂单顺序。
    assert_eq!(
        maker_id(book.best_crossing_maker(&order(200, Side::Buy, 105, 10))),
        Some(OrderId::new(30))
    );

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(201, Side::Sell, 95, 10))),
        Some(OrderId::new(130))
    );
}

#[test]
fn exhausted_maker_does_not_block_next_maker() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(30, Side::Sell, 105, 1)).unwrap();
    book.rest(order(10, Side::Sell, 105, 1)).unwrap();

    // 测试模块内部模拟已经完整成交的挂单订单。
    book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

    assert_eq!(book.orders[0].remaining(), 0);

    assert_eq!(
        maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 1))),
        Some(OrderId::new(10))
    );
}

#[test]
fn repeated_queries_preserve_all_book_state() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(30, Side::Sell, 105, 10),
        order(20, Side::Buy, 95, 10),
        order(10, Side::Sell, 105, 10),
    ] {
        book.rest(resting).unwrap();
    }

    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    // 包括源顺序、全部原始字段、剩余量和优先级。
    let snapshot = |book: &ReferenceOrderBook| {
        book.orders
            .iter()
            .map(|resting| {
                (
                    resting.original_order().clone(),
                    resting.remaining(),
                    resting.priority(),
                )
            })
            .collect::<Vec<_>>()
    };

    let before = snapshot(&book);
    let next_before = book.next_priority;

    let incoming_buy = order(100, Side::Buy, 105, 10);
    let incoming_sell = order(101, Side::Sell, 95, 10);

    for _ in 0..5 {
        assert_eq!(
            maker_id(book.best_crossing_maker(&incoming_buy)),
            Some(OrderId::new(30))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&incoming_sell)),
            Some(OrderId::new(20))
        );
    }

    assert_eq!(snapshot(&book), before);
    assert_eq!(book.next_priority, next_before);
    assert_eq!(book.orders.len(), 3);
    assert_eq!(book.orders[0].remaining(), 7);
    assert_eq!(book.orders[0].original_order().qty.get(), 10);
}

fn assert_index_matches_reference(
    book: &ReferenceOrderBook,
    incoming: &LimitOrder,
    expected: Option<usize>,
) {
    let index = book.best_crossing_maker_index(incoming);
    let reference = book.best_crossing_maker(incoming);

    assert_eq!(index, expected);

    match (index, reference) {
        (Some(index), Some(maker)) => {
            assert!(std::ptr::eq(maker, &book.orders[index]));
        }
        (None, None) => {}
        _ => panic!("maker index and public reference disagree"),
    }
}

#[test]
fn index_skips_exhausted_makers_on_both_sides() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(30, Side::Sell, 105, 1),
        order(130, Side::Buy, 95, 1),
        order(10, Side::Sell, 105, 1),
        order(110, Side::Buy, 95, 1),
    ] {
        book.rest(resting).unwrap();
    }

    // 仅测试数据修改运行态，查询自身不负责扣减。
    book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

    book.orders[1].apply_fill(Qty::try_new(1).unwrap()).unwrap();

    assert_eq!(book.orders[0].remaining(), 0);
    assert_eq!(book.orders[1].remaining(), 0);

    // 跳过耗尽订单，选择下一张同价挂单。
    assert_index_matches_reference(&book, &order(200, Side::Buy, 105, 1), Some(2));

    assert_index_matches_reference(&book, &order(201, Side::Sell, 95, 1), Some(3));
}
