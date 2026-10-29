use super::*;
use matching_domain::UserId;

fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn snapshot(book: &ReferenceOrderBook) -> (Vec<(LimitOrder, u64, QueuePriority)>, u128) {
    (
        book.orders
            .iter()
            .map(|resting| {
                (
                    resting.original_order().clone(),
                    resting.remaining(),
                    resting.priority(),
                )
            })
            .collect(),
        book.next_priority,
    )
}

fn assert_read_only(
    book: &ReferenceOrderBook,
    candidate: LimitOrder,
    expected: Result<(), RejectReason>,
) {
    let before = snapshot(book);
    let candidate_before = candidate.clone();

    for _ in 0..3 {
        assert_eq!(book.preflight_post_only(&candidate), expected);

        assert_eq!(snapshot(book), before);
        assert_eq!(candidate, candidate_before);
    }
}
#[test]
fn post_only_crossing_boundaries_on_both_sides() {
    for (side, maker_side, maker_price, crossing, equal, non_crossing) in [
        (Side::Buy, Side::Sell, 105, 106, 105, 104),
        (Side::Sell, Side::Buy, 95, 94, 95, 96),
    ] {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, maker_side, maker_price, 10)).unwrap();

        for (i, (price, expected)) in [
            (crossing, Err(RejectReason::PostOnlyWouldCross)),
            (equal, Err(RejectReason::PostOnlyWouldCross)),
            (non_crossing, Ok(())),
        ]
        .into_iter()
        .enumerate()
        {
            assert_read_only(&book, order(100 + i as u128, side, price, 5), expected);
        }
    }
}
#[test]
fn post_only_accepts_empty_or_same_side_book() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();

        assert_read_only(&book, order(100, side, 100, 1), Ok(()));

        book.rest(order(1, side, 100, 10)).unwrap();

        assert_read_only(&book, order(101, side, 100, 1), Ok(()));

        assert_read_only(&book, order(102, side, i64::MAX, 1), Ok(()));
    }
}
#[test]
fn duplicate_id_precedes_post_only_crossing() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(7, Side::Buy, 90, 10)).unwrap();

    book.rest(order(8, Side::Sell, 105, 10)).unwrap();

    // 与本方现存标识符重复，同时交叉。
    assert_read_only(
        &book,
        order(7, Side::Buy, 105, 1),
        Err(RejectReason::DuplicateOrderId),
    );

    assert_read_only(
        &book,
        order(8, Side::Sell, 90, 1),
        Err(RejectReason::DuplicateOrderId),
    );

    // 与对手方标识符重复，同时交叉。
    assert_read_only(
        &book,
        order(8, Side::Buy, 105, 1),
        Err(RejectReason::DuplicateOrderId),
    );

    assert_read_only(
        &book,
        order(7, Side::Sell, 90, 1),
        Err(RejectReason::DuplicateOrderId),
    );

    // 即使不交叉，重复标识符仍然拒绝。
    assert_read_only(
        &book,
        order(7, Side::Buy, 89, 1),
        Err(RejectReason::DuplicateOrderId),
    );
}
#[test]
fn multi_level_fifo_and_remaining_are_preserved() {
    let mut book = ReferenceOrderBook::new();

    for resting in [
        order(90, Side::Sell, 110, 10),
        order(30, Side::Sell, 105, 10),
        order(10, Side::Sell, 105, 10),
        order(290, Side::Buy, 90, 10),
        order(230, Side::Buy, 95, 10),
        order(210, Side::Buy, 95, 10),
    ] {
        book.rest(resting).unwrap();
    }

    // 模拟一张已经部分成交的挂单。
    book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

    // 验证现有挂单查询选择的先进先出首单。
    assert_eq!(
        book.best_crossing_maker(&order(300, Side::Buy, 105, 1))
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(30),
    );

    assert_eq!(
        book.best_crossing_maker(&order(301, Side::Sell, 95, 1))
            .unwrap()
            .original_order()
            .order_id,
        OrderId::new(230),
    );

    for (id, side, price, expected) in [
        (300, Side::Buy, 105, Err(RejectReason::PostOnlyWouldCross)),
        (302, Side::Buy, 104, Ok(())),
        (301, Side::Sell, 95, Err(RejectReason::PostOnlyWouldCross)),
        (303, Side::Sell, 96, Ok(())),
    ] {
        assert_read_only(&book, order(id, side, price, 1), expected);
    }

    assert_eq!(book.orders[1].remaining(), 7);
}
#[test]
fn post_only_handles_price_extremes() {
    let mut sell_book = ReferenceOrderBook::new();

    sell_book.rest(order(1, Side::Sell, i64::MAX, 1)).unwrap();

    assert_read_only(
        &sell_book,
        order(2, Side::Buy, i64::MAX, 1),
        Err(RejectReason::PostOnlyWouldCross),
    );

    assert_read_only(&sell_book, order(3, Side::Buy, i64::MAX - 1, 1), Ok(()));

    let mut buy_book = ReferenceOrderBook::new();

    buy_book.rest(order(4, Side::Buy, 1, 1)).unwrap();

    assert_read_only(
        &buy_book,
        order(5, Side::Sell, 1, 1),
        Err(RejectReason::PostOnlyWouldCross),
    );

    assert_read_only(&buy_book, order(6, Side::Sell, 2, 1), Ok(()));

    let mut extreme_book = ReferenceOrderBook::new();

    extreme_book.rest(order(7, Side::Buy, i64::MAX, 1)).unwrap();

    assert_read_only(
        &extreme_book,
        order(8, Side::Sell, 1, 1),
        Err(RejectReason::PostOnlyWouldCross),
    );
}
#[test]
fn preflight_does_not_check_or_consume_priority() {
    let mut book = ReferenceOrderBook::new();

    book.rest(order(1, Side::Sell, 105, 10)).unwrap();

    // 测试专用：模拟已达到优先级极值。
    book.next_priority = u128::MAX;

    assert_read_only(&book, order(2, Side::Buy, 104, 1), Ok(()));

    assert_read_only(
        &book,
        order(3, Side::Buy, 105, 1),
        Err(RejectReason::PostOnlyWouldCross),
    );

    assert_read_only(
        &book,
        order(1, Side::Buy, 105, 1),
        Err(RejectReason::DuplicateOrderId),
    );
}
