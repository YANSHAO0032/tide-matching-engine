use super::*;
use matching_domain::{IcebergOrder, UserId};

fn book() -> ProductionOrderBook {
    ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap())
}
fn order(id: u128, side: Side, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(1),
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}
fn iceberg(id: u128, qty: u64, peak: u64) -> IcebergOrder {
    IcebergOrder::try_new(order(id, Side::Sell, qty), Qty::try_new(peak).unwrap()).unwrap()
}
fn math() -> MarketMath {
    MarketMath::try_new(0, 0, 0, 1, 1).unwrap()
}
fn ids() -> Vec<TradeId> {
    (1..=20).map(TradeId::new).collect()
}

type LevelState = (Price, Option<OrderIndex>, Option<OrderIndex>, u32, u64);
#[derive(Debug, PartialEq, Eq)]
struct BookState {
    /// 买侧价格档及其链表、聚合数量投影。
    bids: Vec<LevelState>,
    /// 卖侧价格档及其链表、聚合数量投影。
    asks: Vec<LevelState>,
    /// 活动订单标识符到 Arena 槽位的投影。
    orders: HashMap<OrderId, OrderIndex>,
    /// Arena 槽位与空闲链表的完整投影。
    arena: (Vec<Option<OrderNode>>, Vec<OrderIndex>),
    /// 快照取得时下一次将分配的队列优先级数值。
    next_priority: u128,
}

fn state(book: &ProductionOrderBook) -> BookState {
    let levels = |side: &BTreeMap<Price, PriceLevel>| {
        side.iter()
            .map(|(price, level)| {
                (
                    *price,
                    level.head(),
                    level.tail(),
                    level.count(),
                    level.total_visible_qty(),
                )
            })
            .collect()
    };
    BookState {
        bids: levels(&book.bids),
        asks: levels(&book.asks),
        orders: book.orders.clone(),
        arena: book.arena.test_snapshot(),
        next_priority: book.next_queue_priority(),
    }
}

#[test]
fn iceberg_refresh_moves_behind_other_maker_and_depth_is_visible() {
    let mut book = book();
    book.place_iceberg_post_only(iceberg(1, 8, 3), &math())
        .unwrap();
    book.rest(order(2, Side::Sell, 2)).unwrap();
    assert_eq!(
        book.asks
            .get(&Price::try_new(100).unwrap())
            .unwrap()
            .total_visible_qty(),
        5
    );
    let result = book
        .place_limit_ioc(order(3, Side::Buy, 6), &math(), &ids())
        .unwrap();
    let trades = ProductionOrderBook::trades_only(result.events().to_vec());
    assert_eq!(
        trades
            .iter()
            .map(|t| (t.maker_order_id, t.qty.get()))
            .collect::<Vec<_>>(),
        vec![
            (OrderId::new(1), 3),
            (OrderId::new(2), 2),
            (OrderId::new(1), 1)
        ]
    );
    let maker = book.order(OrderId::new(1)).unwrap();
    assert_eq!(
        (
            maker.visible_remaining(),
            maker.hidden_remaining(),
            maker.total_remaining()
        ),
        (2, 2, 4)
    );
    assert_eq!(
        book.asks
            .get(&Price::try_new(100).unwrap())
            .unwrap()
            .total_visible_qty(),
        2
    );
    assert!(result.events().iter().any(|e| matches!(e, EngineEvent::IcebergRefreshed { order_id, visible } if *order_id == OrderId::new(1) && visible.get() == 3)));
}

#[test]
fn iceberg_fok_counts_replenished_hidden_liquidity() {
    let mut book = book();
    book.place_iceberg_post_only(iceberg(1, 7, 2), &math())
        .unwrap();
    let result = book
        .place_limit_fok(order(2, Side::Buy, 7), &math(), &ids())
        .unwrap();
    assert_eq!(
        ProductionOrderBook::trades_only(result.events().to_vec())
            .iter()
            .map(|t| t.qty.get())
            .collect::<Vec<_>>(),
        vec![2, 2, 2, 1]
    );
    assert!(book.order(OrderId::new(1)).is_none());
}

#[test]
fn replenishment_aggregate_overflow_rejects_before_first_fill() {
    let mut book = book();
    book.place_iceberg_post_only(iceberg(1, 10, 3), &math())
        .unwrap();
    let partial = book
        .place_limit_ioc(order(3, Side::Buy, 2), &math(), &ids())
        .unwrap();
    assert_eq!(
        ProductionOrderBook::trades_only(partial.events().to_vec()).len(),
        1
    );
    book.rest(order(2, Side::Sell, u64::MAX - 1)).unwrap();
    let before = state(&book);
    assert_eq!(
        book.place_limit_ioc(order(4, Side::Buy, 1), &math(), &ids()),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(state(&book), before);
}

#[test]
fn second_refresh_priority_exhaustion_is_atomic() {
    let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(3)).unwrap();
    let mut book = ProductionOrderBook::new(policy);
    book.place_iceberg_post_only(iceberg(1, 7, 2), &math())
        .unwrap();
    let before = state(&book);
    assert_eq!(
        book.place_limit_ioc(order(2, Side::Buy, 7), &math(), &ids()),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(state(&book), before);
}

#[test]
fn repeated_refresh_requires_every_trade_id_before_mutation() {
    let mut book = book();
    book.place_iceberg_post_only(iceberg(1, 7, 2), &math())
        .unwrap();
    let before = state(&book);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        book.place_limit_ioc(order(2, Side::Buy, 7), &math(), &ids()[..3])
    }));
    assert!(result.is_err());
    assert_eq!(state(&book), before);
}

#[test]
fn iceberg_incoming_validates_each_maker_and_aggregate_notional_before_mutation() {
    for (
        tick,
        lot,
        quote_scale,
        maker_price,
        maker_qty,
        makers,
        incoming_side,
        limit,
        total,
        peak,
        reason,
    ) in [
        (
            10,
            1,
            0,
            95,
            2,
            1,
            Side::Buy,
            100,
            4,
            1,
            RejectReason::InvalidTick,
        ),
        (
            1,
            2,
            0,
            100,
            3,
            1,
            Side::Buy,
            100,
            6,
            2,
            RejectReason::InvalidLot,
        ),
        (
            1,
            1,
            20,
            i64::MAX,
            2,
            1,
            Side::Sell,
            1,
            4,
            1,
            RejectReason::ArithmeticOverflow,
        ),
        (
            1,
            1,
            20,
            1_000_000_000_000_000_000,
            2,
            2,
            Side::Sell,
            1,
            5,
            1,
            RejectReason::ArithmeticOverflow,
        ),
    ] {
        let mut book = book();
        let math = MarketMath::try_new(0, 0, quote_scale, tick, lot).unwrap();
        for id in 1..=makers {
            let mut maker = order(
                id,
                if incoming_side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
                maker_qty,
            );
            maker.price = Price::try_new(maker_price).unwrap();
            book.rest(maker).unwrap();
        }
        let before = book.arena.test_snapshot();
        let before_index = book.orders.clone();
        let before_priority = book.next_queue_priority();
        let mut input = order(99, incoming_side, total);
        input.price = Price::try_new(limit).unwrap();
        let input = IcebergOrder::try_new(input, Qty::try_new(peak).unwrap()).unwrap();
        assert_eq!(book.place_iceberg(input, &math, &ids()), Err(reason));
        assert_eq!(book.arena.test_snapshot(), before);
        assert_eq!(book.orders, before_index);
        assert_eq!(book.next_queue_priority(), before_priority);
    }
}

#[test]
fn quote_dust_stops_before_unexecuted_maker_quantity_validation() {
    let mut book = book();
    let mut reference = crate::ReferenceOrderBook::new();
    let math = MarketMath::try_new(0, 0, 0, 1, 2).unwrap();
    let mut invalid_maker = order(1, Side::Sell, 3);
    invalid_maker.price = Price::try_new(90).unwrap();
    reference.rest(invalid_maker.clone()).unwrap();
    book.rest(invalid_maker).unwrap();
    book.place_iceberg_post_only(iceberg(2, 8, 2), &math)
        .unwrap();
    reference
        .place_iceberg_post_only(iceberg(2, 8, 2), &math)
        .unwrap();
    let before = state(&book);
    let input = MarketByQuote {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        side: Side::Buy,
        quote_budget: matching_domain::QuoteBudget::try_new(1).unwrap(),
        market_protection: None,
    };
    let result = book
        .execute_market_by_quote(input.clone(), &math, &ids())
        .unwrap();
    assert!(result.trades().is_empty());
    assert_eq!(result.unused_quote().get(), 1);
    assert_eq!(
        reference.execute_market_by_quote(input, &math, &ids()),
        Ok(result)
    );
    assert_eq!(state(&book), before);
}

#[test]
fn later_level_broken_prev_panics_before_any_iceberg_fill() {
    let mut book = book();
    book.place_iceberg_post_only(iceberg(1, 4, 2), &math())
        .unwrap();
    let mut other = order(2, Side::Sell, 2);
    other.price = Price::try_new(101).unwrap();
    book.rest(other).unwrap();
    let first = book.orders[&OrderId::new(1)];
    let second = book.orders[&OrderId::new(2)];
    book.arena.get_mut(second).unwrap().set_prev(Some(first));
    let before = state(&book);
    let mut input = order(3, Side::Buy, 6);
    input.price = Price::try_new(101).unwrap();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        book.place_limit_ioc(input, &math(), &ids())
    }));
    assert!(failure.is_err());
    assert_eq!(state(&book), before);
}

#[test]
fn residual_target_corruption_is_checked_before_maker_writes() {
    for iceberg_input in [false, true] {
        let mut book = book();
        book.place_iceberg_post_only(iceberg(1, 4, 2), &math())
            .unwrap();
        // 故意构造损坏的初始状态：绕过挂单时的价格交叉检查，
        // 以覆盖正常入口无法构造的余量挂单目标损坏路径。
        let target = order(2, Side::Buy, 2);
        let plan = book.preflight_rest_common(&target).unwrap();
        book.commit_rest(target, plan);
        let target_index = book.orders[&OrderId::new(2)];
        book.arena
            .get_mut(target_index)
            .unwrap()
            .set_next(Some(target_index));
        let before = state(&book);
        let input = order(3, Side::Buy, 7);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if iceberg_input {
                book.place_iceberg(
                    IcebergOrder::try_new(input, Qty::try_new(2).unwrap()).unwrap(),
                    &math(),
                    &ids(),
                )
            } else {
                book.place_limit_gtc_with_events(input, &ids())
            }
        }));
        assert!(result.is_err());
        assert_eq!(state(&book), before);
    }
}

#[test]
fn residual_priority_after_refresh_is_preflighted_for_both_gtc_inputs() {
    for iceberg_input in [false, true] {
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(2)).unwrap();
        let mut book = ProductionOrderBook::new(policy);
        book.place_iceberg_post_only(iceberg(1, 4, 2), &math())
            .unwrap();
        let before = state(&book);
        let input = order(2, Side::Buy, 5);
        let result = if iceberg_input {
            book.place_iceberg(
                IcebergOrder::try_new(input, Qty::try_new(2).unwrap()).unwrap(),
                &math(),
                &ids(),
            )
        } else {
            book.place_limit_gtc_with_events(input, &ids())
        };
        assert_eq!(result, Err(RejectReason::PrioritySpaceExhaustion));
        assert_eq!(state(&book), before);
    }
}

#[test]
fn legacy_trade_only_paths_keep_refresh_fifo_and_visible_depth() {
    for side in [Side::Buy, Side::Sell] {
        for mode in 0..5 {
            let mut book = book();
            let maker_side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            book.place_iceberg_post_only(
                IcebergOrder::try_new(order(1, maker_side, 8), Qty::try_new(3).unwrap()).unwrap(),
                &math(),
            )
            .unwrap();
            book.rest(order(2, maker_side, 2)).unwrap();
            let input = order(3, side, 6);
            let trades = match mode {
                0 => book.place_limit_gtc(input, &ids()).unwrap(),
                1 => book
                    .execute_fill_trades(&mut IncomingOrder::new(input), &ids())
                    .unwrap(),
                2 => book
                    .execute_market_by_base(
                        MarketByBase {
                            order_id: input.order_id,
                            user_id: input.user_id,
                            side,
                            qty: input.qty,
                            market_protection: None,
                        },
                        &ids(),
                    )
                    .unwrap(),
                3 => book
                    .execute_market_by_quote(
                        MarketByQuote {
                            order_id: input.order_id,
                            user_id: input.user_id,
                            side,
                            quote_budget: matching_domain::QuoteBudget::try_new(600).unwrap(),
                            market_protection: None,
                        },
                        &math(),
                        &ids(),
                    )
                    .unwrap()
                    .trades()
                    .to_vec(),
                _ => {
                    let mut incoming = IncomingOrder::new(input);
                    let mut trades = Vec::new();
                    for id in ids() {
                        let Some(trade) = book.execute_next_fill_trade(&mut incoming, id).unwrap()
                        else {
                            break;
                        };
                        trades.push(trade);
                    }
                    assert_eq!(incoming.remaining(), 0);
                    trades
                }
            };
            assert_eq!(
                trades
                    .iter()
                    .map(|trade| (trade.maker_order_id, trade.qty.get()))
                    .collect::<Vec<_>>(),
                vec![
                    (OrderId::new(1), 3),
                    (OrderId::new(2), 2),
                    (OrderId::new(1), 1)
                ]
            );
            let maker = book.order(OrderId::new(1)).unwrap();
            assert_eq!(
                (
                    maker.remaining(),
                    maker.hidden_remaining(),
                    maker.total_remaining()
                ),
                (2, 2, 4)
            );
            assert!(book.order(OrderId::new(2)).is_none());
            assert_eq!(book.next_queue_priority(), 3);
        }
    }
}
