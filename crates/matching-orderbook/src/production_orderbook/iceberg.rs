//! 先只读模拟冰山的先进先出成交顺序，再提交真实订单簿结构变更。
use super::*;
use matching_domain::{IcebergOrder, LimitImmediateExecution, ProtectionSource};
use std::collections::VecDeque;

#[derive(Clone, Copy)]
struct VirtualMaker {
    id: OrderId,
    visible: u64,
    hidden: u64,
    peak: Option<Qty>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::UserId;

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
        bids: Vec<LevelState>,
        asks: Vec<LevelState>,
        orders: HashMap<OrderId, OrderIndex>,
        arena: (Vec<Option<OrderNode>>, Vec<OrderIndex>),
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
                    IcebergOrder::try_new(order(1, maker_side, 8), Qty::try_new(3).unwrap())
                        .unwrap(),
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
                            let Some(trade) =
                                book.execute_next_fill_trade(&mut incoming, id).unwrap()
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
}

#[derive(Clone, Copy)]
struct SliceFill {
    id: OrderId,
    price: Price,
    qty: Qty,
    total_after: u64,
    refresh: Option<(Qty, QueuePriority)>,
}

struct IcebergPlan {
    fills: Vec<SliceFill>,
    base_remaining: u64,
    quote_remaining: QuoteAmount,
    spent: QuoteAmount,
    stop: Option<ProtectionBoundaryStop>,
    next_priority: u128,
}

enum Budget {
    Base(u64),
    Quote(QuoteAmount),
}

impl ProductionOrderBook {
    pub(super) fn has_iceberg_maker(&self, incoming_side: Side) -> bool {
        let levels = match incoming_side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };
        levels.values().any(|level| {
            let mut cursor = level.head();
            let mut previous = None;
            let mut count = 0u32;
            while let Some(index) = cursor {
                let node = self
                    .arena
                    .get(index)
                    .expect("iceberg detection missing Arena node");
                assert_eq!(
                    node.prev(),
                    previous,
                    "iceberg detection broken FIFO backlink"
                );
                count = count
                    .checked_add(1)
                    .expect("iceberg detection count overflow");
                assert!(
                    count <= level.count(),
                    "iceberg detection FIFO cycle or count mismatch"
                );
                if node.peak_size().is_some() {
                    return true;
                }
                previous = Some(index);
                cursor = node.next();
            }
            false
        })
    }

    fn preflight_iceberg_plan(
        &self,
        side: Side,
        limit: Option<Price>,
        mut budget: Budget,
        math: Option<&MarketMath>,
        protection: Option<(Price, ProtectionSource)>,
        max_fills: Option<usize>,
    ) -> Result<IcebergPlan, RejectReason> {
        let maker_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let levels = match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };
        let ordered: Box<dyn Iterator<Item = (&Price, &PriceLevel)> + '_> = match side {
            Side::Buy => Box::new(levels.iter()),
            Side::Sell => Box::new(levels.iter().rev()),
        };
        let mut fills = Vec::new();
        let mut spent = QuoteAmount::new(0);
        let mut next_priority = self.next_queue_priority();
        let mut stop = None;
        'levels: for (price, level) in ordered {
            if max_fills.is_some_and(|max| fills.len() >= max) {
                break;
            }
            if match budget {
                Budget::Base(v) => v == 0,
                Budget::Quote(v) => v.get() == 0,
            } {
                break;
            }
            if limit.is_some_and(|bound| match side {
                Side::Buy => *price > bound,
                Side::Sell => *price < bound,
            }) {
                break;
            }
            if !maker_price_within_protection(side, *price, protection.map(|(p, _)| p)) {
                let (effective_worst_price, source) = protection.expect("boundary needed for stop");
                stop = Some(ProtectionBoundaryStop {
                    next_price: *price,
                    effective_worst_price,
                    source,
                });
                break;
            }
            let mut virtual_fifo = VecDeque::new();
            let mut cursor = level.head();
            let mut aggregate = 0u64;
            let mut count = 0u32;
            let mut previous_priority = None;
            let mut previous = None;
            assert!(level.count() > 0, "iceberg plan empty price level");
            while let Some(index) = cursor {
                let node = self
                    .arena
                    .get(index)
                    .expect("iceberg preflight missing Arena node");
                assert_eq!(node.prev(), previous, "iceberg plan broken FIFO backlink");
                assert_eq!(node.original_order().side, maker_side);
                assert_eq!(node.original_order().price, *price);
                assert_eq!(
                    self.orders.get(&node.original_order().order_id),
                    Some(&index)
                );
                assert!(node.remaining() > 0);
                assert!(previous_priority.is_none_or(|prev| prev < node.priority()));
                assert!(
                    node.priority().get() < self.next_queue_priority(),
                    "active priority must precede allocator"
                );
                previous_priority = Some(node.priority());
                aggregate = aggregate
                    .checked_add(node.remaining())
                    .expect("existing level aggregate overflow");
                count = count.checked_add(1).expect("existing level count overflow");
                assert!(
                    count <= level.count(),
                    "iceberg plan FIFO cycle or count mismatch"
                );
                virtual_fifo.push_back(VirtualMaker {
                    id: node.original_order().order_id,
                    visible: node.remaining(),
                    hidden: node.hidden_remaining(),
                    peak: node.peak_size(),
                });
                previous = Some(index);
                cursor = node.next();
            }
            assert_eq!(previous, level.tail(), "iceberg plan FIFO tail mismatch");
            assert_eq!(aggregate, level.total_visible_qty());
            assert_eq!(count, level.count());
            while let Some(mut maker) = virtual_fifo.pop_front() {
                if max_fills.is_some_and(|max| fills.len() >= max) {
                    break 'levels;
                }
                if match budget {
                    Budget::Base(v) => v == 0,
                    Budget::Quote(v) => v.get() == 0,
                } {
                    break 'levels;
                }
                let fill = match budget {
                    Budget::Base(remaining) => remaining.min(maker.visible),
                    Budget::Quote(quote) => {
                        let Some(max) = math
                            .expect("quote math")
                            .max_qty_for_quote(quote, *price)
                            .map_err(Self::reject_quote_math_error)?
                        else {
                            break 'levels;
                        };
                        max.get().min(maker.visible)
                    }
                };
                if fill == 0 {
                    break 'levels;
                }
                let qty = Qty::try_new(fill).expect("positive planned fill");
                if let Some(math) = math {
                    let notional = math
                        .trade_notional(*price, qty)
                        .map_err(Self::reject_quote_math_error)?;
                    spent = spent.checked_add(notional)?;
                    if let Budget::Quote(remaining) = budget {
                        budget = Budget::Quote(remaining.checked_sub(notional)?);
                    }
                }
                if let Budget::Base(remaining) = budget {
                    budget = Budget::Base(remaining.checked_sub(fill).expect("fill <= remaining"));
                }
                maker.visible = maker.visible.checked_sub(fill).expect("fill <= visible");
                aggregate = aggregate
                    .checked_sub(fill)
                    .expect("aggregate includes maker");
                let mut refresh = None;
                if maker.visible == 0 && maker.hidden > 0 {
                    let visible = maker
                        .hidden
                        .min(maker.peak.expect("hidden requires peak").get());
                    maker.hidden = maker.hidden.checked_sub(visible).expect("slice <= hidden");
                    maker.visible = visible;
                    aggregate = aggregate
                        .checked_add(visible)
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                    if next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
                        return Err(RejectReason::PrioritySpaceExhaustion);
                    }
                    let priority = QueuePriority::new(next_priority);
                    next_priority = next_priority
                        .checked_add(1)
                        .expect("priority below ceiling");
                    refresh = Some((Qty::try_new(visible).expect("positive refresh"), priority));
                    virtual_fifo.push_back(maker);
                } else if maker.visible > 0 {
                    virtual_fifo.push_front(maker);
                }
                fills.push(SliceFill {
                    id: maker.id,
                    price: *price,
                    qty,
                    total_after: maker
                        .visible
                        .checked_add(maker.hidden)
                        .expect("total remaining overflow"),
                    refresh,
                });
            }
        }
        let (base_remaining, quote_remaining) = match budget {
            Budget::Base(base) => (base, QuoteAmount::new(0)),
            Budget::Quote(quote) => (0, quote),
        };
        Ok(IcebergPlan {
            fills,
            base_remaining,
            quote_remaining,
            spent,
            stop,
            next_priority,
        })
    }

    fn commit_iceberg_plan(
        &mut self,
        plan: &IcebergPlan,
        side: Side,
        taker: OrderId,
        ids: &[TradeId],
    ) -> Vec<EngineEvent> {
        assert!(
            ids.len() >= plan.fills.len(),
            "insufficient TradeIds before iceberg commit"
        );
        let maker_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let mut events = Vec::new();
        for (fill, trade_id) in plan.fills.iter().zip(ids.iter().copied()) {
            assert_eq!(self.best_price(maker_side), Some(fill.price));
            let level = match maker_side {
                Side::Buy => self.bids.get_mut(&fill.price),
                Side::Sell => self.asks.get_mut(&fill.price),
            }
            .expect("planned maker level missing");
            let index = level.head().expect("planned maker head missing");
            let maker = self.arena.get(index).expect("planned maker node missing");
            assert_eq!(maker.original_order().order_id, fill.id);
            let visible_before = maker.remaining();
            let is_iceberg = maker.peak_size().is_some();
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.id,
                taker_order_id: taker,
                price: fill.price,
                qty: fill.qty,
                maker_side,
            };
            if fill.qty.get() < visible_before {
                level.apply_head_partial_fill(&mut self.arena, fill.qty);
            } else if let Some((visible, priority)) = fill.refresh {
                let mut node = level
                    .pop_front(&mut self.arena)
                    .expect("planned exhausted slice missing");
                assert_eq!(node.original_order().order_id, fill.id);
                assert_eq!(self.orders.get(&fill.id), Some(&index));
                assert_eq!(node.replenish(priority), visible);
                let new_index = level
                    .push_back(&mut self.arena, node)
                    .expect("preflighted replenish must fit");
                assert_eq!(new_index, index);
                let actual = self
                    .queue_priority_allocator
                    .allocate()
                    .expect("preflighted priority unavailable");
                assert_eq!(actual, priority);
            } else {
                let removed = level
                    .pop_front(&mut self.arena)
                    .expect("planned full maker missing");
                assert_eq!(removed.original_order().order_id, fill.id);
                assert_eq!(self.orders.remove(&fill.id), Some(index));
                if level.is_empty() {
                    let removed_level = match maker_side {
                        Side::Buy => self.bids.remove(&fill.price),
                        Side::Sell => self.asks.remove(&fill.price),
                    };
                    assert!(removed_level.expect("empty level missing").is_empty());
                }
            }
            events.push(EngineEvent::Trade(trade));
            if is_iceberg {
                if fill.total_after == 0 {
                    events.push(EngineEvent::OrderFilled { order_id: fill.id });
                } else {
                    events.push(EngineEvent::OrderPartiallyFilled {
                        order_id: fill.id,
                        remaining: Qty::try_new(fill.total_after).expect("positive total"),
                    });
                }
                if let Some((visible, _)) = fill.refresh {
                    events.push(EngineEvent::IcebergRefreshed {
                        order_id: fill.id,
                        visible,
                    });
                }
            }
        }
        assert_eq!(self.next_queue_priority(), plan.next_priority);
        events
    }

    pub(super) fn trades_only(events: Vec<EngineEvent>) -> Vec<TradeEvent> {
        events
            .into_iter()
            .filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade),
                _ => None,
            })
            .collect()
    }

    /// 冰山以全部总量主动尝试成交，再将余量的第一片显示为挂单。
    ///
    /// 改簿前校验算术、补量优先级与余量挂单条件。
    /// 事件保留冰山挂单的生命周期和新到订单的总余量。
    /// 调用方提供的 `TradeId` 不足时，在改簿前触发不变量断言。
    pub fn place_iceberg(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let order = iceberg.order();
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        math.trade_notional(order.price, iceberg.peak_size())
            .map_err(Self::reject_quote_math_error)?;
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg"
        );
        self.preflight_iceberg_rest(&iceberg, &plan)?;
        let mut events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        if !plan.fills.is_empty() {
            if plan.base_remaining == 0 {
                events.push(EngineEvent::OrderFilled {
                    order_id: order.order_id,
                });
            } else {
                events.push(EngineEvent::OrderPartiallyFilled {
                    order_id: order.order_id,
                    remaining: Qty::try_new(plan.base_remaining)
                        .expect("positive iceberg remainder"),
                });
            }
        }
        if plan.base_remaining > 0 {
            let mut visible_order = order.clone();
            visible_order.qty = Qty::try_new(plan.base_remaining.min(iceberg.peak_size().get()))
                .expect("visible positive");
            let rest_plan = self
                .preflight_rest_common(&visible_order)
                .expect("preflighted iceberg rest unavailable");
            self.commit_iceberg_rest(iceberg, plan.base_remaining, rest_plan);
        }
        Ok(events)
    }

    /// 挂入可见片前拒绝会发生交叉的价格，等价也属于交叉。
    /// 隐藏量保留至后续挂单补量或取消时处理。
    pub fn place_iceberg_post_only(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
    ) -> Result<(), RejectReason> {
        let order = iceberg.order();
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let crosses = match order.side {
            Side::Buy => self
                .best_price(Side::Sell)
                .is_some_and(|p| order.price >= p),
            Side::Sell => self.best_price(Side::Buy).is_some_and(|p| order.price <= p),
        };
        if crosses {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        math.trade_notional(order.price, iceberg.peak_size())
            .map_err(Self::reject_quote_math_error)?;
        let mut visible_order = order.clone();
        visible_order.qty = iceberg.peak_size();
        let plan = self.preflight_rest_common(&visible_order)?;
        let total = order.qty.get();
        self.commit_iceberg_rest(iceberg, total, plan);
        Ok(())
    }

    fn preflight_iceberg_rest(
        &self,
        iceberg: &IcebergOrder,
        plan: &IcebergPlan,
    ) -> Result<(), RejectReason> {
        if plan.base_remaining == 0 {
            return Ok(());
        }
        if plan.next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let order = iceberg.order();
        let mut visible_order = order.clone();
        visible_order.qty = Qty::try_new(plan.base_remaining.min(iceberg.peak_size().get()))?;
        // 对手侧成交不会修改该目标价档；此时检查其链接与容量，
        // 上方的分配器预演边界已覆盖补量消耗的优先级。
        self.preflight_rest_common(&visible_order)?;
        Ok(())
    }

    fn commit_iceberg_rest(&mut self, iceberg: IcebergOrder, total: u64, plan: RestPlan) {
        let id = iceberg.order().order_id;
        let node = OrderNode::new_iceberg(iceberg, total, plan.priority);
        let slot = match plan.side {
            Side::Buy => commit_rest_target(&mut self.bids, &mut self.arena, node, &plan),
            Side::Sell => commit_rest_target(&mut self.asks, &mut self.arena, node, &plan),
        };
        assert!(self.orders.insert(id, slot).is_none());
        assert_eq!(
            self.queue_priority_allocator
                .allocate()
                .expect("preflighted priority"),
            plan.priority
        );
    }

    /// 执行普通 GTC 订单，并保留冰山挂单的生命周期事件。
    /// 整条命令预检通过后，才挂入新到订单的余量。
    /// 普通挂单沿用仅报告成交事件的既有契约。
    pub fn place_limit_gtc_with_events(
        &mut self,
        order: LimitGtcOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if !self.has_iceberg_maker(order.side) {
            return self
                .place_limit_gtc(order, trade_ids)
                .map(|trades| trades.into_iter().map(EngineEvent::Trade).collect());
        }
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            None,
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg-aware GTC"
        );
        if plan.base_remaining > 0 {
            if plan.next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
                return Err(RejectReason::PrioritySpaceExhaustion);
            }
            let mut residual = order.clone();
            residual.qty = Qty::try_new(plan.base_remaining)?;
            self.preflight_rest_common(&residual)?;
        }
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        if plan.base_remaining > 0 {
            let mut residual = order;
            residual.qty = Qty::try_new(plan.base_remaining).expect("positive remaining");
            let rest_plan = self
                .preflight_rest_common(&residual)
                .expect("preflighted GTC rest");
            self.commit_rest(residual, rest_plan);
        }
        Ok(events)
    }

    pub(super) fn execute_next_fill_iceberg(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        let order = incoming.original_order();
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(incoming.remaining()),
            None,
            None,
            Some(1),
        )?;
        if plan.fills.is_empty() {
            return Ok(None);
        }
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, &[trade_id]);
        let trade = Self::trades_only(events)
            .pop()
            .expect("planned fill emitted Trade");
        incoming
            .apply_fill(trade.qty)
            .expect("preflighted incoming quantity");
        Ok(Some(trade))
    }

    pub(super) fn execute_fill_trades_iceberg(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        let order = incoming.original_order();
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(incoming.remaining()),
            None,
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg batch"
        );
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        for fill in &plan.fills {
            incoming
                .apply_fill(fill.qty)
                .expect("preflighted incoming quantity");
        }
        Ok(Self::trades_only(events))
    }

    pub(super) fn execute_market_base_iceberg(
        &mut self,
        incoming: MarketByBase,
        math: Option<(&CommandStartProtectionContext, &MarketMath)>,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.orders.contains_key(&incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let protection = if let Some((context, math)) = math {
            effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?
        } else {
            None
        };
        let plan = self.preflight_iceberg_plan(
            incoming.side,
            None,
            Budget::Base(incoming.qty.get()),
            math.map(|(_, math)| math),
            protection,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg MarketByBase"
        );
        let mut events =
            self.commit_iceberg_plan(&plan, incoming.side, incoming.order_id, trade_ids);
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }
        Ok(events)
    }

    pub(super) fn execute_market_quote_iceberg(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        context: Option<&CommandStartProtectionContext>,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        if self.orders.contains_key(&incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let protection = if let Some(context) = context {
            effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?
        } else {
            None
        };
        let plan = self.preflight_iceberg_plan(
            incoming.side,
            None,
            Budget::Quote(QuoteAmount::new(incoming.quote_budget.get())),
            Some(math),
            protection,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg MarketByQuote"
        );
        let mut events =
            self.commit_iceberg_plan(&plan, incoming.side, incoming.order_id, trade_ids);
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }
        Ok(ProtectedMarketByQuoteExecution::new(
            events,
            plan.spent,
            plan.quote_remaining,
        ))
    }

    pub(super) fn place_immediate_iceberg(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
        fok: bool,
    ) -> Result<LimitImmediateExecution, RejectReason> {
        if self.orders.contains_key(&order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            None,
        )?;
        if fok && plan.base_remaining > 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg immediate Limit"
        );
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        let canceled = if plan.base_remaining == 0 {
            None
        } else {
            Some(Qty::try_new(plan.base_remaining).expect("positive IOC remaining"))
        };
        Ok(LimitImmediateExecution::from_events(
            order.order_id,
            events,
            canceled,
        ))
    }
}
