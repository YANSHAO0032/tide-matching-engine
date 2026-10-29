//! 常规改单的生产结构、事件顺序及完整失败原子性证据。

use super::*;
use matching_domain::{
    AmendOrder, AmendedQuantity, AssetId, AssetReserve, CommandId, IcebergOrder, LogicalTime,
    MarketId, PriceReference, PriceReferenceKind, PriceReferenceState, ReservationAdjustmentToken,
    ReservationId, StopLimitOrder, StopOrder, StopPriority, TimeInForce, TriggerExpiry,
};
use matching_risk::ReservationState;

fn shadow(input: &LimitOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(input.order_id.get()),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 11,
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 0,
        reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 17,
        }],
    }
}

fn command(input: &LimitOrder) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: input.user_id,
        order_id: input.order_id,
        new_price: None,
        new_qty: None,
        new_quote_budget: None,
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }
}

fn rest(book: &mut ProductionOrderBook, input: &LimitOrder) {
    book.rest_with_reservation(input.clone(), shadow(input))
        .unwrap();
}

#[test]
fn amend_reprice_cancel_taker_keeps_maker_and_removes_taker() {
    let mut book = ProductionOrderBook::new(policy());
    let mut maker = order(1, Side::Buy, 101, 3);
    maker.stp_key = matching_domain::StpKey::new(7);
    let mut target = order(2, Side::Sell, 105, 5);
    target.stp_key = matching_domain::StpKey::new(7);
    target.stp_mode = matching_domain::StpMode::CancelTaker;
    rest(&mut book, &maker);
    rest(&mut book, &target);
    let mut amend = command(&target);
    amend.new_price = Some(Price::try_new(100).unwrap());
    let events = book
        .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert!(
        matches!(events.as_slice(), [EngineEvent::OrderAmended { .. }, EngineEvent::OrderCanceled { order_id, remaining: matching_domain::OrderRemaining::Base(q), reason: matching_domain::CancelReason::SelfTradePreventionTaker }] if *order_id == target.order_id && q.get() == 5)
    );
    assert!(book.order(target.order_id).is_none());
    assert_eq!(book.order(maker.order_id).unwrap().remaining(), 3);
    valid(&book);
}

#[test]
fn amend_reprice_cancel_maker_then_fill_matches_reference_event_order() {
    let mut production = ProductionOrderBook::new(policy());
    let mut reference = crate::ReferenceOrderBook::new();
    let mut own = order(1, Side::Buy, 101, 2);
    own.stp_key = matching_domain::StpKey::new(7);
    let mut external = order(2, Side::Buy, 101, 3);
    external.stp_key = matching_domain::StpKey::new(8);
    let mut target = order(3, Side::Sell, 105, 3);
    target.stp_key = matching_domain::StpKey::new(7);
    target.stp_mode = matching_domain::StpMode::CancelMaker;
    for input in [&own, &external, &target] {
        production
            .rest_with_reservation(input.clone(), shadow(input))
            .unwrap();
        reference
            .rest_with_reservation(input.clone(), shadow(input))
            .unwrap();
    }
    let mut amend = command(&target);
    amend.new_price = Some(Price::try_new(100).unwrap());
    let actual = production
        .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[TradeId::new(1)])
        .unwrap();
    let expected = reference
        .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[TradeId::new(1)])
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        matches!(actual.as_slice(), [EngineEvent::OrderAmended { .. }, EngineEvent::OrderCanceled { order_id, .. }, EngineEvent::Trade(trade), EngineEvent::OrderFilled { .. }] if *order_id == own.order_id && trade.maker_order_id == external.order_id)
    );
    valid(&production);
}

fn valid(book: &ProductionOrderBook) {
    assert_eq!(validate_book(book), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    /// 买方价格档与其队列顺序快照。
    bids: Vec<LevelSnapshotEntry>,
    /// 卖方价格档与其队列顺序快照。
    asks: Vec<LevelSnapshotEntry>,
    /// Arena 槽位与空闲链的完整快照。
    arena: (Vec<Option<OrderNode>>, Vec<OrderIndex>),
    /// 订单身份到 Arena 槽位的有序索引快照。
    index: Vec<(OrderId, OrderIndex)>,
    /// 活动订单到金融预留影子的有序快照。
    reservations: Vec<(OrderId, ReservationState)>,
    /// 未触发止损订单及其优先级的有序快照。
    stops: Vec<(StopOrder, StopPriority)>,
    /// 普通订单到期二级索引的稳定文本表示。
    expiry: String,
    /// 下一普通队列优先级计数。
    next: u128,
    /// 下一止损优先级计数。
    next_stop: u128,
    /// 触发和保护计算使用的参考价状态。
    references: PriceReferenceState,
}

fn snapshot(book: &ProductionOrderBook) -> Snapshot {
    let mut index = book
        .orders
        .iter()
        .map(|(id, slot)| (*id, *slot))
        .collect::<Vec<_>>();
    index.sort_unstable_by_key(|(id, _)| *id);
    let mut reservations = book
        .reservations
        .iter()
        .map(|(id, state)| (*id, state.clone()))
        .collect::<Vec<_>>();
    reservations.sort_unstable_by_key(|(id, _)| *id);
    Snapshot {
        bids: level_snapshot(&book.bids),
        asks: level_snapshot(&book.asks),
        arena: book.arena.test_snapshot(),
        index,
        reservations,
        stops: [Side::Buy, Side::Sell]
            .into_iter()
            .flat_map(|side| {
                book.stop_orders(side)
                    .into_iter()
                    .map(|(stop, priority)| (stop.clone(), priority))
            })
            .collect(),
        expiry: format!("{:?}", book.regular_expiry),
        next: book.next_queue_priority(),
        next_stop: book.next_stop_priority(),
        references: book.reference_prices.clone(),
    }
}

fn seed_unrelated(book: &mut ProductionOrderBook) {
    rest(book, &order(90, Side::Buy, 80, 3));
    rest(book, &order(91, Side::Sell, 130, 4));
    let input = order(92, Side::Buy, 70, 2);
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: input.order_id,
        user_id: input.user_id,
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(92),
        fee_schedule_version: 1,
        side: input.side,
        stop_price: Price::try_new(75).unwrap(),
        price: input.price,
        qty: input.qty,
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(30),
        },
    });
    book.place_stop_with_reservation(stop, LogicalTime::new(1), 9, shadow(&input))
        .unwrap();
    book.reference_prices
        .advance(PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 1,
            price: Price::try_new(99).unwrap(),
        })
        .unwrap();
}

#[test]
fn amend_regular_reduce_preserves_slot_fifo_and_rebases_absolute_total() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ProductionOrderBook::new(policy());
        let input = order(1, side, 100, 10);
        rest(&mut book, &input);
        rest(&mut book, &order(2, side, 100, 2));
        let slot = book.orders[&input.order_id];
        let priority = book.order(input.order_id).unwrap().priority();
        let other = if side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        book.place_limit_gtc(order(3, other, 100, 3), &[TradeId::new(1)])
            .unwrap();
        let next = book.next_queue_priority();
        for (total, remaining) in [(8, 5), (7, 4)] {
            let mut amend = command(&input);
            amend.new_qty = Some(Qty::try_new(total).unwrap());
            let events = book
                .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[])
                .unwrap();
            assert_eq!(events.len(), 1);
            assert!(matches!(
                events[0],
                EngineEvent::OrderAmended {
                    priority_retained: true,
                    ..
                }
            ));
            assert_eq!(book.orders[&input.order_id], slot);
            let node = book.order(input.order_id).unwrap();
            assert_eq!(node.original_order().qty.get(), total);
            assert_eq!(node.remaining(), remaining);
            assert_eq!(node.priority(), priority);
            assert_eq!(node.prev(), None);
            assert_eq!(book.next_queue_priority(), next);
            assert_eq!(
                book.reservation_state(input.order_id),
                Some(&shadow(&input))
            );
            valid(&book);
        }
    }
}

#[test]
fn amend_regular_increase_requeues_at_tail_with_one_new_priority() {
    for side in [Side::Buy, Side::Sell] {
        let mut book = ProductionOrderBook::new(policy());
        let input = order(1, side, 100, 5);
        rest(&mut book, &input);
        rest(&mut book, &order(2, side, 100, 2));
        let next = book.next_queue_priority();
        let mut amend = command(&input);
        amend.new_qty = Some(Qty::try_new(9).unwrap());
        book.amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[])
            .unwrap();
        let node = book.order(input.order_id).unwrap();
        assert_eq!(node.priority().get(), next);
        assert_eq!(node.prev(), Some(book.orders[&OrderId::new(2)]));
        assert_eq!(node.next(), None);
        assert_eq!(node.original_order().qty.get(), 9);
        assert_eq!(node.remaining(), 9);
        assert_eq!(book.next_queue_priority(), next + 1);
        assert_eq!(
            book.reservation_state(input.order_id),
            Some(&shadow(&input))
        );
        valid(&book);
    }
}

#[test]
fn amend_regular_requeue_unlinks_head_middle_tail_and_singleton() {
    for count in [1, 3] {
        for target in 1..=count {
            let mut book = ProductionOrderBook::new(policy());
            for id in 1..=count {
                rest(&mut book, &order(id, Side::Buy, 90, 5));
            }
            let input = order(target, Side::Buy, 90, 5);
            let mut amend = command(&input);
            amend.new_qty = Some(Qty::try_new(7).unwrap());
            book.amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[])
                .unwrap();
            let level = &book.bids[&Price::try_new(90).unwrap()];
            let mut ids = Vec::new();
            let mut cursor = level.head();
            while let Some(slot) = cursor {
                let node = book.arena.get(slot).unwrap();
                ids.push(node.original_order().order_id.get());
                cursor = node.next();
            }
            let mut expected = (1..=count).filter(|id| *id != target).collect::<Vec<_>>();
            expected.push(target);
            assert_eq!(ids, expected);
            assert_eq!(
                level.total_visible_qty(),
                u64::try_from(count).unwrap() * 5 + 2
            );
            valid(&book);
        }
    }
}

#[test]
fn amend_regular_reprice_without_trades_emits_only_amended_event() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 90, 5);
    rest(&mut book, &input);
    let mut amend = command(&input);
    amend.new_price = Some(Price::try_new(95).unwrap());
    let events = book
        .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], EngineEvent::OrderAmended { .. }));
    valid(&book);
}

#[test]
fn amend_regular_reprice_partial_and_full_fill_preserve_event_order_and_terminal_shadow_policy() {
    for side in [Side::Buy, Side::Sell] {
        for quantity in [3, 8] {
            let mut book = ProductionOrderBook::new(policy());
            let old_price = if side == Side::Buy { 90 } else { 110 };
            let input = order(1, side, old_price, quantity);
            rest(&mut book, &input);
            let maker_side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            let maker = order(2, maker_side, 100, 3);
            rest(&mut book, &maker);
            let next = book.next_queue_priority();
            let mut amend = command(&input);
            amend.new_price = Some(Price::try_new(100).unwrap());
            let events = book
                .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[TradeId::new(7)])
                .unwrap();
            assert!(
                matches!(events[0], EngineEvent::OrderAmended { new_quantity: AmendedQuantity::BaseRemaining(qty), priority_retained: false, .. } if qty.get() == quantity)
            );
            assert!(
                matches!(&events[1], EngineEvent::Trade(trade) if trade.trade_id == TradeId::new(7) && trade.maker_order_id == maker.order_id)
            );
            if quantity == 3 {
                assert!(
                    matches!(events[2], EngineEvent::OrderFilled { order_id } if order_id == input.order_id)
                );
                assert!(book.order(input.order_id).is_none());
                assert!(book.reservation_state(input.order_id).is_none());
                assert_eq!(book.next_queue_priority(), next);
            } else {
                assert!(
                    matches!(events[2], EngineEvent::OrderPartiallyFilled { remaining, .. } if remaining.get() == 5)
                );
                let node = book.order(input.order_id).unwrap();
                assert_eq!(node.remaining(), 5);
                assert_eq!(node.original_order().qty.get(), 8);
                assert_eq!(node.priority().get(), next);
                assert_eq!(
                    book.reservation_state(input.order_id),
                    Some(&shadow(&input))
                );
                assert_eq!(book.next_queue_priority(), next + 1);
            }
            assert!(book.reservation_state(maker.order_id).is_none());
            valid(&book);
        }
    }
}

#[test]
fn amend_regular_reprice_preflights_iceberg_refresh_and_orders_trades_before_lifecycle_events() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 90, 7);
    rest(&mut book, &input);
    let maker = order(2, Side::Sell, 100, 6);
    book.place_iceberg_post_only_with_reservation(
        IcebergOrder::try_new(maker.clone(), Qty::try_new(2).unwrap()).unwrap(),
        &MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        shadow(&maker),
    )
    .unwrap();
    let next = book.next_queue_priority();
    let mut amend = command(&input);
    amend.new_price = Some(Price::try_new(100).unwrap());
    let events = book
        .amend_regular_with_events(
            &amend,
            LogicalTime::new(1),
            9,
            &[TradeId::new(1), TradeId::new(2), TradeId::new(3)],
        )
        .unwrap();
    assert!(matches!(events[0], EngineEvent::OrderAmended { .. }));
    assert!(
        events[1..4]
            .iter()
            .all(|event| matches!(event, EngineEvent::Trade(_)))
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::IcebergRefreshed { .. }))
            .count(),
        2
    );
    assert_eq!(book.order(input.order_id).unwrap().remaining(), 1);
    assert_eq!(
        book.order(input.order_id).unwrap().priority().get(),
        next + 2
    );
    assert!(book.reservation_state(maker.order_id).is_none());
    valid(&book);
}

#[test]
fn amend_regular_tif_transitions_migrate_expiry_without_moving_fifo() {
    let tifs = [
        TimeInForce::Gtc,
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        TimeInForce::Day { session_id: 9 },
    ];
    for old in tifs {
        for new in tifs {
            let mut book = ProductionOrderBook::new(policy());
            let input = order(1, Side::Buy, 90, 5);
            book.rest_timed_with_reservation(
                input.clone(),
                old,
                LogicalTime::new(1),
                9,
                shadow(&input),
            )
            .unwrap();
            let before_slot = book.orders[&input.order_id];
            let next = book.next_queue_priority();
            let mut amend = command(&input);
            amend.new_tif = Some(new);
            book.amend_regular_with_events(&amend, LogicalTime::new(2), 9, &[])
                .unwrap();
            assert_eq!(book.orders[&input.order_id], before_slot);
            assert_eq!(book.order(input.order_id).unwrap().time_in_force(), new);
            assert_eq!(book.next_queue_priority(), next);
            valid(&book);
            assert_eq!(
                book.expire_gtd_through(LogicalTime::new(20)).len(),
                usize::from(matches!(new, TimeInForce::Gtd { .. }))
            );
            assert_eq!(
                book.end_session(9, 9).unwrap().len(),
                usize::from(matches!(new, TimeInForce::Day { .. }))
            );
            valid(&book);
        }
    }
}

#[test]
fn amend_regular_rejections_preserve_complete_deterministic_state() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 90, 10);
    book.rest_timed_with_reservation(
        input.clone(),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(1),
        9,
        shadow(&input),
    )
    .unwrap();
    seed_unrelated(&mut book);
    book.place_limit_gtc(order(2, Side::Sell, 90, 3), &[TradeId::new(1)])
        .unwrap();
    let mut cases = Vec::new();
    let mut amend = command(&input);
    amend.new_qty = Some(Qty::try_new(3).unwrap());
    cases.push((amend, RejectReason::InvalidQty));
    let mut amend = command(&input);
    amend.new_tif = Some(TimeInForce::Day { session_id: 10 });
    cases.push((amend, RejectReason::InvalidSession));
    let mut amend = command(&input);
    amend.new_tif = Some(TimeInForce::Gtd {
        expire_at: LogicalTime::new(1),
    });
    cases.push((amend, RejectReason::InvalidExpireTime));
    let mut amend = command(&input);
    amend.new_quote_budget = Some(QuoteAmount::new(2));
    cases.push((amend, RejectReason::InvalidOrderCombination));
    let mut amend = command(&input);
    amend.reservation_adjustment = Some(ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId::new(1),
        reservation_id: ReservationId::new(1),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        expected_reservation_version: 11,
        target_reserves: Default::default(),
        fee_schedule_version: 1,
        fee_tier_id: 0,
        risk_key_id: 1,
        signature: [0; 64],
    });
    cases.push((amend, RejectReason::InvalidReservation));
    for (amend, reason) in cases {
        let before = snapshot(&book);
        assert_eq!(
            book.amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[]),
            Err(reason)
        );
        assert_eq!(snapshot(&book), before);
        valid(&book);
    }
    book.reservations.remove(&input.order_id);
    let before = snapshot(&book);
    assert_eq!(
        book.amend_regular_with_events(&command(&input), LogicalTime::new(1), 9, &[]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(snapshot(&book), before);
    valid(&book);
}

#[test]
fn amend_regular_priority_and_aggregate_failures_preserve_slots_and_indexes() {
    let mut book = ProductionOrderBook::new(
        QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(1)).unwrap(),
    );
    let input = order(1, Side::Buy, 90, 5);
    rest(&mut book, &input);
    for price in [90, 95] {
        let mut amend = command(&input);
        amend.new_price = Some(Price::try_new(price).unwrap());
        amend.new_qty = Some(Qty::try_new(6).unwrap());
        let before = snapshot(&book);
        assert_eq!(
            book.amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[]),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(snapshot(&book), before);
        valid(&book);
    }
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 90, 1);
    rest(&mut book, &input);
    rest(&mut book, &order(2, Side::Buy, 90, u64::MAX - 1));
    let mut amend = command(&input);
    amend.new_qty = Some(Qty::try_new(2).unwrap());
    let before = snapshot(&book);
    assert_eq!(
        book.amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[]),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(snapshot(&book), before);
    valid(&book);
}

#[test]
fn amend_regular_full_fill_at_priority_ceiling_needs_no_residual_allocation() {
    let mut book = ProductionOrderBook::new(
        QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(2)).unwrap(),
    );
    let input = order(1, Side::Buy, 90, 5);
    rest(&mut book, &input);
    rest(&mut book, &order(2, Side::Sell, 100, 5));
    let mut amend = command(&input);
    amend.new_price = Some(Price::try_new(100).unwrap());
    let events = book
        .amend_regular_with_events(&amend, LogicalTime::new(1), 9, &[TradeId::new(1)])
        .unwrap();
    assert!(
        matches!(events.last(), Some(EngineEvent::OrderFilled { order_id }) if *order_id == input.order_id)
    );
    assert_eq!(book.next_queue_priority(), 2);
    assert!(book.order(input.order_id).is_none());
    assert!(book.reservation_state(input.order_id).is_none());
    valid(&book);
}

#[test]
fn amend_regular_insufficient_trade_ids_fail_stops_before_any_state_change() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(1, Side::Buy, 90, 5);
    rest(&mut book, &input);
    seed_unrelated(&mut book);
    rest(&mut book, &order(2, Side::Sell, 100, 3));
    let mut amend = command(&input);
    amend.new_price = Some(Price::try_new(100).unwrap());
    let before = snapshot(&book);
    assert!(
        catch_unwind(AssertUnwindSafe(|| book.amend_regular_with_events(
            &amend,
            LogicalTime::new(1),
            9,
            &[]
        )))
        .is_err()
    );
    assert_eq!(snapshot(&book), before);
    valid(&book);
}

#[test]
fn amend_regular_iceberg_trade_ids_and_refresh_priority_fail_before_unlink() {
    for shortage in [true, false] {
        let ceiling = if shortage { 20 } else { 3 };
        let mut book = ProductionOrderBook::new(
            QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(ceiling)).unwrap(),
        );
        let input = order(1, Side::Buy, 90, 7);
        rest(&mut book, &input);
        let maker = order(2, Side::Sell, 100, 6);
        book.place_iceberg_post_only_with_reservation(
            IcebergOrder::try_new(maker.clone(), Qty::try_new(2).unwrap()).unwrap(),
            &MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            shadow(&maker),
        )
        .unwrap();
        let mut amend = command(&input);
        amend.new_price = Some(Price::try_new(100).unwrap());
        let before = snapshot(&book);
        if shortage {
            assert!(
                catch_unwind(AssertUnwindSafe(|| book.amend_regular_with_events(
                    &amend,
                    LogicalTime::new(1),
                    9,
                    &[TradeId::new(1)]
                )))
                .is_err()
            );
        } else {
            assert_eq!(
                book.amend_regular_with_events(
                    &amend,
                    LogicalTime::new(1),
                    9,
                    &[TradeId::new(1), TradeId::new(2), TradeId::new(3)]
                ),
                Err(RejectReason::PrioritySpaceExhaustion)
            );
        }
        assert_eq!(snapshot(&book), before);
        valid(&book);
    }
}
