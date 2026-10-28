//! 参考 Vec 常规改单的提交、事件顺序与失败原子性。

use super::*;
use matching_domain::{
    AmendOrder, AmendedQuantity, AssetId, AssetReserve, CommandId, LogicalTime, MarketId,
    PriceReference, PriceReferenceKind, PriceReferenceState, ReservationAdjustmentToken,
    ReservationId, StopLimitOrder, StopOrder, StopPriority, TimeInForce, TriggerExpiry, UserId,
};
use matching_risk::ReservationState;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn order(id: u128, user: u64, side: Side, price: u64, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(user),
        side,
        price: Price::try_new(price.try_into().unwrap()).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

fn reservation(order: &LimitOrder, version: u64) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(order.order_id.get()),
        order_id: order.order_id,
        user_id: order.user_id,
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version,
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
        reserves: Vec::new(),
    }
}

fn reservation_with_reserve(order: &LimitOrder, version: u64) -> ReservationState {
    ReservationState {
        reserves: vec![AssetReserve {
            asset: AssetId::new(9),
            atoms: 17,
        }],
        ..reservation(order, version)
    }
}

fn reject_unusable_shadow_without_adjustment(case: u8) {
    let keys = matching_risk::RiskKeySet::new(2, []).unwrap();
    for side in [Side::Buy, Side::Sell] {
        let mut book = ReferenceOrderBook::new();
        let input = order(1, 11, side, 100, 10);
        book.rest_with_reservation(input.clone(), reservation_with_reserve(&input, 5))
            .unwrap();
        let shadow = book.reservations.get_mut(&input.order_id).unwrap();
        match case {
            0 => shadow.status = matching_risk::ReservationStatus::Released,
            1 => shadow.fee_schedule_version = 2,
            2 => shadow.fee_tier_id = 2,
            _ => unreachable!(),
        }
        let mut command = amend(&input);
        command.new_qty = Some(Qty::try_new(8).unwrap());
        assert!(command.reservation_adjustment.is_none());
        let context = crate::AmendExecutionContext {
            market_math: matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            command_id: CommandId::new(80),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_keys: &keys,
            required_reserves: vec![AssetReserve {
                asset: AssetId::new(9),
                atoms: 10,
            }],
        };
        let before = book.test_deterministic_state();
        assert_eq!(
            book.amend_with_context(&command, LogicalTime::new(1), 9, &[], &context),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(book.test_deterministic_state(), before);
    }
}

#[test]
fn amend_no_adjustment_rejects_released_shadow_atomically() {
    reject_unusable_shadow_without_adjustment(0);
}

#[test]
fn amend_no_adjustment_rejects_pinned_fee_version_mismatch_atomically() {
    reject_unusable_shadow_without_adjustment(1);
}

#[test]
fn amend_no_adjustment_rejects_pinned_fee_tier_mismatch_atomically() {
    reject_unusable_shadow_without_adjustment(2);
}

fn amend(order: &LimitOrder) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: order.user_id,
        order_id: order.order_id,
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

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    /// 普通订单原始载荷、可见量、隐藏量、峰值、优先级和有效期的有序快照。
    orders: Vec<(
        LimitOrder,
        u64,
        u64,
        Option<Qty>,
        QueuePriority,
        TimeInForce,
    )>,
    /// 未触发止损订单及其止损优先级的有序快照。
    stops: Vec<(StopOrder, StopPriority)>,
    /// 活动订单到金融预留影子的有序快照。
    reservations: Vec<(OrderId, ReservationState)>,
    /// 下一普通队列优先级计数。
    next_priority: u128,
    /// 下一止损优先级计数。
    next_stop_priority: u128,
    /// 触发和保护计算使用的参考价状态。
    reference_prices: PriceReferenceState,
}

fn snapshot(book: &ReferenceOrderBook) -> Snapshot {
    let mut reservations = book
        .reservations
        .iter()
        .map(|(id, state)| (*id, state.clone()))
        .collect::<Vec<_>>();
    reservations.sort_unstable_by_key(|(id, _)| *id);
    Snapshot {
        orders: book
            .orders
            .iter()
            .map(|entry| {
                (
                    entry.original_order().clone(),
                    entry.visible_remaining(),
                    entry.hidden_remaining(),
                    entry.peak_size(),
                    entry.priority(),
                    entry.time_in_force(),
                )
            })
            .collect(),
        stops: book
            .stop_orders
            .iter()
            .map(|entry| (entry.order.clone(), entry.priority))
            .collect(),
        reservations,
        next_priority: book.next_priority,
        next_stop_priority: book.next_stop_priority,
        reference_prices: book.reference_prices.clone(),
    }
}

/// 写入不参与当前改单撮合但会影响未来行为的双侧、Stop 与价格参考状态。
fn seed_unrelated_deterministic_state(book: &mut ReferenceOrderBook) {
    let bid = order(90, 90, Side::Buy, 90, 4);
    let ask = order(91, 91, Side::Sell, 130, 6);
    book.rest_with_reservation(bid.clone(), reservation_with_reserve(&bid, 8))
        .unwrap();
    book.rest_with_reservation(ask.clone(), reservation_with_reserve(&ask, 9))
        .unwrap();
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(92),
        user_id: UserId::new(92),
        reservation_id: ReservationId::new(92),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(80).unwrap(),
        price: Price::try_new(81).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    let stop_state = ReservationState {
        reservation_id: ReservationId::new(92),
        order_id: OrderId::new(92),
        user_id: UserId::new(92),
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
        version: 10,
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
        reserves: vec![AssetReserve {
            asset: AssetId::new(10),
            atoms: 19,
        }],
    };
    book.place_stop_with_reservation(stop, LogicalTime::new(1), 9, stop_state)
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
fn amend_reduce_keeps_fifo_priority_and_reservation_version() {
    let mut book = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    book.rest_with_reservation(input.clone(), reservation(&input, 7))
        .unwrap();
    let mut command = amend(&input);
    command.new_qty = Some(Qty::try_new(8).unwrap());

    let events = book
        .amend_regular_with_events(&command, LogicalTime::new(1), 9, &[])
        .unwrap();

    assert_eq!(
        events,
        vec![EngineEvent::OrderAmended {
            order_id: input.order_id,
            old_price: Some(Price::try_new(100).unwrap()),
            new_price: Some(Price::try_new(100).unwrap()),
            old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(10).unwrap()),
            new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(8).unwrap()),
            priority_retained: true,
        }]
    );
    assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
    assert_eq!(book.orders[0].total_remaining(), 8);
    assert_eq!(book.next_priority, 1);
    assert_eq!(book.reservation_state(input.order_id).unwrap().version, 7);
}

#[test]
fn amend_reduce_after_partial_fill_rebases_the_next_absolute_target() {
    let mut book = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    book.rest_with_reservation(input.clone(), reservation(&input, 7))
        .unwrap();
    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();
    let mut reduce = amend(&input);
    reduce.new_qty = Some(Qty::try_new(8).unwrap());

    book.amend_regular_with_events(&reduce, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert_eq!(
        book.orders[0].original_order().qty,
        Qty::try_new(8).unwrap()
    );
    assert_eq!(book.orders[0].total_remaining(), 5);

    let mut increase = amend(&input);
    increase.new_qty = Some(Qty::try_new(9).unwrap());
    let events = book
        .amend_regular_with_events(&increase, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert!(matches!(
        events.as_slice(),
        [EngineEvent::OrderAmended {
            new_quantity: AmendedQuantity::BaseRemaining(remaining),
            ..
        }] if *remaining == Qty::try_new(6).unwrap()
    ));
    assert_eq!(book.orders[0].total_remaining(), 6);
}

#[test]
fn amend_increase_requeues_at_same_price_tail_with_new_priority() {
    let mut book = ReferenceOrderBook::new();
    let first = order(1, 11, Side::Buy, 100, 10);
    let second = order(2, 12, Side::Buy, 100, 10);
    for input in [&first, &second] {
        book.rest_with_reservation(input.clone(), reservation(input, 4))
            .unwrap();
    }
    let mut command = amend(&first);
    command.new_qty = Some(Qty::try_new(12).unwrap());

    let events = book
        .amend_regular_with_events(&command, LogicalTime::new(1), 9, &[])
        .unwrap();

    assert!(matches!(
        events.as_slice(),
        [EngineEvent::OrderAmended {
            priority_retained: false,
            ..
        }]
    ));
    assert_eq!(
        book.orders
            .iter()
            .map(|entry| (entry.original_order().order_id, entry.priority()))
            .collect::<Vec<_>>(),
        vec![
            (second.order_id, QueuePriority::new(1)),
            (first.order_id, QueuePriority::new(2)),
        ]
    );
    assert_eq!(book.reservation_state(first.order_id).unwrap().version, 4);
    assert_eq!(book.next_priority, 3);
}

#[test]
fn amend_reprice_emits_amend_then_trades_then_order_events() {
    let mut book = ReferenceOrderBook::new();
    let maker = order(1, 12, Side::Buy, 101, 3);
    let target = order(2, 11, Side::Sell, 105, 5);
    book.rest_with_reservation(maker.clone(), reservation(&maker, 2))
        .unwrap();
    book.rest_with_reservation(target.clone(), reservation(&target, 7))
        .unwrap();
    let mut command = amend(&target);
    command.new_price = Some(Price::try_new(100).unwrap());

    let events = book
        .amend_regular_with_events(&command, LogicalTime::new(1), 9, &[TradeId::new(99)])
        .unwrap();

    assert_eq!(events.len(), 3);
    assert!(matches!(
        events[0],
        EngineEvent::OrderAmended {
            priority_retained: false,
            ..
        }
    ));
    assert!(
        matches!(events[1], EngineEvent::Trade(ref trade) if trade.trade_id == TradeId::new(99)
        && trade.maker_order_id == maker.order_id && trade.taker_order_id == target.order_id
        && trade.qty == Qty::try_new(3).unwrap())
    );
    assert_eq!(
        events[2],
        EngineEvent::OrderPartiallyFilled {
            order_id: target.order_id,
            remaining: Qty::try_new(2).unwrap(),
        }
    );
    assert_eq!(book.orders.len(), 1);
    assert_eq!(book.orders[0].original_order().order_id, target.order_id);
    assert_eq!(
        book.orders[0].original_order().price,
        Price::try_new(100).unwrap()
    );
    assert_eq!(book.orders[0].total_remaining(), 2);
    assert_eq!(book.orders[0].priority(), QueuePriority::new(2));
    assert_eq!(book.reservation_state(target.order_id).unwrap().version, 7);
    assert!(book.reservation_state(maker.order_id).is_none());
}

#[test]
fn amend_reprice_insufficient_trade_ids_panics_before_any_deterministic_state_change() {
    let mut book = ReferenceOrderBook::new();
    let maker = order(1, 12, Side::Buy, 101, 3);
    let target = order(2, 11, Side::Sell, 105, 5);
    book.rest_with_reservation(maker.clone(), reservation_with_reserve(&maker, 2))
        .unwrap();
    book.rest_with_reservation(target.clone(), reservation_with_reserve(&target, 7))
        .unwrap();
    seed_unrelated_deterministic_state(&mut book);
    let before = snapshot(&book);
    let mut command = amend(&target);
    command.new_price = Some(Price::try_new(100).unwrap());

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.amend_regular_with_events(&command, LogicalTime::new(1), 9, &[]);
    }));

    assert!(result.is_err());
    assert_eq!(snapshot(&book), before);
}

#[test]
fn amend_reprice_full_fill_clears_shadow_reservation_without_zero_qty_construction() {
    let mut book = ReferenceOrderBook::new();
    let maker = order(1, 12, Side::Buy, 101, 5);
    let target = order(2, 11, Side::Sell, 105, 5);
    book.rest_with_reservation(maker.clone(), reservation(&maker, 2))
        .unwrap();
    book.rest_with_reservation(target.clone(), reservation(&target, 7))
        .unwrap();
    let mut command = amend(&target);
    command.new_price = Some(Price::try_new(100).unwrap());

    let events = book
        .amend_regular_with_events(&command, LogicalTime::new(1), 9, &[TradeId::new(99)])
        .unwrap();

    assert!(matches!(events.as_slice(), [
        EngineEvent::OrderAmended { .. },
        EngineEvent::Trade(_),
        EngineEvent::OrderFilled { order_id },
    ] if *order_id == target.order_id));
    assert!(book.orders.is_empty());
    assert!(book.reservation_state(target.order_id).is_none());
    assert_eq!(book.next_priority, 2);
}

#[test]
fn amend_tif_migration_keeps_priority_and_rejects_invalid_time_atomically() {
    let mut book = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    book.rest_with_reservation(input.clone(), reservation(&input, 3))
        .unwrap();
    seed_unrelated_deterministic_state(&mut book);
    let mut gtd = amend(&input);
    gtd.new_tif = Some(TimeInForce::Gtd {
        expire_at: LogicalTime::new(2),
    });

    book.amend_regular_with_events(&gtd, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert_eq!(book.orders[0].time_in_force(), gtd.new_tif.unwrap());
    assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
    assert_eq!(book.reservation_state(input.order_id).unwrap().version, 3);

    let before = snapshot(&book);
    let mut invalid = amend(&input);
    invalid.new_tif = Some(TimeInForce::Day { session_id: 8 });
    assert_eq!(
        book.amend_regular_with_events(&invalid, LogicalTime::new(1), 9, &[]),
        Err(RejectReason::InvalidSession)
    );
    assert_eq!(snapshot(&book), before);
}

#[test]
fn amend_day_tif_migration_enters_existing_session_expiry_path() {
    let mut book = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    book.rest_with_reservation(input.clone(), reservation(&input, 3))
        .unwrap();
    let mut command = amend(&input);
    command.new_tif = Some(TimeInForce::Day { session_id: 9 });

    book.amend_regular_with_events(&command, LogicalTime::new(1), 9, &[])
        .unwrap();
    assert_eq!(
        book.orders[0].time_in_force(),
        TimeInForce::Day { session_id: 9 }
    );
    assert_eq!(book.orders[0].priority(), QueuePriority::new(0));

    let expired = book.end_session(9, 9).unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].order_id, input.order_id);
    assert!(book.reservation_state(input.order_id).is_none());
}

#[test]
fn amend_regular_priority_failure_precedes_target_aggregate_failure() {
    for reprice in [false, true] {
        let mut book = ReferenceOrderBook::new();
        let input = order(1, 11, Side::Buy, 90, 1);
        let target_price = if reprice { 100 } else { 90 };
        let peer = order(2, 12, Side::Buy, target_price, u64::MAX - 1);
        book.rest_with_reservation(input.clone(), reservation(&input, 3))
            .unwrap();
        book.rest_with_reservation(peer.clone(), reservation(&peer, 4))
            .unwrap();
        book.next_priority = u128::MAX - PRIORITY_GUARD;
        let before = snapshot(&book);
        let mut command = amend(&input);
        command.new_qty = Some(Qty::try_new(2).unwrap());
        command.new_price = Some(Price::try_new(target_price.try_into().unwrap()).unwrap());
        assert_eq!(
            book.amend_regular_with_events(&command, LogicalTime::new(1), 9, &[]),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(snapshot(&book), before);
    }
}

#[test]
fn amend_requeue_priority_exhaustion_is_atomic_but_full_reprice_needs_no_priority() {
    let mut book = ReferenceOrderBook::new();
    let maker = order(2, 12, Side::Buy, 101, 5);
    let target = order(1, 11, Side::Sell, 105, 5);
    book.rest_with_reservation(maker.clone(), reservation(&maker, 2))
        .unwrap();
    book.rest_with_reservation(target.clone(), reservation(&target, 3))
        .unwrap();
    book.next_priority = u128::MAX - PRIORITY_GUARD;
    let before = snapshot(&book);
    let mut requeue = amend(&target);
    requeue.new_qty = Some(Qty::try_new(6).unwrap());
    assert_eq!(
        book.amend_regular_with_events(&requeue, LogicalTime::new(1), 9, &[]),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(snapshot(&book), before);

    let mut reprice = amend(&target);
    reprice.new_price = Some(Price::try_new(100).unwrap());

    let events = book
        .amend_regular_with_events(&reprice, LogicalTime::new(1), 9, &[TradeId::new(8)])
        .unwrap();
    assert!(
        matches!(events.last(), Some(EngineEvent::OrderFilled { order_id }) if *order_id == target.order_id)
    );
    assert_eq!(book.next_priority, u128::MAX - PRIORITY_GUARD);
    assert!(book.reservation_state(target.order_id).is_none());
}

#[test]
fn amend_missing_shadow_or_target_equal_filled_is_atomic() {
    let mut missing = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    missing.rest(input.clone()).unwrap();
    let before = snapshot(&missing);
    assert_eq!(
        missing.amend_regular_with_events(&amend(&input), LogicalTime::new(1), 9, &[]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(snapshot(&missing), before);

    let mut book = ReferenceOrderBook::new();
    book.rest_with_reservation(input.clone(), reservation(&input, 5))
        .unwrap();
    book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();
    let before = snapshot(&book);
    let mut command = amend(&input);
    command.new_qty = Some(Qty::try_new(3).unwrap());
    assert_eq!(
        book.amend_regular_with_events(&command, LogicalTime::new(1), 9, &[]),
        Err(RejectReason::InvalidQty)
    );
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.reservation_state(input.order_id).unwrap().version, 5);
}

#[test]
fn amend_unverified_reservation_token_rejects_atomically_until_task8() {
    let mut book = ReferenceOrderBook::new();
    let input = order(1, 11, Side::Buy, 100, 10);
    let state = reservation(&input, 5);
    book.rest_with_reservation(input.clone(), state.clone())
        .unwrap();
    seed_unrelated_deterministic_state(&mut book);
    let before = snapshot(&book);
    let mut command = amend(&input);
    command.reservation_adjustment = Some(ReservationAdjustmentToken {
        token_id: 1,
        command_id: CommandId::new(2),
        reservation_id: state.reservation_id,
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        expected_reservation_version: state.version,
        target_reserves: Default::default(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    });

    assert_eq!(
        book.amend_regular_with_events(&command, LogicalTime::new(1), 9, &[]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(snapshot(&book), before);
    assert_eq!(book.reservation_state(input.order_id), Some(&state));
}
