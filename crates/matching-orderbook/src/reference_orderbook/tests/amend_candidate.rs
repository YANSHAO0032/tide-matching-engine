//! 参考簿改单候选的只读构造边界。

use super::*;
use matching_domain::{
    AmendOrder, IcebergOrder, LimitOrder, LogicalTime, MarketId, MarketMath, QueuePriority,
    RegularAmendKind, ReservationId, StopLimitOrder, StopOrder, StopPriority, TimeInForce,
    TriggerExpiry, UserId,
};
use matching_risk::ReservationState;

fn reservation(order_id: OrderId, user_id: UserId) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(order_id.get()),
        order_id,
        user_id,
        market_id: MarketId::new(7),
        version: 3,
        status: matching_risk::ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 0,
        reserves: Vec::new(),
    }
}

fn amend(order_id: OrderId, user_id: UserId, new_qty: Qty) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id,
        order_id,
        new_price: None,
        new_qty: Some(new_qty),
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
struct BookSnapshot {
    /// 普通订单原始载荷、剩余量、峰值、优先级和有效期的有序快照。
    regular: Vec<(
        LimitOrder,
        u64,
        u64,
        Option<Qty>,
        QueuePriority,
        TimeInForce,
    )>,
    /// 未触发止损订单及其优先级的有序快照。
    stops: Vec<(StopOrder, StopPriority)>,
    /// 活动订单到金融预留影子的有序快照。
    reservations: Vec<(OrderId, ReservationState)>,
    /// 下一普通队列优先级计数。
    next_priority: u128,
    /// 下一止损优先级计数。
    next_stop_priority: u128,
}

fn snapshot(book: &ReferenceOrderBook) -> BookSnapshot {
    let regular = book
        .orders
        .iter()
        .map(|order| {
            (
                order.original_order().clone(),
                order.visible_remaining(),
                order.hidden_remaining(),
                order.peak_size(),
                order.priority(),
                order.time_in_force(),
            )
        })
        .collect();
    let stops = book
        .stop_orders
        .iter()
        .map(|stop| (stop.order.clone(), stop.priority))
        .collect();
    let mut reservations = book
        .reservations
        .iter()
        .map(|(order_id, state)| (*order_id, state.clone()))
        .collect::<Vec<_>>();
    reservations.sort_by_key(|(order_id, _)| *order_id);
    BookSnapshot {
        regular,
        stops,
        reservations,
        next_priority: book.next_priority,
        next_stop_priority: book.next_stop_priority,
    }
}

#[test]
fn regular_candidate_is_read_only_before_any_amend_commit_exists() {
    let mut book = ReferenceOrderBook::new();
    let input = LimitOrder {
        order_id: OrderId::new(11),
        user_id: UserId::new(22),
        side: Side::Buy,
        price: Price::try_new(90).unwrap(),
        qty: Qty::try_new(10).unwrap(),
    };
    book.rest_with_reservation(input.clone(), reservation(input.order_id, input.user_id))
        .unwrap();

    let before = snapshot(&book);
    let plan = book
        .build_amend_plan(
            &amend(input.order_id, input.user_id, Qty::try_new(8).unwrap()),
            LogicalTime::new(1),
            1,
        )
        .unwrap();

    assert!(matches!(
        plan,
        super::super::amend_candidate::ReferenceAmendPlan::Regular(plan)
            if plan.old_remaining == Qty::try_new(10).unwrap()
                && plan.new_remaining == Qty::try_new(8).unwrap()
                && plan.kind == RegularAmendKind::InPlaceReduce
                && plan.reservation.version == 3
    ));
    assert_eq!(snapshot(&book), before);
}

#[test]
fn iceberg_and_stop_candidates_keep_every_observable_state_unchanged() {
    let mut book = ReferenceOrderBook::new();
    let iceberg_input = LimitOrder {
        order_id: OrderId::new(12),
        user_id: UserId::new(23),
        side: Side::Sell,
        price: Price::try_new(110).unwrap(),
        qty: Qty::try_new(10).unwrap(),
    };
    let iceberg = IcebergOrder::try_new(iceberg_input.clone(), Qty::try_new(3).unwrap()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    book.place_iceberg_post_only_with_reservation(
        iceberg,
        &math,
        reservation(iceberg_input.order_id, iceberg_input.user_id),
    )
    .unwrap();
    let stop = StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(13),
        user_id: UserId::new(24),
        reservation_id: ReservationId::new(13),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        price: Price::try_new(101).unwrap(),
        qty: Qty::try_new(8).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: TriggerExpiry::Gtc,
    });
    book.place_stop_with_reservation(
        stop.clone(),
        LogicalTime::new(1),
        1,
        reservation(stop.order_id(), UserId::new(24)),
    )
    .unwrap();
    let before = snapshot(&book);

    let mut iceberg_amend = amend(
        iceberg_input.order_id,
        iceberg_input.user_id,
        Qty::try_new(9).unwrap(),
    );
    iceberg_amend.new_visible_qty = Some(Qty::try_new(2).unwrap());
    assert!(matches!(
        book.build_amend_plan(&iceberg_amend, LogicalTime::new(1), 1),
        Ok(super::super::amend_candidate::ReferenceAmendPlan::Iceberg(plan))
            if plan.new_peak == Qty::try_new(2).unwrap()
                && plan.regular.new_remaining == Qty::try_new(9).unwrap()
    ));
    assert!(matches!(
        book.build_amend_plan(
            &amend(stop.order_id(), UserId::new(24), Qty::try_new(7).unwrap()),
            LogicalTime::new(1),
            1,
        ),
        Ok(super::super::amend_candidate::ReferenceAmendPlan::Stop(plan))
            if plan.priority == StopPriority::new(0)
                && plan.new_stop_price == Price::try_new(100).unwrap()
    ));
    assert_eq!(snapshot(&book), before);
}

#[test]
fn missing_or_wrong_quantity_mode_rejects_without_reference_state_drift() {
    let mut book = ReferenceOrderBook::new();
    let input = LimitOrder {
        order_id: OrderId::new(14),
        user_id: UserId::new(25),
        side: Side::Buy,
        price: Price::try_new(90).unwrap(),
        qty: Qty::try_new(10).unwrap(),
    };
    book.rest(input.clone()).unwrap();
    let before = snapshot(&book);
    assert_eq!(
        book.build_amend_plan(
            &amend(input.order_id, input.user_id, Qty::try_new(8).unwrap()),
            LogicalTime::new(1),
            1,
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(snapshot(&book), before);

    let removed = book.cancel(input.order_id).unwrap();
    assert_eq!(removed.original_order(), &input);
    book.rest_with_reservation(input.clone(), reservation(input.order_id, input.user_id))
        .unwrap();
    let before = snapshot(&book);
    let mut wrong_mode = amend(input.order_id, input.user_id, Qty::try_new(8).unwrap());
    wrong_mode.new_quote_budget = Some(QuoteAmount::new(10));
    assert_eq!(
        book.build_amend_plan(&wrong_mode, LogicalTime::new(1), 1),
        Err(RejectReason::InvalidOrderCombination),
    );
    assert_eq!(snapshot(&book), before);
}

#[test]
fn terminal_or_unknown_order_amend_reports_order_not_found_before_reservation_lookup() {
    let mut book = ReferenceOrderBook::new();

    let assert_missing = |book: &ReferenceOrderBook, order_id: OrderId| {
        let before = snapshot(book);
        assert_eq!(
            book.build_amend_plan(
                &amend(
                    order_id,
                    UserId::new(order_id.get() as u64),
                    Qty::try_new(1).unwrap()
                ),
                LogicalTime::new(20),
                1,
            ),
            Err(RejectReason::OrderNotFound),
        );
        assert_eq!(snapshot(book), before);
    };

    assert_missing(&book, OrderId::new(1));

    let canceled = LimitOrder {
        order_id: OrderId::new(2),
        user_id: UserId::new(2),
        side: Side::Buy,
        price: Price::try_new(90).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    book.rest_with_reservation(
        canceled.clone(),
        reservation(canceled.order_id, canceled.user_id),
    )
    .unwrap();
    book.cancel(canceled.order_id).unwrap();
    assert_missing(&book, canceled.order_id);

    let filled = LimitOrder {
        order_id: OrderId::new(3),
        user_id: UserId::new(3),
        side: Side::Sell,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    book.rest_with_reservation(filled.clone(), reservation(filled.order_id, filled.user_id))
        .unwrap();
    let mut incoming = IncomingOrder::new(LimitOrder {
        order_id: OrderId::new(4),
        user_id: UserId::new(4),
        side: Side::Buy,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    });
    book.execute_next_fill(&mut incoming).unwrap();
    assert_missing(&book, filled.order_id);

    let expired = LimitOrder {
        order_id: OrderId::new(5),
        user_id: UserId::new(5),
        side: Side::Buy,
        price: Price::try_new(90).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    };
    book.rest_timed_with_reservation(
        expired.clone(),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(10),
        1,
        reservation(expired.order_id, expired.user_id),
    )
    .unwrap();
    book.expire_gtd_through(LogicalTime::new(20));
    assert_missing(&book, expired.order_id);
}
