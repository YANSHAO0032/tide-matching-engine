//! 生产簿改单候选的只读构造边界。

use super::*;
use matching_domain::{
    AmendOrder, IcebergOrder, LogicalTime, MarketId, MarketMath, RegularAmendKind, ReservationId,
    StopLimitOrder, StopOrder, StopPriority, TimeInForce, TriggerExpiry,
};
use matching_risk::ReservationState;

fn reservation(order_id: OrderId, user_id: UserId) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(order_id.get()),
        order_id,
        user_id,
        market_id: MarketId::new(7),
        origin_session_id: matching_domain::OriginSessionId::new(1),
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
    /// 买方价格档与其队列顺序快照。
    bids: Vec<LevelSnapshotEntry>,
    /// 卖方价格档与其队列顺序快照。
    asks: Vec<LevelSnapshotEntry>,
    /// 指定订单身份对应的节点快照。
    nodes: Vec<NodeSnapshotEntry>,
    /// 活动订单到金融预留影子的有序快照。
    reservations: Vec<(OrderId, ReservationState)>,
    /// 未触发止损订单及其优先级的有序快照。
    stops: Vec<(StopOrder, StopPriority)>,
    /// 下一普通队列优先级计数。
    next_priority: u128,
    /// 下一止损优先级计数。
    next_stop_priority: u128,
}

fn snapshot(book: &ProductionOrderBook, ids: &[OrderId]) -> BookSnapshot {
    let mut reservations = book
        .reservations
        .iter()
        .map(|(order_id, state)| (*order_id, state.clone()))
        .collect::<Vec<_>>();
    reservations.sort_by_key(|(order_id, _)| *order_id);
    let stops = [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| {
            book.stop_orders(side)
                .into_iter()
                .map(|(order, priority)| (order.clone(), priority))
        })
        .collect();
    BookSnapshot {
        bids: level_snapshot(&book.bids),
        asks: level_snapshot(&book.asks),
        nodes: node_snapshot(book, ids),
        reservations,
        stops,
        next_priority: book.next_queue_priority(),
        next_stop_priority: book.next_stop_priority(),
    }
}

#[test]
fn regular_candidate_is_read_only_before_any_amend_commit_exists() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(11, Side::Buy, 90, 10);
    book.rest_with_reservation(input.clone(), reservation(input.order_id, input.user_id))
        .unwrap();

    let before = snapshot(&book, &[input.order_id]);
    let plan = book
        .build_amend_plan(
            &amend(input.order_id, input.user_id, Qty::try_new(8).unwrap()),
            LogicalTime::new(1),
            1,
        )
        .unwrap();

    assert!(matches!(
        plan,
        super::super::amend_candidate::ProductionAmendPlan::Regular(plan)
            if plan.old_remaining == Qty::try_new(10).unwrap()
                && plan.new_remaining == Qty::try_new(8).unwrap()
                && plan.kind == RegularAmendKind::InPlaceReduce
                && plan.reservation.version == 3
    ));
    assert_eq!(snapshot(&book, &[input.order_id]), before);
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
}

#[test]
fn iceberg_and_stop_candidates_keep_every_observable_state_unchanged() {
    let mut book = ProductionOrderBook::new(policy());
    let iceberg_input = order(12, Side::Sell, 110, 10);
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
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
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
    let before = snapshot(&book, &[iceberg_input.order_id]);

    let mut iceberg_amend = amend(
        iceberg_input.order_id,
        iceberg_input.user_id,
        Qty::try_new(9).unwrap(),
    );
    iceberg_amend.new_visible_qty = Some(Qty::try_new(2).unwrap());
    assert!(matches!(
        book.build_amend_plan(&iceberg_amend, LogicalTime::new(1), 1),
        Ok(super::super::amend_candidate::ProductionAmendPlan::Iceberg(plan))
            if plan.new_peak == Qty::try_new(2).unwrap()
                && plan.regular.new_remaining == Qty::try_new(9).unwrap()
    ));
    assert!(matches!(
        book.build_amend_plan(
            &amend(stop.order_id(), UserId::new(24), Qty::try_new(7).unwrap()),
            LogicalTime::new(1),
            1,
        ),
        Ok(super::super::amend_candidate::ProductionAmendPlan::Stop(plan))
            if plan.priority == StopPriority::new(0)
                && plan.new_stop_price == Price::try_new(100).unwrap()
    ));
    assert_eq!(snapshot(&book, &[iceberg_input.order_id]), before);
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
}

#[test]
fn missing_or_wrong_quantity_mode_rejects_without_production_state_drift() {
    let mut book = ProductionOrderBook::new(policy());
    let input = order(14, Side::Buy, 90, 10);
    book.rest(input.clone()).unwrap();
    let before = snapshot(&book, &[input.order_id]);
    assert_eq!(
        book.build_amend_plan(
            &amend(input.order_id, input.user_id, Qty::try_new(8).unwrap()),
            LogicalTime::new(1),
            1,
        ),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(snapshot(&book, &[input.order_id]), before);
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));

    let removed = book.cancel(input.order_id).unwrap();
    assert_eq!(removed.original_order(), &input);
    book.rest_with_reservation(input.clone(), reservation(input.order_id, input.user_id))
        .unwrap();
    let before = snapshot(&book, &[input.order_id]);
    let mut wrong_mode = amend(input.order_id, input.user_id, Qty::try_new(8).unwrap());
    wrong_mode.new_quote_budget = Some(QuoteAmount::new(10));
    assert_eq!(
        book.build_amend_plan(&wrong_mode, LogicalTime::new(1), 1),
        Err(RejectReason::InvalidOrderCombination),
    );
    assert_eq!(snapshot(&book, &[input.order_id]), before);
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
}

#[test]
fn terminal_or_unknown_order_amend_reports_order_not_found_before_reservation_lookup() {
    let mut book = ProductionOrderBook::new(policy());

    let assert_missing = |book: &ProductionOrderBook, order_id: OrderId| {
        let before = snapshot(book, &[]);
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
        assert_eq!(snapshot(book, &[]), before);
    };

    assert_missing(&book, OrderId::new(1));

    let canceled = order(2, Side::Buy, 90, 1);
    book.rest_with_reservation(
        canceled.clone(),
        reservation(canceled.order_id, canceled.user_id),
    )
    .unwrap();
    book.cancel(canceled.order_id).unwrap();
    assert_missing(&book, canceled.order_id);

    let filled = order(3, Side::Sell, 100, 1);
    book.rest_with_reservation(filled.clone(), reservation(filled.order_id, filled.user_id))
        .unwrap();
    let mut incoming = IncomingOrder::new(order(4, Side::Buy, 100, 1));
    book.execute_next_fill_trade(&mut incoming, TradeId::new(1))
        .unwrap();
    assert_missing(&book, filled.order_id);

    let expired = order(5, Side::Buy, 90, 1);
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
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
    assert_eq!(book.validate_regular_expiry(), Ok(()));
}
