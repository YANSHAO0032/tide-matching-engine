//! 验证普通挂单的逻辑到期与会话终结，不覆盖 Stop 触发逻辑。

use super::*;
use matching_domain::{
    ExpireReason, ExpiredOrder, LogicalTime, ReservationId, StopLimitOrder, StopOrder, TimeInForce,
    TriggerExpiry,
};

fn stop_limit(id: u128, expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        reservation_id: ReservationId::new(id + 1_000),
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(110).unwrap(),
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: expiry,
    })
}

#[test]
fn gtd_expiry_uses_time_then_order_id_and_preserves_surviving_fifo() {
    let mut book = ProductionOrderBook::new(policy());
    let now = LogicalTime::new(10);

    book.rest_timed(
        order(30, Side::Buy, 90, 1),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        },
        now,
        7,
    )
    .unwrap();
    book.rest_timed(
        order(40, Side::Buy, 89, 1),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(90),
        },
        now,
        7,
    )
    .unwrap();
    book.rest_timed(
        order(10, Side::Buy, 88, 1),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        },
        now,
        7,
    )
    .unwrap();
    book.rest(order(50, Side::Buy, 87, 1)).unwrap();
    book.place_stop(
        stop_limit(
            20,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(100),
            },
        ),
        now,
        7,
    )
    .unwrap();

    assert_eq!(
        book.expire_gtd_through(LogicalTime::new(100)),
        vec![
            ExpiredOrder {
                order_id: OrderId::new(40),
                reason: ExpireReason::GtdReached,
            },
            ExpiredOrder {
                order_id: OrderId::new(10),
                reason: ExpireReason::GtdReached,
            },
            ExpiredOrder {
                order_id: OrderId::new(20),
                reason: ExpireReason::StopTriggerGtdReached,
            },
            ExpiredOrder {
                order_id: OrderId::new(30),
                reason: ExpireReason::GtdReached,
            },
        ]
    );
    assert!(book.order(OrderId::new(30)).is_none());
    assert!(book.order(OrderId::new(40)).is_none());
    assert!(book.order(OrderId::new(10)).is_none());
    assert_eq!(
        book.order(OrderId::new(50)).unwrap().priority(),
        QueuePriority::new(3)
    );
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn end_session_expires_regular_and_untriggered_stop_day_orders_by_order_id() {
    let mut book = ProductionOrderBook::new(policy());
    let now = LogicalTime::new(10);
    book.rest_timed(
        order(30, Side::Buy, 90, 1),
        TimeInForce::Day { session_id: 7 },
        now,
        7,
    )
    .unwrap();
    book.place_stop(stop_limit(10, TriggerExpiry::Day { session_id: 7 }), now, 7)
        .unwrap();
    book.rest(order(40, Side::Buy, 80, 1)).unwrap();

    assert_eq!(
        book.end_session(7, 7),
        Ok(vec![
            ExpiredOrder {
                order_id: OrderId::new(10),
                reason: ExpireReason::StopTriggerSessionEnded,
            },
            ExpiredOrder {
                order_id: OrderId::new(30),
                reason: ExpireReason::SessionEnded,
            },
        ])
    );
    assert!(book.order(OrderId::new(40)).is_some());
    assert_eq!(book.end_session(6, 7), Err(RejectReason::InvalidSession));
    assert_eq!(validate_book(&book), Ok(()));
    assert_eq!(book.validate_stop_book(), Ok(()));
}

#[test]
fn timed_rest_rejects_invalid_time_and_session_before_priority_allocation() {
    let mut book = ProductionOrderBook::new(policy());
    let now = LogicalTime::new(100);

    assert_eq!(
        book.rest_timed(
            order(1, Side::Buy, 90, 1),
            TimeInForce::Gtd { expire_at: now },
            now,
            7,
        ),
        Err(RejectReason::InvalidExpireTime)
    );
    assert_eq!(
        book.rest_timed(
            order(2, Side::Buy, 90, 1),
            TimeInForce::Day { session_id: 8 },
            now,
            7,
        ),
        Err(RejectReason::InvalidSession)
    );
    assert_eq!(book.next_queue_priority(), 0);
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn fully_filled_gtd_maker_removes_its_secondary_expiry_reference() {
    let mut book = ProductionOrderBook::new(policy());
    let now = LogicalTime::new(10);
    book.rest_timed(
        order(1, Side::Sell, 100, 2),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(100),
        },
        now,
        7,
    )
    .unwrap();

    book.place_limit_gtc(order(2, Side::Buy, 100, 2), &[TradeId::new(1)])
        .unwrap();

    assert!(book.expire_gtd_through(LogicalTime::new(100)).is_empty());
    assert_eq!(validate_book(&book), Ok(()));
}
