//! 验证参考模型使用扫描和排序完成普通挂单到期。

use super::*;
use matching_domain::{ExpireReason, ExpiredOrder, LogicalTime, TimeInForce, UserId};

fn order(id: u128) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side: Side::Buy,
        price: Price::try_new(100 - id as i64).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    }
}

#[test]
fn scanned_gtd_expiry_uses_canonical_time_then_order_id_order() {
    let mut book = ReferenceOrderBook::new();
    let now = LogicalTime::new(10);
    for (id, expire_at) in [(30, 100), (40, 90), (10, 100)] {
        book.rest_timed(
            order(id),
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(expire_at),
            },
            now,
            7,
        )
        .unwrap();
    }
    book.rest(order(50)).unwrap();

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
                order_id: OrderId::new(30),
                reason: ExpireReason::GtdReached,
            },
        ]
    );
    assert_eq!(
        book.orders(Side::Buy)[0].original_order().order_id,
        OrderId::new(50)
    );
}
