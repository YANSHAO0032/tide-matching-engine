use super::*;
use crate::{StpKey, StpMode};

fn base(side: Side) -> MarketByBase {
    MarketByBase {
        order_id: OrderId::new(1),
        user_id: UserId::new(10),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        qty: Qty::try_new(100).unwrap(),
        market_protection: None,
    }
}

fn quote(side: Side) -> MarketByQuote {
    MarketByQuote {
        order_id: OrderId::new(2),
        user_id: UserId::new(20),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        quote_budget: QuoteBudget::try_new(1000).unwrap(),
        market_protection: None,
    }
}

#[test]
fn market_orders_support_both_sides_clone_and_debug() {
    for side in [Side::Buy, Side::Sell] {
        let base_order = base(side);
        let quote_order = quote(side);

        assert_eq!(base_order.clone(), base_order);
        assert_eq!(quote_order.clone(), quote_order);

        assert!(format!("{base_order:?}").contains("MarketByBase"));
        assert!(format!("{quote_order:?}").contains("MarketByQuote"));
    }
}
#[test]
fn market_by_base_has_value_semantics() {
    let original = base(Side::Buy);
    let same = base(Side::Buy);

    assert_eq!(original, same);

    let mut changed = original.clone();
    changed.order_id = OrderId::new(99);
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.user_id = UserId::new(99);
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.side = Side::Sell;
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.qty = Qty::try_new(200).unwrap();
    assert_ne!(original, changed);

    // 原始测试数据明确没有保护。
    assert_eq!(original.market_protection, None);
    assert_eq!(original, same);

    // None 与 Some 不相等。
    let mut changed = original.clone();
    changed.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: Some(Price::try_new(105).unwrap()),
    });
    assert_ne!(original, changed);

    // 不同保护值不相等，其余订单字段保持一致。
    let mut other = changed.clone();
    other.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(200),
        worst_price: Some(Price::try_new(105).unwrap()),
    });
    assert_ne!(changed, other);
}
#[test]
fn market_by_quote_has_value_semantics() {
    let original = quote(Side::Buy);
    let same = quote(Side::Buy);

    assert_eq!(original, same);

    let mut changed = original.clone();
    changed.order_id = OrderId::new(99);
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.user_id = UserId::new(99);
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.side = Side::Sell;
    assert_ne!(original, changed);

    let mut changed = original.clone();
    changed.quote_budget = QuoteBudget::try_new(2000).unwrap();
    assert_ne!(original, changed);

    // 原始测试数据明确没有保护。
    assert_eq!(original.market_protection, None);
    assert_eq!(original, same);

    // None 与 Some 不相等。
    let mut changed = original.clone();
    changed.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: Some(Price::try_new(105).unwrap()),
    });
    assert_ne!(original, changed);

    // 不同保护值不相等，其余订单字段保持一致。
    let mut other = changed.clone();
    other.market_protection = Some(MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: Some(Price::try_new(110).unwrap()),
    });
    assert_ne!(changed, other);
}
#[test]
fn market_by_quote_requires_positive_initial_budget() {
    assert!(QuoteBudget::try_new(0).is_err());

    let order = quote(Side::Buy);
    assert!(order.quote_budget.get() > 0);
}

#[test]
fn market_protection_supports_all_optional_configurations() {
    // 1. 两字段均为 None。
    let empty = MarketProtection {
        max_slippage_bps: None,
        worst_price: None,
    };
    assert_eq!(empty.max_slippage_bps, None);
    assert_eq!(empty.worst_price, None);

    // 2. 仅 max_slippage_bps，包含 Some(0)。
    for bps in [0, 100, 500] {
        let bps_only = MarketProtection {
            max_slippage_bps: Some(bps),
            worst_price: None,
        };
        assert_eq!(bps_only.max_slippage_bps, Some(bps));
        assert_eq!(bps_only.worst_price, None);
    }

    // 3. 仅 worst_price。
    let price = Price::try_new(105).unwrap();
    let price_only = MarketProtection {
        max_slippage_bps: None,
        worst_price: Some(price),
    };
    assert_eq!(price_only.max_slippage_bps, None);
    assert_eq!(price_only.worst_price, Some(price));

    // 4. 两字段同时存在。
    let complete = MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: Some(price),
    };
    assert_eq!(complete.max_slippage_bps, Some(100));
    assert_eq!(complete.worst_price, Some(price));
}

#[test]
fn market_protection_has_value_semantics_clone_and_debug() {
    let original = MarketProtection {
        max_slippage_bps: Some(100),
        worst_price: Some(Price::try_new(105).unwrap()),
    };

    // Clone 后相等。
    assert_eq!(original.clone(), original);

    // Debug 可用。
    assert!(format!("{original:?}").contains("MarketProtection"));

    // 修改基点后不相等。
    let mut changed = original.clone();
    changed.max_slippage_bps = Some(200);
    assert_ne!(original, changed);

    // 修改 worst_price 后不相等。
    let mut changed = original.clone();
    changed.worst_price = Some(Price::try_new(110).unwrap());
    assert_ne!(original, changed);

    // 基点: Some -> None 后不相等。
    let mut changed = original.clone();
    changed.max_slippage_bps = None;
    assert_ne!(original, changed);

    // worst_price: Some -> None 后不相等。
    let mut changed = original.clone();
    changed.worst_price = None;
    assert_ne!(original, changed);
}
