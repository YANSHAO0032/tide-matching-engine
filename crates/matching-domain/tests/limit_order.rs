use matching_domain::{LimitOrder, OrderId, Price, Qty, Side, UserId};

#[test]
fn crate_root_exports_strategy_neutral_limit_order_payload() {
    let order = LimitOrder {
        order_id: OrderId::new(41),
        user_id: UserId::new(7),
        stp_key: matching_domain::StpKey::new(0),
        stp_mode: matching_domain::StpMode::None,
        side: Side::Sell,
        price: Price::try_new(125).unwrap(),
        qty: Qty::try_new(9).unwrap(),
    };

    assert_eq!(order.order_id, OrderId::new(41));
    assert_eq!(order.user_id, UserId::new(7));
    assert_eq!(order.side, Side::Sell);
    assert_eq!(order.price, Price::try_new(125).unwrap());
    assert_eq!(order.qty, Qty::try_new(9).unwrap());
}
