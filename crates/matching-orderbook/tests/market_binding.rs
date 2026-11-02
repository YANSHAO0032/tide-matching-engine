use matching_domain::MarketId;
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};

#[test]
fn production_book_market_binding_is_immutable() {
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    assert_eq!(book.bind_market(MarketId(1)), Ok(()));
    assert!(book.bind_market(MarketId(2)).is_err());
    assert_eq!(book.market_id(), Some(MarketId(1)));
}

#[test]
fn two_bound_books_share_one_registry_without_cross_market_aliasing() {
    use matching_domain::{
        AssetId, AssetReserve, LimitOrder, OrderId, OriginSessionId, Price, Qty, ReservationId,
        Side, StpKey, StpMode, UserId,
    };
    use matching_risk::{ReservationRegistry, ReservationState, ReservationStatus};
    fn pair(id: u128, market: u32) -> (LimitOrder, ReservationState) {
        (
            LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(1),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            ReservationState {
                reservation_id: ReservationId(id),
                order_id: OrderId(id),
                user_id: UserId(1),
                market_id: MarketId(market),
                origin_session_id: OriginSessionId(1),
                version: 0,
                reserves: vec![AssetReserve {
                    asset: AssetId(1),
                    atoms: 100,
                }],
                status: ReservationStatus::Active,
                fee_schedule_version: 1,
                fee_tier_id: 1,
            },
        )
    }
    let (a, sa) = pair(1, 1);
    let (b, sb) = pair(2, 2);
    let mut registry = ReservationRegistry::new([sa.clone(), sb.clone()]).unwrap();
    let mut first = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    let mut second = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    first.bind_market(MarketId(1)).unwrap();
    second.bind_market(MarketId(2)).unwrap();
    assert!(first.rest_with_reservation(b.clone(), sb.clone()).is_err());
    first.rest_with_reservation(a, sa).unwrap();
    second.rest_with_reservation(b, sb).unwrap();
    let first_states = first.reservation_snapshots();
    let second_states = second.reservation_snapshots();
    registry
        .validate_market_order_bindings(MarketId(1), first_states.iter().copied())
        .unwrap();
    registry
        .validate_market_order_bindings(MarketId(2), second_states.iter().copied())
        .unwrap();
    registry
        .validate_order_bindings(first_states.into_iter().chain(second_states))
        .unwrap();
    let plan = first
        .plan_mass_cancel(
            matching_domain::ShardCancelScope::Market {
                market_id: MarketId(1),
            },
            &registry,
        )
        .unwrap();
    let (result, events) = first.apply_mass_cancel(&mut registry, plan).unwrap();
    assert_eq!(result.canceled_orders, 1);
    assert_eq!(events.len(), 1);
    assert!(registry.is_pending_r2(ReservationId(1)));
    assert!(!registry.is_pending_r2(ReservationId(2)));
    assert!(second.order(OrderId(2)).is_some());
    registry
        .validate_order_bindings(second.reservation_snapshots())
        .unwrap();
}
