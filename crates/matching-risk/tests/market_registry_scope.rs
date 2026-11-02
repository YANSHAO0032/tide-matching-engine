use matching_domain::{
    AssetId, AssetReserve, MarketId, OrderId, OriginSessionId, RejectReason, ReservationId, UserId,
};
use matching_risk::{ReservationRegistry, ReservationState, ReservationStatus};

fn state(id: u128, market: u32) -> ReservationState {
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
    }
}

#[test]
fn market_scope_requires_exact_market_bindings_and_preserves_other_market() {
    let a = state(1, 1);
    let b = state(2, 2);
    let registry = ReservationRegistry::new([a.clone(), b.clone()]).unwrap();
    assert_eq!(
        registry.validate_market_order_bindings(MarketId(1), [(a.order_id, &a)]),
        Ok(())
    );
    assert_eq!(
        registry.validate_market_order_bindings(MarketId(1), [(b.order_id, &b)]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.validate_market_order_bindings(MarketId(1), []),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.validate_order_bindings([(a.order_id, &a)]),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        registry.validate_order_bindings([(a.order_id, &a), (b.order_id, &b)]),
        Ok(())
    );
}
