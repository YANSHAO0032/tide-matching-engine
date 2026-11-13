use matching_domain::{
    CancelReason, CircuitBreakerReason, MarketStateReason, RejectReason, ReservationReleaseReason,
};

#[test]
fn control_and_revalidation_reason_codes_are_stable() {
    assert_eq!(CancelReason::KillSwitch as u16, 1105);
    assert_eq!(RejectReason::KillSwitchActive as u16, 1007);
    assert_eq!(CancelReason::PriceBandRevalidation as u16, 1113);
    assert_eq!(ReservationReleaseReason::KillSwitch as u16, 1308);
    assert_eq!(ReservationReleaseReason::PriceBandRevalidation as u16, 1311);
    assert_eq!(MarketStateReason::CircuitBreaker as u16, 1403);
    assert_eq!(MarketStateReason::KillSwitch as u16, 1404);
    assert_eq!(CircuitBreakerReason::PriceDeviation as u16, 1501);
    assert_eq!(CircuitBreakerReason::ReferenceUnavailable as u16, 1502);
    assert_eq!(CircuitBreakerReason::VolatilityLimit as u16, 1503);
    assert_eq!(CircuitBreakerReason::InternalSafety as u16, 1504);
    assert_eq!(CircuitBreakerReason::ManualRiskSignal as u16, 1505);
}

#[test]
fn kill_switch_rejection_decodes_from_durable_snapshot() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, decode_reject_reason};
    let bytes = 1007_u16.to_be_bytes();
    let mut reader = SnapshotDecoder::new(&bytes, 2).unwrap();
    assert_eq!(
        decode_reject_reason(&mut reader).unwrap(),
        RejectReason::KillSwitchActive
    );
    reader.finish().unwrap();
}
