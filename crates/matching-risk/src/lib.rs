//! 交易前校验、价格保护、资金预留和参考资金规则的规划边界。

mod reservation_adjustment;
mod reservation_transition;

pub use reservation_adjustment::{
    ExpectedAmendReservation, ReservationState, ReservationStatus, RiskKeySet,
    VerifiedReservationAdjustment, verify_amend_reservation,
};

pub use reservation_transition::{
    ExpectedNewReservation, ExpectedReplacementReservation, PlannedAmendAdjustment,
    PlannedBindingTransition, PlannedNewReservation, PlannedReservationTransition,
    ReservationApplyError, ReservationRegistry,
};

mod limit_reserve;
pub use limit_reserve::{LimitReserveError, LimitReservePolicy};

mod snapshot;
pub use snapshot::{decode_reservation_snapshot, encode_reservation_snapshot};
