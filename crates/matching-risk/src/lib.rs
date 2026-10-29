//! 交易前校验、价格保护、资金预留和参考资金规则的规划边界。

mod reservation_adjustment;
mod reservation_transition;

pub use reservation_adjustment::{
    ExpectedAmendReservation, ReservationState, ReservationStatus, RiskKeySet,
    VerifiedReservationAdjustment, verify_amend_reservation,
};

pub use reservation_transition::{
    ExpectedReplacementReservation, PlannedAmendAdjustment, PlannedBindingTransition,
    PlannedReservationTransition, ReservationApplyError, ReservationRegistry,
};
