//! 交易前校验、价格保护、资金预留和参考资金规则的规划边界。

mod reservation_adjustment;
mod reservation_transition;

pub use reservation_adjustment::{
    ExpectedAmendReservation, ReservationState, ReservationStatus, RiskKeySet,
    VerifiedReservationAdjustment, verify_amend_reservation,
};

pub use reservation_transition::{
    ExpectedNewReservation, ExpectedReplacementReservation, PlannedAmendAdjustment,
    PlannedBindingTransition, PlannedNewReservation, PlannedR2, PlannedReservationTransition,
    ReservationApplyError, ReservationRegistry, SettlementDebit, SettlementRelease,
};

mod limit_reserve;
pub use limit_reserve::{LimitReserveError, LimitReservePolicy};

mod snapshot;
pub use snapshot::{decode_reservation_snapshot, encode_reservation_snapshot};

mod asset_scale;
pub use asset_scale::{AssetScaleError, convert_asset_atoms};

pub use matching_domain::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};

mod fee_calculation;
pub use fee_calculation::{FeeCalculationError, calculate_fee};

mod trade_settlement;
pub use trade_settlement::{
    FeeAmount, SettlementAsset, SettlementPlanError, TradeSettlement, TradeSettlementContext,
    plan_trade_r2, plan_trade_settlement,
};

mod reference_ledger;
pub use reference_ledger::{ReferenceLedger, ReferenceLedgerError};

mod trigger_reservation;
pub use trigger_reservation::{check_stop_limit_reservation, check_stop_market_reservation};
