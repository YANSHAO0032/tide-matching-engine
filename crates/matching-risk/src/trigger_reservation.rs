//! 触发前对已接管预留和锁定费率做纯本地复核。
use crate::{LimitReservePolicy, ReservationRegistry, ReservationStatus};
use matching_domain::{FeeScheduleBook, MarketId, RejectReason, StopLimitOrder};

/// 检查 StopLimit child 的身份、锁定版本及当前剩余预留覆盖。
/// 市场状态、时段、触发价格与其余 Trigger-R1 约束仍由调用方在 child 撮合前验证。
pub fn check_stop_limit_reservation(
    policy: &LimitReservePolicy,
    fees: &FeeScheduleBook,
    registry: &ReservationRegistry,
    market_id: MarketId,
    stop: &StopLimitOrder,
) -> Result<(), RejectReason> {
    let state = verified_state(
        registry,
        market_id,
        stop.order_id,
        stop.user_id,
        stop.reservation_id,
        stop.fee_schedule_version,
    )?;
    let required = policy.required_versioned_reserves(
        fees,
        state.fee_schedule_version,
        state.fee_tier_id,
        stop.side,
        stop.price,
        stop.qty,
    )?;
    verify_coverage(state, required)
}

/// 用已冻结的有效最差价或报价预算复核止损市价 child 的预留。
/// 有效最差价由调用方按当前参考价和保护规则计算并固定；无保护时使用可表示价格的保守边界。
pub fn check_stop_market_reservation(
    policy: &LimitReservePolicy,
    fees: &FeeScheduleBook,
    registry: &ReservationRegistry,
    market_id: MarketId,
    stop: &matching_domain::StopMarketOrder,
    effective_worst: Option<matching_domain::Price>,
) -> Result<(), RejectReason> {
    use matching_domain::{Side, StopMarketQuantity};
    let state = verified_state(
        registry,
        market_id,
        stop.order_id,
        stop.user_id,
        stop.reservation_id,
        stop.fee_schedule_version,
    )?;
    let required = match stop.quantity {
        StopMarketQuantity::Base(qty) => {
            let worst = match effective_worst {
                Some(price) => price,
                None => {
                    let tick = policy.market_math().tick_size();
                    let raw = if stop.side == Side::Buy {
                        i64::MAX - i64::MAX % tick
                    } else {
                        tick
                    };
                    matching_domain::Price::try_new(raw)?
                }
            };
            policy.required_versioned_reserves(
                fees,
                state.fee_schedule_version,
                state.fee_tier_id,
                stop.side,
                worst,
                qty,
            )?
        }
        StopMarketQuantity::Quote(budget) => {
            if stop.side != Side::Buy {
                return Err(RejectReason::InvalidOrderCombination);
            }
            policy.required_quote_budget_reserves(
                fees,
                state.fee_schedule_version,
                state.fee_tier_id,
                budget,
            )?
        }
    };
    verify_coverage(state, required)
}

/// 校验登记表内 stop 身份，不能把 PendingR2 误作活动订单。
fn verified_state(
    registry: &ReservationRegistry,
    market: MarketId,
    order: matching_domain::OrderId,
    user: matching_domain::UserId,
    reservation: matching_domain::ReservationId,
    version: u64,
) -> Result<&crate::ReservationState, RejectReason> {
    let state = registry
        .get(reservation)
        .ok_or(RejectReason::InvalidReservation)?;
    if state.status != ReservationStatus::Active
        || registry.is_pending_r2(reservation)
        || state.order_id != order
        || state.user_id != user
        || state.market_id != market
    {
        return Err(RejectReason::InvalidReservation);
    }
    if state.fee_schedule_version != version {
        return Err(RejectReason::FeeScheduleMismatch);
    }
    Ok(state)
}

/// 比较每种实际所需资产，不将其他资产余额混作覆盖。
fn verify_coverage(
    state: &crate::ReservationState,
    required: Vec<matching_domain::AssetReserve>,
) -> Result<(), RejectReason> {
    for expected in required {
        let available = state
            .reserves
            .binary_search_by_key(&expected.asset, |entry| entry.asset)
            .ok()
            .map_or(0, |index| state.reserves[index].atoms);
        if available < expected.atoms {
            return Err(RejectReason::InsufficientReservation);
        }
    }
    Ok(())
}
