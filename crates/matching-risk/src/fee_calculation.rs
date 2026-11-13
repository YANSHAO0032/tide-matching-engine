//! 按固定费率及取整策略计算独立费用。
use matching_domain::FeeRounding;

/// 费用结果超出整数表示范围。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeCalculationError {
    /// 中间值或有符号结果不可表示。
    Overflow,
}

/// 基于已确定的资产原子数量计算有符号费用；负值为返佣。
pub fn calculate_fee(
    basis: u128,
    rate_bps: i32,
    rounding: FeeRounding,
) -> Result<i128, FeeCalculationError> {
    let rate = u128::from(rate_bps.unsigned_abs());
    let whole = (basis / 10_000)
        .checked_mul(rate)
        .ok_or(FeeCalculationError::Overflow)?;
    let fraction = (basis % 10_000)
        .checked_mul(rate)
        .ok_or(FeeCalculationError::Overflow)?;
    let mut magnitude = whole
        .checked_add(fraction / 10_000)
        .ok_or(FeeCalculationError::Overflow)?;
    let has_remainder = fraction % 10_000 != 0;
    let away_from_zero = has_remainder
        && matches!(
            (rate_bps.is_negative(), rounding),
            (false, FeeRounding::Ceiling) | (true, FeeRounding::Floor)
        );
    if away_from_zero {
        magnitude = magnitude
            .checked_add(1)
            .ok_or(FeeCalculationError::Overflow)?;
    }
    if rate_bps.is_negative() {
        if magnitude == (1_u128 << 127) {
            return Ok(i128::MIN);
        }
        let positive = i128::try_from(magnitude).map_err(|_| FeeCalculationError::Overflow)?;
        Ok(-positive)
    } else {
        i128::try_from(magnitude).map_err(|_| FeeCalculationError::Overflow)
    }
}
