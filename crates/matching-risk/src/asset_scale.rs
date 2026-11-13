//! 资产最小单位的精确整数换算。

/// 资产精度换算失败的内部分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssetScaleError {
    /// 精度差对应的十进制因子无法表示。
    UnsupportedScale,
    /// 向较低精度换算时出现不可消除的小数部分。
    NonIntegral,
    /// 向较高精度换算的结果无法表示。
    Overflow,
}

/// 精确换算原子数量；不得截断余数或饱和溢出。
pub fn convert_asset_atoms(atoms: u128, source: u8, target: u8) -> Result<u128, AssetScaleError> {
    let difference = u32::from(source.abs_diff(target));
    let factor = 10_u128
        .checked_pow(difference)
        .ok_or(AssetScaleError::UnsupportedScale)?;
    if target >= source {
        atoms.checked_mul(factor).ok_or(AssetScaleError::Overflow)
    } else if atoms.is_multiple_of(factor) {
        Ok(atoms / factor)
    } else {
        Err(AssetScaleError::NonIntegral)
    }
}

/// 按正有理比例精确换算原子单位；先约分避免可消除的中间溢出。
pub(crate) fn convert_ratio_exact(
    atoms: u128,
    numerator: u128,
    denominator: u128,
) -> Result<u128, AssetScaleError> {
    if numerator == 0 || denominator == 0 {
        return Err(AssetScaleError::UnsupportedScale);
    }
    fn gcd(mut a: u128, mut b: u128) -> u128 {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    }
    let divisor = gcd(atoms, denominator);
    let left = atoms / divisor;
    let remaining = denominator / divisor;
    if !numerator.is_multiple_of(remaining) {
        return Err(AssetScaleError::NonIntegral);
    }
    left.checked_mul(numerator / remaining)
        .ok_or(AssetScaleError::Overflow)
}
