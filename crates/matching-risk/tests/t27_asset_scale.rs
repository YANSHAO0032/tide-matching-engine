//! 验证资产原子单位之间的精确换算和失败边界。
use matching_risk::{AssetScaleError, convert_asset_atoms};

#[test]
fn asset_atoms_require_exact_checked_conversion() {
    assert_eq!(convert_asset_atoms(123, 2, 4), Ok(12_300));
    assert_eq!(convert_asset_atoms(12_300, 4, 2), Ok(123));
    assert_eq!(
        convert_asset_atoms(12_301, 4, 2),
        Err(AssetScaleError::NonIntegral)
    );
    assert_eq!(
        convert_asset_atoms(u128::MAX, 0, 1),
        Err(AssetScaleError::Overflow)
    );
    assert_eq!(
        convert_asset_atoms(1, 0, 39),
        Err(AssetScaleError::UnsupportedScale)
    );
    assert_eq!(convert_asset_atoms(7, 255, 255), Ok(7));
}
