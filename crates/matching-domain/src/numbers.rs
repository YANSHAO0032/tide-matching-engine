use crate::reject_reason::RejectReason;

/// 以 ticks 表示的严格正价格。
///
/// 数值排序仅表示底层整数大小，不表示撮合优先级。
///
/// `Price` 与 `Qty` 是不同的业务类型：
///
/// ```compile_fail
/// use matching_domain::{Price, Qty};
///
/// fn submit_qty(qty: Qty) {
///     let _ = qty.get();
/// }
///
/// // 正确类型的调用。
/// let qty = Qty::try_new(10).unwrap();
/// submit_qty(qty);
///
/// // 即使底层都表示整数，也不能混用。
/// let price = Price::try_new(10).unwrap();
/// submit_qty(price);
/// ```
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

impl Price {
    /// 仅接受严格大于零的 ticks
    pub const fn try_new(value: i64) -> Result<Self, RejectReason> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(RejectReason::InvalidPrice)
        }
    }

    /// 读取原始 ticks
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// 以 lots 表示的严格正数量。
///
/// 自然排序仅表示底层整数的数值顺序。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(u64);

impl Qty {
    /// 仅接受严格大于零的 lots；零返回 [`RejectReason::InvalidQty`]。
    pub const fn try_new(value: u64) -> Result<Self, RejectReason> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(RejectReason::InvalidQty)
        }
    }

    /// 读取原始 lots。
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// 非负的报价金额，底层为 u128。
///
/// 允许零，不允许负数。只提供显式 checked 加减。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuoteAmount(u128);

impl QuoteAmount {
    /// 从非负的原始报价金额构造值对象。
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// 读取原始报价金额。
    pub const fn get(self) -> u128 {
        self.0
    }
    /// 相加溢出时返回 [`RejectReason::ArithmeticOverflow`]，且不产生截断值。
    pub const fn checked_add(self, rhs: Self) -> Result<Self, RejectReason> {
        match self.0.checked_add(rhs.0) {
            Some(value) => Ok(Self(value)),
            None => Err(RejectReason::ArithmeticOverflow),
        }
    }
    /// 相减下溢时返回 [`RejectReason::ArithmeticOverflow`]，且不产生截断值。
    pub const fn checked_sub(self, rhs: Self) -> Result<Self, RejectReason> {
        match self.0.checked_sub(rhs.0) {
            Some(value) => Ok(Self(value)),
            None => Err(RejectReason::ArithmeticOverflow),
        }
    }
}

/// 有符号金额，底层为 i128。
///
/// 允许负数、零和正数。自然排序仅表示整数大小。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedAmount(i128);
impl SignedAmount {
    /// 从有符号原始金额构造值对象；允许负数、零和正数。
    pub const fn new(value: i128) -> Self {
        Self(value)
    }

    /// 读取原始有符号金额。
    pub const fn get(self) -> i128 {
        self.0
    }
    /// 正向或负向溢出均返回 ArithmeticOverflow
    pub const fn checked_add(self, rhs: Self) -> Result<Self, RejectReason> {
        match self.0.checked_add(rhs.0) {
            Some(value) => Ok(Self(value)),
            None => Err(RejectReason::ArithmeticOverflow),
        }
    }
    /// 正向或负向溢出均返回 ArithmeticOverflow。
    pub const fn checked_sub(self, rhs: Self) -> Result<Self, RejectReason> {
        match self.0.checked_sub(rhs.0) {
            Some(value) => Ok(Self(value)),
            None => Err(RejectReason::ArithmeticOverflow),
        }
    }
}

#[cfg(test)]
mod tests {
    //! 验证整数值对象的边界、值语义与 checked 算术失败语义。
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn accepts_valid_boundaries() {
        assert_eq!(Price::try_new(1).unwrap().get(), 1);
        assert_eq!(Qty::try_new(1).unwrap().get(), 1);
        assert_eq!(Price::try_new(i64::MAX).unwrap().get(), i64::MAX);
        assert_eq!(Qty::try_new(u64::MAX).unwrap().get(), u64::MAX)
    }

    #[test]
    fn rejects_invalid_boundaries() {
        for value in [0, -1, i64::MIN] {
            assert_eq!(Price::try_new(value), Err(RejectReason::InvalidPrice));
        }
    }

    #[test]
    fn supports_value_semantics_and_numeric_ordering() {
        let price_low = Price::try_new(1).unwrap();
        let price_equal = Price::try_new(1).unwrap();
        let price_high = Price::try_new(i64::MAX).unwrap();

        let qty_low = Qty::try_new(1).unwrap();
        let qty_equal = Qty::try_new(1).unwrap();
        let qty_high = Qty::try_new(u64::MAX).unwrap();

        assert_eq!(price_low, price_equal);
        assert_ne!(price_low, price_high);
        assert!(price_low < price_high);

        let mut values = [price_high, price_low];
        values.sort();
        assert_eq!(values, [price_low, price_high]);

        let copied = price_low;
        assert_eq!(copied, price_low);

        let mut set = std::collections::HashSet::new();
        assert!(set.insert(price_low));
        assert!(!set.insert(price_equal));

        assert_eq!(qty_low, qty_equal);
        assert_ne!(qty_low, qty_high);
        assert!(qty_low < qty_high);

        let mut values = [qty_high, qty_low];
        values.sort();
        assert_eq!(values, [qty_low, qty_high]);

        let copied = qty_low;
        assert_eq!(copied, qty_low);

        let mut set = std::collections::HashSet::new();
        assert!(set.insert(qty_low));
        assert!(!set.insert(qty_equal));
    }

    #[test]
    fn rejects_zero() {
        assert_eq!(Qty::try_new(0), Err(RejectReason::InvalidQty));
    }

    #[test]
    fn quote_amount_preserves_zero_and_max() {
        assert_eq!(QuoteAmount::new(0).get(), 0);
        assert_eq!(QuoteAmount::new(1).get(), 1);
        assert_eq!(QuoteAmount::new(u128::MAX).get(), u128::MAX);
    }

    #[test]
    fn signed_amount_preserves_zero_and_extremes() {
        assert_eq!(SignedAmount::new(0).get(), 0);
        assert_eq!(SignedAmount::new(-1).get(), -1);
        assert_eq!(SignedAmount::new(1).get(), 1);
        assert_eq!(SignedAmount::new(i128::MIN).get(), i128::MIN);
        assert_eq!(SignedAmount::new(i128::MAX).get(), i128::MAX);
    }

    #[test]
    fn quote_amount_value_semantics() {
        let zero = QuoteAmount::new(0);
        let one = QuoteAmount::new(1);
        let max = QuoteAmount::new(u128::MAX);

        assert_eq!(one, QuoteAmount::new(1));
        assert_ne!(zero, one);

        let mut values = [max, one, zero];
        values.sort();
        assert_eq!(values, [zero, one, max]);

        let copied = one;
        assert_eq!(copied, one);
        assert_eq!(format!("{one:?}"), "QuoteAmount(1)");

        let mut set = HashSet::new();
        assert!(set.insert(one));
        assert!(!set.insert(QuoteAmount::new(1)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn signed_amount_value_semantics() {
        let min = SignedAmount::new(i128::MIN);
        let negative = SignedAmount::new(-1);
        let zero = SignedAmount::new(0);
        let positive = SignedAmount::new(1);
        let max = SignedAmount::new(i128::MAX);

        assert_eq!(positive, SignedAmount::new(1));
        assert_ne!(negative, positive);

        let mut values = [max, zero, positive, min, negative];
        values.sort();
        assert_eq!(values, [min, negative, zero, positive, max]);

        let copied = negative;
        assert_eq!(copied, negative);
        assert_eq!(format!("{negative:?}"), "SignedAmount(-1)");

        let mut set = HashSet::new();
        assert!(set.insert(negative));
        assert!(!set.insert(SignedAmount::new(-1)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn quote_amount_checked_add() {
        let zero = QuoteAmount::new(0);
        let one = QuoteAmount::new(1);
        let max = QuoteAmount::new(u128::MAX);

        assert_eq!(zero.checked_add(zero), Ok(zero));
        assert_eq!(zero.checked_add(max), Ok(max));
        assert_eq!(one.checked_add(one), Ok(QuoteAmount::new(2)));
        assert_eq!(max.checked_add(one), Err(RejectReason::ArithmeticOverflow));
    }

    #[test]
    fn quote_amount_checked_sub() {
        let zero = QuoteAmount::new(0);
        let one = QuoteAmount::new(1);
        let max = QuoteAmount::new(u128::MAX);

        assert_eq!(zero.checked_sub(zero), Ok(zero));
        assert_eq!(one.checked_sub(one), Ok(zero));
        assert_eq!(max.checked_sub(one), Ok(QuoteAmount::new(u128::MAX - 1)));
        assert_eq!(zero.checked_sub(one), Err(RejectReason::ArithmeticOverflow));
    }

    #[test]
    fn signed_amount_checked_add() {
        let zero = SignedAmount::new(0);
        let one = SignedAmount::new(1);
        let negative_one = SignedAmount::new(-1);
        let min = SignedAmount::new(i128::MIN);
        let max = SignedAmount::new(i128::MAX);

        assert_eq!(zero.checked_add(zero), Ok(zero));
        assert_eq!(one.checked_add(negative_one), Ok(zero));
        assert_eq!(min.checked_add(max), Ok(negative_one));
        assert_eq!(max.checked_add(one), Err(RejectReason::ArithmeticOverflow));
        assert_eq!(
            min.checked_add(negative_one),
            Err(RejectReason::ArithmeticOverflow)
        );
    }

    #[test]
    fn signed_amount_checked_sub() {
        let zero = SignedAmount::new(0);
        let one = SignedAmount::new(1);
        let negative_one = SignedAmount::new(-1);
        let min = SignedAmount::new(i128::MIN);
        let max = SignedAmount::new(i128::MAX);

        assert_eq!(zero.checked_sub(zero), Ok(zero));
        assert_eq!(one.checked_sub(one), Ok(zero));
        assert_eq!(zero.checked_sub(one), Ok(negative_one));
        assert_eq!(max.checked_sub(max), Ok(zero));
        assert_eq!(
            max.checked_sub(negative_one),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(min.checked_sub(one), Err(RejectReason::ArithmeticOverflow));
    }

    #[test]
    fn transparent_representation() {
        assert_eq!(size_of::<QuoteAmount>(), size_of::<u128>());
        assert_eq!(size_of::<SignedAmount>(), size_of::<i128>());
    }
}
