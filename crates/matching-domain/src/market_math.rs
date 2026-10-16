use crate::{Price, Qty, QuoteAmount, RejectReason};

/// 创建 MarketMath 数值配置时的内部校验失败分类。
///
/// 这些分类尚未进入 wire 协议；管理命令接入时须另行冻结映射。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MathConfigError {
    /// tick_size 不是严格正的 Price raw 步长。
    InvalidTick,
    /// lot_size 不是严格正的 Qty raw 步长。
    InvalidLot,
    /// 约分后的十进制 scale 因子无法以 u128 精确表示。
    UnsupportedScale,
    /// 最小 tick×lot 本金不能精确表示为 quote atoms。
    NonIntegralQuoteUnit,
    /// 计算最小 tick×lot 本金时的 checked 算术失败。
    ArithmeticOverflow,
}

/// 已验证配置下计算单笔成交本金的内部失败分类。
///
/// 当前不属于 wire 协议；命令入口将在后续范围中负责稳定映射。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MathError {
    /// price 未对齐当前市场的 tick_size。
    InvalidTick,
    /// qty 未对齐当前市场的 lot_size。
    InvalidLot,
    /// 计算合法成交本金时发生 checked 算术溢出。
    ArithmeticOverflow,
    /// 已验证配置下仍无法精确表示成交本金，表示不变量破坏。
    NonIntegralQuoteAmount,
}

impl MathError {
    /// 将数学运算错误显式映射为稳定业务拒绝原因。
    ///
    /// 返回 None 表示该错误不能降级为普通业务拒绝，
    /// 调用方必须 fail-closed。
    #[must_use]
    pub const fn to_reject_reason(self) -> Option<RejectReason> {
        match self {
            Self::InvalidTick => Some(RejectReason::InvalidTick),
            Self::InvalidLot => Some(RejectReason::InvalidLot),
            Self::ArithmeticOverflow => Some(RejectReason::ArithmeticOverflow),
            Self::NonIntegralQuoteAmount => None,
        }
    }
}

/// 已验证的市场整数金额换算配置。
///
/// 仅在最小 tick×lot 本金是精确、严格正的 quote atoms 时构造成功。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketMath {
    price_scale: u8,
    qty_scale: u8,
    quote_scale: u8,
    tick_size: i64,
    lot_size: u64,
    min_trade_notional: u128,
}

impl MarketMath {
    /// 校验 scale、tick 与 lot，并在成功时构造不可变数值配置。
    pub fn try_new(
        price_scale: u8,
        qty_scale: u8,
        quote_scale: u8,
        tick_size: i64,
        lot_size: u64,
    ) -> Result<Self, MathConfigError> {
        if tick_size <= 0 {
            return Err(MathConfigError::InvalidTick);
        }
        if lot_size == 0 {
            return Err(MathConfigError::InvalidLot);
        }
        let delta = i32::from(quote_scale)
            .checked_sub(i32::from(price_scale))
            .and_then(|v| v.checked_sub(i32::from(qty_scale)))
            .ok_or(MathConfigError::UnsupportedScale)?;

        let exponent = delta.unsigned_abs();

        let factor = 10u128
            .checked_pow(exponent)
            .ok_or(MathConfigError::UnsupportedScale)?;

        let base = (tick_size as u128)
            .checked_mul(u128::from(lot_size))
            .ok_or(MathConfigError::ArithmeticOverflow)?;

        let k = if delta < 0 {
            if base % factor != 0 {
                return Err(MathConfigError::NonIntegralQuoteUnit);
            }
            base / factor
        } else {
            base.checked_mul(factor)
                .ok_or(MathConfigError::ArithmeticOverflow)?
        };

        if k == 0 {
            return Err(MathConfigError::NonIntegralQuoteUnit);
        }
        Ok(Self {
            price_scale,
            qty_scale,
            quote_scale,
            tick_size,
            lot_size,
            min_trade_notional: k,
        })
    }

    /// 返回构造时已验证的市场价格 tick 步长。
    ///
    /// 保留原始 Price raw 单位，不进行重新校验或换算。
    pub const fn tick_size(&self) -> i64 {
        self.tick_size
    }

    /// 读取最小合法 tick×lot 成交对应的精确 quote 本金 K。
    pub const fn min_trade_notional(&self) -> u128 {
        self.min_trade_notional
    }

    /// 计算成交本金，单位为 quote atoms。
    ///
    /// 前置条件：
    /// - price 必须符合当前市场 tick_size。
    /// - qty 必须符合当前市场 lot_size。
    ///
    /// 使用 N × M × K，所有乘法均执行 checked 检查。
    pub fn trade_notional(&self, price: Price, qty: Qty) -> Result<QuoteAmount, MathError> {
        let price_raw = price.get();
        let qty_raw = qty.get();

        // 1. 先校验 price/tick。
        if price_raw % self.tick_size != 0 {
            return Err(MathError::InvalidTick);
        }

        // 2. 再校验 qty/lot。
        if !qty_raw.is_multiple_of(self.lot_size) {
            return Err(MathError::InvalidLot);
        }

        // 3. 转换为 tick、lot 倍数。
        let n = (price_raw / self.tick_size) as u128;
        let m = u128::from(qty_raw / self.lot_size);

        // 4. checked 计算 N × M × K。
        let notional = n
            .checked_mul(m)
            .and_then(|value| value.checked_mul(self.min_trade_notional))
            .ok_or(MathError::ArithmeticOverflow)?;

        Ok(QuoteAmount::new(notional))
    }

    /// 根据 quote 预算计算当前价格下最多可以买入的合法 Qty。
    ///
    /// - price 必须与 tick_size 对齐。
    /// - 向下取整到完整 lot。
    /// - 数量不得超过 Qty(u64) 的可表示上界。
    /// - 预算不足一 lot 时返回 None，不构造 Qty(0)。
    pub fn max_qty_for_quote(
        &self,
        quote: QuoteAmount,
        price: Price,
    ) -> Result<Option<Qty>, MathError> {
        let price_raw = price.get();

        // 1. 先验证市场 tick 对齐。
        if price_raw % self.tick_size != 0 {
            return Err(MathError::InvalidTick);
        }

        // 2. 一个 tick_size 对应的正整数倍数 N。
        let n = (price_raw / self.tick_size) as u128;

        // 3. 冻结公式：floor(floor(quote / K) / N)。
        let affordable_lots = (quote.get() / self.min_trade_notional) / n;

        // 4. 限制 Qty(u64) 可表达的完整 lot 数。
        let max_lots = u128::from(u64::MAX / self.lot_size);
        let lots = affordable_lots.min(max_lots);

        // 5. 不足一 lot 直接返回 None。
        if lots == 0 {
            return Ok(None);
        }

        // 6. 限幅后，乘法在 u64 范围内安全。
        let raw_qty = (lots as u64)
            .checked_mul(self.lot_size)
            .ok_or(MathError::ArithmeticOverflow)?;

        let qty = Qty::try_new(raw_qty).map_err(|_| MathError::InvalidLot)?;

        Ok(Some(qty))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn market_math_rejects_invalid_tick_and_lot() {
        use MathConfigError::*;

        assert_eq!(MarketMath::try_new(0, 0, 0, 0, 1), Err(InvalidTick));
        assert_eq!(MarketMath::try_new(0, 0, 0, -1, 1), Err(InvalidTick));
        assert_eq!(MarketMath::try_new(0, 0, 0, 1, 0), Err(InvalidLot));

        // 组合非法输入：必须优先 InvalidTick。
        assert_eq!(MarketMath::try_new(0, 0, 0, 0, 0), Err(InvalidTick));
    }

    #[test]
    fn market_math_exposes_configured_tick_size() {
        let unit_tick = MarketMath::try_new(0, 0, 0, 1, 1).expect("tick=1 must be valid");

        let ten_tick = MarketMath::try_new(0, 0, 0, 10, 1).expect("tick=10 must be valid");

        assert_eq!(unit_tick.tick_size(), 1);
        assert_eq!(ten_tick.tick_size(), 10);
    }

    #[test]
    fn market_math_computes_exact_minimum_notional() {
        let cases = [
            (0, 0, 0, 1, 1, 1),
            (2, 8, 8, 1, 1000, 10),
            (0, 0, 2, 1, 1, 100),
            (0, 0, 38, 1, 1, 10u128.pow(38)),
        ];

        for (ps, qs, quote_s, tick, lot, expected) in cases {
            let math = MarketMath::try_new(ps, qs, quote_s, tick, lot).unwrap();

            assert_eq!(math.min_trade_notional(), expected);
        }
    }

    #[test]
    fn market_math_rejects_non_integral_quote_unit() {
        assert_eq!(
            MarketMath::try_new(2, 8, 8, 1, 1),
            Err(MathConfigError::NonIntegralQuoteUnit)
        );
    }

    #[test]
    fn market_math_rejects_unsupported_scale() {
        use MathConfigError::UnsupportedScale;

        let cases = [
            (0, 0, 39),      // delta = +39
            (39, 0, 0),      // delta = -39
            (255, 255, 255), // delta = -255
        ];

        for (ps, qs, quote_s) in cases {
            assert_eq!(
                MarketMath::try_new(ps, qs, quote_s, 1, 1),
                Err(UnsupportedScale)
            );
        }
    }

    #[test]
    fn market_math_rejects_notional_overflow() {
        assert_eq!(
            MarketMath::try_new(0, 0, 1, i64::MAX, u64::MAX,),
            Err(MathConfigError::ArithmeticOverflow)
        );
    }

    #[test]
    fn trade_notional_calculates_exact_amount() {
        let math = MarketMath::try_new(2, 8, 8, 1, 1000).unwrap();

        let cases = [(1, 1000, 10), (123, 5000, 6150), (10000, 10000, 1_000_000)];

        for (price, qty, expected) in cases {
            assert_eq!(
                math.trade_notional(Price::try_new(price).unwrap(), Qty::try_new(qty).unwrap(),),
                Ok(QuoteAmount::new(expected))
            );
        }
    }

    #[test]
    fn trade_notional_rejects_unaligned_price() {
        let math = MarketMath::try_new(0, 0, 0, 5, 10).unwrap();

        assert_eq!(
            math.trade_notional(Price::try_new(6).unwrap(), Qty::try_new(20).unwrap(),),
            Err(MathError::InvalidTick)
        );
    }

    #[test]
    fn trade_notional_rejects_unaligned_qty() {
        let math = MarketMath::try_new(0, 0, 0, 5, 10).unwrap();

        assert_eq!(
            math.trade_notional(Price::try_new(15).unwrap(), Qty::try_new(11).unwrap(),),
            Err(MathError::InvalidLot)
        );
    }

    #[test]
    fn trade_notional_prioritizes_invalid_price() {
        let math = MarketMath::try_new(0, 0, 0, 5, 10).unwrap();

        // price、qty 均未对齐。
        // 必须先返回 InvalidTick。
        assert_eq!(
            math.trade_notional(Price::try_new(6).unwrap(), Qty::try_new(11).unwrap(),),
            Err(MathError::InvalidTick)
        );
    }

    #[test]
    fn trade_notional_rejects_overflow() {
        let math = MarketMath::try_new(0, 0, 38, 1, 1).unwrap();

        // K = 10^38，单个最小成交本金合法。
        assert_eq!(math.min_trade_notional(), 10u128.pow(38));

        // 4 × 1 × 10^38 超过 u128::MAX。
        assert_eq!(
            math.trade_notional(Price::try_new(4).unwrap(), Qty::try_new(1).unwrap(),),
            Err(MathError::ArithmeticOverflow)
        );
    }

    #[test]
    fn trade_notional_accepts_large_exact_amount() {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();

        let result = math.trade_notional(
            Price::try_new(i64::MAX).unwrap(),
            Qty::try_new(u64::MAX).unwrap(),
        );

        let expected = (i64::MAX as u128) * (u64::MAX as u128);

        assert_eq!(result, Ok(QuoteAmount::new(expected)));
    }
    #[test]
    fn trade_notional_btc_exact_quote_atoms() {
        let math = MarketMath::try_new(2, 8, 8, 1, 1000).unwrap();

        let actual = math.trade_notional(
            Price::try_new(6_512_345).unwrap(),
            Qty::try_new(12_345_000).unwrap(),
        );

        assert_eq!(actual, Ok(QuoteAmount::new(803_948_990_250)));
    }
    #[test]
    fn trade_notional_positive_delta_exact_amount() {
        let math = MarketMath::try_new(0, 0, 2, 1, 1).unwrap();

        // K = 100
        // N = 3
        // M = 2
        // 成交本金 = 3 × 2 × 100 = 600
        assert_eq!(
            math.trade_notional(Price::try_new(3).unwrap(), Qty::try_new(2).unwrap(),),
            Ok(QuoteAmount::new(600))
        );
    }
    #[test]
    fn market_math_types_are_reachable_from_crate_root() {
        use crate::{MarketMath, MathError};

        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();

        assert_eq!(math.min_trade_notional(), 1);

        let _: Option<MathError> = None;
    }
    #[test]
    fn max_qty_for_quote_zero_dust_and_exact_lot() {
        let math = MarketMath::try_new(2, 8, 8, 1, 1000).unwrap();
        let price = Price::try_new(100).unwrap();

        // N=100，K=10，因此一个 lot 本金为 1000 quote atoms。
        assert_eq!(math.max_qty_for_quote(QuoteAmount::new(0), price), Ok(None));

        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(999), price),
            Ok(None)
        );

        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(1000), price),
            Ok(Some(Qty::try_new(1000).unwrap()))
        );
    }
    #[test]
    fn max_qty_for_quote_floors_to_whole_lots() {
        let math = MarketMath::try_new(2, 8, 8, 1, 1000).unwrap();
        let price = Price::try_new(123).unwrap();

        // 一个 lot 本金：123 × 10 = 1230。
        // 预算 3000 只能购买两个完整 lot。
        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(3000), price),
            Ok(Some(Qty::try_new(2000).unwrap()))
        );
    }
    #[test]
    fn max_qty_for_quote_rejects_unaligned_tick() {
        let math = MarketMath::try_new(0, 0, 0, 5, 10).unwrap();
        let price = Price::try_new(6).unwrap();

        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(100), price),
            Err(MathError::InvalidTick)
        );

        // 即使预算为零，也必须先检查 tick 对齐。
        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(0), price),
            Err(MathError::InvalidTick)
        );
    }
    #[test]
    fn max_qty_for_quote_caps_at_largest_valid_qty() {
        let math = MarketMath::try_new(0, 0, 0, 1, 10).unwrap();
        let price = Price::try_new(1).unwrap();

        let expected = (u64::MAX / 10) * 10;

        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(u128::MAX), price,),
            Ok(Some(Qty::try_new(expected).unwrap()))
        );
    }
    #[test]
    fn max_qty_for_quote_accepts_u64_max() {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();

        assert_eq!(
            math.max_qty_for_quote(QuoteAmount::new(u128::MAX), Price::try_new(1).unwrap(),),
            Ok(Some(Qty::try_new(u64::MAX).unwrap()))
        );
    }
    #[test]
    fn max_qty_for_quote_preserves_budget_and_maximality() {
        let math = MarketMath::try_new(2, 8, 8, 1, 1000).unwrap();
        let price = Price::try_new(123).unwrap();

        for budget in [0u128, 1, 1229, 1230, 2459, 2460, 3000, 100_000] {
            let quote = QuoteAmount::new(budget);

            let result = math.max_qty_for_quote(quote, price).unwrap();

            match result {
                None => {
                    // 连一个合法 lot 都买不起。
                    let first_lot = Qty::try_new(1000).unwrap();
                    let cost = math.trade_notional(price, first_lot).unwrap();

                    assert!(cost.get() > budget);
                }

                Some(qty) => {
                    assert!(qty.get() > 0);
                    assert!(qty.get().is_multiple_of(1000));

                    // 返回 Qty 的本金绝不超过预算。
                    let cost = math.trade_notional(price, qty).unwrap();
                    assert!(cost.get() <= budget);

                    // 如果还存在可表示的下一 lot，
                    // 下一 lot 的本金必须超过预算。
                    if let Some(next_raw) = qty.get().checked_add(1000) {
                        let next = Qty::try_new(next_raw).unwrap();
                        let next_cost = math.trade_notional(price, next).unwrap();

                        assert!(next_cost.get() > budget, "budget={budget}, qty={:?}", qty);
                    }
                }
            }
        }
    }
    #[test]
    fn math_error_to_reject_reason_maps_all_variants() {
        assert_eq!(
            MathError::InvalidTick.to_reject_reason(),
            Some(RejectReason::InvalidTick)
        );

        assert_eq!(
            MathError::InvalidLot.to_reject_reason(),
            Some(RejectReason::InvalidLot)
        );

        assert_eq!(
            MathError::ArithmeticOverflow.to_reject_reason(),
            Some(RejectReason::ArithmeticOverflow)
        );

        assert_eq!(MathError::NonIntegralQuoteAmount.to_reject_reason(), None);
    }
}
