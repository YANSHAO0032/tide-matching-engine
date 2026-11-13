//! 限价单的可信市场资金需求计算。

use matching_domain::{AssetId, AssetReserve, MarketMath, MathError, Price, Qty, Side};

/// 保存可信市场资产和数值规则，并提供静态或版本化费率预留计算。
///
/// `MarketMath` 的数量单位必须是基础资产原子单位，报价金额单位必须是
/// 报价资产原子单位。每手分别向上取整，以覆盖拆分成交的费用上界。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitReservePolicy {
    /// 基础资产身份。
    base: AssetId,
    /// 报价资产身份。
    quote: AssetId,
    /// 已验证的价格与数量换算规则。
    math: MarketMath,
    /// 接收订单时固定的费率版本。
    fee_schedule_version: u64,
    /// 接收订单时固定的费率层级。
    fee_tier_id: u32,
    /// 该费率版本下的报价资产费用上界，单位为基点。
    fee_bps: u16,
}

/// 资金需求配置或计算失败；不是线协议错误码。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitReserveError {
    /// 市场资产相同、资产缩放位数不匹配，或费率超过报价所得。
    InvalidPolicy,
    /// 市场数值校验失败。
    Math(
        /// 市场规则计算返回的具体错误。
        MathError,
    ),
    /// 费用或总需求溢出。
    Overflow,
}

impl LimitReservePolicy {
    /// 写入可信限价预留策略的完整配置。
    pub fn write_snapshot(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{
            SnapshotEncoder, SnapshotWireError, encode_market_math,
        };
        let mut body = SnapshotEncoder::new(64);
        body.u32(self.base.get())?;
        body.u32(self.quote.get())?;
        encode_market_math(&mut body, &self.math)?;
        body.u64(self.fee_schedule_version)?;
        body.u32(self.fee_tier_id)?;
        body.u16(self.fee_bps)?;
        writer.bytes(&body.finish()).map_err(|_| SnapshotWireError)
    }
    /// 恢复并重新验证资产、缩放及费率配置。
    pub fn read_snapshot(
        reader: &mut matching_domain::snapshot_wire::SnapshotDecoder<'_>,
    ) -> Result<Self, matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{
            SnapshotDecoder, SnapshotWireError, decode_market_math,
        };
        let bytes = reader.bytes()?;
        let mut r = SnapshotDecoder::new(bytes, 64)?;
        let base = AssetId::new(r.u32()?);
        let quote = AssetId::new(r.u32()?);
        let math = decode_market_math(&mut r)?;
        let version = r.u64()?;
        let tier = r.u32()?;
        let bps = r.u16()?;
        r.finish()?;
        Self::new(
            (base, math.qty_scale()),
            (quote, math.quote_scale()),
            math,
            version,
            tier,
            bps,
        )
        .map_err(|_| SnapshotWireError)
    }
    /// 使用可信市场数值配置及锁定费率创建策略。
    /// 基础资产和报价资产参数分别包含身份与十进制缩放位数，
    /// 缩放位数须与 `MarketMath` 的数量和报价缩放位数对应。
    pub fn new(
        base: (AssetId, u8),
        quote: (AssetId, u8),
        math: MarketMath,
        fee_schedule_version: u64,
        fee_tier_id: u32,
        fee_bps: u16,
    ) -> Result<Self, LimitReserveError> {
        if base.0 == quote.0
            || base.1 != math.qty_scale()
            || quote.1 != math.quote_scale()
            || fee_bps > 10_000
        {
            return Err(LimitReserveError::InvalidPolicy);
        }
        Ok(Self {
            base: base.0,
            quote: quote.0,
            math,
            fee_schedule_version,
            fee_tier_id,
            fee_bps,
        })
    }

    /// 返回绑定的市场数值规则。
    pub const fn market_math(&self) -> MarketMath {
        self.math
    }

    /// 返回策略绑定的基础资产和报价资产身份。
    pub const fn assets(&self) -> (AssetId, AssetId) {
        (self.base, self.quote)
    }

    /// 在资产精度不变时更新市场数值规则，保留可信资产和费率配置。
    /// 调用方须先校验历史金额引用及市场状态。
    pub fn with_market_math(&self, math: MarketMath) -> Result<Self, LimitReserveError> {
        if (self.math.qty_scale(), self.math.quote_scale())
            != (math.qty_scale(), math.quote_scale())
        {
            return Err(LimitReserveError::InvalidPolicy);
        }
        Ok(Self { math, ..*self })
    }

    /// 返回锁定的费率版本。
    pub const fn fee_schedule_version(&self) -> u64 {
        self.fee_schedule_version
    }
    /// 返回锁定的费率层级。
    pub const fn fee_tier_id(&self) -> u32 {
        self.fee_tier_id
    }

    /// 按锁定版本和两种成交角色的费用上界计算限价预留。
    /// 按最小 lot 分别向上取整，以覆盖任意合法成交拆分。
    pub fn required_versioned_reserves(
        &self,
        fees: &matching_domain::FeeScheduleBook,
        version: u64,
        tier_id: u32,
        side: Side,
        price: Price,
        qty: Qty,
    ) -> Result<Vec<AssetReserve>, matching_domain::RejectReason> {
        use matching_domain::{FeeAssetPolicy, RejectReason};
        let schedule = fees.schedule(version)?;
        let tier = fees.resolve(version, tier_id)?;
        let rate = u128::try_from(tier.maker_bps.max(tier.taker_bps).max(0))
            .map_err(|_| RejectReason::ArithmeticOverflow)?;
        let lot_qty = self.math.lot_size();
        if !qty.get().is_multiple_of(lot_qty) {
            return Err(RejectReason::InvalidLot);
        }
        let lot = Qty::try_new(lot_qty)?;
        let per_lot = self
            .math
            .trade_notional(price, lot)
            .map_err(|error| {
                error
                    .to_reject_reason()
                    .unwrap_or(RejectReason::InvalidReservation)
            })?
            .get();
        let lots = u128::from(qty.get() / lot_qty);
        let quote = per_lot
            .checked_mul(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let base = u128::from(qty.get());
        let maximum_sell_notional = || -> Result<u128, RejectReason> {
            let tick = self.math.tick_size();
            let max_price = Price::try_new(i64::MAX - (i64::MAX % tick))?;
            self.math
                .trade_notional(max_price, lot)
                .map(|value| value.get())
                .map_err(|error| {
                    error
                        .to_reject_reason()
                        .unwrap_or(RejectReason::InvalidReservation)
                })
        };
        let basis = match schedule.asset_policy() {
            FeeAssetPolicy::Base => u128::from(lot_qty),
            FeeAssetPolicy::Quote if side == Side::Sell && rate > 10_000 => {
                maximum_sell_notional()?
            }
            FeeAssetPolicy::Quote => per_lot,
            FeeAssetPolicy::Third(_) => return Err(RejectReason::InvalidReservation),
            FeeAssetPolicy::ThirdPriced {
                asset,
                quote_asset,
                numerator,
                denominator,
            } => {
                if asset == self.base || quote_asset != self.quote {
                    return Err(RejectReason::InvalidReservation);
                }
                let minimum_price = Price::try_new(self.math.tick_size())?;
                let minimum = self
                    .math
                    .trade_notional(minimum_price, lot)
                    .map_err(|error| {
                        error
                            .to_reject_reason()
                            .unwrap_or(RejectReason::InvalidReservation)
                    })?
                    .get();
                crate::asset_scale::convert_ratio_exact(minimum, numerator, denominator)
                    .map_err(|_| RejectReason::InvalidReservation)?;
                let quote_basis = if side == Side::Sell && rate > 0 {
                    maximum_sell_notional()?
                } else {
                    per_lot
                };
                crate::asset_scale::convert_ratio_exact(quote_basis, numerator, denominator)
                    .map_err(|_| RejectReason::ArithmeticOverflow)?
            }
        };
        let product = basis
            .checked_mul(rate)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let fee_per_lot = (product / 10_000)
            .checked_add(u128::from(product % 10_000 != 0))
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let fee = fee_per_lot
            .checked_mul(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let mut amounts = std::collections::BTreeMap::new();
        match side {
            Side::Buy => {
                amounts.insert(self.quote, quote);
            }
            Side::Sell => {
                amounts.insert(self.base, base);
            }
        }
        let extra = match (side, schedule.asset_policy()) {
            (Side::Buy, FeeAssetPolicy::Quote) => Some((self.quote, fee)),
            (Side::Sell, FeeAssetPolicy::Base) => Some((self.base, fee)),
            (Side::Buy, FeeAssetPolicy::Base) if fee > base => Some((self.base, fee - base)),
            (Side::Sell, FeeAssetPolicy::Quote) if rate > 10_000 => Some((
                self.quote,
                fee_per_lot
                    .checked_sub(basis)
                    .ok_or(RejectReason::ArithmeticOverflow)?
                    .checked_mul(lots)
                    .ok_or(RejectReason::ArithmeticOverflow)?,
            )),
            (_, FeeAssetPolicy::ThirdPriced { asset, .. }) => Some((asset, fee)),
            _ => None,
        };
        if let Some((asset, value)) = extra
            && value != 0
        {
            let amount = amounts.entry(asset).or_insert(0_u128);
            *amount = amount
                .checked_add(value)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        Ok(amounts
            .into_iter()
            .map(|(asset, atoms)| AssetReserve { asset, atoms })
            .collect())
    }

    /// 按最小可成交本金划分预算，保守覆盖任意成交拆分所需的费用。
    pub fn required_quote_budget_reserves(
        &self,
        fees: &matching_domain::FeeScheduleBook,
        version: u64,
        tier_id: u32,
        budget: matching_domain::QuoteBudget,
    ) -> Result<Vec<AssetReserve>, matching_domain::RejectReason> {
        use matching_domain::{FeeAssetPolicy, RejectReason};
        let schedule = fees.schedule(version)?;
        let tier = fees.resolve(version, tier_id)?;
        let rate = u128::try_from(tier.maker_bps.max(tier.taker_bps).max(0))
            .map_err(|_| RejectReason::ArithmeticOverflow)?;
        let lot = Qty::try_new(self.math.lot_size())?;
        let min = self
            .math
            .trade_notional(Price::try_new(self.math.tick_size())?, lot)
            .map_err(|error| {
                error
                    .to_reject_reason()
                    .unwrap_or(RejectReason::InvalidReservation)
            })?
            .get();
        let units = budget.get() / min;
        let (asset, basis) = match schedule.asset_policy() {
            FeeAssetPolicy::Base => (self.base, u128::from(self.math.lot_size())),
            FeeAssetPolicy::Quote => (self.quote, min),
            FeeAssetPolicy::Third(_) => return Err(RejectReason::InvalidReservation),
            FeeAssetPolicy::ThirdPriced {
                asset,
                quote_asset,
                numerator,
                denominator,
            } => {
                if asset == self.base || quote_asset != self.quote {
                    return Err(RejectReason::InvalidReservation);
                }
                (
                    asset,
                    crate::asset_scale::convert_ratio_exact(min, numerator, denominator)
                        .map_err(|_| RejectReason::InvalidReservation)?,
                )
            }
        };
        let product = basis
            .checked_mul(rate)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let per_unit = (product / 10_000)
            .checked_add(u128::from(product % 10_000 != 0))
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let extra = if asset == self.base {
            if per_unit > basis {
                per_unit
                    .checked_sub(basis)
                    .ok_or(RejectReason::ArithmeticOverflow)?
            } else {
                0
            }
        } else {
            per_unit
        };
        let extra = extra
            .checked_mul(units)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let mut amounts = std::collections::BTreeMap::from([(self.quote, budget.get())]);
        if extra > 0 {
            let value = amounts.entry(asset).or_insert(0_u128);
            *value = value
                .checked_add(extra)
                .ok_or(RejectReason::ArithmeticOverflow)?;
        }
        Ok(amounts
            .into_iter()
            .map(|(asset, atoms)| AssetReserve { asset, atoms })
            .collect())
    }

    /// 根据真实订单价格和数量计算按资产排序的需求。
    pub fn required_reserves(
        &self,
        side: Side,
        price: Price,
        qty: Qty,
    ) -> Result<Vec<AssetReserve>, LimitReserveError> {
        let principal = self
            .math
            .trade_notional(price, qty)
            .map_err(LimitReserveError::Math)?
            .get();
        match side {
            Side::Sell => Ok(vec![AssetReserve {
                asset: self.base,
                atoms: u128::from(qty.get()),
            }]),
            Side::Buy => {
                let lot = Qty::try_new(self.math.lot_size())
                    .map_err(|_| LimitReserveError::InvalidPolicy)?;
                let per_lot = self
                    .math
                    .trade_notional(price, lot)
                    .map_err(LimitReserveError::Math)?
                    .get();
                let product = per_lot
                    .checked_mul(u128::from(self.fee_bps))
                    .ok_or(LimitReserveError::Overflow)?;
                let fee_per_lot = product / 10_000 + u128::from(product % 10_000 != 0);
                let fee = fee_per_lot
                    .checked_mul(u128::from(qty.get() / self.math.lot_size()))
                    .ok_or(LimitReserveError::Overflow)?;
                let atoms = principal
                    .checked_add(fee)
                    .ok_or(LimitReserveError::Overflow)?;
                Ok(vec![AssetReserve {
                    asset: self.quote,
                    atoms,
                }])
            }
        }
    }
}
