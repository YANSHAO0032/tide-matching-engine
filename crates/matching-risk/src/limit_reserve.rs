//! 限价单的可信市场资金需求计算。

use matching_domain::{AssetId, AssetReserve, MarketMath, MathError, Price, Qty, Side};

/// 仅支持报价资产计费：买方额外预留，卖方从成交所得扣除。
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

    /// 返回锁定的费率版本。
    pub const fn fee_schedule_version(&self) -> u64 {
        self.fee_schedule_version
    }
    /// 返回锁定的费率层级。
    pub const fn fee_tier_id(&self) -> u32 {
        self.fee_tier_id
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
