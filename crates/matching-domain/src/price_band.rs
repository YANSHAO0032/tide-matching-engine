//! 市场价格带的来源和复用市价保护整数数学的双侧边界。

use crate::{
    Price, PriceReferenceKind, RejectReason, Side, market_order::slippage_protection_price,
};

/// 市场价格带引用的持久价格来源及最大偏离基点。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriceBandConfig {
    /// 计算价格带所需的参考价格来源。
    pub reference_kind: PriceReferenceKind,
    /// 相对参考价格允许的最大偏离基点。
    pub max_deviation_bps: u32,
}

impl PriceBandConfig {
    /// 使用已有方向取整和 tick 对齐规则计算包含式下界与上界。
    ///
    /// 基点不在 `0..=9_999` 时返回 `InvalidSlippageBps`，非正价格步长返回
    /// `InvalidTick`，两者均无效时优先返回 `InvalidSlippageBps`；不可表示的
    /// 边界返回 `ArithmeticOverflow`。参考价未对齐且基点较小时，下界可能高于
    /// 上界，此时不存在满足两侧边界的合法 tick 价格。
    pub fn bounds(self, reference: Price, tick_size: i64) -> Result<(Price, Price), RejectReason> {
        let lower =
            slippage_protection_price(reference, Side::Sell, self.max_deviation_bps, tick_size)?;
        let upper =
            slippage_protection_price(reference, Side::Buy, self.max_deviation_bps, tick_size)?;
        Ok((lower, upper))
    }
}
