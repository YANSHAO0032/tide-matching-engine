//! 单市场价格带配置、参考价边界和静态价格校验。

use super::ProductionOrderBook;
use matching_domain::{Price, RejectReason};

impl ProductionOrderBook {
    /// 设置可选价格带及其对应的市场数值规则，非法基点不得改变配置。
    pub fn set_price_band_config(
        &mut self,
        config: Option<(
            matching_domain::PriceBandConfig,
            matching_domain::MarketMath,
        )>,
    ) -> Result<(), RejectReason> {
        if config.is_some_and(|(band, _)| band.max_deviation_bps >= 10_000) {
            return Err(RejectReason::InvalidSlippageBps);
        }
        self.price_band = config;
        Ok(())
    }

    /// 读取当前生效的价格带配置和市场数值规则。
    pub fn price_band_config(
        &self,
    ) -> Option<(
        matching_domain::PriceBandConfig,
        matching_domain::MarketMath,
    )> {
        self.price_band
    }

    /// 从指定来源当前参考价冻结包含式价格带。
    pub(super) fn price_band_bounds(&self) -> Result<Option<(Price, Price)>, RejectReason> {
        self.price_band
            .map(|(band, math)| {
                let reference = self
                    .reference_prices
                    .latest(band.reference_kind)
                    .ok_or(RejectReason::ReferencePriceUnavailable)?;
                band.bounds(reference.price, math.tick_size())
            })
            .transpose()
    }

    /// 验证限价位于命令开始时的包含式市场价格带内。
    pub(super) fn validate_band_price(&self, price: Price) -> Result<(), RejectReason> {
        if let Some((lower, upper)) = self.price_band_bounds()?
            && (price < lower || price > upper)
        {
            return Err(RejectReason::PriceBandExceeded);
        }
        Ok(())
    }
}
