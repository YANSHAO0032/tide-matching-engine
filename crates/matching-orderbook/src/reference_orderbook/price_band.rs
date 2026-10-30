//! 单市场价格带配置、参考价边界和静态价格校验。

use super::ReferenceOrderBook;
use matching_domain::RejectReason;

impl ReferenceOrderBook {
    /// 更新价格带配置；无效基点不改变既有配置。
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

    /// 返回已安装的市场价格带配置和数值规则。
    pub fn price_band_config(
        &self,
    ) -> Option<(
        matching_domain::PriceBandConfig,
        matching_domain::MarketMath,
    )> {
        self.price_band
    }

    /// 从指定来源冻结参考价并计算双侧包含式边界。
    pub(super) fn price_band_bounds(
        &self,
    ) -> Result<Option<(matching_domain::Price, matching_domain::Price)>, RejectReason> {
        let Some((config, math)) = self.price_band else {
            return Ok(None);
        };
        let reference = self
            .reference_prices
            .latest(config.reference_kind)
            .ok_or(RejectReason::ReferencePriceUnavailable)?;
        config.bounds(reference.price, math.tick_size()).map(Some)
    }

    /// 验证限价位于当前已冻结参考价的双侧包含区间。
    pub(super) fn validate_band_price(
        &self,
        price: matching_domain::Price,
    ) -> Result<(), RejectReason> {
        if let Some((lower, upper)) = self.price_band_bounds()?
            && (price < lower || price > upper)
        {
            return Err(RejectReason::PriceBandExceeded);
        }
        Ok(())
    }
}
