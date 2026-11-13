//! 单市场价格带配置、参考价边界和静态价格校验。

use super::ProductionOrderBook;
use matching_domain::{Price, RejectReason};

impl ProductionOrderBook {
    /// 在副本中验证外部参考价；动态来源仅在不会触发止损单时适用此窄入口。
    ///
    /// 调用方须将价格带重校验、真实预留释放和持久提交放在同一候选中。
    /// 存在跨越触发时必须使用完整触发级联，不能从此入口绕过。
    pub fn plan_external_reference_without_trigger(
        &self,
        reference: matching_domain::PriceReference,
    ) -> Result<Self, RejectReason> {
        let mut candidate = self.clone();
        let previous = candidate.reference_prices.advance_external(reference)?;
        if let Some(previous) = previous
            && reference.kind != matching_domain::PriceReferenceKind::StaticOpening
        {
            let side = if reference.price > previous {
                Some(matching_domain::Side::Buy)
            } else if reference.price < previous {
                Some(matching_domain::Side::Sell)
            } else {
                None
            };
            if let Some(side) = side
                && self.stop_orders(side).into_iter().any(|(stop, _)| {
                    stop.trigger_reference() == reference.kind
                        && match side {
                            matching_domain::Side::Buy => {
                                previous < stop.stop_price() && stop.stop_price() <= reference.price
                            }
                            matching_domain::Side::Sell => {
                                reference.price <= stop.stop_price() && stop.stop_price() < previous
                            }
                        }
                })
            {
                return Err(RejectReason::InvalidOrderCombination);
            }
        }
        if candidate
            .price_band
            .is_some_and(|(config, _)| config.reference_kind == reference.kind)
        {
            candidate.price_band_bounds()?;
        }
        Ok(candidate)
    }
    /// 判断指定来源的最新参考价格是否存在。
    pub fn has_reference_price(&self, kind: matching_domain::PriceReferenceKind) -> bool {
        self.reference_prices.latest(kind).is_some()
    }
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
    pub fn price_band_bounds(&self) -> Result<Option<(Price, Price)>, RejectReason> {
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
