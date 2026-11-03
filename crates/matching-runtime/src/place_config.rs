//! 正式限价下单使用的可信静态市场与有界容量配置。

use matching_domain::MarketId;
use matching_risk::{LimitReservePolicy, RiskKeySet};
use std::collections::BTreeMap;

/// 正式下单活动订单及客户端身份的显式容量限制。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaceLimits {
    /// 分片允许的全部活动订单上限，而不仅是带客户端身份的订单。
    pub active_orders: usize,
    /// 客户端身份终结历史的最大保留数量。
    pub terminal_history: usize,
    /// 每个客户端身份允许的最大 UTF-8 字节数。
    pub client_id_bytes: usize,
}

/// 启动前由可信配置提供的限价资金策略和风控密钥。
///
/// 调用方须保证策略与实际固定费率版本、层级和资产政策一致；
/// 当前策略仅支持报价资产计费，且不提供持久化或远程查询。
#[derive(Clone, Debug)]
pub struct PlaceSupportConfig {
    /// 本地已加载的风控验签公钥。
    pub(crate) keys: RiskKeySet,
    /// 按市场身份确定性排列的可信数值与预留策略。
    pub(crate) policies: BTreeMap<MarketId, LimitReservePolicy>,
    /// 分片内资源边界。
    pub(crate) limits: PlaceLimits,
}

/// 正式下单配置不满足市场唯一性或容量约束。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaceConfigError;

impl PlaceSupportConfig {
    /// 建立可信市场配置；拒绝重复市场与无效容量。
    pub fn new(
        keys: RiskKeySet,
        policies: impl IntoIterator<Item = (MarketId, LimitReservePolicy)>,
        limits: PlaceLimits,
    ) -> Result<Self, PlaceConfigError> {
        if limits.active_orders == 0 || limits.terminal_history == 0 || limits.client_id_bytes == 0
        {
            return Err(PlaceConfigError);
        }
        let mut ordered = BTreeMap::new();
        for (market, policy) in policies {
            if ordered.insert(market, policy).is_some() {
                return Err(PlaceConfigError);
            }
        }
        if ordered.is_empty() {
            return Err(PlaceConfigError);
        }
        Ok(Self {
            keys,
            policies: ordered,
            limits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{AssetId, MarketId, MarketMath};

    #[test]
    fn place_configuration_rejects_zero_capacity_and_duplicate_market() {
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let policy =
            LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
        let keys = RiskKeySet::new(1, []).unwrap();
        let limits = PlaceLimits {
            active_orders: 1,
            terminal_history: 1,
            client_id_bytes: 128,
        };
        assert!(
            PlaceSupportConfig::new(
                keys.clone(),
                [(MarketId(1), policy), (MarketId(1), policy)],
                limits
            )
            .is_err()
        );
        assert!(
            PlaceSupportConfig::new(
                keys.clone(),
                [(MarketId(1), policy)],
                PlaceLimits {
                    active_orders: 0,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(
            PlaceSupportConfig::new(
                keys.clone(),
                [(MarketId(1), policy)],
                PlaceLimits {
                    terminal_history: 0,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(PlaceSupportConfig::new(keys, [(MarketId(1), policy)], limits).is_ok());
    }
}
