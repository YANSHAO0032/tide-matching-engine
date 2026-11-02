//! 生产订单簿与分片配置的市场归属。
use super::ProductionOrderBook;
use matching_domain::MarketId;

/// 市场绑定与现有订单簿状态冲突。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketBindingError;

impl ProductionOrderBook {
    /// 将空簿绑定到可信市场，绑定后不可更换市场。
    pub fn bind_market(&mut self, market_id: MarketId) -> Result<(), MarketBindingError> {
        match self.market_id {
            Some(current) if current == market_id => Ok(()),
            Some(_) => Err(MarketBindingError),
            None if self.orders.is_empty()
                && self
                    .stop_book
                    .ordered(matching_domain::Side::Buy)
                    .is_empty()
                && self
                    .stop_book
                    .ordered(matching_domain::Side::Sell)
                    .is_empty()
                && self.reservations.is_empty() =>
            {
                self.market_id = Some(market_id);
                Ok(())
            }
            None => Err(MarketBindingError),
        }
    }
    /// 输出按订单身份排序的预留快照，供分片核验全部市场归属。
    pub fn reservation_snapshots(
        &self,
    ) -> Vec<(matching_domain::OrderId, &matching_risk::ReservationState)> {
        let mut entries: Vec<_> = self
            .reservations
            .iter()
            .map(|(id, state)| (*id, state))
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        entries
    }

    /// 绑定市场时仅校验其完整归属；旧独立簿继续使用原全量契约。
    pub(super) fn validate_registry_bindings<'a>(
        &self,
        registry: &matching_risk::ReservationRegistry,
        states: impl IntoIterator<
            Item = (
                matching_domain::OrderId,
                &'a matching_risk::ReservationState,
            ),
        >,
    ) -> Result<(), matching_domain::RejectReason> {
        match self.market_id {
            Some(market) => registry.validate_market_order_bindings(market, states),
            None => registry.validate_order_bindings(states),
        }
    }
}
