//! 独立 Vec 参考订单簿的状态、构造与基础只读视图。
//! 挂单、逐笔撮合、限价策略、市价和冰山生命周期在各子模块实现。

use self::stop_orders::ReferenceStopOrder;
use crate::ReferenceOrder;
use matching_domain::{MathError, OrderId, PriceReferenceState, RejectReason, Side};
use matching_risk::ReservationState;
use std::collections::HashMap;

pub(crate) mod amend_candidate;
mod amend_iceberg;
mod amend_stop;
mod expiry;
mod iceberg;
mod limit_execution;
mod market_by_base;
mod market_by_quote;
mod matching;
mod resting;
mod stop_orders;
mod stop_trigger;

/// 为优先级计数器保留不可分配的高位保护空间。
///
/// 到达该边界前必须拒绝新挂单订单，避免计数器回绕后破坏先进先出语义。
const PRIORITY_GUARD: u128 = 1_024;

/// 单市场参考订单簿
///
/// 订单集合的原始顺序表示已知挂单先后。
#[derive(Debug, Default)]
pub struct ReferenceOrderBook {
    /// 挂单的权威存储顺序；同价先进先出的参考基线由该顺序加稳定排序得出。
    orders: Vec<ReferenceOrder>,
    /// 活动普通与未触发止损订单的本地预留快照，不决定参考排序。
    reservations: HashMap<OrderId, ReservationState>,
    /// 下一次实际挂单或冰山补量将取得的市场内优先级。
    next_priority: u128,
    /// 未触发止损订单的独立参考存储；源 Vec 保留同价登记顺序。
    stop_orders: Vec<ReferenceStopOrder>,
    /// 下一次未触发止损订单将取得的独立 StopPriority。
    next_stop_priority: u128,
    /// 各参考来源的最近已接受价格和序号。
    reference_prices: PriceReferenceState,
}

impl ReferenceOrderBook {
    /// 完整差分投影直接读取计数器，避免为读取数值重复格式化源订单。
    #[cfg(test)]
    pub(crate) fn test_next_queue_priority(&self) -> u128 {
        self.next_priority
    }

    /// 仅身份映射按标识排序；源 Vec 与所有业务队列顺序不变。
    #[cfg(test)]
    pub(crate) fn test_reservations(&self) -> Vec<(OrderId, ReservationState)> {
        let mut states = self
            .reservations
            .iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect::<Vec<_>>();
        states.sort_unstable_by_key(|(id, _)| *id);
        states
    }

    /// 供差分测试检查全部来源状态，验证触发检测仍更新而 root 保护参考保持冻结。
    #[cfg(test)]
    pub(crate) fn test_reference_state(&self) -> &PriceReferenceState {
        &self.reference_prices
    }

    /// 测试快照保留源 Vec、完整影子状态、参考价及两个分配器，不改变业务可见性。
    #[cfg(test)]
    pub(crate) fn test_deterministic_state(&self) -> (String, u128) {
        let mut reservations = self.reservations.iter().collect::<Vec<_>>();
        reservations.sort_unstable_by_key(|(id, _)| **id);
        (
            format!(
                "{:?}",
                (
                    &self.orders,
                    reservations,
                    self.next_priority,
                    &self.stop_orders,
                    self.next_stop_priority,
                    &self.reference_prices
                )
            ),
            self.next_priority,
        )
    }

    /// 创建不含挂单且优先级从零开始的单市场参考订单簿。
    pub fn new() -> Self {
        Self::default()
    }

    /// 在参考 Vec 变更前核对候选预留身份及既有影子状态。
    fn validate_new_reservation(
        &self,
        order_id: OrderId,
        user_id: matching_domain::UserId,
        state: &ReservationState,
    ) -> Result<(), RejectReason> {
        if self.contains_active_order(order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if state.order_id != order_id
            || state.user_id != user_id
            || self.reservations.contains_key(&order_id)
        {
            return Err(RejectReason::InvalidReservation);
        }
        Ok(())
    }

    /// 按订单身份只读查询已登记的本地预留快照。
    pub fn reservation_state(&self, order_id: OrderId) -> Option<&ReservationState> {
        self.reservations.get(&order_id)
    }

    /// 批量成交只清除已终结身份，未触发止损仍拥有同一预留。
    fn retain_active_reservations(&mut self) {
        let orders = &self.orders;
        let stops = &self.stop_orders;
        self.reservations.retain(|order_id, _| {
            orders
                .iter()
                .any(|order| order.original_order().order_id == *order_id)
                || stops
                    .iter()
                    .any(|entry| entry.order.order_id() == *order_id)
        });
    }

    /// 返回指定侧按价格优先、同价先进先出排列的只读视图
    ///
    /// 买入: 价格降序
    /// 卖出: 价格升序
    ///
    /// 使用稳定排序保留同价订单在源 Vec 中的顺序
    /// 排序仅作用于引用列表，不修改底层订单
    pub fn orders(&self, side: Side) -> Vec<&ReferenceOrder> {
        let mut result: Vec<_> = self
            .orders
            .iter()
            .filter(|order| order.original_order().side == side)
            .collect();
        result.sort_by(|a, b| {
            let a_price = a.original_order().price;
            let b_price = b.original_order().price;
            match side {
                Side::Buy => b_price.cmp(&a_price),
                Side::Sell => a_price.cmp(&b_price),
            }
        });
        result
    }

    /// 返回指定侧价格/先进先出优先级最高的订单。
    ///
    pub fn best(&self, side: Side) -> Option<&ReferenceOrder> {
        self.orders(side).into_iter().next()
    }

    fn reject_quote_math_error(error: MathError) -> RejectReason {
        error.to_reject_reason().unwrap_or_else(|| {
            panic!(
                "ReferenceOrderBook invariant violation: \
             non-rejectable MarketByQuote math error: {error:?}"
            )
        })
    }
}

#[cfg(test)]
mod tests;
