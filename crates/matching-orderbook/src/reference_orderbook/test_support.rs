//! 参考订单簿跨模块测试所需的状态投影与受控故障注入。
//!
//! 本模块只在测试构建中编译；作为 `reference_orderbook` 的子模块，它可以读取父模块
//! 私有状态，同时避免将测试辅助逻辑留在生产实现文件中。

use super::{PRIORITY_GUARD, ReferenceOrderBook};
use matching_domain::{OrderId, PriceReferenceState};
use matching_risk::ReservationState;

impl ReferenceOrderBook {
    /// 完整差分投影直接读取计数器，避免为读取数值重复格式化源订单。
    pub(crate) fn test_next_queue_priority(&self) -> u128 {
        self.next_priority
    }

    /// 差分夹具恢复真实高水位，只供测试同时比较两种实现的耗尽边界。
    pub(crate) fn test_exhaust_queue_priority(&mut self) -> u128 {
        self.next_priority = u128::MAX - PRIORITY_GUARD;
        self.next_priority
    }

    /// 仅身份映射按标识排序；源 Vec 与所有业务队列顺序不变。
    pub(crate) fn test_reservations(&self) -> Vec<(OrderId, ReservationState)> {
        let mut states = self
            .reservations
            .iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect::<Vec<_>>();
        states.sort_unstable_by_key(|(id, _)| *id);
        states
    }

    /// 供差分测试检查全部来源状态，验证触发检测仍更新而根保护参考保持冻结。
    pub(crate) fn test_reference_state(&self) -> &PriceReferenceState {
        &self.reference_prices
    }

    /// 测试快照保留源 Vec、完整影子状态、参考价及两个分配器，不改变业务可见性。
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
}

/// 差分边界测试专用故障注入，不进入公开订单簿接口。
pub(crate) fn test_exhaust_priority(book: &mut ReferenceOrderBook, stop: bool) {
    if stop {
        book.next_stop_priority = u128::MAX - PRIORITY_GUARD;
    } else {
        book.next_priority = u128::MAX - PRIORITY_GUARD;
    }
}
