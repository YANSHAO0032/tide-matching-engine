//! 撤单替换的独立参考验证视图；不提供执行或资金状态提交入口。

use super::{ReferenceOrderBook, iceberg::SliceRequest};
use crate::ReferenceOrder;
use matching_domain::{
    LimitGtcOrder, MarketMath, OrderId, RejectReason, Side, StopOrder, StopPriority,
};

/// 完整过滤旧身份的不可变参考簿视图，是后续撤单替换计划的只读输入。
///
/// 本类型只证明簿相关预检所见的状态，不代表身份、资金、有效期及提交可行性均已验证。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceBookWithout {
    /// 在全部只读查询中逻辑排除的活动旧订单身份。
    excluded_order_id: OrderId,
    /// 移除了旧订单但保留预留与分配器状态的参考簿副本。
    book: ReferenceOrderBook,
}

impl ReferenceOrderBook {
    /// 构造独立的只读验证副本；不移除真实订单，不更新任何预留或分配器。
    pub fn book_without(
        &self,
        old_order_id: OrderId,
    ) -> Result<ReferenceBookWithout, RejectReason> {
        if !self.contains_active_order(old_order_id) {
            return Err(RejectReason::OrderNotFound);
        }
        if self.stop_orders.iter().any(|entry| {
            entry.order.order_id() == old_order_id && matches!(entry.order, StopOrder::Market(_))
        }) {
            return Err(RejectReason::InvalidOrderCombination);
        }
        // 普通和冰山共享完整运行态；过滤整张订单同时排除可见量、隐藏量及未来补量。
        // 时效信息保存在各自载荷内，过滤载荷也排除其逻辑到期引用。
        let book = Self {
            orders: self
                .orders
                .iter()
                .filter(|order| order.original_order().order_id != old_order_id)
                .cloned()
                .collect(),
            stop_orders: self
                .stop_orders
                .iter()
                .filter(|entry| entry.order.order_id() != old_order_id)
                .cloned()
                .collect(),
            // 旧预留仍供后续独立资金计划读取；视图不代表释放或重绑预留。
            reservations: self.reservations.clone(),
            next_priority: self.next_priority,
            next_stop_priority: self.next_stop_priority,
            reference_prices: self.reference_prices.clone(),
        };
        Ok(ReferenceBookWithout {
            excluded_order_id: old_order_id,
            book,
        })
    }
}

impl ReferenceBookWithout {
    /// 将已过滤的独立副本交给同级纯计划；旧影子由资金过渡计划另行处理。
    pub(super) fn into_candidate(self) -> ReferenceOrderBook {
        self.book
    }

    /// 返回同侧价格优先及同价先进先出的普通挂单；可见量与隐藏量仍分别保留。
    pub fn orders(&self, side: Side) -> Vec<&ReferenceOrder> {
        self.book.orders(side)
    }

    /// 查询排除旧身份后的同侧最优普通挂单。
    pub fn best(&self, side: Side) -> Option<&ReferenceOrder> {
        self.book.best(side)
    }

    /// 查询排除旧身份后的未触发止损订单，绝不转换成普通流动性。
    pub fn stop_order(&self, id: OrderId) -> Option<(&StopOrder, StopPriority)> {
        self.book.stop_order(id)
    }

    /// 按独立止损触发顺序返回过滤后的只读列表。
    pub fn stop_orders(&self, side: Side) -> Vec<(&StopOrder, StopPriority)> {
        self.book.stop_orders(side)
    }

    /// 使用既有限价预检核对包含等价价格的只挂单交叉条件，不分配优先级。
    pub fn preflight_post_only(&self, order: &LimitGtcOrder) -> Result<(), RejectReason> {
        self.book.preflight_post_only(order)
    }

    /// 复用独立参考模型的普通及冰山只读计划，检查限价立即全成条件。
    ///
    /// 冰山补量及其优先级、聚合与精确本金检查仅作用于可丢弃模拟状态。
    pub fn preflight_limit_fok(
        &self,
        order: &LimitGtcOrder,
        math: &MarketMath,
    ) -> Result<(), RejectReason> {
        let remaining = if self.book.has_iceberg_maker(order.side) {
            self.book.check_duplicate(order.order_id)?;
            math.trade_notional(order.price, order.qty)
                .map_err(ReferenceOrderBook::reject_quote_math_error)?;
            self.book
                .plan_slices(SliceRequest {
                    id: order.order_id,
                    side: order.side,
                    remaining: order.qty.get(),
                    limit: Some(order.price),
                    budget: None,
                    math: Some(math),
                    protection: None,
                    one_fill: false,
                })?
                .remaining
        } else {
            self.book.preflight_limit_immediate(order, math)?.remaining
        };
        if remaining != 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        Ok(())
    }
}
