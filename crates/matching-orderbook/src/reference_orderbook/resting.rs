//! 参考簿的普通挂单、优先级分配和按标识符取消。

use super::{PRIORITY_GUARD, ReferenceOrderBook};
use crate::ReferenceOrder;
use matching_domain::{LimitGtcOrder, OrderId, QueuePriority, RejectReason};

impl ReferenceOrderBook {
    /// 将订单加入当前市场的参考订单簿。
    ///
    /// 校验顺序：
    /// 1. 拒绝重复订单标识符；
    /// 2. 检查优先级高水位；
    /// 3. 检查溢出后递增；
    /// 4. 创建运行态订单并追加到源 Vec。
    ///
    /// 失败时不修改现有订单、剩余量或 next_priority。
    pub fn rest(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        // 必须先检查重复标识符，即使计数器已经耗尽也是如此。
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        if self.next_priority >= u128::MAX - PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }

        // 所有可能失败的检查均在修改状态之前执行。
        let next = self
            .next_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;

        let priority = QueuePriority::new(self.next_priority);
        let resting = ReferenceOrder::new(order, priority);

        self.orders.push(resting);
        self.next_priority = next;

        Ok(())
    }

    /// 按 OrderId 取消一张挂单订单。
    ///
    /// 订单不存在时返回 OrderNotFound，不修改订单簿。
    /// 成功时返回被移除的订单，不消耗或重排优先级。
    pub fn cancel(&mut self, order_id: OrderId) -> Result<ReferenceOrder, RejectReason> {
        let index = self
            .orders
            .iter()
            .position(|order| order.original_order().order_id == order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        Ok(self.orders.remove(index))
    }
}
