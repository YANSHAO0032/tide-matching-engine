//! 生产订单簿的未触发止损容器、取消与到期入口。

use super::ProductionOrderBook;
use matching_domain::{LogicalTime, OrderId, RejectReason, Side, StopOrder, StopPriority};

impl ProductionOrderBook {
    /// 登记一个未触发止损订单。
    ///
    /// 该入口只维护 StopBook 和触发前到期索引，不执行触发、child 下单或撮合。
    /// 订单标识在常规簿与 StopBook 间全局唯一；任一校验失败均不分配 StopPriority。
    pub fn place_stop(
        &mut self,
        order: StopOrder,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<StopPriority, RejectReason> {
        if self.contains_active_order(order.order_id()) {
            return Err(RejectReason::DuplicateOrderId);
        }
        self.stop_book.insert(
            order,
            now,
            current_session_id,
            &mut self.stop_priority_allocator,
        )
    }

    /// 查询一个未触发止损订单及其 StopPriority。
    pub fn stop_order(&self, order_id: OrderId) -> Option<(&StopOrder, StopPriority)> {
        self.stop_book.get(order_id)
    }

    /// 以确定性的触发顺序返回一个方向的未触发止损订单快照。
    pub fn stop_orders(&self, side: Side) -> Vec<(&StopOrder, StopPriority)> {
        self.stop_book.ordered(side)
    }

    /// 删除一个未触发止损订单。
    pub fn cancel_stop(&mut self, order_id: OrderId) -> Option<(StopOrder, StopPriority)> {
        self.stop_book.remove(order_id)
    }

    /// 使不晚于当前逻辑时间的 GTD 止损订单到期。
    pub fn expire_stop_gtd_through(&mut self, now: LogicalTime) -> Vec<(StopOrder, StopPriority)> {
        self.stop_book.expire_gtd_through(now)
    }

    /// 使指定交易时段结束时仍未触发的 DAY 止损订单到期。
    pub fn expire_stop_day_session(&mut self, session_id: u64) -> Vec<(StopOrder, StopPriority)> {
        self.stop_book.expire_day_session(session_id)
    }

    /// 校验未触发止损容器及其索引。
    pub fn validate_stop_book(&self) -> Result<(), &'static str> {
        self.stop_book.validate()
    }
}
