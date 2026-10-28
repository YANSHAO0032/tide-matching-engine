//! 生产簿的逻辑删除验证视图；只借用真实索引，不执行解除链接或资金提交。

use super::{
    ProductionOrderBook, RestPlan, RestTargetPlan,
    iceberg::{Budget, IcebergPlan, SliceScan},
};
use crate::OrderNode;
use matching_domain::{
    LimitGtcOrder, MarketMath, OrderId, Price, QueuePriority, RejectReason, Side, StopOrder,
    StopPriority, TimeInForce, TriggerExpiry,
};
use matching_risk::ReservationState;

/// 撤单替换计划的只读前置视图，不代表完整身份、资金或命令验证已完成。
///
/// 所有簿查询和预检都携带同一个排除身份；真实订单、链接、到期桶和预留保持原状。
#[derive(Clone, Copy, Debug)]
pub struct ProductionBookWithout<'a> {
    /// 被借用、从未实际删除订单的生产订单簿。
    book: &'a ProductionOrderBook,
    /// 在全部只读查询中逻辑排除的活动旧订单身份。
    excluded_order_id: OrderId,
}

impl PartialEq for ProductionBookWithout<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.excluded_order_id == other.excluded_order_id
            && self.orders(Side::Buy) == other.orders(Side::Buy)
            && self.orders(Side::Sell) == other.orders(Side::Sell)
            && self.stop_orders(Side::Buy) == other.stop_orders(Side::Buy)
            && self.stop_orders(Side::Sell) == other.stop_orders(Side::Sell)
            && self.book.queue_priority_allocator == other.book.queue_priority_allocator
            && self.book.stop_priority_allocator == other.book.stop_priority_allocator
            && self.book.regular_expiry == other.book.regular_expiry
            && self.book.reservations == other.book.reservations
            && self.book.reference_prices == other.book.reference_prices
    }
}

impl Eq for ProductionBookWithout<'_> {}

impl ProductionOrderBook {
    /// 确认旧身份属于支持的活动容器后，借用其逻辑删除视图。
    pub fn book_without(
        &self,
        old_order_id: OrderId,
    ) -> Result<ProductionBookWithout<'_>, RejectReason> {
        if !self.contains_active_order(old_order_id) {
            return Err(RejectReason::OrderNotFound);
        }
        if self
            .stop_book
            .get(old_order_id)
            .is_some_and(|(order, _)| matches!(order, StopOrder::Market(_)))
        {
            return Err(RejectReason::InvalidOrderCombination);
        }
        Ok(ProductionBookWithout {
            book: self,
            excluded_order_id: old_order_id,
        })
    }
}

impl<'a> ProductionBookWithout<'a> {
    /// 固定排除身份并取得生产片成交计划，后续提交直接消费本计划而不重新扫描。
    pub(super) fn plan_limit_fills(
        &self,
        order: &LimitGtcOrder,
        math: &MarketMath,
        post_only: bool,
    ) -> Result<IcebergPlan, RejectReason> {
        self.check_new_id(order.order_id)?;
        if post_only {
            self.preflight_post_only(order)?;
        }
        self.book.preflight_iceberg_scan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            SliceScan {
                max_fills: None,
                excluded_order_id: Some(self.excluded_order_id),
            },
        )
    }

    /// 冻结旧身份删除后的目标链尾、聚合值和未来分配器位置。
    pub(super) fn plan_rest(
        &self,
        order: &LimitGtcOrder,
        next_priority: u128,
        maker_releases_slot: bool,
    ) -> Result<RestPlan, RejectReason> {
        self.preflight_rest_capacity(order, next_priority, maker_releases_slot)?;
        let nodes = self
            .orders(order.side)
            .into_iter()
            .filter(|node| node.original_order().price == order.price)
            .collect::<Vec<_>>();
        let count_before =
            u32::try_from(nodes.len()).map_err(|_| RejectReason::ArithmeticOverflow)?;
        let total_before = nodes.iter().try_fold(0u64, |total, node| {
            total
                .checked_add(node.remaining())
                .ok_or(RejectReason::ArithmeticOverflow)
        })?;
        let count_after = count_before
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let total_after = total_before
            .checked_add(order.qty.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let target = if let Some(tail) = nodes.last() {
            RestTargetPlan::Existing {
                tail: self.book.orders[&tail.original_order().order_id],
                count_before,
                total_before,
                count_after,
                total_after,
            }
        } else {
            RestTargetPlan::New {
                count_after,
                total_after,
            }
        };
        Ok(RestPlan {
            order_id: order.order_id,
            side: order.side,
            price: order.price,
            priority: QueuePriority::new(next_priority),
            target,
        })
    }

    /// 按真实价格索引和先进先出链接遍历；排除整张旧单及其隐藏量。
    pub fn orders(&self, side: Side) -> Vec<&'a OrderNode> {
        let levels = match side {
            Side::Buy => &self.book.bids,
            Side::Sell => &self.book.asks,
        };
        let ordered: Box<dyn Iterator<Item = _>> = match side {
            Side::Buy => Box::new(levels.iter().rev()),
            Side::Sell => Box::new(levels.iter()),
        };
        let mut result = Vec::new();
        for (price, level) in ordered {
            let mut cursor = level.head();
            let mut previous = None;
            let mut count = 0u32;
            let mut total = 0u64;
            while let Some(index) = cursor {
                let node = self.book.arena.get(index).expect("view missing arena node");
                assert_eq!(node.prev(), previous);
                assert_eq!(node.original_order().side, side);
                assert_eq!(node.original_order().price, *price);
                assert_eq!(
                    self.book.orders.get(&node.original_order().order_id),
                    Some(&index)
                );
                count = count.checked_add(1).expect("view count overflow");
                assert!(count <= level.count(), "view FIFO cycle");
                total = total
                    .checked_add(node.remaining())
                    .expect("view aggregate overflow");
                if node.original_order().order_id != self.excluded_order_id {
                    result.push(node);
                }
                previous = Some(index);
                cursor = node.next();
            }
            assert_eq!(previous, level.tail());
            assert_eq!(count, level.count());
            assert_eq!(total, level.total_visible_qty());
        }
        result
    }

    /// 只返回仍有普通挂单的价格，不保留旧单独占的空价档。
    pub fn best_price(&self, side: Side) -> Option<Price> {
        self.orders(side)
            .first()
            .map(|node| node.original_order().price)
    }

    /// 按价格优先返回可见深度；隐藏量及旧身份均不计入。
    pub fn depth(&self, side: Side) -> Vec<(Price, u64)> {
        let mut result: Vec<(Price, u64)> = Vec::new();
        for node in self.orders(side) {
            let price = node.original_order().price;
            if let Some((last_price, total)) = result.last_mut()
                && *last_price == price
            {
                *total = total
                    .checked_add(node.remaining())
                    .expect("view depth overflow");
                continue;
            }
            result.push((price, node.remaining()));
        }
        result
    }

    /// 逻辑删除后普通活动索引的条目数；不修改真实映射容量。
    pub fn active_regular_count(&self) -> usize {
        self.book
            .orders
            .len()
            .checked_sub(usize::from(
                self.book.orders.contains_key(&self.excluded_order_id),
            ))
            .expect("old index count")
    }

    /// 查询过滤后的止损身份；止损从不成为普通挂单。
    pub fn stop_order(&self, id: OrderId) -> Option<(&'a StopOrder, StopPriority)> {
        if id == self.excluded_order_id {
            None
        } else {
            self.book.stop_book.get(id)
        }
    }

    /// 保留真实止损触发顺序，仅过滤旧身份。
    pub fn stop_orders(&self, side: Side) -> Vec<(&'a StopOrder, StopPriority)> {
        self.book
            .stop_book
            .ordered(side)
            .into_iter()
            .filter(|(order, _)| order.order_id() != self.excluded_order_id)
            .collect()
    }

    /// 返回普通订单的逻辑有效期引用；被排除身份没有到期候选。
    pub fn regular_expiry(&self, id: OrderId) -> Option<TimeInForce> {
        if id == self.excluded_order_id {
            None
        } else {
            self.book.order(id).map(OrderNode::time_in_force)
        }
    }

    /// 返回止损订单的逻辑触发前有效期引用。
    pub fn stop_expiry(&self, id: OrderId) -> Option<TriggerExpiry> {
        self.stop_order(id).map(|(order, _)| order.trigger_expiry())
    }

    /// 资金计划仍读取真实旧预留；逻辑删除不表示释放或重绑。
    pub fn reservation_state(&self, id: OrderId) -> Option<&'a ReservationState> {
        self.book.reservation_state(id)
    }

    /// 读取真实分配器；逻辑删除不返还已分配优先级。
    pub fn next_queue_priority(&self) -> u128 {
        self.book.next_queue_priority()
    }

    fn check_new_id(&self, id: OrderId) -> Result<(), RejectReason> {
        // 新身份不得复用旧身份，即使它已从可执行流动性中逻辑排除。
        if self.book.contains_active_order(id) {
            Err(RejectReason::DuplicateOrderId)
        } else {
            Ok(())
        }
    }

    /// 只挂单交叉预检使用逻辑最优价，包含等价价格。
    pub fn preflight_post_only(&self, order: &LimitGtcOrder) -> Result<(), RejectReason> {
        self.check_new_id(order.order_id)?;
        let crosses = match order.side {
            Side::Buy => self
                .best_price(Side::Sell)
                .is_some_and(|p| order.price >= p),
            Side::Sell => self.best_price(Side::Buy).is_some_and(|p| order.price <= p),
        };
        if crosses {
            Err(RejectReason::PostOnlyWouldCross)
        } else {
            Ok(())
        }
    }

    /// 使用生产只读补量扫描验证全成条件；不调用提交或参考模型。
    pub fn preflight_limit_fok(
        &self,
        order: &LimitGtcOrder,
        math: &MarketMath,
    ) -> Result<(), RejectReason> {
        self.check_new_id(order.order_id)?;
        math.trade_notional(order.price, order.qty)
            .map_err(ProductionOrderBook::reject_quote_math_error)?;
        let plan = self.book.preflight_iceberg_scan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            SliceScan {
                max_fills: None,
                excluded_order_id: Some(self.excluded_order_id),
            },
        )?;
        if plan.base_remaining > 0 {
            Err(RejectReason::FokInsufficientLiquidity)
        } else {
            Ok(())
        }
    }

    /// 核对逻辑删除后的挂单计数、可见聚合、槽位及优先级可行性。
    ///
    /// `next_priority` 必须来自本视图的成交补量预演；本方法不验证完整替换命令。
    pub fn preflight_rest(
        &self,
        order: &LimitGtcOrder,
        next_priority: u128,
    ) -> Result<(), RejectReason> {
        self.preflight_rest_capacity(order, next_priority, false)
    }

    /// 完整成交计划可证明全成挂单将释放槽位；孤立视图预检不能假定这一点。
    fn preflight_rest_capacity(
        &self,
        order: &LimitGtcOrder,
        next_priority: u128,
        maker_releases_slot: bool,
    ) -> Result<(), RejectReason> {
        self.check_new_id(order.order_id)?;
        if next_priority < self.next_queue_priority() {
            return Err(RejectReason::InvalidOrderCombination);
        }
        if next_priority
            >= self
                .book
                .queue_priority_allocator
                .policy()
                .effective_ceiling()
        {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let mut count = 0u32;
        let mut total = 0u64;
        for node in self
            .orders(order.side)
            .into_iter()
            .filter(|node| node.original_order().price == order.price)
        {
            count = count.checked_add(1).expect("existing view count overflow");
            total = total
                .checked_add(node.remaining())
                .expect("existing view aggregate overflow");
            assert!(node.priority().get() < next_priority);
        }
        count
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        total
            .checked_add(order.qty.get())
            .ok_or(RejectReason::ArithmeticOverflow)?;
        self.active_regular_count()
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        // 普通旧单和确定全成的挂单会释放槽位；仅删除 Stop 不会释放槽位。
        if !self.book.orders.contains_key(&self.excluded_order_id) && !maker_releases_slot {
            self.book.arena.preflight_insert()?;
        }
        Ok(())
    }
}
