//! 挂单与取消的只读预检、结构提交和失败原子性边界。

use super::{ProductionOrderBook, RestPlan, RestTargetPlan};
use crate::{OrderArena, OrderIndex, OrderNode, PriceLevel};
use matching_domain::{
    LimitGtcOrder, LogicalTime, OrderId, Price, QueuePriority, RejectReason, Side, TimeInForce,
};
use matching_risk::ReservationState;
use std::collections::BTreeMap;

impl ProductionOrderBook {
    /// 在一次独占提交中挂入普通订单并登记预留状态。
    pub fn rest_with_reservation(
        &mut self,
        order: LimitGtcOrder,
        state: ReservationState,
    ) -> Result<(), RejectReason> {
        self.validate_new_reservation(order.order_id, order.user_id, &state)?;
        let order_id = order.order_id;
        self.rest(order)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(())
    }

    /// 完成时效预检后，原子挂单并保存调用方预留状态。
    pub fn rest_timed_with_reservation(
        &mut self,
        order: LimitGtcOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
        state: ReservationState,
    ) -> Result<(), RejectReason> {
        self.validate_new_reservation(order.order_id, order.user_id, &state)?;
        let order_id = order.order_id;
        self.rest_timed(order, tif, now, current_session_id)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(())
    }

    /// 将已确认不可成交的限价单 GTC 余量挂入对应价格档尾。
    ///
    /// 此窄接口不执行撮合或生成事件；调用方必须先保证其不会价格交叉/价格相等。
    /// 业务拒绝保持订单簿、Arena 和优先级分配器完全不变。
    ///
    /// # 不变量断言
    ///
    /// 若调用方传入会价格交叉或价格相等的订单，或内部状态/提交顺序损坏，
    /// 方法会立即停止处理而不是将其伪装成另一种业务拒绝。
    pub fn rest(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        // 1. 与价格交叉无关的全部可预期失败先预检。
        let plan = self.preflight_rest_common(&order)?;

        // 2. 公开挂单自身要求严格不可成交。
        //    后续完整 GTC 路径不会在成交前调用这一层。
        self.assert_rest_strictly_non_crossing(&plan);

        // 3. 此处开始不再允许预期业务 Err。
        self.commit_rest(order, plan);

        Ok(())
    }

    /// 将不可成交的普通 DAY/GTD/GTC 订单挂入订单簿并登记必要的到期索引。
    ///
    /// 时效和会话校验发生在优先级、Arena 或价格档位发生任何写入之前。
    pub fn rest_timed(
        &mut self,
        order: LimitGtcOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<(), RejectReason> {
        Self::validate_resting_tif(tif, now, current_session_id)?;
        let plan = self.preflight_rest_common(&order)?;
        self.assert_rest_strictly_non_crossing(&plan);
        self.commit_rest_timed(order, tif, plan);
        Ok(())
    }

    /// 校验普通可挂单订单的时效参数。
    pub(super) fn validate_resting_tif(
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<(), RejectReason> {
        match tif {
            TimeInForce::Gtc => Ok(()),
            TimeInForce::Gtd { expire_at } if expire_at > now => Ok(()),
            TimeInForce::Gtd { .. } => Err(RejectReason::InvalidExpireTime),
            TimeInForce::Day { session_id } if session_id == current_session_id => Ok(()),
            TimeInForce::Day { .. } => Err(RejectReason::InvalidSession),
            TimeInForce::Ioc | TimeInForce::Fok => Err(RejectReason::InvalidOrderCombination),
        }
    }

    /// 强制公开 `rest` 的非成交边界。
    ///
    /// 公共挂单预检故意不检查对手侧，使完整限价单 GTC 能在
    /// 连续挂单撮合前预检余量。调用该辅助函数的位置必须已经
    /// 明确：此时余量不应再与任何最优对手价价格交叉或价格相等。
    pub(super) fn assert_rest_strictly_non_crossing(&self, plan: &RestPlan) {
        match plan.side {
            Side::Buy => {
                if let Some((best_ask, _)) = self.asks.first_key_value() {
                    assert!(
                        plan.price < *best_ask,
                        "ProductionOrderBook invariant violation: \
                     resting buy would cross or lock best ask: \
                     order_id={:?}, price={:?}, best_ask={best_ask:?}",
                        plan.order_id,
                        plan.price,
                    );
                }
            }

            Side::Sell => {
                if let Some((best_bid, _)) = self.bids.last_key_value() {
                    assert!(
                        plan.price > *best_bid,
                        "ProductionOrderBook invariant violation: \
                     resting sell would cross or lock best bid: \
                     order_id={:?}, price={:?}, best_bid={best_bid:?}",
                        plan.order_id,
                        plan.price,
                    );
                }
            }
        }
    }

    /// 提交已经预检的挂单，且不再暴露可预期的业务拒绝。
    ///
    /// 本函数只允许在单写者已持有 `RestPlan` 的写屏障之后调用。
    /// 预检/提交之间的身份、优先级、Arena 或目标档位分歧表示
    /// 内部损坏，必须触发不变量断言并停止处理，不能在已经写入部分状态后返回 Err。
    pub(super) fn commit_rest(&mut self, order: LimitGtcOrder, plan: RestPlan) {
        self.commit_rest_timed(order, TimeInForce::Gtc, plan);
    }

    /// 提交已通过所有时效、索引和挂单预检的普通订单。
    pub(super) fn commit_rest_timed(
        &mut self,
        order: LimitGtcOrder,
        tif: TimeInForce,
        plan: RestPlan,
    ) {
        // ------------------------------------------------------------
        // 0. 计划 / 输入身份。
        // ------------------------------------------------------------

        assert_eq!(
            order.order_id, plan.order_id,
            "rest commit received plan for different OrderId"
        );

        assert_eq!(
            order.side, plan.side,
            "rest commit received plan for different side"
        );

        assert_eq!(
            order.price, plan.price,
            "rest commit received plan for different price"
        );

        // 公共预检后不能出现同标识符。
        assert!(
            !self.contains_active_order(plan.order_id),
            "ProductionOrderBook invariant violation: \
         OrderId appeared between rest preflight and commit"
        );

        // 此普通订单提交路径的成交和取消不消耗 QueuePriority。
        assert_eq!(
            self.queue_priority_allocator.next(),
            plan.priority.get(),
            "ProductionOrderBook invariant violation: \
         QueuePriority changed between rest preflight and commit"
        );

        assert!(
            !self.queue_priority_allocator.is_exhausted(),
            "ProductionOrderBook invariant violation: \
         allocator became exhausted between rest preflight and commit"
        );

        // Arena 的具体空闲槽位可以变化，但插入能力不能消失。
        self.arena.preflight_insert().unwrap_or_else(|reason| {
            panic!(
                "ProductionOrderBook invariant violation: \
                 Arena insert feasibility changed after rest preflight: \
                 reason={reason:?}"
            )
        });

        let node = OrderNode::new_timed(order, plan.priority, tif);

        let slot = match plan.side {
            Side::Buy => commit_rest_target(&mut self.bids, &mut self.arena, node, &plan),

            Side::Sell => commit_rest_target(&mut self.asks, &mut self.arena, node, &plan),
        };

        let previous = self.orders.insert(plan.order_id, slot);

        assert!(
            previous.is_none(),
            "ProductionOrderBook invariant violation: \
         duplicate OrderId appeared during rest commit"
        );

        self.regular_expiry.insert(plan.order_id, tif);

        // 优先级必须最后推进。
        let committed_priority =
            self.queue_priority_allocator
                .allocate()
                .unwrap_or_else(|reason| {
                    panic!(
                        "ProductionOrderBook invariant violation: \
                 preflighted QueuePriority allocation failed during commit: \
                 reason={reason:?}"
                    )
                });

        assert_eq!(
            committed_priority, plan.priority,
            "ProductionOrderBook invariant violation: \
         committed priority differs from RestPlan"
        );
    }

    /// 按已知 OrderId 从生产订单簿中取消挂单订单。
    ///
    /// 成功返回被移除的运行态订单；返回节点已经脱离 PriceLevel，
    /// 其 `prev` / `next` 均为 None。
    ///
    /// 取消不分配、消耗或重排 QueuePriority。
    ///
    /// # 错误
    ///
    /// - `OrderNotFound`：当前活动订单簿中不存在该 OrderId。
    ///
    /// # 不变量断言
    ///
    /// 如果标识符索引指向不存在的 Arena 槽位、节点身份不匹配、
    /// 对应价格档不存在或 PriceLevel 内部链接损坏，视为内部状态损坏，
    /// 故障时停止处理。
    pub fn cancel(&mut self, order_id: OrderId) -> Result<OrderNode, RejectReason> {
        // 1. 先按标识符精确定位。
        //
        // 未知标识符必须在任何写操作之前返回。
        let index = match self.orders.get(&order_id) {
            Some(index) => *index,
            None => return Err(RejectReason::OrderNotFound),
        };

        let node = self.arena.get(index).unwrap_or_else(|| {
            panic!(
                "ProductionOrderBook invariant violation: \
             cancel index references missing arena slot: \
             order_id={order_id:?}, index={index:?}"
            )
        });

        assert_eq!(
            node.original_order().order_id,
            order_id,
            "ProductionOrderBook invariant violation: \
         cancel index points to different order: \
         requested={order_id:?}, actual={:?}, index={index:?}",
            node.original_order().order_id,
        );

        let side = node.original_order().side;
        let price = node.original_order().price;

        // 不可变借用在此结束。
        let removed = match side {
            Side::Buy => {
                let remove_level;
                let removed = {
                    let level = self.bids.get_mut(&price).unwrap_or_else(|| {
                        panic!(
                            "ProductionOrderBook invariant violation: \
                         cancel bid level missing: \
                         order_id={order_id:?}, price={price:?}"
                        )
                    });
                    // 3. 头节点必须走 pop_front；
                    // 中间或尾节点走解除链接。
                    let removed = if level.head() == Some(index) {
                        level.pop_front(&mut self.arena).unwrap_or_else(|| {
                            panic!(
                                "ProductionOrderBook invariant violation: \
                             non-empty indexed bid unexpectedly returned None: \
                             order_id={order_id:?}, index={index:?}"
                            )
                        })
                    } else {
                        level.unlink(&mut self.arena, index)
                    };
                    remove_level = level.is_empty();
                    removed
                };

                if remove_level {
                    let level = self
                        .bids
                        .remove(&price)
                        .expect("prechecked empty bid level must exist");

                    assert!(level.is_empty(), "removed bid level must be empty");
                }
                removed
            }
            Side::Sell => {
                let remove_level;
                let removed = {
                    let level = self.asks.get_mut(&price).unwrap_or_else(|| {
                        panic!(
                            "ProductionOrderBook invariant violation: \
                         cancel ask level missing: \
                         order_id={order_id:?}, price={price:?}"
                        )
                    });
                    let removed = if level.head() == Some(index) {
                        level.pop_front(&mut self.arena).unwrap_or_else(|| {
                            panic!(
                                "ProductionOrderBook invariant violation: \
                             non-empty indexed ask unexpectedly returned None: \
                             order_id={order_id:?}, index={index:?}"
                            )
                        })
                    } else {
                        level.unlink(&mut self.arena, index)
                    };
                    remove_level = level.is_empty();
                    removed
                };
                if remove_level {
                    let level = self
                        .asks
                        .remove(&price)
                        .expect("prechecked empty ask level must exist");
                    assert!(level.is_empty(), "removed ask level must be empty");
                }
                removed
            }
        };

        // 4. 解除标识符 -> 槽位映射。
        //
        // 从最初查找到这里一直持有 `&mut self` 的独占访问，
        // 映射不可能合法地发生变化。
        let mapped = self.orders.remove(&order_id);

        assert_eq!(
            mapped,
            Some(index),
            "ProductionOrderBook invariant violation: \
         cancel mapping changed during mutation"
        );

        self.regular_expiry
            .remove(order_id, removed.time_in_force());
        self.reservations.remove(&order_id);

        Ok(removed)
    }

    /// 预检任何将要挂单的限价单 GTC 载荷，而不检查对手侧价格。
    ///
    /// 业务拒绝顺序固定为重复标识符、优先级高水位、同侧聚合/计数
    /// 与 Arena 可行性。这样完整限价单 GTC 可以先模拟成交并用实际
    /// 余量调用本函数，确保所有可预期挂单错误都发生在首笔成交前。
    pub(super) fn preflight_rest_common(
        &self,
        order: &LimitGtcOrder,
    ) -> Result<RestPlan, RejectReason> {
        let order_id = order.order_id;
        let side = order.side;
        let price = order.price;
        let qty = order.qty.get();

        // ------------------------------------------------------------
        // 1. 重复标识符必须绝对最先检查。
        // ------------------------------------------------------------

        if self.contains_active_order(order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // ------------------------------------------------------------
        // 2. QueuePriority 高水位。
        // ------------------------------------------------------------

        if self.queue_priority_allocator.is_exhausted() {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }

        let priority = QueuePriority::new(self.queue_priority_allocator.next());

        // ------------------------------------------------------------
        // 3. 同侧目标 PriceLevel 只读预检。
        // ------------------------------------------------------------

        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };

        let target = if let Some(level) = levels.get(&price) {
            assert!(
                level.count() > 0,
                "ProductionOrderBook invariant violation: \
             rest target level is empty"
            );

            assert!(
                level.head().is_some(),
                "ProductionOrderBook invariant violation: \
             rest target level missing head"
            );

            let tail_index = level.tail().expect(
                "ProductionOrderBook invariant violation: \
                 rest target level missing tail",
            );

            assert!(
                level.total_visible_qty() > 0,
                "ProductionOrderBook invariant violation: \
             non-empty rest target has zero aggregate"
            );

            let tail = self.arena.get(tail_index).expect(
                "ProductionOrderBook invariant violation: \
                 rest target tail missing from Arena",
            );

            assert!(
                tail.next().is_none(),
                "ProductionOrderBook invariant violation: \
             rest target tail has successor"
            );

            assert!(
                tail.remaining() > 0,
                "ProductionOrderBook invariant violation: \
             rest target tail is exhausted"
            );

            assert_eq!(
                tail.original_order().side,
                side,
                "ProductionOrderBook invariant violation: \
             rest target tail side mismatch"
            );

            assert_eq!(
                tail.original_order().price,
                price,
                "ProductionOrderBook invariant violation: \
             rest target tail price mismatch"
            );

            assert!(
                priority > tail.priority(),
                "ProductionOrderBook invariant violation: \
             planned rest priority is not strictly greater than tail"
            );

            let count_before = level.count();
            let total_before = level.total_visible_qty();

            let count_after = count_before
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            let total_after = total_before
                .checked_add(qty)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            RestTargetPlan::Existing {
                tail: tail_index,
                count_before,
                total_before,
                count_after,
                total_after,
            }
        } else {
            // 新档位本身不会发生聚合加法溢出：
            // Qty 已经是合法正 u64。
            RestTargetPlan::New {
                count_after: 1,
                total_after: qty,
            }
        };

        // ------------------------------------------------------------
        // 4. Arena `insert` 可行性。
        //
        // 必须是公共预检的最后一个可预期失败点。
        // ------------------------------------------------------------

        self.arena.preflight_insert()?;

        Ok(RestPlan {
            order_id,
            side,
            price,
            priority,
            target,
        })
    }
}

/// 将已预检 `node` 接入目标价格档，并返回它的稳定 Arena 槽位。
///
/// 调用方随后才写入 OrderId 索引并推进分配器。这里的所有断言都在
/// `push_back` 或插入新档位前比较预检快照；任何差异是内部状态漂移，
/// 不能退化为业务 `RejectReason`。
pub(super) fn commit_rest_target(
    levels: &mut BTreeMap<Price, PriceLevel>,
    arena: &mut OrderArena,
    node: OrderNode,
    plan: &RestPlan,
) -> OrderIndex {
    match plan.target {
        RestTargetPlan::Existing {
            tail,
            count_before,
            total_before,
            count_after,
            total_after,
        } => {
            let level = levels.get_mut(&plan.price).expect(
                "ProductionOrderBook invariant violation: \
                     preflighted existing rest level disappeared",
            );

            // 所有差异检查仍发生在 push_back 写入前。
            assert_eq!(
                level.tail(),
                Some(tail),
                "ProductionOrderBook invariant violation: \
                 rest target tail changed after preflight"
            );

            assert_eq!(
                level.count(),
                count_before,
                "ProductionOrderBook invariant violation: \
                 rest target count changed after preflight"
            );

            assert_eq!(
                level.total_visible_qty(),
                total_before,
                "ProductionOrderBook invariant violation: \
                 rest target aggregate changed after preflight"
            );

            let slot = level.push_back(arena, node).unwrap_or_else(|reason| {
                panic!(
                    "ProductionOrderBook invariant violation: \
                         preflighted existing-level rest failed during commit: \
                         reason={reason:?}"
                )
            });

            assert_eq!(
                level.count(),
                count_after,
                "rest commit count differs from RestPlan"
            );

            assert_eq!(
                level.total_visible_qty(),
                total_after,
                "rest commit aggregate differs from RestPlan"
            );

            slot
        }

        RestTargetPlan::New {
            count_after,
            total_after,
        } => {
            assert!(
                !levels.contains_key(&plan.price),
                "ProductionOrderBook invariant violation: \
                 new rest level appeared after preflight"
            );

            let mut level = PriceLevel::new();

            let slot = level.push_back(arena, node).unwrap_or_else(|reason| {
                panic!(
                    "ProductionOrderBook invariant violation: \
                         preflighted new-level rest failed during commit: \
                         reason={reason:?}"
                )
            });

            assert_eq!(level.count(), count_after);
            assert_eq!(level.total_visible_qty(), total_after);

            let previous = levels.insert(plan.price, level);

            assert!(
                previous.is_none(),
                "ProductionOrderBook invariant violation: \
                 new rest level appeared during commit"
            );

            slot
        }
    }
}
