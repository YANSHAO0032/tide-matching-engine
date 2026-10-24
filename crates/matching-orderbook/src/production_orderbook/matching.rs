//! 基础限价成交、连续撮合及共享的只读链表预检。

use super::{FillPlan, ProductionOrderBook};
use crate::{IncomingOrder, PriceLevel};
use matching_domain::{Price, Qty, RejectReason, Side, TradeEvent, TradeId};

impl ProductionOrderBook {
    /// 对当前新到订单最多执行一笔成交。
    ///
    /// - 仅选择当前对手盘最优价格的先进先出头节点挂单；
    /// - 成交价始终取挂单价格；
    /// - 挂单严格部分成交时保留原槽位/先进先出/优先级；
    /// - 挂单完全成交时通过 `pop_front` 移除；
    /// - 不循环到下一挂单；
    /// - 不将新到订单剩余量重新挂簿；
    /// - TradeId 由调用方提供，本方法不分配。
    ///
    /// 无可成交挂单或新到订单已耗尽时返回 `Ok(None)`，
    /// 且订单簿、新到订单、分配器全部保持不变。
    pub fn execute_next_fill_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.original_order().side) {
            return self.execute_next_fill_iceberg(incoming, trade_id);
        }
        // ------------------------------------------------------------
        // 1. 已耗尽新到订单：不改变任何状态。
        // ------------------------------------------------------------

        if incoming.remaining() == 0 {
            return Ok(None);
        }

        let taker = incoming.original_order();
        let taker_order_id = taker.order_id;
        let taker_side = taker.side;
        let taker_price = taker.price;

        let maker_side = match taker_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        // ------------------------------------------------------------
        // 2. 只读选择最优价格。
        // ------------------------------------------------------------

        let maker_price = match taker_side {
            Side::Buy => {
                let Some((price, _)) = self.asks.first_key_value() else {
                    return Ok(None);
                };

                if taker_price < *price {
                    return Ok(None);
                }

                *price
            }

            Side::Sell => {
                let Some((price, _)) = self.bids.last_key_value() else {
                    return Ok(None);
                };

                if taker_price > *price {
                    return Ok(None);
                }

                *price
            }
        };

        // ------------------------------------------------------------
        // 3. 只读定位最优价档的先进先出头节点。
        // ------------------------------------------------------------

        let (head_index, maker_order_id, maker_remaining) = {
            let level = match maker_side {
                Side::Buy => self
                    .bids
                    .get(&maker_price)
                    .expect("selected best bid level must exist"),

                Side::Sell => self
                    .asks
                    .get(&maker_price)
                    .expect("selected best ask level must exist"),
            };

            assert!(
                level.count() > 0,
                "ProductionOrderBook invariant violation: \
             selected maker level is empty"
            );

            let head_index = level
                .head()
                .expect("ProductionOrderBook invariant violation: maker level missing head");

            let maker = self
                .arena
                .get(head_index)
                .expect("ProductionOrderBook invariant violation: maker head missing from Arena");

            assert_eq!(
                maker.original_order().side,
                maker_side,
                "ProductionOrderBook invariant violation: maker side mismatch"
            );

            assert_eq!(
                maker.original_order().price,
                maker_price,
                "ProductionOrderBook invariant violation: maker price mismatch"
            );

            assert!(
                maker.remaining() > 0,
                "ProductionOrderBook invariant violation: exhausted maker remains live"
            );

            (
                head_index,
                maker.original_order().order_id,
                maker.remaining(),
            )
        };

        // ------------------------------------------------------------
        // 4. 标识符索引必须在任何写入前精确匹配挂单。
        // ------------------------------------------------------------

        assert_eq!(
            self.orders.get(&maker_order_id),
            Some(&head_index),
            "ProductionOrderBook invariant violation: \
         maker OrderId index does not point to selected head"
        );

        // ------------------------------------------------------------
        // 5. 规划本次唯一成交，并把所有数量运算提前。
        // ------------------------------------------------------------

        let fill_lots = incoming.remaining().min(maker_remaining);

        let fill_qty = Qty::try_new(fill_lots)?;

        let incoming_after = incoming
            .remaining()
            .checked_sub(fill_lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        let maker_after = maker_remaining
            .checked_sub(fill_lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        // `min()` 保证至少有一侧被吃完。
        assert!(
            incoming_after == 0 || maker_after == 0,
            "ProductionOrderBook invariant violation: invalid next-fill plan"
        );

        // ------------------------------------------------------------
        // 6. 先构造事件载荷。
        //
        // 此时仍然没有任何状态修改。
        // ------------------------------------------------------------

        let trade = TradeEvent {
            trade_id,
            maker_order_id,
            taker_order_id,
            price: maker_price,
            qty: fill_qty,
            maker_side,
        };

        // ------------------------------------------------------------
        // 7. 提交挂单。
        // ------------------------------------------------------------

        if maker_after > 0 {
            // 严格部分成交：
            //
            // fill_lots < maker_remaining 必然成立。
            let level = match maker_side {
                Side::Buy => self
                    .bids
                    .get_mut(&maker_price)
                    .expect("prechecked bid level must exist"),

                Side::Sell => self
                    .asks
                    .get_mut(&maker_price)
                    .expect("prechecked ask level must exist"),
            };

            level.apply_head_partial_fill(&mut self.arena, fill_qty);
        } else {
            // 全部成交/恰好全部成交挂单：必须物理移除头节点，
            // 绝不能先把剩余量改成零再留下来。
            let (level_became_empty, maker_tif) = match maker_side {
                Side::Buy => {
                    let level = self
                        .bids
                        .get_mut(&maker_price)
                        .expect("prechecked bid level must exist");

                    let removed = level
                        .pop_front(&mut self.arena)
                        .expect("prechecked maker head must exist");

                    assert_eq!(
                        removed.original_order().order_id,
                        maker_order_id,
                        "ProductionOrderBook invariant violation: \
                     removed maker identity changed"
                    );

                    (level.is_empty(), removed.time_in_force())
                }

                Side::Sell => {
                    let level = self
                        .asks
                        .get_mut(&maker_price)
                        .expect("prechecked ask level must exist");

                    let removed = level
                        .pop_front(&mut self.arena)
                        .expect("prechecked maker head must exist");

                    assert_eq!(
                        removed.original_order().order_id,
                        maker_order_id,
                        "ProductionOrderBook invariant violation: \
                     removed maker identity changed"
                    );

                    (level.is_empty(), removed.time_in_force())
                }
            };

            // pop_front 已经释放 Arena 槽位。
            // 这里只解除 OrderId 映射，绝不能再次 `arena.remove`。
            let removed_index = self.orders.remove(&maker_order_id);

            assert_eq!(
                removed_index,
                Some(head_index),
                "ProductionOrderBook invariant violation: \
             maker OrderId mapping changed during fill"
            );

            self.regular_expiry.remove(maker_order_id, maker_tif);

            if level_became_empty {
                let removed_level = match maker_side {
                    Side::Buy => self.bids.remove(&maker_price),
                    Side::Sell => self.asks.remove(&maker_price),
                };

                let removed_level = removed_level.expect("prechecked empty maker level must exist");

                assert!(
                    removed_level.is_empty(),
                    "removed maker level must be empty"
                );
            }
        }

        // ------------------------------------------------------------
        // 8. 最后扣减新到订单。
        //
        // checked_sub 已在任何写入之前成功；
        // 与 ReferenceOrderBook 的执行顺序保持一致。
        // ------------------------------------------------------------

        incoming.apply_fill(fill_qty)?;

        Ok(Some(trade))
    }

    /// 按价格优先、同价先进先出连续执行零笔或多笔挂单成交。
    ///
    /// `trade_ids` 由调用方提供。本订单簿：
    /// - 不自行分配 TradeId；
    /// - 不验证 TradeId 的全局唯一性；
    /// - 不保存或推进 TradeId 分配器。
    ///
    /// 调用方必须提供合法且足量的 TradeId 序列。
    /// 多余的 TradeId 不会被使用。
    ///
    /// 在任何状态修改前，本方法会只读计算本次新到订单实际需要的
    /// TradeId 数量；如果 `trade_ids` 不足则触发不变量断言并停止处理。
    ///
    /// 本方法只连续消费当前可成交的挂单：
    /// - 不将剩余新到订单自动挂单；
    /// - 新到订单不分配 QueuePriority；冰山挂单补量会取得新优先级；
    /// - 支持冰山挂单补量，但不选择新到订单的 IOC/FOK/PostOnly/市价或止损策略。
    pub fn execute_fill_trades(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.original_order().side) {
            return self.execute_fill_trades_iceberg(incoming, trade_ids);
        }
        // ---------- 第一阶段：完整只读预检 ----------

        let plan = self.preflight_fill_plan(incoming);
        let expected_fills = plan.trade_count;

        assert!(
            trade_ids.len() >= expected_fills,
            "ProductionOrderBook invariant violation: \
            insufficient TradeIds for preflighted fills: \
            required={expected_fills}, provided={}",
            trade_ids.len()
        );

        if expected_fills == 0 {
            return Ok(Vec::new());
        }

        // ---------- 第二阶段：提交 ----------
        //
        // 不复制挂单状态变更。
        // 每一笔全部委托已经验收的单步入口。

        let mut trades = Vec::with_capacity(expected_fills);

        for (fill_index, trade_id) in trade_ids.iter().copied().take(expected_fills).enumerate() {
            match self.execute_next_fill_trade(incoming, trade_id) {
                Ok(Some(trade)) => {
                    trades.push(trade);
                }

                Ok(None) => {
                    panic!(
                        "ProductionOrderBook invariant violation: \
                     preflight predicted fill but single-step returned None: \
                     fill_index={fill_index}, expected_fills={expected_fills}"
                    );
                }

                Err(reason) => {
                    panic!(
                        "ProductionOrderBook invariant violation: \
                     preflighted single-step fill unexpectedly failed: \
                     fill_index={fill_index}, \
                     expected_fills={expected_fills}, \
                     reason={reason:?}"
                    );
                }
            }
        }

        assert_eq!(
            trades.len(),
            expected_fills,
            "ProductionOrderBook invariant violation: \
         committed trade count differs from preflight"
        );

        Ok(trades)
    }

    /// 在一个价格档内模拟先进先出成交，并验证单步提交依赖的局部结构。
    ///
    /// 该扫描只读地推进 `incoming_remaining`，返回实际需要的 TradeId 数量。
    /// 它检查的链表、身份、聚合与优先级关系必须足以让后续
    /// `execute_next_fill_trade` 不会因可预期数据错误在部分批量后返回 Err。
    pub(super) fn preflight_level_fill_count(
        &self,
        level: &PriceLevel,
        maker_side: Side,
        price: Price,
        incoming_remaining: &mut u64,
    ) -> usize {
        assert!(
            *incoming_remaining > 0,
            "ProductionOrderBook invariant violation: \
         preflight entered level with exhausted incoming"
        );

        assert!(
            level.count() > 0,
            "ProductionOrderBook invariant violation: \
         matching price tree contains empty level"
        );

        let head_index = level
            .head()
            .expect("ProductionOrderBook invariant violation: matching level missing head");

        let tail_index = level
            .tail()
            .expect("ProductionOrderBook invariant violation: matching level missing tail");

        assert!(
            level.total_visible_qty() > 0,
            "ProductionOrderBook invariant violation: \
         matching level has zero aggregate"
        );

        // pop_front / 部分成交都会依赖合法尾节点，
        // 因此在任何成交发生前先验证尾节点局部状态。
        {
            let tail = self.arena.get(tail_index).expect(
                "ProductionOrderBook invariant violation: matching tail missing from Arena",
            );

            assert!(
                tail.next().is_none(),
                "ProductionOrderBook invariant violation: matching tail has successor"
            );

            assert_eq!(
                tail.original_order().side,
                maker_side,
                "ProductionOrderBook invariant violation: matching tail side mismatch"
            );

            assert_eq!(
                tail.original_order().price,
                price,
                "ProductionOrderBook invariant violation: matching tail price mismatch"
            );

            assert!(
                tail.remaining() > 0,
                "ProductionOrderBook invariant violation: exhausted matching tail"
            );

            if level.count() == 1 {
                assert_eq!(
                    head_index, tail_index,
                    "ProductionOrderBook invariant violation: \
                 single-node matching level has different head/tail"
                );

                assert!(
                    tail.prev().is_none(),
                    "ProductionOrderBook invariant violation: \
                 single-node matching tail has predecessor"
                );
            } else {
                assert_ne!(
                    head_index, tail_index,
                    "ProductionOrderBook invariant violation: \
                 multi-node matching level has identical head/tail"
                );

                let predecessor_index = tail.prev().expect(
                    "ProductionOrderBook invariant violation: matching tail missing predecessor",
                );

                let predecessor = self
                    .arena
                    .get(predecessor_index)
                    .expect("ProductionOrderBook invariant violation: tail predecessor missing");

                assert_eq!(
                    predecessor.next(),
                    Some(tail_index),
                    "ProductionOrderBook invariant violation: broken tail predecessor link"
                );

                assert_eq!(
                    predecessor.original_order().side,
                    maker_side,
                    "ProductionOrderBook invariant violation: tail predecessor side mismatch"
                );

                assert_eq!(
                    predecessor.original_order().price,
                    price,
                    "ProductionOrderBook invariant violation: tail predecessor price mismatch"
                );

                assert!(
                    predecessor.remaining() > 0,
                    "ProductionOrderBook invariant violation: exhausted tail predecessor"
                );

                assert!(
                    predecessor.priority() < tail.priority(),
                    "ProductionOrderBook invariant violation: \
                 tail priority is not strictly increasing"
                );
            }
        }

        let mut current = head_index;
        let mut expected_prev = None;

        // 模拟 pop_front 后的元数据变化，但绝不写回。
        let mut simulated_count = level.count();
        let mut simulated_total = level.total_visible_qty();

        let mut fills = 0usize;

        loop {
            let node = self.arena.get(current).expect(
                "ProductionOrderBook invariant violation: matching node missing from Arena",
            );

            assert_eq!(
                node.prev(),
                expected_prev,
                "ProductionOrderBook invariant violation: broken matching prev link"
            );

            assert_eq!(
                node.original_order().side,
                maker_side,
                "ProductionOrderBook invariant violation: matching maker side mismatch"
            );

            assert_eq!(
                node.original_order().price,
                price,
                "ProductionOrderBook invariant violation: matching maker price mismatch"
            );

            assert!(
                node.remaining() > 0,
                "ProductionOrderBook invariant violation: exhausted maker remains live"
            );

            let maker_order_id = node.original_order().order_id;

            assert_eq!(
                self.orders.get(&maker_order_id),
                Some(&current),
                "ProductionOrderBook invariant violation: \
             matching maker ID index does not point to its Arena slot"
            );

            let maker_remaining = node.remaining();
            let current_priority = node.priority();
            let successor_index = node.next();

            // 验证当前模拟头节点的结构。
            if simulated_count == 1 {
                assert_eq!(
                    current, tail_index,
                    "ProductionOrderBook invariant violation: \
                 last simulated maker is not level tail"
                );

                assert!(
                    successor_index.is_none(),
                    "ProductionOrderBook invariant violation: \
                 last simulated maker still has successor"
                );
            } else {
                assert_ne!(
                    current, tail_index,
                    "ProductionOrderBook invariant violation: \
                 matching tail reached before simulated count exhausted"
                );

                let successor_index = successor_index
                    .expect("ProductionOrderBook invariant violation: matching chain ended early");

                assert_ne!(
                    successor_index, current,
                    "ProductionOrderBook invariant violation: matching node self-cycle"
                );

                let successor = self
                    .arena
                    .get(successor_index)
                    .expect("ProductionOrderBook invariant violation: matching successor missing");

                assert_eq!(
                    successor.prev(),
                    Some(current),
                    "ProductionOrderBook invariant violation: \
                 broken matching successor prev"
                );

                assert_eq!(
                    successor.original_order().side,
                    maker_side,
                    "ProductionOrderBook invariant violation: \
                 matching successor side mismatch"
                );

                assert_eq!(
                    successor.original_order().price,
                    price,
                    "ProductionOrderBook invariant violation: \
                 matching successor price mismatch"
                );

                assert!(
                    successor.remaining() > 0,
                    "ProductionOrderBook invariant violation: \
                 exhausted matching successor"
                );

                assert!(
                    successor.priority() > current_priority,
                    "ProductionOrderBook invariant violation: \
                 matching priority is not strictly increasing"
                );

                if simulated_count == 2 {
                    assert_eq!(
                        successor_index, tail_index,
                        "ProductionOrderBook invariant violation: invalid two-node matching tail"
                    );
                } else {
                    assert_ne!(
                        successor_index, tail_index,
                        "ProductionOrderBook invariant violation: premature matching tail"
                    );
                }
            }

            // ---------- 数量模拟：仍然没有任何写入 ----------

            let fill_lots = (*incoming_remaining).min(maker_remaining);

            assert!(
                fill_lots > 0,
                "ProductionOrderBook invariant violation: zero planned fill"
            );

            let maker_after = maker_remaining
                .checked_sub(fill_lots)
                .expect("ProductionOrderBook invariant violation: maker quantity underflow");

            let incoming_after = (*incoming_remaining)
                .checked_sub(fill_lots)
                .expect("ProductionOrderBook invariant violation: incoming quantity underflow");

            let total_after = simulated_total
                .checked_sub(fill_lots)
                .expect("ProductionOrderBook invariant violation: level aggregate underflow");

            fills = fills
                .checked_add(1)
                .expect("ProductionOrderBook invariant violation: fill count overflow");

            // 挂单部分成交必然意味着新到订单已耗尽，
            // 这一笔就是整个批量的最后一笔。
            if maker_after > 0 {
                assert_eq!(
                    incoming_after, 0,
                    "ProductionOrderBook invariant violation: \
                 partial maker did not exhaust incoming"
                );

                if simulated_count == 1 {
                    assert_eq!(
                        total_after, maker_after,
                        "ProductionOrderBook invariant violation: \
                     single-node partial aggregate mismatch"
                    );
                } else {
                    assert!(
                        total_after > maker_after,
                        "ProductionOrderBook invariant violation: \
                     multi-node partial aggregate mismatch"
                    );
                }

                *incoming_remaining = 0;
                break;
            }

            // 全部成交挂单：模拟 pop_front。
            let count_after = simulated_count
                .checked_sub(1)
                .expect("ProductionOrderBook invariant violation: matching count underflow");

            if count_after == 0 {
                assert_eq!(
                    total_after, 0,
                    "ProductionOrderBook invariant violation: \
                 exhausted matching level retains aggregate"
                );
            } else {
                assert!(
                    total_after > 0,
                    "ProductionOrderBook invariant violation: \
                 non-empty simulated level has zero aggregate"
                );
            }

            *incoming_remaining = incoming_after;
            simulated_count = count_after;
            simulated_total = total_after;

            // 新到订单正好在该挂单上耗尽。
            if *incoming_remaining == 0 {
                break;
            }

            // 当前价格档已经全部模拟吃完，由外层继续下一个价格。
            if simulated_count == 0 {
                break;
            }

            let next =
                successor_index.expect("prechecked non-empty matching chain must have successor");

            expected_prev = Some(current);
            current = next;
        }

        fills
    }

    /// 按价格优先和同价先进先出构造整个连续撮合的只读计划。
    ///
    /// 买入只从低到高扫描可成交 `asks`，卖出只从高到低扫描可成交 `bids`；
    /// HashMap 仅在逐节点身份校验中按单键读取，绝不决定扫描或事件顺序。
    pub(super) fn preflight_fill_plan(&self, incoming: &IncomingOrder) -> FillPlan {
        let mut remaining = incoming.remaining();

        if remaining == 0 {
            return FillPlan {
                trade_count: 0,
                remaining: 0,
            };
        }

        let taker = incoming.original_order();
        let mut trade_count = 0usize;

        match taker.side {
            Side::Buy => {
                // 卖价: 低 -> 高
                for (price, level) in &self.asks {
                    if remaining == 0 || *price > taker.price {
                        break;
                    }

                    let level_fills =
                        self.preflight_level_fill_count(level, Side::Sell, *price, &mut remaining);

                    trade_count = trade_count.checked_add(level_fills).expect(
                        "ProductionOrderBook invariant violation: \
                         fill count overflow",
                    );
                }
            }

            Side::Sell => {
                // 买价: 高 -> 低
                for (price, level) in self.bids.iter().rev() {
                    if remaining == 0 || *price < taker.price {
                        break;
                    }

                    let level_fills =
                        self.preflight_level_fill_count(level, Side::Buy, *price, &mut remaining);

                    trade_count = trade_count.checked_add(level_fills).expect(
                        "ProductionOrderBook invariant violation: \
                         fill count overflow",
                    );
                }
            }
        }

        FillPlan {
            trade_count,
            remaining,
        }
    }
}
