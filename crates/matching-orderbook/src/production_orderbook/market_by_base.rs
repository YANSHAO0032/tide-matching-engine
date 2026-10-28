//! 基础数量市价单及价格保护的只读计划和执行。

use super::{FillPlan, ProductionOrderBook};
use crate::PriceLevel;
use crate::market_plan::ProtectionBoundaryStop;
use matching_domain::market_order::{
    CommandStartProtectionContext, effective_protection_at_command_start_with_source,
    maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, MarketByBase, MarketMath, OrderId, Price, Qty, RejectReason, Side, TradeEvent,
    TradeId,
};

#[cfg(test)]
#[path = "market_by_base/test_support.rs"]
pub(crate) mod test_support;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtectedBaseFill {
    /// 将被成交的挂单订单标识符。
    maker_order_id: OrderId,
    /// 该挂单的成交价格。
    maker_price: Price,
    /// 本笔计划成交的基础资产数量。
    qty: Qty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProtectedMarketByBasePlan {
    /// 按未来事件顺序排列的挂单成交计划。
    fills: Vec<ProtectedBaseFill>,
    /// 因保护边界停止时首个不可成交的价格信息。
    stop: Option<ProtectionBoundaryStop>,
    /// 计划结束后仍未成交的基础资产数量。
    remaining: u64,
}

impl ProductionOrderBook {
    /// 以基础币数量立即成交市价订单。
    ///
    /// 入口先完成只读预检并确认 TradeId 数量，再按对手侧价格优先和
    /// 同价先进先出提交成交。市价余量不挂簿、不取优先级；冰山挂单补量取新优先级。
    pub fn execute_market_by_base(
        &mut self,
        incoming: MarketByBase,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            return self
                .execute_market_base_iceberg(incoming, None, trade_ids)
                .map(Self::trades_only);
        }
        let plan = self.preflight_market_by_base(&incoming)?;

        assert!(
            trade_ids.len() >= plan.trade_count,
            "ProductionOrderBook invariant violation: \
         insufficient TradeIds for MarketByBase: required={}, provided={}",
            plan.trade_count,
            trade_ids.len(),
        );

        let mut remaining = incoming.qty.get();
        let mut trades = Vec::with_capacity(plan.trade_count);

        for trade_id in trade_ids.iter().copied().take(plan.trade_count) {
            trades.push(self.commit_market_by_base_fill(
                incoming.order_id,
                incoming.side,
                &mut remaining,
                trade_id,
            ));
        }

        assert_eq!(remaining, plan.remaining);
        assert_eq!(trades.len(), plan.trade_count);

        Ok(trades)
    }

    /// 按命令开始时固定的保护边界执行 MarketByBase。
    ///
    /// 完整预检成功后才允许首次挂单状态变更。
    /// 保护触发最多产生一个事件，不消耗 TradeId；
    /// 未成交市价余量不挂簿、不分配 QueuePriority；冰山挂单补量取新优先级。
    pub fn execute_market_by_base_with_protection(
        &mut self,
        incoming: MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.has_iceberg_maker(incoming.side) {
            return self.execute_market_base_iceberg(incoming, Some((context, math)), trade_ids);
        }
        // 1. 全量只读预检。
        let plan = self.preflight_protected_market_by_base_plan(&incoming, context, math)?;

        // 2. 在首次状态变更前校验 TradeId。
        // 延续当前生产簿的触发不变量断言并停止处理契约。
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for protected MarketByBase: \
         required={}, provided={}",
            plan.fills.len(),
            trade_ids.len(),
        );

        // 3. 预先为 Trade + 可选保护事件分配全部容量。
        let event_count = plan
            .fills
            .len()
            .checked_add(usize::from(plan.stop.is_some()))
            .expect("protected MarketByBase event count overflow");

        let mut events = Vec::with_capacity(event_count);

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut remaining = incoming.qty.get();

        // 4. 严格按照预检计划提交。
        for (fill, trade_id) in plan.fills.iter().zip(trade_ids.iter().copied()) {
            // 当前全市场最优挂单价格必须等于计划价格。
            let best_price = self
                .best_price(maker_side)
                .expect("preflighted protected maker side became empty");

            assert_eq!(best_price, fill.maker_price);

            // 必须选择最优价档的先进先出头节点。
            let levels = match maker_side {
                Side::Buy => &self.bids,
                Side::Sell => &self.asks,
            };

            let level = levels
                .get(&best_price)
                .expect("preflighted protected level missing");

            let head_slot = level
                .head()
                .expect("preflighted protected FIFO head missing");

            let maker = self
                .arena
                .get(head_slot)
                .expect("preflighted protected Arena node missing");

            assert_eq!(maker.original_order().order_id, fill.maker_order_id);
            assert_eq!(maker.original_order().side, maker_side);
            assert_eq!(maker.original_order().price, fill.maker_price);

            assert_eq!(self.orders.get(&fill.maker_order_id), Some(&head_slot));

            // 计划数量必须与当前可执行数量完全一致。
            assert!(remaining > 0);
            assert!(maker.remaining() > 0);
            assert_eq!(fill.qty.get(), remaining.min(maker.remaining()));

            // 5. 复用已验收的生产簿单笔提交。
            //    本次调用内部完成：
            //    - 挂单部分成交 / 全部成交
            //    - PriceLevel 聚合与先进先出更新
            //    - Arena 槽位释放
            //    - OrderId 索引 / 空价档清理
            //    - 吃单方剩余量扣减
            let trade = self.commit_market_by_base_fill(
                incoming.order_id,
                incoming.side,
                &mut remaining,
                trade_id,
            );

            assert_eq!(trade.maker_order_id, fill.maker_order_id);
            assert_eq!(trade.price, fill.maker_price);
            assert_eq!(trade.qty, fill.qty);

            events.push(EngineEvent::Trade(trade));
        }

        assert_eq!(remaining, plan.remaining);

        // 6. 所有 Trade 之后最多追加一个保护事件。
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }

        assert_eq!(events.len(), event_count);

        Ok(events)
    }

    fn commit_market_by_base_fill(
        &mut self,
        taker_order_id: OrderId,
        taker_side: Side,
        remaining: &mut u64,
        trade_id: TradeId,
    ) -> TradeEvent {
        assert!(*remaining > 0, "preflighted taker exhausted");

        let maker_side = match taker_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let maker_price = match maker_side {
            Side::Buy => {
                *self
                    .bids
                    .last_key_value()
                    .expect("preflighted bid missing")
                    .0
            }
            Side::Sell => {
                *self
                    .asks
                    .first_key_value()
                    .expect("preflighted ask missing")
                    .0
            }
        };

        let (head_index, maker_order_id, maker_remaining) = {
            let level = match maker_side {
                Side::Buy => self.bids.get(&maker_price),
                Side::Sell => self.asks.get(&maker_price),
            }
            .expect("preflighted level missing");

            let index = level.head().expect("preflighted head missing");
            let maker = self
                .arena
                .get(index)
                .expect("preflighted Arena node missing");

            assert_eq!(maker.original_order().side, maker_side);
            assert_eq!(maker.original_order().price, maker_price);
            assert!(maker.remaining() > 0);

            (index, maker.original_order().order_id, maker.remaining())
        };

        assert_eq!(self.orders.get(&maker_order_id), Some(&head_index));

        // 本笔全部数量计算先于写入。
        let fill_lots = (*remaining).min(maker_remaining);
        let fill_qty = Qty::try_new(fill_lots).expect("preflighted fill must be positive");

        let maker_after = maker_remaining
            .checked_sub(fill_lots)
            .expect("preflighted maker underflow");

        let taker_after = (*remaining)
            .checked_sub(fill_lots)
            .expect("preflighted taker underflow");

        let trade = TradeEvent {
            trade_id,
            maker_order_id,
            taker_order_id,
            price: maker_price,
            qty: fill_qty,
            maker_side,
        };

        if maker_after > 0 {
            let level = match maker_side {
                Side::Buy => self.bids.get_mut(&maker_price),
                Side::Sell => self.asks.get_mut(&maker_price),
            }
            .expect("preflighted partial level missing");

            level.apply_head_partial_fill(&mut self.arena, fill_qty);
        } else {
            let (level_became_empty, maker_tif) = {
                let level = match maker_side {
                    Side::Buy => self.bids.get_mut(&maker_price),
                    Side::Sell => self.asks.get_mut(&maker_price),
                }
                .expect("preflighted full level missing");

                // pop_front 同时更新链表/聚合并释放 Arena 槽位。
                let removed = level
                    .pop_front(&mut self.arena)
                    .expect("preflighted head missing");

                assert_eq!(removed.original_order().order_id, maker_order_id);
                (level.is_empty(), removed.time_in_force())
            };

            // 不要重复调用 `arena.remove`。
            assert_eq!(self.orders.remove(&maker_order_id), Some(head_index),);
            self.reservations.remove(&maker_order_id);
            self.regular_expiry.remove(maker_order_id, maker_tif);

            if level_became_empty {
                let removed = match maker_side {
                    Side::Buy => self.bids.remove(&maker_price),
                    Side::Sell => self.asks.remove(&maker_price),
                }
                .expect("preflighted empty level missing");

                assert!(removed.is_empty());
            }
        }

        // 最后才更新本次市价吃单方的局部剩余量。
        *remaining = taker_after;

        trade
    }

    fn preflight_market_by_base(&self, incoming: &MarketByBase) -> Result<FillPlan, RejectReason> {
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        let mut remaining = incoming.qty.get();
        let mut trade_count = 0usize;

        match incoming.side {
            Side::Buy => {
                for (price, level) in &self.asks {
                    if remaining == 0 {
                        break;
                    }

                    let count =
                        self.preflight_level_fill_count(level, Side::Sell, *price, &mut remaining);

                    trade_count = trade_count
                        .checked_add(count)
                        .expect("MarketByBase trade count overflow");
                }
            }
            Side::Sell => {
                for (price, level) in self.bids.iter().rev() {
                    if remaining == 0 {
                        break;
                    }

                    let count =
                        self.preflight_level_fill_count(level, Side::Buy, *price, &mut remaining);

                    trade_count = trade_count
                        .checked_add(count)
                        .expect("MarketByBase trade count overflow");
                }
            }
        }

        Ok(FillPlan {
            trade_count,
            remaining,
        })
    }

    /// 生产簿独立只读规划；不触碰 Arena、价档、标识符索引或分配器。
    /// 业务拒绝优先级：重复标识符 -> 命令开始时保护 -> 扫描。
    /// 返回成交前缀及首个越界价，终结保护本身不消耗 TradeId。
    fn preflight_protected_market_by_base_plan(
        &self,
        incoming: &MarketByBase,
        context: &CommandStartProtectionContext,
        math: &MarketMath,
    ) -> Result<ProtectedMarketByBasePlan, RejectReason> {
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        let effective = effective_protection_at_command_start_with_source(
            incoming.market_protection.as_ref(),
            context,
            incoming.side,
            math,
        )?;

        let maker_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let mut remaining = incoming.qty.get();
        let mut fills = Vec::new();
        let mut stop = None;

        // true：当前价档已处理完，可以继续下一档；false：立即终止整个扫描。
        let mut scan_level = |price: Price, level: &PriceLevel| -> Result<bool, RejectReason> {
            if remaining == 0 {
                return Ok(false);
            }

            if !maker_price_within_protection(
                incoming.side,
                price,
                effective.map(|(worst, _)| worst),
            ) {
                let (effective_worst_price, source) =
                    effective.expect("out-of-bound maker requires a protection boundary");
                stop = Some(ProtectionBoundaryStop {
                    next_price: price,
                    effective_worst_price,
                    source,
                });
                return Ok(false);
            }

            assert!(
                level.count() > 0,
                "protected MarketByBase: empty matching level"
            );
            assert!(
                level.total_visible_qty() > 0,
                "protected MarketByBase: zero matching aggregate"
            );
            let head = level
                .head()
                .expect("protected MarketByBase: missing level head");
            let tail = level
                .tail()
                .expect("protected MarketByBase: missing level tail");
            let tail_node = self
                .arena
                .get(tail)
                .expect("protected MarketByBase: missing tail");
            assert_eq!(tail_node.original_order().side, maker_side);
            assert_eq!(tail_node.original_order().price, price);
            assert!(tail_node.remaining() > 0);
            assert!(tail_node.next().is_none());
            if level.count() == 1 {
                assert_eq!(head, tail);
                assert!(tail_node.prev().is_none());
            } else {
                assert_ne!(head, tail);
                let prev = tail_node
                    .prev()
                    .expect("protected MarketByBase: missing tail prev");
                let predecessor = self
                    .arena
                    .get(prev)
                    .expect("protected MarketByBase: missing tail predecessor");
                assert_eq!(predecessor.next(), Some(tail));
                assert_eq!(predecessor.original_order().side, maker_side);
                assert_eq!(predecessor.original_order().price, price);
                assert!(predecessor.remaining() > 0);
                assert!(predecessor.priority() < tail_node.priority());
            }

            let mut current = Some(head);
            let mut expected_prev = None;
            let mut previous_priority = None;
            let mut visited = 0u32;
            let mut consumed_in_level = 0u64;

            while remaining > 0 {
                let Some(slot) = current else { break };
                visited = visited
                    .checked_add(1)
                    .expect("protected MarketByBase: count overflow");
                assert!(
                    visited <= level.count(),
                    "protected MarketByBase: FIFO cycle"
                );

                let node = self
                    .arena
                    .get(slot)
                    .expect("protected MarketByBase: missing FIFO node");
                assert_eq!(node.prev(), expected_prev);
                assert_eq!(node.original_order().side, maker_side);
                assert_eq!(node.original_order().price, price);
                assert!(node.remaining() > 0);
                if let Some(previous) = previous_priority {
                    assert!(
                        previous < node.priority(),
                        "protected MarketByBase: priority regression"
                    );
                }
                let maker_order_id = node.original_order().order_id;
                assert_eq!(self.orders.get(&maker_order_id), Some(&slot));

                let next = node.next();
                if visited == level.count() {
                    assert_eq!(slot, tail);
                    assert!(next.is_none());
                } else {
                    let successor = next.expect("protected MarketByBase: truncated FIFO chain");
                    let next_node = self
                        .arena
                        .get(successor)
                        .expect("protected MarketByBase: missing successor");
                    assert_eq!(next_node.prev(), Some(slot));
                    assert!(node.priority() < next_node.priority());
                }

                let fill_lots = remaining.min(node.remaining());
                let qty = Qty::try_new(fill_lots)?;
                remaining = remaining
                    .checked_sub(fill_lots)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                consumed_in_level = consumed_in_level
                    .checked_add(fill_lots)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                fills.push(ProtectedBaseFill {
                    maker_order_id,
                    maker_price: price,
                    qty,
                });

                expected_prev = Some(slot);
                previous_priority = Some(node.priority());
                current = next;
            }

            assert!(
                consumed_in_level <= level.total_visible_qty(),
                "protected MarketByBase: planned quantity exceeds level aggregate"
            );
            if remaining > 0 {
                assert_eq!(visited, level.count());
                assert!(current.is_none());
                assert_eq!(consumed_in_level, level.total_visible_qty());
            }
            Ok(remaining > 0)
        };

        // 价格树决定档位顺序；每档侵入式 `next` 链决定先进先出。
        match incoming.side {
            Side::Buy => {
                for (price, level) in &self.asks {
                    if !scan_level(*price, level)? {
                        break;
                    }
                }
            }
            Side::Sell => {
                for (price, level) in self.bids.iter().rev() {
                    if !scan_level(*price, level)? {
                        break;
                    }
                }
            }
        }

        Ok(ProtectedMarketByBasePlan {
            fills,
            stop,
            remaining,
        })
    }
}
