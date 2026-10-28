//! 先只读模拟冰山的先进先出成交顺序，再提交真实订单簿结构变更。

use super::resting::commit_rest_target;
use super::{ProductionOrderBook, RestPlan};
use crate::market_plan::ProtectionBoundaryStop;
use crate::{IncomingOrder, OrderNode, PriceLevel};
use matching_domain::market_order::{
    CommandStartProtectionContext, ProtectedMarketByQuoteExecution,
    effective_protection_at_command_start_with_source, maker_price_within_protection,
};
use matching_domain::{
    EngineEvent, IcebergOrder, LimitGtcOrder, LimitImmediateExecution, MarketByBase, MarketByQuote,
    MarketMath, OrderId, Price, ProtectionSource, Qty, QueuePriority, QuoteAmount, RejectReason,
    Side, TradeEvent, TradeId,
};
use matching_risk::ReservationState;
use std::collections::VecDeque;

#[derive(Clone, Copy)]
struct VirtualMaker {
    /// 挂单订单标识符。
    id: OrderId,
    /// 当前可参与撮合的可见基础数量。
    visible: u64,
    /// 当前尚未展示的隐藏基础数量。
    hidden: u64,
    /// 冰山订单的可选单次展示峰值。
    peak: Option<Qty>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SliceFill {
    /// 被成交冰山挂单的订单标识符。
    id: OrderId,
    /// 本片成交的挂单价格。
    price: Price,
    /// 本片成交的基础资产数量。
    qty: Qty,
    /// 本片成交后该订单的总剩余基础数量。
    total_after: u64,
    /// 可见片耗尽后需要展示的数量及新队列优先级。
    refresh: Option<(Qty, QueuePriority)>,
}

impl SliceFill {
    /// 只有该片耗尽整张挂单时，返回需要终结预留归属的身份。
    pub(super) fn terminal_maker(&self) -> Option<OrderId> {
        (self.total_after == 0).then_some(self.id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct IcebergPlan {
    /// 按撮合顺序排列的冰山片成交计划。
    pub(super) fills: Vec<SliceFill>,
    /// 按基础数量预算执行时的剩余基础数量。
    pub(super) base_remaining: u64,
    /// 按报价预算执行时的剩余报价资产金额。
    quote_remaining: QuoteAmount,
    /// 计划使用的报价资产本金。
    spent: QuoteAmount,
    /// 因保护边界停止时首个不可成交的价格信息。
    stop: Option<ProtectionBoundaryStop>,
    /// 全部补量提交后预期的下一队列优先级数值。
    pub(super) next_priority: u128,
}

pub(super) enum Budget {
    Base(u64),
    Quote(QuoteAmount),
}

/// 同一生产扫描的只读选择条件；排除身份不会进入补量队列。
pub(super) struct SliceScan {
    /// 调用方允许扫描并冻结的最大成交笔数。
    pub(super) max_fills: Option<usize>,
    /// 撤单替换预检中逻辑排除的旧订单身份。
    pub(super) excluded_order_id: Option<OrderId>,
}

impl ProductionOrderBook {
    /// 在同一次独占调用中执行普通限价单并关联实际挂单余量。
    pub fn place_limit_gtc_with_reservation(
        &mut self,
        order: LimitGtcOrder,
        state: ReservationState,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        self.validate_new_reservation(order.order_id, order.user_id, &state)?;
        let order_id = order.order_id;
        let accepted = order.clone();
        let events = self.place_limit_gtc_with_events(order, trade_ids)?;
        if let Some(index) = self.orders.get(&order_id).copied() {
            let node = self
                .arena
                .get_mut(index)
                .expect("resting reservation order");
            // 基础挂单路径仅保存余量；接收总量必须包含入簿前的真实成交。
            node.amend_regular_in_place(
                accepted,
                Qty::try_new(node.total_remaining()).expect("positive placement residual"),
                node.time_in_force(),
            );
            assert!(self.reservations.insert(order_id, state).is_none());
        }
        Ok(events)
    }

    /// 将冰山挂单及预留状态一并提交；失败保持两者不变。
    pub fn place_iceberg_post_only_with_reservation(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
        state: ReservationState,
    ) -> Result<(), RejectReason> {
        let order_id = iceberg.order().order_id;
        self.validate_new_reservation(order_id, iceberg.order().user_id, &state)?;
        self.place_iceberg_post_only(iceberg, math)?;
        assert!(self.reservations.insert(order_id, state).is_none());
        Ok(())
    }

    pub(super) fn has_iceberg_maker(&self, incoming_side: Side) -> bool {
        let levels = match incoming_side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };
        levels.values().any(|level| {
            let mut cursor = level.head();
            let mut previous = None;
            let mut count = 0u32;
            while let Some(index) = cursor {
                let node = self
                    .arena
                    .get(index)
                    .expect("iceberg detection missing Arena node");
                assert_eq!(
                    node.prev(),
                    previous,
                    "iceberg detection broken FIFO backlink"
                );
                count = count
                    .checked_add(1)
                    .expect("iceberg detection count overflow");
                assert!(
                    count <= level.count(),
                    "iceberg detection FIFO cycle or count mismatch"
                );
                if node.peak_size().is_some() {
                    return true;
                }
                previous = Some(index);
                cursor = node.next();
            }
            false
        })
    }

    pub(super) fn preflight_iceberg_plan(
        &self,
        side: Side,
        limit: Option<Price>,
        budget: Budget,
        math: Option<&MarketMath>,
        protection: Option<(Price, ProtectionSource)>,
        max_fills: Option<usize>,
    ) -> Result<IcebergPlan, RejectReason> {
        self.preflight_iceberg_scan(
            side,
            limit,
            budget,
            math,
            protection,
            SliceScan {
                max_fills,
                excluded_order_id: None,
            },
        )
    }

    /// 普通下单与逻辑删除视图共用生产扫描；只在临时可见片队列中模拟。
    pub(super) fn preflight_iceberg_scan(
        &self,
        side: Side,
        limit: Option<Price>,
        mut budget: Budget,
        math: Option<&MarketMath>,
        protection: Option<(Price, ProtectionSource)>,
        scan: SliceScan,
    ) -> Result<IcebergPlan, RejectReason> {
        let SliceScan {
            max_fills,
            excluded_order_id,
        } = scan;
        let maker_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let levels = match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };
        let ordered: Box<dyn Iterator<Item = (&Price, &PriceLevel)> + '_> = match side {
            Side::Buy => Box::new(levels.iter()),
            Side::Sell => Box::new(levels.iter().rev()),
        };
        let mut fills = Vec::new();
        let mut spent = QuoteAmount::new(0);
        let mut next_priority = self.next_queue_priority();
        let mut stop = None;
        'levels: for (price, level) in ordered {
            // 独占价档随旧单逻辑消失，不能触发保护边界或贡献最优价格。
            if level.count() == 1
                && level.head().is_some_and(|index| {
                    Some(
                        self.arena
                            .get(index)
                            .expect("view level head")
                            .original_order()
                            .order_id,
                    ) == excluded_order_id
                })
            {
                continue;
            }
            if max_fills.is_some_and(|max| fills.len() >= max) {
                break;
            }
            if match budget {
                Budget::Base(v) => v == 0,
                Budget::Quote(v) => v.get() == 0,
            } {
                break;
            }
            if limit.is_some_and(|bound| match side {
                Side::Buy => *price > bound,
                Side::Sell => *price < bound,
            }) {
                break;
            }
            if !maker_price_within_protection(side, *price, protection.map(|(p, _)| p)) {
                let (effective_worst_price, source) = protection.expect("boundary needed for stop");
                stop = Some(ProtectionBoundaryStop {
                    next_price: *price,
                    effective_worst_price,
                    source,
                });
                break;
            }
            let mut virtual_fifo = VecDeque::new();
            let mut cursor = level.head();
            let mut aggregate = 0u64;
            let mut count = 0u32;
            let mut previous_priority = None;
            let mut previous = None;
            assert!(level.count() > 0, "iceberg plan empty price level");
            while let Some(index) = cursor {
                let node = self
                    .arena
                    .get(index)
                    .expect("iceberg preflight missing Arena node");
                assert_eq!(node.prev(), previous, "iceberg plan broken FIFO backlink");
                assert_eq!(node.original_order().side, maker_side);
                assert_eq!(node.original_order().price, *price);
                assert_eq!(
                    self.orders.get(&node.original_order().order_id),
                    Some(&index)
                );
                assert!(node.remaining() > 0);
                assert!(previous_priority.is_none_or(|prev| prev < node.priority()));
                assert!(
                    node.priority().get() < self.next_queue_priority(),
                    "active priority must precede allocator"
                );
                previous_priority = Some(node.priority());
                aggregate = aggregate
                    .checked_add(node.remaining())
                    .expect("existing level aggregate overflow");
                count = count.checked_add(1).expect("existing level count overflow");
                assert!(
                    count <= level.count(),
                    "iceberg plan FIFO cycle or count mismatch"
                );
                if Some(node.original_order().order_id) != excluded_order_id {
                    virtual_fifo.push_back(VirtualMaker {
                        id: node.original_order().order_id,
                        visible: node.remaining(),
                        hidden: node.hidden_remaining(),
                        peak: node.peak_size(),
                    });
                }
                previous = Some(index);
                cursor = node.next();
            }
            assert_eq!(previous, level.tail(), "iceberg plan FIFO tail mismatch");
            assert_eq!(aggregate, level.total_visible_qty());
            assert_eq!(count, level.count());
            if let Some(excluded) = excluded_order_id.and_then(|id| self.order(id))
                && excluded.original_order().side == maker_side
                && excluded.original_order().price == *price
            {
                aggregate = aggregate
                    .checked_sub(excluded.remaining())
                    .expect("excluded visible belongs to level");
            }
            while let Some(mut maker) = virtual_fifo.pop_front() {
                if max_fills.is_some_and(|max| fills.len() >= max) {
                    break 'levels;
                }
                if match budget {
                    Budget::Base(v) => v == 0,
                    Budget::Quote(v) => v.get() == 0,
                } {
                    break 'levels;
                }
                let fill = match budget {
                    Budget::Base(remaining) => remaining.min(maker.visible),
                    Budget::Quote(quote) => {
                        let Some(max) = math
                            .expect("quote math")
                            .max_qty_for_quote(quote, *price)
                            .map_err(Self::reject_quote_math_error)?
                        else {
                            break 'levels;
                        };
                        max.get().min(maker.visible)
                    }
                };
                if fill == 0 {
                    break 'levels;
                }
                let qty = Qty::try_new(fill).expect("positive planned fill");
                if let Some(math) = math {
                    let notional = math
                        .trade_notional(*price, qty)
                        .map_err(Self::reject_quote_math_error)?;
                    spent = spent.checked_add(notional)?;
                    if let Budget::Quote(remaining) = budget {
                        budget = Budget::Quote(remaining.checked_sub(notional)?);
                    }
                }
                if let Budget::Base(remaining) = budget {
                    budget = Budget::Base(remaining.checked_sub(fill).expect("fill <= remaining"));
                }
                maker.visible = maker.visible.checked_sub(fill).expect("fill <= visible");
                aggregate = aggregate
                    .checked_sub(fill)
                    .expect("aggregate includes maker");
                let mut refresh = None;
                if maker.visible == 0 && maker.hidden > 0 {
                    let visible = maker
                        .hidden
                        .min(maker.peak.expect("hidden requires peak").get());
                    maker.hidden = maker.hidden.checked_sub(visible).expect("slice <= hidden");
                    maker.visible = visible;
                    aggregate = aggregate
                        .checked_add(visible)
                        .ok_or(RejectReason::ArithmeticOverflow)?;
                    if next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
                        return Err(RejectReason::PrioritySpaceExhaustion);
                    }
                    let priority = QueuePriority::new(next_priority);
                    next_priority = next_priority
                        .checked_add(1)
                        .expect("priority below ceiling");
                    refresh = Some((Qty::try_new(visible).expect("positive refresh"), priority));
                    virtual_fifo.push_back(maker);
                } else if maker.visible > 0 {
                    virtual_fifo.push_front(maker);
                }
                fills.push(SliceFill {
                    id: maker.id,
                    price: *price,
                    qty,
                    total_after: maker
                        .visible
                        .checked_add(maker.hidden)
                        .expect("total remaining overflow"),
                    refresh,
                });
            }
        }
        let (base_remaining, quote_remaining) = match budget {
            Budget::Base(base) => (base, QuoteAmount::new(0)),
            Budget::Quote(quote) => (0, quote),
        };
        Ok(IcebergPlan {
            fills,
            base_remaining,
            quote_remaining,
            spent,
            stop,
            next_priority,
        })
    }

    pub(super) fn commit_iceberg_plan(
        &mut self,
        plan: &IcebergPlan,
        side: Side,
        taker: OrderId,
        ids: &[TradeId],
    ) -> Vec<EngineEvent> {
        assert!(
            ids.len() >= plan.fills.len(),
            "insufficient TradeIds before iceberg commit"
        );
        let maker_side = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let mut events = Vec::new();
        for (fill, trade_id) in plan.fills.iter().zip(ids.iter().copied()) {
            assert_eq!(self.best_price(maker_side), Some(fill.price));
            let level = match maker_side {
                Side::Buy => self.bids.get_mut(&fill.price),
                Side::Sell => self.asks.get_mut(&fill.price),
            }
            .expect("planned maker level missing");
            let index = level.head().expect("planned maker head missing");
            let maker = self.arena.get(index).expect("planned maker node missing");
            assert_eq!(maker.original_order().order_id, fill.id);
            let visible_before = maker.remaining();
            let is_iceberg = maker.peak_size().is_some();
            let trade = TradeEvent {
                trade_id,
                maker_order_id: fill.id,
                taker_order_id: taker,
                price: fill.price,
                qty: fill.qty,
                maker_side,
            };
            if fill.qty.get() < visible_before {
                level.apply_head_partial_fill(&mut self.arena, fill.qty);
            } else if let Some((visible, priority)) = fill.refresh {
                let mut node = level
                    .pop_front(&mut self.arena)
                    .expect("planned exhausted slice missing");
                assert_eq!(node.original_order().order_id, fill.id);
                assert_eq!(self.orders.get(&fill.id), Some(&index));
                assert_eq!(node.replenish(priority), visible);
                let new_index = level
                    .push_back(&mut self.arena, node)
                    .expect("preflighted replenish must fit");
                assert_eq!(new_index, index);
                let actual = self
                    .queue_priority_allocator
                    .allocate()
                    .expect("preflighted priority unavailable");
                assert_eq!(actual, priority);
            } else {
                let removed = level
                    .pop_front(&mut self.arena)
                    .expect("planned full maker missing");
                assert_eq!(removed.original_order().order_id, fill.id);
                assert_eq!(self.orders.remove(&fill.id), Some(index));
                self.reservations.remove(&fill.id);
                self.regular_expiry.remove(fill.id, removed.time_in_force());
                if level.is_empty() {
                    let removed_level = match maker_side {
                        Side::Buy => self.bids.remove(&fill.price),
                        Side::Sell => self.asks.remove(&fill.price),
                    };
                    assert!(removed_level.expect("empty level missing").is_empty());
                }
            }
            events.push(EngineEvent::Trade(trade));
            if is_iceberg {
                if fill.total_after == 0 {
                    events.push(EngineEvent::OrderFilled { order_id: fill.id });
                } else {
                    events.push(EngineEvent::OrderPartiallyFilled {
                        order_id: fill.id,
                        remaining: Qty::try_new(fill.total_after).expect("positive total"),
                    });
                }
                if let Some((visible, _)) = fill.refresh {
                    events.push(EngineEvent::IcebergRefreshed {
                        order_id: fill.id,
                        visible,
                    });
                }
            }
        }
        assert_eq!(self.next_queue_priority(), plan.next_priority);
        events
    }

    pub(super) fn trades_only(events: Vec<EngineEvent>) -> Vec<TradeEvent> {
        events
            .into_iter()
            .filter_map(|event| match event {
                EngineEvent::Trade(trade) => Some(trade),
                _ => None,
            })
            .collect()
    }

    /// 冰山以全部总量主动尝试成交，再将余量的第一片显示为挂单。
    ///
    /// 改簿前校验算术、补量优先级与余量挂单条件。
    /// 事件保留冰山挂单的生命周期和新到订单的总余量。
    /// 调用方提供的 `TradeId` 不足时，在改簿前触发不变量断言。
    pub fn place_iceberg(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        let order = iceberg.order();
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        math.trade_notional(order.price, iceberg.peak_size())
            .map_err(Self::reject_quote_math_error)?;
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg"
        );
        self.preflight_iceberg_rest(&iceberg, &plan)?;
        let mut events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        if !plan.fills.is_empty() {
            if plan.base_remaining == 0 {
                events.push(EngineEvent::OrderFilled {
                    order_id: order.order_id,
                });
            } else {
                events.push(EngineEvent::OrderPartiallyFilled {
                    order_id: order.order_id,
                    remaining: Qty::try_new(plan.base_remaining)
                        .expect("positive iceberg remainder"),
                });
            }
        }
        if plan.base_remaining > 0 {
            let mut visible_order = order.clone();
            visible_order.qty = Qty::try_new(plan.base_remaining.min(iceberg.peak_size().get()))
                .expect("visible positive");
            let rest_plan = self
                .preflight_rest_common(&visible_order)
                .expect("preflighted iceberg rest unavailable");
            self.commit_iceberg_rest(iceberg, plan.base_remaining, rest_plan);
        }
        Ok(events)
    }

    /// 挂入可见片前拒绝会发生交叉的价格，等价也属于交叉。
    /// 隐藏量保留至后续挂单补量或取消时处理。
    pub fn place_iceberg_post_only(
        &mut self,
        iceberg: IcebergOrder,
        math: &MarketMath,
    ) -> Result<(), RejectReason> {
        let order = iceberg.order();
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let crosses = match order.side {
            Side::Buy => self
                .best_price(Side::Sell)
                .is_some_and(|p| order.price >= p),
            Side::Sell => self.best_price(Side::Buy).is_some_and(|p| order.price <= p),
        };
        if crosses {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        math.trade_notional(order.price, iceberg.peak_size())
            .map_err(Self::reject_quote_math_error)?;
        let mut visible_order = order.clone();
        visible_order.qty = iceberg.peak_size();
        let plan = self.preflight_rest_common(&visible_order)?;
        let total = order.qty.get();
        self.commit_iceberg_rest(iceberg, total, plan);
        Ok(())
    }

    fn preflight_iceberg_rest(
        &self,
        iceberg: &IcebergOrder,
        plan: &IcebergPlan,
    ) -> Result<(), RejectReason> {
        if plan.base_remaining == 0 {
            return Ok(());
        }
        if plan.next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }
        let order = iceberg.order();
        let mut visible_order = order.clone();
        visible_order.qty = Qty::try_new(plan.base_remaining.min(iceberg.peak_size().get()))?;
        // 对手侧成交不会修改该目标价档；此时检查其链接与容量，
        // 上方的分配器预演边界已覆盖补量消耗的优先级。
        self.preflight_rest_common(&visible_order)?;
        Ok(())
    }

    pub(super) fn commit_iceberg_rest(
        &mut self,
        iceberg: IcebergOrder,
        total: u64,
        plan: RestPlan,
    ) {
        let id = iceberg.order().order_id;
        let node = OrderNode::new_iceberg(iceberg, total, plan.priority);
        let slot = match plan.side {
            Side::Buy => commit_rest_target(&mut self.bids, &mut self.arena, node, &plan),
            Side::Sell => commit_rest_target(&mut self.asks, &mut self.arena, node, &plan),
        };
        assert!(self.orders.insert(id, slot).is_none());
        assert_eq!(
            self.queue_priority_allocator
                .allocate()
                .expect("preflighted priority"),
            plan.priority
        );
    }

    /// 执行普通 GTC 订单，并保留冰山挂单的生命周期事件。
    /// 整条命令预检通过后，才挂入新到订单的余量。
    /// 普通挂单沿用仅报告成交事件的既有契约。
    pub fn place_limit_gtc_with_events(
        &mut self,
        order: LimitGtcOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if !self.has_iceberg_maker(order.side) {
            return self
                .place_limit_gtc(order, trade_ids)
                .map(|trades| trades.into_iter().map(EngineEvent::Trade).collect());
        }
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            None,
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg-aware GTC"
        );
        if plan.base_remaining > 0 {
            if plan.next_priority >= self.queue_priority_allocator.policy().effective_ceiling() {
                return Err(RejectReason::PrioritySpaceExhaustion);
            }
            let mut residual = order.clone();
            residual.qty = Qty::try_new(plan.base_remaining)?;
            self.preflight_rest_common(&residual)?;
        }
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        if plan.base_remaining > 0 {
            let mut residual = order;
            residual.qty = Qty::try_new(plan.base_remaining).expect("positive remaining");
            let rest_plan = self
                .preflight_rest_common(&residual)
                .expect("preflighted GTC rest");
            self.commit_rest(residual, rest_plan);
        }
        Ok(events)
    }

    pub(super) fn execute_next_fill_iceberg(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        let order = incoming.original_order();
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(incoming.remaining()),
            None,
            None,
            Some(1),
        )?;
        if plan.fills.is_empty() {
            return Ok(None);
        }
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, &[trade_id]);
        let trade = Self::trades_only(events)
            .pop()
            .expect("planned fill emitted Trade");
        incoming
            .apply_fill(trade.qty)
            .expect("preflighted incoming quantity");
        Ok(Some(trade))
    }

    pub(super) fn execute_fill_trades_iceberg(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        let order = incoming.original_order();
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(incoming.remaining()),
            None,
            None,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg batch"
        );
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        for fill in &plan.fills {
            incoming
                .apply_fill(fill.qty)
                .expect("preflighted incoming quantity");
        }
        Ok(Self::trades_only(events))
    }

    pub(super) fn execute_market_base_iceberg(
        &mut self,
        incoming: MarketByBase,
        math: Option<(&CommandStartProtectionContext, &MarketMath)>,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let protection = if let Some((context, math)) = math {
            effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?
        } else {
            None
        };
        let plan = self.preflight_iceberg_plan(
            incoming.side,
            None,
            Budget::Base(incoming.qty.get()),
            math.map(|(_, math)| math),
            protection,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg MarketByBase"
        );
        let mut events =
            self.commit_iceberg_plan(&plan, incoming.side, incoming.order_id, trade_ids);
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }
        Ok(events)
    }

    pub(super) fn execute_market_quote_iceberg(
        &mut self,
        incoming: MarketByQuote,
        math: &MarketMath,
        context: Option<&CommandStartProtectionContext>,
        trade_ids: &[TradeId],
    ) -> Result<ProtectedMarketByQuoteExecution, RejectReason> {
        if self.contains_active_order(incoming.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        let protection = if let Some(context) = context {
            effective_protection_at_command_start_with_source(
                incoming.market_protection.as_ref(),
                context,
                incoming.side,
                math,
            )?
        } else {
            None
        };
        let plan = self.preflight_iceberg_plan(
            incoming.side,
            None,
            Budget::Quote(QuoteAmount::new(incoming.quote_budget.get())),
            Some(math),
            protection,
            None,
        )?;
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg MarketByQuote"
        );
        let mut events =
            self.commit_iceberg_plan(&plan, incoming.side, incoming.order_id, trade_ids);
        if let Some(stop) = plan.stop {
            events.push(EngineEvent::MarketProtectionTriggered {
                order_id: incoming.order_id,
                next_price: stop.next_price,
                effective_worst_price: stop.effective_worst_price,
                source: stop.source,
            });
        }
        Ok(ProtectedMarketByQuoteExecution::new(
            events,
            plan.spent,
            plan.quote_remaining,
        ))
    }

    pub(super) fn place_immediate_iceberg(
        &mut self,
        order: LimitGtcOrder,
        math: &MarketMath,
        trade_ids: &[TradeId],
        fok: bool,
    ) -> Result<LimitImmediateExecution, RejectReason> {
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        math.trade_notional(order.price, order.qty)
            .map_err(Self::reject_quote_math_error)?;
        let plan = self.preflight_iceberg_plan(
            order.side,
            Some(order.price),
            Budget::Base(order.qty.get()),
            Some(math),
            None,
            None,
        )?;
        if fok && plan.base_remaining > 0 {
            return Err(RejectReason::FokInsufficientLiquidity);
        }
        assert!(
            trade_ids.len() >= plan.fills.len(),
            "insufficient TradeIds for iceberg immediate Limit"
        );
        let events = self.commit_iceberg_plan(&plan, order.side, order.order_id, trade_ids);
        let canceled = if plan.base_remaining == 0 {
            None
        } else {
            Some(Qty::try_new(plan.base_remaining).expect("positive IOC remaining"))
        };
        Ok(LimitImmediateExecution::from_events(
            order.order_id,
            events,
            canceled,
        ))
    }
}
