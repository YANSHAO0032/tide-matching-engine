use crate::{
    IncomingOrder, OrderArena, OrderIndex, OrderNode, PriceLevel, QueuePriorityAllocator,
    QueuePriorityPolicy,
};
use matching_domain::{
    LimitGtcOrder, OrderId, Price, Qty, QueuePriority, RejectReason, Side, TradeEvent, TradeId,
};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy)]
/// 指明已预检的 residual 将追加到既有档位还是新建档位。
///
/// 该枚举只保存 commit 前后必须保持一致的值，不保留对容器的借用；
/// 因而预检完成后仍可执行 maker mutation，再以 fail-closed 断言检查
/// commit 时的 owner 状态没有发生意外漂移。
enum RestTargetPlan {
    Existing {
        tail: OrderIndex,
        count_before: u32,
        total_before: u64,
        count_after: u32,
        total_after: u64,
    },
    New {
        count_after: u32,
        total_after: u64,
    },
}

#[derive(Debug, Clone, Copy)]
/// `preflight_rest_common` 产生的只读挂单提交计划。
///
/// 它记录 identity、计划分配的 priority 及目标档位的聚合快照。
/// public `rest` 另行施加 strict non-crossing；完整 Limit GTC 则在
/// maker 已被预检后复用本计划，保证 residual 的可预期拒绝发生在首笔成交前。
struct RestPlan {
    order_id: OrderId,
    side: Side,
    price: Price,
    priority: QueuePriority,
    target: RestTargetPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// 连续成交的只读预测结果。
///
/// `trade_count` 决定调用方至少要提供多少 TradeId，`remaining` 是
/// 本次连续撮合结束后 taker 的预测余量。它不拥有节点或可变引用，
/// 也不改变 incoming、订单簿或 priority allocator。
struct FillPlan {
    trade_count: usize,
    remaining: u64,
}

/// 单市场生产簿的容器组合与只读查询入口。
///
/// 价格顺序由双侧有序索引保存，同价 FIFO 由 PriceLevel 链表维护；
/// OrderId 索引只承担定位，不决定业务顺序。当前支持已确定应挂单的
/// Limit GTC 余量、已知 ID 的 Cancel，以及完整 Limit GTC 的连续
/// maker/taker 成交与 taker 余量挂单。
#[derive(Debug)]
pub struct ProductionOrderBook {
    /// 买侧价格档位；合法非空条目中的节点必须具有相同价格和 Buy 方向。
    bids: BTreeMap<Price, PriceLevel>,
    /// 卖侧价格档位；合法非空条目中的节点必须具有相同价格和 Sell 方向。
    asks: BTreeMap<Price, PriceLevel>,
    /// 当前 active OrderId 到 Arena 槽位的定位索引，必须与节点身份一致。
    orders: HashMap<OrderId, OrderIndex>,
    /// 节点的唯一存储；价格档位和 ID 索引通过 OrderIndex 引用它。
    arena: OrderArena,
    /// 本市场独占的 priority 状态；只读查询不消耗序号。
    queue_priority_allocator: QueuePriorityAllocator,
}

impl ProductionOrderBook {
    /// 使用已验证的策略创建空簿，market-local priority 从零开始。
    pub fn new(policy: QueuePriorityPolicy) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            arena: OrderArena::new(),
            queue_priority_allocator: QueuePriorityAllocator::new(policy),
        }
    }

    /// 查询指定簿侧的最优价格，空侧返回 None；side 不是 incoming 方向。
    ///
    /// 契约要求 Buy 取最高 bid、Sell 取最低 ask，不分配 priority 或缓存价格。
    pub fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.asks.first_key_value().map(|(price, _)| *price),
        }
    }

    /// 经单键 ID 查找借用 Arena 节点；未知 ID 返回 None。
    ///
    /// # Panics
    ///
    /// 索引命中但槽位不存在或节点身份不匹配时，按内部不变量损坏停止，
    /// 不将损坏静默解释成订单不存在。此查询不遍历或校验整条价档链。
    pub fn order(&self, order_id: OrderId) -> Option<&OrderNode> {
        let index = *self.orders.get(&order_id)?;

        let node = self.arena.get(index).unwrap_or_else(|| {
            panic!(
                "ProductionOrderBook invariant violation: \
                 order index references missing arena slot: \
                 order_id={order_id:?}, index={index:?}"
            )
        });
        assert_eq!(
            node.original_order().order_id,
            order_id,
            "ProductionOrderBook invariant violation: \
             order index points to different order: \
             requested={order_id:?}, actual={:?}, index={index:?}",
            node.original_order().order_id,
        );
        Some(node)
    }

    /// 读取下一次分配将使用的 priority，不推进 allocator。
    pub fn next_queue_priority(&self) -> u128 {
        self.queue_priority_allocator.next()
    }

    /// 将已确认不可成交的 Limit GTC 余量挂入对应价格档尾。
    ///
    /// 此窄 API 不执行撮合或生成事件；调用方必须先保证其不会 crossing/locking。
    /// 可返回的拒绝保持簿、Arena 和 priority allocator 完全不变。
    ///
    /// # Panics
    ///
    /// 若调用方传入会 crossing 或 locking 的订单，或内部状态/提交顺序损坏，
    /// 方法会 fail-closed 而不是将其伪装成另一种业务拒绝。
    pub fn rest(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        // 1. 与 crossing 无关的全部可预期失败先预检。
        let plan = self.preflight_rest_common(&order)?;

        // 2. public rest 自身要求严格不可成交。
        //    后续完整 GTC 路径不会在成交前调用这一层。
        self.assert_rest_strictly_non_crossing(&plan);

        // 3. 此处开始不再允许预期业务 Err。
        self.commit_rest(order, plan);

        Ok(())
    }

    /// 强制 public `rest` 的非成交边界。
    ///
    /// common rest preflight 故意不检查对手侧，使完整 Limit GTC 能在
    /// 连续 maker 撮合前预检 residual。调用该 helper 的位置必须已经
    /// 明确：此时 residual 不应再与任何最优对手价 crossing 或 locking。
    fn assert_rest_strictly_non_crossing(&self, plan: &RestPlan) {
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
    /// 预检/提交之间的 identity、priority、Arena 或目标档位分歧表示
    /// 内部损坏，必须 fail-closed panic，不能在已经写入部分状态后返回 Err。
    fn commit_rest(&mut self, order: LimitGtcOrder, plan: RestPlan) {
        // ------------------------------------------------------------
        // 0. plan / input identity。
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

        // Common preflight 后不能出现同 ID。
        assert!(
            !self.orders.contains_key(&plan.order_id),
            "ProductionOrderBook invariant violation: \
         OrderId appeared between rest preflight and commit"
        );

        // matching/cancel 不消费 QueuePriority。
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

        // Arena 的具体 free slot 可以变化，但插入能力不能消失。
        self.arena.preflight_insert().unwrap_or_else(|reason| {
            panic!(
                "ProductionOrderBook invariant violation: \
                 Arena insert feasibility changed after rest preflight: \
                 reason={reason:?}"
            )
        });

        let node = OrderNode::new(order, plan.priority);

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

        // priority 必须最后推进。
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

    /// 按已知 OrderId 从生产订单簿中取消 resting order。
    ///
    /// 成功返回被移除的运行态订单；返回节点已经脱离 PriceLevel，
    /// 其 `prev` / `next` 均为 None。
    ///
    /// Cancel 不分配、消耗或重排 QueuePriority。
    ///
    /// # Errors
    ///
    /// - `OrderNotFound`：当前 active book 中不存在该 OrderId。
    ///
    /// # Panics
    ///
    /// 如果 ID 索引指向不存在的 Arena slot、节点身份不匹配、
    /// 对应价格档不存在或 PriceLevel 内部链接损坏，视为内部状态损坏，
    /// fail-closed。
    pub fn cancel(&mut self, order_id: OrderId) -> Result<OrderNode, RejectReason> {
        // 1. 先按 ID 精确定位。
        //
        // 未知 ID 必须在任何写操作之前返回。
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

        // immutable borrow 到这里结束。
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
                    // 中间或尾节点走 unlink。
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

        // 4. 解除 ID -> slot 映射。
        //
        // 从最初查找到这里一直持有 &mut self 的独占访问，
        // 映射不可能合法地发生变化。
        let mapped = self.orders.remove(&order_id);

        assert_eq!(
            mapped,
            Some(index),
            "ProductionOrderBook invariant violation: \
         cancel mapping changed during mutation"
        );

        Ok(removed)
    }

    /// 对当前 incoming 最多执行一笔成交。
    ///
    /// - 仅选择当前对手盘 best price 的 FIFO head maker；
    /// - 成交价始终取 resting maker price；
    /// - maker 严格部分成交时保留原 slot/FIFO/priority；
    /// - maker 完全成交时通过 `pop_front` 移除；
    /// - 不循环到下一 maker；
    /// - 不将 incoming 剩余量重新挂簿；
    /// - TradeId 由调用方提供，本方法不分配。
    ///
    /// 无可成交 maker 或 incoming 已耗尽时返回 `Ok(None)`，
    /// 且 book、incoming、allocator 全部保持不变。
    pub fn execute_next_fill_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        // ------------------------------------------------------------
        // 1. exhausted incoming：纯 no-op。
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
        // 2. 只读选择 best price。
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
        // 3. 只读定位 best level 的 FIFO head。
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
        // 4. ID index 必须在任何写入前精确匹配 maker。
        // ------------------------------------------------------------

        assert_eq!(
            self.orders.get(&maker_order_id),
            Some(&head_index),
            "ProductionOrderBook invariant violation: \
         maker OrderId index does not point to selected head"
        );

        // ------------------------------------------------------------
        // 5. 规划本次唯一 fill，并把所有数量运算提前。
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

        // min() 保证至少有一侧被吃完。
        assert!(
            incoming_after == 0 || maker_after == 0,
            "ProductionOrderBook invariant violation: invalid next-fill plan"
        );

        // ------------------------------------------------------------
        // 6. 先构造事件 payload。
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
        // 7. 提交 maker。
        // ------------------------------------------------------------

        if maker_after > 0 {
            // strict partial：
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
            // full/exact maker：必须物理移除 head，
            // 绝不能先把 remaining 改成零再留下来。
            let level_became_empty = match maker_side {
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

                    level.is_empty()
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

                    level.is_empty()
                }
            };

            // pop_front 已经释放 Arena slot。
            // 这里只解除 OrderId 映射，绝不能再次 arena.remove。
            let removed_index = self.orders.remove(&maker_order_id);

            assert_eq!(
                removed_index,
                Some(head_index),
                "ProductionOrderBook invariant violation: \
             maker OrderId mapping changed during fill"
            );

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
        // 8. 最后扣减 incoming。
        //
        // checked_sub 已在任何写入之前成功；
        // 与 ReferenceOrderBook 的执行顺序保持一致。
        // ------------------------------------------------------------

        incoming.apply_fill(fill_qty)?;

        Ok(Some(trade))
    }

    /// 按价格优先、同价 FIFO 连续执行零笔或多笔 maker 成交。
    ///
    /// `trade_ids` 由调用方提供。本订单簿：
    /// - 不自行分配 TradeId；
    /// - 不验证 TradeId 的全局唯一性；
    /// - 不保存或推进 TradeId allocator。
    ///
    /// 调用方必须提供合法且足量的 TradeId 序列。
    /// 多余的 TradeId 不会被使用。
    ///
    /// 在任何状态修改前，本方法会只读计算本次 incoming 实际需要的
    /// TradeId 数量；如果 `trade_ids` 不足则 fail-closed panic。
    ///
    /// 本方法只连续消费当前可成交的 resting maker：
    /// - 不将剩余 incoming 自动 rest；
    /// - 不分配新的 QueuePriority；
    /// - 不实现 IOC/FOK/PostOnly/Market/Stop/Iceberg 等更高层语义。
    pub fn execute_fill_trades(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
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
        // 不复制 maker mutation。
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

    /// 在一个价格档内模拟 FIFO 成交，并验证单步提交依赖的局部结构。
    ///
    /// 该扫描只读地推进 `incoming_remaining`，返回实际需要的 TradeId 数量。
    /// 它检查的链表、身份、聚合与 priority 关系必须足以让后续
    /// `execute_next_fill_trade` 不会因可预期数据错误在部分 batch 后返回 Err。
    fn preflight_level_fill_count(
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

        // pop_front / partial-fill 都会依赖合法 tail，
        // 因此在任何成交发生前先验证 tail 局部状态。
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

            // 验证当前模拟 head 的结构。
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

            // maker partial 必然意味着 incoming 已耗尽，
            // 这一笔就是整个 batch 的最后一笔。
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

            // full maker：模拟 pop_front。
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

            // incoming 正好在该 maker 上耗尽。
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

    /// 按价格优先和同价 FIFO 构造整个连续撮合的只读计划。
    ///
    /// Buy 只从低到高扫描可成交 asks，Sell 只从高到低扫描可成交 bids；
    /// HashMap 仅在逐节点身份校验中按单键读取，绝不决定扫描或事件顺序。
    fn preflight_fill_plan(&self, incoming: &IncomingOrder) -> FillPlan {
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
                // ask: 低 -> 高
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
                // bid: 高 -> 低
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

    /// 预检任何将要 rest 的 Limit GTC 载荷，而不检查对手侧价格。
    ///
    /// 业务拒绝顺序固定为 duplicate、priority high-water、同侧聚合/计数
    /// 与 Arena feasibility。这样完整 Limit GTC 可以先模拟成交并用实际
    /// residual 调用本函数，确保所有可预期 rest 错误都发生在首笔成交前。
    fn preflight_rest_common(&self, order: &LimitGtcOrder) -> Result<RestPlan, RejectReason> {
        let order_id = order.order_id;
        let side = order.side;
        let price = order.price;
        let qty = order.qty.get();

        // ------------------------------------------------------------
        // 1. Duplicate 必须绝对最先检查。
        // ------------------------------------------------------------

        if self.orders.contains_key(&order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        // ------------------------------------------------------------
        // 2. QueuePriority high-water。
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
        // 4. Arena insert feasibility。
        //
        // 必须是 common preflight 的最后一个可预期失败点。
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

    /// 完整执行一笔 Limit GTC。
    ///
    /// 顺序：
    /// 1. duplicate precheck；
    /// 2. 只读规划全部可成交 maker；
    /// 3. 检查 TradeId 数量；
    /// 4. 若存在 GTC remainder，预检 remainder rest；
    /// 5. 执行连续成交；
    /// 6. 若仍有 remainder，将其挂回本方订单簿。
    ///
    /// TradeId 由调用方负责提供合法且足量的序列；
    /// 本订单簿不分配、不验证全局唯一性。
    ///
    /// Exact fill 不需要也不会消耗新的 QueuePriority。
    pub fn place_limit_gtc(
        &mut self,
        order: LimitGtcOrder,
        trade_ids: &[TradeId],
    ) -> Result<Vec<TradeEvent>, RejectReason> {
        let order_id = order.order_id;

        // ------------------------------------------------------------
        // 1. Duplicate 必须绝对最先。
        // ------------------------------------------------------------

        if self.orders.contains_key(&order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }

        let mut incoming = IncomingOrder::new(order.clone());

        // ------------------------------------------------------------
        // 2. 完整只读撮合规划。
        // ------------------------------------------------------------

        let incoming_remaining_before_preflight = incoming.remaining();

        let fill_plan = self.preflight_fill_plan(&incoming);

        // preflight 必须只读。
        assert_eq!(
            incoming.remaining(),
            incoming_remaining_before_preflight,
            "ProductionOrderBook invariant violation: \
     fill preflight mutated incoming remaining"
        );

        // ------------------------------------------------------------
        // 3. TradeId 必须在第一笔成交前足量。
        // ------------------------------------------------------------

        assert!(
            trade_ids.len() >= fill_plan.trade_count,
            "ProductionOrderBook invariant violation: \
         insufficient TradeIds for Limit GTC: \
         required={}, provided={}",
            fill_plan.trade_count,
            trade_ids.len(),
        );

        // ------------------------------------------------------------
        // 4. 只有真的存在 remainder，才允许触碰 Rest preflight。
        //
        // exact fill:
        //   不检查 priority
        //   不检查 same-side aggregate
        //   不检查 Arena insert
        // ------------------------------------------------------------

        let rest_commit = if fill_plan.remaining > 0 {
            let residual_qty = Qty::try_new(fill_plan.remaining).expect(
                "positive preflighted Limit GTC remainder \
                 must form a valid Qty",
            );

            let mut residual_order = order.clone();

            // 当前 OrderNode::new 会以 order.qty 初始化 remaining，
            // 因此挂簿 payload 必须使用真实 remainder。
            residual_order.qty = residual_qty;

            let rest_plan = self.preflight_rest_common(&residual_order)?;

            Some((residual_order, rest_plan))
        } else {
            None
        };

        // ------------------------------------------------------------
        // 此处为写屏障。
        //
        // 到这里：
        // - duplicate 已排除
        // - maker 链已预检
        // - TradeId 已足量
        // - 若需要 rest，priority / aggregate / Arena 已预检
        // ------------------------------------------------------------

        let trades = self
            .execute_fill_trades(&mut incoming, &trade_ids[..fill_plan.trade_count])
            .unwrap_or_else(|reason| {
                panic!(
                    "ProductionOrderBook invariant violation: \
                 preflighted Limit GTC execution returned error: \
                 reason={reason:?}"
                )
            });

        assert_eq!(
            trades.len(),
            fill_plan.trade_count,
            "ProductionOrderBook invariant violation: \
         Limit GTC committed trade count differs from preflight"
        );

        assert_eq!(
            incoming.remaining(),
            fill_plan.remaining,
            "ProductionOrderBook invariant violation: \
         Limit GTC committed remaining differs from preflight"
        );

        // ------------------------------------------------------------
        // 5. 有余量才 rest。
        // ------------------------------------------------------------

        if let Some((residual_order, rest_plan)) = rest_commit {
            assert!(
                incoming.remaining() > 0,
                "ProductionOrderBook invariant violation: \
             preflight expected remainder but execution exhausted taker"
            );

            assert_eq!(
                residual_order.qty.get(),
                incoming.remaining(),
                "ProductionOrderBook invariant violation: \
             residual order qty differs from incoming remaining"
            );

            // 此时所有 <= limit 的对手 maker 应已经处理完成。
            // 若仍 crossed / locked，属于内部规划/执行分歧。
            self.assert_rest_strictly_non_crossing(&rest_plan);

            // 已经在第一笔成交前完成 preflight，
            // commit 不允许返回业务错误。
            self.commit_rest(residual_order, rest_plan);
        } else {
            assert_eq!(
                incoming.remaining(),
                0,
                "ProductionOrderBook invariant violation: \
             exact-fill plan left incoming remainder"
            );
        }

        Ok(trades)
    }
}

/// 将已预检 node 接入目标价格档，并返回它的稳定 Arena 槽位。
///
/// 调用方随后才写入 OrderId 索引并推进 allocator。这里的所有断言都在
/// `push_back` 或插入新档位前比较预检快照；任何差异是内部状态漂移，
/// 不能退化为业务 `RejectReason`。
fn commit_rest_target(
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

            // 所有 divergence 检查仍发生在 push_back 写入前。
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

#[cfg(test)]
mod tests {
    //! ProductionOrderBook 的查询、rest、Cancel、状态保持与损坏 fail-closed 验证。
    use super::*;
    use crate::OrderNode;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};
    use std::collections::{BTreeSet, HashSet};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// 使用与参考模型一致的真实 guard 创建有效测试策略。
    fn policy() -> QueuePriorityPolicy {
        QueuePriorityPolicy::try_new(1024).unwrap()
    }

    /// 从受控有效样本构造最小 Limit GTC 输入。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 仅测试代码允许直接构造 ProductionOrderBook 的非空状态。
    ///
    /// 必须经过真实 allocator + Arena + PriceLevel::push_back，
    /// 不允许只往 BTreeMap 塞空 PriceLevel。
    fn rest_fixture(book: &mut ProductionOrderBook, original: LimitGtcOrder) -> OrderIndex {
        let order_id = original.order_id;
        let side = original.side;
        let price = original.price;

        let priority = book.queue_priority_allocator.allocate().unwrap();

        let levels = match side {
            Side::Buy => &mut book.bids,
            Side::Sell => &mut book.asks,
        };

        let level = levels.entry(price).or_insert_with(PriceLevel::new);

        let slot = level
            .push_back(&mut book.arena, OrderNode::new(original, priority))
            .unwrap();

        assert!(
            book.orders.insert(order_id, slot).is_none(),
            "fixture attempted to insert duplicate OrderId"
        );

        slot
    }

    /// 按价格顺序投影档位元数据，用于比较查询前后状态。
    fn level_snapshot(levels: &BTreeMap<Price, PriceLevel>) -> Vec<LevelSnapshotEntry> {
        levels
            .iter()
            .map(|(price, level)| LevelSnapshotEntry {
                price: *price,
                head: level.head(),
                tail: level.tail(),
                count: level.count(),
                total_visible_qty: level.total_visible_qty(),
            })
            .collect()
    }

    fn assert_rest_error_preserves_state(
        book: &mut ProductionOrderBook,
        candidate: LimitGtcOrder,
        expected: RejectReason,
    ) {
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        assert_eq!(book.rest(candidate), Err(expected));

        assert_eq!(
            level_snapshot(&book.bids),
            bids_before,
            "failed rest mutated bids"
        );

        assert_eq!(
            level_snapshot(&book.asks),
            asks_before,
            "failed rest mutated asks"
        );

        assert_eq!(book.orders, orders_before, "failed rest mutated ID index");

        assert_eq!(
            book.arena.test_snapshot(),
            arena_before,
            "failed rest mutated Arena slots/free-list"
        );

        assert_eq!(
            book.queue_priority_allocator.next(),
            next_before,
            "failed rest consumed QueuePriority"
        );

        assert_eq!(
            book.queue_priority_allocator.policy(),
            &policy_before,
            "failed rest mutated allocator policy"
        );

        // 输入前簿本身是合法状态，因此失败后仍必须合法。
        assert_eq!(validate_book(book), Ok(()));
    }

    /// 一个价格档位的只读测试投影，不是持久化快照格式。
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LevelSnapshotEntry {
        /// 有序价格索引中的 key。
        price: Price,
        /// 此档声明的 FIFO 首尾槽位。
        head: Option<OrderIndex>,
        tail: Option<OrderIndex>,
        /// 此档声明的节点数和可见数量合计。
        count: u32,
        total_visible_qty: u64,
    }

    /// 按指定 ID 查询得到的完整节点投影，用于查询前后相等性比较。
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NodeSnapshotEntry {
        /// 发起查询时使用的 ID，用于保留查询结果与输入的对应关系。
        order_id: OrderId,
        /// 原始订单请求及当前剩余量。
        original: LimitGtcOrder,
        remaining: u64,
        /// 当前 FIFO priority 及双向邻居槽位。
        priority: QueuePriority,
        prev: Option<OrderIndex>,
        next: Option<OrderIndex>,
    }

    /// 按调用方给定的 ID 顺序投影 live 节点，不依赖 HashMap 迭代序。
    ///
    /// 仅覆盖这些 ID 可达的节点，不包含 Arena 空槽和 free-list。
    fn node_snapshot(book: &ProductionOrderBook, ids: &[OrderId]) -> Vec<NodeSnapshotEntry> {
        ids.iter()
            .map(|id| {
                let node = book.order(*id).unwrap();

                NodeSnapshotEntry {
                    order_id: *id,
                    original: node.original_order().clone(),
                    remaining: node.remaining(),
                    priority: node.priority(),
                    prev: node.prev(),
                    next: node.next(),
                }
            })
            .collect()
    }
    #[test]
    fn empty_book_and_single_side_queries() {
        let mut book = ProductionOrderBook::new(policy());

        assert_eq!(book.best_price(Side::Buy), None);
        assert_eq!(book.best_price(Side::Sell), None);
        assert_eq!(book.order(OrderId::new(1)), None);
        assert_eq!(book.next_queue_priority(), 0);

        rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(100).unwrap())
        );
        assert_eq!(book.best_price(Side::Sell), None);

        assert_eq!(
            book.order(OrderId::new(1))
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(1)
        );

        assert_eq!(book.order(OrderId::new(999)), None);
        assert_eq!(book.next_queue_priority(), 1);
    }
    #[test]
    fn best_price_uses_tree_endpoints_and_keeps_sides_isolated() {
        let mut book = ProductionOrderBook::new(policy());

        // 故意不按价格顺序插入。
        for original in [
            order(1, Side::Buy, 90, 1),
            order(2, Side::Buy, 105, 1),
            order(3, Side::Buy, 95, 1),
            order(4, Side::Sell, 130, 1),
            order(5, Side::Sell, 110, 1),
            order(6, Side::Sell, 120, 1),
        ] {
            rest_fixture(&mut book, original);
        }

        // Buy 取最高 bid。
        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(105).unwrap())
        );

        // Sell 取最低 ask。
        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(110).unwrap())
        );

        // 两侧树完全隔离。
        assert!(book.bids.contains_key(&Price::try_new(105).unwrap()));
        assert!(!book.asks.contains_key(&Price::try_new(105).unwrap()));

        assert!(book.asks.contains_key(&Price::try_new(110).unwrap()));
        assert!(!book.bids.contains_key(&Price::try_new(110).unwrap()));
    }
    #[test]
    fn best_price_handles_price_boundaries_without_crossing_fixture() {
        let min_price = 1;
        let max_price = i64::MAX;

        // ---------- Buy-only book ----------

        let mut bids = ProductionOrderBook::new(policy());

        rest_fixture(&mut bids, order(1, Side::Buy, min_price, 1));

        rest_fixture(&mut bids, order(2, Side::Buy, max_price, 1));

        assert_eq!(
            bids.best_price(Side::Buy),
            Some(Price::try_new(max_price).unwrap())
        );

        // 单侧 fixture，另一侧必须保持为空。
        assert_eq!(bids.best_price(Side::Sell), None);
        assert!(bids.asks.is_empty());

        // 两个边界价格都真实存在于 bid tree。
        assert!(bids.bids.contains_key(&Price::try_new(min_price).unwrap()));

        assert!(bids.bids.contains_key(&Price::try_new(max_price).unwrap()));

        // ---------- Sell-only book ----------

        let mut asks = ProductionOrderBook::new(policy());

        rest_fixture(&mut asks, order(3, Side::Sell, max_price, 1));

        rest_fixture(&mut asks, order(4, Side::Sell, min_price, 1));

        assert_eq!(
            asks.best_price(Side::Sell),
            Some(Price::try_new(min_price).unwrap())
        );

        // 单侧 fixture，另一侧必须保持为空。
        assert_eq!(asks.best_price(Side::Buy), None);
        assert!(asks.bids.is_empty());

        assert!(asks.asks.contains_key(&Price::try_new(min_price).unwrap()));

        assert!(asks.asks.contains_key(&Price::try_new(max_price).unwrap()));
    }

    #[test]
    fn same_price_orders_are_individually_addressable() {
        let mut book = ProductionOrderBook::new(policy());

        let first = rest_fixture(&mut book, order(30, Side::Buy, 100, 4));

        let second = rest_fixture(&mut book, order(10, Side::Buy, 100, 5));

        let third = rest_fixture(&mut book, order(20, Side::Buy, 100, 6));

        let price = Price::try_new(100).unwrap();
        let level = book.bids.get(&price).unwrap();

        assert_eq!(level.count(), 3);
        assert_eq!(level.head(), Some(first));
        assert_eq!(level.tail(), Some(third));

        assert_eq!(book.arena.get(first).unwrap().next(), Some(second));
        assert_eq!(book.arena.get(second).unwrap().next(), Some(third));

        // 查询按 OrderId 定位，与数值 ID 排序无关。
        for id in [30, 10, 20] {
            assert_eq!(
                book.order(OrderId::new(id))
                    .unwrap()
                    .original_order()
                    .order_id,
                OrderId::new(id)
            );
        }

        // 三次 fixture allocation。
        assert_eq!(book.next_queue_priority(), 3);
    }
    #[test]
    fn repeated_queries_preserve_state() {
        let mut book = ProductionOrderBook::new(policy());

        for original in [
            order(30, Side::Buy, 95, 4),
            order(10, Side::Buy, 95, 5),
            order(40, Side::Sell, 105, 6),
            order(20, Side::Sell, 110, 7),
        ] {
            rest_fixture(&mut book, original);
        }

        let ids = [
            OrderId::new(30),
            OrderId::new(10),
            OrderId::new(40),
            OrderId::new(20),
        ];

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let nodes_before = node_snapshot(&book, &ids);
        let next_before = book.next_queue_priority();
        let arena_before = book.arena.test_snapshot();

        let allocator_policy_before = *book.queue_priority_allocator.policy();
        for _ in 0..32 {
            assert_eq!(
                book.best_price(Side::Buy),
                Some(Price::try_new(95).unwrap())
            );

            assert_eq!(
                book.best_price(Side::Sell),
                Some(Price::try_new(105).unwrap())
            );

            for id in ids {
                assert_eq!(book.order(id).unwrap().original_order().order_id, id);
            }

            assert_eq!(book.order(OrderId::new(999)), None);

            assert_eq!(book.next_queue_priority(), next_before);
        }

        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);

        assert_eq!(book.orders, orders_before);

        assert_eq!(node_snapshot(&book, &ids), nodes_before);

        // Arena 的 slots + free-list 必须完整不变。
        assert_eq!(book.arena.test_snapshot(), arena_before);

        // Query 不得消耗 QueuePriority。
        assert_eq!(book.next_queue_priority(), next_before);

        // Query 同样不能修改 allocator policy。
        assert_eq!(
            book.queue_priority_allocator.policy(),
            &allocator_policy_before
        );
    }
    #[test]
    fn order_panics_when_index_references_missing_slot() {
        let mut book = ProductionOrderBook::new(policy());

        let order_id = OrderId::new(1);

        let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        assert_eq!(book.orders.get(&order_id), Some(&slot));

        // 只破坏 Arena，orders 索引仍存在。
        let removed = book.arena.remove(slot).unwrap();
        assert_eq!(removed.original_order().order_id, order_id);

        assert!(book.arena.get(slot).is_none());
        assert!(book.orders.contains_key(&order_id));

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.order(order_id);
        }));

        assert!(
            result.is_err(),
            "missing indexed arena slot must fail closed"
        );
    }
    #[test]
    fn order_panics_when_index_points_to_different_order() {
        let mut book = ProductionOrderBook::new(policy());

        let first_id = OrderId::new(1);
        let second_id = OrderId::new(2);

        let first_slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let second_slot = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

        assert_ne!(first_slot, second_slot);

        // PriceLevel / Arena 都仍是真实合法链。
        // 仅破坏 OrderId -> OrderIndex 映射。
        book.orders.insert(first_id, second_slot);

        assert_eq!(
            book.arena
                .get(second_slot)
                .unwrap()
                .original_order()
                .order_id,
            second_id
        );

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.order(first_id);
        }));

        assert!(
            result.is_err(),
            "index pointing to another order must fail closed"
        );

        // 被错误指向的第二张订单自身仍可正常查询。
        assert_eq!(
            book.order(second_id).unwrap().original_order().order_id,
            second_id
        );
    }
    fn validate_book(book: &ProductionOrderBook) -> Result<(), String> {
        fn validate_side(
            book: &ProductionOrderBook,
            side: Side,
            levels: &BTreeMap<Price, PriceLevel>,
            visited_slots: &mut BTreeSet<OrderIndex>,
            seen_ids: &mut HashSet<OrderId>,
            max_priority: &mut Option<QueuePriority>,
        ) -> Result<(), String> {
            for (price, level) in levels {
                // 价格树中禁止保留空档。
                if level.is_empty() || level.count() == 0 {
                    return Err(format!("level/empty: side={side:?}, price={price:?}"));
                }

                let head = level
                    .head()
                    .ok_or_else(|| format!("level/missing-head: side={side:?}, price={price:?}"))?;

                let expected_tail = level
                    .tail()
                    .ok_or_else(|| format!("level/missing-tail: side={side:?}, price={price:?}"))?;

                let mut current = Some(head);
                let mut expected_prev = None;
                let mut previous_priority = None;
                let mut actual_tail = None;

                let mut actual_count = 0u32;
                let mut actual_qty = 0u64;

                while let Some(index) = current {
                    // 全 book 共用 visited。
                    //
                    // 因此同时防：
                    // 1. 单档位内部 cycle；
                    // 2. 同一个 slot 被多个档位重复引用。
                    if !visited_slots.insert(index) {
                        return Err(format!(
                            "link/cycle-or-duplicate: \
                         side={side:?}, price={price:?}, slot={index:?}"
                        ));
                    }

                    let node = book.arena.get(index).ok_or_else(|| {
                        format!(
                            "arena/missing-linked-slot: \
                         side={side:?}, price={price:?}, slot={index:?}"
                        )
                    })?;

                    if node.prev() != expected_prev {
                        return Err(format!(
                            "link/broken-prev: \
                         side={side:?}, price={price:?}, slot={index:?}, \
                         expected={expected_prev:?}, actual={:?}",
                            node.prev()
                        ));
                    }

                    if node.original_order().side != side {
                        return Err(format!(
                            "node/side-mismatch: \
                         tree_side={side:?}, node_side={:?}, \
                         price={price:?}, slot={index:?}",
                            node.original_order().side
                        ));
                    }

                    if node.original_order().price != *price {
                        return Err(format!(
                            "node/price-mismatch: \
                         tree_price={price:?}, node_price={:?}, \
                         side={side:?}, slot={index:?}",
                            node.original_order().price
                        ));
                    }

                    if node.remaining() == 0 {
                        return Err(format!(
                            "node/zero-remaining: \
                         side={side:?}, price={price:?}, slot={index:?}"
                        ));
                    }

                    if let Some(previous) = previous_priority
                        && node.priority() <= previous
                    {
                        return Err(format!(
                            "priority/non-increasing: \
                         side={side:?}, price={price:?}, slot={index:?}, \
                         previous={previous:?}, current={:?}",
                            node.priority()
                        ));
                    }

                    let order_id = node.original_order().order_id;

                    if !seen_ids.insert(order_id) {
                        return Err(format!(
                            "id-index/duplicate-order-id: \
                         order_id={order_id:?}, slot={index:?}"
                        ));
                    }

                    match book.orders.get(&order_id) {
                        None => {
                            return Err(format!(
                                "id-index/missing: \
                             order_id={order_id:?}, actual_slot={index:?}"
                            ));
                        }

                        Some(mapped) if *mapped != index => {
                            return Err(format!(
                                "id-index/wrong-slot: \
                             order_id={order_id:?}, expected={index:?}, \
                             actual={mapped:?}"
                            ));
                        }

                        Some(_) => {}
                    }

                    actual_count = actual_count.checked_add(1).ok_or_else(|| {
                        format!("level/count-overflow: side={side:?}, price={price:?}")
                    })?;

                    actual_qty = actual_qty.checked_add(node.remaining()).ok_or_else(|| {
                        format!("level/qty-overflow: side={side:?}, price={price:?}")
                    })?;

                    *max_priority = Some(match *max_priority {
                        Some(current_max) if current_max >= node.priority() => current_max,
                        _ => node.priority(),
                    });

                    previous_priority = Some(node.priority());
                    expected_prev = Some(index);
                    actual_tail = Some(index);
                    current = node.next();
                }

                if actual_tail != Some(expected_tail) {
                    return Err(format!(
                        "level/tail-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={expected_tail:?}, actual={actual_tail:?}"
                    ));
                }

                if actual_count != level.count() {
                    return Err(format!(
                        "level/count-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={}, actual={actual_count}",
                        level.count()
                    ));
                }

                if actual_qty != level.total_visible_qty() {
                    return Err(format!(
                        "level/qty-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={}, actual={actual_qty}",
                        level.total_visible_qty()
                    ));
                }
            }

            Ok(())
        }

        let mut visited_slots = BTreeSet::new();
        let mut seen_ids = HashSet::new();
        let mut max_priority = None;

        validate_side(
            book,
            Side::Buy,
            &book.bids,
            &mut visited_slots,
            &mut seen_ids,
            &mut max_priority,
        )?;

        validate_side(
            book,
            Side::Sell,
            &book.asks,
            &mut visited_slots,
            &mut seen_ids,
            &mut max_priority,
        )?;

        // ------------------------------------------------------------
        // OrderId -> OrderIndex 必须是链表节点集合的精确映射。
        // ------------------------------------------------------------

        for (order_id, index) in &book.orders {
            if !visited_slots.contains(index) {
                return Err(format!(
                    "id-index/extra-or-stale: \
                 order_id={order_id:?}, slot={index:?}"
                ));
            }

            let node = book.arena.get(*index).ok_or_else(|| {
                format!(
                    "id-index/missing-arena-slot: \
                 order_id={order_id:?}, slot={index:?}"
                )
            })?;

            if node.original_order().order_id != *order_id {
                return Err(format!(
                    "id-index/node-id-mismatch: \
                 map_id={order_id:?}, node_id={:?}, slot={index:?}",
                    node.original_order().order_id
                ));
            }
        }

        if book.orders.len() != visited_slots.len() {
            return Err(format!(
                "id-index/count-mismatch: \
             index_count={}, linked_count={}",
                book.orders.len(),
                visited_slots.len()
            ));
        }

        // ------------------------------------------------------------
        // Arena occupied slots 必须全部属于某条价格链。
        //
        // test_snapshot:
        // (
        //   Vec<Option<(LimitGtcOrder, u64, QueuePriority, prev, next)>>,
        //   Vec<OrderIndex>,
        // )
        // ------------------------------------------------------------

        let (arena_slots, _) = book.arena.test_snapshot();

        for (position, slot_state) in arena_slots.iter().enumerate() {
            if slot_state.is_none() {
                continue;
            }

            let index = OrderIndex::try_from_usize(position).map_err(|reason| {
                format!("arena/index-overflow: position={position}, reason={reason:?}")
            })?;

            if !visited_slots.contains(&index) {
                return Err(format!("arena/orphan: slot={index:?}"));
            }
        }

        // ------------------------------------------------------------
        // allocator next 必须严格大于所有 active priority。
        // ------------------------------------------------------------

        if let Some(max_active_priority) = max_priority {
            let allocator_next = QueuePriority::new(book.queue_priority_allocator.next());

            if allocator_next <= max_active_priority {
                return Err(format!(
                    "allocator/next-not-ahead: \
                 next={allocator_next:?}, max_active={max_active_priority:?}"
                ));
            }
        }

        // ------------------------------------------------------------
        // 当前 continuous book 不允许 crossed / locked book。
        // highest bid 必须严格小于 lowest ask。
        // ------------------------------------------------------------

        match (book.bids.last_key_value(), book.asks.first_key_value()) {
            (Some((best_bid, _)), Some((best_ask, _))) if *best_bid >= *best_ask => {
                return Err(format!(
                    "book/crossed-or-locked: \
             best_bid={best_bid:?}, best_ask={best_ask:?}"
                ));
            }
            _ => {}
        }

        Ok(())
    }
    #[test]
    fn validate_book_accepts_empty_book() {
        let book = ProductionOrderBook::new(policy());

        assert_eq!(validate_book(&book), Ok(()));
    }

    #[test]
    fn validate_book_accepts_single_side_book() {
        let mut book = ProductionOrderBook::new(policy());

        rest_fixture(&mut book, order(1, Side::Buy, 90, 4));

        rest_fixture(&mut book, order(2, Side::Buy, 100, 5));

        assert_eq!(validate_book(&book), Ok(()));
    }

    #[test]
    fn validate_book_accepts_multi_level_two_sided_book() {
        let mut book = ProductionOrderBook::new(policy());

        for original in [
            order(1, Side::Buy, 90, 4),
            order(2, Side::Buy, 95, 5),
            order(3, Side::Sell, 105, 6),
            order(4, Side::Sell, 110, 7),
        ] {
            rest_fixture(&mut book, original);
        }

        assert_eq!(validate_book(&book), Ok(()));
    }

    #[test]
    fn validate_book_accepts_same_price_fifo_chain() {
        let mut book = ProductionOrderBook::new(policy());

        for original in [
            order(30, Side::Buy, 100, 4),
            order(10, Side::Buy, 100, 5),
            order(20, Side::Buy, 100, 6),
        ] {
            rest_fixture(&mut book, original);
        }

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn validate_book_does_not_mutate_state() {
        let mut book = ProductionOrderBook::new(policy());

        let ids = [
            OrderId::new(1),
            OrderId::new(2),
            OrderId::new(3),
            OrderId::new(4),
        ];

        for original in [
            order(1, Side::Buy, 90, 4),
            order(2, Side::Buy, 95, 5),
            order(3, Side::Sell, 105, 6),
            order(4, Side::Sell, 110, 7),
        ] {
            rest_fixture(&mut book, original);
        }

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let nodes_before = node_snapshot(&book, &ids);
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();
        let policy_before = *book.queue_priority_allocator.policy();

        assert_eq!(validate_book(&book), Ok(()));

        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);
        assert_eq!(book.orders, orders_before);
        assert_eq!(node_snapshot(&book, &ids), nodes_before);
        assert_eq!(book.arena.test_snapshot(), arena_before);

        assert_eq!(book.queue_priority_allocator.next(), next_before);

        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);
    }
    fn assert_book_error(book: &ProductionOrderBook, expected_category: &str) {
        // validator 调用前记录全部可观察业务状态。
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);

        let orders_before = book.orders.clone();

        // 完整覆盖 Arena slots + free-list，
        // 同时也包含所有 occupied OrderNode 状态。
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        // ---------- validate ----------

        let error = validate_book(book).expect_err("corrupted book must fail validation");

        assert!(
            error.starts_with(expected_category),
            "expected category={expected_category:?}, actual={error:?}"
        );

        // ---------- 必须完全只读 ----------

        assert_eq!(
            level_snapshot(&book.bids),
            bids_before,
            "validator mutated bid levels: error={error:?}"
        );

        assert_eq!(
            level_snapshot(&book.asks),
            asks_before,
            "validator mutated ask levels: error={error:?}"
        );

        assert_eq!(
            book.orders, orders_before,
            "validator mutated ID index: error={error:?}"
        );

        assert_eq!(
            book.arena.test_snapshot(),
            arena_before,
            "validator mutated Arena slots/free-list: error={error:?}"
        );

        assert_eq!(
            book.queue_priority_allocator.next(),
            next_before,
            "validator consumed queue priority: error={error:?}"
        );

        assert_eq!(
            book.queue_priority_allocator.policy(),
            &policy_before,
            "validator mutated allocator policy: error={error:?}"
        );
    }
    #[test]
    fn validate_book_rejects_missing_id_index() {
        let mut book = ProductionOrderBook::new(policy());

        rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        book.orders.remove(&OrderId::new(1));

        assert_book_error(&book, "id-index/missing");
    }
    #[test]
    fn validate_book_rejects_wrong_id_index() {
        let mut book = ProductionOrderBook::new(policy());

        let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

        assert_ne!(first, second);

        book.orders.insert(OrderId::new(1), second);

        assert_book_error(&book, "id-index/wrong-slot");
    }
    #[test]
    fn validate_book_rejects_extra_id_index() {
        let mut book = ProductionOrderBook::new(policy());

        let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        book.orders.insert(OrderId::new(999), slot);

        assert_book_error(&book, "id-index/node-id-mismatch");
    }
    #[test]
    fn validate_book_rejects_wrong_node_price() {
        let mut book = ProductionOrderBook::new(policy());

        let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let removed = book.arena.remove(slot).unwrap();

        let replacement = OrderNode::new(order(1, Side::Buy, 101, 5), removed.priority());

        assert_eq!(book.arena.insert(replacement).unwrap(), slot);

        assert_book_error(&book, "node/price-mismatch");
    }
    #[test]
    fn validate_book_rejects_wrong_node_side() {
        let mut book = ProductionOrderBook::new(policy());

        let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let removed = book.arena.remove(slot).unwrap();

        let replacement = OrderNode::new(order(1, Side::Sell, 100, 5), removed.priority());

        assert_eq!(book.arena.insert(replacement).unwrap(), slot);

        assert_book_error(&book, "node/side-mismatch");
    }
    #[test]
    fn validate_book_rejects_empty_price_level() {
        let mut book = ProductionOrderBook::new(policy());

        book.bids
            .insert(Price::try_new(100).unwrap(), PriceLevel::new());

        assert_book_error(&book, "level/empty");
    }
    #[test]
    fn validate_book_rejects_orphan_arena_node() {
        let mut book = ProductionOrderBook::new(policy());

        book.arena
            .insert(OrderNode::new(
                order(99, Side::Buy, 100, 5),
                QueuePriority::new(0),
            ))
            .unwrap();

        assert_book_error(&book, "arena/orphan");
    }
    #[test]
    fn validate_book_rejects_allocator_next_behind_active_priority() {
        let mut book = ProductionOrderBook::new(policy());

        let original = order(1, Side::Buy, 100, 5);
        let order_id = original.order_id;
        let price = original.price;

        // allocator.next() 仍然是 0，
        // 但主动构造一个 priority=10 的 active node。
        let priority = QueuePriority::new(10);

        let mut level = PriceLevel::new();

        let slot = level
            .push_back(&mut book.arena, OrderNode::new(original, priority))
            .unwrap();

        book.bids.insert(price, level);
        book.orders.insert(order_id, slot);

        assert_eq!(book.next_queue_priority(), 0);

        assert_book_error(&book, "allocator/next-not-ahead");
    }
    #[test]
    fn validate_book_rejects_crossed_book() {
        let mut book = ProductionOrderBook::new(policy());

        rest_fixture(&mut book, order(1, Side::Buy, 105, 5));

        rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

        assert_book_error(&book, "book/crossed-or-locked");
    }
    #[test]
    fn validate_book_rejects_cycle_without_looping_forever() {
        let mut book = ProductionOrderBook::new(policy());

        let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

        book.arena.get_mut(second).unwrap().set_next(Some(first));

        assert_book_error(&book, "link/cycle-or-duplicate");
    }
    #[test]
    fn validate_book_accepts_free_arena_slot_outside_active_book() {
        let mut book = ProductionOrderBook::new(policy());

        // 先建立真实合法价格档位。
        rest_fixture(&mut book, order(1, Side::Buy, 95, 5));

        rest_fixture(&mut book, order(2, Side::Sell, 105, 7));

        assert_eq!(validate_book(&book), Ok(()));

        // 再直接向 Arena 插入一个与 book 无关的临时节点。
        // 它从未进入 PriceLevel，也没有进入 orders map。
        let temporary = book
            .arena
            .insert(OrderNode::new(
                order(999, Side::Buy, 90, 1),
                QueuePriority::new(999),
            ))
            .unwrap();

        assert!(book.arena.get(temporary).is_some());

        // 真实 remove，留下 free-list slot。
        let removed = book.arena.remove(temporary).unwrap();

        assert_eq!(removed.original_order().order_id, OrderId::new(999));

        assert!(book.arena.get(temporary).is_none());

        // free slot 不属于 occupied node，因此不是 orphan。
        // 活动盘口仍然完全合法。
        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn validate_book_rejects_locked_book() {
        let mut book = ProductionOrderBook::new(policy());

        rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

        assert_book_error(&book, "book/crossed-or-locked");
    }
    #[test]
    fn validate_book_rejects_non_increasing_level_priority() {
        let mut book = ProductionOrderBook::new(policy());

        let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

        let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

        let second_original = book.arena.get(second).unwrap().original_order().clone();

        let _ = book.arena.remove(second).unwrap();

        let mut replacement =
            OrderNode::new(second_original, book.arena.get(first).unwrap().priority());

        replacement.set_prev(Some(first));

        assert_eq!(book.arena.insert(replacement).unwrap(), second);

        assert_book_error(&book, "priority/non-increasing");

        let _ = book.arena.remove(second).unwrap();
    }
    #[test]
    fn rest_accepts_first_and_two_sided_non_crossing_orders() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(95).unwrap())
        );

        assert_eq!(book.best_price(Side::Sell), None);

        assert_eq!(book.next_queue_priority(), 1);
        assert_eq!(validate_book(&book), Ok(()));

        book.rest(order(2, Side::Sell, 105, 7)).unwrap();

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(95).unwrap())
        );

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(105).unwrap())
        );

        assert_eq!(book.next_queue_priority(), 2);
        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn rest_preserves_same_price_fifo() {
        let mut book = ProductionOrderBook::new(policy());

        for id in [30, 10, 20] {
            book.rest(order(id, Side::Buy, 100, 5)).unwrap();

            assert_eq!(validate_book(&book), Ok(()));
        }

        let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

        assert_eq!(level.count(), 3);
        assert_eq!(level.total_visible_qty(), 15);

        let first = level.head().unwrap();
        let second = book.arena.get(first).unwrap().next().unwrap();
        let third = book.arena.get(second).unwrap().next().unwrap();

        assert_eq!(
            book.arena.get(first).unwrap().original_order().order_id,
            OrderId::new(30)
        );

        assert_eq!(
            book.arena.get(second).unwrap().original_order().order_id,
            OrderId::new(10)
        );

        assert_eq!(
            book.arena.get(third).unwrap().original_order().order_id,
            OrderId::new(20)
        );

        assert_eq!(
            book.arena.get(first).unwrap().priority(),
            QueuePriority::new(0)
        );

        assert_eq!(
            book.arena.get(second).unwrap().priority(),
            QueuePriority::new(1)
        );

        assert_eq!(
            book.arena.get(third).unwrap().priority(),
            QueuePriority::new(2)
        );

        assert_eq!(level.tail(), Some(third));
        assert_eq!(book.next_queue_priority(), 3);
    }
    #[test]
    fn rest_updates_best_price_from_tree_endpoints() {
        let mut book = ProductionOrderBook::new(policy());

        for candidate in [
            order(1, Side::Buy, 90, 1),
            order(2, Side::Buy, 95, 1),
            order(3, Side::Buy, 92, 1),
            order(4, Side::Sell, 110, 1),
            order(5, Side::Sell, 105, 1),
            order(6, Side::Sell, 108, 1),
        ] {
            book.rest(candidate).unwrap();
            assert_eq!(validate_book(&book), Ok(()));
        }

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(95).unwrap())
        );

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(105).unwrap())
        );
    }
    #[test]
    fn rest_rejects_duplicate_id_without_mutation() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        assert_rest_error_preserves_state(
            &mut book,
            // 故意修改 side / price / qty；
            // ID 相同仍必须首先命中 DuplicateOrderId。
            order(1, Side::Sell, 105, 999),
            RejectReason::DuplicateOrderId,
        );

        assert_eq!(book.next_queue_priority(), 1);
    }
    fn policy_with_ceiling(ceiling: u128) -> QueuePriorityPolicy {
        QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(ceiling)).unwrap()
    }
    #[test]
    fn rest_rejects_priority_exhaustion_without_mutation() {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        // next=0，可以完成最后一次合法 allocation。
        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        assert_eq!(book.next_queue_priority(), 1);
        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(validate_book(&book), Ok(()));

        assert_rest_error_preserves_state(
            &mut book,
            order(2, Side::Buy, 90, 5),
            RejectReason::PrioritySpaceExhaustion,
        );
    }
    #[test]
    fn duplicate_id_precedes_priority_exhaustion() {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        assert!(book.queue_priority_allocator.is_exhausted());

        assert_rest_error_preserves_state(
            &mut book,
            order(1, Side::Sell, 105, 7),
            RejectReason::DuplicateOrderId,
        );
    }
    #[test]
    fn rest_same_level_qty_overflow_is_atomic() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 100, u64::MAX)).unwrap();

        assert_eq!(validate_book(&book), Ok(()));
        assert_eq!(book.next_queue_priority(), 1);

        assert_rest_error_preserves_state(
            &mut book,
            order(2, Side::Buy, 100, 1),
            RejectReason::ArithmeticOverflow,
        );

        // 失败不能建立第二张订单。
        assert_eq!(book.order(OrderId::new(2)), None);

        // allocator 同样没有被消耗。
        assert_eq!(book.next_queue_priority(), 1);

        let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

        assert_eq!(level.count(), 1);
        assert_eq!(level.total_visible_qty(), u64::MAX);
    }
    #[test]
    fn rest_panics_before_mutation_if_candidate_would_cross_or_lock() {
        let cases = [
            // Buy == best ask：locked。
            (order(1, Side::Sell, 105, 5), order(2, Side::Buy, 105, 1)),
            // Buy > best ask：crossed。
            (order(1, Side::Sell, 105, 5), order(2, Side::Buy, 110, 1)),
            // Sell == best bid：locked。
            (order(1, Side::Buy, 95, 5), order(2, Side::Sell, 95, 1)),
            // Sell < best bid：crossed。
            (order(1, Side::Buy, 95, 5), order(2, Side::Sell, 90, 1)),
        ];

        for (resting, candidate) in cases {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(resting).unwrap();

            assert_eq!(validate_book(&book), Ok(()));

            let bids_before = level_snapshot(&book.bids);
            let asks_before = level_snapshot(&book.asks);
            let orders_before = book.orders.clone();
            let arena_before = book.arena.test_snapshot();

            let next_before = book.queue_priority_allocator.next();

            let policy_before = *book.queue_priority_allocator.policy();

            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = book.rest(candidate);
            }));

            assert!(result.is_err(), "crossed/locked rest must fail closed");

            assert_eq!(
                level_snapshot(&book.bids),
                bids_before,
                "panic path mutated bids"
            );

            assert_eq!(
                level_snapshot(&book.asks),
                asks_before,
                "panic path mutated asks"
            );

            assert_eq!(book.orders, orders_before, "panic path mutated ID index");

            assert_eq!(
                book.arena.test_snapshot(),
                arena_before,
                "panic path mutated Arena slots/free-list"
            );

            assert_eq!(
                book.queue_priority_allocator.next(),
                next_before,
                "panic path consumed QueuePriority"
            );

            assert_eq!(
                book.queue_priority_allocator.policy(),
                &policy_before,
                "panic path mutated QueuePriority policy"
            );

            assert_eq!(
                validate_book(&book),
                Ok(()),
                "book must remain valid after fail-closed panic"
            );
        }
    }
    fn assert_cancel_error_preserves_state(
        book: &mut ProductionOrderBook,
        order_id: OrderId,
        expected: RejectReason,
    ) {
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        assert_eq!(book.cancel(order_id), Err(expected));

        assert_eq!(
            level_snapshot(&book.bids),
            bids_before,
            "failed cancel mutated bids"
        );

        assert_eq!(
            level_snapshot(&book.asks),
            asks_before,
            "failed cancel mutated asks"
        );

        assert_eq!(book.orders, orders_before, "failed cancel mutated ID index");

        assert_eq!(
            book.arena.test_snapshot(),
            arena_before,
            "failed cancel mutated Arena slots/free-list"
        );

        assert_eq!(
            book.queue_priority_allocator.next(),
            next_before,
            "failed cancel consumed QueuePriority"
        );

        assert_eq!(
            book.queue_priority_allocator.policy(),
            &policy_before,
            "failed cancel mutated allocator policy"
        );

        assert_eq!(validate_book(book), Ok(()));
    }
    #[test]
    fn cancel_single_order_removes_level_on_both_sides() {
        for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
            let mut book = ProductionOrderBook::new(policy());

            let original = order(1, side, price, 7);

            book.rest(original.clone()).unwrap();

            let next_before = book.next_queue_priority();
            let policy_before = *book.queue_priority_allocator.policy();

            let removed = book.cancel(OrderId::new(1)).unwrap();

            assert_eq!(removed.original_order(), &original);
            assert_eq!(removed.remaining(), 7);
            assert_eq!(removed.priority(), QueuePriority::new(0));

            // 低层删除必须清空 intrusive links。
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            assert_eq!(book.order(OrderId::new(1)), None);

            match side {
                Side::Buy => {
                    assert!(book.bids.is_empty());
                    assert_eq!(book.best_price(Side::Buy), None);
                }

                Side::Sell => {
                    assert!(book.asks.is_empty());
                    assert_eq!(book.best_price(Side::Sell), None);
                }
            }

            // Cancel 不消费 allocator。
            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn cancel_same_price_head_middle_and_tail_preserves_fifo() {
        for target in [1u128, 2, 3] {
            let mut book = ProductionOrderBook::new(policy());

            for id in [1u128, 2, 3] {
                book.rest(order(id, Side::Buy, 100, id as u64)).unwrap();
            }

            let next_before = book.next_queue_priority();

            let removed = book.cancel(OrderId::new(target)).unwrap();

            assert_eq!(removed.original_order().order_id, OrderId::new(target));

            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(book.order(OrderId::new(target)), None);

            let level = book.bids.get(&Price::try_new(100).unwrap()).unwrap();

            assert_eq!(level.count(), 2);

            let head = level.head().unwrap();
            let tail = level.tail().unwrap();

            let head_node = book.arena.get(head).unwrap();
            let tail_node = book.arena.get(tail).unwrap();

            let expected = match target {
                1 => [2u128, 3],
                2 => [1u128, 3],
                3 => [1u128, 2],
                _ => unreachable!(),
            };

            assert_eq!(
                head_node.original_order().order_id,
                OrderId::new(expected[0])
            );

            assert_eq!(
                tail_node.original_order().order_id,
                OrderId::new(expected[1])
            );

            assert_eq!(head_node.next(), Some(tail));

            assert_eq!(tail_node.prev(), Some(head));

            // survivor priority 不得重排。
            assert_eq!(head_node.priority(), QueuePriority::new(expected[0] - 1));

            assert_eq!(tail_node.priority(), QueuePriority::new(expected[1] - 1));

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn cancel_best_level_exposes_next_best_price() {
        let mut book = ProductionOrderBook::new(policy());

        for candidate in [
            order(1, Side::Buy, 90, 1),
            order(2, Side::Buy, 95, 1),
            order(3, Side::Buy, 100, 1),
            order(4, Side::Sell, 105, 1),
            order(5, Side::Sell, 110, 1),
            order(6, Side::Sell, 115, 1),
        ] {
            book.rest(candidate).unwrap();
        }

        let next_before = book.next_queue_priority();

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(100).unwrap())
        );

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(105).unwrap())
        );

        book.cancel(OrderId::new(3)).unwrap();

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(95).unwrap())
        );

        // 原 best level 已经空，因此必须从树中消失。
        assert!(!book.bids.contains_key(&Price::try_new(100).unwrap()));

        assert_eq!(validate_book(&book), Ok(()));

        book.cancel(OrderId::new(4)).unwrap();

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(110).unwrap())
        );

        assert!(!book.asks.contains_key(&Price::try_new(105).unwrap()));

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn cancel_unknown_id_is_atomic() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        book.rest(order(2, Side::Sell, 105, 7)).unwrap();

        assert_cancel_error_preserves_state(
            &mut book,
            OrderId::new(999),
            RejectReason::OrderNotFound,
        );
    }
    #[test]
    fn repeated_cancel_returns_order_not_found_without_mutation() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 95, 5)).unwrap();

        let removed = book.cancel(OrderId::new(1)).unwrap();

        assert_eq!(removed.original_order().order_id, OrderId::new(1));

        assert_eq!(validate_book(&book), Ok(()));

        assert_cancel_error_preserves_state(
            &mut book,
            OrderId::new(1),
            RejectReason::OrderNotFound,
        );
    }
    #[test]
    fn canceled_slot_can_be_reused_without_reviving_old_order_id() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(10, Side::Buy, 95, 5)).unwrap();

        let old_slot = *book.orders.get(&OrderId::new(10)).unwrap();

        let removed = book.cancel(OrderId::new(10)).unwrap();

        assert_eq!(removed.priority(), QueuePriority::new(0));

        assert_eq!(book.order(OrderId::new(10)), None);

        assert_eq!(book.next_queue_priority(), 1);

        // Arena free-list 应复用刚释放的 slot。
        book.rest(order(20, Side::Buy, 95, 7)).unwrap();

        let new_slot = *book.orders.get(&OrderId::new(20)).unwrap();

        assert_eq!(new_slot, old_slot, "canceled Arena slot should be reused");

        // 相同物理 slot 已属于全新的 OrderId。
        assert_eq!(
            book.arena.get(new_slot).unwrap().original_order().order_id,
            OrderId::new(20)
        );

        assert_eq!(
            book.arena.get(new_slot).unwrap().priority(),
            QueuePriority::new(1)
        );

        // 旧 ID mapping 已彻底解除，不能因为 slot reuse 命中新订单。
        assert_eq!(book.order(OrderId::new(10)), None);

        assert_eq!(
            book.order(OrderId::new(20))
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(20)
        );

        assert_eq!(book.next_queue_priority(), 2);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn cancel_same_price_sell_head_middle_and_tail_preserves_fifo() {
        for target in [1u128, 2, 3] {
            let mut book = ProductionOrderBook::new(policy());

            for (id, qty) in [(1u128, 4u64), (2u128, 5u64), (3u128, 6u64)] {
                book.rest(order(id, Side::Sell, 100, qty)).unwrap();
            }

            assert_eq!(validate_book(&book), Ok(()));

            let next_before = book.next_queue_priority();
            assert_eq!(next_before, 3);

            let removed = book.cancel(OrderId::new(target)).unwrap();

            assert_eq!(removed.original_order().order_id, OrderId::new(target));
            assert_eq!(removed.priority(), QueuePriority::new(target - 1));
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            // Cancel 不能消耗 / 重排 QueuePriority。
            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(book.order(OrderId::new(target)), None);

            let level = book.asks.get(&Price::try_new(100).unwrap()).unwrap();

            assert_eq!(level.count(), 2);

            let expected_qty = match target {
                1 => 11, // 5 + 6
                2 => 10, // 4 + 6
                3 => 9,  // 4 + 5
                _ => unreachable!(),
            };

            assert_eq!(level.total_visible_qty(), expected_qty);

            let expected_ids = match target {
                1 => [2u128, 3],
                2 => [1u128, 3],
                3 => [1u128, 2],
                _ => unreachable!(),
            };

            let head = level.head().unwrap();
            let tail = level.tail().unwrap();

            assert_ne!(head, tail);

            let head_node = book.arena.get(head).unwrap();
            let tail_node = book.arena.get(tail).unwrap();

            // FIFO survivor 顺序。
            assert_eq!(
                head_node.original_order().order_id,
                OrderId::new(expected_ids[0])
            );
            assert_eq!(
                tail_node.original_order().order_id,
                OrderId::new(expected_ids[1])
            );

            // 两节点链必须完整互反。
            assert_eq!(head_node.prev(), None);
            assert_eq!(head_node.next(), Some(tail));

            assert_eq!(tail_node.prev(), Some(head));
            assert_eq!(tail_node.next(), None);

            // survivor priority 保持原值，不能因为 Cancel 压缩。
            assert_eq!(
                head_node.priority(),
                QueuePriority::new(expected_ids[0] - 1)
            );
            assert_eq!(
                tail_node.priority(),
                QueuePriority::new(expected_ids[1] - 1)
            );

            assert_eq!(book.next_queue_priority(), 3);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    fn assert_cancel_panics_without_mutation(book: &mut ProductionOrderBook, order_id: OrderId) {
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();

        // 完整包含 occupied slots + free-list。
        let arena_before = book.arena.test_snapshot();

        let next_before = book.queue_priority_allocator.next();

        let policy_before = *book.queue_priority_allocator.policy();

        // 这里必须直接调用 cancel。
        //
        // 不允许先调用 order() 或 validate_book()，
        // 因为当前 book 本身就是有意构造的损坏状态。
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.cancel(order_id);
        }));

        assert!(result.is_err(), "corrupted cancel path must fail closed");

        assert_eq!(
            level_snapshot(&book.bids),
            bids_before,
            "cancel panic path mutated bids"
        );

        assert_eq!(
            level_snapshot(&book.asks),
            asks_before,
            "cancel panic path mutated asks"
        );

        assert_eq!(
            book.orders, orders_before,
            "cancel panic path mutated ID index"
        );

        assert_eq!(
            book.arena.test_snapshot(),
            arena_before,
            "cancel panic path mutated Arena slots/free-list"
        );

        assert_eq!(
            book.queue_priority_allocator.next(),
            next_before,
            "cancel panic path changed next priority"
        );

        assert_eq!(
            book.queue_priority_allocator.policy(),
            &policy_before,
            "cancel panic path changed allocator policy"
        );
    }
    #[test]
    fn cancel_panics_without_mutation_when_index_points_to_freed_slot() {
        let mut book = ProductionOrderBook::new(policy());

        let order_id = OrderId::new(1);

        book.rest(order(1, Side::Buy, 100, 5)).unwrap();

        let slot = *book.orders.get(&order_id).unwrap();

        let removed = book.arena.remove(slot).unwrap();

        assert_eq!(removed.original_order().order_id, order_id);

        // orders 与 PriceLevel 故意保留，只有 Arena slot 被释放。
        assert!(book.orders.contains_key(&order_id));
        assert!(book.arena.get(slot).is_none());

        assert_cancel_panics_without_mutation(&mut book, order_id);
    }
    #[test]
    fn cancel_panics_without_mutation_when_index_points_to_other_live_order() {
        let mut book = ProductionOrderBook::new(policy());

        let first_id = OrderId::new(1);
        let second_id = OrderId::new(2);

        book.rest(order(1, Side::Buy, 100, 5)).unwrap();

        book.rest(order(2, Side::Buy, 100, 7)).unwrap();

        let second_slot = *book.orders.get(&second_id).unwrap();

        // 故意让 first_id 指向第二张 live order。
        book.orders.insert(first_id, second_slot);

        assert_cancel_panics_without_mutation(&mut book, first_id);
    }
    #[test]
    fn cancel_panics_without_mutation_when_node_side_points_to_missing_level() {
        let mut book = ProductionOrderBook::new(policy());

        let order_id = OrderId::new(1);

        book.rest(order(1, Side::Buy, 100, 5)).unwrap();

        let slot = *book.orders.get(&order_id).unwrap();

        let removed = book.arena.remove(slot).unwrap();
        let priority = removed.priority();

        // 原节点真实位于 bids[100]，
        // 但替换后的节点自述为 Sell，因此 cancel 会寻找 asks[100]。
        let replacement = OrderNode::new(order(1, Side::Sell, 100, 5), priority);

        assert_eq!(book.arena.insert(replacement).unwrap(), slot);

        // asks[100] 不存在。
        assert!(!book.asks.contains_key(&Price::try_new(100).unwrap()));

        assert_cancel_panics_without_mutation(&mut book, order_id);
    }
    #[test]
    fn cancel_panics_without_mutation_when_node_price_points_to_missing_level() {
        let mut book = ProductionOrderBook::new(policy());

        let order_id = OrderId::new(1);

        book.rest(order(1, Side::Buy, 100, 5)).unwrap();

        let slot = *book.orders.get(&order_id).unwrap();

        let removed = book.arena.remove(slot).unwrap();
        let priority = removed.priority();

        // PriceLevel 仍然是真实的 bids[100]，
        // 但节点现在自述 price=101。
        let replacement = OrderNode::new(order(1, Side::Buy, 101, 5), priority);

        assert_eq!(book.arena.insert(replacement).unwrap(), slot);

        assert!(!book.bids.contains_key(&Price::try_new(101).unwrap()));

        assert_cancel_panics_without_mutation(&mut book, order_id);
    }
    #[test]
    fn cancel_panics_without_mutation_when_exact_price_level_is_missing() {
        for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
            let mut book = ProductionOrderBook::new(policy());

            let order_id = OrderId::new(1);
            let price = Price::try_new(price).unwrap();

            book.rest(LimitGtcOrder {
                order_id,
                user_id: UserId::new(1),
                side,
                price,
                qty: Qty::try_new(5).unwrap(),
            })
            .unwrap();

            // Arena 与 ID map 都仍然存在；
            // 只删除节点应该所属的准确价格档。
            let removed_level = match side {
                Side::Buy => book.bids.remove(&price),
                Side::Sell => book.asks.remove(&price),
            };

            assert!(removed_level.is_some());

            assert_cancel_panics_without_mutation(&mut book, order_id);
        }
    }
    #[test]
    fn head_partial_fill_keeps_production_book_valid_on_both_sides() {
        for (side, raw_price) in [(Side::Buy, 95), (Side::Sell, 105)] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, side, raw_price, 10)).unwrap();
            book.rest(order(2, side, raw_price, 7)).unwrap();

            let price = Price::try_new(raw_price).unwrap();

            let orders_before = book.orders.clone();
            let next_before = book.next_queue_priority();
            let policy_before = *book.queue_priority_allocator.policy();

            match side {
                Side::Buy => {
                    book.bids
                        .get_mut(&price)
                        .unwrap()
                        .apply_head_partial_fill(&mut book.arena, Qty::try_new(3).unwrap());
                }

                Side::Sell => {
                    book.asks
                        .get_mut(&price)
                        .unwrap()
                        .apply_head_partial_fill(&mut book.arena, Qty::try_new(3).unwrap());
                }
            }

            assert_eq!(book.orders, orders_before);
            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn next_fill_trade_partially_fills_maker_on_both_sides() {
        for (taker_side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let maker_priority = book.order(OrderId::new(1)).unwrap().priority();

            let next_before = book.next_queue_priority();

            let mut incoming = IncomingOrder::new(order(2, taker_side, taker_price, 6));

            let trade_id = TradeId::new(100);

            let trade = book
                .execute_next_fill_trade(&mut incoming, trade_id)
                .unwrap()
                .unwrap();

            assert_eq!(trade.trade_id, trade_id);
            assert_eq!(trade.maker_order_id, OrderId::new(1));
            assert_eq!(trade.taker_order_id, OrderId::new(2));
            assert_eq!(trade.price, Price::try_new(maker_price).unwrap());
            assert_eq!(trade.qty, Qty::try_new(6).unwrap());
            assert_eq!(trade.maker_side, maker_side);

            let maker = book.order(OrderId::new(1)).unwrap();

            assert_eq!(maker.remaining(), 4);
            assert_eq!(maker.priority(), maker_priority);

            assert_eq!(incoming.remaining(), 0);

            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn next_fill_trade_exact_fill_removes_maker_on_both_sides() {
        for (taker_side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let next_before = book.next_queue_priority();

            let mut incoming = IncomingOrder::new(order(2, taker_side, taker_price, 10));

            let trade = book
                .execute_next_fill_trade(&mut incoming, TradeId::new(1))
                .unwrap()
                .unwrap();

            assert_eq!(trade.maker_order_id, OrderId::new(1));

            assert_eq!(trade.qty, Qty::try_new(10).unwrap());

            assert_eq!(book.order(OrderId::new(1)), None);

            assert_eq!(book.best_price(maker_side), None);

            assert_eq!(incoming.remaining(), 0);

            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn next_fill_trade_removes_full_maker_but_keeps_taker_remainder_unrested() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 4)).unwrap();

        let next_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 10));

        let trade = book
            .execute_next_fill_trade(&mut incoming, TradeId::new(1))
            .unwrap()
            .unwrap();

        assert_eq!(trade.qty, Qty::try_new(4).unwrap());

        assert_eq!(incoming.remaining(), 6);

        assert_eq!(book.order(OrderId::new(1)), None);

        // Taker 没有被偷偷挂进 production book。
        assert_eq!(book.order(OrderId::new(2)), None);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn next_fill_trade_selects_best_price_then_fifo_head() {
        let mut book = ProductionOrderBook::new(policy());

        // 故意先挂更差 ask。
        book.rest(order(90, Side::Sell, 110, 5)).unwrap();

        // best ask 同价两张，ID 顺序故意与 FIFO 无关。
        book.rest(order(30, Side::Sell, 105, 5)).unwrap();

        book.rest(order(10, Side::Sell, 105, 5)).unwrap();

        let mut incoming = IncomingOrder::new(order(200, Side::Buy, 120, 2));

        let trade = book
            .execute_next_fill_trade(&mut incoming, TradeId::new(1))
            .unwrap()
            .unwrap();

        assert_eq!(trade.maker_order_id, OrderId::new(30));

        assert_eq!(trade.price, Price::try_new(105).unwrap());

        assert_eq!(book.order(OrderId::new(30)).unwrap().remaining(), 3);

        // 同价第二张完全没动。
        assert_eq!(book.order(OrderId::new(10)).unwrap().remaining(), 5);

        // 更差价格也没动。
        assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 5);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn next_fill_trade_stops_after_one_full_maker() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Sell, 105, 4)).unwrap();

        let second_priority = book.order(OrderId::new(2)).unwrap().priority();

        let mut incoming = IncomingOrder::new(order(10, Side::Buy, 110, 10));

        let trade = book
            .execute_next_fill_trade(&mut incoming, TradeId::new(1))
            .unwrap()
            .unwrap();

        assert_eq!(trade.maker_order_id, OrderId::new(1));

        // 第一 maker 被完整吃掉。
        assert_eq!(book.order(OrderId::new(1)), None);

        // 本次不能继续吃第二 maker。
        let second = book.order(OrderId::new(2)).unwrap();

        assert_eq!(second.remaining(), 4);
        assert_eq!(second.priority(), second_priority);

        // incoming 只扣这一笔。
        assert_eq!(incoming.remaining(), 7);

        let level = book.asks.get(&Price::try_new(105).unwrap()).unwrap();

        assert_eq!(level.count(), 1);
        assert_eq!(level.total_visible_qty(), 4);

        assert_eq!(validate_book(&book), Ok(()));
    }
    fn assert_no_fill_preserves_state(
        book: &mut ProductionOrderBook,
        incoming: &mut IncomingOrder,
    ) {
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.next_queue_priority();
        let policy_before = *book.queue_priority_allocator.policy();

        let incoming_original = incoming.original_order().clone();

        let incoming_remaining = incoming.remaining();

        let result = book
            .execute_next_fill_trade(incoming, TradeId::new(999))
            .unwrap();

        assert_eq!(result, None);

        assert_eq!(level_snapshot(&book.bids), bids_before);

        assert_eq!(level_snapshot(&book.asks), asks_before);

        assert_eq!(book.orders, orders_before);

        assert_eq!(book.arena.test_snapshot(), arena_before);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(incoming.original_order(), &incoming_original);

        assert_eq!(incoming.remaining(), incoming_remaining);

        assert_eq!(validate_book(book), Ok(()));
    }
    #[test]
    fn next_fill_trade_no_cross_is_pure_noop() {
        for (side, price) in [(Side::Buy, 104), (Side::Sell, 96)] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, Side::Sell, 105, 10)).unwrap();

            book.rest(order(2, Side::Buy, 95, 10)).unwrap();

            let mut incoming = IncomingOrder::new(order(3, side, price, 5));

            assert_no_fill_preserves_state(&mut book, &mut incoming);
        }
    }
    #[test]
    fn next_fill_trade_exhausted_incoming_is_pure_noop() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 5));

        incoming.apply_fill(Qty::try_new(5).unwrap()).unwrap();

        assert_eq!(incoming.remaining(), 0);

        assert_no_fill_preserves_state(&mut book, &mut incoming);
    }
    #[test]
    fn next_fill_trade_buy_with_empty_asks_is_pure_noop() {
        let mut book = ProductionOrderBook::new(policy());

        // 只建立同侧 bids，明确 asks 为空。
        book.rest(order(1, Side::Buy, 95, 10)).unwrap();

        assert!(book.asks.is_empty());
        assert!(!book.bids.is_empty());
        assert_eq!(validate_book(&book), Ok(()));

        let mut incoming = IncomingOrder::new(order(2, Side::Buy, 110, 6));

        assert_no_fill_preserves_state(&mut book, &mut incoming);
    }
    #[test]
    fn next_fill_trade_sell_with_empty_bids_is_pure_noop() {
        let mut book = ProductionOrderBook::new(policy());

        // 只建立同侧 asks，明确 bids 为空。
        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        assert!(book.bids.is_empty());
        assert!(!book.asks.is_empty());
        assert_eq!(validate_book(&book), Ok(()));

        let mut incoming = IncomingOrder::new(order(2, Side::Sell, 90, 6));

        assert_no_fill_preserves_state(&mut book, &mut incoming);
    }
    #[test]
    fn execute_fill_trades_buy_consumes_asks_by_price_then_fifo() {
        let mut book = ProductionOrderBook::new(policy());

        // 故意先放更差价格。
        book.rest(order(90, Side::Sell, 115, 7)).unwrap();

        book.rest(order(30, Side::Sell, 105, 2)).unwrap();

        book.rest(order(10, Side::Sell, 105, 3)).unwrap();

        book.rest(order(40, Side::Sell, 110, 5)).unwrap();

        let maker_40_priority = book.order(OrderId::new(40)).unwrap().priority();

        let next_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(200, Side::Buy, 110, 8));

        let trade_ids = [TradeId::new(100), TradeId::new(101), TradeId::new(102)];

        let trades = book.execute_fill_trades(&mut incoming, &trade_ids).unwrap();

        assert_eq!(trades.len(), 3);

        assert_eq!(trades[0].trade_id, TradeId::new(100));
        assert_eq!(trades[0].maker_order_id, OrderId::new(30));
        assert_eq!(trades[0].taker_order_id, OrderId::new(200));
        assert_eq!(trades[0].price, Price::try_new(105).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
        assert_eq!(trades[0].maker_side, Side::Sell);

        assert_eq!(trades[1].trade_id, TradeId::new(101));
        assert_eq!(trades[1].maker_order_id, OrderId::new(10));
        assert_eq!(trades[1].taker_order_id, OrderId::new(200));
        assert_eq!(trades[1].price, Price::try_new(105).unwrap());
        assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
        assert_eq!(trades[1].maker_side, Side::Sell);

        assert_eq!(trades[2].trade_id, TradeId::new(102));
        assert_eq!(trades[2].maker_order_id, OrderId::new(40));
        assert_eq!(trades[2].taker_order_id, OrderId::new(200));
        assert_eq!(trades[2].price, Price::try_new(110).unwrap());
        assert_eq!(trades[2].qty, Qty::try_new(3).unwrap());
        assert_eq!(trades[2].maker_side, Side::Sell);

        assert_eq!(incoming.remaining(), 0);

        assert_eq!(book.order(OrderId::new(30)), None);
        assert_eq!(book.order(OrderId::new(10)), None);

        let maker_40 = book.order(OrderId::new(40)).unwrap();

        assert_eq!(maker_40.remaining(), 2);
        assert_eq!(maker_40.priority(), maker_40_priority);

        // 115 完全没碰。
        assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 7);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn execute_fill_trades_sell_consumes_bids_by_price_then_fifo() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(90, Side::Buy, 85, 7)).unwrap();

        book.rest(order(30, Side::Buy, 95, 2)).unwrap();

        book.rest(order(10, Side::Buy, 95, 3)).unwrap();

        book.rest(order(40, Side::Buy, 90, 5)).unwrap();

        let maker_40_priority = book.order(OrderId::new(40)).unwrap().priority();

        let next_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(200, Side::Sell, 90, 8));

        let trades = book
            .execute_fill_trades(
                &mut incoming,
                &[TradeId::new(200), TradeId::new(201), TradeId::new(202)],
            )
            .unwrap();

        assert_eq!(trades.len(), 3);

        assert_eq!(trades[0].trade_id, TradeId::new(200));
        assert_eq!(trades[0].maker_order_id, OrderId::new(30));
        assert_eq!(trades[0].price, Price::try_new(95).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
        assert_eq!(trades[0].maker_side, Side::Buy);

        assert_eq!(trades[1].trade_id, TradeId::new(201));
        assert_eq!(trades[1].maker_order_id, OrderId::new(10));
        assert_eq!(trades[1].price, Price::try_new(95).unwrap());
        assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
        assert_eq!(trades[1].maker_side, Side::Buy);

        assert_eq!(trades[2].trade_id, TradeId::new(202));
        assert_eq!(trades[2].maker_order_id, OrderId::new(40));
        assert_eq!(trades[2].price, Price::try_new(90).unwrap());
        assert_eq!(trades[2].qty, Qty::try_new(3).unwrap());
        assert_eq!(trades[2].maker_side, Side::Buy);

        assert_eq!(incoming.remaining(), 0);

        assert_eq!(book.order(OrderId::new(30)), None);
        assert_eq!(book.order(OrderId::new(10)), None);

        let maker_40 = book.order(OrderId::new(40)).unwrap();

        assert_eq!(maker_40.remaining(), 2);
        assert_eq!(maker_40.priority(), maker_40_priority);

        assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 7);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn execute_fill_trades_stops_exactly_when_taker_is_exhausted() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 2)).unwrap();

        book.rest(order(2, Side::Sell, 105, 4)).unwrap();

        book.rest(order(3, Side::Sell, 105, 6)).unwrap();

        let third_before = book.order(OrderId::new(3)).unwrap().clone();

        let mut incoming = IncomingOrder::new(order(100, Side::Buy, 105, 5));

        let trades = book
            .execute_fill_trades(
                &mut incoming,
                &[
                    TradeId::new(1),
                    TradeId::new(2),
                    // 多余 ID 必须被忽略。
                    TradeId::new(3),
                ],
            )
            .unwrap();

        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].maker_order_id, OrderId::new(1));
        assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());

        assert_eq!(trades[1].maker_order_id, OrderId::new(2));
        assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());

        assert_eq!(incoming.remaining(), 0);

        // maker 2 partial。
        assert_eq!(book.order(OrderId::new(2)).unwrap().remaining(), 1);

        // 后续仍可成交 maker 完全未触碰。
        assert_eq!(book.order(OrderId::new(3)).unwrap(), &third_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn execute_fill_trades_stops_before_non_crossing_next_price() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Sell, 110, 7)).unwrap();

        let next_level_before = book.order(OrderId::new(2)).unwrap().clone();

        let next_priority_before = book.next_queue_priority();

        let mut incoming = IncomingOrder::new(order(100, Side::Buy, 105, 10));

        let trades = book
            .execute_fill_trades(&mut incoming, &[TradeId::new(1)])
            .unwrap();

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].maker_order_id, OrderId::new(1));

        assert_eq!(incoming.remaining(), 7);

        // 110 > taker limit 105。
        assert_eq!(book.order(OrderId::new(2)).unwrap(), &next_level_before);

        // incoming 余量没有自动 rest。
        assert_eq!(book.order(OrderId::new(100)), None);

        assert_eq!(book.next_queue_priority(), next_priority_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn execute_fill_trades_empty_or_non_crossing_returns_empty_without_mutation() {
        // 空簿。
        {
            let mut book = ProductionOrderBook::new(policy());

            let mut incoming = IncomingOrder::new(order(100, Side::Buy, 110, 5));

            let arena_before = book.arena.test_snapshot();
            let next_before = book.next_queue_priority();

            let trades = book.execute_fill_trades(&mut incoming, &[]).unwrap();

            assert!(trades.is_empty());
            assert_eq!(incoming.remaining(), 5);
            assert_eq!(book.arena.test_snapshot(), arena_before);
            assert_eq!(book.next_queue_priority(), next_before);
            assert_eq!(validate_book(&book), Ok(()));
        }

        // 有对手盘，但价格不交叉。
        {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, Side::Sell, 105, 5)).unwrap();

            let bids_before = level_snapshot(&book.bids);
            let asks_before = level_snapshot(&book.asks);
            let orders_before = book.orders.clone();
            let arena_before = book.arena.test_snapshot();
            let next_before = book.next_queue_priority();
            let policy_before = *book.queue_priority_allocator.policy();

            let mut incoming = IncomingOrder::new(order(100, Side::Buy, 104, 5));

            let original_before = incoming.original_order().clone();
            let remaining_before = incoming.remaining();

            let trades = book.execute_fill_trades(&mut incoming, &[]).unwrap();

            assert!(trades.is_empty());

            assert_eq!(level_snapshot(&book.bids), bids_before);
            assert_eq!(level_snapshot(&book.asks), asks_before);
            assert_eq!(book.orders, orders_before);
            assert_eq!(book.arena.test_snapshot(), arena_before);

            assert_eq!(book.next_queue_priority(), next_before);
            assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

            assert_eq!(incoming.original_order(), &original_before);
            assert_eq!(incoming.remaining(), remaining_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn execute_fill_trades_insufficient_trade_ids_panics_before_any_mutation() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 2)).unwrap();

        book.rest(order(2, Side::Sell, 105, 3)).unwrap();

        book.rest(order(3, Side::Sell, 110, 5)).unwrap();

        // qty=8，需要：
        // id1 2
        // id2 3
        // id3 3
        // 共 3 个 TradeId。
        let mut incoming = IncomingOrder::new(order(100, Side::Buy, 110, 8));

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.next_queue_priority();

        let policy_before = *book.queue_priority_allocator.policy();

        let incoming_original_before = incoming.original_order().clone();

        let incoming_remaining_before = incoming.remaining();

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.execute_fill_trades(&mut incoming, &[TradeId::new(1), TradeId::new(2)]);
        }));

        assert!(
            result.is_err(),
            "insufficient TradeIds must fail closed before first fill"
        );

        assert_eq!(level_snapshot(&book.bids), bids_before);

        assert_eq!(level_snapshot(&book.asks), asks_before);

        assert_eq!(book.orders, orders_before);

        assert_eq!(book.arena.test_snapshot(), arena_before);

        assert_eq!(book.next_queue_priority(), next_before);

        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(incoming.original_order(), &incoming_original_before);

        assert_eq!(incoming.remaining(), incoming_remaining_before);

        assert_eq!(validate_book(&book), Ok(()));
    }

    #[test]
    fn common_rest_preflight_accepts_currently_crossing_order_without_mutation() {
        for (maker_side, maker_price, taker_side, taker_price) in [
            (Side::Sell, 105, Side::Buy, 110),
            (Side::Buy, 95, Side::Sell, 90),
        ] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, maker_price, 5)).unwrap();

            let candidate = order(2, taker_side, taker_price, 7);

            let bids_before = level_snapshot(&book.bids);
            let asks_before = level_snapshot(&book.asks);
            let orders_before = book.orders.clone();
            let arena_before = book.arena.test_snapshot();

            let next_before = book.queue_priority_allocator.next();

            let policy_before = *book.queue_priority_allocator.policy();

            let plan = book.preflight_rest_common(&candidate).unwrap();

            assert_eq!(plan.order_id, candidate.order_id);
            assert_eq!(plan.side, candidate.side);
            assert_eq!(plan.price, candidate.price);

            // common preflight 必须完全只读。
            assert_eq!(level_snapshot(&book.bids), bids_before);
            assert_eq!(level_snapshot(&book.asks), asks_before);
            assert_eq!(book.orders, orders_before);
            assert_eq!(book.arena.test_snapshot(), arena_before);
            assert_eq!(book.queue_priority_allocator.next(), next_before);
            assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn duplicate_rest_precedes_crossing_check() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 5)).unwrap();

        let candidate = order(1, Side::Buy, 110, 5);

        assert_rest_error_preserves_state(&mut book, candidate, RejectReason::DuplicateOrderId);
    }
    #[test]
    fn place_limit_gtc_buy_matches_multi_price_fifo_and_rests_remainder() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(30, Side::Sell, 105, 2)).unwrap();

        book.rest(order(10, Side::Sell, 105, 3)).unwrap();

        book.rest(order(40, Side::Sell, 110, 2)).unwrap();

        book.rest(order(90, Side::Sell, 115, 9)).unwrap();

        let next_before = book.next_queue_priority();

        let trades = book
            .place_limit_gtc(
                order(200, Side::Buy, 110, 10),
                &[TradeId::new(100), TradeId::new(101), TradeId::new(102)],
            )
            .unwrap();

        assert_eq!(trades.len(), 3);

        assert_eq!(
            (
                trades[0].trade_id,
                trades[0].maker_order_id,
                trades[0].price,
                trades[0].qty,
                trades[0].maker_side,
            ),
            (
                TradeId::new(100),
                OrderId::new(30),
                Price::try_new(105).unwrap(),
                Qty::try_new(2).unwrap(),
                Side::Sell,
            )
        );

        assert_eq!(
            (
                trades[1].trade_id,
                trades[1].maker_order_id,
                trades[1].price,
                trades[1].qty,
            ),
            (
                TradeId::new(101),
                OrderId::new(10),
                Price::try_new(105).unwrap(),
                Qty::try_new(3).unwrap(),
            )
        );

        assert_eq!(
            (
                trades[2].trade_id,
                trades[2].maker_order_id,
                trades[2].price,
                trades[2].qty,
            ),
            (
                TradeId::new(102),
                OrderId::new(40),
                Price::try_new(110).unwrap(),
                Qty::try_new(2).unwrap(),
            )
        );

        for trade in &trades {
            assert_eq!(trade.taker_order_id, OrderId::new(200));
        }

        assert_eq!(book.order(OrderId::new(30)), None);
        assert_eq!(book.order(OrderId::new(10)), None);
        assert_eq!(book.order(OrderId::new(40)), None);

        // 115 不可成交，完全没动。
        assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 9);

        // GTC remainder = 3。
        let resting = book.order(OrderId::new(200)).unwrap();

        assert_eq!(resting.remaining(), 3);
        assert_eq!(resting.original_order().side, Side::Buy);
        assert_eq!(resting.original_order().price, Price::try_new(110).unwrap());
        assert_eq!(resting.original_order().qty, Qty::try_new(3).unwrap());

        // 只有 remainder rest 消耗一个 priority。
        assert_eq!(book.next_queue_priority(), next_before + 1);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn place_limit_gtc_exact_fill_does_not_allocate_queue_priority() {
        for (maker_side, maker_price, taker_side, taker_price) in [
            (Side::Sell, 105, Side::Buy, 110),
            (Side::Buy, 95, Side::Sell, 90),
        ] {
            let mut book = ProductionOrderBook::new(policy());

            book.rest(order(1, maker_side, maker_price, 5)).unwrap();

            let next_before = book.next_queue_priority();

            let trades = book
                .place_limit_gtc(order(2, taker_side, taker_price, 5), &[TradeId::new(100)])
                .unwrap();

            assert_eq!(trades.len(), 1);

            assert_eq!(book.order(OrderId::new(1)), None);

            // Taker exact-filled，不应该进入 orders。
            assert_eq!(book.order(OrderId::new(2)), None);

            assert_eq!(book.next_queue_priority(), next_before);

            assert_eq!(validate_book(&book), Ok(()));
        }
    }
    #[test]
    fn place_limit_gtc_exact_fill_succeeds_when_priority_allocator_is_exhausted() {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        // 唯一一次合法 allocation：
        // maker priority=0，之后 allocator.next=1 且 exhausted。
        book.rest(order(1, Side::Sell, 105, 5)).unwrap();

        assert_eq!(book.next_queue_priority(), 1);
        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(validate_book(&book), Ok(()));

        let trades = book
            .place_limit_gtc(order(2, Side::Buy, 110, 5), &[TradeId::new(100)])
            .unwrap();

        assert_eq!(trades.len(), 1);

        assert_eq!(trades[0].trade_id, TradeId::new(100));
        assert_eq!(trades[0].maker_order_id, OrderId::new(1));
        assert_eq!(trades[0].taker_order_id, OrderId::new(2));
        assert_eq!(trades[0].price, Price::try_new(105).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(5).unwrap());
        assert_eq!(trades[0].maker_side, Side::Sell);

        // maker exact full 后移除。
        assert_eq!(book.order(OrderId::new(1)), None);

        // taker 也 exact fill，不允许 rest。
        assert_eq!(book.order(OrderId::new(2)), None);

        assert!(book.asks.is_empty());
        assert!(book.bids.is_empty());

        // Exact fill 必须完全绕过 rest priority preflight，
        // allocator 已 exhausted 也不能影响成交。
        assert_eq!(book.next_queue_priority(), 1);
        assert!(book.queue_priority_allocator.is_exhausted());

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn place_limit_gtc_remainder_priority_exhaustion_is_atomic_before_first_fill() {
        let mut book = ProductionOrderBook::new(policy_with_ceiling(1));

        // 与 exact-fill 测试完全相同的初始状态。
        //
        // maker priority=0；
        // rest 完成后 next=1，并命中 synthetic ceiling。
        book.rest(order(1, Side::Sell, 105, 5)).unwrap();

        assert_eq!(book.next_queue_priority(), 1);
        assert!(book.queue_priority_allocator.is_exhausted());
        assert_eq!(validate_book(&book), Ok(()));

        // Buy qty=8：
        //
        // 计划成交 maker qty=5；
        // residual qty=3；
        // 实际只需要一笔成交，所以一个 TradeId 是足量的。
        //
        // 必须在执行这第一笔成交之前，由 residual rest preflight
        // 返回 PrioritySpaceExhaustion。
        assert_place_error_preserves_state(
            &mut book,
            order(2, Side::Buy, 110, 8),
            &[TradeId::new(100)],
            RejectReason::PrioritySpaceExhaustion,
        );

        // helper 已比较完整状态，这里再直接锁定最关键的业务证据：
        // maker 根本没被成交。
        let maker = book.order(OrderId::new(1)).unwrap();

        assert_eq!(maker.remaining(), 5);
        assert_eq!(maker.original_order().side, Side::Sell);
        assert_eq!(maker.original_order().price, Price::try_new(105).unwrap());

        // rejected taker 未进入 ID index / book。
        assert_eq!(book.order(OrderId::new(2)), None);

        assert_eq!(book.next_queue_priority(), 1);
        assert!(book.queue_priority_allocator.is_exhausted());

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn place_limit_gtc_non_crossing_order_rests_directly() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 7)).unwrap();

        let next_before = book.next_queue_priority();

        let trades = book
            .place_limit_gtc(order(2, Side::Buy, 100, 5), &[])
            .unwrap();

        assert!(trades.is_empty());

        let resting = book.order(OrderId::new(2)).unwrap();

        assert_eq!(resting.remaining(), 5);

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(100).unwrap())
        );

        assert_eq!(book.next_queue_priority(), next_before + 1);

        assert_eq!(validate_book(&book), Ok(()));
    }
    fn assert_place_error_preserves_state(
        book: &mut ProductionOrderBook,
        candidate: LimitGtcOrder,
        trade_ids: &[TradeId],
        expected: RejectReason,
    ) {
        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.next_queue_priority();

        let policy_before = *book.queue_priority_allocator.policy();

        assert_eq!(book.place_limit_gtc(candidate, trade_ids,), Err(expected));

        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);
        assert_eq!(book.orders, orders_before);
        assert_eq!(book.arena.test_snapshot(), arena_before);
        assert_eq!(book.next_queue_priority(), next_before);
        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(validate_book(book), Ok(()));
    }
    #[test]
    fn place_limit_gtc_duplicate_is_rejected_before_matching() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 5)).unwrap();

        // 同 ID，同时也是可以成交的 Buy。
        assert_place_error_preserves_state(
            &mut book,
            order(1, Side::Buy, 110, 10),
            &[TradeId::new(1)],
            RejectReason::DuplicateOrderId,
        );
    }
    #[test]
    fn place_limit_gtc_insufficient_trade_ids_panics_before_any_fill() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 2)).unwrap();

        book.rest(order(2, Side::Sell, 105, 3)).unwrap();

        book.rest(order(3, Side::Sell, 110, 4)).unwrap();

        let bids_before = level_snapshot(&book.bids);
        let asks_before = level_snapshot(&book.asks);
        let orders_before = book.orders.clone();
        let arena_before = book.arena.test_snapshot();

        let next_before = book.next_queue_priority();

        let policy_before = *book.queue_priority_allocator.policy();

        // qty=8 需要 3 笔，只给 2 个 TradeId。
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = book.place_limit_gtc(
                order(100, Side::Buy, 110, 8),
                &[TradeId::new(1), TradeId::new(2)],
            );
        }));

        assert!(result.is_err());

        assert_eq!(level_snapshot(&book.bids), bids_before);
        assert_eq!(level_snapshot(&book.asks), asks_before);
        assert_eq!(book.orders, orders_before);
        assert_eq!(book.arena.test_snapshot(), arena_before);
        assert_eq!(book.next_queue_priority(), next_before);
        assert_eq!(book.queue_priority_allocator.policy(), &policy_before);

        assert_eq!(validate_book(&book), Ok(()));
    }
    #[test]
    fn place_limit_gtc_same_side_aggregate_overflow_fails_before_any_write() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Buy, 100, u64::MAX)).unwrap();

        assert_place_error_preserves_state(
            &mut book,
            order(2, Side::Buy, 100, 1),
            &[],
            RejectReason::ArithmeticOverflow,
        );
    }
    #[test]
    fn place_limit_gtc_sell_matches_multi_price_fifo_and_rests_remainder() {
        let mut book = ProductionOrderBook::new(policy());

        // 更差价格故意先挂，证明匹配不是按插入全局顺序。
        book.rest(order(90, Side::Buy, 85, 9)).unwrap();

        // best bid，同价 FIFO。
        book.rest(order(30, Side::Buy, 95, 2)).unwrap();

        book.rest(order(10, Side::Buy, 95, 3)).unwrap();

        // 第二价格档。
        book.rest(order(40, Side::Buy, 90, 2)).unwrap();

        let next_before = book.next_queue_priority();

        let trades = book
            .place_limit_gtc(
                order(200, Side::Sell, 90, 10),
                &[TradeId::new(200), TradeId::new(201), TradeId::new(202)],
            )
            .unwrap();

        assert_eq!(trades.len(), 3);

        // 95 的 FIFO head。
        assert_eq!(trades[0].trade_id, TradeId::new(200));
        assert_eq!(trades[0].maker_order_id, OrderId::new(30));
        assert_eq!(trades[0].taker_order_id, OrderId::new(200));
        assert_eq!(trades[0].price, Price::try_new(95).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
        assert_eq!(trades[0].maker_side, Side::Buy);

        // 95 的第二个 FIFO maker。
        assert_eq!(trades[1].trade_id, TradeId::new(201));
        assert_eq!(trades[1].maker_order_id, OrderId::new(10));
        assert_eq!(trades[1].taker_order_id, OrderId::new(200));
        assert_eq!(trades[1].price, Price::try_new(95).unwrap());
        assert_eq!(trades[1].qty, Qty::try_new(3).unwrap());
        assert_eq!(trades[1].maker_side, Side::Buy);

        // 然后才是 90。
        assert_eq!(trades[2].trade_id, TradeId::new(202));
        assert_eq!(trades[2].maker_order_id, OrderId::new(40));
        assert_eq!(trades[2].taker_order_id, OrderId::new(200));
        assert_eq!(trades[2].price, Price::try_new(90).unwrap());
        assert_eq!(trades[2].qty, Qty::try_new(2).unwrap());
        assert_eq!(trades[2].maker_side, Side::Buy);

        // 总成交 7，GTC remainder=3。
        let resting = book.order(OrderId::new(200)).unwrap();

        assert_eq!(resting.remaining(), 3);
        assert_eq!(resting.original_order().side, Side::Sell);
        assert_eq!(resting.original_order().price, Price::try_new(90).unwrap());
        assert_eq!(resting.original_order().qty, Qty::try_new(3).unwrap());

        // residual rest 获得撮合前 allocator 的下一 priority。
        assert_eq!(resting.priority(), QueuePriority::new(next_before));

        assert_eq!(book.next_queue_priority(), next_before + 1);

        // 被完整成交的 maker 已消失。
        assert_eq!(book.order(OrderId::new(30)), None);
        assert_eq!(book.order(OrderId::new(10)), None);
        assert_eq!(book.order(OrderId::new(40)), None);

        // 85 不可成交，完全保留。
        assert_eq!(book.order(OrderId::new(90)).unwrap().remaining(), 9);

        // 成交完成后：
        // best bid = 85
        // residual sell 成为 best ask = 90
        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(85).unwrap())
        );

        assert_eq!(
            book.best_price(Side::Sell),
            Some(Price::try_new(90).unwrap())
        );

        let level = book.asks.get(&Price::try_new(90).unwrap()).unwrap();

        assert_eq!(level.count(), 1);

        let taker_slot = *book.orders.get(&OrderId::new(200)).unwrap();

        assert_eq!(level.head(), Some(taker_slot));
        assert_eq!(level.tail(), Some(taker_slot));
        assert_eq!(level.total_visible_qty(), 3);

        assert_eq!(validate_book(&book), Ok(()));
    }

    #[test]
    fn place_limit_gtc_remainder_reuses_fully_filled_maker_slot() {
        let mut book = ProductionOrderBook::new(policy());

        book.rest(order(1, Side::Sell, 105, 2)).unwrap();

        let maker_id = OrderId::new(1);

        let taker_id = OrderId::new(100);

        let maker_slot = *book.orders.get(&maker_id).unwrap();

        let maker_priority = book.order(maker_id).unwrap().priority();

        let next_before = book.next_queue_priority();

        // Buy qty=5:
        // - full maker 2
        // - residual 3 rest 到 bid@110
        let trades = book
            .place_limit_gtc(order(100, Side::Buy, 110, 5), &[TradeId::new(10)])
            .unwrap();

        assert_eq!(trades.len(), 1);

        assert_eq!(trades[0].maker_order_id, maker_id);
        assert_eq!(trades[0].taker_order_id, taker_id);
        assert_eq!(trades[0].price, Price::try_new(105).unwrap());
        assert_eq!(trades[0].qty, Qty::try_new(2).unwrap());
        assert_eq!(trades[0].maker_side, Side::Sell);

        // 旧 maker 已从 ID index 消失。
        assert_eq!(book.order(maker_id), None);

        assert!(!book.orders.contains_key(&maker_id));

        // Arena free-list 是 LIFO。
        // maker pop_front 释放的 slot 应立即被 residual rest 复用。
        assert_eq!(book.orders.get(&taker_id), Some(&maker_slot));

        let taker = book.order(taker_id).unwrap();

        assert_eq!(taker.remaining(), 3);

        assert_eq!(taker.original_order().order_id, taker_id);

        assert_eq!(taker.original_order().price, Price::try_new(110).unwrap());

        // 复用 slot ≠ 复用 priority。
        assert_ne!(taker.priority(), maker_priority);

        assert_eq!(taker.priority(), QueuePriority::new(next_before));

        assert_eq!(book.next_queue_priority(), next_before + 1);

        // slot 内现在必须是新 taker，而非 stale maker。
        assert_eq!(
            book.arena
                .get(maker_slot)
                .unwrap()
                .original_order()
                .order_id,
            taker_id
        );

        assert_eq!(book.best_price(Side::Sell), None);

        assert_eq!(
            book.best_price(Side::Buy),
            Some(Price::try_new(110).unwrap())
        );

        assert_eq!(validate_book(&book), Ok(()));
    }
}
