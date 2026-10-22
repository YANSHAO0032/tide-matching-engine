use crate::{OrderArena, OrderIndex, OrderNode};
use matching_domain::{Qty, RejectReason};

/// 单个价格档位的运行态。
///
/// 使用双向链表维护同价订单的先进先出顺序。
/// 支持尾插、头部部分成交、头删、中尾删除和只读访问。
/// 修改前执行涉及节点的只读预检；内部状态损坏时直接触发不变量断言。
#[derive(Debug, Default)]
pub struct PriceLevel {
    /// 先进先出头节点的 Arena 槽位；合法空档必须为 `None`。
    head: Option<OrderIndex>,
    /// 先进先出尾节点的 Arena 槽位；合法空档必须为 `None`。
    tail: Option<OrderIndex>,
    /// 本档位链表中可达有效节点数量，必须与端点和链接一致。
    count: u32,
    /// 对所有可达节点的 `remaining` 进行溢出检查后得到的聚合量，合法空档必须为零。
    total_visible_qty: u64,
}

impl PriceLevel {
    /// 创建端点为空、数量与聚合均为零的空价格档位。
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回先进先出头节点槽位；不遍历或验证 Arena 中的链接。
    pub fn head(&self) -> Option<OrderIndex> {
        self.head
    }

    /// 返回先进先出尾节点槽位；不遍历或验证 Arena 中的链接。
    pub fn tail(&self) -> Option<OrderIndex> {
        self.tail
    }

    /// 返回本档位声明的有效节点数量。
    pub fn count(&self) -> u32 {
        self.count
    }

    /// 返回本档位声明的可见剩余数量聚合。
    pub fn total_visible_qty(&self) -> u64 {
        self.total_visible_qty
    }

    /// 报告计数是否为零。
    ///
    /// 合法空档还要求头节点/尾节点为 `None` 且可见数量为零；这些更强条件由
    /// 状态转换预检和测试校验器维护。
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// 将未连接的订单节点追加到当前价格档位尾部。
    ///
    /// 保持同价先进先出，优先级必须严格递增。
    /// 可复用 Arena 的空槽，但不能改变队列顺序。
    ///
    /// 校验失败不修改价档、Arena 或空闲链表。
    /// 内部链接不变量违反时直接触发不变量断言。
    ///
    /// 此低层原语不建立 OrderId 查找，也不分配 QueuePriority；调用方必须
    /// 提供同方向/价格、严格高于旧尾优先级的未链接节点。
    pub fn push_back(
        &mut self,
        arena: &mut OrderArena,
        mut node: OrderNode,
    ) -> Result<OrderIndex, RejectReason> {
        // 第一阶段：只读预检。
        assert!(
            node.prev().is_none() && node.next().is_none(),
            "PriceLevel invariant violation: incoming node is linked"
        );
        let previous_tail = if self.count == 0 {
            assert!(
                self.head.is_none() && self.tail.is_none() && self.total_visible_qty == 0,
                "PriceLevel invariant violation: invalid empty level"
            );

            None
        } else {
            assert!(
                self.head.is_some(),
                "PriceLevel invariant violation: missing head"
            );
            assert!(
                self.total_visible_qty > 0,
                "PriceLevel invariant violation: non-empty level has zero total visible qty"
            );
            let tail_index = self
                .tail
                .expect("PriceLevel invariant violation: missing tail");
            let tail = arena
                .get(tail_index)
                .expect("PriceLevel invariant violation: invalid tail");
            assert!(
                tail.next().is_none(),
                "PriceLevel invariant violation: tail has next"
            );
            assert_eq!(
                tail.original_order().side,
                node.original_order().side,
                "PriceLevel invariant violation: side mismatch"
            );
            assert_eq!(
                tail.original_order().price,
                node.original_order().price,
                "PriceLevel invariant violation: price mismatch"
            );
            assert!(
                node.priority() > tail.priority(),
                "PriceLevel invariant violation: non-increasing priority"
            );

            Some(tail_index)
        };
        // 第二阶段：先计算，不修改状态。
        let new_count = self
            .count
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        let new_total = self
            .total_visible_qty
            .checked_add(node.remaining())
            .ok_or(RejectReason::ArithmeticOverflow)?;

        // 第三阶段：分配槽位。
        // 只有成功分配后才允许修改已有节点和价档。
        node.set_prev(previous_tail);
        let new_index = arena.insert(node)?;

        // 第四阶段：连接双向链表。
        if let Some(tail_index) = previous_tail {
            arena
                .get_mut(tail_index)
                .expect("prechecked tail must remain valid")
                .set_next(Some(new_index));
        } else {
            self.head = Some(new_index);
        }

        // 第五阶段：提交已预检的统计值。
        self.tail = Some(new_index);
        self.count = new_count;
        self.total_visible_qty = new_total;

        Ok(new_index)
    }

    /// 对当前先进先出头节点应用严格部分成交。
    ///
    /// 只允许 `0 < fill < head.remaining()`。
    /// 完整成交必须由后续路径使用 `pop_front`，
    /// 不允许把剩余量=0 的节点留在价格档中。
    ///
    /// 成功时仅修改：
    /// - 头节点可见余量 `head.remaining`
    /// - 价档总可见量 `total_visible_qty`
    ///
    /// 不修改节点数、头节点/尾节点、前驱/`next`、优先级或 Arena 生命周期。
    pub(crate) fn apply_head_partial_fill(&mut self, arena: &mut OrderArena, fill: Qty) {
        // 1. PriceLevel 基本状态只读预检。
        assert!(
            self.count > 0,
            "PriceLevel invariant violation: partial fill on empty level"
        );
        let head_index = self
            .head
            .expect("PriceLevel invariant violation: missing head");

        let tail_index = self
            .tail
            .expect("PriceLevel invariant violation: missing tail");

        assert!(
            self.total_visible_qty > 0,
            "PriceLevel invariant violation: zero aggregate"
        );
        // 2. 只读取得头节点状态。
        let (head_remaining, successor_index, head_side, head_price, head_priority) = {
            let head = arena
                .get(head_index)
                .expect("PriceLevel invariant violation: missing head node");

            assert!(
                head.prev().is_none(),
                "PriceLevel invariant violation: head has prev"
            );

            assert!(
                head.remaining() > 0,
                "PriceLevel invariant violation: exhausted head"
            );

            (
                head.remaining(),
                head.next(),
                head.original_order().side,
                head.original_order().price,
                head.priority(),
            )
        };

        let fill_lots = fill.get();

        // Qty 已保证成交 > 0。
        // 相等关系属于完整成交，不属于本原语。
        assert!(
            fill_lots < head_remaining,
            "PriceLevel invariant violation: \
         partial fill must be strictly smaller than head remaining"
        );

        // 3. 所有新数量必须在任何写入前预计算。
        let new_remaining = head_remaining
            .checked_sub(fill_lots)
            .expect("PriceLevel invariant violation: head quantity underflow");

        let new_total = self
            .total_visible_qty
            .checked_sub(fill_lots)
            .expect("PriceLevel invariant violation: aggregate quantity underflow");

        assert!(
            new_remaining > 0,
            "PriceLevel invariant violation: partial fill exhausted head"
        );

        // 4. 检查当前头节点所在档位的局部结构。
        if self.count == 1 {
            assert_eq!(
                head_index, tail_index,
                "PriceLevel invariant violation: invalid single-node tail"
            );

            assert!(
                successor_index.is_none(),
                "PriceLevel invariant violation: single node has successor"
            );

            // 单节点可以精确证明聚合量正确。
            assert_eq!(
                self.total_visible_qty, head_remaining,
                "PriceLevel invariant violation: single-node aggregate mismatch"
            );

            assert_eq!(
                new_total, new_remaining,
                "PriceLevel invariant violation: partial aggregate mismatch"
            );
        } else {
            assert_ne!(
                head_index, tail_index,
                "PriceLevel invariant violation: invalid multi-node endpoints"
            );

            let successor_index =
                successor_index.expect("PriceLevel invariant violation: missing successor");

            assert_ne!(
                successor_index, head_index,
                "PriceLevel invariant violation: head self-cycle"
            );

            let successor = arena
                .get(successor_index)
                .expect("PriceLevel invariant violation: missing successor node");

            assert_eq!(
                successor.prev(),
                Some(head_index),
                "PriceLevel invariant violation: broken successor prev"
            );

            assert_eq!(
                successor.original_order().side,
                head_side,
                "PriceLevel invariant violation: successor side mismatch"
            );

            assert_eq!(
                successor.original_order().price,
                head_price,
                "PriceLevel invariant violation: successor price mismatch"
            );

            assert!(
                successor.remaining() > 0,
                "PriceLevel invariant violation: exhausted successor"
            );

            assert!(
                successor.priority() > head_priority,
                "PriceLevel invariant violation: \
             successor priority not strictly increasing"
            );

            if self.count == 2 {
                assert_eq!(
                    successor_index, tail_index,
                    "PriceLevel invariant violation: invalid two-node tail"
                );
            } else {
                assert_ne!(
                    successor_index, tail_index,
                    "PriceLevel invariant violation: premature tail"
                );
            }

            let tail = arena
                .get(tail_index)
                .expect("PriceLevel invariant violation: missing tail node");

            assert!(
                tail.next().is_none(),
                "PriceLevel invariant violation: tail has next"
            );

            // 多节点至少还有其它正剩余量。
            assert!(
                new_total > new_remaining,
                "PriceLevel invariant violation: invalid multi-node aggregate"
            );
        }

        // ---------- 到这里之前绝不能发生任何写入 ----------

        // 5. 提交节点剩余量。
        arena
            .get_mut(head_index)
            .expect("prechecked head must exist")
            .commit_partial_remaining(new_remaining);

        // 6. 提交档位聚合量。
        self.total_visible_qty = new_total;
    }

    /// 移除并返回当前档位的先进先出头节点。
    ///
    /// 合法空档返回 None。
    /// 内部状态损坏时在修改前触发不变量断言。
    ///
    /// 先解除本档链接，最后释放 Arena 槽位。
    /// 返回节点保留原始订单、剩余量和优先级，
    /// 但不再携带任何链表链接。
    ///
    /// 该操作只维护档位和 Arena 生命周期；不执行取消业务语义，也不维护
    /// ProductionOrderBook 的订单索引；所有者须在同一独占状态变更内清理，
    /// 且清理前不能再次查询旧映射或复用已释放槽位。
    pub fn pop_front(&mut self, arena: &mut OrderArena) -> Option<OrderNode> {
        // 1. 空档必须满足全部空状态不变量。
        if self.count == 0 {
            assert!(
                self.head.is_none() && self.tail.is_none() && self.total_visible_qty == 0,
                "PriceLevel invariant violation: invalid empty level"
            );

            return None;
        }
        // 2. 非空档只读预检。
        let head_index = self
            .head
            .expect("PriceLevel invariant violation: missing head");
        let tail_index = self
            .tail
            .expect("PriceLevel invariant violation: missing tail");

        assert!(
            self.total_visible_qty > 0,
            "PriceLevel invariant violation: zero aggregate"
        );

        let head = arena
            .get(head_index)
            .expect("PriceLevel invariant violation: missing head node");

        assert!(
            head.prev().is_none(),
            "PriceLevel invariant violation: head has prev"
        );
        assert!(
            head.remaining() > 0,
            "PriceLevel invariant violation: exhausted head"
        );

        let head_qty = head.remaining();
        let successor_index = head.next();

        // 3. 所有统计值先计算，不能先写入。
        let new_count = self
            .count
            .checked_sub(1)
            .expect("PriceLevel invariant violation: count underflow");
        let new_qty = self
            .total_visible_qty
            .checked_sub(head_qty)
            .expect("PriceLevel invariant violation: quantity underflow");

        let new_head = if new_count == 0 {
            // 单节点移除。
            assert_eq!(
                head_index, tail_index,
                "PriceLevel invariant violation: invalid single-node tail"
            );
            assert!(
                successor_index.is_none(),
                "PriceLevel invariant violation: single node has next"
            );
            assert_eq!(
                new_qty, 0,
                "PriceLevel invariant violation: invalid single-node aggregate"
            );

            None
        } else {
            // 多节点移除。
            assert_ne!(
                head_index, tail_index,
                "PriceLevel invariant violation: invalid multi-node endpoints"
            );
            assert!(
                new_qty > 0,
                "PriceLevel invariant violation: invalid remaining aggregate"
            );

            let next = successor_index.expect("PriceLevel invariant violation: missing successor");

            assert_ne!(
                next, head_index,
                "PriceLevel invariant violation: head self-cycle"
            );

            let successor = arena
                .get(next)
                .expect("PriceLevel invariant violation: missing successor node");

            assert_eq!(
                successor.prev(),
                Some(head_index),
                "PriceLevel invariant violation: broken successor prev"
            );
            assert_eq!(
                successor.original_order().side,
                head.original_order().side,
                "PriceLevel invariant violation: successor side mismatch"
            );

            assert_eq!(
                successor.original_order().price,
                head.original_order().price,
                "PriceLevel invariant violation: successor price mismatch"
            );

            assert!(
                successor.priority() > head.priority(),
                "PriceLevel invariant violation: successor priority not strictly increasing"
            );
            assert!(
                successor.remaining() > 0,
                "PriceLevel invariant violation: exhausted successor"
            );

            if new_count == 1 {
                assert_eq!(
                    next, tail_index,
                    "PriceLevel invariant violation: invalid two-node tail"
                );
            } else {
                assert_ne!(
                    next, tail_index,
                    "PriceLevel invariant violation: premature tail"
                );
            }

            // 验证旧尾及其反向链接。
            let tail = arena
                .get(tail_index)
                .expect("PriceLevel invariant violation: missing tail node");

            assert!(
                tail.next().is_none(),
                "PriceLevel invariant violation: tail has next"
            );

            let tail_prev = tail
                .prev()
                .expect("PriceLevel invariant violation: tail has no prev");

            let predecessor = arena
                .get(tail_prev)
                .expect("PriceLevel invariant violation: missing tail predecessor");

            assert_eq!(
                predecessor.next(),
                Some(tail_index),
                "PriceLevel invariant violation: broken tail prev"
            );

            Some(next)
        };

        // 4. 预检全部成功后，解除剩余链表对旧头的引用。
        if let Some(next) = new_head {
            arena
                .get_mut(next)
                .expect("prechecked successor must exist")
                .set_prev(None);
        }

        self.head = new_head;

        if new_head.is_none() {
            self.tail = None;
        }

        self.count = new_count;
        self.total_visible_qty = new_qty;

        // 5. 最后释放旧头槽位。
        let mut removed = arena
            .remove(head_index)
            .expect("prechecked head must exist");

        removed.set_prev(None);
        removed.set_next(None);

        Some(removed)
    }

    /// 移除调用者已知、属于本价格档位的非头节点。
    ///
    /// 仅支持中间节点和尾节点；头节点应使用 `pop_front`。
    ///
    /// 不遍历链表，只对目标节点及其直接相邻节点执行 O(1) 预检。
    /// 内部状态损坏时必须在任何修改发生前触发不变量断言。
    ///
    /// 成功后返回已解除链接的节点，并最后释放 Arena 槽位。
    ///
    /// 调用者必须提供当前有效、属于本档且不是头节点的索引。该低层接口
    /// 不扫描整条链以建立成员归属；头删除统一由 `pop_front` 处理。
    pub fn unlink(&mut self, arena: &mut OrderArena, index: OrderIndex) -> OrderNode {
        // 1. 价档基本只读预检。
        assert!(
            self.count >= 2,
            "PriceLevel invariant violation: unlink requires at least two nodes"
        );
        let head_index = self
            .head
            .expect("PriceLevel invariant violation: missing head");

        let tail_index = self
            .tail
            .expect("PriceLevel invariant violation: missing tail");

        assert!(
            self.total_visible_qty > 0,
            "PriceLevel invariant violation: zero aggregate"
        );

        // 头节点统一交给 pop_front，避免两套头删语义。
        assert_ne!(
            index, head_index,
            "PriceLevel invariant violation: unlink target is head"
        );

        // 2. 读取目标节点，之后所有计算都在写入前完成。
        let (
            predecessor_index,
            successor_index,
            target_qty,
            target_side,
            target_price,
            target_priority,
        ) = {
            let target = arena
                .get(index)
                .expect("PriceLevel invariant violation: missing unlink target");

            assert!(
                target.remaining() > 0,
                "PriceLevel invariant violation: exhausted unlink target"
            );

            let predecessor = target
                .prev()
                .expect("PriceLevel invariant violation: unlink target has no prev");

            assert_ne!(
                predecessor, index,
                "PriceLevel invariant violation: target prev self-cycle"
            );

            (
                predecessor,
                target.next(),
                target.remaining(),
                target.original_order().side,
                target.original_order().price,
                target.priority(),
            )
        };

        // 3. 前驱节点 <-> 目标局部不变量。
        {
            let predecessor = arena
                .get(predecessor_index)
                .expect("PriceLevel invariant violation: missing predecessor");

            assert_eq!(
                predecessor.next(),
                Some(index),
                "PriceLevel invariant violation: broken predecessor next"
            );

            assert_eq!(
                predecessor.original_order().side,
                target_side,
                "PriceLevel invariant violation: predecessor side mismatch"
            );

            assert_eq!(
                predecessor.original_order().price,
                target_price,
                "PriceLevel invariant violation: predecessor price mismatch"
            );

            assert!(
                predecessor.priority() < target_priority,
                "PriceLevel invariant violation: predecessor priority not strictly smaller"
            );
        }

        // 4. 目标 <-> 后继节点或尾节点不变量。
        if let Some(successor_index) = successor_index {
            assert_ne!(
                successor_index, index,
                "PriceLevel invariant violation: target next self-cycle"
            );

            assert_ne!(
                index, tail_index,
                "PriceLevel invariant violation: tail has successor"
            );

            let successor = arena
                .get(successor_index)
                .expect("PriceLevel invariant violation: missing successor");

            assert_eq!(
                successor.prev(),
                Some(index),
                "PriceLevel invariant violation: broken successor prev"
            );

            assert!(
                successor.remaining() > 0,
                "PriceLevel invariant violation: exhausted successor"
            );

            assert_eq!(
                successor.original_order().side,
                target_side,
                "PriceLevel invariant violation: successor side mismatch"
            );

            assert_eq!(
                successor.original_order().price,
                target_price,
                "PriceLevel invariant violation: successor price mismatch"
            );

            assert!(
                successor.priority() > target_priority,
                "PriceLevel invariant violation: successor priority not strictly greater"
            );

            // 中删意味着删除后至少仍有两个节点。
            assert!(
                self.count >= 3,
                "PriceLevel invariant violation: middle unlink with invalid count"
            );
        } else {
            assert_eq!(
                index, tail_index,
                "PriceLevel invariant violation: non-tail target has no successor"
            );
        }

        // 5. 统计值带溢出检查的预计算。
        let new_count = self
            .count
            .checked_sub(1)
            .expect("PriceLevel invariant violation: count underflow");

        let new_qty = self
            .total_visible_qty
            .checked_sub(target_qty)
            .expect("PriceLevel invariant violation: quantity underflow");

        // 解除链接不处理头节点，因此至少仍存在前驱节点。
        assert!(
            new_count > 0,
            "PriceLevel invariant violation: unlink removed last node"
        );

        assert!(
            new_qty > 0,
            "PriceLevel invariant violation: invalid remaining aggregate"
        );

        if new_count == 1 {
            assert_eq!(
                predecessor_index, head_index,
                "PriceLevel invariant violation: invalid two-node predecessor"
            );

            assert!(
                successor_index.is_none(),
                "PriceLevel invariant violation: two-node unlink is not tail"
            );
        }

        // ---------- 到这里之前绝不能发生任何写入 ----------

        // 6. 重接前驱节点 -> 后继节点。
        arena
            .get_mut(predecessor_index)
            .expect("prechecked predecessor must exist")
            .set_next(successor_index);

        // 7. 后继节点 -> 前驱节点；如果没有后继节点，则前驱节点成为新尾节点。
        if let Some(successor_index) = successor_index {
            arena
                .get_mut(successor_index)
                .expect("prechecked successor must exist")
                .set_prev(Some(predecessor_index));
        } else {
            self.tail = Some(predecessor_index);
        }

        // 8. 提交已预检的统计值。
        self.count = new_count;
        self.total_visible_qty = new_qty;

        // 9. 最后释放目标槽位。
        let mut removed = arena
            .remove(index)
            .expect("prechecked unlink target must exist");

        removed.set_prev(None);
        removed.set_next(None);

        removed
    }
}

#[cfg(test)]
mod tests {
    //! 单档先进先出、局部故障时停止处理预检与独立结构模型的验证。
    //!
    //! 验证器和性质测试模型只在测试中存在，不是生产订单簿的不变量入口。

    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn empty_price_level_initializes_all_fields() {
        let arena = OrderArena::new();
        for level in [PriceLevel::new(), PriceLevel::default()] {
            assert_eq!(level.head(), None);
            assert_eq!(level.tail(), None);
            assert_eq!(level.count(), 0);
            assert_eq!(level.total_visible_qty(), 0);
            assert!(level.is_empty());
            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};

    /// 将受控测试位置转换为已知可表示的 Arena 槽位。
    fn index(position: usize) -> OrderIndex {
        OrderIndex::try_from_usize(position).unwrap()
    }

    /// 构造具有显式方向、价格和正数量的最小测试订单。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 复制 PriceLevel 的全部元数据，用于失败原子性比较。
    fn level_state(level: &PriceLevel) -> (Option<OrderIndex>, Option<OrderIndex>, u32, u64) {
        (
            level.head(),
            level.tail(),
            level.count(),
            level.total_visible_qty(),
        )
    }

    /// 复制节点的完整可观察状态，包括两条侵入式链接。
    fn node_state(
        node: &OrderNode,
    ) -> (
        LimitGtcOrder,
        u64,
        QueuePriority,
        Option<OrderIndex>,
        Option<OrderIndex>,
    ) {
        (
            node.original_order().clone(),
            node.remaining(),
            node.priority(),
            node.prev(),
            node.next(),
        )
    }
    fn assert_partial_fill_panics_without_mutation(
        level: &mut PriceLevel,
        arena: &mut OrderArena,
        fill: Qty,
    ) {
        let level_before = level_state(level);
        let arena_before = arena.test_snapshot();

        let result = catch_unwind(AssertUnwindSafe(|| {
            level.apply_head_partial_fill(arena, fill);
        }));

        assert!(result.is_err(), "invalid partial fill must fail closed");

        assert_eq!(
            level_state(level),
            level_before,
            "failed partial fill mutated level"
        );

        assert_eq!(
            arena.test_snapshot(),
            arena_before,
            "failed partial fill mutated Arena slots/free-list"
        );
    }
    /// 独立 Vec 先进先出模型生成的单步结构操作。
    #[derive(Debug, Clone)]
    enum LevelOp {
        /// 在尾部追加给定正数量的同价节点。
        Push(u64),
        /// 移除当前先进先出头；空档时应保持不变并返回 `None`。
        PopFront,
        /// 选择非头逻辑位置的选择器，由模型映射为中间或尾节点。
        Unlink(u16),
    }

    /// 独立先进先出模型中一个仍应存在的逻辑成员。
    #[derive(Debug, Clone)]
    struct ModelEntry {
        /// 原始订单载荷，用于验证复用槽位没有改变逻辑成员。
        original: LimitGtcOrder,
        /// 该成员当前应计入档位聚合的数量。
        remaining: u64,
        /// 模型分配的严格递增优先级。
        priority: QueuePriority,
        /// 真实操作返回的有效槽位，用于与 Arena 集成状态比对。
        slot: OrderIndex,
    }

    /// 生成有界混合操作；权重保证有足够的非空先进先出状态可验证。
    fn level_op_strategy() -> impl Strategy<Value = LevelOp> {
        prop_oneof![
            // 尾插略微提高权重，否则大量空档移除头节点/解除链接的覆盖价值较低。
            5 => (1u16..=1000u16).prop_map(|qty| LevelOp::Push(qty as u64)),
            3 => Just(LevelOp::PopFront),
            3 => any::<u16>().prop_map(LevelOp::Unlink),
        ]
    }

    /// 比较独立 Vec 模型与真实价档/Arena 的完整可观察结构。
    ///
    /// 每一步同时检查元数据、每个有效节点的载荷/双链、严格优先级
    /// 和测试校验器，以便缩减后仍能指出首个偏离操作。
    fn assert_level_matches_model(
        level: &PriceLevel,
        arena: &OrderArena,
        model: &[ModelEntry],
        step: usize,
        op: &LevelOp,
    ) -> Result<(), TestCaseError> {
        let expected_count =
            u32::try_from(model.len()).expect("property model length is bounded to <= 96");

        let expected_total = model
            .iter()
            .try_fold(0u64, |total, entry| total.checked_add(entry.remaining));

        let Some(expected_total) = expected_total else {
            return Err(TestCaseError::fail(format!(
                "model quantity overflow: step={step}, op={op:?}"
            )));
        };

        prop_assert_eq!(
            level.count(),
            expected_count,
            "count mismatch: step={}, op={:?}",
            step,
            op
        );

        prop_assert_eq!(
            level.total_visible_qty(),
            expected_total,
            "total_visible_qty mismatch: step={}, op={:?}",
            step,
            op
        );

        prop_assert_eq!(
            level.head(),
            model.first().map(|entry| entry.slot),
            "head mismatch: step={}, op={:?}",
            step,
            op
        );

        prop_assert_eq!(
            level.tail(),
            model.last().map(|entry| entry.slot),
            "tail mismatch: step={}, op={:?}",
            step,
            op
        );

        prop_assert_eq!(
            level.is_empty(),
            model.is_empty(),
            "is_empty mismatch: step={}, op={:?}",
            step,
            op
        );

        for (position, entry) in model.iter().enumerate() {
            let Some(node) = arena.get(entry.slot) else {
                return Err(TestCaseError::fail(format!(
                    "live model slot missing from arena: \
                 step={step}, op={op:?}, position={position}, slot={:?}",
                    entry.slot
                )));
            };

            let expected_prev = if position == 0 {
                None
            } else {
                Some(model[position - 1].slot)
            };

            let expected_next = model.get(position + 1).map(|entry| entry.slot);

            prop_assert_eq!(
                node_state(node),
                (
                    entry.original.clone(),
                    entry.remaining,
                    entry.priority,
                    expected_prev,
                    expected_next,
                ),
                "node mismatch: step={}, op={:?}, position={}, slot={:?}",
                step,
                op,
                position,
                entry.slot
            );
        }

        prop_assert_eq!(
            validate_level(level, arena),
            Ok(()),
            "validate_level failed: step={}, op={:?}",
            step,
            op
        );

        Ok(())
    }

    #[test]
    fn push_back_builds_fifo_on_both_sides() {
        for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let originals = [
                order(10, side, price, 4),
                order(20, side, price, 7),
                order(30, side, price, 9),
            ];

            assert!(level.is_empty());

            for (position, original) in originals.iter().enumerate() {
                let priority = QueuePriority::new(position as u128 + 10);

                let inserted = level
                    .push_back(&mut arena, OrderNode::new(original.clone(), priority))
                    .unwrap();

                assert_eq!(inserted, index(position));
                assert_eq!(level.head(), Some(index(0)));
                assert_eq!(level.tail(), Some(inserted));
                assert_eq!(level.count(), position as u32 + 1);

                let expected_total: u64 = originals[..=position]
                    .iter()
                    .map(|order| order.qty.get())
                    .sum();

                assert_eq!(level.total_visible_qty(), expected_total);
                assert!(!level.is_empty());

                // 每次尾插后验证完整链表，而不只检查新节点。
                for (current, expected_order) in originals.iter().take(position + 1).enumerate() {
                    let node = arena.get(index(current)).unwrap();

                    assert_eq!(node.original_order(), expected_order);
                    assert_eq!(node.remaining(), expected_order.qty.get());
                    assert_eq!(node.priority(), QueuePriority::new(current as u128 + 10));

                    let expected_prev = (current > 0).then(|| index(current - 1));

                    let expected_next = (current < position).then(|| index(current + 1));

                    assert_eq!(node.prev(), expected_prev);
                    assert_eq!(node.next(), expected_next);
                }
                assert_eq!(validate_level(&level, &arena), Ok(()));
            }
        }
    }

    #[test]
    fn reused_lower_index_still_appends_at_tail() {
        let mut arena = OrderArena::new();
        let mut level = PriceLevel::new();

        // 先占用槽位 0。
        let unused = arena
            .insert(OrderNode::new(
                order(90, Side::Sell, 110, 1),
                QueuePriority::new(1),
            ))
            .unwrap();

        assert_eq!(unused, index(0));

        let first_order = order(10, Side::Buy, 95, 4);

        // 第一张真正属于档位的订单位于槽位 1。
        let first = level
            .push_back(
                &mut arena,
                OrderNode::new(first_order.clone(), QueuePriority::new(10)),
            )
            .unwrap();

        assert_eq!(first, index(1));

        // 释放与档位无关的槽位 0。
        arena.remove(unused).unwrap();

        let second_order = order(20, Side::Buy, 95, 6);

        // 新节点应复用槽位 0，但必须链接在槽位 1 之后。
        let second = level
            .push_back(
                &mut arena,
                OrderNode::new(second_order.clone(), QueuePriority::new(11)),
            )
            .unwrap();

        assert_eq!(second, index(0));
        assert_eq!(level.head(), Some(first));
        assert_eq!(level.tail(), Some(second));
        assert_eq!(level.count(), 2);
        assert_eq!(level.total_visible_qty(), 10);

        let head = arena.get(first).unwrap();
        assert_eq!(head.original_order(), &first_order);
        assert_eq!(head.remaining(), 4);
        assert_eq!(head.priority(), QueuePriority::new(10));
        assert_eq!(head.prev(), None);
        assert_eq!(head.next(), Some(second));

        let tail = arena.get(second).unwrap();
        assert_eq!(tail.original_order(), &second_order);
        assert_eq!(tail.remaining(), 6);
        assert_eq!(tail.priority(), QueuePriority::new(11));
        assert_eq!(tail.prev(), Some(first));
        assert_eq!(tail.next(), None);
        assert_eq!(validate_level(&level, &arena), Ok(()));
    }

    #[test]
    fn arithmetic_overflow_preserves_level_arena_and_free() {
        for count_overflow in [false, true] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            // 两个独立空槽，用于验证失败后后进先出顺序不变。
            let hole0 = arena
                .insert(OrderNode::new(
                    order(90, Side::Sell, 110, 1),
                    QueuePriority::new(0),
                ))
                .unwrap();

            let hole1 = arena
                .insert(OrderNode::new(
                    order(91, Side::Sell, 110, 1),
                    QueuePriority::new(1),
                ))
                .unwrap();

            let first_qty = if count_overflow { 10 } else { u64::MAX };

            let tail_index = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, first_qty), QueuePriority::new(10)),
                )
                .unwrap();

            arena.remove(hole0).unwrap();
            arena.remove(hole1).unwrap();

            if count_overflow {
                // 只在测试中合成节点数溢出边界。
                level.count = u32::MAX;
            }
            let arena_before = arena.test_snapshot();

            let before_level = level_state(&level);
            let before_tail = node_state(arena.get(tail_index).unwrap());

            assert_eq!(
                level.push_back(
                    &mut arena,
                    OrderNode::new(order(2, Side::Buy, 100, 1), QueuePriority::new(11),),
                ),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(arena.test_snapshot(), arena_before);

            assert_eq!(level_state(&level), before_level);
            assert_eq!(node_state(arena.get(tail_index).unwrap()), before_tail);

            assert!(arena.get(hole0).is_none());
            assert!(arena.get(hole1).is_none());

            // 失败没有消费空闲，仍按原来的后进先出顺序复用。
            for (expected, id) in [(hole1, 92), (hole0, 93)] {
                let actual = arena
                    .insert(OrderNode::new(
                        order(id, Side::Sell, 110, 1),
                        QueuePriority::new(id),
                    ))
                    .unwrap();

                assert_eq!(actual, expected);
            }

            // 存活的旧尾节点未改变。
            assert_eq!(node_state(arena.get(tail_index).unwrap()), before_tail);
        }
    }

    #[test]
    fn invalid_input_or_tail_panics_before_mutation() {
        for case in 0..10 {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let hole = arena
                .insert(OrderNode::new(
                    order(90, Side::Sell, 110, 1),
                    QueuePriority::new(1),
                ))
                .unwrap();

            let tail_index = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 5), QueuePriority::new(10)),
                )
                .unwrap();

            arena.remove(hole).unwrap();

            let mut incoming = OrderNode::new(order(2, Side::Buy, 100, 3), QueuePriority::new(11));

            match case {
                0 => incoming.set_prev(Some(tail_index)),
                1 => incoming.set_next(Some(tail_index)),

                2 => incoming = OrderNode::new(order(2, Side::Buy, 100, 3), QueuePriority::new(10)),

                3 => incoming = OrderNode::new(order(2, Side::Buy, 100, 3), QueuePriority::new(9)),

                4 => {
                    incoming = OrderNode::new(order(2, Side::Sell, 100, 3), QueuePriority::new(11))
                }

                5 => incoming = OrderNode::new(order(2, Side::Buy, 101, 3), QueuePriority::new(11)),

                6 => arena
                    .get_mut(tail_index)
                    .unwrap()
                    .set_next(Some(tail_index)),

                7 => level.tail = Some(index(99)),

                8 => level.head = None,

                9 => {
                    // 已有挂单剩余量=5，但档位总量被破坏为 0。
                    level.total_visible_qty = 0;
                }
                _ => unreachable!(),
            }

            let before_level = level_state(&level);
            let before_tail = node_state(arena.get(tail_index).unwrap());
            let arena_before = arena.test_snapshot();
            let result = catch_unwind(AssertUnwindSafe(|| level.push_back(&mut arena, incoming)));

            assert!(result.is_err(), "case={case}");

            assert_eq!(level_state(&level), before_level, "case={case}");
            assert_eq!(
                arena.test_snapshot(),
                arena_before,
                "arena mutated before panic: case={case}"
            );

            assert_eq!(
                node_state(arena.get(tail_index).unwrap()),
                before_tail,
                "case={case}"
            );

            // 验证失败没有占用或消费原来的空槽。
            assert!(arena.get(hole).is_none());

            let reused = arena
                .insert(OrderNode::new(
                    order(99, Side::Sell, 110, 1),
                    QueuePriority::new(99),
                ))
                .unwrap();

            assert_eq!(reused, hole, "case={case}");
        }
    }
    #[test]
    fn push_back_slot_access_count_is_constant() {
        for side in [Side::Buy, Side::Sell] {
            for initial_len in [0usize, 1, 8, 128, 1024] {
                for reuse in [false, true] {
                    let mut arena = OrderArena::new();
                    let mut level = PriceLevel::new();

                    let price = match side {
                        Side::Buy => 95,
                        Side::Sell => 105,
                    };

                    // 准备已有价格档位。
                    let mut first_index = None;

                    for i in 0..initial_len {
                        let node = OrderNode::new(
                            order(i as u128 + 1, side, price, 2),
                            QueuePriority::new(i as u128),
                        );

                        let inserted = level.push_back(&mut arena, node).unwrap();

                        if first_index.is_none() {
                            first_index = Some(inserted);
                        }
                    }

                    let previous_tail = level.tail();

                    // 复用路径：由独立、未挂链的节点产生空槽。
                    if reuse {
                        let isolated = arena
                            .insert(OrderNode::new(
                                order(900_000, Side::Buy, 77, 1),
                                QueuePriority::new(0),
                            ))
                            .unwrap();

                        assert!(arena.remove(isolated).is_some());
                        assert!(arena.get(isolated).is_none());
                    }

                    let original = order(10_000 + initial_len as u128, side, price, 3);

                    let priority = QueuePriority::new(initial_len as u128);

                    // 测试顺序：
                    // 准备完成 -> 清零 -> 一次尾插 -> 立即读取次数。
                    arena.reset_slot_accesses();

                    let inserted = level
                        .push_back(&mut arena, OrderNode::new(original.clone(), priority))
                        .unwrap();

                    let actual_accesses = arena.slot_accesses();

                    let expected_accesses = match (initial_len == 0, reuse) {
                        (true, false) => 1,
                        (true, true) => 2,
                        (false, false) => 3,
                        (false, true) => 4,
                    };

                    assert_eq!(
                        actual_accesses, expected_accesses,
                        "side={side:?}, len={initial_len}, reuse={reuse}"
                    );

                    // 读取次数已保存，下面才开始结构检查。
                    assert_eq!(level.head(), Some(first_index.unwrap_or(inserted)));
                    assert_eq!(level.tail(), Some(inserted));
                    assert_eq!(level.count(), initial_len as u32 + 1);
                    assert_eq!(level.total_visible_qty(), initial_len as u64 * 2 + 3);
                    assert!(!level.is_empty());

                    let tail = arena.get(inserted).unwrap();

                    assert_eq!(tail.original_order(), &original);
                    assert_eq!(tail.remaining(), 3);
                    assert_eq!(tail.priority(), priority);
                    assert_eq!(tail.prev(), previous_tail);
                    assert_eq!(tail.next(), None);

                    if let Some(old_tail) = previous_tail {
                        let previous = arena.get(old_tail).unwrap();

                        assert_eq!(previous.next(), Some(inserted));
                        assert_eq!(previous.remaining(), 2);
                        assert_eq!(
                            previous.priority(),
                            QueuePriority::new(initial_len as u128 - 1)
                        );
                    }
                }
            }
        }
    }
    /// 验证单价先进先出的全链结构，返回可读失败原因而不是触发不变量断言。
    ///
    /// 该测试辅助从头节点正向和尾节点反向遍历，核对端点、双链、节点数、
    /// 可见数量、同方向/价格和严格递增优先级；无关有效 Arena
    /// 槽位不属于此档位，故不参与检查。
    fn validate_level(level: &PriceLevel, arena: &OrderArena) -> Result<(), String> {
        // 节点数是空档的权威判断。
        if level.count() == 0 {
            if level.head().is_some() || level.tail().is_some() || level.total_visible_qty() != 0 {
                return Err("empty level has inconsistent endpoints or quantity".into());
            }

            return Ok(());
        }

        let head = level
            .head()
            .ok_or_else(|| "non-empty level is missing head".to_string())?;

        let tail = level
            .tail()
            .ok_or_else(|| "non-empty level is missing tail".to_string())?;

        if level.total_visible_qty() == 0 {
            return Err("non-empty level has zero total_visible_qty".into());
        }

        // ---------- 正向遍历: 头节点 -> `next` ----------

        let mut forward = Vec::new();
        let mut visited = BTreeSet::new();

        let mut current = Some(head);
        let mut expected_prev = None;

        let mut actual_count = 0u32;
        let mut actual_qty = 0u64;

        let mut expected_side = None;
        let mut expected_price = None;
        let mut previous_priority = None;

        while let Some(index) = current {
            if !visited.insert(index) {
                return Err(format!(
                    "cycle or duplicate index in forward chain: {}",
                    index.get()
                ));
            }

            let node = arena
                .get(index)
                .ok_or_else(|| format!("forward chain references missing node: {}", index.get()))?;

            // 当前节点必须反向指回真实前驱节点。
            if node.prev() != expected_prev {
                return Err(format!("broken prev link at index {}", index.get()));
            }

            if node.remaining() == 0 {
                return Err(format!("zero remaining node at index {}", index.get()));
            }

            let order = node.original_order();

            match expected_side {
                None => expected_side = Some(order.side),
                Some(side) if side != order.side => {
                    return Err(format!("side mismatch at index {}", index.get()));
                }
                Some(_) => {}
            }

            match expected_price {
                None => expected_price = Some(order.price),
                Some(price) if price != order.price => {
                    return Err(format!("price mismatch at index {}", index.get()));
                }
                Some(_) => {}
            }

            if let Some(previous) = previous_priority
                && node.priority() <= previous
            {
                return Err(format!(
                    "priority is not strictly increasing at index {}",
                    index.get()
                ));
            }

            previous_priority = Some(node.priority());

            actual_count = actual_count
                .checked_add(1)
                .ok_or_else(|| "count overflow while validating".to_string())?;

            actual_qty = actual_qty
                .checked_add(node.remaining())
                .ok_or_else(|| "qty overflow while validating".to_string())?;

            forward.push(index);

            expected_prev = Some(index);
            current = node.next();
        }

        if forward.last().copied() != Some(tail) {
            return Err("forward chain does not end at tail".into());
        }

        if actual_count != level.count() {
            return Err(format!(
                "count mismatch: actual={actual_count}, level={}",
                level.count()
            ));
        }

        if actual_qty != level.total_visible_qty() {
            return Err(format!(
                "quantity mismatch: actual={actual_qty}, level={}",
                level.total_visible_qty()
            ));
        }

        // ---------- 反向遍历: 尾节点 -> 前驱 ----------

        let mut reverse = Vec::new();
        let mut reverse_visited = BTreeSet::new();

        let mut current = Some(tail);
        let mut expected_next = None;

        while let Some(index) = current {
            if !reverse_visited.insert(index) {
                return Err(format!(
                    "cycle or duplicate index in reverse chain: {}",
                    index.get()
                ));
            }

            let node = arena
                .get(index)
                .ok_or_else(|| format!("reverse chain references missing node: {}", index.get()))?;

            // 当前节点必须正向指回真实后继节点。
            if node.next() != expected_next {
                return Err(format!("broken next link at index {}", index.get()));
            }

            reverse.push(index);

            expected_next = Some(index);
            current = node.prev();
        }

        let expected_reverse: Vec<_> = forward.iter().rev().copied().collect();

        if reverse != expected_reverse {
            return Err("reverse traversal is not the exact inverse of forward traversal".into());
        }

        Ok(())
    }
    /// 校验器测试注入的单一损坏维度，每种样本只应违反其目标不变量。
    #[derive(Debug, Clone, Copy)]
    enum LevelDamage {
        /// 元数据节点数与实际链长不一致。
        WrongCount,
        /// 元数据可见量与节点数量聚合不一致。
        WrongQty,
        /// 非空档缺少头节点端点。
        MissingHead,
        /// 非空档缺少尾节点端点。
        MissingTail,
        /// 前驱节点没有正向指向其后继。
        BrokenForwardLink,
        /// 正向链包含循环。
        Cycle,
        /// 链接引用 Arena 中不存在的节点。
        MissingNode,
        /// 同一档位节点拥有不同价格。
        PriceMismatch,
        /// 先进先出链中的优先级不再严格递增。
        PriorityViolation,
        /// 空节点数与非空端点或数量的矛盾状态。
        NonEmptyCountZero,
        /// 后继节点没有反向指向其前驱。
        BrokenBackwardLink,
        /// 同一档位节点拥有不同方向。
        SideMismatch,
        /// 测试遍历累加可见数量时发生溢出。
        QtyOverflow,
    }
    #[test]
    fn validator_rejects_corrupted_levels() {
        let cases = [
            LevelDamage::WrongCount,
            LevelDamage::WrongQty,
            LevelDamage::MissingHead,
            LevelDamage::MissingTail,
            LevelDamage::BrokenForwardLink,
            LevelDamage::Cycle,
            LevelDamage::MissingNode,
            LevelDamage::PriceMismatch,
            LevelDamage::PriorityViolation,
            LevelDamage::NonEmptyCountZero,
            LevelDamage::BrokenBackwardLink,
            LevelDamage::SideMismatch,
            LevelDamage::QtyOverflow,
        ];

        for damage in cases {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 5), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, Side::Buy, 100, 7), QueuePriority::new(11)),
                )
                .unwrap();

            assert_eq!(validate_level(&level, &arena), Ok(()));

            match damage {
                LevelDamage::WrongCount => {
                    level.count = 3;
                }

                LevelDamage::WrongQty => {
                    level.total_visible_qty = 13;
                }

                LevelDamage::MissingHead => {
                    level.head = None;
                }

                LevelDamage::MissingTail => {
                    level.tail = None;
                }

                LevelDamage::BrokenForwardLink => {
                    arena.get_mut(first).unwrap().set_next(None);
                }

                LevelDamage::Cycle => {
                    // 头节点 -> 第二个 -> 头节点
                    arena.get_mut(second).unwrap().set_next(Some(first));
                }

                LevelDamage::MissingNode => {
                    // 价档仍引用第二个，但 Arena 已无该节点。
                    arena.remove(second).unwrap();
                }

                LevelDamage::PriceMismatch => {
                    // 保留相同槽位和链关系，但换成错误价格节点。
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Buy, 101, 7), QueuePriority::new(11));
                    replacement.set_prev(Some(first));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, second);
                }

                LevelDamage::PriorityViolation => {
                    arena.remove(second).unwrap();

                    let mut replacement = OrderNode::new(
                        order(2, Side::Buy, 100, 7),
                        // 与前驱相等，不再严格递增。
                        QueuePriority::new(10),
                    );
                    replacement.set_prev(Some(first));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, second);
                }
                LevelDamage::NonEmptyCountZero => {
                    // 头节点、尾节点仍然存在，但节点数错误地归零。
                    level.count = 0;
                }

                LevelDamage::BrokenBackwardLink => {
                    // `first.next` 仍指向第二个，
                    // 但 `second.prev` 不再指向第一个。
                    arena.get_mut(second).unwrap().set_prev(None);
                }

                LevelDamage::SideMismatch => {
                    // 复用原槽位，保持链接、价格和优先级不变，
                    // 只改变节点方向。
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Sell, 100, 7), QueuePriority::new(11));

                    replacement.set_prev(Some(first));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, second);
                }

                LevelDamage::QtyOverflow => {
                    // `first.remaining` = 5
                    // `second.remaining` = `u64::MAX`
                    // 仅替换第二个节点，保留合法链接和优先级。
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Buy, 100, u64::MAX), QueuePriority::new(11));

                    replacement.set_prev(Some(first));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, second);
                }
            }

            let result = validate_level(&level, &arena);

            if matches!(damage, LevelDamage::QtyOverflow) {
                assert_eq!(result, Err("qty overflow while validating".to_string()));
            } else {
                assert!(
                    result.is_err(),
                    "validator unexpectedly accepted damage={damage:?}"
                );
            }
        }
    }
    #[test]
    fn validator_ignores_unrelated_live_nodes() {
        let mut arena = OrderArena::new();
        let mut level = PriceLevel::new();

        for (id, qty, priority) in [(1, 5, 10), (2, 7, 11)] {
            level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(id, Side::Buy, 100, qty), QueuePriority::new(priority)),
                )
                .unwrap();
        }

        // 不属于当前档位，方向、价格及优先级均不同。
        let unrelated = arena
            .insert(OrderNode::new(
                order(99, Side::Sell, 999, 3),
                QueuePriority::new(0),
            ))
            .unwrap();

        assert!(arena.get(unrelated).is_some());

        // 校验器只检查当前档位的可达链。
        assert_eq!(validate_level(&level, &arena), Ok(()));
    }
    #[test]
    fn head_partial_fill_updates_only_remaining_and_total_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, side, 100, 10), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, side, 100, 7), QueuePriority::new(11)),
                )
                .unwrap();

            let head_before = level.head();
            let tail_before = level.tail();
            let count_before = level.count();

            let first_priority = arena.get(first).unwrap().priority();
            let first_prev = arena.get(first).unwrap().prev();
            let first_next = arena.get(first).unwrap().next();

            let second_before = node_state(arena.get(second).unwrap());

            let (_, free_before) = arena.test_snapshot();

            level.apply_head_partial_fill(&mut arena, Qty::try_new(3).unwrap());

            assert_eq!(arena.get(first).unwrap().remaining(), 7);

            assert_eq!(level.total_visible_qty(), 14);

            // 价档结构不变。
            assert_eq!(level.head(), head_before);
            assert_eq!(level.tail(), tail_before);
            assert_eq!(level.count(), count_before);

            // 头节点除剩余量外全部不变。
            let first_after = arena.get(first).unwrap();

            assert_eq!(first_after.priority(), first_priority);
            assert_eq!(first_after.prev(), first_prev);
            assert_eq!(first_after.next(), first_next);

            // 未参与本次成交的节点完全不变。
            assert_eq!(node_state(arena.get(second).unwrap()), second_before);

            // 部分成交不操作 Arena 生命周期。
            let (_, free_after) = arena.test_snapshot();
            assert_eq!(free_after, free_before);

            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn head_partial_fill_rejects_full_and_overfill_without_mutation() {
        for side in [Side::Buy, Side::Sell] {
            for fill in [5u64, 6] {
                let mut arena = OrderArena::new();
                let mut level = PriceLevel::new();

                level
                    .push_back(
                        &mut arena,
                        OrderNode::new(order(1, side, 100, 5), QueuePriority::new(10)),
                    )
                    .unwrap();

                assert_partial_fill_panics_without_mutation(
                    &mut level,
                    &mut arena,
                    Qty::try_new(fill).unwrap(),
                );
            }
        }
    }
    #[test]
    fn head_partial_fill_corruption_panics_without_mutation() {
        for case in 0..7 {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            if case == 0 {
                // 合法空档。
                assert_partial_fill_panics_without_mutation(
                    &mut level,
                    &mut arena,
                    Qty::try_new(1).unwrap(),
                );
                continue;
            }

            let head = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 5), QueuePriority::new(10)),
                )
                .unwrap();

            match case {
                // 非空档缺头节点。
                1 => {
                    level.head = None;
                }

                // 非空档缺尾节点。
                2 => {
                    level.tail = None;
                }

                // 价档仍引用头节点，但 Arena 槽位已释放。
                3 => {
                    arena.remove(head).unwrap();
                }

                // 聚合量为零。
                4 => {
                    level.total_visible_qty = 0;
                }

                // 聚合量小于成交，checked_sub 发生错误时必须停止处理。
                5 => {
                    level.total_visible_qty = 1;
                }

                // 剩余量=5，但聚合量错误为 6。
                // checked_sub 自身仍然成功，因此必须由单节点
                // 聚合量不变量捕获。
                6 => {
                    level.total_visible_qty = 6;
                }

                _ => unreachable!(),
            }

            assert_partial_fill_panics_without_mutation(
                &mut level,
                &mut arena,
                Qty::try_new(2).unwrap(),
            );
        }
    }
    #[test]
    fn head_partial_fill_broken_successor_panics_without_mutation() {
        let mut arena = OrderArena::new();
        let mut level = PriceLevel::new();

        let first = level
            .push_back(
                &mut arena,
                OrderNode::new(order(1, Side::Buy, 100, 10), QueuePriority::new(10)),
            )
            .unwrap();

        let second = level
            .push_back(
                &mut arena,
                OrderNode::new(order(2, Side::Buy, 100, 7), QueuePriority::new(11)),
            )
            .unwrap();

        // `head.next` 仍指向第二个，
        // 但 `successor.prev` 被破坏。
        arena.get_mut(second).unwrap().set_prev(None);

        assert_eq!(arena.get(first).unwrap().next(), Some(second));

        assert_partial_fill_panics_without_mutation(
            &mut level,
            &mut arena,
            Qty::try_new(3).unwrap(),
        );
    }
    #[test]
    fn pop_front_preserves_fifo_on_both_sides() {
        for (side, price) in [(Side::Buy, 95), (Side::Sell, 105)] {
            for len in [0usize, 1, 4] {
                let mut arena = OrderArena::new();
                let mut level = PriceLevel::new();
                let mut expected = Vec::new();

                for i in 0..len {
                    let original = order(100 + i as u128, side, price, i as u64 + 2);
                    let priority = QueuePriority::new(10 + i as u128);

                    let slot = level
                        .push_back(&mut arena, OrderNode::new(original.clone(), priority))
                        .unwrap();

                    expected.push((slot, original, priority));
                }

                assert_eq!(validate_level(&level, &arena), Ok(()));

                // 空档返回 None，Arena 和价档均不变。
                if len == 0 {
                    let before_arena = arena.test_snapshot();
                    let before_level = level_state(&level);

                    assert!(level.pop_front(&mut arena).is_none());

                    assert_eq!(arena.test_snapshot(), before_arena);
                    assert_eq!(level_state(&level), before_level);
                    continue;
                }

                for (position, (slot, original, priority)) in expected.iter().enumerate() {
                    let removed = level.pop_front(&mut arena).unwrap();

                    assert_eq!(removed.original_order(), original);
                    assert_eq!(removed.remaining(), original.qty.get());
                    assert_eq!(removed.priority(), *priority);
                    assert_eq!(removed.prev(), None);
                    assert_eq!(removed.next(), None);

                    assert!(arena.get(*slot).is_none());

                    let surviving = &expected[position + 1..];

                    assert_eq!(level.count(), surviving.len() as u32);
                    assert_eq!(
                        level.total_visible_qty(),
                        surviving
                            .iter()
                            .map(|(_, order, _)| order.qty.get())
                            .sum::<u64>()
                    );

                    assert_eq!(level.head(), surviving.first().map(|(slot, _, _)| *slot));
                    assert_eq!(level.tail(), surviving.last().map(|(slot, _, _)| *slot));

                    // 其余节点的订单内容、数量和优先级不变。
                    for (slot, original, priority) in surviving {
                        let node = arena.get(*slot).unwrap();

                        assert_eq!(node.original_order(), original);
                        assert_eq!(node.remaining(), original.qty.get());
                        assert_eq!(node.priority(), *priority);
                    }

                    // 每次成功删除后检查剩余双向链表。
                    assert_eq!(validate_level(&level, &arena), Ok(()));
                }

                assert!(level.is_empty());
                assert!(level.pop_front(&mut arena).is_none());
            }
        }
    }
    #[test]
    fn pop_front_single_max_quantity() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let original = order(1, side, 100, u64::MAX);
            let priority = QueuePriority::new(10);

            let slot = level
                .push_back(&mut arena, OrderNode::new(original.clone(), priority))
                .unwrap();

            let removed = level.pop_front(&mut arena).unwrap();

            assert_eq!(removed.original_order(), &original);
            assert_eq!(removed.remaining(), u64::MAX);
            assert_eq!(removed.priority(), priority);
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            assert!(arena.get(slot).is_none());
            assert_eq!(level.count(), 0);
            assert_eq!(level.total_visible_qty(), 0);
            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn popped_head_slot_can_be_reused_at_tail() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, side, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, side, 100, 7), QueuePriority::new(11)),
                )
                .unwrap();

            let removed = level.pop_front(&mut arena).unwrap();

            assert_eq!(removed.original_order().order_id, OrderId::new(1));
            assert!(arena.get(first).is_none());
            assert_eq!(validate_level(&level, &arena), Ok(()));

            // 复用旧头的数值索引，但新订单必须排在当前尾部。
            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, side, 100, 3), QueuePriority::new(12)),
                )
                .unwrap();

            assert_eq!(third, first);
            assert_eq!(level.head(), Some(second));
            assert_eq!(level.tail(), Some(third));
            assert_eq!(level.count(), 2);
            assert_eq!(level.total_visible_qty(), 10);

            assert_eq!(arena.get(second).unwrap().next(), Some(third));
            assert_eq!(arena.get(third).unwrap().prev(), Some(second));

            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn pop_front_corruption_panics_without_mutation() {
        for case in 0..14 {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 5), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, Side::Buy, 100, 7), QueuePriority::new(11)),
                )
                .unwrap();

            match case {
                0 => level.count = 0,
                1 => level.head = None,
                2 => level.tail = None,
                3 => {
                    arena.get_mut(first).unwrap().set_prev(Some(second));
                }
                4 => {
                    arena.get_mut(first).unwrap().set_next(None);
                }
                5 => {
                    arena.get_mut(second).unwrap().set_prev(None);
                }
                6 => level.total_visible_qty = 4,
                7 => {
                    arena.remove(first).unwrap();
                }
                8 => {
                    arena.remove(second).unwrap();
                }
                9 => {
                    arena.get_mut(second).unwrap().set_next(Some(first));
                }
                10..=13 => {
                    let (side, price, priority) = match case {
                        10 => (Side::Sell, 100, 11), // 后继节点方向错误
                        11 => (Side::Buy, 101, 11),  // 后继节点价格错误
                        12 => (Side::Buy, 100, 10),  // 优先级与头节点相等
                        13 => (Side::Buy, 100, 9),   // 优先级小于头节点
                        _ => unreachable!(),
                    };

                    // 保留原有索引和双向链接，仅替换损坏节点。
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, side, price, 7), QueuePriority::new(priority));

                    replacement.set_prev(Some(first));

                    assert_eq!(arena.insert(replacement).unwrap(), second);
                }
                _ => unreachable!(),
            }

            let before_level = level_state(&level);
            let before_arena = arena.test_snapshot();

            let result = catch_unwind(AssertUnwindSafe(|| level.pop_front(&mut arena)));

            assert!(result.is_err(), "case={case}");

            // 必须比较包含全部槽位和有序空闲的完整快照。
            assert_eq!(
                level_state(&level),
                before_level,
                "level mutated: case={case}"
            );
            assert_eq!(
                arena.test_snapshot(),
                before_arena,
                "arena mutated: case={case}"
            );
        }
    }
    #[test]
    fn pop_front_single_node_nonzero_residual_panics_without_mutation() {
        let mut arena = OrderArena::new();
        let mut level = PriceLevel::new();

        level
            .push_back(
                &mut arena,
                OrderNode::new(order(1, Side::Buy, 100, 5), QueuePriority::new(10)),
            )
            .unwrap();

        // 单节点剩余量=5，但档位统计值错误地增加至 6。
        level.total_visible_qty = 6;

        let before_level = level_state(&level);
        let before_arena = arena.test_snapshot();

        let result = catch_unwind(AssertUnwindSafe(|| level.pop_front(&mut arena)));

        assert!(result.is_err());
        assert_eq!(level_state(&level), before_level);
        assert_eq!(arena.test_snapshot(), before_arena);
    }
    #[test]
    fn unlink_middle_from_three_nodes_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, side, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, side, 100, 5), QueuePriority::new(11)),
                )
                .unwrap();

            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, side, 100, 6), QueuePriority::new(12)),
                )
                .unwrap();

            let removed = level.unlink(&mut arena, second);

            assert_eq!(removed.original_order().order_id, OrderId::new(2));
            assert_eq!(removed.remaining(), 5);
            assert_eq!(removed.priority(), QueuePriority::new(11));
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            assert!(arena.get(second).is_none());

            assert_eq!(level.head(), Some(first));
            assert_eq!(level.tail(), Some(third));
            assert_eq!(level.count(), 2);
            assert_eq!(level.total_visible_qty(), 10);

            assert_eq!(arena.get(first).unwrap().prev(), None);
            assert_eq!(arena.get(first).unwrap().next(), Some(third));

            assert_eq!(arena.get(third).unwrap().prev(), Some(first));
            assert_eq!(arena.get(third).unwrap().next(), None);

            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn unlink_tail_from_three_nodes_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, side, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, side, 100, 5), QueuePriority::new(11)),
                )
                .unwrap();

            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, side, 100, 6), QueuePriority::new(12)),
                )
                .unwrap();

            let removed = level.unlink(&mut arena, third);

            assert_eq!(removed.original_order().order_id, OrderId::new(3));
            assert_eq!(removed.remaining(), 6);
            assert_eq!(removed.priority(), QueuePriority::new(12));
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            assert!(arena.get(third).is_none());

            assert_eq!(level.head(), Some(first));
            assert_eq!(level.tail(), Some(second));
            assert_eq!(level.count(), 2);
            assert_eq!(level.total_visible_qty(), 9);

            assert_eq!(arena.get(first).unwrap().next(), Some(second));
            assert_eq!(arena.get(second).unwrap().prev(), Some(first));
            assert_eq!(arena.get(second).unwrap().next(), None);

            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn unlinked_middle_slot_can_be_reused_at_tail() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, side, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, side, 100, 5), QueuePriority::new(11)),
                )
                .unwrap();

            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, side, 100, 6), QueuePriority::new(12)),
                )
                .unwrap();

            level.unlink(&mut arena, second);

            let fourth = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(4, side, 100, 7), QueuePriority::new(13)),
                )
                .unwrap();

            assert_eq!(fourth, second);

            assert_eq!(level.head(), Some(first));
            assert_eq!(level.tail(), Some(fourth));
            assert_eq!(level.count(), 3);
            assert_eq!(level.total_visible_qty(), 17);

            assert_eq!(arena.get(first).unwrap().next(), Some(third));

            assert_eq!(arena.get(third).unwrap().prev(), Some(first));
            assert_eq!(arena.get(third).unwrap().next(), Some(fourth));

            assert_eq!(arena.get(fourth).unwrap().prev(), Some(third));
            assert_eq!(arena.get(fourth).unwrap().next(), None);

            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    #[test]
    fn unlink_corruption_panics_without_mutation() {
        for case in 0..10 {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, Side::Buy, 100, 5), QueuePriority::new(11)),
                )
                .unwrap();

            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, Side::Buy, 100, 6), QueuePriority::new(12)),
                )
                .unwrap();

            let target = match case {
                // 中间节点解除链接
                0..=7 => second,

                // 尾节点解除链接
                8..=9 => third,

                _ => unreachable!(),
            };

            match case {
                // `target.prev` 缺失
                0 => {
                    arena.get_mut(second).unwrap().set_prev(None);
                }

                // `predecessor.next` 没有指回目标
                1 => {
                    arena.get_mut(first).unwrap().set_next(Some(third));
                }

                // `successor.prev` 没有指回目标
                2 => {
                    arena.get_mut(third).unwrap().set_prev(Some(first));
                }

                // 目标与前驱节点方向不一致
                3 => {
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Sell, 100, 5), QueuePriority::new(11));
                    replacement.set_prev(Some(first));
                    replacement.set_next(Some(third));

                    assert_eq!(arena.insert(replacement).unwrap(), second);
                }

                // 目标与前驱节点价格不一致
                4 => {
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Buy, 101, 5), QueuePriority::new(11));
                    replacement.set_prev(Some(first));
                    replacement.set_next(Some(third));

                    assert_eq!(arena.insert(replacement).unwrap(), second);
                }

                // 前驱节点优先级与目标相等
                5 => {
                    arena.remove(second).unwrap();

                    let mut replacement =
                        OrderNode::new(order(2, Side::Buy, 100, 5), QueuePriority::new(10));
                    replacement.set_prev(Some(first));
                    replacement.set_next(Some(third));

                    assert_eq!(arena.insert(replacement).unwrap(), second);
                }

                // 后继节点优先级没有严格大于目标
                6 => {
                    arena.remove(third).unwrap();

                    let mut replacement =
                        OrderNode::new(order(3, Side::Buy, 100, 6), QueuePriority::new(11));
                    replacement.set_prev(Some(second));

                    assert_eq!(arena.insert(replacement).unwrap(), third);
                }

                // 聚合量小于被删节点剩余量
                7 => {
                    level.total_visible_qty = 4;
                }

                // 尾节点却还有 `next`
                8 => {
                    arena.get_mut(third).unwrap().set_next(Some(first));
                }

                // `Level.tail` 与实际待删除尾节点不一致
                9 => {
                    level.tail = Some(second);
                }

                _ => unreachable!(),
            }

            let before_level = level_state(&level);
            let before_arena = arena.test_snapshot();

            let result = catch_unwind(AssertUnwindSafe(|| {
                level.unlink(&mut arena, target);
            }));

            assert!(result.is_err(), "case={case}");

            assert_eq!(
                level_state(&level),
                before_level,
                "level mutated: case={case}"
            );

            assert_eq!(
                arena.test_snapshot(),
                before_arena,
                "arena mutated: case={case}"
            );
        }
    }
    #[test]
    fn unlink_additional_corruption_panics_without_mutation() {
        for case in 0..4 {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(1, Side::Buy, 100, 4), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(2, Side::Buy, 100, 5), QueuePriority::new(11)),
                )
                .unwrap();

            let third = level
                .push_back(
                    &mut arena,
                    OrderNode::new(order(3, Side::Buy, 100, 6), QueuePriority::new(12)),
                )
                .unwrap();

            let target = match case {
                // 已释放的目标。
                0 => {
                    let removed = arena.remove(second).unwrap();
                    assert_eq!(removed.original_order().order_id, OrderId::new(2));
                    second
                }

                // 头节点不允许通过解除链接删除。
                1 => first,

                // 仅后继节点方向错误。
                2 => {
                    arena.remove(third).unwrap();

                    let mut replacement =
                        OrderNode::new(order(3, Side::Sell, 100, 6), QueuePriority::new(12));
                    replacement.set_prev(Some(second));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, third);

                    second
                }

                // 仅后继节点价格错误。
                3 => {
                    arena.remove(third).unwrap();

                    let mut replacement =
                        OrderNode::new(order(3, Side::Buy, 101, 6), QueuePriority::new(12));
                    replacement.set_prev(Some(second));

                    let reused = arena.insert(replacement).unwrap();
                    assert_eq!(reused, third);

                    second
                }

                _ => unreachable!(),
            };

            let before_level = level_state(&level);
            let before_arena = arena.test_snapshot();

            let result = catch_unwind(AssertUnwindSafe(|| {
                level.unlink(&mut arena, target);
            }));

            assert!(result.is_err(), "case={case}");

            assert_eq!(
                level_state(&level),
                before_level,
                "level mutated before panic: case={case}"
            );

            assert_eq!(
                arena.test_snapshot(),
                before_arena,
                "arena mutated before panic: case={case}"
            );
        }
    }
    #[test]
    fn unlink_tail_from_two_nodes_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            let first_order = order(1, side, 100, 4);
            let second_order = order(2, side, 100, 7);

            let first = level
                .push_back(
                    &mut arena,
                    OrderNode::new(first_order.clone(), QueuePriority::new(10)),
                )
                .unwrap();

            let second = level
                .push_back(
                    &mut arena,
                    OrderNode::new(second_order.clone(), QueuePriority::new(11)),
                )
                .unwrap();

            let removed = level.unlink(&mut arena, second);

            // 被删除节点完整返回，但必须已经脱离链表。
            assert_eq!(removed.original_order(), &second_order);
            assert_eq!(removed.remaining(), 7);
            assert_eq!(removed.priority(), QueuePriority::new(11));
            assert_eq!(removed.prev(), None);
            assert_eq!(removed.next(), None);

            // Arena 槽位已经释放。
            assert!(arena.get(second).is_none());

            // 原头节点成为唯一节点，同时也是头节点 / 尾节点。
            assert_eq!(level.head(), Some(first));
            assert_eq!(level.tail(), Some(first));
            assert_eq!(level.count(), 1);
            assert_eq!(level.total_visible_qty(), 4);
            assert!(!level.is_empty());

            let remaining = arena.get(first).unwrap();

            assert_eq!(remaining.original_order(), &first_order);
            assert_eq!(remaining.remaining(), 4);
            assert_eq!(remaining.priority(), QueuePriority::new(10));

            // 唯一节点两侧链接都为空。
            assert_eq!(remaining.prev(), None);
            assert_eq!(remaining.next(), None);

            // 完整档位不变量仍成立。
            assert_eq!(validate_level(&level, &arena), Ok(()));
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn mixed_push_pop_unlink_matches_fifo_model(
            buy_side in any::<bool>(),
            price in 1i64..=1_000_000i64,
            ops in prop::collection::vec(level_op_strategy(), 0..=96),
        ) {
            let side = if buy_side {
                Side::Buy
            } else {
                Side::Sell
            };

            let mut arena = OrderArena::new();
            let mut level = PriceLevel::new();

            // 独立先进先出模型。
            let mut model = Vec::<ModelEntry>::new();

            // 每个性质测试 `case` 自己维护有界标识符 / 优先级。
            //
            // 最多只有 96 次尾插，因此不会接近 u128 边界。
            let mut next_id = 1u128;
            let mut next_priority = 1u128;

            prop_assert_eq!(validate_level(&level, &arena), Ok(()));

            for (step, op) in ops.into_iter().enumerate() {
                let priority_before = next_priority;

                match &op {
                    LevelOp::Push(qty) => {
                        let following_id = next_id
                            .checked_add(1)
                            .expect("bounded property id cannot overflow");

                        let following_priority = next_priority
                            .checked_add(1)
                            .expect("bounded property priority cannot overflow");

                        let original = order(next_id, side, price, *qty);
                        let priority = QueuePriority::new(next_priority);

                        let inserted = level.push_back(
                            &mut arena,
                            OrderNode::new(original.clone(), priority),
                        );

                        prop_assert!(
                            inserted.is_ok(),
                            "push_back unexpectedly failed: step={}, op={:?}, result={:?}",
                            step,
                            op,
                            inserted
                        );

                        let slot = inserted.unwrap();

                        model.push(ModelEntry {
                            original,
                            remaining: *qty,
                            priority,
                            slot,
                        });

                        // 标识符 / 优先级只在成功尾插后提交递增。
                        next_id = following_id;
                        next_priority = following_priority;
                    }

                    LevelOp::PopFront => {
                        if model.is_empty() {
                            // 合法空档移除头节点必须保持全部状态不变。
                            let level_before = level_state(&level);
                            let arena_before = arena.test_snapshot();

                            let removed = level.pop_front(&mut arena);

                        prop_assert!(
                            removed.is_none(),
                            "empty pop_front returned a node: step={}, op={:?}",
                            step,
                            op
                        );

                        prop_assert_eq!(
                            level_state(&level),
                            level_before,
                            "empty pop_front mutated level: step={}, op={:?}",
                            step,
                            op
                        );
                        prop_assert_eq!(
                            arena.test_snapshot(),
                            arena_before,
                            "empty pop_front mutated arena/free-list: step={}, op={:?}",
                            step,
                            op
                        );
                        } else {
                            let expected = model[0].clone();

                            let removed = level.pop_front(&mut arena);

                            prop_assert!(
                                removed.is_some(),
                                "non-empty pop_front returned None: step={}, op={:?}",
                                step,
                                op
                            );

                            let removed = removed.unwrap();

                            prop_assert_eq!(
                                node_state(&removed),
                                (
                                    expected.original.clone(),
                                    expected.remaining,
                                    expected.priority,
                                    None,
                                    None,
                                ),
                                "pop_front returned wrong node: step={}, op={:?}, slot={:?}",
                                step,
                                op,
                                expected.slot
                            );

                            // 必须在任何后续尾插发生前验证旧槽位已释放。
                            prop_assert!(
                                arena.get(expected.slot).is_none(),
                                "popped slot still live in arena: step={}, op={:?}, slot={:?}",
                                step,
                                op,
                                expected.slot
                            );

                            model.remove(0);
                        }
                    }

                    LevelOp::Unlink(selector) => {
                        // 解除链接明确不处理头节点。
                        //
                        // `len` == 0/1 时，本操作不调用生产接口。
                        if model.len() >= 2 {
                            let position =
                                1 + (*selector as usize % (model.len() - 1));

                            let expected = model[position].clone();

                            let removed =
                                level.unlink(&mut arena, expected.slot);

                            prop_assert_eq!(
                                node_state(&removed),
                                (
                                    expected.original.clone(),
                                    expected.remaining,
                                    expected.priority,
                                    None,
                                    None,
                                ),
                                "unlink returned wrong node: step={}, op={:?}, position={}, slot={:?}",
                                step,
                                op,
                                position,
                                expected.slot
                            );

                            // 删除当刻旧数值槽位必须为空。
                            //
                            // 下一次尾插可以合法复用相同数值槽位。
                            prop_assert!(
                                arena.get(expected.slot).is_none(),
                                "unlinked slot still live in arena: step={}, op={:?}, position={}, slot={:?}",
                                step,
                                op,
                                position,
                                expected.slot
                            );

                            model.remove(position);
                        }
                    }
                }

                // 优先级的所有权在性质测试模型自身。
                //
                // 尾插恰好消耗一个；PopFront / 解除链接不能消耗。
                let expected_next_priority = if matches!(&op, LevelOp::Push(_)) {
                    priority_before
                        .checked_add(1)
                        .expect("bounded property priority cannot overflow")
                } else {
                    priority_before
                };

                prop_assert_eq!(
                    next_priority,
                    expected_next_priority,
                    "unexpected priority consumption: step={}, op={:?}",
                    step,
                    op
                );

                // 每一步操作结束后，模型必须与完整 PriceLevel 状态一致。
                assert_level_matches_model(
                    &level,
                    &arena,
                    &model,
                    step,
                    &op,
                )?;
            }
        }
    }
}
