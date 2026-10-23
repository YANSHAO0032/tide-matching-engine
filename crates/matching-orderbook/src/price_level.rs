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
#[path = "price_level/tests.rs"]
mod tests;
