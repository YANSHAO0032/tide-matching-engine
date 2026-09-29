use crate::{OrderIndex, OrderNode};
use matching_domain::RejectReason;
#[cfg(test)]
use std::cell::Cell;

/// 索引式订单 Arena。
///
/// OrderIndex 表示槽位位置，继续追加不会改变已有索引。
/// 空槽由 None 表示，不使用特殊索引值。
#[derive(Debug, Default)]
pub struct OrderArena {
    /// 按 `OrderIndex` 定位的稳定槽位；删除留下 `None`，不会压缩或重排。
    slots: Vec<Option<OrderNode>>,
    /// 已释放空槽的 LIFO 栈；每个元素必须有效且恰好对应一个 `None` 槽位。
    free: Vec<OrderIndex>,
    /// 仅测试中的实际槽位访问计数，不是业务状态，也不参与 replay。
    #[cfg(test)]
    slot_accesses: Cell<usize>,
}

impl OrderArena {
    /// 创建没有槽位和 free-list 条目的空 Arena。
    pub fn new() -> Self {
        Self::default()
    }

    /// 插入订单节点，返回其槽位索引。
    ///
    /// 优先按照 LIFO 顺序复用 free-list 中的空槽；
    /// 没有空槽时，进行 checked 索引转换后追加。
    ///
    /// 复用前必须验证索引有效且槽位为空，
    /// 内部不变量被破坏时直接 panic，不覆盖现存节点。
    ///
    /// 删除后旧逻辑索引失效；复用后，相同的 OrderIndex
    /// 数值会指向新节点。当前尚无 generation 校验。
    pub fn insert(&mut self, node: OrderNode) -> Result<OrderIndex, RejectReason> {
        // LIFO：优先复用最后释放的槽位。
        if let Some(&index) = self.free.last() {
            let position = usize::try_from(index.get()).expect("OrderIndex must fit usize");
            // 验证 free 尾部指向的槽位。
            #[cfg(test)]
            self.record_slot_access();
            // 必须先验证，不能先 pop。
            // 越界或指向 occupied 槽位时，在修改任何状态前 panic。
            assert!(
                matches!(self.slots.get(position), Some(None)),
                "OrderArena invariant violation: free index is invalid or occupied"
            );
            // 只有验证成功，才能消费 free 并写入节点。
            self.free.pop();
            // 写入复用槽位。
            #[cfg(test)]
            self.record_slot_access();
            self.slots[position] = Some(node);
            return Ok(index);
        }

        let index = OrderIndex::try_from_usize(self.slots.len())?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.push(Some(node));
        Ok(index)
    }

    /// 获取指定槽位中的订单节点。
    ///
    /// 索引越界或槽位为空时返回 None；该查询不验证节点链接或其
    /// `PriceLevel` 成员归属。
    pub fn get(&self, index: OrderIndex) -> Option<&OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.get(position)?.as_ref()
    }

    /// 取走节点并将其槽位登记到 free-list。
    ///
    /// 仅成功删除时登记一次，不压缩 slots。
    /// 越界、空槽及尚未复用前的重复删除返回 None，
    /// 并保持 slots 和 free 不变。
    ///
    /// 拥有者必须先解除订单 ID 和链表中的相关引用，
    /// 再释放槽位，避免旧索引在复用后指向其他订单。
    pub fn remove(&mut self, index: OrderIndex) -> Option<OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        // 只有成功取出节点才登记 free。
        let node = self.slots.get_mut(position)?.take()?;

        self.free.push(index);

        Some(node)
    }

    /// 仅供 crate 内部在完成局部不变量预检后维护节点运行态。
    ///
    /// `None` 同时表示越界或已释放槽位；此访问不修复链接，也不验证
    /// 节点属于某个 PriceLevel。
    pub(crate) fn get_mut(&mut self, index: OrderIndex) -> Option<&mut OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.get_mut(position)?.as_mut()
    }

    /// 记录一次真实 slots 读取或写入，仅用于 O(1) 访问路径测试。
    ///
    /// 计数溢出是测试诊断失败，不能改变生产错误语义。
    #[cfg(test)]
    fn record_slot_access(&self) {
        let next = self
            .slot_accesses
            .get()
            .checked_add(1)
            .expect("OrderArena slot access counter overflow");

        self.slot_accesses.set(next);
    }

    /// 将测试访问计数归零；不会触碰 slots 或 free-list。
    #[cfg(test)]
    pub(crate) fn reset_slot_accesses(&self) {
        self.slot_accesses.set(0);
    }

    /// 读取测试访问计数；它不是运行时指标或持久化数据。
    #[cfg(test)]
    pub(crate) fn slot_accesses(&self) -> usize {
        self.slot_accesses.get()
    }
}

#[cfg(test)]
impl OrderArena {
    /// 复制完整 Arena 内存态，用于测试失败原子性。
    ///
    /// 此 helper 不模拟快照、WAL 或 replay，也不会暴露给生产调用方。
    pub(crate) fn test_snapshot(&self) -> (Vec<Option<OrderNode>>, Vec<OrderIndex>) {
        (self.slots.clone(), self.free.clone())
    }
}

#[cfg(test)]
mod tests {
    //! Arena 生命周期、free-list 和常数槽位访问证据。
    //!
    //! 此模块的模型与快照只用于验证实现，不参与生产状态或 replay。

    use super::*;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// 构造具有固定有效价格的最小测试订单。
    fn order(id: u128, side: Side, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将受控测试位置转换为已知可表示的 Arena 槽位。
    fn index(position: usize) -> OrderIndex {
        OrderIndex::try_from_usize(position).unwrap()
    }

    /// 断言一个 live 槽位完整保留订单、数量、priority 和未链接状态。
    fn assert_node(
        arena: &OrderArena,
        index: OrderIndex,
        expected: &LimitGtcOrder,
        priority: QueuePriority,
    ) {
        let node = arena.get(index).expect("node must exist");

        assert_eq!(node.original_order(), expected);
        assert_eq!(node.remaining(), expected.qty.get());
        assert_eq!(node.priority(), priority);
        assert_eq!(node.prev(), None);
        assert_eq!(node.next(), None);
    }

    /// 用于比较槽位内容的完整节点投影，包含所有运行态字段。
    type NodeState = (
        LimitGtcOrder,
        u64,
        QueuePriority,
        Option<OrderIndex>,
        Option<OrderIndex>,
    );

    /// 从节点复制完整测试投影，避免断言遗漏任何字段。
    fn node_state(node: &OrderNode) -> NodeState {
        (
            node.original_order().clone(),
            node.remaining(),
            node.priority(),
            node.prev(),
            node.next(),
        )
    }

    /// 复制 slots 的内容投影，用于验证失败不会改变任何槽位。
    fn snapshot_slots(arena: &OrderArena) -> Vec<Option<NodeState>> {
        arena
            .slots
            .iter()
            .map(|slot| slot.as_ref().map(node_state))
            .collect()
    }

    #[test]
    fn empty_arena_out_of_bounds_and_empty_slot() {
        let empty = OrderArena::new();

        assert!(empty.slots.is_empty());
        assert!(empty.get(index(0)).is_none());
        assert!(empty.get(index(10)).is_none());
        assert!(empty.get(index(u32::MAX as usize)).is_none());

        // 通过真实插入、删除形成空槽。
        let mut arena = OrderArena::new();

        let original = order(1, Side::Buy, 10);
        let priority = QueuePriority::new(7);

        let first = arena
            .insert(OrderNode::new(original.clone(), priority))
            .unwrap();

        assert_eq!(first, index(0));

        let removed = arena.remove(first).unwrap();

        assert_eq!(removed.original_order(), &original);
        assert_eq!(removed.remaining(), 10);
        assert_eq!(removed.priority(), priority);

        // 删除后保留槽位及其编号。
        assert_eq!(arena.slots.len(), 1);
        assert_eq!(arena.free, vec![index(0)]);

        assert!(arena.get(index(0)).is_none());
        assert!(arena.get(index(1)).is_none());
        assert!(arena.get(index(u32::MAX as usize)).is_none());

        // 下次插入应复用空槽 0，而不是追加到 1。
        let replacement = order(2, Side::Sell, 20);
        let replacement_priority = QueuePriority::new(9);

        let reused = arena
            .insert(OrderNode::new(replacement.clone(), replacement_priority))
            .unwrap();

        assert_eq!(reused, index(0));
        assert_eq!(arena.slots.len(), 1);
        assert!(arena.free.is_empty());

        // 新节点的全部状态正确。
        assert_node(&arena, reused, &replacement, replacement_priority);

        assert!(arena.get(index(1)).is_none());
        assert!(arena.get(index(u32::MAX as usize)).is_none());
    }

    #[test]
    fn consecutive_insertions_return_stable_indices() {
        let mut arena = OrderArena::new();

        let cases = [
            (1, Side::Buy, 10, 5),
            (2, Side::Sell, 20, 9),
            (3, Side::Buy, u64::MAX, 15),
        ];

        for (position, (id, side, qty, priority)) in cases.into_iter().enumerate() {
            let original = order(id, side, qty);
            let priority = QueuePriority::new(priority);

            let inserted = arena
                .insert(OrderNode::new(original.clone(), priority))
                .unwrap();

            assert_eq!(inserted.get(), position as u32);
            assert_node(&arena, inserted, &original, priority);
        }

        assert_eq!(arena.slots.len(), 3);
    }

    #[test]
    fn appending_preserves_existing_indices_and_nodes() {
        let mut arena = OrderArena::new();

        let first_order = order(10, Side::Buy, 10);
        let first_priority = QueuePriority::new(3);

        let first = arena
            .insert(OrderNode::new(first_order.clone(), first_priority))
            .unwrap();

        let second_order = order(20, Side::Sell, 20);
        let second_priority = QueuePriority::new(8);

        let second = arena
            .insert(OrderNode::new(second_order.clone(), second_priority))
            .unwrap();

        assert_eq!(first.get(), 0);
        assert_eq!(second.get(), 1);

        // 持续追加，并反复读取已有索引。
        for (expected_index, id, qty) in [(2, 30, 30), (3, 40, u64::MAX), (4, 50, 5)] {
            let original = order(id, Side::Buy, qty);
            let priority = QueuePriority::new(expected_index);

            let inserted = arena
                .insert(OrderNode::new(original.clone(), priority))
                .unwrap();

            assert_eq!(inserted.get(), expected_index as u32);

            for _ in 0..3 {
                assert_node(&arena, first, &first_order, first_priority);

                assert_node(&arena, second, &second_order, second_priority);

                assert_node(&arena, inserted, &original, priority);
            }
        }

        assert_eq!(arena.slots.len(), 5);
    }

    #[test]
    fn identical_insertions_produce_identical_indices_and_content() {
        let mut first = OrderArena::new();
        let mut second = OrderArena::new();

        let cases = [
            (10, Side::Buy, 7, 0),
            (20, Side::Sell, 15, 1),
            (30, Side::Buy, u64::MAX, 2),
        ];

        for (position, (id, side, qty, priority)) in cases.into_iter().enumerate() {
            let original = order(id, side, qty);
            let priority = QueuePriority::new(priority);

            let first_index = first
                .insert(OrderNode::new(original.clone(), priority))
                .unwrap();

            let second_index = second
                .insert(OrderNode::new(original.clone(), priority))
                .unwrap();

            assert_eq!(first_index, second_index);
            assert_eq!(first_index.get(), position as u32);

            assert_node(&first, first_index, &original, priority);

            assert_node(&second, second_index, &original, priority);
        }

        assert_eq!(first.slots.len(), second.slots.len());
    }

    #[test]
    fn remove_returns_complete_node_and_preserves_other_slots() {
        // 单节点删除。
        let mut single = OrderArena::new();
        let original = order(1, Side::Buy, 10);

        let idx = single
            .insert(OrderNode::new(original, QueuePriority::new(7)))
            .unwrap();

        let before = snapshot_slots(&single);
        let removed = single.remove(idx).unwrap();

        assert_eq!(node_state(&removed), before[0].clone().unwrap());
        assert_eq!(single.slots.len(), 1);
        assert_eq!(snapshot_slots(&single), vec![None]);

        // 每次重新构造三个节点，分别删除首、中、尾。
        for remove_at in 0..3 {
            let mut arena = OrderArena::new();

            for (id, side, qty, priority) in [
                (30, Side::Buy, 10, 5),
                (10, Side::Sell, 20, 8),
                (20, Side::Buy, u64::MAX, 13),
            ] {
                arena
                    .insert(OrderNode::new(
                        order(id, side, qty),
                        QueuePriority::new(priority),
                    ))
                    .unwrap();
            }

            let before = snapshot_slots(&arena);
            let removed = arena.remove(index(remove_at)).unwrap();

            // 返回完整原节点，包括原订单、remaining、priority 和链接。
            assert_eq!(node_state(&removed), before[remove_at].clone().unwrap());

            let mut expected = before.clone();
            expected[remove_at] = None;

            assert_eq!(snapshot_slots(&arena), expected);
            assert_eq!(arena.slots.len(), 3);

            // 其余索引仍可读取原节点。
            for (position, expected_node) in before.iter().enumerate() {
                if position != remove_at {
                    assert_eq!(
                        node_state(arena.get(index(position)).unwrap()),
                        expected_node.clone().unwrap()
                    );
                }
            }
        }
    }
    #[test]
    fn invalid_remove_preserves_entire_arena() {
        let max_index = index(u32::MAX as usize);

        // 空 Arena。
        let mut empty = OrderArena::new();
        let empty_before = snapshot_slots(&empty);

        for invalid in [index(0), max_index] {
            assert!(empty.get(invalid).is_none());
            assert!(empty.remove(invalid).is_none());
            assert_eq!(snapshot_slots(&empty), empty_before);
        }

        // 构造包含真实空槽的 Arena。
        let mut arena = OrderArena::new();

        for id in [1, 2] {
            arena
                .insert(OrderNode::new(
                    order(id, Side::Buy, 10),
                    QueuePriority::new(id),
                ))
                .unwrap();
        }

        // 第一次删除成功，后续重复删除不得修改状态。
        arena.remove(index(0)).unwrap();

        let before = snapshot_slots(&arena);

        // 空槽、越界和最大 u32 索引。
        for invalid in [index(0), index(2), max_index] {
            assert!(arena.get(invalid).is_none());
            assert!(arena.remove(invalid).is_none());

            assert_eq!(snapshot_slots(&arena), before);
            assert_eq!(arena.slots.len(), 2);
        }

        // 未删除的节点仍然完整。
        assert_eq!(
            node_state(arena.get(index(1)).unwrap()),
            before[1].clone().unwrap()
        );
    }
    #[test]
    fn insert_after_remove_reuses_hole() {
        let mut arena = OrderArena::new();

        for id in [10, 20, 30] {
            arena
                .insert(OrderNode::new(
                    order(id, Side::Buy, 10),
                    QueuePriority::new(id),
                ))
                .unwrap();
        }

        let before = snapshot_slots(&arena);

        arena.remove(index(1)).unwrap();
        assert_eq!(arena.free, vec![index(1)]);

        let replacement = OrderNode::new(order(40, Side::Sell, 20), QueuePriority::new(40));
        let replacement_state = node_state(&replacement);

        let reused = arena.insert(replacement).unwrap();

        assert_eq!(reused, index(1));
        assert!(arena.free.is_empty());
        assert_eq!(arena.slots.len(), 3);

        // 相同数值的旧索引现在指向新节点。
        assert_eq!(node_state(arena.get(reused).unwrap()), replacement_state);

        // 其他存活节点保持不变。
        assert_eq!(snapshot_slots(&arena)[0], before[0]);
        assert_eq!(snapshot_slots(&arena)[2], before[2]);

        // free 用尽后恢复追加。
        let appended = arena
            .insert(OrderNode::new(
                order(50, Side::Buy, 5),
                QueuePriority::new(50),
            ))
            .unwrap();

        assert_eq!(appended, index(3));
        assert_eq!(arena.slots.len(), 4);
    }
    /// 每个 None 槽位必须在 free 中恰好出现一次。
    /// occupied 槽位不能出现在 free 中。
    fn assert_free_bijection(arena: &OrderArena) {
        let mut seen = vec![0usize; arena.slots.len()];

        for &index in &arena.free {
            let position = usize::try_from(index.get()).unwrap();

            assert!(position < arena.slots.len());
            assert!(arena.slots[position].is_none());

            seen[position] += 1;
            assert_eq!(seen[position], 1);
        }

        for (position, slot) in arena.slots.iter().enumerate() {
            assert_eq!(
                seen[position] == 1,
                slot.is_none(),
                "slot/free mismatch at {position}"
            );
        }
    }
    #[test]
    fn multiple_holes_are_reused_in_lifo_order() {
        let mut arena = OrderArena::new();

        for id in [10, 20, 30, 40] {
            arena
                .insert(OrderNode::new(
                    order(id, Side::Buy, 10),
                    QueuePriority::new(id),
                ))
                .unwrap();
        }

        let before = snapshot_slots(&arena);

        arena.remove(index(1)).unwrap();
        arena.remove(index(3)).unwrap();

        assert_eq!(arena.free, vec![index(1), index(3)]);
        assert_free_bijection(&arena);

        // 重复删除不能重复登记。
        let free_before = arena.free.clone();
        let slots_before = snapshot_slots(&arena);

        assert!(arena.remove(index(3)).is_none());
        assert_eq!(arena.free, free_before);
        assert_eq!(snapshot_slots(&arena), slots_before);

        // 先复用最后释放的 3，再复用 1。
        let first = arena
            .insert(OrderNode::new(
                order(50, Side::Sell, 5),
                QueuePriority::new(50),
            ))
            .unwrap();

        assert_eq!(first, index(3));
        assert_eq!(arena.free, vec![index(1)]);
        assert_free_bijection(&arena);

        let second = arena
            .insert(OrderNode::new(
                order(60, Side::Sell, 6),
                QueuePriority::new(60),
            ))
            .unwrap();

        assert_eq!(second, index(1));
        assert!(arena.free.is_empty());
        assert_free_bijection(&arena);

        // free 用尽后才追加。
        let third = arena
            .insert(OrderNode::new(
                order(70, Side::Buy, 7),
                QueuePriority::new(70),
            ))
            .unwrap();

        assert_eq!(third, index(4));
        assert_eq!(arena.slots.len(), 5);

        // 存活节点没有被覆盖或移动。
        let after = snapshot_slots(&arena);
        assert_eq!(after[0], before[0]);
        assert_eq!(after[2], before[2]);
        assert_free_bijection(&arena);
    }

    #[test]
    fn invalid_removal_preserves_slots_and_free() {
        let mut arena = OrderArena::new();

        // 空 arena。
        assert!(arena.remove(index(0)).is_none());
        assert!(arena.remove(index(u32::MAX as usize)).is_none());
        assert!(arena.free.is_empty());

        arena
            .insert(OrderNode::new(
                order(1, Side::Buy, 10),
                QueuePriority::new(0),
            ))
            .unwrap();

        arena.remove(index(0)).unwrap();

        let slots_before = snapshot_slots(&arena);
        let free_before = arena.free.clone();

        // 空槽、重复删除和越界。
        for invalid in [index(0), index(1), index(u32::MAX as usize)] {
            assert!(arena.remove(invalid).is_none());

            assert_eq!(snapshot_slots(&arena), slots_before);
            assert_eq!(arena.free, free_before);
        }

        assert_free_bijection(&arena);
    }

    #[test]
    fn corrupted_free_must_panic_without_overwriting_nodes() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        // 分别制造 occupied 和越界的 free 索引。
        for bad_index in [index(0), index(99)] {
            let mut arena = OrderArena::new();

            arena
                .insert(OrderNode::new(
                    order(1, Side::Buy, 10),
                    QueuePriority::new(0),
                ))
                .unwrap();

            arena.free.push(bad_index);

            let slots_before = snapshot_slots(&arena);
            let free_before = arena.free.clone();

            let result = catch_unwind(AssertUnwindSafe(|| {
                arena.insert(OrderNode::new(
                    order(2, Side::Sell, 20),
                    QueuePriority::new(1),
                ))
            }));

            assert!(result.is_err());
            assert_eq!(snapshot_slots(&arena), slots_before);
            assert_eq!(arena.free, free_before);
        }
    }
    #[test]
    fn identical_mixed_operations_are_deterministic() {
        // 用相同操作带独立运行两次，比较可观察 Arena 状态而非实现细节。
        fn run() -> (Vec<OrderIndex>, Vec<Option<NodeState>>, Vec<OrderIndex>) {
            let mut arena = OrderArena::new();
            let mut allocated = Vec::new();

            for id in [10, 20, 30, 40] {
                allocated.push(
                    arena
                        .insert(OrderNode::new(
                            order(id, Side::Buy, 10),
                            QueuePriority::new(id),
                        ))
                        .unwrap(),
                );
            }

            arena.remove(index(1)).unwrap();
            arena.remove(index(3)).unwrap();
            assert_free_bijection(&arena);

            for id in [50, 60] {
                allocated.push(
                    arena
                        .insert(OrderNode::new(
                            order(id, Side::Sell, 20),
                            QueuePriority::new(id),
                        ))
                        .unwrap(),
                );
                assert_free_bijection(&arena);
            }

            arena.remove(index(0)).unwrap();
            allocated.push(
                arena
                    .insert(OrderNode::new(
                        order(70, Side::Buy, 30),
                        QueuePriority::new(70),
                    ))
                    .unwrap(),
            );

            assert_free_bijection(&arena);

            (allocated, snapshot_slots(&arena), arena.free.clone())
        }

        let first = run();
        let second = run();

        assert_eq!(first, second);

        // 初始追加 0..3；LIFO 复用 3、1、0。
        assert_eq!(
            first.0,
            vec![
                index(0),
                index(1),
                index(2),
                index(3),
                index(3),
                index(1),
                index(0),
            ]
        );
    }

    /// property 模型选择的槽位类别，而非实际 Arena index。
    #[derive(Debug, Clone, Copy)]
    enum SlotTarget {
        /// 当前模型中的 live 槽位。
        Live(u8),
        /// 当前模型中已释放但尚未复用的槽位。
        Empty(u8),
        /// 已分配范围以外的受界槽位。
        OutOfBounds(u8),
        /// `u32::MAX` 表示边界转换与越界路径。
        Max,
    }

    /// property 操作集，覆盖插入、删除及只读访问的生命周期分支。
    #[derive(Debug, Clone, Copy)]
    enum ArenaAction {
        /// 插入正数量节点；布尔值只选择 Buy/Sell fixture。
        Insert { qty: u64, sell: bool },
        /// 对模型选定的类别执行删除。
        Remove(SlotTarget),
        /// 对模型选定的类别执行只读访问。
        Get(SlotTarget),
    }

    /// 生成有界、偏向有效插入和 live 访问的 property 操作序列。
    fn action_strategy() -> impl Strategy<Value = ArenaAction> {
        prop_oneof![
            5 => (1u64..=100, any::<bool>())
                .prop_map(|(qty, sell)| ArenaAction::Insert { qty, sell }),

            3 => any::<u8>()
                .prop_map(|n| ArenaAction::Remove(SlotTarget::Live(n))),

            2 => any::<u8>()
                .prop_map(|n| ArenaAction::Remove(SlotTarget::Empty(n))),

            1 => (0u8..=8)
                .prop_map(|n| ArenaAction::Remove(SlotTarget::OutOfBounds(n))),

            3 => any::<u8>()
                .prop_map(|n| ArenaAction::Get(SlotTarget::Live(n))),

            2 => any::<u8>()
                .prop_map(|n| ArenaAction::Get(SlotTarget::Empty(n))),

            1 => (0u8..=8)
                .prop_map(|n| ArenaAction::Get(SlotTarget::OutOfBounds(n))),

            1 => Just(ArenaAction::Get(SlotTarget::Max)),
        ]
    }
    /// 将抽象槽位类别确定性映射为模型当前的具体 `u32` index。
    ///
    /// 选择 live/empty 时使用有序 `BTreeMap`，避免测试模型依赖 HashMap
    /// 迭代顺序；没有对应项时故意返回越界值以覆盖 `None` 分支。
    fn model_target(
        target: SlotTarget,
        live: &BTreeMap<u32, NodeState>,
        released: &BTreeMap<u64, u32>,
        allocated: usize,
    ) -> u32 {
        let invalid = allocated as u32 + 1;

        match target {
            SlotTarget::Live(n) => {
                if live.is_empty() {
                    invalid
                } else {
                    *live.keys().nth(n as usize % live.len()).unwrap()
                }
            }

            SlotTarget::Empty(n) => {
                if released.is_empty() {
                    invalid
                } else {
                    *released.values().nth(n as usize % released.len()).unwrap()
                }
            }

            SlotTarget::OutOfBounds(offset) => invalid + u32::from(offset),

            SlotTarget::Max => u32::MAX,
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn prop_arena_lifecycle_matches_model(
            random_actions in prop::collection::vec(
                action_strategy(),
                0..=118,
            ),
        ) {
            let mut arena = OrderArena::new();

            // 独立模型：不使用 arena 返回的索引更新预期。
            let mut live: BTreeMap<u32, NodeState> = BTreeMap::new();

            // release_seq -> slot，序号越大表示越晚释放。
            let mut released: BTreeMap<u64, u32> = BTreeMap::new();

            let mut allocated = 0usize;
            let mut release_seq = 0u64;

            // 保证每组测试实际覆盖三种访问状态。
            let mut actions = vec![
                ArenaAction::Insert {
                    qty: 10,
                    sell: false,
                },
                ArenaAction::Insert {
                    qty: 20,
                    sell: true,
                },
                ArenaAction::Remove(SlotTarget::Live(0)),
                ArenaAction::Get(SlotTarget::Empty(0)),
                ArenaAction::Get(SlotTarget::Live(0)),
                ArenaAction::Get(SlotTarget::OutOfBounds(0)),
                ArenaAction::Remove(SlotTarget::Empty(0)),
                ArenaAction::Remove(SlotTarget::OutOfBounds(0)),
                ArenaAction::Insert {
                    qty: 30,
                    sell: false,
                },
                ArenaAction::Get(SlotTarget::Live(0)),
            ];

            actions.extend(random_actions);

            for (step, action) in actions.into_iter().enumerate() {
                let slots_before = snapshot_slots(&arena);
                let free_before = arena.free.clone();

                match action {
                    ArenaAction::Insert { qty, sell } => {
                        let side = if sell {
                            Side::Sell
                        } else {
                            Side::Buy
                        };

                        let original = order(
                            10_000 + step as u128,
                            side,
                            qty,
                        );

                        let priority = QueuePriority::new(step as u128);

                        // 手工构建预期节点，不读取 Arena。
                        let expected_node: NodeState = (
                            original.clone(),
                            qty,
                            priority,
                            None,
                            None,
                        );

                        // 按最新释放序号预测 LIFO 复用。
                        let expected_position =
                            if let Some((&seq, &position)) =
                                released.iter().next_back()
                            {
                                released.remove(&seq);
                                position
                            } else {
                                let position = allocated as u32;
                                allocated += 1;
                                position
                            };

                        live.insert(
                            expected_position,
                            expected_node,
                        );

                        let actual = arena.insert(
                            OrderNode::new(original, priority)
                        );

                        prop_assert_eq!(
                            actual,
                            Ok(index(expected_position as usize)),
                            "step={} action={:?}",
                            step,
                            action
                        );
                    }

                    ArenaAction::Remove(target) => {
                        let position = model_target(
                            target,
                            &live,
                            &released,
                            allocated,
                        );

                        // 预期结果完全取自独立模型。
                        let expected = live.remove(&position);
                        let succeeded = expected.is_some();

                        if succeeded {
                            released.insert(release_seq, position);
                            release_seq += 1;
                        }

                        let actual = arena.remove(
                            index(position as usize)
                        );

                        let actual_state =
                            actual.as_ref().map(node_state);

                        prop_assert_eq!(
                            &actual_state,
                            &expected,
                            "step={} action={:?}",
                            step,
                            action
                        );

                        // 失败 Remove 不允许改变任何状态。
                        if !succeeded {
                            prop_assert_eq!(
                                snapshot_slots(&arena),
                                slots_before,
                                "failed remove at step={}",
                                step
                            );

                            prop_assert_eq!(
                                &arena.free,
                                &free_before,
                                "failed remove at step={}",
                                step
                            );
                        }
                    }

                    ArenaAction::Get(target) => {
                        let position = model_target(
                            target,
                            &live,
                            &released,
                            allocated,
                        );

                        let expected = live.get(&position).cloned();

                        let actual = arena
                            .get(index(position as usize))
                            .map(node_state);

                        prop_assert_eq!(
                            &actual,
                            &expected,
                            "step={} action={:?}",
                            step,
                            action
                        );

                        // Get 无论命中与否，均不得修改状态。
                        prop_assert_eq!(
                            snapshot_slots(&arena),
                            slots_before,
                            "get mutated slots at step={}",
                            step
                        );

                        prop_assert_eq!(
                            &arena.free,
                            &free_before,
                            "get mutated free at step={}",
                            step
                        );
                    }
                }

                // 每一步均由独立模型生成完整预期 slots。
                let expected_slots: Vec<Option<NodeState>> =
                    (0..allocated)
                        .map(|position| {
                            live.get(&(position as u32)).cloned()
                        })
                        .collect();

                let expected_free: Vec<OrderIndex> =
                    released
                        .values()
                        .map(|&position| index(position as usize))
                        .collect();

                prop_assert_eq!(
                    snapshot_slots(&arena),
                    expected_slots,
                    "slots mismatch at step={} action={:?}",
                    step,
                    action
                );

                prop_assert_eq!(
                    arena.slots.len(),
                    allocated,
                    "length mismatch at step={} action={:?}",
                    step,
                    action
                );

                prop_assert_eq!(
                    &arena.free,
                    &expected_free,
                    "free order mismatch at step={} action={:?}",
                    step,
                    action
                );

                // 独立检查 free 与空槽一一对应。
                let mut free_positions = BTreeSet::new();

                for &free_index in &arena.free {
                    let position = free_index.get() as usize;

                    prop_assert!(
                        position < arena.slots.len(),
                        "free out of bounds at step={}",
                        step
                    );

                    prop_assert!(
                        free_positions.insert(position),
                        "duplicate free index at step={}",
                        step
                    );

                    prop_assert!(
                        arena.slots[position].is_none(),
                        "occupied slot in free at step={}",
                        step
                    );
                }

                for (position, slot) in
                    arena.slots.iter().enumerate()
                {
                    prop_assert_eq!(
                        slot.is_none(),
                        free_positions.contains(&position),
                        "slot/free mismatch at step={} slot={}",
                        step,
                        position
                    );
                }
            }
        }
    }
    #[test]
    fn slot_access_counter_tracks_actual_operations() {
        let mut arena = OrderArena::new();

        let first = arena
            .insert(OrderNode::new(
                order(1, Side::Buy, 10),
                QueuePriority::new(0),
            ))
            .unwrap();

        arena.reset_slot_accesses();

        assert!(arena.get(first).is_some());
        assert!(arena.get_mut(first).is_some());
        assert!(arena.remove(first).is_some());
        assert!(arena.get(first).is_none());
        assert!(arena.remove(first).is_none());

        assert_eq!(arena.slot_accesses(), 5);

        // 复用槽位：验空一次，写入一次。
        arena.reset_slot_accesses();

        assert_eq!(
            arena
                .insert(OrderNode::new(
                    order(2, Side::Sell, 20),
                    QueuePriority::new(1),
                ))
                .unwrap(),
            first
        );

        assert_eq!(arena.slot_accesses(), 2);

        // free 耗尽后，追加新槽只访问一次。
        arena.reset_slot_accesses();

        let second = arena
            .insert(OrderNode::new(
                order(3, Side::Buy, 30),
                QueuePriority::new(2),
            ))
            .unwrap();

        assert_eq!(second.get(), 1);
        assert_eq!(arena.slot_accesses(), 1);
    }
}
