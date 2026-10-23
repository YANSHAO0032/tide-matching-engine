use super::*;

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

        // 返回完整原节点，包括原订单、剩余量、优先级和链接。
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

    // 空闲用尽后恢复追加。
    let appended = arena
        .insert(OrderNode::new(
            order(50, Side::Buy, 5),
            QueuePriority::new(50),
        ))
        .unwrap();

    assert_eq!(appended, index(3));
    assert_eq!(arena.slots.len(), 4);
}
/// 每个 None 槽位必须在空闲中恰好出现一次。
/// 已占用槽位不能出现在空闲中。
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

    // 空闲用尽后才追加。
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

    // 空 `arena`。
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

    // 分别制造已占用和越界的空闲索引。
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

    // 初始追加 0..3；后进先出复用 3、1、0。
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
