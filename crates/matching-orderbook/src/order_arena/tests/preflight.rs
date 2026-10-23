use super::*;

#[test]
fn preflight_insert_accepts_append_without_mutation() {
    let arena = OrderArena::new();

    let before = arena.test_snapshot();

    assert_eq!(arena.preflight_insert(), Ok(()));

    assert_eq!(arena.test_snapshot(), before);
}
#[test]
fn preflight_insert_accepts_nonempty_append_without_mutation() {
    let mut arena = OrderArena::new();

    arena
        .insert(OrderNode::new(
            order(1, Side::Buy, 100),
            QueuePriority::new(10),
        ))
        .unwrap();

    let before = arena.test_snapshot();

    assert_eq!(arena.preflight_insert(), Ok(()));

    assert_eq!(arena.test_snapshot(), before);
}
#[test]
fn preflight_insert_accepts_lifo_reuse_without_consuming_free_slot() {
    let mut arena = OrderArena::new();

    let first = arena
        .insert(OrderNode::new(
            order(1, Side::Buy, 100),
            QueuePriority::new(10),
        ))
        .unwrap();

    let second = arena
        .insert(OrderNode::new(
            order(2, Side::Buy, 100),
            QueuePriority::new(11),
        ))
        .unwrap();

    arena.remove(first).unwrap();
    arena.remove(second).unwrap();

    let before = arena.test_snapshot();

    assert_eq!(arena.preflight_insert(), Ok(()));
    assert_eq!(arena.test_snapshot(), before);

    // 真正 `insert` 仍然复用最后进入空闲链表的第二个。
    let reused = arena
        .insert(OrderNode::new(
            order(3, Side::Buy, 100),
            QueuePriority::new(12),
        ))
        .unwrap();

    assert_eq!(reused, second);
}

#[test]
fn preflight_insert_panics_on_corrupted_free_tail_without_mutation() {
    for occupied in [false, true] {
        let mut arena = OrderArena::new();

        let live = arena
            .insert(OrderNode::new(
                order(1, Side::Buy, 100),
                QueuePriority::new(10),
            ))
            .unwrap();

        if occupied {
            // 故意把仍已占用的槽位塞进空闲尾节点。
            arena.free.push(live);
        } else {
            arena.free.push(OrderIndex::try_from_usize(99).unwrap());
        }

        let before = arena.test_snapshot();

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = arena.preflight_insert();
        }));

        assert!(result.is_err());

        assert_eq!(arena.test_snapshot(), before);
    }
}
