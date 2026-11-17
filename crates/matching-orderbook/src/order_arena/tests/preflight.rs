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

    // 实际插入仍复用最后释放的第二个槽位。
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
            // 故意把已占用槽位放到空闲栈顶。
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
