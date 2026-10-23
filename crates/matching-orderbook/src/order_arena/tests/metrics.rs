use super::*;

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

    // 空闲耗尽后，追加新槽只访问一次。
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
