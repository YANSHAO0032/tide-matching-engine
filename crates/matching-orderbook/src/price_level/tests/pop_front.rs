use super::*;

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
