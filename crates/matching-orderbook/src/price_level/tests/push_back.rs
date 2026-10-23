use super::*;

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

            4 => incoming = OrderNode::new(order(2, Side::Sell, 100, 3), QueuePriority::new(11)),

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
