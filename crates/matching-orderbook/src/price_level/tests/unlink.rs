use super::*;

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
