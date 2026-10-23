use super::*;

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

    assert_partial_fill_panics_without_mutation(&mut level, &mut arena, Qty::try_new(3).unwrap());
}
