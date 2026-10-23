use super::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn mixed_push_pop_unlink_matches_fifo_model(
        buy_side in any::<bool>(),
        price in 1i64..=1_000_000i64,
        ops in prop::collection::vec(level_op_strategy(), 0..=96),
    ) {
        let side = if buy_side {
            Side::Buy
        } else {
            Side::Sell
        };

        let mut arena = OrderArena::new();
        let mut level = PriceLevel::new();

        // 独立先进先出模型。
        let mut model = Vec::<ModelEntry>::new();

        // 每个性质测试 `case` 自己维护有界标识符 / 优先级。
        //
        // 最多只有 96 次尾插，因此不会接近 u128 边界。
        let mut next_id = 1u128;
        let mut next_priority = 1u128;

        prop_assert_eq!(validate_level(&level, &arena), Ok(()));

        for (step, op) in ops.into_iter().enumerate() {
            let priority_before = next_priority;

            match &op {
                LevelOp::Push(qty) => {
                    let following_id = next_id
                        .checked_add(1)
                        .expect("bounded property id cannot overflow");

                    let following_priority = next_priority
                        .checked_add(1)
                        .expect("bounded property priority cannot overflow");

                    let original = order(next_id, side, price, *qty);
                    let priority = QueuePriority::new(next_priority);

                    let inserted = level.push_back(
                        &mut arena,
                        OrderNode::new(original.clone(), priority),
                    );

                    prop_assert!(
                        inserted.is_ok(),
                        "push_back unexpectedly failed: step={}, op={:?}, result={:?}",
                        step,
                        op,
                        inserted
                    );

                    let slot = inserted.unwrap();

                    model.push(ModelEntry {
                        original,
                        remaining: *qty,
                        priority,
                        slot,
                    });

                    // 标识符 / 优先级只在成功尾插后提交递增。
                    next_id = following_id;
                    next_priority = following_priority;
                }

                LevelOp::PopFront => {
                    if model.is_empty() {
                        // 合法空档移除头节点必须保持全部状态不变。
                        let level_before = level_state(&level);
                        let arena_before = arena.test_snapshot();

                        let removed = level.pop_front(&mut arena);

                    prop_assert!(
                        removed.is_none(),
                        "empty pop_front returned a node: step={}, op={:?}",
                        step,
                        op
                    );

                    prop_assert_eq!(
                        level_state(&level),
                        level_before,
                        "empty pop_front mutated level: step={}, op={:?}",
                        step,
                        op
                    );
                    prop_assert_eq!(
                        arena.test_snapshot(),
                        arena_before,
                        "empty pop_front mutated arena/free-list: step={}, op={:?}",
                        step,
                        op
                    );
                    } else {
                        let expected = model[0].clone();

                        let removed = level.pop_front(&mut arena);

                        prop_assert!(
                            removed.is_some(),
                            "non-empty pop_front returned None: step={}, op={:?}",
                            step,
                            op
                        );

                        let removed = removed.unwrap();

                        prop_assert_eq!(
                            node_state(&removed),
                            (
                                expected.original.clone(),
                                expected.remaining,
                                expected.priority,
                                None,
                                None,
                            ),
                            "pop_front returned wrong node: step={}, op={:?}, slot={:?}",
                            step,
                            op,
                            expected.slot
                        );

                        // 必须在任何后续尾插发生前验证旧槽位已释放。
                        prop_assert!(
                            arena.get(expected.slot).is_none(),
                            "popped slot still live in arena: step={}, op={:?}, slot={:?}",
                            step,
                            op,
                            expected.slot
                        );

                        model.remove(0);
                    }
                }

                LevelOp::Unlink(selector) => {
                    // 解除链接明确不处理头节点。
                    //
                    // `len` == 0/1 时，本操作不调用生产接口。
                    if model.len() >= 2 {
                        let position =
                            1 + (*selector as usize % (model.len() - 1));

                        let expected = model[position].clone();

                        let removed =
                            level.unlink(&mut arena, expected.slot);

                        prop_assert_eq!(
                            node_state(&removed),
                            (
                                expected.original.clone(),
                                expected.remaining,
                                expected.priority,
                                None,
                                None,
                            ),
                            "unlink returned wrong node: step={}, op={:?}, position={}, slot={:?}",
                            step,
                            op,
                            position,
                            expected.slot
                        );

                        // 删除当刻旧数值槽位必须为空。
                        //
                        // 下一次尾插可以合法复用相同数值槽位。
                        prop_assert!(
                            arena.get(expected.slot).is_none(),
                            "unlinked slot still live in arena: step={}, op={:?}, position={}, slot={:?}",
                            step,
                            op,
                            position,
                            expected.slot
                        );

                        model.remove(position);
                    }
                }
            }

            // 优先级的所有权在性质测试模型自身。
            //
            // 尾插恰好消耗一个；PopFront / 解除链接不能消耗。
            let expected_next_priority = if matches!(&op, LevelOp::Push(_)) {
                priority_before
                    .checked_add(1)
                    .expect("bounded property priority cannot overflow")
            } else {
                priority_before
            };

            prop_assert_eq!(
                next_priority,
                expected_next_priority,
                "unexpected priority consumption: step={}, op={:?}",
                step,
                op
            );

            // 每一步操作结束后，模型必须与完整 PriceLevel 状态一致。
            assert_level_matches_model(
                &level,
                &arena,
                &model,
                step,
                &op,
            )?;
        }
    }
}
