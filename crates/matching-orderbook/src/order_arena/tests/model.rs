use super::*;

/// 性质测试模型选择的槽位类别，而非实际 Arena 索引。
#[derive(Debug, Clone, Copy)]
enum SlotTarget {
    /// 模型中当前有效的槽位。
    Live(u8),
    /// 当前模型中已释放但尚未复用的槽位。
    Empty(u8),
    /// 已分配范围以外的受界槽位。
    OutOfBounds(u8),
    /// `u32::MAX` 表示边界转换与越界路径。
    Max,
}

/// 性质测试操作集，覆盖插入、删除及只读访问的生命周期分支。
#[derive(Debug, Clone, Copy)]
enum ArenaAction {
    /// 插入正数量节点；布尔值只选择买入/卖出测试数据。
    Insert { qty: u64, sell: bool },
    /// 对模型选定的类别执行删除。
    Remove(SlotTarget),
    /// 对模型选定的类别执行只读访问。
    Get(SlotTarget),
}

/// 生成有界、偏向有效插入和有效访问的性质测试操作序列。
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
/// 将抽象槽位类别确定性映射为模型当前的具体 `u32` 索引。
///
/// 选择有效/空时使用有序 `BTreeMap`，避免测试模型依赖 HashMap
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

        // 独立模型：不使用 `arena` 返回的索引更新预期。
        let mut live: BTreeMap<u32, NodeState> = BTreeMap::new();

        // release_seq -> 槽位，序号越大表示越晚释放。
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

                    // 按最新释放序号预测后进先出复用。
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

                    // 失败删除不允许改变任何状态。
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

                    // 读取无论命中与否，均不得修改状态。
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

            // 每一步均由独立模型生成完整预期槽位。
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

            // 独立检查空闲与空槽一一对应。
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
