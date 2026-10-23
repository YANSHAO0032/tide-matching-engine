use super::*;
use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn empty_price_level_initializes_all_fields() {
    let arena = OrderArena::new();
    for level in [PriceLevel::new(), PriceLevel::default()] {
        assert_eq!(level.head(), None);
        assert_eq!(level.tail(), None);
        assert_eq!(level.count(), 0);
        assert_eq!(level.total_visible_qty(), 0);
        assert!(level.is_empty());
        assert_eq!(validate_level(&level, &arena), Ok(()));
    }
}
use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};

/// 将受控测试位置转换为已知可表示的 Arena 槽位。
pub(super) fn index(position: usize) -> OrderIndex {
    OrderIndex::try_from_usize(position).unwrap()
}

/// 构造具有显式方向、价格和正数量的最小测试订单。
pub(super) fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 复制 PriceLevel 的全部元数据，用于失败原子性比较。
pub(super) fn level_state(
    level: &PriceLevel,
) -> (Option<OrderIndex>, Option<OrderIndex>, u32, u64) {
    (
        level.head(),
        level.tail(),
        level.count(),
        level.total_visible_qty(),
    )
}

/// 复制节点的完整可观察状态，包括两条侵入式链接。
pub(super) fn node_state(
    node: &OrderNode,
) -> (
    LimitGtcOrder,
    u64,
    QueuePriority,
    Option<OrderIndex>,
    Option<OrderIndex>,
) {
    (
        node.original_order().clone(),
        node.remaining(),
        node.priority(),
        node.prev(),
        node.next(),
    )
}
pub(super) fn assert_partial_fill_panics_without_mutation(
    level: &mut PriceLevel,
    arena: &mut OrderArena,
    fill: Qty,
) {
    let level_before = level_state(level);
    let arena_before = arena.test_snapshot();

    let result = catch_unwind(AssertUnwindSafe(|| {
        level.apply_head_partial_fill(arena, fill);
    }));

    assert!(result.is_err(), "invalid partial fill must fail closed");

    assert_eq!(
        level_state(level),
        level_before,
        "failed partial fill mutated level"
    );

    assert_eq!(
        arena.test_snapshot(),
        arena_before,
        "failed partial fill mutated Arena slots/free-list"
    );
}
/// 独立 Vec 先进先出模型生成的单步结构操作。
#[derive(Debug, Clone)]
pub(super) enum LevelOp {
    /// 在尾部追加给定正数量的同价节点。
    Push(u64),
    /// 移除当前先进先出头；空档时应保持不变并返回 `None`。
    PopFront,
    /// 选择非头逻辑位置的选择器，由模型映射为中间或尾节点。
    Unlink(u16),
}

/// 独立先进先出模型中一个仍应存在的逻辑成员。
#[derive(Debug, Clone)]
pub(super) struct ModelEntry {
    /// 原始订单载荷，用于验证复用槽位没有改变逻辑成员。
    pub(super) original: LimitGtcOrder,
    /// 该成员当前应计入档位聚合的数量。
    pub(super) remaining: u64,
    /// 模型分配的严格递增优先级。
    pub(super) priority: QueuePriority,
    /// 真实操作返回的有效槽位，用于与 Arena 集成状态比对。
    pub(super) slot: OrderIndex,
}

/// 生成有界混合操作；权重保证有足够的非空先进先出状态可验证。
pub(super) fn level_op_strategy() -> impl Strategy<Value = LevelOp> {
    prop_oneof![
        // 尾插略微提高权重，否则大量空档移除头节点/解除链接的覆盖价值较低。
        5 => (1u16..=1000u16).prop_map(|qty| LevelOp::Push(qty as u64)),
        3 => Just(LevelOp::PopFront),
        3 => any::<u16>().prop_map(LevelOp::Unlink),
    ]
}

/// 比较独立 Vec 模型与真实价档/Arena 的完整可观察结构。
///
/// 每一步同时检查元数据、每个有效节点的载荷/双链、严格优先级
/// 和测试校验器，以便缩减后仍能指出首个偏离操作。
pub(super) fn assert_level_matches_model(
    level: &PriceLevel,
    arena: &OrderArena,
    model: &[ModelEntry],
    step: usize,
    op: &LevelOp,
) -> Result<(), TestCaseError> {
    let expected_count =
        u32::try_from(model.len()).expect("property model length is bounded to <= 96");

    let expected_total = model
        .iter()
        .try_fold(0u64, |total, entry| total.checked_add(entry.remaining));

    let Some(expected_total) = expected_total else {
        return Err(TestCaseError::fail(format!(
            "model quantity overflow: step={step}, op={op:?}"
        )));
    };

    prop_assert_eq!(
        level.count(),
        expected_count,
        "count mismatch: step={}, op={:?}",
        step,
        op
    );

    prop_assert_eq!(
        level.total_visible_qty(),
        expected_total,
        "total_visible_qty mismatch: step={}, op={:?}",
        step,
        op
    );

    prop_assert_eq!(
        level.head(),
        model.first().map(|entry| entry.slot),
        "head mismatch: step={}, op={:?}",
        step,
        op
    );

    prop_assert_eq!(
        level.tail(),
        model.last().map(|entry| entry.slot),
        "tail mismatch: step={}, op={:?}",
        step,
        op
    );

    prop_assert_eq!(
        level.is_empty(),
        model.is_empty(),
        "is_empty mismatch: step={}, op={:?}",
        step,
        op
    );

    for (position, entry) in model.iter().enumerate() {
        let Some(node) = arena.get(entry.slot) else {
            return Err(TestCaseError::fail(format!(
                "live model slot missing from arena: \
             step={step}, op={op:?}, position={position}, slot={:?}",
                entry.slot
            )));
        };

        let expected_prev = if position == 0 {
            None
        } else {
            Some(model[position - 1].slot)
        };

        let expected_next = model.get(position + 1).map(|entry| entry.slot);

        prop_assert_eq!(
            node_state(node),
            (
                entry.original.clone(),
                entry.remaining,
                entry.priority,
                expected_prev,
                expected_next,
            ),
            "node mismatch: step={}, op={:?}, position={}, slot={:?}",
            step,
            op,
            position,
            entry.slot
        );
    }

    prop_assert_eq!(
        validate_level(level, arena),
        Ok(()),
        "validate_level failed: step={}, op={:?}",
        step,
        op
    );

    Ok(())
}

/// 验证单价先进先出的全链结构，返回可读失败原因而不是触发不变量断言。
///
/// 该测试辅助从头节点正向和尾节点反向遍历，核对端点、双链、节点数、
/// 可见数量、同方向/价格和严格递增优先级；无关有效 Arena
/// 槽位不属于此档位，故不参与检查。
pub(super) fn validate_level(level: &PriceLevel, arena: &OrderArena) -> Result<(), String> {
    // 节点数是空档的权威判断。
    if level.count() == 0 {
        if level.head().is_some() || level.tail().is_some() || level.total_visible_qty() != 0 {
            return Err("empty level has inconsistent endpoints or quantity".into());
        }

        return Ok(());
    }

    let head = level
        .head()
        .ok_or_else(|| "non-empty level is missing head".to_string())?;

    let tail = level
        .tail()
        .ok_or_else(|| "non-empty level is missing tail".to_string())?;

    if level.total_visible_qty() == 0 {
        return Err("non-empty level has zero total_visible_qty".into());
    }

    // ---------- 正向遍历: 头节点 -> `next` ----------

    let mut forward = Vec::new();
    let mut visited = BTreeSet::new();

    let mut current = Some(head);
    let mut expected_prev = None;

    let mut actual_count = 0u32;
    let mut actual_qty = 0u64;

    let mut expected_side = None;
    let mut expected_price = None;
    let mut previous_priority = None;

    while let Some(index) = current {
        if !visited.insert(index) {
            return Err(format!(
                "cycle or duplicate index in forward chain: {}",
                index.get()
            ));
        }

        let node = arena
            .get(index)
            .ok_or_else(|| format!("forward chain references missing node: {}", index.get()))?;

        // 当前节点必须反向指回真实前驱节点。
        if node.prev() != expected_prev {
            return Err(format!("broken prev link at index {}", index.get()));
        }

        if node.remaining() == 0 {
            return Err(format!("zero remaining node at index {}", index.get()));
        }

        let order = node.original_order();

        match expected_side {
            None => expected_side = Some(order.side),
            Some(side) if side != order.side => {
                return Err(format!("side mismatch at index {}", index.get()));
            }
            Some(_) => {}
        }

        match expected_price {
            None => expected_price = Some(order.price),
            Some(price) if price != order.price => {
                return Err(format!("price mismatch at index {}", index.get()));
            }
            Some(_) => {}
        }

        if let Some(previous) = previous_priority
            && node.priority() <= previous
        {
            return Err(format!(
                "priority is not strictly increasing at index {}",
                index.get()
            ));
        }

        previous_priority = Some(node.priority());

        actual_count = actual_count
            .checked_add(1)
            .ok_or_else(|| "count overflow while validating".to_string())?;

        actual_qty = actual_qty
            .checked_add(node.remaining())
            .ok_or_else(|| "qty overflow while validating".to_string())?;

        forward.push(index);

        expected_prev = Some(index);
        current = node.next();
    }

    if forward.last().copied() != Some(tail) {
        return Err("forward chain does not end at tail".into());
    }

    if actual_count != level.count() {
        return Err(format!(
            "count mismatch: actual={actual_count}, level={}",
            level.count()
        ));
    }

    if actual_qty != level.total_visible_qty() {
        return Err(format!(
            "quantity mismatch: actual={actual_qty}, level={}",
            level.total_visible_qty()
        ));
    }

    // ---------- 反向遍历: 尾节点 -> 前驱 ----------

    let mut reverse = Vec::new();
    let mut reverse_visited = BTreeSet::new();

    let mut current = Some(tail);
    let mut expected_next = None;

    while let Some(index) = current {
        if !reverse_visited.insert(index) {
            return Err(format!(
                "cycle or duplicate index in reverse chain: {}",
                index.get()
            ));
        }

        let node = arena
            .get(index)
            .ok_or_else(|| format!("reverse chain references missing node: {}", index.get()))?;

        // 当前节点必须正向指回真实后继节点。
        if node.next() != expected_next {
            return Err(format!("broken next link at index {}", index.get()));
        }

        reverse.push(index);

        expected_next = Some(index);
        current = node.prev();
    }

    let expected_reverse: Vec<_> = forward.iter().rev().copied().collect();

    if reverse != expected_reverse {
        return Err("reverse traversal is not the exact inverse of forward traversal".into());
    }

    Ok(())
}
/// 校验器测试注入的单一损坏维度，每种样本只应违反其目标不变量。
#[derive(Debug, Clone, Copy)]
enum LevelDamage {
    /// 元数据节点数与实际链长不一致。
    WrongCount,
    /// 元数据可见量与节点数量聚合不一致。
    WrongQty,
    /// 非空档缺少头节点端点。
    MissingHead,
    /// 非空档缺少尾节点端点。
    MissingTail,
    /// 前驱节点没有正向指向其后继。
    BrokenForwardLink,
    /// 正向链包含循环。
    Cycle,
    /// 链接引用 Arena 中不存在的节点。
    MissingNode,
    /// 同一档位节点拥有不同价格。
    PriceMismatch,
    /// 先进先出链中的优先级不再严格递增。
    PriorityViolation,
    /// 空节点数与非空端点或数量的矛盾状态。
    NonEmptyCountZero,
    /// 后继节点没有反向指向其前驱。
    BrokenBackwardLink,
    /// 同一档位节点拥有不同方向。
    SideMismatch,
    /// 测试遍历累加可见数量时发生溢出。
    QtyOverflow,
}
#[test]
fn validator_rejects_corrupted_levels() {
    let cases = [
        LevelDamage::WrongCount,
        LevelDamage::WrongQty,
        LevelDamage::MissingHead,
        LevelDamage::MissingTail,
        LevelDamage::BrokenForwardLink,
        LevelDamage::Cycle,
        LevelDamage::MissingNode,
        LevelDamage::PriceMismatch,
        LevelDamage::PriorityViolation,
        LevelDamage::NonEmptyCountZero,
        LevelDamage::BrokenBackwardLink,
        LevelDamage::SideMismatch,
        LevelDamage::QtyOverflow,
    ];

    for damage in cases {
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

        assert_eq!(validate_level(&level, &arena), Ok(()));

        match damage {
            LevelDamage::WrongCount => {
                level.count = 3;
            }

            LevelDamage::WrongQty => {
                level.total_visible_qty = 13;
            }

            LevelDamage::MissingHead => {
                level.head = None;
            }

            LevelDamage::MissingTail => {
                level.tail = None;
            }

            LevelDamage::BrokenForwardLink => {
                arena.get_mut(first).unwrap().set_next(None);
            }

            LevelDamage::Cycle => {
                // 头节点 -> 第二个 -> 头节点
                arena.get_mut(second).unwrap().set_next(Some(first));
            }

            LevelDamage::MissingNode => {
                // 价档仍引用第二个，但 Arena 已无该节点。
                arena.remove(second).unwrap();
            }

            LevelDamage::PriceMismatch => {
                // 保留相同槽位和链关系，但换成错误价格节点。
                arena.remove(second).unwrap();

                let mut replacement =
                    OrderNode::new(order(2, Side::Buy, 101, 7), QueuePriority::new(11));
                replacement.set_prev(Some(first));

                let reused = arena.insert(replacement).unwrap();
                assert_eq!(reused, second);
            }

            LevelDamage::PriorityViolation => {
                arena.remove(second).unwrap();

                let mut replacement = OrderNode::new(
                    order(2, Side::Buy, 100, 7),
                    // 与前驱相等，不再严格递增。
                    QueuePriority::new(10),
                );
                replacement.set_prev(Some(first));

                let reused = arena.insert(replacement).unwrap();
                assert_eq!(reused, second);
            }
            LevelDamage::NonEmptyCountZero => {
                // 头节点、尾节点仍然存在，但节点数错误地归零。
                level.count = 0;
            }

            LevelDamage::BrokenBackwardLink => {
                // `first.next` 仍指向第二个，
                // 但 `second.prev` 不再指向第一个。
                arena.get_mut(second).unwrap().set_prev(None);
            }

            LevelDamage::SideMismatch => {
                // 复用原槽位，保持链接、价格和优先级不变，
                // 只改变节点方向。
                arena.remove(second).unwrap();

                let mut replacement =
                    OrderNode::new(order(2, Side::Sell, 100, 7), QueuePriority::new(11));

                replacement.set_prev(Some(first));

                let reused = arena.insert(replacement).unwrap();
                assert_eq!(reused, second);
            }

            LevelDamage::QtyOverflow => {
                // `first.remaining` = 5
                // `second.remaining` = `u64::MAX`
                // 仅替换第二个节点，保留合法链接和优先级。
                arena.remove(second).unwrap();

                let mut replacement =
                    OrderNode::new(order(2, Side::Buy, 100, u64::MAX), QueuePriority::new(11));

                replacement.set_prev(Some(first));

                let reused = arena.insert(replacement).unwrap();
                assert_eq!(reused, second);
            }
        }

        let result = validate_level(&level, &arena);

        if matches!(damage, LevelDamage::QtyOverflow) {
            assert_eq!(result, Err("qty overflow while validating".to_string()));
        } else {
            assert!(
                result.is_err(),
                "validator unexpectedly accepted damage={damage:?}"
            );
        }
    }
}
#[test]
fn validator_ignores_unrelated_live_nodes() {
    let mut arena = OrderArena::new();
    let mut level = PriceLevel::new();

    for (id, qty, priority) in [(1, 5, 10), (2, 7, 11)] {
        level
            .push_back(
                &mut arena,
                OrderNode::new(order(id, Side::Buy, 100, qty), QueuePriority::new(priority)),
            )
            .unwrap();
    }

    // 不属于当前档位，方向、价格及优先级均不同。
    let unrelated = arena
        .insert(OrderNode::new(
            order(99, Side::Sell, 999, 3),
            QueuePriority::new(0),
        ))
        .unwrap();

    assert!(arena.get(unrelated).is_some());

    // 校验器只检查当前档位的可达链。
    assert_eq!(validate_level(&level, &arena), Ok(()));
}
