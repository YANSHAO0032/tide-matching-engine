//! 生产簿测试专用的输入构造、状态投影和结构不变量验证。

use super::*;

/// 使用与参考模型一致的真实预留边界创建有效测试策略。
pub(super) fn policy() -> QueuePriorityPolicy {
    QueuePriorityPolicy::try_new(1024).unwrap()
}

/// 从受控有效样本构造最小限价单 GTC 输入。
pub(super) fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    LimitGtcOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(price).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 仅测试代码允许直接构造 ProductionOrderBook 的非空状态。
///
/// 必须经过真实分配器 + Arena + `PriceLevel::push_back`，
/// 不允许只往 BTreeMap 塞空 PriceLevel。
pub(super) fn rest_fixture(book: &mut ProductionOrderBook, original: LimitGtcOrder) -> OrderIndex {
    let order_id = original.order_id;
    let side = original.side;
    let price = original.price;

    let priority = book.queue_priority_allocator.allocate().unwrap();

    let levels = match side {
        Side::Buy => &mut book.bids,
        Side::Sell => &mut book.asks,
    };

    let level = levels.entry(price).or_insert_with(PriceLevel::new);

    let slot = level
        .push_back(&mut book.arena, OrderNode::new(original, priority))
        .unwrap();

    assert!(
        book.orders.insert(order_id, slot).is_none(),
        "fixture attempted to insert duplicate OrderId"
    );

    slot
}

/// 按价格顺序投影档位元数据，用于比较查询前后状态。
pub(super) fn level_snapshot(levels: &BTreeMap<Price, PriceLevel>) -> Vec<LevelSnapshotEntry> {
    levels
        .iter()
        .map(|(price, level)| LevelSnapshotEntry {
            price: *price,
            head: level.head(),
            tail: level.tail(),
            count: level.count(),
            total_visible_qty: level.total_visible_qty(),
        })
        .collect()
}

pub(super) fn assert_rest_error_preserves_state(
    book: &mut ProductionOrderBook,
    candidate: LimitGtcOrder,
    expected: RejectReason,
) {
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();

    let policy_before = *book.queue_priority_allocator.policy();

    assert_eq!(book.rest(candidate), Err(expected));

    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "failed rest mutated bids"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "failed rest mutated asks"
    );

    assert_eq!(book.orders, orders_before, "failed rest mutated ID index");

    assert_eq!(
        book.arena.test_snapshot(),
        arena_before,
        "failed rest mutated Arena slots/free-list"
    );

    assert_eq!(
        book.queue_priority_allocator.next(),
        next_before,
        "failed rest consumed QueuePriority"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "failed rest mutated allocator policy"
    );

    // 输入前簿本身是合法状态，因此失败后仍必须合法。
    assert_eq!(validate_book(book), Ok(()));
}

/// 按指定标识符查询得到的完整节点投影，用于查询前后相等性比较。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NodeSnapshotEntry {
    /// 发起查询时使用的标识符，用于保留查询结果与输入的对应关系。
    order_id: OrderId,
    /// 原始订单请求及当前剩余量。
    original: LimitGtcOrder,
    /// 快照取得时该节点剩余的基础资产数量。
    remaining: u64,
    /// 当前先进先出优先级及双向邻居槽位。
    priority: QueuePriority,
    /// 当前节点在同价队列中的前驱槽位。
    prev: Option<OrderIndex>,
    /// 当前节点在同价队列中的后继槽位。
    next: Option<OrderIndex>,
}

/// 按调用方给定的标识符顺序投影有效节点，不依赖 HashMap 迭代序。
///
/// 仅覆盖这些标识符可达的节点，不包含 Arena 空槽和空闲链表。
pub(super) fn node_snapshot(book: &ProductionOrderBook, ids: &[OrderId]) -> Vec<NodeSnapshotEntry> {
    ids.iter()
        .map(|id| {
            let node = book.order(*id).unwrap();

            NodeSnapshotEntry {
                order_id: *id,
                original: node.original_order().clone(),
                remaining: node.remaining(),
                priority: node.priority(),
                prev: node.prev(),
                next: node.next(),
            }
        })
        .collect()
}

pub(super) fn validate_book(book: &ProductionOrderBook) -> Result<(), String> {
    fn validate_side(
        book: &ProductionOrderBook,
        side: Side,
        levels: &BTreeMap<Price, PriceLevel>,
        visited_slots: &mut BTreeSet<OrderIndex>,
        seen_ids: &mut HashSet<OrderId>,
        max_priority: &mut Option<QueuePriority>,
    ) -> Result<(), String> {
        for (price, level) in levels {
            // 价格树中禁止保留空档。
            if level.is_empty() || level.count() == 0 {
                return Err(format!("level/empty: side={side:?}, price={price:?}"));
            }

            let head = level
                .head()
                .ok_or_else(|| format!("level/missing-head: side={side:?}, price={price:?}"))?;

            let expected_tail = level
                .tail()
                .ok_or_else(|| format!("level/missing-tail: side={side:?}, price={price:?}"))?;

            let mut current = Some(head);
            let mut expected_prev = None;
            let mut previous_priority = None;
            let mut actual_tail = None;

            let mut actual_count = 0u32;
            let mut actual_qty = 0u64;

            while let Some(index) = current {
                // 整个订单簿共用已访问节点集合。
                //
                // 因此同时防：
                // 1. 单档位内部环；
                // 2. 同一个槽位被多个档位重复引用。
                if !visited_slots.insert(index) {
                    return Err(format!(
                        "link/cycle-or-duplicate: \
                         side={side:?}, price={price:?}, slot={index:?}"
                    ));
                }

                let node = book.arena.get(index).ok_or_else(|| {
                    format!(
                        "arena/missing-linked-slot: \
                         side={side:?}, price={price:?}, slot={index:?}"
                    )
                })?;

                if node.prev() != expected_prev {
                    return Err(format!(
                        "link/broken-prev: \
                         side={side:?}, price={price:?}, slot={index:?}, \
                         expected={expected_prev:?}, actual={:?}",
                        node.prev()
                    ));
                }

                if node.original_order().side != side {
                    return Err(format!(
                        "node/side-mismatch: \
                         tree_side={side:?}, node_side={:?}, \
                         price={price:?}, slot={index:?}",
                        node.original_order().side
                    ));
                }

                if node.original_order().price != *price {
                    return Err(format!(
                        "node/price-mismatch: \
                         tree_price={price:?}, node_price={:?}, \
                         side={side:?}, slot={index:?}",
                        node.original_order().price
                    ));
                }

                if node.remaining() == 0 {
                    return Err(format!(
                        "node/zero-remaining: \
                         side={side:?}, price={price:?}, slot={index:?}"
                    ));
                }

                if let Some(previous) = previous_priority
                    && node.priority() <= previous
                {
                    return Err(format!(
                        "priority/non-increasing: \
                         side={side:?}, price={price:?}, slot={index:?}, \
                         previous={previous:?}, current={:?}",
                        node.priority()
                    ));
                }

                let order_id = node.original_order().order_id;

                if !seen_ids.insert(order_id) {
                    return Err(format!(
                        "id-index/duplicate-order-id: \
                         order_id={order_id:?}, slot={index:?}"
                    ));
                }

                match book.orders.get(&order_id) {
                    None => {
                        return Err(format!(
                            "id-index/missing: \
                             order_id={order_id:?}, actual_slot={index:?}"
                        ));
                    }

                    Some(mapped) if *mapped != index => {
                        return Err(format!(
                            "id-index/wrong-slot: \
                             order_id={order_id:?}, expected={index:?}, \
                             actual={mapped:?}"
                        ));
                    }

                    Some(_) => {}
                }

                actual_count = actual_count.checked_add(1).ok_or_else(|| {
                    format!("level/count-overflow: side={side:?}, price={price:?}")
                })?;

                actual_qty = actual_qty
                    .checked_add(node.remaining())
                    .ok_or_else(|| format!("level/qty-overflow: side={side:?}, price={price:?}"))?;

                *max_priority = Some(match *max_priority {
                    Some(current_max) if current_max >= node.priority() => current_max,
                    _ => node.priority(),
                });

                previous_priority = Some(node.priority());
                expected_prev = Some(index);
                actual_tail = Some(index);
                current = node.next();
            }

            if actual_tail != Some(expected_tail) {
                return Err(format!(
                    "level/tail-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={expected_tail:?}, actual={actual_tail:?}"
                ));
            }

            if actual_count != level.count() {
                return Err(format!(
                    "level/count-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={}, actual={actual_count}",
                    level.count()
                ));
            }

            if actual_qty != level.total_visible_qty() {
                return Err(format!(
                    "level/qty-mismatch: \
                     side={side:?}, price={price:?}, \
                     expected={}, actual={actual_qty}",
                    level.total_visible_qty()
                ));
            }
        }

        Ok(())
    }

    let mut visited_slots = BTreeSet::new();
    let mut seen_ids = HashSet::new();
    let mut max_priority = None;

    validate_side(
        book,
        Side::Buy,
        &book.bids,
        &mut visited_slots,
        &mut seen_ids,
        &mut max_priority,
    )?;

    validate_side(
        book,
        Side::Sell,
        &book.asks,
        &mut visited_slots,
        &mut seen_ids,
        &mut max_priority,
    )?;

    // ------------------------------------------------------------
    // OrderId -> OrderIndex 必须是链表节点集合的精确映射。
    // ------------------------------------------------------------

    for (order_id, index) in &book.orders {
        if !visited_slots.contains(index) {
            return Err(format!(
                "id-index/extra-or-stale: \
                 order_id={order_id:?}, slot={index:?}"
            ));
        }

        let node = book.arena.get(*index).ok_or_else(|| {
            format!(
                "id-index/missing-arena-slot: \
                 order_id={order_id:?}, slot={index:?}"
            )
        })?;

        if node.original_order().order_id != *order_id {
            return Err(format!(
                "id-index/node-id-mismatch: \
                 map_id={order_id:?}, node_id={:?}, slot={index:?}",
                node.original_order().order_id
            ));
        }
    }

    if book.orders.len() != visited_slots.len() {
        return Err(format!(
            "id-index/count-mismatch: \
             index_count={}, linked_count={}",
            book.orders.len(),
            visited_slots.len()
        ));
    }

    // ------------------------------------------------------------
    // Arena 已占用槽位必须全部属于某条价格链。
    //
    // 测试快照 `test_snapshot`：
    // (
    //   Vec<Option<OrderNode>>,
    //   Vec<OrderIndex>,
    // )
    // ------------------------------------------------------------

    let (arena_slots, _) = book.arena.test_snapshot();

    for (position, slot_state) in arena_slots.iter().enumerate() {
        if slot_state.is_none() {
            continue;
        }

        let index = OrderIndex::try_from_usize(position).map_err(|reason| {
            format!("arena/index-overflow: position={position}, reason={reason:?}")
        })?;

        if !visited_slots.contains(&index) {
            return Err(format!("arena/orphan: slot={index:?}"));
        }
    }

    // ------------------------------------------------------------
    // 分配器 `next` 必须严格大于所有活动优先级。
    // ------------------------------------------------------------

    if let Some(max_active_priority) = max_priority {
        let allocator_next = QueuePriority::new(book.queue_priority_allocator.next());

        if allocator_next <= max_active_priority {
            return Err(format!(
                "allocator/next-not-ahead: \
                 next={allocator_next:?}, max_active={max_active_priority:?}"
            ));
        }
    }

    // ------------------------------------------------------------
    // 连续交易状态下，订单簿不允许买卖价格交叉或相等。
    // 最高买价必须严格小于最低卖价。
    // ------------------------------------------------------------

    match (book.bids.last_key_value(), book.asks.first_key_value()) {
        (Some((best_bid, _)), Some((best_ask, _))) if *best_bid >= *best_ask => {
            return Err(format!(
                "book/crossed-or-locked: \
             best_bid={best_bid:?}, best_ask={best_ask:?}"
            ));
        }
        _ => {}
    }

    Ok(())
}

pub(super) fn policy_with_ceiling(ceiling: u128) -> QueuePriorityPolicy {
    QueuePriorityPolicy::try_with_synthetic_ceiling(1024, Some(ceiling)).unwrap()
}
