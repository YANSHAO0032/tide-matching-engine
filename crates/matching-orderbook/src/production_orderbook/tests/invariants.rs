use super::*;

#[test]
fn validate_book_accepts_empty_book() {
    let book = ProductionOrderBook::new(policy());

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_single_side_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 90, 4));

    rest_fixture(&mut book, order(2, Side::Buy, 100, 5));

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_multi_level_two_sided_book() {
    let mut book = ProductionOrderBook::new(policy());

    for original in [
        order(1, Side::Buy, 90, 4),
        order(2, Side::Buy, 95, 5),
        order(3, Side::Sell, 105, 6),
        order(4, Side::Sell, 110, 7),
    ] {
        rest_fixture(&mut book, original);
    }

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_accepts_same_price_fifo_chain() {
    let mut book = ProductionOrderBook::new(policy());

    for original in [
        order(30, Side::Buy, 100, 4),
        order(10, Side::Buy, 100, 5),
        order(20, Side::Buy, 100, 6),
    ] {
        rest_fixture(&mut book, original);
    }

    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_does_not_mutate_state() {
    let mut book = ProductionOrderBook::new(policy());

    let ids = [
        OrderId::new(1),
        OrderId::new(2),
        OrderId::new(3),
        OrderId::new(4),
    ];

    for original in [
        order(1, Side::Buy, 90, 4),
        order(2, Side::Buy, 95, 5),
        order(3, Side::Sell, 105, 6),
        order(4, Side::Sell, 110, 7),
    ] {
        rest_fixture(&mut book, original);
    }

    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);
    let orders_before = book.orders.clone();
    let nodes_before = node_snapshot(&book, &ids);
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();
    let policy_before = *book.queue_priority_allocator.policy();

    assert_eq!(validate_book(&book), Ok(()));

    assert_eq!(level_snapshot(&book.bids), bids_before);
    assert_eq!(level_snapshot(&book.asks), asks_before);
    assert_eq!(book.orders, orders_before);
    assert_eq!(node_snapshot(&book, &ids), nodes_before);
    assert_eq!(book.arena.test_snapshot(), arena_before);

    assert_eq!(book.queue_priority_allocator.next(), next_before);

    assert_eq!(book.queue_priority_allocator.policy(), &policy_before);
}

fn assert_book_error(book: &ProductionOrderBook, expected_category: &str) {
    // 校验器调用前记录全部可观察业务状态。
    let bids_before = level_snapshot(&book.bids);
    let asks_before = level_snapshot(&book.asks);

    let orders_before = book.orders.clone();

    // 完整覆盖 Arena 槽位 + 空闲链表，
    // 同时也包含所有已占用 OrderNode 状态。
    let arena_before = book.arena.test_snapshot();

    let next_before = book.queue_priority_allocator.next();

    let policy_before = *book.queue_priority_allocator.policy();

    // ---------- 校验 ----------

    let error = validate_book(book).expect_err("corrupted book must fail validation");

    assert!(
        error.starts_with(expected_category),
        "expected category={expected_category:?}, actual={error:?}"
    );

    // ---------- 必须完全只读 ----------

    assert_eq!(
        level_snapshot(&book.bids),
        bids_before,
        "validator mutated bid levels: error={error:?}"
    );

    assert_eq!(
        level_snapshot(&book.asks),
        asks_before,
        "validator mutated ask levels: error={error:?}"
    );

    assert_eq!(
        book.orders, orders_before,
        "validator mutated ID index: error={error:?}"
    );

    assert_eq!(
        book.arena.test_snapshot(),
        arena_before,
        "validator mutated Arena slots/free-list: error={error:?}"
    );

    assert_eq!(
        book.queue_priority_allocator.next(),
        next_before,
        "validator consumed queue priority: error={error:?}"
    );

    assert_eq!(
        book.queue_priority_allocator.policy(),
        &policy_before,
        "validator mutated allocator policy: error={error:?}"
    );
}

#[test]
fn validate_book_rejects_missing_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    book.orders.remove(&OrderId::new(1));

    assert_book_error(&book, "id-index/missing");
}

#[test]
fn validate_book_rejects_wrong_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    assert_ne!(first, second);

    book.orders.insert(OrderId::new(1), second);

    assert_book_error(&book, "id-index/wrong-slot");
}

#[test]
fn validate_book_rejects_extra_id_index() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    book.orders.insert(OrderId::new(999), slot);

    assert_book_error(&book, "id-index/node-id-mismatch");
}

#[test]
fn validate_book_rejects_wrong_node_price() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let removed = book.arena.remove(slot).unwrap();

    let replacement = OrderNode::new(order(1, Side::Buy, 101, 5), removed.priority());

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    assert_book_error(&book, "node/price-mismatch");
}

#[test]
fn validate_book_rejects_wrong_node_side() {
    let mut book = ProductionOrderBook::new(policy());

    let slot = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let removed = book.arena.remove(slot).unwrap();

    let replacement = OrderNode::new(order(1, Side::Sell, 100, 5), removed.priority());

    assert_eq!(book.arena.insert(replacement).unwrap(), slot);

    assert_book_error(&book, "node/side-mismatch");
}

#[test]
fn validate_book_rejects_empty_price_level() {
    let mut book = ProductionOrderBook::new(policy());

    book.bids
        .insert(Price::try_new(100).unwrap(), PriceLevel::new());

    assert_book_error(&book, "level/empty");
}

#[test]
fn validate_book_rejects_orphan_arena_node() {
    let mut book = ProductionOrderBook::new(policy());

    book.arena
        .insert(OrderNode::new(
            order(99, Side::Buy, 100, 5),
            QueuePriority::new(0),
        ))
        .unwrap();

    assert_book_error(&book, "arena/orphan");
}

#[test]
fn validate_book_rejects_allocator_next_behind_active_priority() {
    let mut book = ProductionOrderBook::new(policy());

    let original = order(1, Side::Buy, 100, 5);
    let order_id = original.order_id;
    let price = original.price;

    // `allocator.next()` 仍然是 0，
    // 但主动构造一个优先级=10 的活动 `node`。
    let priority = QueuePriority::new(10);

    let mut level = PriceLevel::new();

    let slot = level
        .push_back(&mut book.arena, OrderNode::new(original, priority))
        .unwrap();

    book.bids.insert(price, level);
    book.orders.insert(order_id, slot);

    assert_eq!(book.next_queue_priority(), 0);

    assert_book_error(&book, "allocator/next-not-ahead");
}

#[test]
fn validate_book_rejects_crossed_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 105, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

    assert_book_error(&book, "book/crossed-or-locked");
}

#[test]
fn validate_book_rejects_cycle_without_looping_forever() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    book.arena.get_mut(second).unwrap().set_next(Some(first));

    assert_book_error(&book, "link/cycle-or-duplicate");
}

#[test]
fn validate_book_accepts_free_arena_slot_outside_active_book() {
    let mut book = ProductionOrderBook::new(policy());

    // 先建立真实合法价格档位。
    rest_fixture(&mut book, order(1, Side::Buy, 95, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 105, 7));

    assert_eq!(validate_book(&book), Ok(()));

    // 再直接向 Arena 插入一个与订单簿无关的临时节点。
    // 它从未进入 PriceLevel，也没有进入订单集合映射。
    let temporary = book
        .arena
        .insert(OrderNode::new(
            order(999, Side::Buy, 90, 1),
            QueuePriority::new(999),
        ))
        .unwrap();

    assert!(book.arena.get(temporary).is_some());

    // 真实 `remove`，留下空闲链表槽位。
    let removed = book.arena.remove(temporary).unwrap();

    assert_eq!(removed.original_order().order_id, OrderId::new(999));

    assert!(book.arena.get(temporary).is_none());

    // 空闲槽位不属于已占用 `node`，因此不是孤立节点。
    // 活动盘口仍然完全合法。
    assert_eq!(validate_book(&book), Ok(()));
}

#[test]
fn validate_book_rejects_locked_book() {
    let mut book = ProductionOrderBook::new(policy());

    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    rest_fixture(&mut book, order(2, Side::Sell, 100, 5));

    assert_book_error(&book, "book/crossed-or-locked");
}

#[test]
fn validate_book_rejects_non_increasing_level_priority() {
    let mut book = ProductionOrderBook::new(policy());

    let first = rest_fixture(&mut book, order(1, Side::Buy, 100, 5));

    let second = rest_fixture(&mut book, order(2, Side::Buy, 100, 7));

    let second_original = book.arena.get(second).unwrap().original_order().clone();

    let _ = book.arena.remove(second).unwrap();

    let mut replacement =
        OrderNode::new(second_original, book.arena.get(first).unwrap().priority());

    replacement.set_prev(Some(first));

    assert_eq!(book.arena.insert(replacement).unwrap(), second);

    assert_book_error(&book, "priority/non-increasing");

    let _ = book.arena.remove(second).unwrap();
}

#[test]
fn restored_book_invariants_reject_missing_index_and_broken_aggregate() {
    let mut book = ProductionOrderBook::new(policy());
    rest_fixture(&mut book, order(1, Side::Buy, 100, 5));
    rest_fixture(&mut book, order(2, Side::Buy, 100, 7));
    assert!(book.validate_snapshot_invariants().is_ok());
    let mut missing = book.clone();
    missing.orders.remove(&OrderId::new(1));
    assert!(missing.validate_snapshot_invariants().is_err());
    let mut broken = book.clone();
    let index = broken.orders[&OrderId::new(1)];
    broken
        .arena
        .get_mut(index)
        .unwrap()
        .commit_partial_remaining(1);
    assert!(broken.validate_snapshot_invariants().is_err());
}

#[test]
fn snapshot_book_roundtrip_preserves_arena_fifo_and_allocators() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    let mut book = ProductionOrderBook::new(policy());
    rest_fixture(&mut book, order(1, Side::Buy, 90, 4));
    rest_fixture(&mut book, order(2, Side::Buy, 90, 5));
    rest_fixture(&mut book, order(3, Side::Sell, 110, 6));
    book.cancel(OrderId::new(1)).unwrap();
    rest_fixture(&mut book, order(4, Side::Buy, 90, 8));
    let mut w = SnapshotEncoder::new(10000);
    book.write_snapshot(&mut w).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 10000).unwrap();
    let restored = ProductionOrderBook::read_snapshot(&mut r).unwrap();
    r.finish().unwrap();
    let mut rewritten = SnapshotEncoder::new(10000);
    restored.write_snapshot(&mut rewritten).unwrap();
    assert_eq!(rewritten.finish(), bytes);
    assert!(restored.validate_snapshot_invariants().is_ok());
    for end in 0..bytes.len() {
        let mut r = SnapshotDecoder::new(&bytes[..end], 10000).unwrap();
        assert!(ProductionOrderBook::read_snapshot(&mut r).is_err());
    }
}

#[test]
fn snapshot_validator_rejects_reservation_wrong_owner_or_market() {
    use matching_domain::{
        AssetId, AssetReserve, MarketId, OriginSessionId, ReservationId, UserId,
    };
    use matching_risk::{ReservationState, ReservationStatus};
    let mut book = ProductionOrderBook::new(policy());
    book.bind_market(MarketId::new(7)).unwrap();
    rest_fixture(&mut book, order(1, Side::Buy, 90, 4));
    let state = ReservationState {
        reservation_id: ReservationId::new(1),
        order_id: OrderId::new(1),
        user_id: UserId::new(99),
        market_id: MarketId::new(7),
        origin_session_id: OriginSessionId::new(1),
        version: 1,
        reserves: vec![AssetReserve {
            asset: AssetId::new(1),
            atoms: 2,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    book.reservations.insert(OrderId::new(1), state);
    assert!(book.validate_snapshot_invariants().is_err());
    book.reservations.get_mut(&OrderId::new(1)).unwrap().user_id = UserId::new(1);
    book.reservations
        .get_mut(&OrderId::new(1))
        .unwrap()
        .market_id = MarketId::new(8);
    assert!(book.validate_snapshot_invariants().is_err());
}

#[test]
fn snapshot_book_restores_stop_expiry_reference_and_price_band() {
    use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
    use matching_domain::{
        LogicalTime, MarketMath, PriceBandConfig, PriceReference, PriceReferenceKind,
        ReservationId, StopLimitOrder, StopOrder, TimeInForce, TriggerExpiry,
    };
    let mut book = ProductionOrderBook::new(policy());
    book.place_limit_timed_with_events(
        order(7, Side::Sell, 130, 4),
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        },
        LogicalTime::new(1),
        3,
        &[],
    )
    .unwrap();
    book.place_stop(
        StopOrder::Limit(StopLimitOrder {
            trigger_reference: PriceReferenceKind::MarkPrice,
            order_id: OrderId::new(8),
            user_id: matching_domain::UserId::new(8),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            reservation_id: ReservationId::new(8),
            fee_schedule_version: 1,
            side: Side::Buy,
            stop_price: Price::try_new(110).unwrap(),
            price: Price::try_new(111).unwrap(),
            qty: matching_domain::Qty::try_new(2).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_expiry: TriggerExpiry::Day { session_id: 3 },
        }),
        LogicalTime::new(1),
        3,
    )
    .unwrap();
    book.reference_prices
        .advance(PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 4,
            price: Price::try_new(100).unwrap(),
        })
        .unwrap();
    book.set_price_band_config(Some((
        PriceBandConfig {
            reference_kind: PriceReferenceKind::MarkPrice,
            max_deviation_bps: 1000,
        },
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
    )))
    .unwrap();
    let mut w = SnapshotEncoder::new(10000);
    book.write_snapshot(&mut w).unwrap();
    let bytes = w.finish();
    let mut r = SnapshotDecoder::new(&bytes, 10000).unwrap();
    let mut restored = ProductionOrderBook::read_snapshot(&mut r).unwrap();
    r.finish().unwrap();
    let canonical = |book: &ProductionOrderBook| {
        let mut w = SnapshotEncoder::new(10000);
        book.write_canonical_state(&mut w).unwrap();
        w.finish()
    };
    let baseline = canonical(&book);
    assert_eq!(baseline, canonical(&restored));
    let mut changed = book.clone();
    changed.stop_priority_allocator.allocate().unwrap();
    assert_ne!(baseline, canonical(&changed));
    let mut changed = book.clone();
    changed.queue_priority_allocator.allocate().unwrap();
    assert_ne!(baseline, canonical(&changed));
    let mut changed = book.clone();
    changed
        .reference_prices
        .advance(PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 5,
            price: Price::try_new(100).unwrap(),
        })
        .unwrap();
    assert_ne!(baseline, canonical(&changed));
    assert_eq!(
        restored.expire_stop_day_session(3),
        book.expire_stop_day_session(3)
    );
    assert_eq!(
        restored.expire_gtd_through(LogicalTime::new(20)),
        book.expire_gtd_through(LogicalTime::new(20))
    );
    assert_eq!(restored.price_band, book.price_band);
    assert_eq!(restored.reference_prices, book.reference_prices);
}

#[test]
fn canonical_snapshot_state_ignores_arena_slot_layout_but_preserves_fifo() {
    use crate::OrderArena;
    use matching_domain::snapshot_wire::SnapshotEncoder;
    let mut original = ProductionOrderBook::new(policy());
    rest_fixture(&mut original, order(1, Side::Buy, 90, 4));
    rest_fixture(&mut original, order(2, Side::Buy, 90, 5));
    let mut relocated = original.clone();
    let mut first = original.order(OrderId::new(1)).unwrap().clone();
    first.set_prev(None);
    first.set_next(None);
    let mut second = original.order(OrderId::new(2)).unwrap().clone();
    second.set_prev(None);
    second.set_next(None);
    let mut arena = OrderArena::new();
    let temporary = arena
        .insert(OrderNode::new(
            order(99, Side::Buy, 90, 1),
            QueuePriority::new(0),
        ))
        .unwrap();
    let mut level = PriceLevel::new();
    let first_index = level.push_back(&mut arena, first).unwrap();
    arena.remove(temporary).unwrap();
    let second_index = level.push_back(&mut arena, second).unwrap();
    relocated.arena = arena;
    relocated.bids.insert(Price::try_new(90).unwrap(), level);
    relocated.orders.insert(OrderId::new(1), first_index);
    relocated.orders.insert(OrderId::new(2), second_index);
    assert!(relocated.validate_snapshot_invariants().is_ok());
    let mut a = SnapshotEncoder::new(10000);
    let mut b = SnapshotEncoder::new(10000);
    original.write_canonical_state(&mut a).unwrap();
    relocated.write_canonical_state(&mut b).unwrap();
    assert_eq!(a.finish(), b.finish());
    relocated.cancel(OrderId::new(1)).unwrap();
    let mut changed = SnapshotEncoder::new(10000);
    relocated.write_canonical_state(&mut changed).unwrap();
    let mut before = SnapshotEncoder::new(10000);
    original.write_canonical_state(&mut before).unwrap();
    assert_ne!(before.finish(), changed.finish());
}
