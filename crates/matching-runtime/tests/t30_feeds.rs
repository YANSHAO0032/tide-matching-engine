use matching_domain::{MarketId, OrderId, Price, Qty, Side, TradeEvent, TradeId};
use matching_runtime::{FeedBuilder, SourceEvent, SourcePayload};

fn add(sequence: u64, id: u128, price: i64, qty: u64) -> SourceEvent {
    SourceEvent {
        market_id: MarketId(1),
        sequence,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(id),
            side: Side::Buy,
            price: Price::try_new(price).unwrap(),
            visible: Qty::try_new(qty).unwrap(),
        },
    }
}

#[test]
fn independent_feeds_filter_source_and_preserve_trade_identity() {
    let mut b = FeedBuilder::new(MarketId(1));
    let first = b.apply(&add(1, 1, 10, 2)).unwrap();
    assert_eq!(
        (first.mbp.unwrap().feed_seq, first.bbo.unwrap().feed_seq),
        (1, 1)
    );
    let second = b.apply(&add(2, 2, 9, 3)).unwrap();
    assert_eq!(second.mbp.unwrap().feed_seq, 2);
    assert!(second.bbo.is_none());
    let trade = |sequence, id| SourceEvent {
        market_id: MarketId(1),
        sequence,
        payload: SourcePayload::TimedTradeOccurred {
            trade: TradeEvent {
                trade_id: TradeId(id),
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(8),
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                maker_side: Side::Buy,
            },
            engine_time: 42,
        },
    };
    let a = b.apply(&trade(3, 100)).unwrap().trade.unwrap();
    let z = b.apply(&trade(4, 1)).unwrap().trade.unwrap();
    assert_eq!(
        (a.feed_seq, z.feed_seq, z.trade_id, z.engine_time),
        (1, 2, TradeId(1), 42)
    );
    let changed = b
        .apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 5,
            payload: SourcePayload::OrderReduced {
                order_id: OrderId(1),
                visible: Qty::try_new(1).unwrap(),
            },
        })
        .unwrap();
    assert_eq!(
        (changed.mbp.unwrap().feed_seq, changed.bbo.unwrap().feed_seq),
        (3, 2)
    );
    assert_eq!(
        b.depth(Side::Buy),
        vec![
            (Price::try_new(10).unwrap(), 1),
            (Price::try_new(9).unwrap(), 3)
        ]
    );
}

#[test]
fn invalid_input_and_aggregate_overflow_leave_every_feed_unchanged() {
    let mut b = FeedBuilder::new(MarketId(1));
    b.apply(&add(1, 1, 10, u64::MAX)).unwrap();
    let before = b.clone();
    assert!(b.apply(&add(2, 2, 10, 1)).is_err());
    assert_eq!(b, before);
    assert!(b.apply(&add(3, 2, 9, 1)).is_err());
    assert_eq!(b, before);
    let result = b.apply(&add(2, 2, 9, 1)).unwrap();
    assert_eq!(result.mbp.unwrap().feed_seq, 2);
    assert!(result.bbo.is_none());
}

#[test]
fn best_price_quantity_and_empty_side_changes_are_reported() {
    let mut b = FeedBuilder::new(MarketId(1));
    let mut ask = add(1, 1, 11, 4);
    if let SourcePayload::OrderAdded { side, .. } = &mut ask.payload {
        *side = Side::Sell;
    }
    let first = b.apply(&ask).unwrap();
    let quote = first.bbo.unwrap();
    assert_eq!(
        (quote.feed_seq, quote.bid, quote.ask.unwrap().visible),
        (1, None, 4)
    );
    b.apply(&add(2, 2, 10, 2)).unwrap();
    let mut deeper = add(3, 3, 12, 7);
    if let SourcePayload::OrderAdded { side, .. } = &mut deeper.payload {
        *side = Side::Sell;
    }
    assert!(b.apply(&deeper).unwrap().bbo.is_none());
    let removed = b
        .apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 4,
            payload: SourcePayload::OrderRemoved {
                order_id: OrderId(1),
            },
        })
        .unwrap();
    assert_eq!(removed.mbp.unwrap().visible, 0);
    let quote = removed.bbo.unwrap();
    assert_eq!(
        (
            quote.feed_seq,
            quote.source_seq,
            quote.ask.unwrap().price.get()
        ),
        (3, 4, 12)
    );
    let last = b
        .apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 5,
            payload: SourcePayload::OrderRemoved {
                order_id: OrderId(3),
            },
        })
        .unwrap()
        .bbo
        .unwrap();
    assert!(last.ask.is_none());
    assert_eq!(last.bid.unwrap().visible, 2);
}

#[test]
fn fixed_tape_checks_each_feed_against_independent_price_aggregation() {
    use std::collections::BTreeMap;
    let mut orders = BTreeMap::<u128, (Side, i64, u64)>::new();
    let mut b = FeedBuilder::new(MarketId(1));
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    let mut mbp_seq = 0;
    let mut bbo_seq = 0;
    let mut previous = [Vec::new(), Vec::new()];
    for seq in 1..=2000_u64 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = u128::from((state >> 32) % 80);
        let payload = if let Some(&(side, price, qty)) = orders.get(&id) {
            if qty > 1 && state & 1 == 0 {
                orders.insert(id, (side, price, qty - 1));
                SourcePayload::OrderReduced {
                    order_id: OrderId(id),
                    visible: Qty::try_new(qty - 1).unwrap(),
                }
            } else {
                orders.remove(&id);
                SourcePayload::OrderRemoved {
                    order_id: OrderId(id),
                }
            }
        } else {
            let side = if state & 2 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = 90 + ((state >> 16) % 21) as i64;
            let qty = 1 + ((state >> 24) % 10);
            orders.insert(id, (side, price, qty));
            SourcePayload::OrderAdded {
                order_id: OrderId(id),
                side,
                price: Price::try_new(price).unwrap(),
                visible: Qty::try_new(qty).unwrap(),
            }
        };
        let mut expected = [Vec::new(), Vec::new()];
        for (index, side) in [Side::Buy, Side::Sell].into_iter().enumerate() {
            let mut levels = BTreeMap::new();
            for &(order_side, price, qty) in orders.values() {
                if side == order_side {
                    *levels
                        .entry(Price::try_new(price).unwrap())
                        .or_insert(0_u64) += qty;
                }
            }
            expected[index] = levels.into_iter().collect();
            if side == Side::Buy {
                expected[index].reverse();
            }
        }
        let out = b
            .apply(&SourceEvent {
                market_id: MarketId(1),
                sequence: seq,
                payload,
            })
            .unwrap();
        mbp_seq += 1;
        assert_eq!(out.mbp.unwrap().feed_seq, mbp_seq);
        let best_changed = previous[0].first() != expected[0].first()
            || previous[1].first() != expected[1].first();
        if best_changed {
            bbo_seq += 1;
            assert_eq!(out.bbo.unwrap().feed_seq, bbo_seq);
        } else {
            assert!(out.bbo.is_none());
        }
        assert_eq!(b.depth(Side::Buy), expected[0]);
        assert_eq!(b.depth(Side::Sell), expected[1]);
        previous = expected;
    }
}

#[test]
fn timed_trade_wire_preserves_time_and_legacy_trade_is_explicitly_rejected() {
    let trade = TradeEvent {
        trade_id: TradeId(42),
        maker_order_id: OrderId(2),
        taker_order_id: OrderId(3),
        price: Price::try_new(7).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        maker_side: Side::Sell,
    };
    let event = SourceEvent {
        market_id: MarketId(1),
        sequence: 1,
        payload: SourcePayload::TimedTradeOccurred {
            trade: trade.clone(),
            engine_time: 123,
        },
    };
    let bytes = event.encode(1024).unwrap();
    assert_eq!(
        &bytes[..17],
        &[
            b'M', b'S', b'E', b'1', 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 6
        ]
    );
    assert_eq!(&bytes[bytes.len() - 8..], &123_u64.to_be_bytes());
    assert_eq!(SourceEvent::decode(&bytes, 1024).unwrap(), event);
    for end in 0..bytes.len() {
        assert!(SourceEvent::decode(&bytes[..end], 1024).is_err());
    }
    let legacy = SourceEvent {
        payload: SourcePayload::TradeOccurred(trade),
        ..event.clone()
    };
    assert_eq!(
        SourceEvent::decode(&legacy.encode(1024).unwrap(), 1024).unwrap(),
        legacy
    );
    let mut builder = FeedBuilder::new(MarketId(1));
    let before = builder.clone();
    assert!(builder.apply(&legacy).is_err());
    assert_eq!(builder, before);
    assert_eq!(
        builder.apply(&event).unwrap().trade.unwrap().engine_time,
        123
    );
}

#[test]
fn formal_projection_passes_logical_time_into_public_trade() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome, MarketProjector};
    let mut projector = MarketProjector::new(MarketId(1));
    let open = projector
        .apply_engine_events(
            &[EngineEvent::OrderOpened {
                order_id: OrderId(1),
            }],
            |_, _| {
                Some((
                    Side::Buy,
                    Price::try_new(10).unwrap(),
                    Qty::try_new(2).unwrap(),
                ))
            },
        )
        .unwrap();
    let trade = TradeEvent {
        trade_id: TradeId(11),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        maker_side: Side::Buy,
    };
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 1,
        event_index: 0,
        command_id: CommandId(1),
        engine_time: 987,
        market_id: Some(MarketId(1)),
        payload: EngineEvent::Trade(trade),
    };
    let execution = FormalExecution {
        command_id: CommandId(1),
        seq: 1,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(1),
            reject_reason: None,
        },
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: vec![],
        market_data_open_details: vec![],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let events = projector
        .apply_formal_execution(&execution, |_, _| None)
        .unwrap();
    let mut feed = FeedBuilder::new(MarketId(1));
    feed.apply(&open[0]).unwrap();
    let public = feed.apply(&events[0]).unwrap().trade.unwrap();
    assert_eq!(
        (public.trade_id, public.engine_time, public.feed_seq),
        (TradeId(11), 987, 1)
    );
    assert!(feed.apply(&events[1]).unwrap().mbp.is_some());
}

#[test]
fn real_iceberg_source_rebuilds_visible_engine_depth_and_replays() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        IcebergOrder, LimitOrder, MarketMath, ShardId, StpKey, StpMode, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{FormalExecution, FormalOutcome, MarketProjector};
    let order = |id, side, qty| LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    book.place_iceberg_post_only(
        IcebergOrder::try_new(order(1, Side::Sell, 8), Qty::try_new(3).unwrap()).unwrap(),
        &math,
    )
    .unwrap();
    book.rest(order(2, Side::Sell, 2)).unwrap();
    let mut projector = MarketProjector::new(MarketId(1));
    let mut tape = Vec::new();
    for id in [OrderId(1), OrderId(2)] {
        tape.extend(
            projector
                .apply_engine_events(&[EngineEvent::OrderOpened { order_id: id }], |_, id| {
                    book.order(id).map(|node| {
                        (
                            node.original_order().side,
                            node.original_order().price,
                            Qty::try_new(node.visible_remaining()).unwrap(),
                        )
                    })
                })
                .unwrap(),
        );
    }
    let result = book
        .place_limit_ioc(
            order(3, Side::Buy, 6),
            &math,
            &[TradeId(3), TradeId(2), TradeId(1)],
        )
        .unwrap();
    let events: Vec<_> = result
        .events()
        .iter()
        .enumerate()
        .map(|(index, payload)| EventEnvelope {
            cluster_id: ClusterId(1),
            shard_id: ShardId(1),
            shard_seq: 1,
            event_index: index as u32,
            command_id: CommandId(1),
            engine_time: 77,
            market_id: Some(MarketId(1)),
            payload: payload.clone(),
        })
        .collect();
    let execution = FormalExecution {
        command_id: CommandId(1),
        seq: 1,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: None,
            last_event_index: None,
            durable_seq: Some(1),
            reject_reason: None,
        },
        events,
        event_range: None,
        market_data_child_opens: vec![],
        market_data_open_details: vec![],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    tape.extend(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .unwrap(),
    );
    let mut first = FeedBuilder::new(MarketId(1));
    let mut replay = FeedBuilder::new(MarketId(1));
    for event in &tape {
        assert_eq!(first.apply(event).unwrap(), replay.apply(event).unwrap());
    }
    assert_eq!(first, replay);
    for side in [Side::Buy, Side::Sell] {
        let mut expected = std::collections::BTreeMap::<Price, u64>::new();
        for node in book.visible_orders(side) {
            *expected.entry(node.original_order().price).or_default() += node.visible_remaining();
        }
        let mut expected: Vec<_> = expected.into_iter().collect();
        if side == Side::Buy {
            expected.reverse();
        }
        assert_eq!(first.depth(side), expected);
    }
    assert_eq!(
        first.depth(Side::Sell),
        vec![(Price::try_new(100).unwrap(), 2)]
    );
}

#[test]
fn real_book_multilevel_both_sides_matches_after_every_change() {
    use matching_domain::{EngineEvent, LimitOrder, StpKey, StpMode, UserId};
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    let mut projector = matching_runtime::MarketProjector::new(MarketId(1));
    let mut feed = FeedBuilder::new(MarketId(1));
    for (id, side, price, qty) in [
        (1, Side::Buy, 9, 2),
        (2, Side::Sell, 11, 3),
        (3, Side::Buy, 8, 4),
        (4, Side::Sell, 12, 5),
        (5, Side::Buy, 9, 6),
    ] {
        let order = LimitOrder {
            order_id: OrderId(id),
            user_id: UserId(id as u64),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        };
        book.rest(order).unwrap();
        for source in projector
            .apply_engine_events(
                &[EngineEvent::OrderOpened {
                    order_id: OrderId(id),
                }],
                |_, id| {
                    book.order(id).map(|node| {
                        (
                            node.original_order().side,
                            node.original_order().price,
                            Qty::try_new(node.visible_remaining()).unwrap(),
                        )
                    })
                },
            )
            .unwrap()
        {
            feed.apply(&source).unwrap();
        }
        for side in [Side::Buy, Side::Sell] {
            let mut levels = std::collections::BTreeMap::new();
            for node in book.visible_orders(side) {
                *levels.entry(node.original_order().price).or_insert(0_u64) +=
                    node.visible_remaining();
            }
            let mut expected: Vec<_> = levels.into_iter().collect();
            if side == Side::Buy {
                expected.reverse();
            }
            assert_eq!(feed.depth(side), expected);
        }
    }
}
