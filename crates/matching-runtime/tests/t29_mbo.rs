use matching_domain::{MarketId, OrderId, Price, Qty, Side};
use matching_runtime::{MboBuilder, MboChange, SourceEvent, SourcePayload};

#[test]
fn builder_filters_boundary_but_advances_source_and_rejects_gaps_atomically() {
    let market = MarketId(1);
    let mut builder = MboBuilder::new(market);
    let add = SourceEvent {
        market_id: market,
        sequence: 1,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(1),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            visible: Qty::try_new(2).unwrap(),
        },
    };
    let first = builder.apply(&add).unwrap().unwrap();
    assert_eq!((first.feed_seq, first.source_seq), (1, 1));
    assert!(matches!(first.change, MboChange::Added { .. }));
    let filtered = SourceEvent {
        market_id: market,
        sequence: 2,
        payload: SourcePayload::Boundary,
    };
    assert!(builder.apply(&filtered).unwrap().is_none());
    let checkpoint = builder.checkpoint();
    let gap = SourceEvent {
        market_id: market,
        sequence: 4,
        payload: SourcePayload::OrderRemoved {
            order_id: OrderId(1),
        },
    };
    assert!(builder.apply(&gap).is_err());
    assert_eq!(builder.checkpoint(), checkpoint);
    let remove = SourceEvent { sequence: 3, ..gap };
    let message = builder.apply(&remove).unwrap().unwrap();
    assert_eq!((message.feed_seq, message.source_seq), (2, 3));
}

#[test]
fn builder_rejects_invalid_changes_without_consuming_sequence() {
    let mut builder = MboBuilder::new(MarketId(1));
    let added = SourceEvent {
        market_id: MarketId(1),
        sequence: 1,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(2),
            side: Side::Sell,
            price: Price::try_new(12).unwrap(),
            visible: Qty::try_new(3).unwrap(),
        },
    };
    builder.apply(&added).unwrap();
    let before = builder.checkpoint();
    let duplicate = SourceEvent {
        sequence: 2,
        ..added.clone()
    };
    assert!(builder.apply(&duplicate).is_err());
    let increase = SourceEvent {
        market_id: MarketId(1),
        sequence: 2,
        payload: SourcePayload::OrderReduced {
            order_id: OrderId(2),
            visible: Qty::try_new(4).unwrap(),
        },
    };
    assert!(builder.apply(&increase).is_err());
    assert_eq!(builder.checkpoint(), before);
    let reduced = SourceEvent {
        payload: SourcePayload::OrderReduced {
            order_id: OrderId(2),
            visible: Qty::try_new(1).unwrap(),
        },
        ..increase
    };
    assert_eq!(builder.apply(&reduced).unwrap().unwrap().feed_seq, 2);
}

#[test]
fn builder_replay_digest_tracks_fifo_and_filtered_source_watermark() {
    let event = |sequence, order_id| SourceEvent {
        market_id: MarketId(4),
        sequence,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(order_id),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            visible: Qty::try_new(2).unwrap(),
        },
    };
    let mut a = MboBuilder::new(MarketId(4));
    let mut b = MboBuilder::new(MarketId(4));
    for e in [event(1, 7), event(2, 2)] {
        a.apply(&e).unwrap();
        b.apply(&e).unwrap();
    }
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.orders_in_insertion_order(), vec![OrderId(7), OrderId(2)]);
    let boundary = SourceEvent {
        market_id: MarketId(4),
        sequence: 3,
        payload: SourcePayload::Boundary,
    };
    a.apply(&boundary).unwrap();
    assert_ne!(a.digest(), b.digest());
}

#[test]
fn checkpoint_roundtrip_rejects_uncommitted_prefix_and_corruption() {
    let mut b = MboBuilder::new(MarketId(4));
    b.apply(&SourceEvent {
        market_id: MarketId(4),
        sequence: 1,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(3),
            side: Side::Sell,
            price: Price::try_new(12).unwrap(),
            visible: Qty::try_new(2).unwrap(),
        },
    })
    .unwrap();
    let bytes = b.encode_checkpoint(4096).unwrap();
    assert!(MboBuilder::decode_checkpoint(&bytes, 4096, MarketId(4), 0).is_err());
    let recovered = MboBuilder::decode_checkpoint(&bytes, 4096, MarketId(4), 1).unwrap();
    assert_eq!(b.digest(), recovered.digest());
    for end in 0..bytes.len() {
        assert!(MboBuilder::decode_checkpoint(&bytes[..end], 4096, MarketId(4), 1).is_err());
    }
    let mut corrupt = bytes.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    assert!(MboBuilder::decode_checkpoint(&corrupt, 4096, MarketId(4), 1).is_err());
}

#[test]
fn trade_source_is_filtered_without_consuming_mbo_sequence() {
    let mut b = MboBuilder::new(MarketId(1));
    let trade = matching_domain::TradeEvent {
        trade_id: matching_domain::TradeId(1),
        maker_order_id: OrderId(2),
        taker_order_id: OrderId(3),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        maker_side: Side::Buy,
    };
    assert!(
        b.apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 1,
            payload: SourcePayload::TradeOccurred(trade)
        })
        .unwrap()
        .is_none()
    );
    let msg = b
        .apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 2,
            payload: SourcePayload::OrderAdded {
                order_id: OrderId(2),
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                visible: Qty::try_new(1).unwrap(),
            },
        })
        .unwrap()
        .unwrap();
    assert_eq!((msg.feed_seq, msg.source_seq), (1, 2));
}

#[test]
fn source_batch_assigns_contiguous_market_local_sequence_atomically() {
    let mut source = matching_runtime::MarketSource::new(MarketId(9));
    let batch = source
        .append(&[SourcePayload::Boundary, SourcePayload::Boundary])
        .unwrap();
    assert_eq!(
        batch
            .iter()
            .map(|e| (e.market_id, e.sequence))
            .collect::<Vec<_>>(),
        vec![(MarketId(9), 1), (MarketId(9), 2)]
    );
    assert_eq!(source.last_sequence(), 2);
    assert!(source.append(&[]).unwrap().is_empty());
    assert_eq!(source.last_sequence(), 2);
}

#[test]
fn projected_iceberg_trade_preserves_intermediate_removal_and_replenishment_order() {
    use matching_domain::{EngineEvent, TradeEvent, TradeId};
    let mut projection = matching_runtime::MarketProjector::new(MarketId(7));
    let order = |_, id| {
        if id == OrderId(1) {
            Some((
                Side::Sell,
                Price::try_new(12).unwrap(),
                Qty::try_new(2).unwrap(),
            ))
        } else {
            None
        }
    };
    let first = projection
        .apply_engine_events(
            &[EngineEvent::OrderOpened {
                order_id: OrderId(1),
            }],
            order,
        )
        .unwrap();
    assert!(matches!(first[0].payload, SourcePayload::OrderAdded { .. }));
    let trade = TradeEvent {
        trade_id: TradeId(4),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(12).unwrap(),
        qty: Qty::try_new(2).unwrap(),
        maker_side: Side::Sell,
    };
    let events = [
        EngineEvent::Trade(trade),
        EngineEvent::OrderPartiallyFilled {
            order_id: OrderId(1),
            remaining: Qty::try_new(5).unwrap(),
        },
        EngineEvent::IcebergRefreshed {
            order_id: OrderId(1),
            visible: Qty::try_new(2).unwrap(),
        },
    ];
    let out = projection
        .apply_engine_events(&events, |_, _| None)
        .unwrap();
    assert!(matches!(out[0].payload, SourcePayload::TradeOccurred(_)));
    assert!(matches!(out[1].payload, SourcePayload::OrderRemoved { .. }));
    assert!(matches!(out[2].payload, SourcePayload::OrderAdded { .. }));
    assert_eq!(
        out.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
}

#[test]
fn projector_rejects_invalid_trade_atomically_without_consuming_source_sequence() {
    use matching_domain::{EngineEvent, TradeEvent, TradeId};
    let mut projection = matching_runtime::MarketProjector::new(MarketId(7));
    let open = EngineEvent::OrderOpened {
        order_id: OrderId(1),
    };
    let bad = EngineEvent::Trade(TradeEvent {
        trade_id: TradeId(1),
        maker_order_id: OrderId(1),
        taker_order_id: OrderId(2),
        price: Price::try_new(12).unwrap(),
        qty: Qty::try_new(3).unwrap(),
        maker_side: Side::Sell,
    });
    assert!(
        projection
            .apply_engine_events(&[open.clone(), bad], |_, _| Some((
                Side::Sell,
                Price::try_new(12).unwrap(),
                Qty::try_new(2).unwrap()
            )))
            .is_err()
    );
    let out = projection
        .apply_engine_events(&[open], |_, _| {
            Some((
                Side::Sell,
                Price::try_new(12).unwrap(),
                Qty::try_new(2).unwrap(),
            ))
        })
        .unwrap();
    assert_eq!(out[0].sequence, 1);
}

#[test]
fn projector_rejects_unimplemented_amend_for_active_order_without_mutation() {
    use matching_domain::{AmendedQuantity, EngineEvent};
    let mut projection = matching_runtime::MarketProjector::new(MarketId(7));
    let open = EngineEvent::OrderOpened {
        order_id: OrderId(1),
    };
    projection
        .apply_engine_events(&[open], |_, _| {
            Some((
                Side::Buy,
                Price::try_new(12).unwrap(),
                Qty::try_new(2).unwrap(),
            ))
        })
        .unwrap();
    let before = projection.clone();
    let amend = EngineEvent::OrderAmended {
        order_id: OrderId(1),
        old_price: Some(Price::try_new(12).unwrap()),
        new_price: Some(Price::try_new(12).unwrap()),
        old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(2).unwrap()),
        new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(1).unwrap()),
        priority_retained: true,
    };
    assert!(
        projection
            .apply_engine_events(&[amend], |_, _| None)
            .is_err()
    );
    assert_eq!(projection, before);
}

#[test]
fn projector_resolves_each_open_at_its_own_event_position() {
    use matching_domain::{CancelReason, EngineEvent, OrderRemaining};
    let mut p = matching_runtime::MarketProjector::new(MarketId(1));
    let events = [
        EngineEvent::OrderOpened {
            order_id: OrderId(1),
        },
        EngineEvent::OrderCanceled {
            order_id: OrderId(1),
            remaining: OrderRemaining::Base(Qty::try_new(2).unwrap()),
            reason: CancelReason::MassCancel,
        },
        EngineEvent::OrderOpened {
            order_id: OrderId(1),
        },
    ];
    let projected = p
        .apply_engine_events(&events, |index, _| {
            Some((
                Side::Sell,
                Price::try_new(10).unwrap(),
                Qty::try_new(if index == 0 { 2 } else { 1 }).unwrap(),
            ))
        })
        .unwrap();
    assert!(
        matches!(&projected[0].payload, SourcePayload::OrderAdded { visible, .. } if visible.get() == 2)
    );
    assert!(
        matches!(&projected[2].payload, SourcePayload::OrderAdded { visible, .. } if visible.get() == 1)
    );
}

#[test]
fn projector_rejects_filled_event_if_visible_order_still_exists() {
    use matching_domain::EngineEvent;
    let mut projection = matching_runtime::MarketProjector::new(MarketId(7));
    projection
        .apply_engine_events(
            &[EngineEvent::OrderOpened {
                order_id: OrderId(1),
            }],
            |_, _| {
                Some((
                    Side::Buy,
                    Price::try_new(12).unwrap(),
                    Qty::try_new(2).unwrap(),
                ))
            },
        )
        .unwrap();
    let before = projection.clone();
    assert!(
        projection
            .apply_engine_events(
                &[EngineEvent::OrderFilled {
                    order_id: OrderId(1)
                }],
                |_, _| None
            )
            .is_err()
    );
    assert_eq!(projection, before);
}

#[test]
fn retained_amend_projects_visible_reduction_and_preserves_arrival() {
    use matching_domain::{AmendedQuantity, EngineEvent};
    let mut p = matching_runtime::MarketProjector::new(MarketId(7));
    p.apply_engine_events(
        &[EngineEvent::OrderOpened {
            order_id: OrderId(1),
        }],
        |_, _| {
            Some((
                Side::Buy,
                Price::try_new(12).unwrap(),
                Qty::try_new(2).unwrap(),
            ))
        },
    )
    .unwrap();
    let amend = EngineEvent::OrderAmended {
        order_id: OrderId(1),
        old_price: Some(Price::try_new(12).unwrap()),
        new_price: Some(Price::try_new(12).unwrap()),
        old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(2).unwrap()),
        new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(1).unwrap()),
        priority_retained: true,
    };
    let out = p
        .apply_engine_events(&[amend], |_, _| {
            Some((
                Side::Buy,
                Price::try_new(12).unwrap(),
                Qty::try_new(1).unwrap(),
            ))
        })
        .unwrap();
    assert!(
        matches!(out[0].payload, SourcePayload::OrderReduced { order_id: OrderId(1), visible } if visible.get() == 1)
    );
    assert_eq!(out[0].sequence, 2);
}

#[test]
fn projector_checkpoint_restores_source_continuity_and_visible_state() {
    use matching_domain::EngineEvent;
    let mut original = matching_runtime::MarketProjector::new(MarketId(8));
    original
        .apply_engine_events(
            &[EngineEvent::OrderOpened {
                order_id: OrderId(2),
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
    let bytes = original.encode_checkpoint(4096).unwrap();
    assert!(
        matching_runtime::MarketProjector::decode_checkpoint(&bytes, 4096, MarketId(8), 0).is_err()
    );
    let mut restored =
        matching_runtime::MarketProjector::decode_checkpoint(&bytes, 4096, MarketId(8), 1).unwrap();
    assert_eq!(restored, original);
    let cancel = EngineEvent::OrderCanceled {
        order_id: OrderId(2),
        remaining: matching_domain::OrderRemaining::Base(Qty::try_new(2).unwrap()),
        reason: matching_domain::CancelReason::MassCancel,
    };
    assert_eq!(
        restored
            .apply_engine_events(std::slice::from_ref(&cancel), |_, _| None)
            .unwrap(),
        original
            .apply_engine_events(&[cancel], |_, _| None)
            .unwrap()
    );
}

#[test]
fn projector_matches_real_iceberg_replenishment_and_fifo() {
    use matching_domain::{
        EngineEvent, IcebergOrder, LimitOrder, MarketMath, StpKey, StpMode, TradeId, UserId,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    let make = |id, side, qty| LimitOrder {
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
        IcebergOrder::try_new(make(1, Side::Sell, 8), Qty::try_new(3).unwrap()).unwrap(),
        &math,
    )
    .unwrap();
    book.rest(make(2, Side::Sell, 2)).unwrap();
    let mut projector = matching_runtime::MarketProjector::new(MarketId(1));
    let mut builder = MboBuilder::new(MarketId(1));
    for id in [OrderId(1), OrderId(2)] {
        let sources = projector
            .apply_engine_events(
                &[EngineEvent::OrderOpened { order_id: id }],
                |_, order_id| {
                    book.order(order_id).map(|node| {
                        (
                            node.original_order().side,
                            node.original_order().price,
                            Qty::try_new(node.visible_remaining()).unwrap(),
                        )
                    })
                },
            )
            .unwrap();
        for source in sources {
            builder.apply(&source).unwrap();
        }
    }
    let actual = book
        .place_limit_ioc(
            make(3, Side::Buy, 6),
            &math,
            &[TradeId(1), TradeId(2), TradeId(3)],
        )
        .unwrap();
    let projected = projector
        .apply_engine_events(actual.events(), |_, _| None)
        .unwrap();
    for event in &projected {
        builder.apply(event).unwrap();
    }
    let actual_depth = [OrderId(1), OrderId(2)]
        .iter()
        .filter_map(|id| book.order(*id))
        .map(|node| node.visible_remaining())
        .sum::<u64>();
    assert_eq!(
        builder.visible_depth(Side::Sell).unwrap(),
        vec![(Price::try_new(100).unwrap(), actual_depth)]
    );
    assert_eq!(
        projected
            .iter()
            .filter(|event| matches!(event.payload, SourcePayload::TradeOccurred(_)))
            .count(),
        3
    );
    assert_eq!(
        projector.visible_order(OrderId(1)).unwrap().2.get(),
        book.order(OrderId(1)).unwrap().visible_remaining()
    );
    assert!(projector.visible_order(OrderId(2)).is_none());
}

#[test]
fn mbo_depth_aggregates_visible_orders_and_detects_overflow() {
    let mut b = MboBuilder::new(MarketId(2));
    let add = |sequence, id, price, qty| SourceEvent {
        market_id: MarketId(2),
        sequence,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(id),
            side: Side::Sell,
            price: Price::try_new(price).unwrap(),
            visible: Qty::try_new(qty).unwrap(),
        },
    };
    b.apply(&add(1, 1, 100, 2)).unwrap();
    b.apply(&add(2, 2, 100, 3)).unwrap();
    b.apply(&add(3, 3, 101, 1)).unwrap();
    assert_eq!(
        b.visible_depth(Side::Sell).unwrap(),
        vec![
            (Price::try_new(100).unwrap(), 5),
            (Price::try_new(101).unwrap(), 1)
        ]
    );
    let mut overflow = MboBuilder::new(MarketId(2));
    overflow.apply(&add(1, 1, 100, u64::MAX)).unwrap();
    overflow.apply(&add(2, 2, 100, 1)).unwrap();
    assert!(overflow.visible_depth(Side::Sell).is_err());
}

#[test]
fn projector_rejects_trade_when_missing_intermediate_child_open() {
    use matching_domain::{EngineEvent, TradeEvent, TradeId, TriggeredOrderKind};
    let mut p = matching_runtime::MarketProjector::new(MarketId(1));
    let before = p.clone();
    let events = [
        EngineEvent::StopTriggered {
            order_id: OrderId(1),
            child_kind: TriggeredOrderKind::Limit,
        },
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId(1),
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(1).unwrap(),
            maker_side: Side::Sell,
        }),
    ];
    assert!(p.apply_engine_events(&events, |_, _| None).is_err());
    assert_eq!(p, before);
}

#[test]
fn traced_child_open_is_inserted_before_later_child_trade() {
    use matching_domain::{EngineEvent, TradeEvent, TradeId, TriggeredOrderKind};
    use matching_orderbook::production_orderbook::TriggeredChildOpen;
    let mut p = matching_runtime::MarketProjector::new(MarketId(1));
    let events = [
        EngineEvent::StopTriggered {
            order_id: OrderId(1),
            child_kind: TriggeredOrderKind::Limit,
        },
        EngineEvent::StopTriggered {
            order_id: OrderId(2),
            child_kind: TriggeredOrderKind::Limit,
        },
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId(1),
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(1).unwrap(),
            maker_side: Side::Sell,
        }),
    ];
    let opens = [TriggeredChildOpen {
        order_id: OrderId(1),
        side: Side::Sell,
        price: Price::try_new(10).unwrap(),
        visible: Qty::try_new(1).unwrap(),
        after_event_count: 1,
    }];
    let out = p
        .apply_engine_events_with_child_opens(&events, &opens, |_, _| None)
        .unwrap();
    assert!(matches!(out[0].payload, SourcePayload::OrderAdded { .. }));
    assert!(matches!(out[1].payload, SourcePayload::TradeOccurred(_)));
    assert!(matches!(out[2].payload, SourcePayload::OrderRemoved { .. }));
}

#[test]
fn formal_projection_rejects_malformed_envelope_and_ignores_deduplicated_result() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome};
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 9,
        event_index: 0,
        command_id: CommandId(4),
        engine_time: 0,
        market_id: Some(MarketId(1)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(5),
        },
    };
    let mut execution = FormalExecution {
        command_id: CommandId(4),
        seq: 9,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(9),
            reject_reason: None,
        },
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut projector = matching_runtime::MarketProjector::new(MarketId(1));
    let before = projector.clone();
    execution.events[0].event_index = 1;
    assert!(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .is_err()
    );
    assert_eq!(projector, before);
    execution.events[0].event_index = 0;
    execution.events[0].market_id = Some(MarketId(2));
    assert!(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .is_err()
    );
    assert_eq!(projector, before);
    execution.events[0].market_id = Some(MarketId(1));
    assert_eq!(
        projector
            .apply_formal_execution(&execution, |_, _| Some((
                Side::Buy,
                Price::try_new(10).unwrap(),
                Qty::try_new(1).unwrap()
            )))
            .unwrap()
            .len(),
        1
    );
    let after = projector.clone();
    execution.deduplicated = true;
    execution.events.clear();
    assert!(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(projector, after);
}

#[test]
fn formal_projection_filters_other_markets_without_source_gaps() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome};
    let mk = |index, market, id| EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 5,
        event_index: index,
        command_id: CommandId(9),
        engine_time: 0,
        market_id: Some(MarketId(market)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(id),
        },
    };
    let exec = FormalExecution {
        command_id: CommandId(9),
        seq: 5,
        outcome: FormalOutcome::ShardControl,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(2),
            durable_seq: Some(5),
            reject_reason: None,
        },
        events: vec![mk(0, 1, 1), mk(1, 2, 2), mk(2, 1, 3)],
        event_range: Some((0, 2)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut p = matching_runtime::MarketProjector::new(MarketId(1));
    let sources = p
        .apply_formal_execution(&exec, |index, id| {
            assert_eq!(
                (index, id),
                if index == 0 {
                    (0, OrderId(1))
                } else {
                    (2, OrderId(3))
                }
            );
            Some((
                Side::Buy,
                Price::try_new(10).unwrap(),
                Qty::try_new(1).unwrap(),
            ))
        })
        .unwrap();
    assert_eq!(
        sources.iter().map(|item| item.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn formal_projection_rejects_mixed_shard_and_cluster_atomically() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome};
    let event = |index, shard| EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(shard),
        shard_seq: 9,
        event_index: index,
        command_id: CommandId(4),
        engine_time: 0,
        market_id: Some(MarketId(1)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(u128::from(index) + 1),
        },
    };
    let exec = FormalExecution {
        command_id: CommandId(4),
        seq: 9,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(1),
            durable_seq: Some(9),
            reject_reason: None,
        },
        events: vec![event(0, 1), event(1, 2)],
        event_range: Some((0, 1)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: Vec::new(),
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut p = matching_runtime::MarketProjector::new(MarketId(1));
    let before = p.clone();
    assert!(
        p.apply_formal_execution(&exec, |_, _| Some((
            Side::Buy,
            Price::try_new(10).unwrap(),
            Qty::try_new(1).unwrap()
        )))
        .is_err()
    );
    assert_eq!(p, before);
}

#[test]
fn formal_projection_rejects_forged_or_duplicate_open_details() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome, MarketDataOpenDetail};
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 9,
        event_index: 0,
        command_id: CommandId(4),
        engine_time: 0,
        market_id: Some(MarketId(1)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(5),
        },
    };
    let detail = MarketDataOpenDetail {
        event_index: 0,
        order_id: OrderId(6),
        side: Side::Buy,
        price: Price::try_new(10).unwrap(),
        visible: Qty::try_new(1).unwrap(),
    };
    let mut execution = FormalExecution {
        command_id: CommandId(4),
        seq: 9,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(9),
            reject_reason: None,
        },
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: vec![detail],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut projector = matching_runtime::MarketProjector::new(MarketId(1));
    let before = projector.clone();
    assert!(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .is_err()
    );
    assert_eq!(projector, before);
    execution.market_data_open_details[0].order_id = OrderId(5);
    execution
        .market_data_open_details
        .push(execution.market_data_open_details[0].clone());
    assert!(
        projector
            .apply_formal_execution(&execution, |_, _| None)
            .is_err()
    );
    assert_eq!(projector, before);
}

#[test]
fn shard_market_data_candidate_is_atomic_across_markets() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome, MarketDataOpenDetail, MarketDataState};
    let event = |index, market, id| EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 1,
        event_index: index,
        command_id: CommandId(1),
        engine_time: 0,
        market_id: Some(MarketId(market)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(id),
        },
    };
    let detail = |index, id| MarketDataOpenDetail {
        event_index: index,
        order_id: OrderId(id),
        side: Side::Buy,
        price: Price::try_new(10).unwrap(),
        visible: Qty::try_new(1).unwrap(),
    };
    let mut execution = FormalExecution {
        command_id: CommandId(1),
        seq: 1,
        outcome: FormalOutcome::ShardControl,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(1),
            durable_seq: Some(1),
            reject_reason: None,
        },
        events: vec![event(0, 1, 1), event(1, 2, 2)],
        event_range: Some((0, 1)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: vec![detail(0, 1), detail(1, 3)],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut data = MarketDataState::new([MarketId(1), MarketId(2)]).unwrap();
    let before = data.clone();
    assert!(data.apply(&execution).is_err());
    assert_eq!(data, before);
    execution.market_data_open_details[1].order_id = OrderId(2);
    let output = data.apply(&execution).unwrap();
    assert_eq!(output[&MarketId(1)][0].sequence, 1);
    assert_eq!(output[&MarketId(2)][0].sequence, 1);
}

#[test]
fn shard_market_data_checkpoint_requires_each_trusted_market_prefix() {
    use matching_domain::{
        ClusterId, CommandId, CommandResultSummary, CommandStatus, EngineEvent, EventEnvelope,
        ShardId,
    };
    use matching_runtime::{FormalExecution, FormalOutcome, MarketDataOpenDetail, MarketDataState};
    let event = EventEnvelope {
        cluster_id: ClusterId(1),
        shard_id: ShardId(1),
        shard_seq: 1,
        event_index: 0,
        command_id: CommandId(1),
        engine_time: 0,
        market_id: Some(MarketId(1)),
        payload: EngineEvent::OrderOpened {
            order_id: OrderId(1),
        },
    };
    let detail = MarketDataOpenDetail {
        event_index: 0,
        order_id: OrderId(1),
        side: Side::Buy,
        price: Price::try_new(10).unwrap(),
        visible: Qty::try_new(1).unwrap(),
    };
    let execution = FormalExecution {
        command_id: CommandId(1),
        seq: 1,
        outcome: FormalOutcome::Placed(OrderId(1)),
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: Some(0),
            last_event_index: Some(0),
            durable_seq: Some(1),
            reject_reason: None,
        },
        events: vec![event],
        event_range: Some((0, 0)),
        market_data_child_opens: Vec::new(),
        market_data_open_details: vec![detail],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut data = MarketDataState::new([MarketId(1), MarketId(2)]).unwrap();
    data.apply(&execution).unwrap();
    let bytes = data.encode_checkpoint(4096).unwrap();
    let limits = [(MarketId(1), 1), (MarketId(2), 0)].into_iter().collect();
    assert_eq!(
        MarketDataState::decode_checkpoint(&bytes, 4096, 1, &limits).unwrap(),
        data
    );
    assert!(MarketDataState::decode_checkpoint(&bytes, 4096, 0, &limits).is_err());
    let stale = [(MarketId(1), 0), (MarketId(2), 0)].into_iter().collect();
    assert!(MarketDataState::decode_checkpoint(&bytes, 4096, 1, &stale).is_err());
    let missing = [(MarketId(1), 1)].into_iter().collect();
    assert!(MarketDataState::decode_checkpoint(&bytes, 4096, 1, &missing).is_err());
    for end in 0..bytes.len() {
        assert!(MarketDataState::decode_checkpoint(&bytes[..end], 4096, 1, &limits).is_err());
    }
    let mut damaged = bytes.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert!(MarketDataState::decode_checkpoint(&damaged, 4096, 1, &limits).is_err());
}

#[test]
fn shard_candidate_rejects_orphan_open_details_without_market_events() {
    use matching_domain::{CommandId, CommandResultSummary, CommandStatus};
    use matching_runtime::{FormalExecution, FormalOutcome, MarketDataOpenDetail, MarketDataState};
    let exec = FormalExecution {
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
        events: vec![],
        event_range: None,
        market_data_child_opens: vec![],
        market_data_open_details: vec![MarketDataOpenDetail {
            event_index: 0,
            order_id: OrderId(1),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            visible: Qty::try_new(1).unwrap(),
        }],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut data = MarketDataState::new([MarketId(1)]).unwrap();
    let before = data.clone();
    assert!(data.apply(&exec).is_err());
    assert_eq!(data, before);
}

#[test]
fn shard_candidate_enforces_command_prefix_even_without_market_events() {
    use matching_domain::{CommandId, CommandResultSummary, CommandStatus};
    use matching_runtime::{FormalExecution, FormalOutcome, MarketDataState};
    let make = |seq| FormalExecution {
        command_id: CommandId(u128::from(seq)),
        seq,
        outcome: FormalOutcome::MarketManaged,
        summary: CommandResultSummary {
            status: CommandStatus::Succeeded,
            first_event_index: None,
            last_event_index: None,
            durable_seq: Some(seq),
            reject_reason: None,
        },
        events: vec![],
        event_range: None,
        market_data_child_opens: vec![],
        market_data_open_details: vec![],
        market_data_source: Default::default(),
        deduplicated: false,
    };
    let mut data = MarketDataState::new([MarketId(1)]).unwrap();
    assert!(data.apply(&make(1)).unwrap().is_empty());
    let before = data.clone();
    assert!(data.apply(&make(3)).is_err());
    assert!(data.apply(&make(1)).is_err());
    assert_eq!(data, before);
    assert!(data.apply(&make(2)).unwrap().is_empty());
    assert_eq!(data.last_applied_shard_seq(), 2);
}

#[test]
fn source_event_wire_roundtrips_and_rejects_all_truncations() {
    use matching_domain::{TradeEvent, TradeId};
    let payloads = [
        SourcePayload::OrderAdded {
            order_id: OrderId(5),
            side: Side::Buy,
            price: Price::try_new(12).unwrap(),
            visible: Qty::try_new(4).unwrap(),
        },
        SourcePayload::OrderReduced {
            order_id: OrderId(5),
            visible: Qty::try_new(2).unwrap(),
        },
        SourcePayload::OrderRemoved {
            order_id: OrderId(5),
        },
        SourcePayload::TradeOccurred(TradeEvent {
            trade_id: TradeId(10),
            maker_order_id: OrderId(5),
            taker_order_id: OrderId(8),
            price: Price::try_new(12).unwrap(),
            qty: Qty::try_new(2).unwrap(),
            maker_side: Side::Buy,
        }),
        SourcePayload::Boundary,
    ];
    for payload in payloads {
        let event = SourceEvent {
            market_id: MarketId(7),
            sequence: 2,
            payload,
        };
        let bytes = event.encode(1024).unwrap();
        assert_eq!(SourceEvent::decode(&bytes, 1024).unwrap(), event);
        for end in 0..bytes.len() {
            assert!(SourceEvent::decode(&bytes[..end], 1024).is_err());
        }
        assert!(SourceEvent::decode(&bytes, bytes.len() - 1).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(SourceEvent::decode(&trailing, 1024).is_err());
    }
}

#[test]
fn source_event_wire_uses_stable_bytes_and_rejects_invalid_tags() {
    let event = SourceEvent {
        market_id: MarketId(7),
        sequence: 2,
        payload: SourcePayload::OrderRemoved {
            order_id: OrderId(5),
        },
    };
    let bytes = event.encode(128).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x4d, 0x53, 0x45, 0x31, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 5
        ]
    );
    let mut unknown = bytes.clone();
    unknown[16] = 255;
    assert!(SourceEvent::decode(&unknown, 128).is_err());
    let mut zero_sequence = bytes;
    zero_sequence[8..16].fill(0);
    assert!(SourceEvent::decode(&zero_sequence, 128).is_err());
}
