use matching_domain::{MarketId, OrderId, Price, Qty, Side};
use matching_runtime::{FeedBuilder, SourceEvent, SourcePayload};

fn event(sequence: u64, id: u128, price: i64) -> SourceEvent {
    SourceEvent {
        market_id: MarketId(1),
        sequence,
        payload: SourcePayload::OrderAdded {
            order_id: OrderId(id),
            side: Side::Buy,
            price: Price::try_new(price).unwrap(),
            visible: Qty::try_new(2).unwrap(),
        },
    }
}

#[test]
fn checkpoint_restarts_all_sequences_and_rejects_corruption() {
    let mut live = FeedBuilder::new(MarketId(1));
    live.apply(&event(1, 1, 10)).unwrap();
    live.apply(&event(2, 2, 9)).unwrap();
    let bytes = live.encode_checkpoint(4096).unwrap();
    let mut resumed = FeedBuilder::decode_checkpoint(&bytes, 4096, MarketId(1), 2).unwrap();
    assert_eq!(live.digest(), resumed.digest());
    assert_eq!(
        live.apply(&event(3, 3, 11)),
        resumed.apply(&event(3, 3, 11))
    );
    assert_eq!(live.digest(), resumed.digest());
    assert!(FeedBuilder::decode_checkpoint(&bytes, 4096, MarketId(1), 1).is_err());
    for end in 0..bytes.len() {
        assert!(FeedBuilder::decode_checkpoint(&bytes[..end], 4096, MarketId(1), 2).is_err());
    }
    let mut corrupt = bytes.clone();
    corrupt[8] ^= 1;
    assert!(FeedBuilder::decode_checkpoint(&corrupt, 4096, MarketId(1), 2).is_err());
}

#[test]
fn l2_snapshot_resume_uses_feed_sequence_and_ignores_duplicates() {
    use matching_runtime::{L2Client, ResumeError};
    let mut builder = FeedBuilder::new(MarketId(1));
    let first = builder.apply(&event(1, 1, 10)).unwrap().mbp.unwrap();
    let snapshot = builder.l2_snapshot(1);
    let mut client = L2Client::from_snapshot(snapshot).unwrap();
    assert!(!client.apply(&first).unwrap());
    let second = builder.apply(&event(2, 2, 9)).unwrap().mbp.unwrap();
    assert!(client.apply(&second).unwrap());
    assert_eq!(client.depth(Side::Buy), builder.depth(Side::Buy));
    let mut gap = second.clone();
    gap.feed_seq += 2;
    assert_eq!(client.apply(&gap), Err(ResumeError::Gap));
    assert_eq!(client.depth(Side::Buy), builder.depth(Side::Buy));
}

#[test]
fn l3_resume_accepts_filtered_source_gaps_and_rejects_feed_gaps() {
    use matching_runtime::{L3Client, ResumeError};
    let mut builder = FeedBuilder::new(MarketId(1));
    let first = builder.apply(&event(1, 1, 10)).unwrap().mbo.unwrap();
    let mut client = L3Client::from_snapshot(builder.l3_snapshot(1));
    assert!(!client.apply(&first).unwrap());
    let boundary = SourceEvent {
        market_id: MarketId(1),
        sequence: 2,
        payload: SourcePayload::Boundary,
    };
    builder.apply(&boundary).unwrap();
    let next = builder.apply(&event(3, 2, 9)).unwrap().mbo.unwrap();
    assert!(client.apply(&next).unwrap());
    assert_eq!(client.depth(Side::Buy).unwrap(), builder.depth(Side::Buy));
    let mut gap = next.clone();
    gap.feed_seq += 2;
    assert_eq!(client.apply(&gap), Err(ResumeError::Gap));
}

#[test]
fn migration_boundary_records_all_watermarks_and_restart_continues() {
    use matching_domain::{ShardId, TradeEvent, TradeId};
    let mut builder = FeedBuilder::new(MarketId(1));
    builder.apply(&event(1, 1, 10)).unwrap();
    builder.apply(&event(2, 2, 9)).unwrap();
    builder
        .apply(&SourceEvent {
            market_id: MarketId(1),
            sequence: 3,
            payload: SourcePayload::TimedTradeOccurred {
                trade: TradeEvent {
                    trade_id: TradeId(1),
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(2),
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(1).unwrap(),
                    maker_side: Side::Buy,
                },
                engine_time: 4,
            },
        })
        .unwrap();
    let boundary = builder.migration_boundary(ShardId(1), ShardId(2), 7);
    assert_eq!(
        (
            boundary.last_market_event_seq,
            boundary.last_feed_seq.mbo,
            boundary.last_feed_seq.mbp,
            boundary.last_feed_seq.bbo,
            boundary.last_feed_seq.trade
        ),
        (3, 2, 2, 1, 1)
    );
    let bytes = builder.encode_checkpoint(4096).unwrap();
    let mut target = FeedBuilder::decode_checkpoint(&bytes, 4096, MarketId(1), 3).unwrap();
    let out = target.apply(&event(4, 3, 11)).unwrap();
    assert_eq!(
        (
            out.mbo.unwrap().feed_seq,
            out.mbp.unwrap().feed_seq,
            out.bbo.unwrap().feed_seq
        ),
        (3, 3, 2)
    );
}

#[test]
fn repeated_disconnects_resume_exactly_and_checkpoint_replay_matches() {
    use matching_runtime::{L2Client, L3Client};
    let mut reference_digest = None;
    for run in 0..100 {
        let mut live = FeedBuilder::new(MarketId(1));
        let mut state = 0x1234_5678_u64;
        for batch in 0..20_u64 {
            let mut l2 = L2Client::from_snapshot(live.l2_snapshot(1)).unwrap();
            let mut l3 = L3Client::from_snapshot(live.l3_snapshot(1));
            let bytes = live.encode_checkpoint(65536).unwrap();
            let mut restarted =
                FeedBuilder::decode_checkpoint(&bytes, 65536, MarketId(1), batch * 10).unwrap();
            let mut outputs = Vec::new();
            for offset in 1..=10_u64 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let seq = batch * 10 + offset;
                let e = event(seq, u128::from(seq), 90 + ((state >> 32) % 20) as i64);
                let out = live.apply(&e).unwrap();
                assert_eq!(out, restarted.apply(&e).unwrap());
                outputs.push(out);
            }
            for out in &outputs {
                if let Some(msg) = &out.mbp {
                    assert!(l2.apply(msg).unwrap());
                    assert!(!l2.apply(msg).unwrap());
                }
                if let Some(msg) = &out.mbo {
                    assert!(l3.apply(msg).unwrap());
                    assert!(!l3.apply(msg).unwrap());
                }
            }
            assert_eq!(live.digest(), restarted.digest());
            for side in [Side::Buy, Side::Sell] {
                assert_eq!(l2.depth(side), live.depth(side));
                assert_eq!(l3.depth(side).unwrap(), live.depth(side));
            }
        }
        if run == 0 {
            reference_digest = Some(live.digest());
        }
        assert_eq!(Some(live.digest()), reference_digest);
    }
}

#[test]
fn snapshot_rejects_invalid_external_depth_and_carries_route_epoch() {
    use matching_runtime::{L2Client, ResumeError};
    let mut builder = FeedBuilder::new(MarketId(1));
    builder.apply(&event(1, 1, 10)).unwrap();
    let mut snapshot = builder.l2_snapshot(9);
    assert_eq!(snapshot.route_epoch, 9);
    snapshot.bids.push((Price::try_new(10).unwrap(), 1));
    assert_eq!(L2Client::from_snapshot(snapshot), Err(ResumeError::Invalid));

    let mut impossible = builder.l2_snapshot(9);
    impossible.feed_seq = 0;
    assert_eq!(
        L2Client::from_snapshot(impossible),
        Err(ResumeError::Invalid)
    );
}

#[test]
fn l3_snapshot_transfers_full_order_state_and_watermarks() {
    use matching_runtime::{L3Client, L3Snapshot};
    let mut builder = FeedBuilder::new(MarketId(1));
    builder.apply(&event(1, 1, 10)).unwrap();
    let snapshot = builder.l3_snapshot(12);
    let bytes = snapshot.encode(4096).unwrap();
    let restored = L3Snapshot::decode(&bytes, 4096, MarketId(1)).unwrap();
    assert_eq!(
        (restored.route_epoch(), restored.watermarks()),
        (12, (1, 1))
    );
    let mut client = L3Client::from_snapshot(restored);
    let next = builder.apply(&event(2, 2, 9)).unwrap().mbo.unwrap();
    client.apply(&next).unwrap();
    assert_eq!(client.depth(Side::Buy).unwrap(), builder.depth(Side::Buy));
}

#[test]
fn l3_rejects_overflow_without_advancing_cursor() {
    use matching_runtime::{L3Client, MboChange, MboMessage, ResumeError};
    let mut builder = FeedBuilder::new(MarketId(1));
    let mut e = event(1, 1, 10);
    if let SourcePayload::OrderAdded { visible, .. } = &mut e.payload {
        *visible = Qty::try_new(u64::MAX).unwrap();
    }
    builder.apply(&e).unwrap();
    let mut client = L3Client::from_snapshot(builder.l3_snapshot(1));
    let bad = MboMessage {
        market_id: MarketId(1),
        feed_seq: 2,
        source_seq: 2,
        change: MboChange::Added {
            order_id: OrderId(2),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            visible: Qty::try_new(1).unwrap(),
        },
    };
    assert_eq!(client.apply(&bad), Err(ResumeError::Invalid));
    assert_eq!(client.depth(Side::Buy).unwrap(), builder.depth(Side::Buy));
    let good = MboMessage {
        change: MboChange::Added {
            order_id: OrderId(2),
            side: Side::Buy,
            price: Price::try_new(9).unwrap(),
            visible: Qty::try_new(1).unwrap(),
        },
        ..bad
    };
    assert!(client.apply(&good).unwrap());
}

#[test]
fn filtered_feed_cursor_detects_only_its_own_gaps() {
    use matching_runtime::{FeedCursor, ResumeError};
    let mut cursor = FeedCursor::new(MarketId(1), 3);
    assert!(!cursor.accept(MarketId(1), 3).unwrap());
    assert!(cursor.accept(MarketId(1), 4).unwrap());
    assert_eq!(cursor.accept(MarketId(1), 6), Err(ResumeError::Gap));
    assert_eq!(cursor.accept(MarketId(2), 5), Err(ResumeError::Market));
    assert!(cursor.accept(MarketId(1), 5).unwrap());
}

#[test]
fn recovery_validates_source_continuity_and_expected_digest() {
    let mut live = FeedBuilder::new(MarketId(1));
    live.apply(&event(1, 1, 10)).unwrap();
    let checkpoint = live.encode_checkpoint(4096).unwrap();
    let suffix = vec![event(2, 2, 9), event(3, 3, 11)];
    for source in &suffix {
        live.apply(source).unwrap();
    }
    let restored =
        FeedBuilder::recover(&checkpoint, 4096, MarketId(1), 3, &suffix, live.digest()).unwrap();
    assert_eq!(restored, live);
    assert!(
        FeedBuilder::recover(
            &checkpoint,
            4096,
            MarketId(1),
            3,
            &suffix[1..],
            live.digest()
        )
        .is_err()
    );
    assert!(FeedBuilder::recover(&checkpoint, 4096, MarketId(1), 3, &suffix, [0; 32]).is_err());
}

#[test]
fn feed_gap_requires_new_snapshot_even_if_missing_message_arrives_later() {
    use matching_runtime::{L2Client, ResumeError};
    let mut b = FeedBuilder::new(MarketId(1));
    b.apply(&event(1, 1, 10)).unwrap();
    let mut c = L2Client::from_snapshot(b.l2_snapshot(1)).unwrap();
    let second = b.apply(&event(2, 2, 9)).unwrap().mbp.unwrap();
    let third = b.apply(&event(3, 3, 8)).unwrap().mbp.unwrap();
    assert_eq!(c.apply(&third), Err(ResumeError::Gap));
    assert_eq!(c.apply(&second), Err(ResumeError::Gap));
}

#[test]
fn random_disconnects_with_add_reduce_remove_and_filtered_events() {
    use matching_runtime::{L2Client, L3Client};
    use std::collections::BTreeMap;
    let mut live = FeedBuilder::new(MarketId(1));
    let mut orders = BTreeMap::<u128, u64>::new();
    let mut rng = 0xdead_beef_1234_5678_u64;
    let mut seq = 0_u64;
    while seq < 3000 {
        let mut l2 = L2Client::from_snapshot(live.l2_snapshot(1)).unwrap();
        let mut l3 = L3Client::from_snapshot(live.l3_snapshot(1));
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let count = 1 + (rng % 23);
        let mut buffered = Vec::new();
        for _ in 0..count {
            if seq == 3000 {
                break;
            }
            seq += 1;
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = u128::from((rng >> 32) % 50);
            let payload = if rng.is_multiple_of(11) {
                SourcePayload::Boundary
            } else if let Some(&qty) = orders.get(&id) {
                if qty > 1 && rng & 1 == 0 {
                    orders.insert(id, qty - 1);
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
                let qty = 1 + ((rng >> 16) % 10);
                orders.insert(id, qty);
                SourcePayload::OrderAdded {
                    order_id: OrderId(id),
                    side: if rng & 2 == 0 { Side::Buy } else { Side::Sell },
                    price: Price::try_new(90 + ((rng >> 24) % 20) as i64).unwrap(),
                    visible: Qty::try_new(qty).unwrap(),
                }
            };
            buffered.push(
                live.apply(&SourceEvent {
                    market_id: MarketId(1),
                    sequence: seq,
                    payload,
                })
                .unwrap(),
            );
        }
        for output in buffered {
            if let Some(m) = output.mbp {
                l2.apply(&m).unwrap();
            }
            if let Some(m) = output.mbo {
                l3.apply(&m).unwrap();
            }
        }
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(l2.depth(side), live.depth(side));
            assert_eq!(l3.depth(side).unwrap(), live.depth(side));
        }
    }
}

#[test]
fn projector_and_feed_checkpoint_handoff_preserves_source_and_feed_sequences() {
    use matching_domain::{EngineEvent, ShardId};
    use matching_runtime::MarketProjector;
    let mut projector = MarketProjector::new(MarketId(1));
    let mut feed = FeedBuilder::new(MarketId(1));
    for id in 1..=2_u128 {
        let sources = projector
            .apply_engine_events(
                &[EngineEvent::OrderOpened {
                    order_id: OrderId(id),
                }],
                |_, _| {
                    Some((
                        Side::Buy,
                        Price::try_new(10 - id as i64).unwrap(),
                        Qty::try_new(2).unwrap(),
                    ))
                },
            )
            .unwrap();
        for source in sources {
            feed.apply(&source).unwrap();
        }
    }
    let boundary = feed.migration_boundary(ShardId(1), ShardId(2), 8);
    let mut target_projector = MarketProjector::decode_checkpoint(
        &projector.encode_checkpoint(4096).unwrap(),
        4096,
        MarketId(1),
        boundary.last_market_event_seq,
    )
    .unwrap();
    let mut target_feed = FeedBuilder::decode_checkpoint(
        &feed.encode_checkpoint(4096).unwrap(),
        4096,
        MarketId(1),
        boundary.last_market_event_seq,
    )
    .unwrap();
    let sources = target_projector
        .apply_engine_events(
            &[EngineEvent::OrderOpened {
                order_id: OrderId(3),
            }],
            |_, _| {
                Some((
                    Side::Buy,
                    Price::try_new(11).unwrap(),
                    Qty::try_new(2).unwrap(),
                ))
            },
        )
        .unwrap();
    assert_eq!(sources[0].sequence, boundary.last_market_event_seq + 1);
    let output = target_feed.apply(&sources[0]).unwrap();
    assert_eq!(output.mbo.unwrap().feed_seq, boundary.last_feed_seq.mbo + 1);
    assert_eq!(output.mbp.unwrap().feed_seq, boundary.last_feed_seq.mbp + 1);
    assert_eq!(output.bbo.unwrap().feed_seq, boundary.last_feed_seq.bbo + 1);
}

#[test]
fn l3_snapshot_exposes_fifo_orders_and_reductions() {
    let mut b = FeedBuilder::new(MarketId(1));
    b.apply(&event(1, 7, 10)).unwrap();
    b.apply(&event(2, 3, 10)).unwrap();
    let snapshot = b.l3_snapshot(1);
    let orders = snapshot.orders();
    assert_eq!(
        orders.iter().map(|o| o.order_id).collect::<Vec<_>>(),
        vec![OrderId(7), OrderId(3)]
    );
    assert_eq!(orders[0].visible.get(), 2);
}

#[test]
fn persisted_builder_checkpoint_roundtrips_and_rejects_wrong_identity() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "tide-t31-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let mut builder = FeedBuilder::new(MarketId(1));
    builder.apply(&event(1, 1, 10)).unwrap();
    builder.write_checkpoint_file(&path, 7, 4096).unwrap();
    let restored = FeedBuilder::read_checkpoint_file(&path, 7, 1, 4096, MarketId(1)).unwrap();
    assert_eq!(restored, builder);
    assert!(FeedBuilder::read_checkpoint_file(&path, 7, 2, 4096, MarketId(1)).is_err());
    assert!(builder.write_checkpoint_file(&path, 7, 4096).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persisted_checkpoint_rejects_truncation_and_ignores_uncommitted_temporary_file() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "tide-t31-crash-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let mut b = FeedBuilder::new(MarketId(1));
    b.apply(&event(1, 1, 10)).unwrap();
    b.write_checkpoint_file(&path, 1, 4096).unwrap();
    std::fs::write(path.join("snapshot-2.tmp"), b"partial").unwrap();
    assert_eq!(
        FeedBuilder::read_checkpoint_file(&path, 1, 1, 4096, MarketId(1)).unwrap(),
        b
    );
    assert!(FeedBuilder::read_checkpoint_file(&path, 2, 1, 4096, MarketId(1)).is_err());
    let file = path.join("snapshot-1.bin");
    let bytes = std::fs::read(&file).unwrap();
    for end in [0, 1, bytes.len() / 2, bytes.len() - 1] {
        std::fs::write(&file, &bytes[..end]).unwrap();
        assert!(FeedBuilder::read_checkpoint_file(&path, 1, 1, 4096, MarketId(1)).is_err());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn matching_and_market_data_digests_repeat_for_one_hundred_command_tapes() {
    use matching_domain::{
        ClusterId, CommandId, EngineEvent, EventEnvelope, LimitOrder, ShardId, StpKey, StpMode,
        UserId, snapshot_wire::SnapshotEncoder,
    };
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{EventHasher, MarketProjector};
    use sha2::{Digest, Sha256};
    let mut expected = None;
    for _ in 0..100 {
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
        let mut projector = MarketProjector::new(MarketId(1));
        let mut builder = FeedBuilder::new(MarketId(1));
        let mut source_hash = Sha256::new();
        let mut event_hash = EventHasher::new();
        for id in 1..=30_u128 {
            let order = LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(id as u64),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(100 - (id % 8) as i64).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            };
            book.rest(order).unwrap();
            event_hash
                .update(&EventEnvelope {
                    cluster_id: ClusterId(1),
                    shard_id: ShardId(1),
                    shard_seq: id as u64,
                    event_index: 0,
                    command_id: CommandId(id),
                    engine_time: 0,
                    market_id: Some(MarketId(1)),
                    payload: EngineEvent::OrderOpened {
                        order_id: OrderId(id),
                    },
                })
                .unwrap();
            let events = projector
                .apply_engine_events(
                    &[EngineEvent::OrderOpened {
                        order_id: OrderId(id),
                    }],
                    |_, id| {
                        book.order(id).map(|n| {
                            (
                                n.original_order().side,
                                n.original_order().price,
                                Qty::try_new(n.visible_remaining()).unwrap(),
                            )
                        })
                    },
                )
                .unwrap();
            for event in events {
                source_hash.update(event.encode(1024).unwrap());
                builder.apply(&event).unwrap();
            }
        }
        let mut writer = SnapshotEncoder::new(1_000_000);
        book.write_canonical_state(&mut writer).unwrap();
        let result = (
            Sha256::digest(writer.finish()).to_vec(),
            builder.digest(),
            source_hash.finalize().to_vec(),
            event_hash.finish(),
        );
        if let Some(reference) = &expected {
            assert_eq!(&result, reference);
        } else {
            expected = Some(result);
        }
    }
}
