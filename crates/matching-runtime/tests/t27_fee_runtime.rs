//! 费率版本更新必须随正式命令持久提交并可确定性重放。
mod t22_support;
use matching_domain::{
    AssetId, ClusterId, Command, CommandEnvelope, CommandId, FeeAssetPolicy, FeeRounding,
    FeeSchedule, FeeTier, MarketId, ShardId, UpdateFeeSchedule,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
    SnapshotRestoreConfig,
};
use std::time::{Duration, Instant};

/// 只重试尚未入队的暂时背压。
fn retry<T>(mut submit: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match submit() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if Instant::now() < until => {
                std::thread::yield_now()
            }
            Err(error) => panic!("请求未入队: {error:?}"),
        }
    }
}

#[test]
fn fee_update_is_durable_and_genesis_replay_matches() {
    fee_update_replays(true);
}

#[test]
fn fee_update_is_durable_with_fsync_each() {
    fee_update_replays(false);
}

#[test]
fn bootstrap_preserves_distinct_legacy_market_versions_and_replays() {
    for group in [false, true] {
        for pending_first in [false, true] {
            bootstrap_legacy_markets(group, pending_first);
        }
    }
}

fn bootstrap_legacy_markets(group: bool, pending_first: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, EngineEvent, LimitOrder, LogicalTime, MassCancelShard, OrderId,
        PlaceLimitOrder, Price, Qty, ReservationId, ReservationReleaseReason, ReservationToken,
        ShardCancelScope, Side, StpKey, StpMode, UserId,
    };
    let tmp = t22_support::TestDirectory::new();
    let snapshots = tmp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let (market_a, market_b, shard_id, cluster) =
        (MarketId(84), MarketId(85), ShardId(84), ClusterId(2));
    let routes = RouteTable::new([market_a, market_b].map(|market_id| MarketRoute {
        market_id,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }))
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap(),
        [
            (
                market_a,
                LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap(),
            ),
            (
                market_b,
                LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 2, 1, 0).unwrap(),
            ),
        ],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let books = vec![
        (market_a, QueuePriorityPolicy::try_new(32).unwrap()),
        (market_b, QueuePriorityPolicy::try_new(32).unwrap()),
    ];
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            books.clone(),
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            books.clone(),
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (tmp.0.join("wal"), tmp.0.join("manifest")),
        )
    }
    .unwrap();
    let command = |id, market, user, payload| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 1,
        client_seq: id as u64,
        market_id: Some(market),
        user_id: user,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload,
    };
    let submit = |command: CommandEnvelope| {
        retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
    };
    let order = |id: u128, market, version| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(id as u64),
            market_id: market,
            reserves: vec![AssetReserve {
                asset: AssetId(1),
                atoms: 2,
            }]
            .into(),
            fee_schedule_version: version,
            fee_tier_id: 1,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        command(
            id,
            market,
            Some(UserId(id as u64)),
            Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: UserId(id as u64),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Sell,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(2).unwrap(),
                },
                client_order_id: None,
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        )
    };
    let a = submit(order(1, market_a, 1));
    let b = submit(order(2, market_b, 2));
    assert!(matches!(
        a.events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    assert!(matches!(
        b.events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    let before = if pending_first {
        Some(submit(command(
            3,
            market_a,
            None,
            Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market {
                    market_id: market_a,
                },
            }),
        )))
    } else {
        None
    };
    if let Some(result) = &before {
        assert!(
            !result
                .events
                .iter()
                .any(|e| matches!(e.payload, EngineEvent::ReservationReleased { .. }))
        );
    }
    let tier = FeeTier {
        maker_bps: 0,
        taker_bps: 0,
        rounding: FeeRounding::Floor,
    };
    let old = FeeSchedule::new(1, FeeAssetPolicy::Quote, [(1, tier)]).unwrap();
    let current = FeeSchedule::new(2, FeeAssetPolicy::Quote, [(1, tier)]).unwrap();
    let missing = submit(command(
        4,
        market_a,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            schedule: current.clone(),
            history: Vec::new(),
        })),
    ));
    assert!(matches!(
        missing.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::FeeScheduleMismatch
        }
    ));
    let bootstrap = submit(command(
        5,
        market_a,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            schedule: current.clone(),
            history: vec![old.clone()],
        })),
    ));
    assert!(matches!(
        bootstrap.events[0].payload,
        EngineEvent::FeeScheduleUpdated { version: 2 }
    ));
    let bad = submit(command(
        6,
        market_a,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            schedule: FeeSchedule::new(3, FeeAssetPolicy::Quote, [(1, tier)]).unwrap(),
            history: vec![old],
        })),
    ));
    assert!(matches!(
        bad.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidOrderCombination
        }
    ));
    let cancel = |id, market| {
        submit(command(
            id,
            market,
            None,
            Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        ))
    };
    let ca = cancel(7, market_a);
    let cb = cancel(8, market_b);
    if pending_first {
        assert!(ca.events.is_empty());
    } else {
        assert!(
            matches!(&ca.events[1].payload, EngineEvent::ReservationReleased { reason: ReservationReleaseReason::MassCancel, released: Some(assets), .. } if assets[0].atoms == 2)
        );
    }
    assert!(
        matches!(&cb.events[1].payload, EngineEvent::ReservationReleased {
        reason: ReservationReleaseReason::MassCancel, released: Some(assets), ..
    } if assets[0].atoms == 2)
    );
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    let encoded = image.encode_snapshot(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 7,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 2,
        max_bytes: 1024 * 1024,
    };
    let decoded = matching_runtime::SnapshotImage::decode_snapshot(&encoded, &trusted).unwrap();
    assert_eq!(decoded.state_digest(1024 * 1024).unwrap(), digest);
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &books,
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    let mut executions = vec![a, b];
    executions.extend(before);
    executions.extend([missing, bootstrap, bad, ca, cb]);
    for (i, live) in executions.iter().enumerate() {
        assert_eq!(replay.executions[i].events, live.events);
    }
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
}

#[test]
fn signed_orders_with_active_fee_schedule_settle_and_replay() {
    signed_orders_replay(false);
    signed_orders_replay(true);
}

fn signed_orders_replay(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, EngineEvent, LimitOrder, LogicalTime, OrderId, PlaceLimitOrder, Price, Qty,
        ReservationId, ReservationToken, Side, StpKey, StpMode, UserId,
    };
    let tmp = t22_support::TestDirectory::new();
    let snapshots = tmp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let (market, shard_id, cluster) = (MarketId(83), ShardId(83), ClusterId(2));
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(32).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(32).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (tmp.0.join("wal"), tmp.0.join("manifest")),
        )
    }
    .unwrap();
    let command = |id, user, payload| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 1,
        client_seq: id as u64,
        market_id: Some(market),
        user_id: user,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload,
    };
    let submit = |command: CommandEnvelope| {
        retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
    };
    let updated = submit(command(
        1,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 1_000,
                        taker_bps: 500,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    let order = |id: u128, side: Side, qty: u64, reserves| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(id as u64),
            market_id: market,
            reserves: if (5..=6).contains(&id) {
                vec![
                    AssetReserve {
                        asset: AssetId(2),
                        atoms: 20,
                    },
                    AssetReserve {
                        asset: AssetId(3),
                        atoms: reserves - 20,
                    },
                ]
                .into()
            } else {
                vec![AssetReserve {
                    asset: if side == Side::Sell {
                        AssetId(1)
                    } else {
                        AssetId(2)
                    },
                    atoms: reserves,
                }]
                .into()
            },
            fee_schedule_version: if id >= 8 {
                3
            } else if id >= 5 {
                2
            } else {
                1
            },
            fee_tier_id: 1,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        command(
            id,
            Some(UserId(id as u64)),
            Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: UserId(id as u64),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(qty).unwrap(),
                },
                client_order_id: None,
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        )
    };
    let maker = submit(order(2, Side::Sell, 1, 1));
    assert!(matches!(
        maker.events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    let maker2 = submit(order(3, Side::Sell, 1, 1));
    assert!(matches!(
        maker2.events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    let updated2 = submit(command(
        4,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                2,
                FeeAssetPolicy::ThirdPriced {
                    asset: AssetId(3),
                    quote_asset: AssetId(2),
                    numerator: 1,
                    denominator: 1,
                },
                [(
                    1,
                    FeeTier {
                        maker_bps: 3_000,
                        taker_bps: 500,
                        rounding: FeeRounding::Ceiling,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    let rejected = submit(order(5, Side::Buy, 2, 25));
    assert!(matches!(
        rejected.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InsufficientReservation
        }
    ));
    let taker = submit(order(6, Side::Buy, 2, 26));
    assert_eq!(
        taker
            .events
            .iter()
            .filter(|e| matches!(e.payload, EngineEvent::Trade(_)))
            .count(),
        2
    );
    assert_eq!(
        taker
            .events
            .iter()
            .filter_map(|e| match &e.payload {
                EngineEvent::Trade(trade) => Some(trade.maker_order_id),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![OrderId(2), OrderId(3)]
    );
    assert_eq!(taker.events.iter().filter(|e| matches!(&e.payload, EngineEvent::SettlementDelta(delta) if delta.maker_fee.amount.get() == 1 && delta.taker_fee.amount.get() == 1 && delta.maker_fee.fee_schedule_version == 1 && delta.taker_fee.fee_schedule_version == 2)).count(), 2);
    assert!(taker.events.iter().any(|e| matches!(&e.payload, EngineEvent::ReservationReleased { order_id: OrderId(6), released: Some(assets), .. } if assets == &vec![AssetReserve { asset: AssetId(3), atoms: 4 }])));
    let mut ledger = matching_risk::ReferenceLedger::new(
        UserId(99),
        [
            (UserId(2), AssetId(1), 1),
            (UserId(3), AssetId(1), 1),
            (UserId(6), AssetId(2), 20),
            (UserId(6), AssetId(3), 6),
        ],
    )
    .unwrap();
    for settlement in taker.events.iter().filter_map(|e| {
        if let EngineEvent::SettlementDelta(value) = &e.payload {
            Some(value)
        } else {
            None
        }
    }) {
        ledger.apply_event(settlement).unwrap();
    }
    assert_eq!(
        (
            ledger.balance(UserId(2), AssetId(2)),
            ledger.balance(UserId(6), AssetId(1))
        ),
        (9, 2)
    );
    assert_eq!(
        (
            ledger.balance(UserId(99), AssetId(2)),
            ledger.balance(UserId(99), AssetId(3))
        ),
        (2, 2)
    );
    let updated3 = submit(command(
        7,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                3,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::Floor,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    let stp_maker = submit(order(8, Side::Sell, 1, 1));
    let mut stp_taker_command = order(9, Side::Buy, 1, 10);
    stp_taker_command.user_id = Some(UserId(8));
    if let Command::PlaceLimit(request) = &mut stp_taker_command.payload {
        request.order.user_id = UserId(8);
        request.order.stp_mode = StpMode::CancelMaker;
        request.reservation.user_id = UserId(8);
        request.reservation.signature = signing
            .sign(&request.reservation.canonical_signing_payload().unwrap())
            .to_bytes();
    }
    let stp_taker = submit(stp_taker_command);
    assert!(!stp_taker.events.iter().any(|event| matches!(
        event.payload,
        EngineEvent::Trade(_) | EngineEvent::SettlementDelta(_)
    )));
    assert!(stp_taker.events.iter().any(
        |event| matches!(&event.payload, EngineEvent::ReservationReleased {
        order_id:OrderId(8), reason:matching_domain::ReservationReleaseReason::SelfTradePrevention,
        released:Some(assets), .. } if assets[0].atoms == 1)
    ));
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 7,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(32).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions[0].events, updated.events);
    assert_eq!(replay.executions[1].events, maker.events);
    assert_eq!(replay.executions[2].events, maker2.events);
    assert_eq!(replay.executions[3].events, updated2.events);
    assert_eq!(replay.executions[4].events, rejected.events);
    assert_eq!(replay.executions[5].events, taker.events);
    assert_eq!(replay.executions[6].events, updated3.events);
    assert_eq!(replay.executions[7].events, stp_maker.events);
    assert_eq!(replay.executions[8].events, stp_taker.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    let mut hash = matching_runtime::EventHasher::new();
    for event in updated
        .events
        .iter()
        .chain(maker.events.iter())
        .chain(maker2.events.iter())
        .chain(updated2.events.iter())
        .chain(rejected.events.iter())
        .chain(taker.events.iter())
        .chain(updated3.events.iter())
        .chain(stp_maker.events.iter())
        .chain(stp_taker.events.iter())
    {
        hash.update(event).unwrap();
    }
    assert_eq!(replay.event_hash, hash.finish());
}

#[test]
fn closing_market_releases_real_reserves_after_fee_activation() {
    close_market_replays(false);
    close_market_replays(true);
}

fn close_market_replays(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, CloseMarket, ClosePolicy, CreateMarket, EngineEvent, ExecutionLimits,
        LimitOrder, LogicalTime, MarketConfig, OpenMarket, OrderId, OrderLimits, PlaceLimitOrder,
        Price, Qty, QuoteAmount, ReservationId, ReservationReleaseReason, ReservationToken, Side,
        StartSession, StpKey, StpMode, UserId,
    };
    let tmp = t22_support::TestDirectory::new();
    let snapshots = tmp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let (market, shard_id, cluster) = (MarketId(82), ShardId(82), ClusterId(2));
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .require_managed_markets();
    let config = MarketConfig {
        market_id: market,
        price_scale: 0,
        qty_scale: 0,
        quote_scale: 0,
        tick_size: 1,
        lot_size: 1,
        order_limits: OrderLimits {
            min_qty: Qty::try_new(1).unwrap(),
            max_qty: Qty::try_new(10).unwrap(),
            min_notional: QuoteAmount::new(1),
            max_notional: QuoteAmount::new(100),
        },
        price_band: None,
        active_fee_schedule_version: 1,
        close_policy: ClosePolicy::CancelAll,
        execution_limits: ExecutionLimits {
            max_internal_actions_per_command: 10,
            max_trades_per_command: 10,
            priority_exhaustion_guard: 1,
            synthetic_priority_ceiling: None,
        },
    };
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (tmp.0.join("wal"), tmp.0.join("manifest")),
        )
    }
    .unwrap();
    let command = |id, payload| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 1,
        client_seq: id as u64,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload,
    };
    let submit = |command: CommandEnvelope| {
        retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
    };
    submit(command(
        1,
        Command::CreateMarket(Box::new(CreateMarket {
            config: config.clone(),
        })),
    ));
    submit(command(
        2,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 9,
        }),
    ));
    let mut token = ReservationToken {
        token_id: 3,
        command_id: CommandId(3),
        reservation_id: ReservationId(3),
        order_id: OrderId(3),
        user_id: UserId(7),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 2,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 31,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut order = command(
        3,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(3),
                user_id: UserId(7),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    order.user_id = Some(UserId(7));
    assert!(matches!(
        submit(order).events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    submit(command(
        4,
        Command::OpenMarket(OpenMarket {
            market_id: market,
            session_id: 9,
        }),
    ));
    let wrong_first = submit(command(
        5,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                2,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::Floor,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    assert!(matches!(
        wrong_first.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::FeeScheduleMismatch
        }
    ));
    submit(command(
        6,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                1,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::Floor,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    submit(command(
        7,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule: FeeSchedule::new(
                2,
                FeeAssetPolicy::Quote,
                [(
                    1,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::Floor,
                    },
                )],
            )
            .unwrap(),
        })),
    ));
    let mut old_version_token = ReservationToken {
        token_id: 8,
        command_id: CommandId(8),
        reservation_id: ReservationId(8),
        order_id: OrderId(8),
        user_id: UserId(8),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 1,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 31,
        signature: [0; 64],
    };
    old_version_token.signature = signing
        .sign(&old_version_token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut old_version_order = command(
        8,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(8),
                user_id: UserId(8),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: old_version_token,
        })),
    );
    old_version_order.user_id = Some(UserId(8));
    assert!(matches!(
        submit(old_version_order).events.last().unwrap().payload,
        EngineEvent::OrderOpened {
            order_id: OrderId(8)
        }
    ));
    let mut next_config = config;
    next_config.active_fee_schedule_version = 2;
    let config_update = submit(command(
        9,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: next_config,
        })),
    ));
    assert!(matches!(
        config_update.events[0].payload,
        EngineEvent::MarketConfigUpdated { .. }
    ));
    let closed = submit(command(
        10,
        Command::CloseMarket(CloseMarket { market_id: market }),
    ));
    assert!(matches!(
        &closed.events[0].payload,
        EngineEvent::OrderCanceled {
            order_id: OrderId(3),
            ..
        }
    ));
    assert!(
        matches!(&closed.events[1].payload, EngineEvent::ReservationReleased { order_id: OrderId(3),
        reason: ReservationReleaseReason::MarketClosed, released: Some(assets), .. } if assets[0].atoms == 2)
    );
    assert!(closed.events.iter().any(|event| matches!(&event.payload, EngineEvent::ReservationReleased { order_id: OrderId(8),
        reason: ReservationReleaseReason::MarketClosed, released: Some(assets), .. } if assets[0].atoms == 1)));
    let digest = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 7,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions.last().unwrap().events, closed.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
}

#[test]
fn mass_cancel_releases_actual_reserve_and_replays_after_fee_activation() {
    mass_cancel_replays(false);
    mass_cancel_replays(true);
}

fn mass_cancel_replays(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, EngineEvent, LimitOrder, LogicalTime, MassCancelShard, OrderId,
        PlaceLimitOrder, Price, Qty, ReservationId, ReservationReleaseReason, ReservationToken,
        ShardCancelScope, Side, StpKey, StpMode, UserId,
    };
    let tmp = t22_support::TestDirectory::new();
    let snapshots = tmp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let (market, shard_id, cluster) = (MarketId(81), ShardId(81), ClusterId(2));
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(32).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(32).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (tmp.0.join("wal"), tmp.0.join("manifest")),
        )
    }
    .unwrap();
    let mut token = ReservationToken {
        token_id: 1,
        command_id: CommandId(1),
        reservation_id: ReservationId(1),
        order_id: OrderId(1),
        user_id: UserId(1),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 2,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 31,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: Some(market),
        user_id: Some(UserId(1)),
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(1),
                user_id: UserId(1),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Sell,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    };
    let opened = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(matches!(
        opened.events.last().unwrap().payload,
        EngineEvent::OrderOpened { .. }
    ));
    command.command_id = CommandId(2);
    command.client_seq = 2;
    command.user_id = None;
    command.payload = Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
        history: Vec::new(),
        schedule: FeeSchedule::new(
            1,
            FeeAssetPolicy::Quote,
            [(
                1,
                FeeTier {
                    maker_bps: 0,
                    taker_bps: 0,
                    rounding: FeeRounding::Floor,
                },
            )],
        )
        .unwrap(),
    }));
    let updated = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    command.command_id = CommandId(3);
    command.client_seq = 3;
    command.payload = Command::MassCancelShard(MassCancelShard {
        scope: ShardCancelScope::Market { market_id: market },
    });
    let canceled = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(
        matches!(canceled.events.as_slice(), [a, b] if matches!(a.payload, EngineEvent::OrderCanceled { .. }) &&
        matches!(&b.payload, EngineEvent::ReservationReleased { order_id: OrderId(1), reason: ReservationReleaseReason::MassCancel,
            released: Some(assets), .. } if assets == &vec![AssetReserve { asset: AssetId(1), atoms: 2 }]))
    );
    let digest = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 7,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(32).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions[0].events, opened.events);
    assert_eq!(replay.executions[1].events, updated.events);
    assert_eq!(replay.executions[2].events, canceled.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
}

fn fee_update_replays(group: bool) {
    let tmp = t22_support::TestDirectory::new();
    let snapshots = tmp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let market = MarketId(if group { 76 } else { 77 });
    let shard_id = ShardId(if group { 76 } else { 77 });
    let cluster = ClusterId(2);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (
                ReservationRegistry::new([]).unwrap(),
                matching_domain::LogicalTime::new(0),
            ),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (
                ReservationRegistry::new([]).unwrap(),
                matching_domain::LogicalTime::new(0),
            ),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (tmp.0.join("wal"), tmp.0.join("manifest")),
        )
    }
    .unwrap();
    let schedule = FeeSchedule::new(
        1,
        FeeAssetPolicy::Quote,
        [(
            1,
            FeeTier {
                maker_bps: 0,
                taker_bps: 0,
                rounding: FeeRounding::TowardZero,
            },
        )],
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: Vec::new(),
            schedule,
        })),
    };
    let result = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(result.seq, 1);
    assert_eq!(
        result.events[0].payload,
        matching_domain::EngineEvent::FeeScheduleUpdated { version: 1 }
    );
    let duplicate = retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.seq, result.seq);
    assert!(duplicate.events.is_empty());
    let mut stale = command.clone();
    stale.command_id = CommandId(2);
    stale.client_seq = 2;
    let rejected = retry(|| shard.handle().submit_command(stale.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(rejected.seq, 2);
    assert_eq!(
        rejected.events[0].payload,
        matching_domain::EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::FeeScheduleMismatch
        }
    );
    let next_schedule = FeeSchedule::new(
        2,
        FeeAssetPolicy::ThirdPriced {
            asset: AssetId(3),
            quote_asset: AssetId(2),
            numerator: 3,
            denominator: 2,
        },
        [(
            1,
            FeeTier {
                maker_bps: -1,
                taker_bps: 2,
                rounding: FeeRounding::Floor,
            },
        )],
    )
    .unwrap();
    let mut next = command.clone();
    next.command_id = CommandId(3);
    next.client_seq = 3;
    next.payload = Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
        history: Vec::new(),
        schedule: next_schedule,
    }));
    let updated = retry(|| shard.handle().submit_command(next.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(updated.seq, 3);
    assert_eq!(
        updated.events[0].payload,
        matching_domain::EngineEvent::FeeScheduleUpdated { version: 2 }
    );
    let mut order = command.clone();
    order.command_id = CommandId(4);
    order.client_seq = 4;
    order.user_id = Some(matching_domain::UserId(8));
    order.payload = Command::PlaceLimit(Box::new(matching_domain::PlaceLimitOrder {
        order: matching_domain::LimitOrder {
            order_id: matching_domain::OrderId(4),
            user_id: matching_domain::UserId(8),
            stp_key: matching_domain::StpKey::new(0),
            stp_mode: matching_domain::StpMode::None,
            side: matching_domain::Side::Buy,
            price: matching_domain::Price::try_new(10).unwrap(),
            qty: matching_domain::Qty::try_new(1).unwrap(),
        },
        client_order_id: None,
        user_control_epoch: 0,
        post_only: false,
        reservation: matching_domain::ReservationToken {
            token_id: 4,
            command_id: CommandId(4),
            reservation_id: matching_domain::ReservationId(4),
            order_id: matching_domain::OrderId(4),
            user_id: matching_domain::UserId(8),
            market_id: market,
            reserves: vec![matching_domain::AssetReserve {
                asset: AssetId(2),
                atoms: 10,
            }]
            .into(),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_key_id: 1,
            signature: [0; 64],
        },
    }));
    let blocked = retry(|| shard.handle().submit_command(order.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        blocked.events[0].payload,
        matching_domain::EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::FeeScheduleMismatch
        }
    );
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    let encoded = image.encode_snapshot(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = SnapshotRestoreConfig {
        cluster,
        shard: shard_id,
        primary_epoch: 7,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let decoded = matching_runtime::SnapshotImage::decode_snapshot(&encoded, &trusted).unwrap();
    assert_eq!(decoded.state_digest(1024 * 1024).unwrap(), digest);
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions[0].events, result.events);
    assert_eq!(replay.executions[1].events, rejected.events);
    assert_eq!(replay.executions[2].events, updated.events);
    assert_eq!(replay.executions[3].events, blocked.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    let mut hasher = matching_runtime::EventHasher::new();
    for event in result
        .events
        .iter()
        .chain(rejected.events.iter())
        .chain(updated.events.iter())
        .chain(blocked.events.iter())
    {
        hasher.update(event).unwrap();
    }
    assert_eq!(replay.event_hash, hasher.finish());
    drop(manager);
    let mut recovered = Shard::recover_group_local_with_genesis(
        tmp.0.join("wal"),
        tmp.0.join("manifest"),
        &snapshots,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
        (4, 10_000),
    )
    .unwrap();
    let mut writer =
        matching_runtime::SnapshotWriter::start(recovered.handle(), snapshots.clone(), 1024 * 1024)
            .unwrap();
    let checkpoint = retry(|| recovered.handle().take_snapshot(2))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer
        .submit(2, checkpoint)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let tail_schedule = FeeSchedule::new(
        3,
        FeeAssetPolicy::ThirdPriced {
            asset: AssetId(3),
            quote_asset: AssetId(2),
            numerator: 5,
            denominator: 4,
        },
        [(
            1,
            FeeTier {
                maker_bps: 1,
                taker_bps: 3,
                rounding: FeeRounding::Ceiling,
            },
        )],
    )
    .unwrap();
    let mut tail = command.clone();
    tail.command_id = CommandId(5);
    tail.client_seq = 5;
    tail.payload = Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
        history: Vec::new(),
        schedule: tail_schedule,
    }));
    let tail_result = retry(|| recovered.handle().submit_command(tail.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let tail_digest = retry(|| recovered.handle().take_snapshot(3))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    writer.shutdown().unwrap();
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    let snapshot_file = snapshots.join("snapshot-2.bin");
    let good_snapshot = std::fs::read(&snapshot_file).unwrap();
    let wal_file = tmp.0.join("wal").join("00000000000000000001.wal");
    let wal_before = std::fs::read(&wal_file).unwrap();
    let mut corrupted = good_snapshot.clone();
    corrupted[35] ^= 1;
    std::fs::write(&snapshot_file, corrupted).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    assert!(
        matching_runtime::recover_snapshot_records(
            tmp.0.join("wal"),
            &snapshots,
            &manager,
            &trusted
        )
        .is_err()
    );
    drop(manager);
    assert_eq!(std::fs::read(&wal_file).unwrap(), wal_before);
    std::fs::write(&snapshot_file, good_snapshot).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records(
        tmp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions.len(), 1);
    assert_eq!(replay.executions[0].events, tail_result.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), tail_digest);
    let mut hasher = matching_runtime::EventHasher::new();
    for event in &tail_result.events {
        hasher.update(event).unwrap();
    }
    assert_eq!(replay.event_hash, hasher.finish());
    drop(manager);
    let mut corrupt_wal = std::fs::read(&wal_file).unwrap();
    let last = corrupt_wal.len() - 1;
    corrupt_wal[last] ^= 1;
    std::fs::write(&wal_file, &corrupt_wal).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(tmp.0.join("manifest")).unwrap();
    assert!(
        matching_runtime::recover_snapshot_records(
            tmp.0.join("wal"),
            &snapshots,
            &manager,
            &trusted
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&wal_file).unwrap(), corrupt_wal);
}
