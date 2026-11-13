//! 验证市场管理命令的持久化、重放及失败关闭边界。
mod t22_support;
use matching_domain::{
    ClusterId, Command, CommandEnvelope, CommandId, LogicalTime, MarketId, ShardId, StartSession,
};
use matching_risk::ReservationRegistry;
use matching_runtime::{IdempotencyConfig, Shard};
use std::time::{Duration, Instant};

/// 仅重试尚未入队的暂时背压。
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
fn trade_limit_halts_before_uncommitted_mutation_is_published() {
    trade_limit_halts(true);
}

#[test]
fn fsync_each_trade_limit_halts_before_uncommitted_mutation_is_published() {
    trade_limit_halts(false);
}

fn trade_limit_halts(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetId, AssetReserve, ClosePolicy, CreateMarket, ExecutionLimits, LimitOrder,
        MarketConfig, OpenMarket, OrderId, OrderLimits, PlaceLimitOrder, Price, Qty, QuoteAmount,
        ReservationId, ReservationToken, Side, StpKey, StpMode, UserId,
    };
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, RiskKeySet};
    use matching_runtime::{MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable};
    let temp = t22_support::TestDirectory::new();
    let market = MarketId(if group { 87 } else { 86 });
    let shard_id = ShardId(if group { 87 } else { 86 });
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
    let signing = SigningKey::from_bytes(&[6; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap();
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
            max_internal_actions_per_command: 100,
            max_trades_per_command: 1,
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
            ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (temp.0.join("wal"), temp.0.join("manifest")),
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
    for item in [
        command(1, Command::CreateMarket(Box::new(CreateMarket { config }))),
        command(
            2,
            Command::StartSession(StartSession {
                market_id: market,
                new_session_id: 1,
            }),
        ),
        command(
            3,
            Command::OpenMarket(OpenMarket {
                market_id: market,
                session_id: 1,
            }),
        ),
    ] {
        retry(|| shard.handle().submit_command(item.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    let order = |id, user, side, qty| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(user),
            market_id: market,
            reserves: vec![AssetReserve {
                asset: if side == Side::Sell {
                    AssetId(1)
                } else {
                    AssetId(2)
                },
                atoms: if side == Side::Sell {
                    qty as u128
                } else {
                    qty as u128 * 10
                },
            }]
            .into(),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_key_id: 1,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let mut cmd = command(
            id,
            Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: UserId(user),
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
        );
        cmd.user_id = Some(UserId(user));
        cmd
    };
    for item in [order(4, 1, Side::Sell, 1), order(5, 2, Side::Sell, 1)] {
        let result = retry(|| shard.handle().submit_command(item.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(matches!(
            result.outcome,
            matching_runtime::FormalOutcome::Placed(_)
        ));
    }
    let within_limit = order(6, 3, Side::Buy, 1);
    let result = retry(|| shard.handle().submit_command(within_limit.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        result
            .events
            .iter()
            .filter(|event| matches!(event.payload, matching_domain::EngineEvent::Trade(_)))
            .count(),
        1
    );
    let replacement = order(7, 4, Side::Sell, 1);
    retry(|| shard.handle().submit_command(replacement.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let incoming = order(8, 5, Side::Buy, 2);
    let reply = retry(|| shard.handle().submit_command(incoming.clone()));
    assert!(reply.recv_timeout(Duration::from_secs(5)).is_err());
    assert_eq!(
        shard.shutdown(Duration::from_secs(5)),
        Err(matching_runtime::ShardError::Failed)
    );
    let records = matching_persistence::scan_segments(temp.0.join("wal"))
        .unwrap()
        .records;
    assert_eq!(records.len(), 7);
    assert_eq!(records.last().unwrap().shard_seq, 7);
    let snapshots = temp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
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
    let mut recovered = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        snapshots,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
        (4, 10_000),
    )
    .unwrap();
    for id in [5, 7] {
        assert!(
            retry(|| recovered.handle().order(market, OrderId(id)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .is_some()
        );
    }
    assert!(
        retry(|| recovered.handle().order(market, OrderId(8)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .is_none()
    );
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    let forged = t22_support::TestDirectory::new();
    let forged_wal = forged.0.join("wal");
    let forged_manifest = forged.0.join("manifest");
    let forged_snapshots = forged.0.join("snapshot");
    std::fs::create_dir(&forged_snapshots).unwrap();
    let writer = matching_persistence::SegmentWriter::new(&forged_wal, 64 * 1024 * 1024).unwrap();
    let manager =
        matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open(
            &forged_manifest,
        )
        .unwrap();
    let appender = matching_persistence::RegisteredAppender::new(writer, manager).unwrap();
    let durability = matching_persistence::DurabilityConfig {
        profile: matching_persistence::Profile::Development,
        mode: matching_persistence::DurabilityMode::FsyncEach,
        replication: matching_persistence::ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batcher =
        matching_persistence::DurableBatcher::new_with_config(appender, durability, 7).unwrap();
    for record in &records {
        batcher.append_at(record, Instant::now()).unwrap();
    }
    let over_limit = matching_persistence::WalRecord {
        cluster_id: cluster.0,
        shard_id: shard_id.0,
        flags: 0,
        epoch: 7,
        shard_seq: 8,
        engine_time: 0,
        command_id: 8,
        payload: matching_domain::encode_wal_command(&incoming).unwrap(),
    };
    batcher.append_at(&over_limit, Instant::now()).unwrap();
    drop(batcher);
    let wal_file = forged_wal.join("00000000000000000001.wal");
    let before = std::fs::read(&wal_file).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(&forged_manifest).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        &forged_wal,
        &forged_snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap();
    assert!(replay.replay().is_err());
    drop(manager);
    assert!(
        Shard::recover_group_local_with_genesis(
            &forged_wal,
            &forged_manifest,
            &forged_snapshots,
            &trusted,
            &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (4, 10_000)
        )
        .is_err()
    );
    assert_eq!(std::fs::read(wal_file).unwrap(), before);
}

#[test]
fn invalid_create_configurations_are_durable_and_replayed() {
    use matching_domain::{
        AssetId, ClosePolicy, CreateMarket, EngineEvent, ExecutionLimits, MarketConfig,
        OrderLimits, Qty, QuoteAmount, RejectReason,
    };
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, RiskKeySet};
    use matching_runtime::{MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable};
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let market = MarketId(89);
    let shard_id = ShardId(89);
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
    let mut shard = Shard::start_group_place_support(
        shard_id,
        cluster,
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 7, 8, place.clone()),
        ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
    )
    .unwrap();
    let mut results = Vec::new();
    for (index, reason) in [
        RejectReason::MarketConfigInvalidTick,
        RejectReason::MarketConfigInvalidLot,
        RejectReason::MarketConfigUnsupportedScale,
        RejectReason::MarketConfigNonIntegralQuoteUnit,
        RejectReason::MarketConfigArithmeticOverflow,
        RejectReason::InvalidSlippageBps,
    ]
    .into_iter()
    .enumerate()
    {
        let mut bad = config.clone();
        match index {
            0 => bad.tick_size = 0,
            1 => bad.lot_size = 0,
            2 => bad.quote_scale = 39,
            3 => bad.price_scale = 1,
            4 => {
                bad.quote_scale = 38;
                bad.tick_size = 4;
            }
            5 => {
                bad.price_band = Some(matching_domain::PriceBandConfig {
                    reference_kind: matching_domain::PriceReferenceKind::LastTrade,
                    max_deviation_bps: 10_000,
                })
            }
            _ => unreachable!(),
        }
        let id = index as u128 + 1;
        let command = CommandEnvelope {
            command_id: CommandId(id),
            session_id: 1,
            client_seq: id as u64,
            market_id: Some(market),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::CreateMarket(Box::new(CreateMarket { config: bad })),
        };
        let result = retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(result.seq, id as u64);
        assert_eq!(
            result.events[0].payload,
            EngineEvent::CommandRejected { reason }
        );
        results.push(result);
    }
    let digest = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
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
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    for (actual, expected) in replay.executions.iter().zip(&results) {
        assert_eq!(actual.events, expected.events);
    }
    assert_eq!(replay.executions.len(), results.len());
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    let mut event_hash = matching_runtime::EventHasher::new();
    for event in results.iter().flat_map(|result| result.events.iter()) {
        event_hash.update(event).unwrap();
    }
    assert_eq!(replay.event_hash, event_hash.finish());
}

#[test]
fn unused_market_can_update_price_scale_and_replay() {
    unused_market_scale_replays(true);
}

#[test]
fn fsync_each_unused_market_can_update_price_scale_and_replay() {
    unused_market_scale_replays(false);
}

fn unused_market_scale_replays(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetId, ClosePolicy, CreateMarket, EngineEvent, ExecutionLimits, MarketConfig,
        OrderLimits, Qty, QuoteAmount, UpdateMarket,
    };
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, RiskKeySet};
    use matching_runtime::{MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable};
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let market = MarketId(if group { 88 } else { 85 });
    let shard_id = ShardId(if group { 88 } else { 85 });
    let cluster = ClusterId(2);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = matching_domain::MarketMath::try_new(0, 0, 0, 10, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[9; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .require_managed_markets();
    let mut config = MarketConfig {
        market_id: market,
        price_scale: 0,
        qty_scale: 0,
        quote_scale: 0,
        tick_size: 10,
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
    let mut unmanaged_book =
        matching_orderbook::ProductionOrderBook::new(QueuePriorityPolicy::try_new(1).unwrap());
    unmanaged_book.bind_market(market).unwrap();
    assert!(matches!(
        Shard::start_with_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, unmanaged_book)],
            ReservationRegistry::new([]).unwrap(),
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 7, 8, place.clone())
        ),
        Err(matching_runtime::ShardError::InvalidConfig)
    ));
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (temp.0.join("wal"), temp.0.join("manifest")),
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
    assert_eq!(
        retry(|| shard.handle().order(market, matching_domain::OrderId(90)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Err(matching_domain::RejectReason::MarketNotFound)
    );
    let token = matching_domain::ReservationToken {
        token_id: 90,
        command_id: CommandId(90),
        reservation_id: matching_domain::ReservationId(90),
        order_id: matching_domain::OrderId(90),
        user_id: matching_domain::UserId(7),
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
    };
    let mut uncreated = command(
        90,
        Command::PlaceLimit(Box::new(matching_domain::PlaceLimitOrder {
            order: matching_domain::LimitOrder {
                order_id: matching_domain::OrderId(90),
                user_id: matching_domain::UserId(7),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                side: matching_domain::Side::Buy,
                price: matching_domain::Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    uncreated.user_id = Some(matching_domain::UserId(7));
    uncreated.session_id = 3;
    uncreated.client_seq = 1;
    let uncreated_result = retry(|| shard.handle().submit_command(uncreated.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(uncreated_result.seq, 1);
    assert_eq!(
        uncreated_result.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::MarketNotFound
        }
    );
    let mut missing_cancel = command(
        91,
        Command::MassCancelShard(matching_domain::MassCancelShard {
            scope: matching_domain::ShardCancelScope::Market { market_id: market },
        }),
    );
    missing_cancel.session_id = 4;
    missing_cancel.client_seq = 1;
    let missing_cancelled = retry(|| shard.handle().submit_command(missing_cancel.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(missing_cancelled.seq, 2);
    assert_eq!(
        missing_cancelled.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::MarketNotFound
        }
    );
    let uncreated_image = retry(|| shard.handle().take_snapshot(77))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let uncreated_digest = uncreated_image.state_digest(1024 * 1024).unwrap();
    let uncreated_bytes = uncreated_image.encode_snapshot(1024 * 1024).unwrap();
    let create = command(
        1,
        Command::CreateMarket(Box::new(CreateMarket {
            config: config.clone(),
        })),
    );
    let created = retry(|| shard.handle().submit_command(create.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(created.seq, 3);
    assert_eq!(
        created.events[0].payload,
        EngineEvent::MarketCreated { market_id: market }
    );
    assert!(
        retry(|| shard.handle().order(market, matching_domain::OrderId(90)))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .is_none()
    );
    let repeated = retry(|| shard.handle().submit_command(missing_cancel.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(repeated.deduplicated);
    assert_eq!(repeated.summary, missing_cancelled.summary);
    assert!(repeated.events.is_empty());
    let repeated_place = retry(|| shard.handle().submit_command(uncreated.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(repeated_place.deduplicated);
    assert_eq!(repeated_place.summary, uncreated_result.summary);
    assert!(repeated_place.events.is_empty());
    let mut conflict = missing_cancel.clone();
    conflict.payload = Command::MassCancelShard(matching_domain::MassCancelShard {
        scope: matching_domain::ShardCancelScope::Market {
            market_id: MarketId(999),
        },
    });
    assert_eq!(
        retry(|| shard.handle().submit_command(conflict.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err(),
        matching_runtime::FormalError::ProtocolError
    );
    let mut closed_cancel = command(
        92,
        Command::MassCancelShard(matching_domain::MassCancelShard {
            scope: matching_domain::ShardCancelScope::Market { market_id: market },
        }),
    );
    closed_cancel.session_id = 5;
    closed_cancel.client_seq = 1;
    let closed_cancelled = retry(|| shard.handle().submit_command(closed_cancel.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(closed_cancelled.seq, 4);
    assert_eq!(
        closed_cancelled.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::MarketClosed
        }
    );
    let rejected_token = matching_domain::ReservationToken {
        token_id: 88,
        command_id: CommandId(88),
        reservation_id: matching_domain::ReservationId(88),
        order_id: matching_domain::OrderId(88),
        user_id: matching_domain::UserId(7),
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
    };
    let mut before_open = command(
        88,
        Command::PlaceLimit(Box::new(matching_domain::PlaceLimitOrder {
            order: matching_domain::LimitOrder {
                order_id: matching_domain::OrderId(88),
                user_id: matching_domain::UserId(7),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                side: matching_domain::Side::Buy,
                price: matching_domain::Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: rejected_token,
        })),
    );
    before_open.user_id = Some(matching_domain::UserId(7));
    before_open.session_id = 2;
    before_open.client_seq = 1;
    let early_reject = retry(|| shard.handle().submit_command(before_open.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        early_reject.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::MarketClosed
        }
    );
    config.price_scale = 1;
    config.execution_limits.priority_exhaustion_guard = 2;
    let update = command(
        2,
        Command::UpdateMarket(Box::new(UpdateMarket {
            config: config.clone(),
        })),
    );
    let updated = retry(|| shard.handle().submit_command(update.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.events[0].payload,
        EngineEvent::MarketConfigUpdated { market_id: market }
    );
    let start = command(
        3,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 3,
        }),
    );
    let started = retry(|| shard.handle().submit_command(start.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let mut token = matching_domain::ReservationToken {
        token_id: 4,
        command_id: CommandId(4),
        reservation_id: matching_domain::ReservationId(4),
        order_id: matching_domain::OrderId(4),
        user_id: matching_domain::UserId(7),
        market_id: market,
        reserves: vec![matching_domain::AssetReserve {
            asset: AssetId(2),
            atoms: 2,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut place_order = command(
        4,
        Command::PlaceLimit(Box::new(matching_domain::PlaceLimitOrder {
            order: matching_domain::LimitOrder {
                order_id: matching_domain::OrderId(4),
                user_id: matching_domain::UserId(7),
                stp_key: matching_domain::StpKey::new(0),
                stp_mode: matching_domain::StpMode::None,
                side: matching_domain::Side::Buy,
                price: matching_domain::Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    place_order.user_id = Some(matching_domain::UserId(7));
    let placed = retry(|| shard.handle().submit_command(place_order.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(placed.events.iter().any(|e| matches!(
        e.payload,
        EngineEvent::OrderOpened {
            order_id: matching_domain::OrderId(4)
        }
    )));
    config.execution_limits.priority_exhaustion_guard = 3;
    let mut active_update = command(
        93,
        Command::UpdateMarket(Box::new(UpdateMarket {
            config: config.clone(),
        })),
    );
    active_update.session_id = 6;
    active_update.client_seq = 1;
    let active_updated = retry(|| shard.handle().submit_command(active_update.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        active_updated.events[0].payload,
        EngineEvent::MarketConfigUpdated { market_id: market }
    );
    let close = command(
        5,
        Command::CloseMarket(matching_domain::CloseMarket { market_id: market }),
    );
    let closed = retry(|| shard.handle().submit_command(close.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    config.price_scale = 0;
    let unsafe_update = command(6, Command::UpdateMarket(Box::new(UpdateMarket { config })));
    let rejected = retry(|| shard.handle().submit_command(unsafe_update.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidOrderCombination
        }
    );
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    let bytes = image.encode_snapshot(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
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
    assert_eq!(
        matching_runtime::SnapshotImage::decode_snapshot(&uncreated_bytes, &trusted)
            .unwrap()
            .state_digest(1024 * 1024)
            .unwrap(),
        uncreated_digest
    );
    let uncreated_restore =
        matching_runtime::SnapshotImage::decode_snapshot(&uncreated_bytes, &trusted).unwrap();
    assert!(Shard::start_in_memory_from_snapshot(uncreated_restore, &trusted).is_err());
    assert_eq!(
        matching_runtime::SnapshotImage::decode_snapshot(&bytes, &trusted)
            .unwrap()
            .state_digest(1024 * 1024)
            .unwrap(),
        digest
    );
    let mut downgraded = trusted.clone();
    downgraded.place = Some(
        PlaceSupportConfig::new(
            RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
            [(market, policy)],
            PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap(),
    );
    assert!(matching_runtime::SnapshotImage::decode_snapshot(&bytes, &downgraded).is_err());
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions[0].events, uncreated_result.events);
    assert_eq!(replay.executions[1].events, missing_cancelled.events);
    assert_eq!(replay.executions[2].events, created.events);
    assert_eq!(replay.executions[3].events, closed_cancelled.events);
    assert_eq!(replay.executions[4].events, early_reject.events);
    assert_eq!(replay.executions[5].events, updated.events);
    assert_eq!(replay.executions[6].events, started.events);
    assert_eq!(replay.executions[7].events, placed.events);
    assert_eq!(replay.executions[8].events, active_updated.events);
    assert_eq!(replay.executions[9].events, closed.events);
    assert_eq!(replay.executions[10].events, rejected.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    let mut event_hash = matching_runtime::EventHasher::new();
    for event in uncreated_result
        .events
        .iter()
        .chain(missing_cancelled.events.iter())
        .chain(created.events.iter())
        .chain(closed_cancelled.events.iter())
        .chain(early_reject.events.iter())
        .chain(updated.events.iter())
        .chain(started.events.iter())
        .chain(placed.events.iter())
        .chain(active_updated.events.iter())
        .chain(closed.events.iter())
        .chain(rejected.events.iter())
    {
        event_hash.update(event).unwrap();
    }
    assert_eq!(replay.event_hash, event_hash.finish());
    drop(manager);
    let mut recovered = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
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
    let next_start = command(
        7,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 4,
        }),
    );
    let next = retry(|| recovered.handle().submit_command(next_start.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let after = retry(|| recovered.handle().take_snapshot(3))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    writer.shutdown().unwrap();
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions.len(), 1);
    assert_eq!(replay.executions[0].events, next.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), after);
}

#[test]
fn unsupported_management_command_does_not_allocate_sequence_or_write_wal() {
    let temp = t22_support::TestDirectory::new();
    let mut shard = Shard::start_group_fence_only(
        ShardId(123),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (4, 10_000),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 1,
        client_seq: 1,
        market_id: Some(MarketId(4)),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::StartSession(StartSession {
            market_id: MarketId(4),
            new_session_id: 9,
        }),
    };
    let until = Instant::now() + Duration::from_secs(5);
    let reply = loop {
        match shard.handle().submit_command(command.clone()) {
            Ok(reply) => break reply,
            Err(matching_runtime::ShardError::Busy) if Instant::now() < until => {
                std::thread::yield_now()
            }
            Err(error) => panic!("命令未入队: {error:?}"),
        }
    };
    assert!(reply.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
    let mut fee_command = command;
    fee_command.payload =
        Command::UpdateFeeSchedule(Box::new(matching_domain::UpdateFeeSchedule {
            history: Vec::new(),
            schedule: matching_domain::FeeSchedule::new(
                1,
                matching_domain::FeeAssetPolicy::Quote,
                [(
                    1,
                    matching_domain::FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: matching_domain::FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        }));
    assert!(
        retry(|| shard.handle().submit_command(fee_command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        matching_persistence::scan_segments(temp.0.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn committed_start_session_with_invalid_route_fails_closed() {
    committed_unsupported_management_fails_closed(false);
}

#[test]
fn committed_fee_update_without_trusted_place_state_fails_closed() {
    committed_unsupported_management_fails_closed(true);
}

fn committed_unsupported_management_fails_closed(fee_update: bool) {
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
        Profile, RegisteredAppender, ReplicationMode, SegmentWriter, WalRecord,
    };
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let wal = temp.0.join("wal");
    let manifest = temp.0.join("manifest");
    let command = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 1,
        client_seq: 1,
        market_id: Some(MarketId(4)),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: if fee_update {
            Command::UpdateFeeSchedule(Box::new(matching_domain::UpdateFeeSchedule {
                history: Vec::new(),
                schedule: matching_domain::FeeSchedule::new(
                    1,
                    matching_domain::FeeAssetPolicy::Quote,
                    [(
                        1,
                        matching_domain::FeeTier {
                            maker_bps: 0,
                            taker_bps: 0,
                            rounding: matching_domain::FeeRounding::TowardZero,
                        },
                    )],
                )
                .unwrap(),
            }))
        } else {
            Command::StartSession(StartSession {
                market_id: MarketId(4),
                new_session_id: 9,
            })
        },
    };
    let writer = SegmentWriter::new(&wal, 4096).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    let appender = RegisteredAppender::new(writer, manager).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, 7).unwrap();
    batcher
        .append_at(
            &WalRecord {
                cluster_id: 2,
                shard_id: 124,
                flags: 0,
                epoch: 7,
                shard_seq: 1,
                engine_time: 0,
                command_id: 3,
                payload: matching_domain::encode_wal_command(&command).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
    drop(batcher);
    let file = wal.join("00000000000000000001.wal");
    let before = std::fs::read(&file).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(2),
        shard: ShardId(124),
        primary_epoch: 7,
        routes: matching_runtime::RouteTable::new([]).unwrap(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: None,
        expect_registry: true,
        max_markets: 0,
        max_bytes: 1024 * 1024,
    };
    let manager = ManifestManager::<FileManifestStorage>::open_recovery(&manifest).unwrap();
    let batch = matching_runtime::recover_snapshot_records(&wal, &snapshots, &manager, &trusted)
        .unwrap()
        .unwrap();
    let error = batch.replay().err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("已提交命令重放时被前置拒绝"));
    drop(manager);
    assert!(Shard::recover_group_local(&wal, &manifest, snapshots, &trusted, (4, 10_000)).is_err());
    assert_eq!(std::fs::read(file).unwrap(), before);
}

#[test]
fn create_and_start_session_are_durable_and_replay_from_genesis() {
    market_lifecycle_replays(true);
}

#[test]
fn fsync_each_market_lifecycle_replays_from_genesis() {
    market_lifecycle_replays(false);
}

fn market_lifecycle_replays(group: bool) {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetId, AssetReserve, ClosePolicy, CreateMarket, EngineEvent, ExecutionLimits, LimitOrder,
        MarketConfig, MarketState, MarketStateReason, OpenMarket, OrderId, OrderLimits,
        PlaceLimitOrder, Price, Qty, QuoteAmount, ReservationId, ReservationToken, ResumeMarket,
        Side, StpKey, StpMode, SuspendMarket, UserId,
    };
    use matching_orderbook::QueuePriorityPolicy;
    use matching_risk::{LimitReservePolicy, RiskKeySet};
    use matching_runtime::{MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable};
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let market = MarketId(if group { 45 } else { 46 });
    let shard_id = ShardId(if group { 45 } else { 46 });
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
    let signing = SigningKey::from_bytes(&[7; 32]);
    let place = PlaceSupportConfig::new(
        RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
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
            ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            (temp.0.join("wal"), temp.0.join("manifest")),
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
    let submit = |value: CommandEnvelope| {
        let deadline = Instant::now() + Duration::from_secs(5);
        let rx = loop {
            match shard.handle().submit_command(value.clone()) {
                Ok(reply) => break reply,
                Err(matching_runtime::ShardError::Busy) if Instant::now() < deadline => {
                    std::thread::yield_now()
                }
                Err(error) => panic!("入队失败: {error:?}"),
            }
        };
        rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap()
    };
    let create_command = command(
        1,
        Command::CreateMarket(Box::new(CreateMarket {
            config: config.clone(),
        })),
    );
    let mut stale_create = create_command.clone();
    stale_create.route_epoch = Some(0);
    assert!(matches!(
        retry(|| shard.handle().submit_command(stale_create.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::StaleRouteEpoch
        ))
    ));
    let created = submit(create_command.clone());
    assert_eq!(created.seq, 1);
    assert_eq!(
        created.events[0].payload,
        EngineEvent::MarketCreated { market_id: market }
    );
    let repeated_create = submit(create_command.clone());
    assert!(repeated_create.deduplicated);
    assert_eq!(repeated_create.seq, created.seq);
    assert_eq!(repeated_create.summary, created.summary);
    assert!(repeated_create.events.is_empty());
    assert!(matches!(
        retry(|| shard.handle().submit_command(stale_create.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::StaleRouteEpoch
        ))
    ));
    let mut conflicting_create = create_command.clone();
    if let Command::CreateMarket(ref mut request) = conflicting_create.payload {
        request.config.execution_limits.max_trades_per_command += 1;
    }
    assert!(matches!(
        retry(|| shard.handle().submit_command(conflicting_create.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Err(matching_runtime::FormalError::ProtocolError)
    ));
    let started = submit(command(
        2,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 9,
        }),
    ));
    assert_eq!(started.seq, 2);
    assert_eq!(
        started
            .events
            .iter()
            .map(|e| e.payload.clone())
            .collect::<Vec<_>>(),
        vec![
            EngineEvent::SessionStarted {
                market_id: market,
                session_id: 9
            },
            EngineEvent::MarketStateChanged {
                market_id: market,
                old: MarketState::Closed,
                new: MarketState::PreOpen,
                reason: MarketStateReason::SessionStarted
            },
        ]
    );
    let mut token = ReservationToken {
        token_id: 3,
        command_id: CommandId(3),
        reservation_id: ReservationId(3),
        order_id: OrderId(3),
        user_id: UserId(7),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 20,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut place_command = command(
        3,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(3),
                user_id: UserId(7),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(2).unwrap(),
            },
            client_order_id: Some("preopen-order".into()),
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    place_command.user_id = Some(UserId(7));
    let placed = submit(place_command);
    assert_eq!(placed.seq, 3);
    assert!(placed.events.iter().any(|event| matches!(
        event.payload,
        EngineEvent::OrderOpened {
            order_id: OrderId(3)
        }
    )));
    let mut valid_config = config.clone();
    valid_config.order_limits.max_qty = Qty::try_new(20).unwrap();
    let active_updated = submit(command(
        4,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: valid_config.clone(),
        })),
    ));
    assert_eq!(
        active_updated.events[0].payload,
        EngineEvent::MarketConfigUpdated { market_id: market }
    );
    valid_config.order_limits.min_qty = Qty::try_new(3).unwrap();
    let rejected_update = submit(command(
        5,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: valid_config,
        })),
    ));
    assert_eq!(
        rejected_update.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidOrderCombination
        }
    );
    let opened = submit(command(
        6,
        Command::OpenMarket(OpenMarket {
            market_id: market,
            session_id: 9,
        }),
    ));
    assert_eq!(
        opened.events[0].payload,
        EngineEvent::MarketStateChanged {
            market_id: market,
            old: MarketState::PreOpen,
            new: MarketState::Open,
            reason: MarketStateReason::Opened
        }
    );
    let suspended = submit(command(
        7,
        Command::SuspendMarket(SuspendMarket { market_id: market }),
    ));
    assert_eq!(
        suspended.events[0].payload,
        EngineEvent::MarketStateChanged {
            market_id: market,
            old: MarketState::Open,
            new: MarketState::Suspended,
            reason: MarketStateReason::Admin
        }
    );
    let mut blocked_tick_config = config.clone();
    blocked_tick_config.tick_size = 2;
    let blocked_tick = submit(command(
        8,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: blocked_tick_config,
        })),
    ));
    assert_eq!(
        blocked_tick.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidOrderCombination
        }
    );
    let resumed = submit(command(
        9,
        Command::ResumeMarket(ResumeMarket { market_id: market }),
    ));
    assert_eq!(
        resumed.events[0].payload,
        EngineEvent::MarketStateChanged {
            market_id: market,
            old: MarketState::Suspended,
            new: MarketState::Open,
            reason: MarketStateReason::Admin
        }
    );
    let wrong_session = submit(command(
        10,
        Command::EndSession(matching_domain::EndSession {
            market_id: market,
            session_id: 10,
        }),
    ));
    assert_eq!(
        wrong_session.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidSession
        }
    );
    let ended = submit(command(
        11,
        Command::EndSession(matching_domain::EndSession {
            market_id: market,
            session_id: 9,
        }),
    ));
    assert_eq!(ended.events.len(), 1);
    assert_eq!(
        ended.events[0].payload,
        EngineEvent::SessionEnded {
            market_id: market,
            session_id: 9
        }
    );
    let duplicate_end = submit(command(
        12,
        Command::EndSession(matching_domain::EndSession {
            market_id: market,
            session_id: 9,
        }),
    ));
    assert_eq!(
        duplicate_end.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidSession
        }
    );
    let closed = submit(command(
        13,
        Command::CloseMarket(matching_domain::CloseMarket { market_id: market }),
    ));
    assert!(matches!(
        closed.events[0].payload,
        EngineEvent::OrderCanceled {
            order_id: OrderId(3),
            reason: matching_domain::CancelReason::MarketClosed,
            ..
        }
    ));
    assert_eq!(
        closed.events[1].payload,
        EngineEvent::MarketStateChanged {
            market_id: market,
            old: MarketState::Open,
            new: MarketState::Closed,
            reason: MarketStateReason::Admin
        }
    );
    let restarted = submit(command(
        14,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 10,
        }),
    ));
    let closed_active = submit(command(
        15,
        Command::CloseMarket(matching_domain::CloseMarket { market_id: market }),
    ));
    assert_eq!(
        closed_active.events[0].payload,
        EngineEvent::SessionEnded {
            market_id: market,
            session_id: 10
        }
    );
    let mut next_config = config;
    next_config.order_limits.max_qty = Qty::try_new(20).unwrap();
    let updated = submit(command(
        16,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: next_config.clone(),
        })),
    ));
    assert_eq!(
        updated.events[0].payload,
        EngineEvent::MarketConfigUpdated { market_id: market }
    );
    let started_again = submit(command(
        17,
        Command::StartSession(StartSession {
            market_id: market,
            new_session_id: 11,
        }),
    ));
    let opened_again = submit(command(
        18,
        Command::OpenMarket(OpenMarket {
            market_id: market,
            session_id: 11,
        }),
    ));
    let suspended_again = submit(command(
        19,
        Command::SuspendMarket(SuspendMarket { market_id: market }),
    ));
    next_config.tick_size = 2;
    next_config.lot_size = 2;
    let updated_tick = submit(command(
        20,
        Command::UpdateMarket(Box::new(matching_domain::UpdateMarket {
            config: next_config.clone(),
        })),
    ));
    assert_eq!(
        updated_tick.events[0].payload,
        EngineEvent::MarketConfigUpdated { market_id: market }
    );
    let resumed_again = submit(command(
        21,
        Command::ResumeMarket(ResumeMarket { market_id: market }),
    ));
    let make_order = |id, price, qty: u64| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(7),
            market_id: market,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms: (price as u128) * u128::from(qty),
            }]
            .into(),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_key_id: 1,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        let mut envelope = command(
            id,
            Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: UserId(7),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(price).unwrap(),
                    qty: Qty::try_new(qty).unwrap(),
                },
                client_order_id: None,
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        );
        envelope.user_id = Some(UserId(7));
        envelope
    };
    let invalid_tick = submit(make_order(22, 11, 2));
    assert_eq!(
        invalid_tick.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidTick
        }
    );
    let placed_again = submit(make_order(23, 10, 2));
    assert!(placed_again.events.iter().any(|event| matches!(
        event.payload,
        EngineEvent::OrderOpened {
            order_id: OrderId(23)
        }
    )));
    let invalid_qty = submit(make_order(24, 10, 22));
    assert_eq!(
        invalid_qty.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidQty
        }
    );
    let excessive_notional = submit(make_order(25, 60, 2));
    assert_eq!(
        excessive_notional.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::MaxNotional
        }
    );
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let live = image.state_digest(1024 * 1024).unwrap();
    let encoded = image.encode_snapshot(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
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
    assert_eq!(decoded.state_digest(1024 * 1024).unwrap(), live);
    let mut wrong_trust = trusted.clone();
    let other_policy =
        LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 1).unwrap();
    wrong_trust.place = Some(
        PlaceSupportConfig::new(
            RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap(),
            [(market, other_policy)],
            PlaceLimits {
                active_orders: 8,
                terminal_history: 8,
                client_id_bytes: 32,
            },
        )
        .unwrap(),
    );
    assert!(matching_runtime::SnapshotImage::decode_snapshot(&encoded, &wrong_trust).is_err());
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let records = matching_runtime::recover_snapshot_records_with_genesis(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap();
    let mut live_hash = matching_runtime::EventHasher::new();
    for event in created
        .events
        .iter()
        .chain(started.events.iter())
        .chain(placed.events.iter())
        .chain(active_updated.events.iter())
        .chain(rejected_update.events.iter())
        .chain(opened.events.iter())
        .chain(suspended.events.iter())
        .chain(blocked_tick.events.iter())
        .chain(resumed.events.iter())
        .chain(wrong_session.events.iter())
        .chain(ended.events.iter())
        .chain(duplicate_end.events.iter())
        .chain(closed.events.iter())
        .chain(restarted.events.iter())
        .chain(closed_active.events.iter())
        .chain(updated.events.iter())
        .chain(started_again.events.iter())
        .chain(opened_again.events.iter())
        .chain(suspended_again.events.iter())
        .chain(updated_tick.events.iter())
        .chain(resumed_again.events.iter())
        .chain(invalid_tick.events.iter())
        .chain(placed_again.events.iter())
        .chain(invalid_qty.events.iter())
        .chain(excessive_notional.events.iter())
    {
        live_hash.update(event).unwrap();
    }
    let replay_result = records.replay().unwrap();
    assert_eq!(replay_result.executions[0].events, created.events);
    assert_eq!(replay_result.executions[1].events, started.events);
    assert_eq!(replay_result.executions[2].events, placed.events);
    assert_eq!(replay_result.executions[3].events, active_updated.events);
    assert_eq!(replay_result.executions[4].events, rejected_update.events);
    assert_eq!(replay_result.executions[5].events, opened.events);
    assert_eq!(replay_result.executions[6].events, suspended.events);
    assert_eq!(replay_result.executions[7].events, blocked_tick.events);
    assert_eq!(replay_result.executions[8].events, resumed.events);
    assert_eq!(replay_result.executions[9].events, wrong_session.events);
    assert_eq!(replay_result.executions[10].events, ended.events);
    assert_eq!(replay_result.executions[11].events, duplicate_end.events);
    assert_eq!(replay_result.executions[12].events, closed.events);
    assert_eq!(replay_result.executions[13].events, restarted.events);
    assert_eq!(replay_result.executions[14].events, closed_active.events);
    assert_eq!(replay_result.executions[15].events, updated.events);
    assert_eq!(replay_result.executions[16].events, started_again.events);
    assert_eq!(replay_result.executions[17].events, opened_again.events);
    assert_eq!(replay_result.executions[18].events, suspended_again.events);
    assert_eq!(replay_result.executions[19].events, updated_tick.events);
    assert_eq!(replay_result.executions[20].events, resumed_again.events);
    assert_eq!(replay_result.executions[21].events, invalid_tick.events);
    assert_eq!(replay_result.executions[22].events, placed_again.events);
    assert_eq!(replay_result.executions[23].events, invalid_qty.events);
    assert_eq!(
        replay_result.executions[24].events,
        excessive_notional.events
    );
    assert_eq!(replay_result.event_hash, live_hash.finish());
    assert_eq!(replay_result.image.state_digest(1024 * 1024).unwrap(), live);
    drop(manager);
    let mut recovered = Shard::recover_group_local_with_genesis(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        snapshots.clone(),
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
        (4, 10_000),
    )
    .unwrap();
    assert!(matches!(
        retry(|| recovered.handle().submit_command(stale_create.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Err(matching_runtime::FormalError::Rejected(
            matching_domain::RejectReason::StaleRouteEpoch
        ))
    ));
    let recovered_create = retry(|| recovered.handle().submit_command(create_command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(recovered_create.deduplicated);
    assert_eq!(recovered_create.seq, created.seq);
    assert_eq!(recovered_create.summary, created.summary);
    assert!(recovered_create.events.is_empty());
    let replay = retry(|| recovered.handle().take_snapshot(2))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    assert_eq!(replay, live);
    let mut snapshot_writer =
        matching_runtime::SnapshotWriter::start(recovered.handle(), snapshots.clone(), 1024 * 1024)
            .unwrap();
    let checkpoint = retry(|| recovered.handle().take_snapshot(3))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    snapshot_writer
        .submit(3, checkpoint)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let suspend_after_snapshot = command(
        26,
        Command::SuspendMarket(SuspendMarket { market_id: market }),
    );
    let after_checkpoint = retry(|| {
        recovered
            .handle()
            .submit_command(suspend_after_snapshot.clone())
    })
    .recv_timeout(Duration::from_secs(5))
    .unwrap()
    .unwrap();
    let end_after_checkpoint = command(
        27,
        Command::EndSession(matching_domain::EndSession {
            market_id: market,
            session_id: 11,
        }),
    );
    let ended_after_checkpoint = retry(|| {
        recovered
            .handle()
            .submit_command(end_after_checkpoint.clone())
    })
    .recv_timeout(Duration::from_secs(5))
    .unwrap()
    .unwrap();
    let resume_after_end = command(
        28,
        Command::ResumeMarket(ResumeMarket { market_id: market }),
    );
    let rejected_resume = retry(|| recovered.handle().submit_command(resume_after_end.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected_resume.events[0].payload,
        EngineEvent::CommandRejected {
            reason: matching_domain::RejectReason::InvalidSession
        }
    );
    let mut invalid_cases = Vec::new();
    for (offset, reason) in [
        matching_domain::RejectReason::MarketConfigInvalidTick,
        matching_domain::RejectReason::MarketConfigInvalidLot,
        matching_domain::RejectReason::MarketConfigUnsupportedScale,
        matching_domain::RejectReason::MarketConfigNonIntegralQuoteUnit,
        matching_domain::RejectReason::MarketConfigArithmeticOverflow,
        matching_domain::RejectReason::InvalidSlippageBps,
    ]
    .into_iter()
    .enumerate()
    {
        let mut bad = next_config.clone();
        match offset {
            0 => bad.tick_size = 0,
            1 => bad.lot_size = 0,
            2 => bad.quote_scale = 39,
            3 => {
                bad.price_scale = 1;
                bad.tick_size = 1;
                bad.lot_size = 1;
            }
            4 => {
                bad.quote_scale = 38;
                bad.tick_size = 4;
                bad.lot_size = 1;
            }
            5 => {
                bad.price_band = Some(matching_domain::PriceBandConfig {
                    reference_kind: matching_domain::PriceReferenceKind::LastTrade,
                    max_deviation_bps: 10_000,
                })
            }
            _ => unreachable!(),
        }
        let invalid = command(
            29 + offset as u128,
            Command::UpdateMarket(Box::new(matching_domain::UpdateMarket { config: bad })),
        );
        let result = retry(|| recovered.handle().submit_command(invalid.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            result.events[0].payload,
            EngineEvent::CommandRejected { reason }
        );
        invalid_cases.push(result);
    }
    let later_checkpoint = retry(|| recovered.handle().take_snapshot(4))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let checkpoint_digest = later_checkpoint.state_digest(1024 * 1024).unwrap();
    snapshot_writer
        .submit(4, later_checkpoint)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    snapshot_writer.shutdown().unwrap();
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    let newest = snapshots.join("snapshot-4.bin");
    let mut corrupted = std::fs::read(&newest).unwrap();
    corrupted[35] ^= 1;
    std::fs::write(&newest, corrupted).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let batch = matching_runtime::recover_snapshot_records(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
    )
    .unwrap()
    .unwrap();
    assert!(batch.snapshot().used_previous);
    let replay = batch.replay().unwrap();
    assert_eq!(replay.executions.len(), 9);
    assert_eq!(replay.executions[0].events, after_checkpoint.events);
    assert_eq!(replay.executions[1].events, ended_after_checkpoint.events);
    assert_eq!(replay.executions[2].events, rejected_resume.events);
    for (actual, expected) in replay.executions[3..].iter().zip(&invalid_cases) {
        assert_eq!(actual.events, expected.events);
    }
    assert_eq!(
        replay.image.state_digest(1024 * 1024).unwrap(),
        checkpoint_digest
    );
    let mut wal_files: Vec<_> = std::fs::read_dir(temp.0.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    wal_files.sort();
    let wal_file = wal_files.last().unwrap();
    let mut damaged_wal = std::fs::read(wal_file).unwrap();
    *damaged_wal.last_mut().unwrap() ^= 1;
    std::fs::write(wal_file, &damaged_wal).unwrap();
    assert!(
        matching_runtime::recover_snapshot_records(
            temp.0.join("wal"),
            &snapshots,
            &manager,
            &trusted
        )
        .is_err()
    );
    assert_eq!(std::fs::read(wal_file).unwrap(), damaged_wal);
}
