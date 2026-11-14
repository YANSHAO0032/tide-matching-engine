//! 验证开市前参考价更新的正式持久路径。
mod t22_support;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
};
use std::time::{Duration, Instant};

/// 只重试尚未入队的暂时背压。
fn retry<T>(mut action: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match action() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if Instant::now() < deadline => {
                std::thread::yield_now()
            }
            Err(error) => panic!("请求未入队: {error:?}"),
        }
    }
}

#[test]
fn group_preopen_revalidation_replays_cancellations_and_release() {
    run(true, 0);
}

#[test]
fn fsync_preopen_revalidation_replays_cancellations_and_release() {
    run(false, 0);
}

#[test]
fn group_market_kill_cancels_rejects_and_replays() {
    run(true, 1);
}

#[test]
fn fsync_market_kill_cancels_rejects_and_replays() {
    run(false, 1);
}

#[test]
fn group_market_kill_retains_orders_and_allows_cancel() {
    run(true, 2);
}

#[test]
fn fsync_market_kill_retains_orders_and_allows_cancel() {
    run(false, 2);
}
#[test]
fn group_circuit_breaker_halts_and_replays() {
    run(true, 3);
}
#[test]
fn fsync_circuit_breaker_halts_and_replays() {
    run(false, 3);
}
#[test]
fn preopen_halt_cannot_resume_with_stale_band_orders() {
    run(true, 4);
}
#[test]
fn shard_kill_requires_current_lease_and_replays() {
    run(true, 5);
}
#[test]
fn shard_kill_without_lease_is_rejected_before_sequence() {
    run(true, 6);
}
#[test]
fn fsync_shard_kill_requires_current_lease_and_replays() {
    run(false, 7);
}
#[test]
fn previously_open_halt_resumes_without_preopen_revalidation() {
    run(true, 8);
}

fn run(group: bool, kill: u8) {
    let temp = t22_support::TestDirectory::new();
    let snapshots = temp.0.join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    let id = 88 + u16::from(!group) + u16::from(kill) * 2;
    let market = MarketId(u32::from(id));
    let shard_id = ShardId(id);
    let cluster = ClusterId(2);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let policy = LimitReservePolicy::new((AssetId(1), 0), (AssetId(2), 0), math, 1, 1, 0).unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let mut place = PlaceSupportConfig::new(
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
    let lease = if kill == 5 || kill == 7 {
        let store =
            matching_runtime::ControlStore::new(temp.0.join("primary.lease"), cluster, shard_id);
        let lease = std::sync::Arc::new(store.acquire_primary().unwrap());
        place = place.with_primary_lease(lease.clone());
        Some(lease)
    } else {
        None
    };
    let epoch = lease.as_ref().map_or(7, |lease| lease.epoch());
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
            max_notional: QuoteAmount::new(1000),
        },
        price_band: Some(PriceBandConfig {
            reference_kind: PriceReferenceKind::StaticOpening,
            max_deviation_bps: 1000,
        }),
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
            (IdempotencyConfig::default(), epoch, 8, place.clone()),
            ((temp.0.join("wal"), temp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(1).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), epoch, 8, place.clone()),
            (temp.0.join("wal"), temp.0.join("manifest")),
        )
    }
    .unwrap();
    let command = |id: u128, payload| CommandEnvelope {
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
    let reference = |source_seq, price| {
        Command::UpdateReferencePrice(UpdateReferencePrice {
            market_id: market,
            kind: PriceReferenceKind::StaticOpening,
            price: Price::try_new(price).unwrap(),
            source_seq,
        })
    };
    let mut results = Vec::new();
    for cmd in [
        command(1, Command::CreateMarket(Box::new(CreateMarket { config }))),
        command(
            2,
            Command::StartSession(StartSession {
                market_id: market,
                new_session_id: 1,
            }),
        ),
    ] {
        results.push(
            retry(|| shard.handle().submit_command(cmd.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
    }
    let mut early_token = ReservationToken {
        token_id: 99,
        command_id: CommandId(99),
        reservation_id: ReservationId(99),
        order_id: OrderId(99),
        user_id: UserId(3),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 90,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [0; 64],
    };
    early_token.signature = signing
        .sign(&early_token.canonical_signing_payload().unwrap())
        .to_bytes();
    let mut early = command(
        99,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(99),
                user_id: UserId(3),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(90).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: early_token,
        })),
    );
    early.user_id = Some(UserId(3));
    early.session_id = 2;
    let rejected = retry(|| shard.handle().submit_command(early.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected.outcome,
        matching_runtime::FormalOutcome::Rejected(RejectReason::ReferencePriceUnavailable)
    );
    results.push(rejected);
    let bootstrap = command(3, reference(1, 100));
    results.push(
        retry(|| shard.handle().submit_command(bootstrap.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
    );
    let mut token = ReservationToken {
        token_id: 4,
        command_id: CommandId(4),
        reservation_id: ReservationId(4),
        order_id: OrderId(4),
        user_id: UserId(1),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 90,
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
    let mut order = command(
        4,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(4),
                user_id: UserId(1),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(90).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    order.user_id = Some(UserId(1));
    results.push(
        retry(|| shard.handle().submit_command(order.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
    );
    let mut token = ReservationToken {
        token_id: 5,
        command_id: CommandId(5),
        reservation_id: ReservationId(5),
        order_id: OrderId(5),
        user_id: UserId(2),
        market_id: market,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 95,
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
    let mut order = command(
        5,
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(5),
                user_id: UserId(2),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(95).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            },
            client_order_id: None,
            user_control_epoch: 0,
            post_only: false,
            reservation: token,
        })),
    );
    order.user_id = Some(UserId(2));
    results.push(
        retry(|| shard.handle().submit_command(order.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
    );
    if kill == 8 {
        let open = command(
            6,
            Command::OpenMarket(OpenMarket {
                market_id: market,
                session_id: 1,
            }),
        );
        results.push(
            retry(|| shard.handle().submit_command(open.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let breaker = command(
            7,
            Command::TriggerCircuitBreaker(TriggerCircuitBreaker {
                market_id: market,
                reason: CircuitBreakerReason::ManualRiskSignal,
            }),
        );
        results.push(
            retry(|| shard.handle().submit_command(breaker.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let update = command(8, reference(2, 120));
        results.push(
            retry(|| shard.handle().submit_command(update.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let resume = command(9, Command::ResumeMarket(ResumeMarket { market_id: market }));
        let accepted = retry(|| shard.handle().submit_command(resume.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            accepted.outcome,
            matching_runtime::FormalOutcome::MarketManaged
        );
        results.push(accepted);
    } else if kill == 6 {
        let parent = CommandId(60);
        let id = matching_runtime::ControlCoordinator::derive_shard_kill_id(
            cluster, shard_id, parent, true,
        );
        let mut control = command(
            id.get(),
            Command::ShardKillSwitch(ShardKillSwitch {
                target_shard_id: shard_id,
                parent_command_id: parent,
                expected_primary_epoch: epoch,
                active: true,
                cancel_all: false,
            }),
        );
        control.market_id = None;
        control.route_epoch = None;
        control.session_id = 3;
        control.client_seq = 1;
        assert!(matches!(
            retry(|| shard.handle().submit_command(control.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                RejectReason::StalePrimaryEpoch
            ))
        ));
    } else if kill == 5 || kill == 7 {
        let (id, payload) = matching_runtime::ControlCoordinator::shard_kill_subcommand(
            lease.as_ref().unwrap(),
            CommandId(60),
            true,
            true,
        );
        let mut control = command(id.get(), Command::ShardKillSwitch(payload));
        control.market_id = None;
        control.route_epoch = None;
        control.session_id = 3;
        control.client_seq = 1;
        let mut stale = control.clone();
        if let Command::ShardKillSwitch(ref mut item) = stale.payload {
            item.expected_primary_epoch += 1;
        }
        assert!(matches!(
            retry(|| shard.handle().submit_command(stale.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                RejectReason::StalePrimaryEpoch
            ))
        ));
        let mut wrong_shard = control.clone();
        if let Command::ShardKillSwitch(ref mut item) = wrong_shard.payload {
            item.target_shard_id = ShardId(shard_id.get() + 1);
        }
        assert!(matches!(
            retry(|| shard.handle().submit_command(wrong_shard.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                RejectReason::StalePrimaryEpoch
            ))
        ));
        let mut forged_id = control.clone();
        forged_id.command_id = CommandId(999);
        assert!(matches!(
            retry(|| shard.handle().submit_command(forged_id.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            Err(matching_runtime::FormalError::Rejected(
                RejectReason::InvalidOrderCombination
            ))
        ));
        let activated = retry(|| shard.handle().submit_command(control.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(activated.seq, results.last().unwrap().seq + 1);
        assert!(
            matches!(activated.events.first().map(|e| &e.payload), Some(EngineEvent::KillSwitchActivated { scope: KillScope::Shard(id) }) if *id == shard_id)
        );
        assert_eq!(
            activated
                .events
                .iter()
                .filter(|e| matches!(
                    e.payload,
                    EngineEvent::OrderCanceled {
                        reason: CancelReason::KillSwitch,
                        ..
                    }
                ))
                .count(),
            2
        );
        results.push(activated);
        let active_image = retry(|| shard.handle().take_snapshot(3))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        let active_bytes = active_image.encode_snapshot(1024 * 1024).unwrap();
        let active_config = matching_runtime::SnapshotRestoreConfig {
            cluster,
            shard: shard_id,
            primary_epoch: epoch,
            routes: routes.clone(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place.clone()),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let restored =
            matching_runtime::SnapshotImage::decode_snapshot(&active_bytes, &active_config)
                .unwrap();
        assert_eq!(
            restored.state_digest(1024 * 1024).unwrap(),
            active_image.state_digest(1024 * 1024).unwrap()
        );
        let mut blocked = order.clone();
        blocked.command_id = CommandId(9);
        blocked.session_id = 2;
        blocked.client_seq = 1;
        let rejected = retry(|| shard.handle().submit_command(blocked.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            rejected.outcome,
            matching_runtime::FormalOutcome::Rejected(RejectReason::KillSwitchActive)
        );
        results.push(rejected);
        let (id, payload) = matching_runtime::ControlCoordinator::shard_kill_subcommand(
            lease.as_ref().unwrap(),
            CommandId(61),
            false,
            false,
        );
        let mut clear = command(id.get(), Command::ShardKillSwitch(payload));
        clear.market_id = None;
        clear.route_epoch = None;
        clear.session_id = 3;
        clear.client_seq = 2;
        results.push(
            retry(|| shard.handle().submit_command(clear.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
    } else if kill == 4 {
        let breaker = command(
            6,
            Command::TriggerCircuitBreaker(TriggerCircuitBreaker {
                market_id: market,
                reason: CircuitBreakerReason::ManualRiskSignal,
            }),
        );
        results.push(
            retry(|| shard.handle().submit_command(breaker.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let update = command(7, reference(2, 120));
        results.push(
            retry(|| shard.handle().submit_command(update.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let resume = command(8, Command::ResumeMarket(ResumeMarket { market_id: market }));
        let rejected = retry(|| shard.handle().submit_command(resume.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            rejected.outcome,
            matching_runtime::FormalOutcome::Rejected(RejectReason::InvalidMarketStateTransition)
        );
        results.push(rejected);
        let cancel = command(
            9,
            Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market { market_id: market },
            }),
        );
        results.push(
            retry(|| shard.handle().submit_command(cancel.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let resume = command(
            10,
            Command::ResumeMarket(ResumeMarket { market_id: market }),
        );
        let accepted = retry(|| shard.handle().submit_command(resume.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            accepted.outcome,
            matching_runtime::FormalOutcome::MarketManaged
        );
        results.push(accepted);
    } else if kill == 3 {
        let open = command(
            6,
            Command::OpenMarket(OpenMarket {
                market_id: market,
                session_id: 1,
            }),
        );
        results.push(
            retry(|| shard.handle().submit_command(open.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
        let breaker = command(
            7,
            Command::TriggerCircuitBreaker(TriggerCircuitBreaker {
                market_id: market,
                reason: CircuitBreakerReason::ManualRiskSignal,
            }),
        );
        let halted = retry(|| shard.handle().submit_command(breaker.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(matches!(
            halted.events.as_slice(),
            [
                EventEnvelope {
                    payload: EngineEvent::CircuitBreakerTriggered {
                        reason: CircuitBreakerReason::ManualRiskSignal,
                        ..
                    },
                    ..
                },
                EventEnvelope {
                    payload: EngineEvent::MarketStateChanged {
                        new: MarketState::Halted,
                        reason: MarketStateReason::CircuitBreaker,
                        ..
                    },
                    ..
                }
            ]
        ));
        results.push(halted);
        let mut blocked = order.clone();
        blocked.command_id = CommandId(9);
        blocked.session_id = 2;
        blocked.client_seq = 1;
        let rejected = retry(|| shard.handle().submit_command(blocked.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            rejected.outcome,
            matching_runtime::FormalOutcome::Rejected(RejectReason::MarketHalted)
        );
        results.push(rejected);
        let resume = command(8, Command::ResumeMarket(ResumeMarket { market_id: market }));
        results.push(
            retry(|| shard.handle().submit_command(resume.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
    } else if kill != 0 {
        let kill_command = command(
            6,
            Command::MarketKillSwitch(MarketKillSwitch {
                market_id: market,
                active: true,
                cancel_all: kill == 1,
            }),
        );
        let mut stale_route = kill_command.clone();
        stale_route.route_epoch = Some(0);
        let failure = retry(|| shard.handle().submit_command(stale_route.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            failure,
            Err(matching_runtime::FormalError::Rejected(
                RejectReason::StaleRouteEpoch
            ))
        ));
        let activated = retry(|| shard.handle().submit_command(kill_command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(activated.seq, results.last().unwrap().seq + 1);
        assert!(
            matches!(activated.events.first().map(|e| &e.payload), Some(EngineEvent::KillSwitchActivated { scope: KillScope::Market(id) }) if *id == market)
        );
        assert_eq!(
            activated
                .events
                .iter()
                .filter(|e| matches!(
                    e.payload,
                    EngineEvent::OrderCanceled {
                        reason: CancelReason::KillSwitch,
                        ..
                    }
                ))
                .count(),
            if kill == 1 { 2 } else { 0 }
        );
        assert_eq!(
            activated
                .events
                .iter()
                .filter(|e| matches!(
                    e.payload,
                    EngineEvent::ReservationReleased {
                        reason: ReservationReleaseReason::KillSwitch,
                        released: Some(_),
                        ..
                    }
                ))
                .count(),
            if kill == 1 { 2 } else { 0 }
        );
        results.push(activated);
        let duplicate = retry(|| shard.handle().submit_command(kill_command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.seq, results.last().unwrap().seq);
        assert!(duplicate.events.is_empty());
        let active_image = retry(|| shard.handle().take_snapshot(2))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        let active_bytes = active_image.encode_snapshot(1024 * 1024).unwrap();
        let active_config = matching_runtime::SnapshotRestoreConfig {
            cluster,
            shard: shard_id,
            primary_epoch: epoch,
            routes: routes.clone(),
            user_capacity: 8,
            idempotency: IdempotencyConfig::default(),
            place: Some(place.clone()),
            expect_registry: true,
            max_markets: 1,
            max_bytes: 1024 * 1024,
        };
        let restored =
            matching_runtime::SnapshotImage::decode_snapshot(&active_bytes, &active_config)
                .unwrap();
        assert_eq!(
            restored.state_digest(1024 * 1024).unwrap(),
            active_image.state_digest(1024 * 1024).unwrap()
        );
        let mut blocked = order.clone();
        blocked.command_id = CommandId(9);
        blocked.session_id = 2;
        blocked.client_seq = 1;
        let rejected = retry(|| shard.handle().submit_command(blocked.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            rejected.outcome,
            matching_runtime::FormalOutcome::Rejected(RejectReason::KillSwitchActive)
        );
        results.push(rejected);
        if kill == 2 {
            let found = retry(|| shard.handle().order(market, OrderId(4)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            assert!(found.unwrap().is_some());
            let mut cancel = command(
                10,
                Command::MassCancelShard(MassCancelShard {
                    scope: ShardCancelScope::Market { market_id: market },
                }),
            );
            cancel.session_id = 2;
            cancel.client_seq = 2;
            let canceled = retry(|| shard.handle().submit_command(cancel.clone()))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert_eq!(
                canceled
                    .events
                    .iter()
                    .filter(|e| matches!(
                        e.payload,
                        EngineEvent::OrderCanceled {
                            reason: CancelReason::MassCancel,
                            ..
                        }
                    ))
                    .count(),
                2
            );
            results.push(canceled);
        }
        let cleared = command(
            7,
            Command::MarketKillSwitch(MarketKillSwitch {
                market_id: market,
                active: false,
                cancel_all: false,
            }),
        );
        let cleared = retry(|| shard.handle().submit_command(cleared.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(
            matches!(cleared.events.first().map(|e| &e.payload), Some(EngineEvent::KillSwitchCleared { scope: KillScope::Market(id) }) if *id == market)
        );
        results.push(cleared);
    } else {
        let changed = retry(|| shard.handle().submit_command(command(6, reference(2, 120))))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(matches!(
            changed.events.as_slice(),
            [
                EventEnvelope {
                    payload: EngineEvent::OrderCanceled {
                        order_id: OrderId(5),
                        reason: CancelReason::PriceBandRevalidation,
                        ..
                    },
                    ..
                },
                EventEnvelope {
                    payload: EngineEvent::ReservationReleased {
                        order_id: OrderId(5),
                        reason: ReservationReleaseReason::PriceBandRevalidation,
                        released: Some(_),
                        ..
                    },
                    ..
                },
                EventEnvelope {
                    payload: EngineEvent::OrderCanceled {
                        order_id: OrderId(4),
                        reason: CancelReason::PriceBandRevalidation,
                        ..
                    },
                    ..
                },
                EventEnvelope {
                    payload: EngineEvent::ReservationReleased {
                        order_id: OrderId(4),
                        reason: ReservationReleaseReason::PriceBandRevalidation,
                        released: Some(_),
                        ..
                    },
                    ..
                }
            ]
        ));
        results.push(changed);
        let stale = retry(|| shard.handle().submit_command(command(7, reference(2, 100))))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(
            stale.outcome,
            matching_runtime::FormalOutcome::Rejected(RejectReason::StaleReferencePriceUpdate)
        );
        results.push(stale);
    }
    if kill != 3 && kill != 4 && kill != 8 {
        let opened = retry(|| {
            shard.handle().submit_command(command(
                8,
                Command::OpenMarket(OpenMarket {
                    market_id: market,
                    session_id: 1,
                }),
            ))
        })
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
        assert_eq!(
            opened.outcome,
            matching_runtime::FormalOutcome::MarketManaged
        );
        assert_eq!(opened.seq, results.last().unwrap().seq + 1);
        results.push(opened);
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
        primary_epoch: epoch,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(place),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let market_replay = matching_runtime::recover_snapshot_records_with_genesis(
        temp.0.join("wal"),
        &snapshots,
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay_market_data(market)
    .unwrap();
    let expected_sources: Vec<_> = results
        .iter()
        .flat_map(|execution| {
            execution
                .market_data_source
                .get(&market)
                .into_iter()
                .flatten()
                .cloned()
        })
        .collect();
    assert_eq!(market_replay.events(), expected_sources);
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
    let full_replay = matching_runtime::recover_market_data_from_genesis(
        temp.0.join("wal"),
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
    )
    .unwrap();
    for (actual, expected) in full_replay.executions.iter().zip(&results) {
        assert_eq!(actual.market_data_source, expected.market_data_source);
    }
    assert_eq!(full_replay.executions.len(), results.len());
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    assert_eq!(replay.executions.len(), results.len());
    if kill == 0 {
        let placed = results
            .iter()
            .find(|result| result.command_id == CommandId(4))
            .unwrap();
        assert_eq!(placed.market_data_source[&market][0].sequence, 1);
        let replayed_place = replay
            .executions
            .iter()
            .find(|result| result.command_id == CommandId(4))
            .unwrap();
        assert_eq!(placed.market_data_source, replayed_place.market_data_source);
        let mut live_projector = matching_runtime::MarketProjector::new(market);
        let mut replay_projector = matching_runtime::MarketProjector::new(market);
        let mut live_builder = matching_runtime::MboBuilder::new(market);
        let mut replay_builder = matching_runtime::MboBuilder::new(market);
        let detail = |_, _| None;
        for (live, recovered) in results.iter().zip(&replay.executions) {
            let live_events = live_projector.apply_formal_execution(live, detail).unwrap();
            let replay_events = replay_projector
                .apply_formal_execution(recovered, detail)
                .unwrap();
            assert_eq!(live_events, replay_events);
            for (a, b) in live_events.iter().zip(&replay_events) {
                assert_eq!(
                    live_builder.apply(a).unwrap(),
                    replay_builder.apply(b).unwrap()
                );
            }
        }
        assert_eq!(live_builder.digest(), replay_builder.digest());
        assert!(live_builder.visible_depth(Side::Buy).unwrap().is_empty());
    }
    let mut hash = matching_runtime::EventHasher::new();
    for (actual, expected) in replay.executions.iter().zip(&results) {
        assert_eq!(actual.events, expected.events);
        for event in &expected.events {
            hash.update(event).unwrap();
        }
    }
    assert_eq!(replay.event_hash, hash.finish());
    if kill == 0 && group {
        drop(manager);
        let wal = temp.0.join("wal").join("00000000000000000001.wal");
        let mut damaged = std::fs::read(&wal).unwrap();
        *damaged.last_mut().unwrap() ^= 1;
        std::fs::write(&wal, &damaged).unwrap();
        let manager = matching_persistence::ManifestManager::<
            matching_persistence::FileManifestStorage,
        >::open_recovery(temp.0.join("manifest"))
        .unwrap();
        assert!(
            matching_runtime::recover_market_data_from_genesis(
                temp.0.join("wal"),
                &manager,
                &trusted,
                &[(market, QueuePriorityPolicy::try_new(1).unwrap())],
            )
            .is_err()
        );
        assert!(
            matching_runtime::recover_snapshot_records_with_genesis(
                temp.0.join("wal"),
                &snapshots,
                &manager,
                &trusted,
                &[(market, QueuePriorityPolicy::try_new(1).unwrap())]
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&wal).unwrap(), damaged);
    }
}
