//! 以包含真实止损、到期索引和预留的市场镜像验证切换及后续命令重放。
use super::*;
use crate::{MarketRoute, MigrationId, PlaceLimits, RecoveredCommand};
use matching_domain::{
    AssetId, AssetReserve, ClosePolicy, ExecutionLimits, FeeAssetPolicy, FeeRounding, FeeSchedule,
    FeeScheduleBook, FeeTier, MarketConfig, MarketMath, OrderLimits, OriginSessionId, Price,
    PriceReference, PriceReferenceKind, Qty, QuoteAmount, Side, StopLimitOrder, StopOrder, StpKey,
    StpMode, TimeInForce, TriggerExpiry, UpdateReferencePrice, UserId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationStatus, RiskKeySet};

const LIMIT: usize = 1 << 20;

fn stop(id: u128, expiry: TriggerExpiry, market: MarketId) -> (StopOrder, ReservationState) {
    let user = UserId(u64::try_from(id).unwrap());
    let state = ReservationState {
        reservation_id: ReservationId(id),
        order_id: OrderId(id),
        user_id: user,
        market_id: market,
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 10,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    };
    let order = StopOrder::Limit(StopLimitOrder {
        trigger_reference: PriceReferenceKind::MarkPrice,
        order_id: state.order_id,
        user_id: user,
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        reservation_id: state.reservation_id,
        fee_schedule_version: 1,
        side: Side::Buy,
        stop_price: Price::try_new(12).unwrap(),
        price: Price::try_new(10).unwrap(),
        qty: Qty::try_new(1).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: expiry,
    });
    (order, state)
}

fn trusted(
    id: MigrationId,
    shard: ShardId,
    routes: RouteTable,
    markets: &[MarketId],
) -> SnapshotRestoreConfig {
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let limits = PlaceLimits {
        active_orders: 8,
        terminal_history: 8,
        client_id_bytes: 32,
    };
    let keys = RiskKeySet::new(1, []).unwrap();
    let place = if markets.is_empty() {
        PlaceSupportConfig::empty_for_migration(keys, limits)
    } else {
        PlaceSupportConfig::new(keys, markets.iter().map(|m| (*m, policy)), limits)
    }
    .unwrap();
    SnapshotRestoreConfig {
        cluster: id.cluster,
        shard,
        primary_epoch: 1,
        routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig {
            command_capacity: 8,
            session_capacity: 4,
            aliases_per_session: 4,
        },
        place: Some(place),
        expect_registry: true,
        max_markets: 2,
        max_bytes: LIMIT,
    }
}

#[test]
fn migrated_stops_keep_trigger_fifo_expiry_floors_and_replay_the_reference_update() {
    let id = MigrationId {
        cluster: ClusterId(95),
        market: MarketId(11),
        source: ShardId(7),
        target: ShardId(8),
        old_epoch: 4,
    };
    let market = id.market;
    let old = MarketRoute {
        market_id: market,
        shard_id: id.source,
        route_epoch: id.old_epoch,
        state: RouteState::Active,
    };
    let frozen = RouteTable::new([MarketRoute {
        state: RouteState::Migrating {
            source: id.source,
            target: id.target,
        },
        route_epoch: 5,
        ..old
    }])
    .unwrap();
    let active = RouteTable::new([MarketRoute {
        shard_id: id.target,
        route_epoch: 5,
        ..old
    }])
    .unwrap();
    let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(3)).unwrap();
    let src_cfg = trusted(id, id.source, frozen.clone(), &[market]);
    let dst_cfg = trusted(id, id.target, frozen, &[]);
    let mut source = empty_recovery_candidate(&src_cfg, &[(market, policy)], 0)
        .unwrap()
        .image;
    let mut target = empty_recovery_candidate(&dst_cfg, &[], 0).unwrap().image;
    let mut managed = matching_domain::ManagedMarket::new(MarketConfig {
        market_id: market,
        price_scale: 0,
        qty_scale: 0,
        quote_scale: 0,
        tick_size: 1,
        lot_size: 1,
        order_limits: OrderLimits {
            min_qty: Qty::try_new(1).unwrap(),
            max_qty: Qty::try_new(5).unwrap(),
            min_notional: QuoteAmount::new(1),
            max_notional: QuoteAmount::new(50),
        },
        price_band: None,
        active_fee_schedule_version: 1,
        close_policy: ClosePolicy::CancelAll,
        execution_limits: ExecutionLimits {
            max_internal_actions_per_command: 32,
            max_trades_per_command: 10,
            priority_exhaustion_guard: 1,
            synthetic_priority_ceiling: Some(3),
        },
    })
    .unwrap();
    managed.start_session(1, true).unwrap();
    managed.open(1, true, true).unwrap();
    managed.record_referenced_history();
    source.managed_markets.insert(market, managed);
    let fees = FeeScheduleBook::new(
        FeeSchedule::new(
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
    );
    source.place.as_mut().unwrap().fee_book = Some(fees.clone());
    target.place.as_mut().unwrap().fee_book = Some(fees);
    source.user_epochs.advance(UserId(90), 4).unwrap();
    target.user_epochs.advance(UserId(90), 6).unwrap();
    source.user_epochs.advance(UserId(22), 3).unwrap();
    let src_book = source.markets.get_mut(&market).unwrap();
    src_book
        .trigger_stops(
            PriceReference {
                kind: PriceReferenceKind::MarkPrice,
                source_seq: 1,
                price: Price::try_new(10).unwrap(),
            },
            LogicalTime::new(0),
            1,
            &MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            &[],
            |_| Ok(()),
        )
        .unwrap();
    let mut reservations = Vec::new();
    let first = TriggerExpiry::Day { session_id: 1 };
    let second = TriggerExpiry::Gtd {
        expire_at: LogicalTime::new(5),
    };
    for (number, expiry) in [(90, first), (22, second)] {
        let (order, state) = stop(number, expiry, market);
        src_book
            .place_stop_with_reservation(order, LogicalTime::new(0), 1, state.clone())
            .unwrap();
        reservations.push(state);
    }
    source.registry = Some(ReservationRegistry::new(reservations.clone()).unwrap());
    source.snapshot_seq = 100_000;
    let (source_data, _) =
        crate::MarketDataState::bootstrap_legacy(&source.markets, source.snapshot_seq).unwrap();
    source.place.as_mut().unwrap().market_data = Some(source_data);
    let before = source.market_state_digest(market, LIMIT).unwrap();
    assert_eq!(
        source
            .transfer_market_to(&mut target, market, &active, LIMIT)
            .unwrap(),
        before
    );
    let trusted = trusted(id, id.target, active.clone(), &[market]);
    let mut live =
        SnapshotImage::decode_snapshot(&target.encode_snapshot(LIMIT).unwrap(), &trusted).unwrap();
    assert_eq!(live.market_state_digest(market, LIMIT).unwrap(), before);
    assert_eq!(live.next_priorities(market), Ok((0, 2)));
    assert_eq!(
        live.check_user_epoch(UserId(22), 2),
        Err(RejectReason::StaleUserControlEpoch)
    );
    assert_eq!(
        live.check_user_epoch(UserId(90), 5),
        Err(RejectReason::StaleUserControlEpoch)
    );
    assert!(
        source
            .registry
            .as_ref()
            .unwrap()
            .get(ReservationId(90))
            .is_none()
    );
    let book = live.markets.get_mut(&market).unwrap();
    let mut expiry_probe = book.clone();
    assert_eq!(
        expiry_probe.expire_stop_day_session(1)[0].0.order_id(),
        OrderId(90)
    );
    assert!(
        expiry_probe
            .expire_stop_gtd_through(LogicalTime::new(4))
            .is_empty()
    );
    assert_eq!(
        expiry_probe.expire_stop_gtd_through(LogicalTime::new(5))[0]
            .0
            .order_id(),
        OrderId(22)
    );
    // 正式协议尚无新止损命令，从订单簿已支持的预留入口补入第三笔并对照原有队列。
    let (new_order, new_state) = stop(7, TriggerExpiry::Gtc, market);
    assert_eq!(
        book.place_stop_with_reservation(new_order, LogicalTime::new(0), 1, new_state.clone())
            .unwrap()
            .get(),
        2
    );
    reservations.push(new_state);
    live.registry = Some(ReservationRegistry::new(reservations).unwrap());
    let (overflow, _) = stop(8, TriggerExpiry::Gtc, market);
    assert_eq!(
        book.place_stop(overflow, LogicalTime::new(0), 1),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(
        book.stop_orders(Side::Buy)
            .iter()
            .map(|(s, p)| (s.order_id().get(), p.get()))
            .collect::<Vec<_>>(),
        [(90, 0), (22, 1), (7, 2)]
    );
    let bytes = live.encode_snapshot(LIMIT).unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(96),
        session_id: 2,
        client_seq: 1,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(5),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::UpdateReferencePrice(UpdateReferencePrice {
            market_id: market,
            kind: PriceReferenceKind::MarkPrice,
            price: Price::try_new(13).unwrap(),
            source_seq: 2,
        }),
    };
    let epoch = live.primary_epoch;
    let mut seq = Sequencer::from_last_applied(live.snapshot_seq).unwrap();
    let execution = execute_market_management(
        MarketManagementContext {
            identity: FormalIdentity {
                cluster: id.cluster,
                shard: id.target,
                event_capacity: 64,
            },
            routes: &active,
            seq: &mut seq,
            clock: &LogicalClock::new(live.now),
            markets: &mut live.markets,
            managed_markets: &mut live.managed_markets,
            dedup: live.dedup.as_mut().unwrap(),
            place: &mut live.place,
            registry: &mut live.registry,
        },
        command.clone(),
    )
    .unwrap();
    assert_eq!(
        execution
            .events
            .iter()
            .filter_map(|event| match event.payload {
                EngineEvent::StopTriggered { order_id, .. } => Some(order_id.get()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [90, 22, 7]
    );
    let projected = live
        .place
        .as_mut()
        .unwrap()
        .market_data
        .as_mut()
        .unwrap()
        .apply(&execution)
        .unwrap();
    assert_eq!(projected[&market][0].sequence, 2);
    assert_eq!(live.next_priorities(market), Ok((3, 3)));
    live.snapshot_seq = execution.seq;
    let recovered = replay_snapshot_records(
        SnapshotRecoveryCandidate {
            image: SnapshotImage::decode_snapshot(&bytes, &trusted).unwrap(),
            committed_through: execution.seq,
            used_previous: false,
        },
        vec![RecoveredCommand {
            shard_seq: execution.seq,
            epoch,
            engine_time: 0,
            command: matching_domain::decode_wal_command(
                &matching_domain::encode_wal_command(&command).unwrap(),
            )
            .unwrap(),
        }],
    )
    .unwrap();
    let mut hash = crate::EventHasher::new();
    for e in &execution.events {
        hash.update(e).unwrap();
    }
    assert_eq!(execution.events, recovered.executions[0].events);
    assert_eq!(hash.finish(), recovered.event_hash);
    assert_eq!(
        live.state_digest(LIMIT).unwrap(),
        recovered.image.state_digest(LIMIT).unwrap()
    );
}
