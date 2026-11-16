//! 真实 TCP 帧、网关、持久撮合、行情与结算事件贯通验证。
mod t22_support;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    AssetId, AssetReserve, ClusterId, Command, CommandEnvelope, CommandId, EngineEvent,
    FeeAssetPolicy, FeeRounding, FeeSchedule, FeeTier, LimitOrder, LogicalTime, MarketId,
    MarketMath, OrderId, OriginSessionId, PlaceLimitOrder, Price, Qty, ReservationId,
    ReservationToken, ShardId, Side, StpKey, StpMode, UpdateFeeSchedule, UserId,
};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    GatewayAuthenticator, GatewayPrincipal, GatewaySession, Health, HealthDependency,
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable,
    SessionControlJournal, SessionTarget, Shard, SnapshotRestoreConfig,
};
use std::io::{Read, Write};
use std::time::Duration;

struct Auth;
impl GatewayAuthenticator for Auth {
    fn authenticate(&self, key: u64, secret: &[u8]) -> Option<GatewayPrincipal> {
        (key == 2 && secret == b"test-secret").then_some(GatewayPrincipal {
            user: UserId(2),
            stp_key: StpKey::new(0),
        })
    }
}

#[test]
fn tcp_order_replays_to_matching_public_trade_and_settlement() {
    tcp_order_replays(false, 0);
    tcp_order_replays(true, 0);
}

#[test]
fn failed_event_handoff_blocks_readiness_and_remains_recoverable() {
    tcp_order_replays(false, 1);
}

#[test]
fn panicking_event_handoff_fails_closed_and_cleans_up() {
    tcp_order_replays(false, 2);
}

fn tcp_order_replays(group: bool, delivery_mode: u16) {
    let tmp = t22_support::TestDirectory::new();
    let id = 194 + delivery_mode;
    let (market, shard_id, cluster) = (MarketId(u32::from(id)), ShardId(id), ClusterId(id));
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let signing = SigningKey::from_bytes(&[19; 32]);
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
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
    let priority = QueuePriorityPolicy::try_new(32).unwrap();
    let mut shard = if group {
        Shard::start_group_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, priority)],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 7, 8, place.clone()),
            ((tmp.0.join("wal"), tmp.0.join("manifest")), (4, 10_000)),
        )
    } else {
        Shard::start_with_durable_place_support(
            shard_id,
            cluster,
            routes.clone(),
            vec![(market, priority)],
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
    let fee = command(
        1,
        None,
        Command::UpdateFeeSchedule(Box::new(UpdateFeeSchedule {
            history: vec![],
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
    );
    let fee_result = shard
        .handle()
        .submit_command(fee)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let token = |id: u128, user: u64, asset, atoms| {
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: UserId(user),
            market_id: market,
            reserves: vec![AssetReserve { asset, atoms }].into(),
            fee_schedule_version: 1,
            fee_tier_id: 1,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = signing
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        token
    };
    let maker = command(
        2,
        Some(UserId(1)),
        Command::PlaceLimit(Box::new(PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(2),
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
            reservation: token(2, 1, AssetId(1), 2),
        })),
    );
    let maker_result = shard
        .handle()
        .submit_command(maker)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let journal = SessionControlJournal::create_with_policy(
        tmp.0.join("tcp-session.log"),
        UserId(2),
        OriginSessionId(66),
        67,
        vec![SessionTarget {
            market,
            route_epoch: 1,
            command_id: CommandId(777),
        }],
        1,
        false,
    )
    .unwrap();
    let mut gateway = GatewaySession::open(&Auth, 2, b"test-secret", journal, 2, 32).unwrap();
    let store =
        matching_runtime::GatewayIntentStore::create(tmp.0.join("intents.log"), 2, 1024).unwrap();
    let bucket = matching_protocol::BucketConfig {
        capacity: 4,
        refill_interval: 1,
    };
    let limits = matching_protocol::GatewayRateLimiters::new(bucket, bucket, bucket, 2).unwrap();
    let shared = matching_runtime::GatewayShared::new(store, limits, || 0);
    let shards = [shard.handle()];
    let mut live_feed =
        matching_runtime::FeedMonitor::new(matching_runtime::FeedBuilder::new(market));
    for execution in [&fee_result, &maker_result] {
        for event in execution
            .market_data_source
            .get(&market)
            .into_iter()
            .flatten()
        {
            live_feed.apply(event).unwrap();
        }
    }
    let health = Health::new(shards.to_vec())
        .unwrap()
        .with_feeds(vec![live_feed.telemetry()], 1)
        .unwrap();
    for dependency in [
        HealthDependency::Recovery,
        HealthDependency::Persistence,
        HealthDependency::Control,
    ] {
        health.set_healthy(dependency, true);
    }
    let frame = matching_protocol::encode_place_request(
        42,
        &matching_protocol::PlaceRequest {
            key: 3,
            market: market.0,
            price: 10,
            qty: 2,
            stp_key: 0,
            user_control_epoch: 0,
            side: matching_protocol::PlaceSide::Buy,
            stp: matching_protocol::StpPolicy::None,
            post_only: false,
            client_order_id: None,
        },
        1024,
        32,
    )
    .unwrap();
    let risk_token = token(3, 2, AssetId(2), 20);
    let mut live_events = Vec::new();
    let mut live_trades = Vec::new();
    let mut live_deduplicated = Vec::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut transport = matching_runtime::GatewayTransport {
                session: &mut gateway,
                shared: &shared,
                health: &health,
                shards: &shards,
                max_frame: 1024,
                timeout: Duration::from_secs(5),
                allocate: || Ok((CommandId(3), OrderId(3))),
                authorize: |_: &matching_runtime::PreparedPlace| Ok(risk_token.clone()),
            };
            transport.serve_with(&mut socket, |result: &matching_runtime::FormalExecution| {
                if delivery_mode == 1 {
                    return Err(std::io::Error::other("模拟交接失败"));
                }
                assert_ne!(delivery_mode, 2, "模拟交接异常");
                live_deduplicated.push(result.deduplicated);
                live_events.extend(result.events.iter().cloned());
                for event in result.market_data_source.get(&market).into_iter().flatten() {
                    if let Some(trade) = live_feed
                        .apply(event)
                        .map_err(|_| std::io::Error::other("行情源不连续"))?
                        .trade
                    {
                        live_trades.push(trade);
                    }
                }
                Ok(())
            })
        });
        let mut client = std::net::TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.write_all(&frame).unwrap();
        if delivery_mode != 0 {
            let mut byte = [0];
            assert_eq!(client.read(&mut byte).unwrap(), 0);
            assert!(matches!(
                server.join().unwrap(),
                Err(matching_runtime::GatewayTransportError::Delivery(_))
            ));
            assert!(!health.ready());
            return;
        }
        let mut header = [0; matching_protocol::HEADER_LEN];
        client.read_exact(&mut header).unwrap();
        let len = matching_protocol::frame_length(&header, 1024).unwrap();
        let mut reply = header.to_vec();
        reply.resize(len, 0);
        client
            .read_exact(&mut reply[matching_protocol::HEADER_LEN..])
            .unwrap();
        let (request_id, acceptance) = matching_protocol::decode_acceptance(&reply, 1024).unwrap();
        assert_eq!(request_id, 42);
        assert_eq!(
            (
                acceptance.command_id,
                acceptance.order_id,
                acceptance.shard_seq
            ),
            (3, 3, 3)
        );
        client.write_all(&frame).unwrap();
        let mut retry_header = [0; matching_protocol::HEADER_LEN];
        client.read_exact(&mut retry_header).unwrap();
        let retry_len = matching_protocol::frame_length(&retry_header, 1024).unwrap();
        let mut retry_reply = retry_header.to_vec();
        retry_reply.resize(retry_len, 0);
        client
            .read_exact(&mut retry_reply[matching_protocol::HEADER_LEN..])
            .unwrap();
        assert_eq!(
            matching_protocol::decode_acceptance(&retry_reply, 1024).unwrap(),
            (request_id, acceptance)
        );
        drop(client);
        server.join().unwrap().unwrap();
    });
    if delivery_mode == 0 {
        assert!(health.prometheus().unwrap().contains(&format!(
            "market_data_feed_seq_last{{market_id=\"{}\",feed=\"trade\"}} 1\n",
            market.0
        )));
    }
    assert!(matches!(
        gateway.prepare_wire(&frame, 1024, CommandId(9), OrderId(9)),
        Err(matching_runtime::GatewayError::Closed)
    ));
    drop(gateway);
    let session_path = tmp.0.join("tcp-session.log");
    let closed_journal = std::fs::read(&session_path).unwrap();
    drop(SessionControlJournal::open(&session_path, 1).unwrap());
    assert_eq!(std::fs::read(&session_path).unwrap(), closed_journal);
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
    let replay = matching_runtime::recover_market_data_from_genesis(
        tmp.0.join("wal"),
        &manager,
        &trusted,
        &[(market, priority)],
    )
    .unwrap();
    assert_eq!(replay.executions.len(), 3);
    let taker = &replay.executions[2];
    if delivery_mode == 0 {
        assert_eq!(live_events, taker.events);
        assert_eq!(live_deduplicated, [false, true]);
    }
    assert_eq!(taker.command_id, CommandId(3));
    let trade = taker
        .events
        .iter()
        .find_map(|e| match &e.payload {
            EngineEvent::Trade(value) => Some(value),
            _ => None,
        })
        .unwrap();
    let settlement = taker
        .events
        .iter()
        .find_map(|e| match &e.payload {
            EngineEvent::SettlementDelta(value) => Some(value),
            _ => None,
        })
        .unwrap();
    assert_eq!(trade.trade_id, settlement.trade_id);
    assert_eq!((trade.price.get(), trade.qty.get()), (10, 2));
    let mut feed = matching_runtime::FeedBuilder::new(market);
    let mut public_trades = Vec::new();
    for execution in &replay.executions {
        for event in execution
            .market_data_source
            .get(&market)
            .into_iter()
            .flatten()
        {
            if let Some(trade) = feed.apply(event).unwrap().trade {
                public_trades.push(trade);
            }
        }
    }
    assert_eq!(public_trades.len(), 1);
    if delivery_mode == 0 {
        assert_eq!(live_trades, public_trades);
    }
    assert_eq!(public_trades[0].trade_id, trade.trade_id);
    assert_eq!(
        (public_trades[0].price.get(), public_trades[0].qty.get()),
        (10, 2)
    );
    let mut ledger = matching_risk::ReferenceLedger::new(
        UserId(99),
        [(UserId(1), AssetId(1), 2), (UserId(2), AssetId(2), 20)],
    )
    .unwrap();
    ledger.apply_event(settlement).unwrap();
    assert_eq!(ledger.balance(UserId(1), AssetId(2)), 20);
    assert_eq!(ledger.balance(UserId(2), AssetId(1)), 2);
}
