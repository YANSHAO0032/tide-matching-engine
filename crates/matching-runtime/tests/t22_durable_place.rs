//! 验证正式限价入口的可信启动与客户端身份生命周期。
mod t22_support;
use matching_domain::{AssetId, ClusterId, LogicalTime, MarketId, MarketMath, ShardId};
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{LimitReservePolicy, ReservationRegistry, RiskKeySet};
use matching_runtime::{
    IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState, RouteTable, Shard,
};

/// 在有界队列暂时繁忙时重试尚未入队的请求。
fn retry<T>(mut submit: impl FnMut() -> Result<T, matching_runtime::ShardError>) -> T {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match submit() {
            Ok(value) => return value,
            Err(matching_runtime::ShardError::Busy) if std::time::Instant::now() < until => {
                std::thread::yield_now()
            }
            Err(error) => panic!("入口失败: {error:?}"),
        }
    }
}
#[test]
fn durable_place_and_mass_cancel_write_before_reply() {
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetReserve, Command, CommandEnvelope, CommandId, LimitOrder, MassCancelShard, OrderId,
        PlaceLimitOrder, Price, Qty, ReservationId, ReservationToken, ShardCancelScope, Side,
        StpKey, StpMode, UserId,
    };
    use std::time::Duration;
    let market = MarketId(124);
    let shard_id = ShardId(124);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(1, [(1, signing.verifying_key())]).unwrap();
    let policy = LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let config = PlaceSupportConfig::new(
        keys,
        [(market, policy)],
        PlaceLimits {
            active_orders: 2,
            terminal_history: 2,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let temp = t22_support::TestDirectory::new();
    let base = &temp.0;
    let shared = base.join("wal");
    assert!(
        Shard::start_with_durable_place_support(
            shard_id,
            ClusterId(1),
            routes.clone(),
            vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
            (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
            (IdempotencyConfig::default(), 1, 8, config.clone()),
            (shared.clone(), shared.join("."))
        )
        .is_err()
    );
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
    let mut shard = Shard::start_with_durable_place_support(
        shard_id,
        ClusterId(1),
        routes.clone(),
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 1, 8, config.clone()),
        (base.join("wal"), base.join("manifest")),
    )
    .unwrap();
    let make = |command_id: u128, order_id: u128, client_seq: u64| {
        let mut token = ReservationToken {
            token_id: command_id,
            command_id: CommandId(command_id),
            reservation_id: ReservationId(command_id),
            order_id: OrderId(order_id),
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
        CommandEnvelope {
            command_id: CommandId(command_id),
            session_id: 11,
            client_seq,
            market_id: Some(market),
            user_id: Some(UserId(7)),
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(order_id),
                    user_id: UserId(7),
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(10).unwrap(),
                    qty: Qty::try_new(2).unwrap(),
                },
                client_order_id: Some("client-1".into()),
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        }
    };
    let mut command = make(1, 1, 1);
    struct Auth;
    impl matching_runtime::GatewayAuthenticator for Auth {
        fn authenticate(
            &self,
            key: u64,
            secret: &[u8],
        ) -> Option<matching_runtime::GatewayPrincipal> {
            (key == 7 && secret == b"secret").then_some(matching_runtime::GatewayPrincipal {
                user: UserId(7),
                stp_key: StpKey::new(0),
            })
        }
    }
    let journal_path = base.join("gateway-session.log");
    let journal = matching_runtime::SessionControlJournal::create(
        &journal_path,
        UserId(7),
        matching_domain::OriginSessionId(11),
        99,
        vec![matching_runtime::SessionTarget {
            market,
            route_epoch: 1,
            command_id: CommandId(31),
        }],
        1,
    )
    .unwrap();
    let mut gateway =
        matching_runtime::GatewaySession::open(&Auth, 7, b"secret", journal, 8, 32).unwrap();
    let Command::PlaceLimit(input) = &mut command.payload else {
        unreachable!()
    };
    let wire = matching_protocol::encode_place_request(
        99,
        &matching_protocol::PlaceRequest {
            key: 1,
            market: market.0,
            price: 10,
            qty: 2,
            stp_key: 0,
            user_control_epoch: 0,
            side: matching_protocol::PlaceSide::Buy,
            stp: matching_protocol::StpPolicy::None,
            post_only: false,
            client_order_id: Some("client-1".into()),
        },
        1024,
        32,
    )
    .unwrap();
    let rate = matching_protocol::BucketConfig {
        capacity: 1,
        refill_interval: 100,
    };
    let mut limiter = matching_protocol::GatewayRateLimiters::new(rate, rate, rate, 8).unwrap();
    let mut intents =
        matching_runtime::GatewayIntentStore::create(base.join("intents.log"), 8, 1024).unwrap();
    let (request_id, prepared) = gateway
        .prepare_wire_durable(&wire, 1024, &mut intents, &mut limiter, 0, || {
            Ok((command.command_id, OrderId(1)))
        })
        .unwrap();
    assert_eq!(request_id, 99);
    assert_eq!(prepared.command_id(), input.reservation.command_id);
    input.reservation.signature = signing
        .sign(&input.reservation.canonical_signing_payload().unwrap())
        .to_bytes();
    let token = input.reservation.clone();
    let mut wrong_token = token.clone();
    wrong_token.command_id = CommandId(999);
    assert!(matches!(
        gateway.submit_place(&prepared, wrong_token, 1, &mut limiter, 0, &shard.handle()),
        Err(matching_runtime::GatewayError::Reservation)
    ));
    assert!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let mut stale = command.clone();
    stale.route_epoch = Some(999);
    assert!(
        retry(|| shard.handle().submit_command(stale.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let mut huge_stale = command.clone();
    huge_stale.route_epoch = Some(999);
    if let Command::PlaceLimit(place) = &mut huge_stale.payload {
        place.client_order_id = Some("x".repeat(16 * 1024 * 1024 + 1));
    }
    let rejection = retry(|| shard.handle().submit_command(huge_stale.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    assert_eq!(rejection, matching_domain::RejectReason::StaleRouteEpoch);
    assert!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
    let first = gateway
        .submit_place(&prepared, token, 1, &mut limiter, 0, &shard.handle())
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    assert!(
        retry(|| shard.handle().submit_command(command.clone().clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .deduplicated
    );
    let mut huge_conflict = command.clone();
    if let Command::PlaceLimit(place) = &mut huge_conflict.payload {
        place.client_order_id = Some("z".repeat(16 * 1024 * 1024 + 1));
    }
    let rejected = retry(|| shard.handle().submit_command(huge_conflict.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    assert_eq!(rejected, matching_runtime::FormalError::ProtocolError);
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    let mut conflict = command.clone();
    if let Command::PlaceLimit(place) = &mut conflict.payload {
        place.order.qty = Qty::try_new(1).unwrap();
    }
    assert!(
        retry(|| shard.handle().submit_command(conflict.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    let mut taker = make(2, 2, 2);
    taker.user_id = Some(UserId(8));
    taker.session_id = 12;
    if let Command::PlaceLimit(place) = &mut taker.payload {
        place.order.user_id = UserId(8);
        place.order.side = Side::Sell;
        place.order.qty = Qty::try_new(1).unwrap();
        place.client_order_id = Some("client-2".into());
        place.reservation.user_id = UserId(8);
        place.reservation.reserves[0].asset = AssetId(1);
        place.reservation.reserves[0].atoms = 1;
        place.reservation.signature = signing
            .sign(&place.reservation.canonical_signing_payload().unwrap())
            .to_bytes();
    }
    let filled = retry(|| shard.handle().submit_command(taker.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(filled.seq, 2);
    let expected_id = matching_domain::TradeId((1_u128 << 112) | (124_u128 << 96) | (2_u128 << 32));
    let payloads: Vec<_> = filled
        .events
        .iter()
        .map(|event| event.payload.clone())
        .collect();
    assert_eq!(
        payloads,
        vec![
            matching_domain::EngineEvent::OrderAccepted {
                order_id: OrderId(2)
            },
            matching_domain::EngineEvent::Trade(matching_domain::TradeEvent {
                trade_id: expected_id,
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                maker_side: Side::Buy
            }),
            matching_domain::EngineEvent::OrderPartiallyFilled {
                order_id: OrderId(1),
                remaining: Qty::try_new(1).unwrap()
            },
            matching_domain::EngineEvent::OrderFilled {
                order_id: OrderId(2)
            },
        ]
    );
    for (index, event) in filled.events.iter().enumerate() {
        assert_eq!(event.event_index, index as u32);
        assert_eq!(event.shard_seq, 2);
    }
    let session_cancel = |id, session| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 99,
        client_seq: id as u64,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Session {
                user_id: UserId(7),
                session_id: matching_domain::OriginSessionId(session),
            },
        }),
    };
    let wrong_session = session_cancel(30, 12);
    let mut untrusted = wrong_session.clone();
    untrusted.user_id = Some(UserId(7));
    assert_eq!(
        retry(|| shard.handle().submit_command(untrusted.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err(),
        matching_domain::RejectReason::InvalidOrderCombination
    );
    let untouched = retry(|| shard.handle().submit_command(wrong_session.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(untouched.events.is_empty());
    gateway
        .disconnect(&[shard.handle()], Duration::from_secs(5))
        .unwrap();
    drop(gateway);
    let journal = matching_runtime::SessionControlJournal::open(&journal_path, 1).unwrap();
    let right_session = journal.command(market).unwrap();
    let canceled = retry(|| shard.handle().submit_command(right_session.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(canceled.events.iter().any(|event| matches!(
        event.payload,
        matching_domain::EngineEvent::OrderCanceled {
            order_id: OrderId(1),
            ..
        }
    )));
    // 分片已提交而协调日志尚未确认时，恢复后重发同一命令。
    drop(journal);
    let mut journal = matching_runtime::SessionControlJournal::open(&journal_path, 1).unwrap();
    assert_eq!(journal.command(market).unwrap(), right_session);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let summary = loop {
        match matching_runtime::dispatch_session_cancel(
            &mut journal,
            market,
            &shard.handle(),
            Duration::from_secs(5),
        ) {
            Err(matching_runtime::SessionDispatchError::Ingress(
                matching_runtime::ShardError::Busy,
            )) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            result => break result.unwrap(),
        }
    };
    assert_eq!(summary.canceled_orders, 1);
    drop(journal);
    assert!(
        matching_runtime::SessionControlJournal::open(&journal_path, 1)
            .unwrap()
            .pending()
            .unwrap()
            .is_empty()
    );
    let cancel = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 11,
        client_seq: 3,
        market_id: Some(market),
        user_id: None,
        route_epoch: Some(1),
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::MassCancelShard(MassCancelShard {
            scope: ShardCancelScope::Market { market_id: market },
        }),
    };
    let mut audit =
        matching_runtime::AuditLog::create(base.join("mass-cancel-audit.log"), 2).unwrap();
    let result = audit
        .dispatch(77, &shard.handle(), cancel.clone(), Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        audit.records()[0].action,
        matching_runtime::AuditAction::MassCancel
    );
    assert_eq!(
        audit.records()[1].outcome,
        matching_runtime::AuditOutcome::Succeeded
    );
    assert_eq!(result.seq, 5);
    let fanout_path = base.join("fanout-session.log");
    let mut fanout = matching_runtime::SessionControlJournal::create(
        &fanout_path,
        UserId(7),
        matching_domain::OriginSessionId(11),
        100,
        vec![
            matching_runtime::SessionTarget {
                market,
                route_epoch: 1,
                command_id: CommandId(41),
            },
            matching_runtime::SessionTarget {
                market: MarketId(125),
                route_epoch: 1,
                command_id: CommandId(42),
            },
        ],
        2,
    )
    .unwrap();
    fanout.disconnect().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match matching_runtime::dispatch_session_cancels(
            &mut fanout,
            &[shard.handle()],
            Duration::from_secs(5),
        ) {
            Err(matching_runtime::SessionDispatchError::Ingress(
                matching_runtime::ShardError::Busy,
            )) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            Err(matching_runtime::SessionDispatchError::Route) => break,
            other => panic!("扇出结果错误: {other:?}"),
        }
    }
    drop(fanout);
    let resumed = matching_runtime::SessionControlJournal::open(&fanout_path, 2).unwrap();
    assert_eq!(resumed.pending().unwrap().len(), 1);
    assert_eq!(resumed.pending().unwrap()[0].market, MarketId(125));
    let fence = CommandEnvelope {
        command_id: CommandId(60),
        session_id: 198,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(7)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(matching_domain::ApplyUserControlFenceShard {
            parent_command_id: CommandId(60),
            user_id: UserId(7),
            new_epoch: 1,
            action: matching_domain::UserFenceAction::FenceOnly,
            expected_primary_epoch: 1,
        }),
    };
    retry(|| shard.handle().submit_command(fence.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    // 通过真实 TCP 连接接收请求，客户端断开后自动排空并撤销来源会话的挂单。
    {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let tcp_journal = matching_runtime::SessionControlJournal::create(
            base.join("tcp-session.log"),
            UserId(7),
            matching_domain::OriginSessionId(22),
            199,
            vec![matching_runtime::SessionTarget {
                market,
                route_epoch: 1,
                command_id: CommandId(51),
            }],
            1,
        )
        .unwrap();
        let mut tcp_gateway =
            matching_runtime::GatewaySession::open(&Auth, 7, b"secret", tcp_journal, 5, 32)
                .unwrap();
        let tcp_intents =
            matching_runtime::GatewayIntentStore::create(base.join("tcp-intents.log"), 6, 1024)
                .unwrap();
        let config = matching_protocol::BucketConfig {
            capacity: 8,
            refill_interval: 1,
        };
        let tcp_limits =
            matching_protocol::GatewayRateLimiters::new(config, config, config, 4).unwrap();
        let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let pause_clock = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let report_second = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (sampled, sampled_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_clock, resume_clock_rx) = std::sync::mpsc::sync_channel(1);
        let (second_sampled, second_sampled_rx) = std::sync::mpsc::sync_channel(1);
        let clock_source = clock.clone();
        let pause_source = pause_clock.clone();
        let report_source = report_second.clone();
        let shared = matching_runtime::GatewayShared::new(tcp_intents, tcp_limits, move || {
            use std::sync::atomic::Ordering;
            let value = clock_source.load(Ordering::SeqCst);
            if pause_source.swap(false, Ordering::SeqCst) {
                sampled.send(()).unwrap();
                resume_clock_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            } else if report_source.swap(false, Ordering::SeqCst) {
                second_sampled.send(()).unwrap();
            }
            value
        });
        let shards = [shard.handle()];
        let health = matching_runtime::Health::new(shards.to_vec()).unwrap();
        for dependency in [
            matching_runtime::HealthDependency::Recovery,
            matching_runtime::HealthDependency::Persistence,
            matching_runtime::HealthDependency::Control,
        ] {
            health.set_healthy(dependency, true);
        }
        let wire = matching_protocol::encode_place_request(
            101,
            &matching_protocol::PlaceRequest {
                key: 50,
                market: market.0,
                price: 10,
                qty: 2,
                stp_key: 0,
                user_control_epoch: 1,
                side: matching_protocol::PlaceSide::Buy,
                stp: matching_protocol::StpPolicy::None,
                post_only: false,
                client_order_id: None,
            },
            1024,
            32,
        )
        .unwrap();
        let mut authorization_count = 0_u64;
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut next = 50_u128;
                let mut transport = matching_runtime::GatewayTransport {
                    session: &mut tcp_gateway,
                    shared: &shared,
                    health: &health,
                    shards: &shards,
                    max_frame: 1024,
                    timeout: Duration::from_secs(5),
                    allocate: || {
                        let id = next;
                        next = next.checked_add(2).unwrap();
                        Ok((CommandId(id), OrderId(id)))
                    },
                    authorize: |prepared: &matching_runtime::PreparedPlace| {
                        authorization_count = authorization_count.checked_add(1).unwrap();
                        if prepared.command_id() == CommandId(56) {
                            return Err(matching_runtime::GatewayError::Rejected(
                                matching_domain::RejectReason::InsufficientReservation,
                            ));
                        }
                        let mut token = ReservationToken {
                            token_id: prepared
                                .command_id()
                                .0
                                .checked_add(u128::from(authorization_count))
                                .unwrap(),
                            command_id: prepared.command_id(),
                            reservation_id: ReservationId(prepared.command_id().0),
                            order_id: prepared.intent().order.order_id,
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
                        Ok(token)
                    },
                };
                transport.serve(&mut socket).unwrap();
            });
            let mut client = TcpStream::connect(address).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            health.set_healthy(matching_runtime::HealthDependency::Persistence, false);
            client.write_all(&wire).unwrap();
            let mut rejected_header = [0_u8; matching_protocol::HEADER_LEN];
            client.read_exact(&mut rejected_header).unwrap();
            let rejected_length = matching_protocol::frame_length(&rejected_header, 1024).unwrap();
            let mut rejected = rejected_header.to_vec();
            rejected.resize(rejected_length, 0);
            client
                .read_exact(&mut rejected[matching_protocol::HEADER_LEN..])
                .unwrap();
            assert_eq!(
                matching_protocol::decode_rejection(&rejected, 1024)
                    .unwrap()
                    .1
                    .get(),
                5002
            );
            health.set_healthy(matching_runtime::HealthDependency::Persistence, true);
            client.write_all(&wire).unwrap();
            let mut header = [0_u8; matching_protocol::HEADER_LEN];
            client.read_exact(&mut header).unwrap();
            let length = matching_protocol::frame_length(&header, 1024).unwrap();
            let mut response = header.to_vec();
            response.resize(length, 0);
            client
                .read_exact(&mut response[matching_protocol::HEADER_LEN..])
                .unwrap();
            let response = matching_protocol::decode_frame(&response, 1024).unwrap();
            assert_eq!(response.message_type, 3);
            assert_eq!(response.request_id, 101);
            assert_eq!(
                u128::from_be_bytes(response.payload[16..32].try_into().unwrap()),
                50
            );
            for (range, reason) in [(20..28, 2002_u16), (28..36, 2003_u16)] {
                let mut bad = matching_protocol::decode_frame(&wire, 1024).unwrap();
                bad.payload[range].fill(0);
                client
                    .write_all(&matching_protocol::encode_frame(&bad, 1024).unwrap())
                    .unwrap();
                let mut header = [0_u8; matching_protocol::HEADER_LEN];
                client.read_exact(&mut header).unwrap();
                let len = matching_protocol::frame_length(&header, 1024).unwrap();
                let mut reply = header.to_vec();
                reply.resize(len, 0);
                client
                    .read_exact(&mut reply[matching_protocol::HEADER_LEN..])
                    .unwrap();
                assert_eq!(
                    matching_protocol::decode_rejection(&reply, 1024)
                        .unwrap()
                        .1
                        .get(),
                    reason
                );
            }
            assert!(
                retry(|| shard.handle().order(market, OrderId(50)))
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
            client.write_all(&wire).unwrap();
            let mut retry_header = [0_u8; matching_protocol::HEADER_LEN];
            client.read_exact(&mut retry_header).unwrap();
            let retry_length = matching_protocol::frame_length(&retry_header, 1024).unwrap();
            let mut retry_frame = retry_header.to_vec();
            retry_frame.resize(retry_length, 0);
            client
                .read_exact(&mut retry_frame[matching_protocol::HEADER_LEN..])
                .unwrap();
            assert_eq!(
                matching_protocol::decode_frame(&retry_frame, 1024).unwrap(),
                response
            );
            let stale = matching_protocol::encode_place_request(
                102,
                &matching_protocol::PlaceRequest {
                    key: 52,
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
            client.write_all(&stale).unwrap();
            let mut header = [0_u8; matching_protocol::HEADER_LEN];
            client.read_exact(&mut header).unwrap();
            let length = matching_protocol::frame_length(&header, 1024).unwrap();
            let mut rejection = header.to_vec();
            rejection.resize(length, 0);
            client
                .read_exact(&mut rejection[matching_protocol::HEADER_LEN..])
                .unwrap();
            assert_eq!(
                matching_protocol::decode_rejection(&rejection, 1024)
                    .unwrap()
                    .1
                    .get(),
                4005
            );
            assert!(
                retry(|| shard.handle().order(market, OrderId(50)))
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
            // 第一连接保持打开时，第二连接共享同一身份表和限流器继续服务。
            let wire_for_retry = wire.clone();
            std::thread::scope(|nested| {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let journal = matching_runtime::SessionControlJournal::create(
                    base.join("tcp-session-2.log"),
                    UserId(7),
                    matching_domain::OriginSessionId(23),
                    200,
                    vec![matching_runtime::SessionTarget {
                        market,
                        route_epoch: 1,
                        command_id: CommandId(71),
                    }],
                    1,
                )
                .unwrap();
                let mut gateway =
                    matching_runtime::GatewaySession::open(&Auth, 7, b"secret", journal, 3, 32)
                        .unwrap();
                let shared_ref = &shared;
                let shards_ref = &shards;
                let health_ref = &health;
                let signing_ref = &signing;
                let (risk_started, risk_started_rx) = std::sync::mpsc::sync_channel(1);
                let (resume_risk, resume_risk_rx) = std::sync::mpsc::sync_channel(1);
                let second = nested.spawn(move || {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut transport = matching_runtime::GatewayTransport {
                        session: &mut gateway,
                        shared: shared_ref,
                        health: health_ref,
                        shards: shards_ref,
                        max_frame: 1024,
                        timeout: Duration::from_secs(5),
                        allocate: || Ok((CommandId(70), OrderId(70))),
                        authorize: |prepared: &matching_runtime::PreparedPlace| {
                            risk_started.send(()).unwrap();
                            resume_risk_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                            let mut token = ReservationToken {
                                token_id: 70,
                                command_id: prepared.command_id(),
                                reservation_id: ReservationId(70),
                                order_id: OrderId(70),
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
                            token.signature = signing_ref
                                .sign(&token.canonical_signing_payload().unwrap())
                                .to_bytes();
                            Ok(token)
                        },
                    };
                    transport.serve(&mut socket).unwrap();
                });
                let mut client2 = TcpStream::connect(address).unwrap();
                client2
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let wire = matching_protocol::encode_place_request(
                    201,
                    &matching_protocol::PlaceRequest {
                        key: 70,
                        market: market.0,
                        price: 10,
                        qty: 2,
                        stp_key: 0,
                        user_control_epoch: 1,
                        side: matching_protocol::PlaceSide::Buy,
                        stp: matching_protocol::StpPolicy::None,
                        post_only: false,
                        client_order_id: None,
                    },
                    1024,
                    32,
                )
                .unwrap();
                // 控制取时交错：较早时间必须先持共享锁提交。
                clock.store(100, std::sync::atomic::Ordering::SeqCst);
                pause_clock.store(true, std::sync::atomic::Ordering::SeqCst);
                client.write_all(&wire_for_retry).unwrap();
                sampled_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                clock.store(101, std::sync::atomic::Ordering::SeqCst);
                report_second.store(true, std::sync::atomic::Ordering::SeqCst);
                client2.write_all(&wire).unwrap();
                assert!(matches!(
                    second_sampled_rx.recv_timeout(Duration::from_millis(100)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ));
                resume_clock.send(()).unwrap();
                let mut header = [0_u8; matching_protocol::HEADER_LEN];
                client.read_exact(&mut header).unwrap();
                let length = matching_protocol::frame_length(&header, 1024).unwrap();
                let mut frame = header.to_vec();
                frame.resize(length, 0);
                client
                    .read_exact(&mut frame[matching_protocol::HEADER_LEN..])
                    .unwrap();
                assert_eq!(
                    matching_protocol::decode_acceptance(&frame, 1024)
                        .unwrap()
                        .1
                        .order_id,
                    50
                );
                second_sampled_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                risk_started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                // 第二连接等待风险服务时，第一连接仍可通过共享状态重试。
                client.write_all(&wire_for_retry).unwrap();
                let mut first_header = [0_u8; matching_protocol::HEADER_LEN];
                client.read_exact(&mut first_header).unwrap();
                let first_len = matching_protocol::frame_length(&first_header, 1024).unwrap();
                let mut first_reply = first_header.to_vec();
                first_reply.resize(first_len, 0);
                client
                    .read_exact(&mut first_reply[matching_protocol::HEADER_LEN..])
                    .unwrap();
                assert_eq!(
                    matching_protocol::decode_acceptance(&first_reply, 1024)
                        .unwrap()
                        .1
                        .order_id,
                    50
                );
                resume_risk.send(()).unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let mut header = [0_u8; matching_protocol::HEADER_LEN];
                    client2.read_exact(&mut header).unwrap();
                    let length = matching_protocol::frame_length(&header, 1024).unwrap();
                    let mut frame = header.to_vec();
                    frame.resize(length, 0);
                    client2
                        .read_exact(&mut frame[matching_protocol::HEADER_LEN..])
                        .unwrap();
                    if let Ok((_, accepted)) = matching_protocol::decode_acceptance(&frame, 1024) {
                        assert_eq!(accepted.order_id, 70);
                        break;
                    }
                    assert_eq!(
                        matching_protocol::decode_rejection(&frame, 1024)
                            .unwrap()
                            .1
                            .get(),
                        5002
                    );
                    assert!(std::time::Instant::now() < deadline);
                    clock
                        .fetch_update(
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                            |value| value.checked_add(1),
                        )
                        .unwrap();
                    client2.write_all(&wire).unwrap();
                    std::thread::yield_now();
                }
                drop(client2);
                second.join().unwrap();
            });
            // 已回复的关联身份可复用；它不充当会话去重序号。
            let next_wire = matching_protocol::encode_place_request(
                101,
                &matching_protocol::PlaceRequest {
                    key: 54,
                    market: market.0,
                    price: 10,
                    qty: 2,
                    stp_key: 0,
                    user_control_epoch: 1,
                    side: matching_protocol::PlaceSide::Buy,
                    stp: matching_protocol::StpPolicy::None,
                    post_only: false,
                    client_order_id: None,
                },
                1024,
                32,
            )
            .unwrap();
            client.write_all(&next_wire).unwrap();
            let mut header = [0_u8; matching_protocol::HEADER_LEN];
            client.read_exact(&mut header).unwrap();
            let length = matching_protocol::frame_length(&header, 1024).unwrap();
            let mut next_response = header.to_vec();
            next_response.resize(length, 0);
            client
                .read_exact(&mut next_response[matching_protocol::HEADER_LEN..])
                .unwrap();
            let (_, accepted) = matching_protocol::decode_acceptance(&next_response, 1024).unwrap();
            assert_eq!(accepted.order_id, 54);
            // 风险拒绝、分片容量和网关容量均不能关闭连接或撤销旧单。
            for (request_id, key, expected) in [(103, 56, 4001), (104, 58, 5002), (105, 60, 5002)] {
                let overloaded = matching_protocol::encode_place_request(
                    request_id,
                    &matching_protocol::PlaceRequest {
                        key,
                        market: market.0,
                        price: 10,
                        qty: 2,
                        stp_key: 0,
                        user_control_epoch: 1,
                        side: matching_protocol::PlaceSide::Buy,
                        stp: matching_protocol::StpPolicy::None,
                        post_only: false,
                        client_order_id: None,
                    },
                    1024,
                    32,
                )
                .unwrap();
                client.write_all(&overloaded).unwrap();
                let mut header = [0_u8; matching_protocol::HEADER_LEN];
                client.read_exact(&mut header).unwrap();
                let length = matching_protocol::frame_length(&header, 1024).unwrap();
                let mut frame = header.to_vec();
                frame.resize(length, 0);
                client
                    .read_exact(&mut frame[matching_protocol::HEADER_LEN..])
                    .unwrap();
                assert_eq!(
                    matching_protocol::decode_rejection(&frame, 1024)
                        .unwrap()
                        .1
                        .get(),
                    expected
                );
            }
            assert!(
                retry(|| shard.handle().order(market, OrderId(50)))
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
            drop(client);
            worker.join().unwrap();
        });
        assert_eq!(authorization_count, 5);
        assert!(
            retry(|| shard.handle().order(market, OrderId(50)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            retry(|| shard.handle().order(market, OrderId(54)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .is_none()
        );
    }
    let records = matching_persistence::scan_segments(base.join("wal"))
        .unwrap()
        .records;
    assert_eq!(records.len(), 12);
    assert_eq!(
        matching_domain::decode_wal_command(&records[4].payload).unwrap(),
        cancel
    );
    let updated_routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id,
        route_epoch: 2,
        state: RouteState::Active,
    }])
    .unwrap();
    let route_dir = base.join("route-control");
    std::fs::create_dir(&route_dir).unwrap();
    let route_store = matching_runtime::RouteControlStore::create_for_cluster(
        &route_dir,
        ClusterId(1),
        routes.clone(),
        8,
        8,
    )
    .unwrap();
    let mut route_coordinator =
        matching_runtime::RouteCoordinator::new(route_store, vec![shard.handle()]).unwrap();
    route_coordinator
        .update(1, updated_routes.clone(), Duration::from_secs(5))
        .unwrap();
    let mut after_refresh = cancel.clone();
    after_refresh.command_id = CommandId(200);
    after_refresh.session_id = 200;
    after_refresh.client_seq = 1;
    assert_eq!(
        retry(|| shard.handle().submit_command(after_refresh.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err(),
        matching_domain::RejectReason::StaleRouteEpoch
    );
    after_refresh.route_epoch = Some(2);
    let refreshed_result = retry(|| shard.handle().submit_command(after_refresh.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(refreshed_result.seq, 13);
    let refreshed_journal = matching_runtime::SessionControlJournal::create(
        base.join("gateway-stale-session.log"),
        UserId(7),
        matching_domain::OriginSessionId(201),
        202,
        vec![matching_runtime::SessionTarget {
            market,
            route_epoch: 1,
            command_id: CommandId(202),
        }],
        1,
    )
    .unwrap();
    let mut refreshed_gateway =
        matching_runtime::GatewaySession::open(&Auth, 7, b"secret", refreshed_journal, 8, 32)
            .unwrap();
    let mut refreshed_command = make(201, 201, 1);
    let Command::PlaceLimit(ref mut refreshed_place) = refreshed_command.payload else {
        unreachable!()
    };
    refreshed_place.client_order_id = Some("client-refresh".into());
    refreshed_place.user_control_epoch = 1;
    let refreshed_wire = matching_protocol::encode_place_request(
        201,
        &matching_protocol::PlaceRequest {
            key: 201,
            market: market.0,
            price: 10,
            qty: 2,
            stp_key: 0,
            user_control_epoch: 1,
            side: matching_protocol::PlaceSide::Buy,
            stp: matching_protocol::StpPolicy::None,
            post_only: false,
            client_order_id: Some("client-refresh".into()),
        },
        1024,
        32,
    )
    .unwrap();
    let mut refreshed_limits =
        matching_protocol::GatewayRateLimiters::new(rate, rate, rate, 8).unwrap();
    let mut refreshed_intents =
        matching_runtime::GatewayIntentStore::create(base.join("refreshed-intents.log"), 8, 1024)
            .unwrap();
    let (_, refreshed_prepared) = refreshed_gateway
        .prepare_wire_durable(
            &refreshed_wire,
            1024,
            &mut refreshed_intents,
            &mut refreshed_limits,
            0,
            || Ok((CommandId(201), OrderId(201))),
        )
        .unwrap();
    let refreshed_token = refreshed_place.reservation.clone();
    let result = refreshed_gateway
        .submit_place(
            &refreshed_prepared,
            refreshed_token,
            1,
            &mut refreshed_limits,
            0,
            &shard.handle(),
        )
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!((result.command_id, result.seq), (CommandId(201), 14));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match refreshed_gateway.disconnect(&[shard.handle()], Duration::from_secs(5)) {
            Err(matching_runtime::GatewayError::Ingress(matching_runtime::ShardError::Busy))
                if std::time::Instant::now() < deadline =>
            {
                std::thread::yield_now()
            }
            result => {
                result.unwrap();
                break;
            }
        }
    }
    let refreshed_cancel = loop {
        match refreshed_gateway.dispatch_cancel(market, &shard.handle(), Duration::from_secs(5)) {
            Err(matching_runtime::SessionDispatchError::Ingress(
                matching_runtime::ShardError::Busy,
            )) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            result => break result.unwrap(),
        }
    };
    assert_eq!(refreshed_cancel.canceled_orders, 1);
    let mut future_journal = matching_runtime::SessionControlJournal::create(
        base.join("future-route-session.log"),
        UserId(7),
        matching_domain::OriginSessionId(203),
        204,
        vec![matching_runtime::SessionTarget {
            market,
            route_epoch: 3,
            command_id: CommandId(203),
        }],
        1,
    )
    .unwrap();
    future_journal.disconnect().unwrap();
    assert!(matches!(
        matching_runtime::dispatch_session_cancel(
            &mut future_journal,
            market,
            &shard.handle(),
            Duration::from_secs(5)
        ),
        Err(matching_runtime::SessionDispatchError::Route)
    ));
    assert_eq!(future_journal.pending().unwrap().len(), 1);
    let future_open = matching_runtime::SessionControlJournal::create(
        base.join("future-open-session.log"),
        UserId(7),
        matching_domain::OriginSessionId(205),
        206,
        vec![matching_runtime::SessionTarget {
            market,
            route_epoch: 3,
            command_id: CommandId(205),
        }],
        1,
    )
    .unwrap();
    let mut future_gateway =
        matching_runtime::GatewaySession::open(&Auth, 7, b"secret", future_open, 8, 32).unwrap();
    let mut future_limits =
        matching_protocol::GatewayRateLimiters::new(rate, rate, rate, 8).unwrap();
    let (_, future_prepared) = future_gateway
        .prepare_wire_durable(
            &refreshed_wire,
            1024,
            &mut refreshed_intents,
            &mut future_limits,
            0,
            || panic!("已有身份不得重新分配"),
        )
        .unwrap();
    assert!(matches!(
        future_gateway.submit_place(
            &future_prepared,
            refreshed_place.reservation.clone(),
            1,
            &mut future_limits,
            0,
            &shard.handle()
        ),
        Err(matching_runtime::GatewayError::Route)
    ));
    assert!(matches!(
        future_gateway.disconnect(&[shard.handle()], Duration::from_secs(5)),
        Err(matching_runtime::GatewayError::Route)
    ));
    let image = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let digest = image.state_digest(1024 * 1024).unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    drop(route_coordinator);
    let trusted = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(1),
        shard: shard_id,
        primary_epoch: 1,
        routes: updated_routes,
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: Some(config),
        expect_registry: true,
        max_markets: 1,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(base.join("manifest")).unwrap();
    let replay = matching_runtime::recover_snapshot_records_with_genesis(
        base.join("wal"),
        base.join("snapshots"),
        &manager,
        &trusted,
        &[(market, QueuePriorityPolicy::try_new(100).unwrap())],
    )
    .unwrap()
    .unwrap()
    .replay()
    .unwrap();
    assert_eq!(replay.executions[3].events, canceled.events);
    assert_eq!(replay.executions[12].events, refreshed_result.events);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
}
