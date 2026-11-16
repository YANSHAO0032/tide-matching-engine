//! 网关认证和风险授权前命令身份的测试。
mod t22_support;
use matching_domain::{
    CommandId, LimitOrder, MarketId, OrderId, OriginSessionId, Price, Qty, Side, StpKey, StpMode,
    UserId,
};
use matching_runtime::{
    GatewayAuthenticator, GatewayError, GatewaySession, PlaceIntent, SessionControlJournal,
    SessionTarget,
};

struct Auth;
fn limiter() -> matching_protocol::GatewayRateLimiters {
    let config = matching_protocol::BucketConfig {
        capacity: 50,
        refill_interval: 1,
    };
    matching_protocol::GatewayRateLimiters::new(config, config, config, 10).unwrap()
}
impl GatewayAuthenticator for Auth {
    fn authenticate(&self, key: u64, secret: &[u8]) -> Option<matching_runtime::GatewayPrincipal> {
        (key == 7 && secret == b"secret").then_some(matching_runtime::GatewayPrincipal {
            user: UserId(7),
            stp_key: StpKey::new(0),
        })
    }
}
fn intent() -> PlaceIntent {
    PlaceIntent {
        market: MarketId(1),
        order: LimitOrder {
            order_id: OrderId(5),
            user_id: UserId(7),
            stp_key: StpKey::new(0),
            stp_mode: StpMode::None,
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(2).unwrap(),
        },
        client_order_id: Some("order-5".into()),
        user_control_epoch: 3,
        post_only: false,
    }
}
fn journal(path: &std::path::Path) -> SessionControlJournal {
    SessionControlJournal::create(
        path,
        UserId(7),
        OriginSessionId(9),
        88,
        vec![SessionTarget {
            market: MarketId(1),
            route_epoch: 2,
            command_id: CommandId(100),
        }],
        2,
    )
    .unwrap()
}

#[test]
fn authentication_and_idempotency_are_checked_before_risk_authorization() {
    let dir = t22_support::TestDirectory::new();
    let session = journal(&dir.0.join("bad.log"));
    assert!(matches!(
        GatewaySession::open(&Auth, 7, b"wrong", session, 2, 32),
        Err(GatewayError::Unauthorized)
    ));
    let session = journal(&dir.0.join("good.log"));
    let mut gateway = GatewaySession::open(&Auth, 7, b"secret", session, 2, 32).unwrap();
    let first = gateway.prepare(55, CommandId(10), intent()).unwrap();
    assert_eq!(first.command_id(), CommandId(10));
    let retry = gateway.prepare(55, CommandId(11), intent()).unwrap();
    assert_eq!(retry.command_id(), CommandId(10));
    let mut changed = intent();
    changed.order.qty = Qty::try_new(3).unwrap();
    assert!(matches!(
        gateway.prepare(55, CommandId(12), changed),
        Err(GatewayError::Conflict)
    ));
    let mut other = intent();
    other.order.user_id = UserId(8);
    assert!(matches!(
        gateway.prepare(56, CommandId(13), other),
        Err(GatewayError::Unauthorized)
    ));
    let mut forged_group = intent();
    forged_group.order.stp_key = StpKey::new(99);
    assert!(matches!(
        gateway.prepare(57, CommandId(14), forged_group),
        Err(GatewayError::Unauthorized)
    ));
    assert!(matches!(
        gateway.disconnect(&[], std::time::Duration::from_secs(1)),
        Err(GatewayError::Route)
    ));
    assert!(matches!(
        gateway.prepare(55, CommandId(10), intent()),
        Err(GatewayError::Closed)
    ));
}

#[test]
fn wire_request_is_decoded_and_bound_to_authenticated_user() {
    use matching_protocol::{PlaceRequest, PlaceSide, StpPolicy};
    let dir = t22_support::TestDirectory::new();
    let mut gateway =
        GatewaySession::open(&Auth, 7, b"secret", journal(&dir.0.join("wire.log")), 2, 32).unwrap();
    let wire = PlaceRequest {
        key: 9,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let bytes = matching_protocol::encode_place_request(123, &wire, 1024, 32).unwrap();
    let (request_id, prepared) = gateway
        .prepare_wire(&bytes, 1024, CommandId(50), OrderId(60))
        .unwrap();
    assert_eq!(request_id, 123);
    assert_eq!(prepared.command_id(), CommandId(50));
    assert_eq!(prepared.intent().order.user_id, UserId(7));
    assert_eq!(prepared.intent().order.order_id, OrderId(60));
    assert_eq!(
        gateway
            .prepare_wire(&bytes, 1024, CommandId(51), OrderId(61))
            .unwrap()
            .1
            .command_id(),
        CommandId(50)
    );
}

#[test]
fn retry_does_not_call_identity_allocator_again() {
    use matching_protocol::{PlaceRequest, PlaceSide, StpPolicy};
    let dir = t22_support::TestDirectory::new();
    let mut gateway = GatewaySession::open(
        &Auth,
        7,
        b"secret",
        journal(&dir.0.join("retry.log")),
        2,
        32,
    )
    .unwrap();
    let request = PlaceRequest {
        key: 9,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let bytes = matching_protocol::encode_place_request(1, &request, 1024, 32).unwrap();
    let first = gateway
        .prepare_wire_with(&bytes, 1024, || Ok((CommandId(50), OrderId(60))))
        .unwrap();
    let retry = gateway
        .prepare_wire_with(&bytes, 1024, || panic!("retry allocated identity"))
        .unwrap();
    assert_eq!(first.1.command_id(), retry.1.command_id());
    assert_eq!(
        first.1.intent().order.order_id,
        retry.1.intent().order.order_id
    );
}

#[test]
fn durable_intent_reuses_ids_across_sessions_and_rejects_conflict() {
    use matching_protocol::{PlaceRequest, PlaceSide, StpPolicy};
    use matching_runtime::GatewayIntentStore;
    let dir = t22_support::TestDirectory::new();
    let mut limiter = limiter();
    let path = dir.0.join("intents.log");
    let request = PlaceRequest {
        key: 9,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let bytes = matching_protocol::encode_place_request(1, &request, 1024, 32).unwrap();
    let mut gateway = GatewaySession::open(
        &Auth,
        7,
        b"secret",
        journal(&dir.0.join("first.log")),
        2,
        32,
    )
    .unwrap();
    let mut store = GatewayIntentStore::create(&path, 10, 1024).unwrap();
    let first = gateway
        .prepare_wire_durable(&bytes, 1024, &mut store, &mut limiter, 0, || {
            Ok((CommandId(50), OrderId(60)))
        })
        .unwrap()
        .1;
    drop(store);
    drop(gateway);
    let mut store = GatewayIntentStore::open(&path, 10, 1024).unwrap();
    let next_session = SessionControlJournal::create(
        dir.0.join("second.log"),
        UserId(7),
        OriginSessionId(10),
        89,
        vec![SessionTarget {
            market: MarketId(1),
            route_epoch: 2,
            command_id: CommandId(101),
        }],
        2,
    )
    .unwrap();
    let mut gateway = GatewaySession::open(&Auth, 7, b"secret", next_session, 2, 32).unwrap();
    let second = gateway
        .prepare_wire_durable(&bytes, 1024, &mut store, &mut limiter, 0, || {
            panic!("retry must not allocate")
        })
        .unwrap()
        .1;
    assert_eq!(first.command_id(), second.command_id());
    assert_eq!(
        first.intent().order.order_id,
        second.intent().order.order_id
    );
    let mut altered = request;
    altered.qty = 3;
    let conflict = matching_protocol::encode_place_request(2, &altered, 1024, 32).unwrap();
    assert!(matches!(
        gateway.prepare_wire_durable(&conflict, 1024, &mut store, &mut limiter, 0, || panic!(
            "conflict allocated"
        )),
        Err(GatewayError::Conflict)
    ));
}

#[test]
fn durable_intent_log_is_exclusive_and_rejects_torn_records() {
    use matching_protocol::{PlaceRequest, PlaceSide, StpPolicy};
    use matching_runtime::GatewayIntentStore;
    let dir = t22_support::TestDirectory::new();
    let mut limiter = limiter();
    let path = dir.0.join("intents-truncated.log");
    let mut store = GatewayIntentStore::create(&path, 5, 1024).unwrap();
    assert!(GatewayIntentStore::open(&path, 5, 1024).is_err());
    let mut gateway = GatewaySession::open(
        &Auth,
        7,
        b"secret",
        journal(&dir.0.join("session.log")),
        2,
        32,
    )
    .unwrap();
    let request = PlaceRequest {
        key: 1,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let bytes = matching_protocol::encode_place_request(1, &request, 1024, 32).unwrap();
    gateway
        .prepare_wire_durable(&bytes, 1024, &mut store, &mut limiter, 0, || {
            Ok((CommandId(10), OrderId(20)))
        })
        .unwrap();
    let first_len = std::fs::metadata(&path).unwrap().len() as usize;
    let mut next = request.clone();
    next.key = 2;
    let second = matching_protocol::encode_place_request(2, &next, 1024, 32).unwrap();
    gateway
        .prepare_wire_durable(&second, 1024, &mut store, &mut limiter, 0, || {
            Ok((CommandId(11), OrderId(21)))
        })
        .unwrap();
    drop(store);
    let log = std::fs::read(&path).unwrap();
    let mut uncommitted = log.clone();
    uncommitted.extend_from_slice(&[1, 2, 3]);
    std::fs::write(&path, &uncommitted).unwrap();
    drop(GatewayIntentStore::open(&path, 5, 1024).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), log);
    for cut in 1..=8 {
        std::fs::write(&path, &log[..log.len() - cut]).unwrap();
        assert!(
            GatewayIntentStore::open(&path, 5, 1024).is_err(),
            "cut={cut}"
        );
    }
    std::fs::write(&path, &log).unwrap();
    drop(GatewayIntentStore::open(&path, 5, 1024).unwrap());
    std::fs::write(&path, &log[..first_len]).unwrap();
    assert!(GatewayIntentStore::open(&path, 5, 1024).is_err());
    // 丢失完整且已确认的最后一条记录也必须识别，不能当作空日志。
    std::fs::write(&path, &log[..4]).unwrap();
    assert!(GatewayIntentStore::open(&path, 5, 1024).is_err());
}

#[test]
fn rate_limit_precedes_persistent_identity_allocation() {
    use matching_protocol::{
        BucketConfig, GatewayRateLimiters, PlaceRequest, PlaceSide, RateError, StpPolicy,
    };
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("limited.log");
    let mut store = matching_runtime::GatewayIntentStore::create(&path, 10, 1024).unwrap();
    let mut gateway = GatewaySession::open(
        &Auth,
        7,
        b"secret",
        journal(&dir.0.join("limited-session.log")),
        3,
        32,
    )
    .unwrap();
    let config = BucketConfig {
        capacity: 1,
        refill_interval: 100,
    };
    let mut limiter = GatewayRateLimiters::new(config, config, config, 3).unwrap();
    let mut request = PlaceRequest {
        key: 1,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let first = matching_protocol::encode_place_request(1, &request, 1024, 32).unwrap();
    gateway
        .prepare_wire_durable(&first, 1024, &mut store, &mut limiter, 0, || {
            Ok((CommandId(1), OrderId(1)))
        })
        .unwrap();
    let size = std::fs::metadata(&path).unwrap().len();
    request.key = 2;
    let second = matching_protocol::encode_place_request(2, &request, 1024, 32).unwrap();
    assert!(matches!(
        gateway.prepare_wire_durable(&second, 1024, &mut store, &mut limiter, 0, || panic!(
            "limited request allocated"
        )),
        Err(GatewayError::Rate(RateError::Limited))
    ));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
}

#[test]
fn overload_maps_to_stable_busy_reason_without_masking_other_faults() {
    use matching_domain::RejectReason;
    use matching_protocol::RateError;
    assert_eq!(
        GatewayError::Rate(RateError::Limited).ingress_reason(),
        Some(RejectReason::EngineBusy)
    );
    assert_eq!(
        GatewayError::Capacity.ingress_reason(),
        Some(RejectReason::EngineBusy)
    );
    assert_eq!(
        GatewayError::Ingress(matching_runtime::ShardError::Busy).ingress_reason(),
        Some(RejectReason::EngineBusy)
    );
    assert_eq!(
        GatewayError::Rate(RateError::ClockRegression).ingress_reason(),
        None
    );
    assert_eq!(
        GatewayError::Route.ingress_reason(),
        Some(RejectReason::StaleRouteEpoch)
    );
    assert_eq!(
        GatewayError::Reservation.ingress_reason(),
        Some(RejectReason::InvalidReservation)
    );
    assert_eq!(
        matching_protocol::ReasonCode::ENGINE_BUSY.get(),
        RejectReason::EngineBusy as u16
    );
}

#[test]
fn persisted_risk_token_is_reused_after_reopen() {
    use matching_domain::{AssetId, AssetReserve, ReservationId, ReservationToken};
    use matching_protocol::{PlaceRequest, PlaceSide, StpPolicy};
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("auth-intents.log");
    let mut store = matching_runtime::GatewayIntentStore::create(&path, 4, 1024).unwrap();
    let mut gateway = GatewaySession::open(
        &Auth,
        7,
        b"secret",
        journal(&dir.0.join("auth-session.log")),
        4,
        32,
    )
    .unwrap();
    let request = PlaceRequest {
        key: 5,
        market: 1,
        price: 10,
        qty: 2,
        stp_key: 0,
        user_control_epoch: 0,
        side: PlaceSide::Buy,
        stp: StpPolicy::None,
        post_only: false,
        client_order_id: None,
    };
    let bytes = matching_protocol::encode_place_request(1, &request, 1024, 32).unwrap();
    let mut limits = limiter();
    let prepared = gateway
        .prepare_wire_durable(&bytes, 1024, &mut store, &mut limits, 0, || {
            Ok((CommandId(10), OrderId(20)))
        })
        .unwrap()
        .1;
    let token = ReservationToken {
        token_id: 30,
        command_id: CommandId(10),
        reservation_id: ReservationId(40),
        order_id: OrderId(20),
        user_id: UserId(7),
        market_id: MarketId(1),
        reserves: vec![AssetReserve {
            asset: AssetId(2),
            atoms: 20,
        }]
        .into(),
        fee_schedule_version: 1,
        fee_tier_id: 1,
        risk_key_id: 1,
        signature: [7; 64],
    };
    let intent_len = std::fs::metadata(&path).unwrap().len() as usize;
    assert_eq!(
        store.authorize(&prepared, || Ok(token.clone())).unwrap(),
        token
    );
    let client_seq = store.client_seq(CommandId(10)).unwrap();
    drop(store);
    let full_log = std::fs::read(&path).unwrap();
    std::fs::write(&path, &full_log[..intent_len]).unwrap();
    assert!(matching_runtime::GatewayIntentStore::open(&path, 4, 1024).is_err());
    std::fs::write(&path, &full_log).unwrap();
    let mut store = matching_runtime::GatewayIntentStore::open(&path, 4, 1024).unwrap();
    assert_eq!(store.client_seq(CommandId(10)), Some(client_seq));
    assert_eq!(
        store
            .authorize(&prepared, || panic!("reissued authorization"))
            .unwrap(),
        token
    );
}
