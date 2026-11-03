//! 记录分片实际序号并验证进程内确定性重放。
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    ApplyUserControlFenceShard, AssetId, AssetReserve, ClusterId, Command, CommandEnvelope,
    CommandId, LimitOrder, LogicalTime, MarketId, MarketMath, OrderId, PlaceLimitOrder, Price, Qty,
    ReservationId, ReservationToken, ShardId, Side, StpKey, StpMode, UserFenceAction, UserId,
};
use matching_orderbook::{OrderNode, ProductionOrderBook, QueuePriorityPolicy};
use matching_risk::{LimitReservePolicy, ReservationRegistry, ReservationState, RiskKeySet};
use matching_runtime::{
    FormalExecution, IdempotencyConfig, MarketRoute, PlaceLimits, PlaceSupportConfig, RouteState,
    RouteTable, Shard, ShardError,
};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const MARKET: MarketId = MarketId(226);
const SHARD: ShardId = ShardId(226);
/// 仅对尚未入队的繁忙结果进行有期限重试。
fn retry<T>(mut f: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match f() {
            Ok(v) => return v,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("入口失败: {e:?}"),
        }
    }
}
/// 按指定集群身份启动空状态测试分片。
fn start(cluster: ClusterId) -> Shard {
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
        [(MARKET, policy)],
        PlaceLimits {
            active_orders: 16,
            terminal_history: 16,
            client_id_bytes: 32,
        },
    )
    .unwrap();
    let routes = RouteTable::new([MarketRoute {
        market_id: MARKET,
        shard_id: SHARD,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    book.bind_market(MARKET).unwrap();
    Shard::start_with_place_support(
        SHARD,
        cluster,
        routes,
        vec![(MARKET, book)],
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 1, 16, config),
    )
    .unwrap()
}
/// 构造确定的签名限价单与用户栅栏命令序列。
fn tape() -> Vec<CommandEnvelope> {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let mut commands = Vec::new();
    for id in 1u128..=6 {
        let user = UserId(id as u64);
        let mut token = ReservationToken {
            token_id: id,
            command_id: CommandId(id),
            reservation_id: ReservationId(id),
            order_id: OrderId(id),
            user_id: user,
            market_id: MARKET,
            reserves: vec![AssetReserve {
                asset: AssetId(2),
                atoms: (10 + id) * 2,
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
        commands.push(CommandEnvelope {
            command_id: CommandId(id),
            session_id: id as u64,
            client_seq: 1,
            market_id: Some(MARKET),
            user_id: Some(user),
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order: LimitOrder {
                    order_id: OrderId(id),
                    user_id: user,
                    stp_key: StpKey::new(0),
                    stp_mode: StpMode::None,
                    side: Side::Buy,
                    price: Price::try_new(10 + id as i64).unwrap(),
                    qty: Qty::try_new(2).unwrap(),
                },
                client_order_id: Some(format!("client-{id}")),
                user_control_epoch: 0,
                post_only: false,
                reservation: token,
            })),
        });
    }
    for id in 7u128..=9 {
        let user = UserId(100 + id as u64);
        commands.push(CommandEnvelope {
            command_id: CommandId(id),
            session_id: id as u64,
            client_seq: 1,
            market_id: None,
            user_id: Some(user),
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: CommandId(id + 100),
                user_id: user,
                new_epoch: id as u64,
                action: UserFenceAction::FenceOnly,
                expected_primary_epoch: 1,
            }),
        });
    }
    commands
}
type Snapshot = Vec<(Option<OrderNode>, Option<(ReservationState, bool)>)>;
/// 读取全部测试订单及其预留状态。
fn snapshot(shard: &Shard) -> Snapshot {
    (1u128..=6)
        .map(|id| {
            let order = retry(|| shard.handle().order(MARKET, OrderId(id)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            let reservation = retry(|| shard.handle().reservation(ReservationId(id)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            assert!(order.is_some());
            assert!(reservation.is_some());
            (order, reservation)
        })
        .collect()
}
/// 提交一条完整命令并等待分片结果。
fn run(shard: &Shard, command: &CommandEnvelope) -> FormalExecution {
    retry(|| shard.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
}
/// 逐项比较两次执行的业务结果及有序事件。
fn compare(a: &FormalExecution, b: &FormalExecution) {
    assert_eq!(a.summary.status, matching_domain::CommandStatus::Succeeded);
    assert_eq!(a.seq, b.seq);
    assert_eq!(a.command_id, b.command_id);
    assert_eq!(a.outcome, b.outcome);
    assert_eq!(a.summary, b.summary);
    assert_eq!(a.events, b.events);
    assert_eq!(a.event_range, b.event_range);
    assert!(!a.deduplicated && !b.deduplicated);
}

#[test]
fn concurrent_sequence_record_replays_exact_events_and_state() {
    let mut first = start(ClusterId(9));
    let commands = tape();
    let gate = Arc::new(Barrier::new(commands.len() + 1));
    let mut threads = Vec::new();
    for command in commands {
        let h = first.handle();
        let gate = Arc::clone(&gate);
        threads.push(std::thread::spawn(move || {
            gate.wait();
            let result = retry(|| h.submit_command(command.clone()))
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap();
            (command, result)
        }));
    }
    gate.wait();
    let mut record: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    // seq 由单写者分配，代表真实业务线性化顺序；响应到达顺序不作为重放输入。
    record.sort_by_key(|(_, result)| result.seq);
    for (index, (_, result)) in record.iter().enumerate() {
        assert_eq!(result.seq, index as u64 + 1);
    }
    let before = snapshot(&first);
    first.shutdown(Duration::from_secs(5)).unwrap();
    let mut second = start(ClusterId(9));
    for (command, expected) in &record {
        let actual = run(&second, command);
        compare(expected, &actual);
    }
    assert_eq!(snapshot(&second), before);
    second.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn fixed_tape_repeats_exactly() {
    let commands = tape();
    let mut first = start(ClusterId(10));
    let expected: Vec<_> = commands.iter().map(|c| run(&first, c)).collect();
    let before = snapshot(&first);
    first.shutdown(Duration::from_secs(5)).unwrap();
    let mut second = start(ClusterId(10));
    for (command, result) in commands.iter().zip(&expected) {
        compare(result, &run(&second, command));
    }
    assert_eq!(snapshot(&second), before);
    second.shutdown(Duration::from_secs(5)).unwrap();
}
