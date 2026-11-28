//! 使用并发的认证 TCP 连接提交被栅栏拦截或尚未开放的请求。
use matching_domain::{CommandId, MarketId, OriginSessionId, StpKey, UserId};
use matching_protocol::{BucketConfig, Frame, GatewayRateLimiters};
use matching_runtime::*;
use std::{
    io::{Read, Write},
    path::Path,
    sync::Barrier,
    time::Duration,
};
struct Auth;
impl GatewayAuthenticator for Auth {
    fn authenticate(&self, key: u64, _: &[u8]) -> Option<GatewayPrincipal> {
        Some(GatewayPrincipal {
            user: UserId(key),
            stp_key: StpKey::new(0),
        })
    }
}

/// 每行给出帧类型、认证用户与预期的稳定原因；任何命令身份分配或风险授权都令测试失败。
pub fn assert_rejected(
    dir: &Path,
    market: MarketId,
    shards: &[ShardHandle],
    cases: &[(u16, u64, u16)],
) {
    let health = Health::new(shards.to_vec()).unwrap();
    for dep in [
        HealthDependency::Recovery,
        HealthDependency::Persistence,
        HealthDependency::Control,
    ] {
        health.set_healthy(dep, true);
    }
    let bucket = BucketConfig {
        capacity: 32,
        refill_interval: 1,
    };
    let shared = GatewayShared::new(
        GatewayIntentStore::create(dir.join("intents"), 16, 1024).unwrap(),
        GatewayRateLimiters::new(bucket, bucket, bucket, 8).unwrap(),
        || 0,
    );
    let start = Barrier::new(cases.len());
    let limit = Duration::from_secs(10);
    std::thread::scope(|scope| {
        for (i, (kind, user, expected)) in cases.iter().copied().enumerate() {
            let request_id = u64::try_from(i + 1).unwrap();
            let journal = SessionControlJournal::create_with_policy(
                dir.join(format!("session-{i}")),
                UserId(user),
                OriginSessionId(request_id),
                request_id + 10,
                vec![SessionTarget {
                    market,
                    route_epoch: 1,
                    command_id: CommandId(u128::from(request_id)),
                }],
                1,
                false,
            )
            .unwrap();
            let mut session = GatewaySession::open(&Auth, user, b"", journal, 8, 32).unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let mut client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            client.set_read_timeout(Some(limit)).unwrap();
            let (shards, health, shared, start) = (shards, &health, &shared, &start);
            scope.spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket.set_read_timeout(Some(limit)).unwrap();
                GatewayTransport {
                    session: &mut session,
                    shared,
                    health,
                    shards,
                    max_frame: 1024,
                    timeout: limit,
                    allocate: || panic!("被拦截的请求分配了命令身份"),
                    authorize: |_: &PreparedPlace| panic!("被拦截的请求申请了资金授权"),
                }
                .serve(&mut socket)
                .unwrap();
            });
            scope.spawn(move || {
                let bytes = if kind == 1 {
                    matching_protocol::encode_place_request(
                        request_id,
                        &matching_protocol::PlaceRequest {
                            key: 33,
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
                    .unwrap()
                } else {
                    matching_protocol::encode_frame(
                        &Frame {
                            message_type: kind,
                            request_id,
                            flags: 0,
                            payload: vec![],
                        },
                        1024,
                    )
                    .unwrap()
                };
                start.wait();
                client.write_all(&bytes).unwrap();
                let mut reply = vec![0; matching_protocol::HEADER_LEN];
                client.read_exact(&mut reply).unwrap();
                let len = matching_protocol::frame_length(&reply, 1024).unwrap();
                reply.resize(len, 0);
                client
                    .read_exact(&mut reply[matching_protocol::HEADER_LEN..])
                    .unwrap();
                assert_eq!(
                    matching_protocol::decode_rejection(&reply, 1024).unwrap(),
                    (request_id, matching_protocol::ReasonCode::new(expected))
                );
            });
        }
    });
}
