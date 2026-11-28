//! 通过独立认证连接检查三种受保护请求在栅栏期间的前置拒绝。
mod t22_support;
#[path = "t36_support/tcp.rs"]
mod tcp;
use matching_domain::*;
use matching_orderbook::QueuePriorityPolicy;
use matching_risk::{ReservationRegistry, RiskKeySet};
use matching_runtime::{cross_control::ControlAdmission, *};
use std::{io, sync::Arc, time::Duration};

struct Fenced;
impl ControlAdmission for Fenced {
    fn cluster_id(&self) -> ClusterId {
        ClusterId(374)
    }
    fn namespace(&self) -> String {
        "测试栅栏".into()
    }
    fn check(&self, user: UserId) -> io::Result<(u64, u64, Option<RejectReason>)> {
        let reason = match user.0 {
            7 => Some(RejectReason::ControlFenceActive),
            8 => Some(RejectReason::KillSwitchActive),
            _ => None,
        };
        Ok((0, 0, reason))
    }
}
#[test]
fn simultaneous_authenticated_place_amend_and_replace_return_4006_without_wal() {
    let dir = t22_support::TestDirectory::new();
    let market = MarketId(399);
    let sid = ShardId(11);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: sid,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let policy = matching_risk::LimitReservePolicy::new(
        (AssetId(1), 0),
        (AssetId(2), 0),
        MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        1,
        1,
        0,
    )
    .unwrap();
    let config = PlaceSupportConfig::new(
        RiskKeySet::new(1, []).unwrap(),
        [(market, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 32,
        },
    )
    .unwrap()
    .with_control_admission(Arc::new(Fenced));
    let mut shard = Shard::start_with_durable_place_support(
        sid,
        ClusterId(374),
        routes,
        vec![(market, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), 1, 8, config),
        (dir.0.join("wal"), dir.0.join("manifest")),
    )
    .unwrap();
    let shards = [shard.handle()];
    let cases = [
        (1_u16, 7, 4006_u16),
        (4, 7, 4006),
        (5, 7, 4006),
        (4, 9, 2001),
        (5, 9, 2001),
        (1, 8, 1007),
    ];
    tcp::assert_rejected(&dir.0, market, &shards, &cases);
    let limit = Duration::from_secs(10);
    assert!(
        shards[0]
            .order(market, OrderId(9))
            .unwrap()
            .recv_timeout(limit)
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        shards[0].barrier(1).unwrap().recv_timeout(limit).unwrap(),
        (1, 0)
    );
    shard.shutdown(limit).unwrap();
    assert!(
        matching_persistence::scan_segments(dir.0.join("wal"))
            .unwrap()
            .records
            .is_empty()
    );
}
