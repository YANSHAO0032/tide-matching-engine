//! 在权威读取与入队之间精确插入分片控制，验证两类旧纪元与未来纪元均不能穿过切面。
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{Receiver, SyncSender, sync_channel},
};

/// 在权威读取、分片控制提交和迟到请求入队之间建立可重复的先后顺序。
struct PausedReader {
    /// 下次读取在取得快照后阻塞，便于在其入队前提交控制。
    armed: AtomicBool,
    /// 当前全局发布纪元。
    global: AtomicU64,
    /// 测试用户的已发布纪元。
    user: AtomicU64,
    /// 将读到的快照通知控制线程。
    observed: SyncSender<(u64, u64)>,
    /// 控制线程提交后允许旧读取返回。
    release: Mutex<Receiver<()>>,
}
const RACE_CLUSTER: ClusterId = ClusterId(2036);
impl ControlAdmission for PausedReader {
    fn namespace(&self) -> String {
        "admission-cut".to_owned()
    }
    fn cluster_id(&self) -> ClusterId {
        RACE_CLUSTER
    }
    fn check(&self, _: UserId) -> std::io::Result<(u64, u64, Option<RejectReason>)> {
        let global = self.global.load(Ordering::SeqCst);
        let user = self.user.load(Ordering::SeqCst);
        if self.armed.swap(false, Ordering::SeqCst) {
            self.observed.send((global, user)).unwrap();
            self.release.lock().unwrap().recv_timeout(WAIT).unwrap();
        }
        Ok((global, user, None))
    }
}

#[test]
fn raced_user_and_global_reads_cannot_submit_past_the_shard_fence() {
    let (observed, observed_rx) = sync_channel(1);
    let (release, release_rx) = sync_channel(1);
    let reader = Arc::new(PausedReader {
        armed: false.into(),
        global: 0.into(),
        user: 0.into(),
        observed,
        release: Mutex::new(release_rx),
    });
    let dir = crate::test_directory::TestDirectory::new();
    let lease = Arc::new(
        ControlStore::new(dir.0.join("lease"), RACE_CLUSTER, SHARD)
            .acquire_primary()
            .unwrap(),
    );
    let owner = lease.epoch();
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
        RiskKeySet::new(1, [(1, SigningKey::from_bytes(&[7; 32]).verifying_key())]).unwrap(),
        [(MARKET, policy)],
        PlaceLimits {
            active_orders: 8,
            terminal_history: 8,
            client_id_bytes: 16,
        },
    )
    .unwrap()
    .with_primary_lease(lease)
    .with_control_admission(reader.clone());
    let routes = RouteTable::new([MarketRoute {
        market_id: MARKET,
        shard_id: SHARD,
        route_epoch: 1,
        state: RouteState::Active,
    }])
    .unwrap();
    let mut shard = Shard::start_group_place_support(
        SHARD,
        RACE_CLUSTER,
        routes,
        vec![(MARKET, QueuePriorityPolicy::try_new(100).unwrap())],
        (ReservationRegistry::new([]).unwrap(), LogicalTime::new(0)),
        (IdempotencyConfig::default(), owner, 8, config),
        ((dir.0.join("wal"), dir.0.join("manifest")), (4, 1000)),
    )
    .unwrap();
    let handle = shard.handle();
    let user = UserId(1);
    assert_eq!(
        run(&shard, &place(70, user, 42)).unwrap_err(),
        FormalError::Rejected(RejectReason::StaleUserControlEpoch)
    );
    for (kind, parent, admitted_user, seq, expected) in [
        (
            ControlKind::UserMassCancel(user),
            80,
            0,
            1,
            RejectReason::StaleUserControlEpoch,
        ),
        (
            ControlKind::GlobalMassCancel,
            81,
            1,
            2,
            RejectReason::ControlFenceActive,
        ),
    ] {
        let fence = control(kind, parent, 1, owner);
        assert!(matches!(
            handle.submit_command(fence.clone()),
            Err(ShardError::InvalidConfig)
        ));
        assert!(matches!(
            handle.submit_control_command("different", fence.clone()),
            Err(ShardError::InvalidConfig)
        ));
        reader.armed.store(true, Ordering::SeqCst);
        std::thread::scope(|scope| {
            let send = scope.spawn(|| {
                handle
                    .submit_command(place(parent + 100, user, admitted_user))
                    .unwrap()
                    .recv_timeout(WAIT)
                    .unwrap()
            });
            assert_eq!(observed_rx.recv_timeout(WAIT).unwrap(), (0, admitted_user));
            let ack = handle
                .submit_control_command("admission-cut", fence)
                .unwrap()
                .recv_timeout(WAIT)
                .unwrap()
                .unwrap();
            assert_eq!(ack.seq, seq);
            if kind == ControlKind::GlobalMassCancel {
                reader.global.store(1, Ordering::SeqCst)
            } else {
                reader.user.store(1, Ordering::SeqCst)
            }
            release.send(()).unwrap();
            assert_eq!(
                send.join().unwrap().unwrap_err(),
                FormalError::Rejected(expected)
            );
        });
        assert_eq!(
            handle
                .barrier(seq.into())
                .unwrap()
                .recv_timeout(WAIT)
                .unwrap()
                .1,
            seq
        );
    }
    let accepted = run(&shard, &place(95, user, 1)).unwrap();
    assert_eq!(accepted.seq, 3);
    assert!(matches!(accepted.outcome, FormalOutcome::Placed(_)));
    shard.shutdown(WAIT).unwrap();
    assert_eq!(
        matching_persistence::scan_segments(dir.0.join("wal"))
            .unwrap()
            .records
            .len(),
        3
    );
}
