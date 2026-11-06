//! 验证真实持久命令带的快照后缀与原始执行一致。
mod t22_support;

use matching_domain::{
    ApplyUserControlFenceShard, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime,
    MarketId, OrderId, ShardId, UserFenceAction, UserId,
};
use matching_risk::ReservationRegistry;
use matching_runtime::{
    EventHasher, IdempotencyConfig, RouteTable, Shard, ShardError, SnapshotRestoreConfig,
    SnapshotWriter,
};
use std::time::{Duration, Instant};

/// 只重试尚未入队的请求。
fn retry<T>(mut submit: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match submit() {
            Ok(value) => return value,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("入口失败: {error:?}"),
        }
    }
}

/// 在持久化快照前后执行固定输入，并在穿插查询和快照时比较后缀。
fn tape(count: u64, insert_reads: bool) -> ([u8; 32], [u8; 32]) {
    let temp = t22_support::TestDirectory::new();
    let snapshot_dir = temp.0.join("snapshot");
    std::fs::create_dir(&snapshot_dir).unwrap();
    let config = IdempotencyConfig {
        command_capacity: 32,
        session_capacity: 4,
        aliases_per_session: 16,
    };
    let mut shard = Shard::start_group_fence_only(
        ShardId(19),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (config, 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (32, 10_000),
    )
    .unwrap();
    let mut writer =
        SnapshotWriter::start(shard.handle(), snapshot_dir.clone(), 1024 * 1024).unwrap();
    let initial = retry(|| shard.handle().take_snapshot(1))
        .recv_timeout(Duration::from_secs(30))
        .unwrap()
        .unwrap();
    writer
        .submit(1, initial)
        .unwrap()
        .recv_timeout(Duration::from_secs(30))
        .unwrap()
        .unwrap();
    let mut hash = EventHasher::new();
    let mut rng = 0x8b8b_1234_9876_4321_u64;
    let mut next = 1;
    while next <= count {
        let end = (next + 31).min(count);
        let mut pending = Vec::new();
        for id in next..=end {
            let command = CommandEnvelope {
                command_id: CommandId(u128::from(id)),
                session_id: 1,
                client_seq: id,
                market_id: None,
                user_id: Some(UserId(6)),
                route_epoch: None,
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                    parent_command_id: CommandId(u128::from(id)),
                    user_id: UserId(6),
                    new_epoch: id,
                    action: UserFenceAction::FenceOnly,
                    expected_primary_epoch: 7,
                }),
            };
            pending.push((id, retry(|| shard.handle().submit_command(command.clone()))));
        }
        for (id, response) in pending {
            let execution = response
                .recv_timeout(Duration::from_secs(30))
                .unwrap()
                .unwrap();
            assert_eq!(execution.seq, id);
            for event in &execution.events {
                hash.update(event).unwrap();
            }
        }
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        if insert_reads && rng & 31 == 0 {
            assert!(
                retry(|| shard.handle().order(MarketId(1), OrderId(1)))
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap()
                    .is_err()
            );
        }
        if insert_reads && rng & 127 == 0 {
            let image = retry(|| shard.handle().take_snapshot(u128::from(end)))
                .recv_timeout(Duration::from_secs(30))
                .unwrap()
                .unwrap();
            assert_eq!(image.snapshot_seq(), end);
        }
        next = end + 1;
    }
    let digest = retry(|| shard.handle().take_snapshot(2))
        .recv_timeout(Duration::from_secs(30))
        .unwrap()
        .unwrap()
        .state_digest(1024 * 1024)
        .unwrap();
    writer.shutdown().unwrap();
    shard.shutdown(Duration::from_secs(30)).unwrap();
    let restore = SnapshotRestoreConfig {
        cluster: ClusterId(2),
        shard: ShardId(19),
        primary_epoch: 7,
        routes: RouteTable::new([]).unwrap(),
        user_capacity: 8,
        idempotency: config,
        place: None,
        expect_registry: true,
        max_markets: 0,
        max_bytes: 1024 * 1024,
    };
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let batch = matching_runtime::recover_snapshot_records(
        temp.0.join("wal"),
        snapshot_dir,
        &manager,
        &restore,
    )
    .unwrap()
    .unwrap();
    assert_eq!(batch.commands().len() as u64, count);
    let replay = batch.replay().unwrap();
    let live_hash = hash.finish();
    assert_eq!(replay.event_hash, live_hash);
    assert_eq!(replay.image.state_digest(1024 * 1024).unwrap(), digest);
    (digest, live_hash)
}

#[test]
fn replay_1000_and_10000_commands() {
    assert_eq!(tape(1_000, false), tape(1_000, true));
    assert_eq!(tape(10_000, false), tape(10_000, true));
}

#[test]
#[ignore = "百万条持久命令验收需要显式运行"]
fn replay_1000000_commands() {
    assert_eq!(tape(1_000_000, false), tape(1_000_000, true));
}
