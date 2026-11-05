//! 验证真实文件同步完成后才确认用户栅栏。
mod t22_support;
use matching_domain::{
    ApplyUserControlFenceShard, ClusterId, Command, CommandEnvelope, CommandId, LogicalTime,
    ShardId, UserFenceAction, UserId,
};
use matching_risk::ReservationRegistry;
use matching_runtime::{IdempotencyConfig, Shard};
use std::time::Duration;
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
fn fence_is_durable_before_reply_and_dedup_does_not_reappend() {
    let temp = t22_support::TestDirectory::new();
    let base = &temp.0;
    let mut shard = Shard::start_durable_fence_only(
        ShardId(1),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        base.join("wal"),
        base.join("manifest"),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 4,
        client_seq: 5,
        market_id: None,
        user_id: Some(UserId(6)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(8),
            user_id: UserId(6),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let mut stale = command.clone();
    if let Command::ApplyUserControlFenceShard(fence) = &mut stale.payload {
        fence.expected_primary_epoch = 6;
    }
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
    let first = retry(|| shard.handle().submit_command(command.clone().clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(first.seq, 1);
    let scan = matching_persistence::scan_segments(base.join("wal")).unwrap();
    assert_eq!(scan.records.len(), 1);
    assert_eq!(
        matching_domain::decode_wal_command(&scan.records[0].payload).unwrap(),
        command
    );
    assert!(
        retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        matching_persistence::scan_segments(base.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn append_error_stops_shard_without_reply_or_followup_acceptance() {
    let temp = t22_support::TestDirectory::new();
    let base = &temp.0;
    let shard = Shard::start_durable_fence_only(
        ShardId(2),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        base.join("wal"),
        base.join("manifest"),
    )
    .unwrap();
    std::fs::write(
        base.join("wal").join("00000000000000000001.wal"),
        b"collision",
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 4,
        client_seq: 5,
        market_id: None,
        user_id: Some(UserId(6)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(8),
            user_id: UserId(6),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let reply = retry(|| shard.handle().submit_command(command.clone()));
    assert!(reply.recv_timeout(Duration::from_secs(5)).is_err());
    match shard.handle().barrier(0) {
        Err(_) => {}
        Ok(reply) => assert!(reply.recv_timeout(Duration::from_secs(5)).is_err()),
    }
    drop(shard);
}

#[test]
fn durable_fence_rejects_unrecoverable_nonzero_initial_time() {
    let temp = t22_support::TestDirectory::new();
    let base = &temp.0;
    let result = Shard::start_durable_fence_only(
        ShardId(3),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(1),
        (IdempotencyConfig::default(), 7, 8),
        base.join("wal"),
        base.join("manifest"),
    );
    assert!(result.is_err());
}

#[test]
fn durable_fence_rejects_nonempty_reservation_baseline() {
    use matching_domain::{
        AssetId, AssetReserve, MarketId, OrderId, OriginSessionId, ReservationId,
    };
    use matching_risk::{ReservationState, ReservationStatus};
    let temp = t22_support::TestDirectory::new();
    let registry = ReservationRegistry::new([ReservationState {
        reservation_id: ReservationId(1),
        order_id: OrderId(1),
        user_id: UserId(1),
        market_id: MarketId(1),
        origin_session_id: OriginSessionId(1),
        version: 0,
        reserves: vec![AssetReserve {
            asset: AssetId(1),
            atoms: 1,
        }],
        status: ReservationStatus::Active,
        fee_schedule_version: 1,
        fee_tier_id: 1,
    }])
    .unwrap();
    assert!(
        Shard::start_durable_fence_only(
            ShardId(3),
            ClusterId(2),
            registry,
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 7, 8),
            temp.0.join("wal"),
            temp.0.join("manifest")
        )
        .is_err()
    );
}

#[test]
fn grouped_fences_apply_in_order_and_aliases_do_not_consume_wal() {
    let temp = t22_support::TestDirectory::new();
    let mut shard = Shard::start_group_fence_only(
        ShardId(5),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (3, 50_000),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(3),
        session_id: 4,
        client_seq: 5,
        market_id: None,
        user_id: Some(UserId(6)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(8),
            user_id: UserId(6),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let one = retry(|| shard.handle().submit_command(command.clone()));
    let two = retry(|| shard.handle().submit_command(command.clone()));
    let mut newer = command.clone();
    newer.command_id = CommandId(4);
    newer.client_seq = 6;
    if let Command::ApplyUserControlFenceShard(fence) = &mut newer.payload {
        fence.new_epoch = 11;
    }
    let three = retry(|| shard.handle().submit_command(newer.clone()));
    assert_eq!(
        one.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert!(
        two.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        three
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    assert_eq!(
        matching_persistence::scan_segments(temp.0.join("wal"))
            .unwrap()
            .records
            .len(),
        2
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn group_waits_then_barrier_flushes_before_reply() {
    let temp = t22_support::TestDirectory::new();
    let mut shard = Shard::start_group_fence_only(
        ShardId(6),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (8, 500_000),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(2)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(3),
            user_id: UserId(2),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let reply = retry(|| shard.handle().submit_command(command.clone()));
    assert!(matches!(
        reply.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    let barrier = retry(|| shard.handle().barrier(9));
    assert_eq!(
        reply
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        barrier.recv_timeout(Duration::from_secs(5)).unwrap(),
        (9, 1)
    );
    assert_eq!(
        matching_persistence::scan_segments(temp.0.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn group_timeout_flushes_without_further_messages() {
    let temp = t22_support::TestDirectory::new();
    let mut shard = Shard::start_group_fence_only(
        ShardId(7),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (8, 20_000),
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(2)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(3),
            user_id: UserId(2),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    assert_eq!(
        retry(|| shard.handle().submit_command(command.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        matching_persistence::scan_segments(temp.0.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn group_wal_failure_closes_all_waiters_and_shard() {
    let temp = t22_support::TestDirectory::new();
    let shard = Shard::start_group_fence_only(
        ShardId(8),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (8, 100_000),
    )
    .unwrap();
    std::fs::write(
        temp.0.join("wal").join("00000000000000000001.wal"),
        b"collision",
    )
    .unwrap();
    let command = CommandEnvelope {
        command_id: CommandId(1),
        session_id: 1,
        client_seq: 1,
        market_id: None,
        user_id: Some(UserId(2)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(3),
            user_id: UserId(2),
            new_epoch: 9,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let one = retry(|| shard.handle().submit_command(command.clone()));
    assert!(one.recv_timeout(Duration::from_secs(5)).is_err());
    match shard.handle().barrier(9) {
        Err(_) => {}
        Ok(reply) => assert!(reply.recv_timeout(Duration::from_secs(5)).is_err()),
    }
}

#[test]
fn group_rejects_zero_and_excessive_batch_configuration() {
    for batch in [0, 1025] {
        let temp = t22_support::TestDirectory::new();
        assert!(
            Shard::start_group_fence_only(
                ShardId(9),
                ClusterId(2),
                ReservationRegistry::new([]).unwrap(),
                LogicalTime::new(0),
                (IdempotencyConfig::default(), 7, 8),
                (temp.0.join("wal"), temp.0.join("manifest")),
                (batch, 20_000)
            )
            .is_err()
        );
    }
}

#[test]
fn durable_start_rejects_aliasing_wal_and_manifest_before_creating_files() {
    let temp = t22_support::TestDirectory::new();
    let shared = temp.0.join("wal");
    let alias = shared.join(".");
    assert!(
        Shard::start_durable_fence_only(
            ShardId(50),
            ClusterId(2),
            ReservationRegistry::new([]).unwrap(),
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 7, 8),
            &shared,
            &alias
        )
        .is_err()
    );
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
    assert!(
        Shard::start_group_fence_only(
            ShardId(50),
            ClusterId(2),
            ReservationRegistry::new([]).unwrap(),
            LogicalTime::new(0),
            (IdempotencyConfig::default(), 7, 8),
            (&shared, &alias),
            (3, 100)
        )
        .is_err()
    );
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
}

#[cfg(windows)]
#[test]
fn durable_start_rejects_windows_junction_alias() {
    let temp = t22_support::TestDirectory::new();
    let shared = temp.0.join("wal");
    let junction = temp.0.join("wal-junction");
    let status = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(&junction)
        .arg(&shared)
        .status()
        .unwrap();
    assert!(status.success());
    let result = Shard::start_durable_fence_only(
        ShardId(51),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        &shared,
        &junction,
    );
    assert!(result.is_err());
    assert!(std::fs::read_dir(&shared).unwrap().next().is_none());
    std::fs::remove_dir(junction).unwrap();
}
