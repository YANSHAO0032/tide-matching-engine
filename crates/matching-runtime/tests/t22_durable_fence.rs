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
    let recovered =
        matching_runtime::recover_local_commands(base.join("wal"), base.join("manifest"), 0, 2, 1)
            .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].shard_seq, first.seq);
    assert_eq!(recovered[0].command, command);
    let snapshots = base.join("snapshot");
    std::fs::create_dir(&snapshots).unwrap();
    let trusted = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(2),
        shard: ShardId(1),
        primary_epoch: 7,
        routes: matching_runtime::RouteTable::new([]).unwrap(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: None,
        expect_registry: true,
        max_markets: 0,
        max_bytes: 1024 * 1024,
    };
    let mut competing = Shard::start(
        ShardId(1),
        ClusterId(2),
        matching_runtime::RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let path = base.join("wal").join("00000000000000000001.wal");
    let mut tail = scan.records[0].clone();
    tail.shard_seq = 2;
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&tail.encode().unwrap())
        .unwrap();
    let original = std::fs::read(&path).unwrap();
    assert!(
        Shard::recover_group_local(
            base.join("wal"),
            base.join("manifest"),
            &snapshots,
            &trusted,
            (8, 10_000)
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    competing.shutdown(Duration::from_secs(5)).unwrap();
    let mut restarted = Shard::recover_group_local(
        base.join("wal"),
        base.join("manifest"),
        snapshots,
        &trusted,
        (8, 10_000),
    )
    .unwrap();
    let mut next = command;
    next.command_id = CommandId(4);
    next.client_seq = 6;
    if let Command::ApplyUserControlFenceShard(fence) = &mut next.payload {
        fence.new_epoch = 10;
    }
    assert_eq!(
        retry(|| restarted.handle().submit_command(next.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    restarted.shutdown(Duration::from_secs(5)).unwrap();
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

#[test]
fn recovered_wal_decodes_command_and_rejects_mismatched_identity() {
    use matching_domain::{
        ApplyUserControlFenceShard, Command, CommandEnvelope, CommandId, UserFenceAction, UserId,
        encode_wal_command,
    };
    use matching_persistence::{
        FileManifestStorage, ManifestIntent, ManifestManager, SegmentWriter, WalRecord,
    };
    use matching_runtime::recover_local_commands;
    let temp = t22_support::TestDirectory::new();
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
    let wal = temp.0.join("wal");
    let manifest = temp.0.join("manifest");
    let record = WalRecord {
        cluster_id: 2,
        shard_id: 1,
        flags: 0,
        epoch: 7,
        shard_seq: 1,
        engine_time: 0,
        command_id: 3,
        payload: encode_wal_command(&command).unwrap(),
    };
    let mut writer = SegmentWriter::new(&wal, 1024).unwrap();
    writer.append(&record).unwrap();
    drop(writer);
    let mut manager = ManifestManager::<FileManifestStorage>::open(&manifest).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 1,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    drop(manager);
    assert_eq!(
        recover_local_commands(&wal, &manifest, 0, 2, 1).unwrap()[0].command,
        command
    );
    let mut bad = record;
    bad.command_id = 999;
    std::fs::write(wal.join("00000000000000000001.wal"), bad.encode().unwrap()).unwrap();
    assert!(recover_local_commands(&wal, &manifest, 0, 2, 1).is_err());
}

#[test]
fn snapshot_barrier_flushes_group_and_captures_fence_and_dedup() {
    let temp = t22_support::TestDirectory::new();
    let mut shard = Shard::start_group_fence_only(
        ShardId(9),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (8, 500_000),
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
    let result = retry(|| shard.handle().submit_command(command.clone()));
    let snapshot = retry(|| shard.handle().take_snapshot(91));
    assert_eq!(
        result
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    let image = snapshot
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(image.snapshot_seq(), 1);
    assert!(image.contains_command(&command).unwrap());
    assert!(image.check_user_epoch(UserId(6), 8).is_err());
    let mut config = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(2),
        shard: ShardId(9),
        primary_epoch: 7,
        routes: image.routes().clone(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: None,
        expect_registry: true,
        max_markets: 0,
        max_bytes: 1024 * 1024,
    };
    let bytes = image.encode_snapshot(config.max_bytes).unwrap();
    let restored = matching_runtime::SnapshotImage::decode_snapshot(&bytes, &config).unwrap();
    assert_eq!(
        image.canonical_state(config.max_bytes).unwrap(),
        restored.canonical_state(config.max_bytes).unwrap()
    );
    assert!(restored.contains_command(&command).unwrap());
    assert!(restored.check_user_epoch(UserId(6), 8).is_err());
    let mut collision = command.clone();
    collision.command_id = CommandId(4);
    assert!(restored.contains_command(&collision).is_err());
    config.primary_epoch = 8;
    assert!(matching_runtime::SnapshotImage::decode_snapshot(&bytes, &config).is_err());
    let mut seed = 817_u64;
    for step in 0..128_u64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        if (seed >> 32) & 1 == 0 {
            let captured = retry(|| shard.handle().take_snapshot(u128::from(step)))
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert_eq!(captured.snapshot_seq(), 1);
        } else {
            let result = retry(|| {
                shard
                    .handle()
                    .reservation(matching_domain::ReservationId(99))
            })
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
            assert!(result.is_none());
        }
    }
    assert_eq!(
        matching_persistence::scan_segments(temp.0.join("wal"))
            .unwrap()
            .records
            .len(),
        1
    );
    assert_eq!(
        retry(|| shard.handle().barrier(92))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (92, 1)
    );
    let directory = temp.0.join("snapshot");
    std::fs::create_dir(&directory).unwrap();
    let mut writer =
        matching_runtime::SnapshotWriter::start(shard.handle(), directory.clone(), 1024 * 1024)
            .unwrap();
    assert!(
        matching_runtime::SnapshotWriter::start(shard.handle(), directory.clone(), 1024 * 1024)
            .is_err()
    );
    assert!(
        writer
            .submit(53, restored)
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(!directory.join("snapshot-53.bin").exists());
    writer
        .submit(51, image)
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let duplicate = retry(|| shard.handle().take_snapshot(93))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(
        writer
            .submit(52, duplicate)
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert_eq!(
        retry(|| shard.handle().barrier(94))
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        (94, 1)
    );
    writer.shutdown().unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    config.primary_epoch = 7;
    let unverified = matching_persistence::ManifestManager::<
        matching_persistence::FileManifestStorage,
    >::open(temp.0.join("manifest"))
    .unwrap();
    assert!(
        matching_runtime::SnapshotImage::read_committed_snapshot(
            temp.0.join("snapshot"),
            &unverified,
            &config
        )
        .is_err()
    );
    drop(unverified);
    let mut manager = matching_persistence::ManifestManager::<
        matching_persistence::FileManifestStorage,
    >::open_recovery(temp.0.join("manifest"))
    .unwrap();
    let (_, seq, payload) = matching_persistence::read_committed_snapshot(directory, &manager)
        .unwrap()
        .unwrap();
    assert_eq!(manager.state().latest_snapshot, Some((51, 1)));
    assert_eq!(seq, 1);
    config.primary_epoch = 7;
    let restored = matching_runtime::SnapshotImage::read_committed_snapshot(
        temp.0.join("snapshot"),
        &manager,
        &config,
    )
    .unwrap()
    .unwrap();
    assert!(restored.contains_command(&command).unwrap());
    assert_eq!(restored.snapshot_seq(), seq);
    assert!(!payload.is_empty());
    let canonical = restored.canonical_state(config.max_bytes).unwrap();
    let mut recovered = Shard::start_in_memory_from_snapshot(restored, &config).unwrap();
    let retry_result = retry(|| recovered.handle().submit_command(command.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(retry_result.deduplicated);
    assert_eq!(retry_result.seq, 1);
    let mut next = command.clone();
    next.command_id = CommandId(4);
    next.client_seq = 6;
    if let Command::ApplyUserControlFenceShard(ref mut fence) = next.payload {
        fence.new_epoch = 10;
    }
    let next_result = retry(|| recovered.handle().submit_command(next.clone()))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(next_result.seq, 2);
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    let file = temp.0.join("snapshot").join("snapshot-51.bin");
    let mut damaged = std::fs::read(&file).unwrap();
    let offset = 30 + 2 + 4 + env!("CARGO_PKG_VERSION").len() + 16;
    damaged[offset..offset + 8].copy_from_slice(&2_u64.to_be_bytes());
    let end = damaged.len() - 4;
    let mut altered_state = canonical.clone();
    altered_state[6..14].copy_from_slice(&2_u64.to_be_bytes());
    use sha2::{Digest, Sha256};
    damaged[end - 32..end].copy_from_slice(&Sha256::digest(&altered_state));
    let crc = matching_persistence::crc32c(&damaged[..end]);
    damaged[end..].copy_from_slice(&crc.to_be_bytes());
    std::fs::write(&file, damaged).unwrap();
    assert!(
        matching_runtime::SnapshotImage::read_committed_snapshot(
            temp.0.join("snapshot"),
            &manager,
            &config
        )
        .is_err()
    );
    let mut forged = payload.clone();
    let offset = 2 + 4 + env!("CARGO_PKG_VERSION").len() + 16;
    forged[offset..offset + 8].copy_from_slice(&999_u64.to_be_bytes());
    let mut altered_state = canonical;
    altered_state[6..14].copy_from_slice(&999_u64.to_be_bytes());
    let end = forged.len();
    forged[end - 32..].copy_from_slice(&Sha256::digest(&altered_state));
    matching_persistence::write_snapshot_file(temp.0.join("snapshot"), 999, 999, &forged).unwrap();
    manager
        .submit(matching_persistence::ManifestIntent::SnapshotCommitted {
            snapshot_id: 999,
            snapshot_seq: 999,
        })
        .unwrap();
    assert!(
        matching_runtime::SnapshotImage::read_committed_snapshot(
            temp.0.join("snapshot"),
            &manager,
            &config
        )
        .is_err()
    );
}

#[test]
fn damaged_latest_snapshot_selects_prior_candidate_without_rolling_back_watermark() {
    let temp = t22_support::TestDirectory::new();
    let directory = temp.0.join("snapshot");
    std::fs::create_dir(&directory).unwrap();
    let mut shard = Shard::start_group_fence_only(
        ShardId(19),
        ClusterId(2),
        ReservationRegistry::new([]).unwrap(),
        LogicalTime::new(0),
        (IdempotencyConfig::default(), 7, 8),
        (temp.0.join("wal"), temp.0.join("manifest")),
        (8, 500_000),
    )
    .unwrap();
    let command = |id: u128, epoch: u64| CommandEnvelope {
        command_id: CommandId(id),
        session_id: 1,
        client_seq: id as u64,
        market_id: None,
        user_id: Some(UserId(6)),
        route_epoch: None,
        client_timestamp: None,
        received_at_unix_ns: None,
        payload: Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(id),
            user_id: UserId(6),
            new_epoch: epoch,
            action: UserFenceAction::FenceOnly,
            expected_primary_epoch: 7,
        }),
    };
    let mut writer =
        matching_runtime::SnapshotWriter::start(shard.handle(), directory.clone(), 1024 * 1024)
            .unwrap();
    let mut stale = None;
    let mut live_events = Vec::new();
    let mut live_digest = None;
    for (id, epoch) in [(1, 9), (2, 10)] {
        let cmd = command(id, epoch);
        let execution = retry(|| shard.handle().submit_command(cmd.clone()))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(execution.seq, id as u64);
        if id == 2 {
            live_events = execution.events;
        }
        let image = retry(|| shard.handle().take_snapshot(id))
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        if id == 2 {
            live_digest = Some(image.state_digest(1024 * 1024).unwrap());
        }
        if id == 1 {
            stale = Some(
                retry(|| shard.handle().take_snapshot(100))
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
            );
        }
        writer
            .submit(id as u64, image)
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    writer.shutdown().unwrap();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let config = matching_runtime::SnapshotRestoreConfig {
        cluster: ClusterId(2),
        shard: ShardId(19),
        primary_epoch: 7,
        routes: matching_runtime::RouteTable::new([]).unwrap(),
        user_capacity: 8,
        idempotency: IdempotencyConfig::default(),
        place: None,
        expect_registry: true,
        max_markets: 0,
        max_bytes: 1024 * 1024,
    };
    assert!(Shard::start_in_memory_from_snapshot(stale.unwrap(), &config).is_err());
    let file = directory.join("snapshot-2.bin");
    let mut bytes = std::fs::read(&file).unwrap();
    bytes[35] ^= 1;
    std::fs::write(&file, bytes).unwrap();
    let manager=matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let candidate =
        matching_runtime::SnapshotImage::read_recovery_candidate(&directory, &manager, &config)
            .unwrap()
            .unwrap();
    assert!(candidate.used_previous);
    assert_eq!(candidate.committed_through, 2);
    assert_eq!(candidate.image.snapshot_seq(), 1);
    let wal_file = temp.0.join("wal").join("00000000000000000001.wal");
    let wal_bytes = std::fs::read(&wal_file).unwrap();
    let mut corrupted = matching_persistence::scan_segments(temp.0.join("wal"))
        .unwrap()
        .records;
    corrupted[1].epoch = 8;
    let bad: Vec<u8> = corrupted
        .iter()
        .flat_map(|record| record.encode().unwrap())
        .collect();
    std::fs::write(&wal_file, bad).unwrap();
    let bad_batch = matching_runtime::recover_snapshot_records(
        temp.0.join("wal"),
        &directory,
        &manager,
        &config,
    )
    .unwrap()
    .unwrap();
    assert!(bad_batch.replay().is_err());
    drop(manager);
    let before_failed_restart = std::fs::read(&wal_file).unwrap();
    assert!(
        Shard::recover_group_local(
            temp.0.join("wal"),
            temp.0.join("manifest"),
            &directory,
            &config,
            (8, 500_000),
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&wal_file).unwrap(), before_failed_restart);
    std::fs::write(&wal_file, wal_bytes).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    let batch = matching_runtime::recover_snapshot_records(
        temp.0.join("wal"),
        &directory,
        &manager,
        &config,
    )
    .unwrap()
    .unwrap();
    assert!(batch.snapshot().used_previous);
    assert_eq!(batch.commands().len(), 1);
    assert_eq!(batch.commands()[0].shard_seq, 2);
    assert_eq!(batch.commands()[0].command, command(2, 10));
    let mut live_hash = matching_runtime::EventHasher::new();
    for event in &live_events {
        live_hash.update(event).unwrap();
    }
    let replay = batch.replay().unwrap();
    assert_eq!(replay.executions[0].events, live_events);
    assert_eq!(replay.event_hash, live_hash.finish());
    assert_eq!(
        replay.image.state_digest(1024 * 1024).unwrap(),
        live_digest.unwrap()
    );
    assert!(candidate.image.check_user_epoch(UserId(6), 8).is_err());
    assert!(candidate.image.check_user_epoch(UserId(6), 9).is_ok());
    assert!(Shard::start_in_memory_from_snapshot(candidate.image, &config).is_err());
    drop(manager);
    let mut resumed = Shard::recover_group_local(
        temp.0.join("wal"),
        temp.0.join("manifest"),
        &directory,
        &config,
        (8, 500_000),
    )
    .unwrap();
    let resumed_execution = retry(|| resumed.handle().submit_command(command(3, 11)))
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(resumed_execution.seq, 3);
    resumed.shutdown(Duration::from_secs(5)).unwrap();
    let manager = matching_persistence::ManifestManager::<matching_persistence::FileManifestStorage>::open_recovery(temp.0.join("manifest")).unwrap();
    assert_eq!(manager.state().local_durable_seq, 3);
    std::fs::write(directory.join("snapshot-1.bin"), b"broken").unwrap();
    assert!(
        matching_runtime::SnapshotImage::read_recovery_candidate(&directory, &manager, &config)
            .is_err()
    );
}
