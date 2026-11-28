use matching_control::{EtcdClient, EtcdControlStore};
use matching_domain::{ClusterId, CommandId, RejectReason, ShardId, UserId};
use matching_runtime::cross_control::{
    ControlAck, ControlAdmission, ControlAuthority, ControlKind, ControlPhase, ControlUpdate,
};
use std::time::Duration;

fn client() -> EtcdClient {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap()
}

/// 在专用测试前缀中删除一个真实键，确认服务端确实执行了故障注入。
fn delete_key(key: &str) {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let result = ureq::post(&format!(
        "{}/v3/kv/deleterange",
        endpoints.split(',').next().unwrap()
    ))
    .set("Content-Type", "application/json")
    .send_string(&serde_json::json!({"key":STANDARD.encode(key)}).to_string())
    .unwrap()
    .into_string()
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result).unwrap()["deleted"],
        "1"
    );
}

/// 租约失效前已持久确认的分片不可回滚，继任者保留各子命令的身份和结果。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn takeover_after_partial_ack_retains_fence_and_completes() {
    let client = client();
    let prefix = format!("/tide-test/control-{}", std::process::id());
    let cluster = ClusterId(11);
    let first = std::sync::Arc::new(
        EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap(),
    );
    assert!(
        matching_runtime::cross_control::CrossShardCoordinator::new(
            first.clone(),
            ClusterId(12),
            &[ShardId(3)],
            Duration::from_secs(1)
        )
        .is_err()
    );
    let observer = EtcdControlStore::open(client.clone(), &prefix, cluster).unwrap();
    assert!(EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).is_err());
    let user = UserId(13);
    let parent = CommandId(14);
    let kind = ControlKind::UserMassCancel(user);
    let targets = [ShardId(3), ShardId(4)];
    let mut record = first.prepare(parent, kind, &targets).unwrap();
    assert!(observer.prepare(parent, kind, &targets).is_err());
    assert_eq!(observer.check(user).unwrap().2, None);
    record = first.advance(&record, ControlUpdate::Next).unwrap();
    assert_eq!(record.epoch, 1);
    assert_eq!(
        observer.check(user).unwrap().2,
        Some(RejectReason::ControlFenceActive)
    );
    assert_eq!(observer.check(UserId(44)).unwrap().2, None);
    assert!(
        first
            .prepare(parent, ControlKind::GlobalMassCancel, &targets)
            .is_err()
    );
    assert!(first.prepare(CommandId(25), kind, &targets[..1]).is_err());
    let competing = first.prepare(CommandId(15), kind, &targets).unwrap();
    assert!(first.advance(&competing, ControlUpdate::Next).is_err());
    first.advance(&competing, ControlUpdate::Abort).unwrap();
    record = first.advance(&record, ControlUpdate::Next).unwrap();
    record = first.advance(&record, ControlUpdate::Next).unwrap();
    let ack = ControlAck {
        shard_seq: 42,
        canceled: 2,
    };
    record = first
        .advance(&record, ControlUpdate::Ack(targets[0], ack))
        .unwrap();
    first.revoke().unwrap();
    let next = EtcdControlStore::acquire(client, &prefix, cluster, 30).unwrap();
    assert!(next.owner_epoch() > first.owner_epoch());
    assert!(first.advance(&record, ControlUpdate::Next).is_err());
    assert_eq!(next.pending().unwrap(), vec![parent]);
    assert_eq!(next.prepare(parent, kind, &targets).unwrap(), record);
    assert!(next.advance(&record, ControlUpdate::Abort).is_err());
    assert!(next.advance(&record, ControlUpdate::Next).is_err());
    assert_eq!(
        observer.check(user).unwrap().2,
        Some(RejectReason::ControlFenceActive)
    );
    record = next
        .advance(&record, ControlUpdate::Ack(targets[1], ack))
        .unwrap();
    record = next.advance(&record, ControlUpdate::Next).unwrap();
    assert_eq!(
        observer.check(user).unwrap().2,
        Some(RejectReason::ControlFenceActive)
    );
    record = next.advance(&record, ControlUpdate::Next).unwrap();
    assert_eq!(observer.check(user).unwrap().2, None);
    assert_eq!(observer.current_epochs(user).unwrap(), (1, 0));
    record = next.advance(&record, ControlUpdate::Next).unwrap();
    assert_eq!(record.phase, ControlPhase::Done);
    assert!(next.pending().unwrap().is_empty());
    assert_eq!(record.acks.len(), 2);
    next.revoke().unwrap();
}

/// 导入预先提取的历史下限后接任，并在授权派发前解除临时围栏。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn imported_floor_persists_and_unsent_fanout_can_abort() {
    let client = client();
    let prefix = format!("/tide-test/import-{}", std::process::id());
    let user = UserId(42);
    let cluster = ClusterId(12);
    let first = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 20).unwrap();
    let observer = EtcdControlStore::open(client.clone(), &prefix, cluster).unwrap();
    assert!(observer.import_user_epoch(user, 123).is_err());
    first.import_user_epoch(user, 123).unwrap();
    first.import_user_epoch(user, 123).unwrap();
    assert!(first.import_user_epoch(user, 122).is_err());
    assert!(first.import_user_epoch(user, 124).is_err());
    first.revoke().unwrap();
    let next = EtcdControlStore::acquire(client, &prefix, cluster, 20).unwrap();
    let mut op = next
        .prepare(
            CommandId(9),
            ControlKind::UserMassCancel(user),
            &[ShardId(1)],
        )
        .unwrap();
    op = next.advance(&op, ControlUpdate::Next).unwrap();
    assert_eq!(op.epoch, 124);
    assert!(next.import_user_epoch(user, 123).is_err());
    op = next.advance(&op, ControlUpdate::Next).unwrap();
    assert_eq!(op.phase, ControlPhase::Fanout);
    let aborted = next.advance(&op, ControlUpdate::Abort).unwrap();
    assert_eq!(next.load(op.parent).unwrap(), aborted);
    assert!(next.pending().unwrap().is_empty());
    assert_eq!(observer.check(user).unwrap(), (0, 123, None));
    assert_eq!(observer.current_epochs(user).unwrap(), (123, 0));
    next.import_user_epoch(UserId(99), u64::MAX).unwrap();
    let high = next
        .prepare(
            CommandId(10),
            ControlKind::UserMassCancel(UserId(99)),
            &[ShardId(1)],
        )
        .unwrap();
    assert!(next.advance(&high, ControlUpdate::Next).is_err());
    assert_eq!(next.load(high.parent).unwrap(), high);
    assert_eq!(observer.current_epochs(UserId(99)).unwrap(), (u64::MAX, 0));
    next.revoke().unwrap();
}

/// 两个线程登记同一父命令时得到同一记录。全局开关跨纪元发布并在清除后继续放行。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn concurrent_parent_registration_and_global_kill_release() {
    let client = client();
    let prefix = format!("/tide-test/kill-{}", std::process::id());
    let cluster = ClusterId(13);
    let owner = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
    let reader = EtcdControlStore::open(client, &prefix, cluster).unwrap();
    let members = [ShardId(2), ShardId(1)];
    let user = UserId(81);
    for (parent, active) in [(CommandId(100), true), (CommandId(101), false)] {
        let kind = ControlKind::GlobalKill {
            active,
            cancel_all: active,
        };
        let gate = std::sync::Barrier::new(2);
        let (a, b) = std::thread::scope(|scope| {
            let submit = || {
                gate.wait();
                owner.prepare(parent, kind, &members)
            };
            let joined = scope.spawn(submit);
            (submit().unwrap(), joined.join().unwrap().unwrap())
        });
        assert_eq!(a, b);
        let mut op = owner.advance(&a, ControlUpdate::Next).unwrap();
        assert_eq!(
            reader.check(user).unwrap().2,
            Some(RejectReason::ControlFenceActive)
        );
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
        for member in members {
            op = owner
                .advance(
                    &op,
                    ControlUpdate::Ack(
                        member,
                        ControlAck {
                            shard_seq: op.epoch,
                            canceled: 0,
                        },
                    ),
                )
                .unwrap();
        }
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
        let epoch = if active { 1 } else { 2 };
        assert_eq!(
            reader.check(user).unwrap(),
            (epoch, 0, active.then_some(RejectReason::KillSwitchActive))
        );
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
        assert_eq!(op.phase, ControlPhase::Done);
    }
    owner.revoke().unwrap();
}

/// 子进程登记预设的首条回执后立即退出，继任者在租约到期后仅凭 etcd 续办。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn coordinator_process_exit_keeps_partial_operation_until_takeover() {
    const SHARDS: [ShardId; 2] = [ShardId(1), ShardId(2)];
    const PARENT: CommandId = CommandId(55);
    const USER: UserId = UserId(34);
    const CLUSTER: ClusterId = ClusterId(15);
    const KIND: ControlKind = ControlKind::UserMassCancel(USER);
    const ACK: ControlAck = ControlAck {
        shard_seq: 83,
        canceled: 9,
    };
    const PREFIX_VAR: &str = "TIDE_T36_EXIT_AFTER_ACK";
    if let Ok(prefix) = std::env::var(PREFIX_VAR) {
        let store = EtcdControlStore::acquire(client(), &prefix, CLUSTER, 3).unwrap();
        let mut record = store.prepare(PARENT, KIND, &SHARDS).unwrap();
        while record.phase != ControlPhase::AwaitingAcks {
            record = store.advance(&record, ControlUpdate::Next).unwrap();
        }
        store
            .advance(&record, ControlUpdate::Ack(SHARDS[0], ACK))
            .unwrap();
        std::process::exit(36);
    }
    let prefix = format!("/tide-test/process-control-{}", std::process::id());
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "coordinator_process_exit_keeps_partial_operation_until_takeover",
            "--ignored",
        ])
        .env(PREFIX_VAR, &prefix);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(36),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reader = EtcdControlStore::open(client(), &prefix, CLUSTER).unwrap();
    let client = client();
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    let owner = loop {
        assert_eq!(
            reader.check(USER).unwrap().2,
            Some(RejectReason::ControlFenceActive)
        );
        match EtcdControlStore::acquire(client.clone(), &prefix, CLUSTER, 30) {
            Ok(owner) => break owner,
            Err(e)
                if e.kind() == std::io::ErrorKind::AlreadyExists
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("继任者无法取得失效租约：{e}"),
        }
    };
    assert_eq!(owner.pending().unwrap(), vec![PARENT]);
    let partial = owner.load(PARENT).unwrap();
    assert_eq!(partial.phase, ControlPhase::AwaitingAcks);
    assert_eq!(partial.acks.len(), 1);
    assert_eq!(partial.acks[&SHARDS[0]], ACK);
    assert_eq!(partial.targets[&SHARDS[1]], KIND.derive(PARENT, SHARDS[1]));
    assert!(owner.advance(&partial, ControlUpdate::Abort).is_err());
    let mut op = owner
        .advance(
            &partial,
            ControlUpdate::Ack(
                SHARDS[1],
                ControlAck {
                    shard_seq: 2,
                    canceled: 1,
                },
            ),
        )
        .unwrap();
    while op.phase != ControlPhase::Done {
        op = owner.advance(&op, ControlUpdate::Next).unwrap();
    }
    assert_eq!(op.total_canceled().unwrap(), 10);
    assert_eq!(reader.check(USER).unwrap(), (0, 1, None));
    owner.revoke().unwrap();
}

/// 删除持久索引模拟恢复资料缺失。接任必须保留原范围和操作，拒绝重建空索引。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn takeover_rejects_a_missing_index_in_initialized_namespace() {
    let prefix = format!("/tide-test/partial-index-{}", std::process::id());
    let cluster = ClusterId(16);
    let parent = CommandId(81);
    let user = UserId(67);
    let client = client();
    let owner = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
    let record = owner
        .prepare(parent, ControlKind::UserMassCancel(user), &[ShardId(1)])
        .unwrap();
    let record = owner.advance(&record, ControlUpdate::Next).unwrap();
    let ns = format!("{prefix}/cluster/{:04x}/cross-control", cluster.get());
    let op_key = format!("{ns}/op/{:032x}", record.parent.get());
    let original = client.get(op_key.as_bytes()).unwrap().unwrap();
    let index = format!("{ns}/pending");
    owner.revoke().unwrap();
    delete_key(&index);
    assert!(EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).is_err());
    assert!(client.get(index.as_bytes()).unwrap().is_none());
    assert_eq!(client.get(op_key.as_bytes()).unwrap(), Some(original));
    assert_eq!(
        owner.check(user).unwrap().2,
        Some(RejectReason::ControlFenceActive)
    );
}

/// 待办已清空时删除成员表，继任者仍能辨认部署已绑定拓扑，不能登记缩小的目标集。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn missing_members_cannot_rebind_a_smaller_topology() {
    let client = client();
    let prefix = format!("/tide-test/lost-members-{}", std::process::id());
    let cluster = ClusterId(17);
    let owner = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
    let kind = ControlKind::GlobalMassCancel;
    let first = owner
        .prepare(CommandId(1), kind, &[ShardId(1), ShardId(2)])
        .unwrap();
    let first = owner.advance(&first, ControlUpdate::Abort).unwrap();
    let members = format!("{prefix}/cluster/0011/cross-control/members");
    owner.revoke().unwrap();
    delete_key(&members);
    let next = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
    assert!(next.prepare(CommandId(2), kind, &[ShardId(1)]).is_err());
    assert!(client.get(members.as_bytes()).unwrap().is_none());
    assert!(next.pending().unwrap().is_empty());
    assert_eq!(next.load(first.parent).unwrap(), first);
    assert_eq!(
        next.load(CommandId(2)).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    next.revoke().unwrap();
}

/// 已有首次回执的用户栅栏、已发布的全局开关分别丢失范围键时，准入读者均须停止放行。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn missing_scope_after_ack_or_kill_cannot_reopen_admission() {
    let client = client();
    let user = UserId(42);
    let cluster = ClusterId(18);
    for global in [false, true] {
        let prefix = format!("/tide-test/lost-scope-{global}-{}", std::process::id());
        let owner = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
        let reader = EtcdControlStore::open(client.clone(), &prefix, cluster).unwrap();
        let (kind, suffix) = if global {
            (
                ControlKind::GlobalKill {
                    active: true,
                    cancel_all: false,
                },
                "global".to_owned(),
            )
        } else {
            (
                ControlKind::UserMassCancel(user),
                format!("user/{:016x}", user.get()),
            )
        };
        let mut record = owner
            .prepare(CommandId(1), kind, &[ShardId(1), ShardId(2)])
            .unwrap();
        while record.phase != ControlPhase::AwaitingAcks {
            record = owner.advance(&record, ControlUpdate::Next).unwrap();
        }
        let ack = ControlAck {
            shard_seq: 1,
            canceled: 0,
        };
        record = owner
            .advance(&record, ControlUpdate::Ack(ShardId(1), ack))
            .unwrap();
        if global {
            record = owner
                .advance(&record, ControlUpdate::Ack(ShardId(2), ack))
                .unwrap();
            while record.phase != ControlPhase::Done {
                record = owner.advance(&record, ControlUpdate::Next).unwrap();
            }
        }
        assert!(reader.check(user).unwrap().2.is_some());
        let key = format!("{prefix}/cluster/0012/cross-control/{suffix}");
        delete_key(&key);
        assert!(reader.check(user).is_err());
        assert!(reader.current_epochs(user).is_err());
        owner.revoke().unwrap();
        let next = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
        let reader = EtcdControlStore::open(client.clone(), &prefix, cluster).unwrap();
        assert!(reader.check(user).is_err());
        let new_record = next
            .prepare(CommandId(2), kind, &[ShardId(1), ShardId(2)])
            .unwrap();
        assert!(next.advance(&new_record, ControlUpdate::Next).is_err());
        if !global {
            assert!(next.import_user_epoch(user, 9).is_err());
        }
        assert!(client.get(key.as_bytes()).unwrap().is_none());
        assert_eq!(next.load(record.parent).unwrap(), record);
        next.revoke().unwrap();
    }
}

/// 已导入范围丢失见证时拒绝读取和补写，缺少见证契约的旧格式也不能用于准入或接任。
#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn imported_scope_needs_its_witness_and_supported_namespace_format() {
    let client = client();
    let user = UserId(42);
    let cluster = ClusterId(19);
    let prefix = format!("/tide-test/scope-format-{}", std::process::id());
    let namespace = format!("{prefix}/cluster/0013/cross-control");
    let store = EtcdControlStore::acquire(client.clone(), &prefix, cluster, 30).unwrap();
    let reader = EtcdControlStore::open(client.clone(), &prefix, cluster).unwrap();
    store.import_user_epoch(user, 8).unwrap();
    assert_eq!(reader.current_epochs(user).unwrap(), (8, 0));
    let key = format!("{namespace}/user/{:016x}", user.get());
    let original = client.get(key.as_bytes()).unwrap().unwrap();
    let witness = format!("{key}/witness");
    assert_eq!(
        client.get(witness.as_bytes()).unwrap(),
        Some(original.clone())
    );
    delete_key(&witness);
    assert!(reader.check(user).is_err());
    assert!(store.import_user_epoch(user, 8).is_err());
    assert_eq!(client.get(key.as_bytes()).unwrap(), Some(original));
    assert_eq!(reader.check(UserId(99)).unwrap(), (0, 0, None));
    store.revoke().unwrap();
    let marker = format!("{namespace}/initialized");
    let (revision, _) = client.get(marker.as_bytes()).unwrap().unwrap();
    assert!(
        client
            .compare_and_swap(
                marker.as_bytes(),
                matching_control::KvVersion::Revision(revision),
                b"CCS1"
            )
            .unwrap()
            .is_some()
    );
    assert!(reader.check(UserId(99)).is_err());
    assert!(EtcdControlStore::open(client.clone(), &prefix, cluster).is_err());
    assert!(EtcdControlStore::acquire(client, &prefix, cluster, 30).is_err());
}
