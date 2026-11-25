//! 跨进程写者终止后的权威日志和纪元验证。
use matching_control::{EtcdClient, FencedLog};
use matching_domain::{ClusterId, ShardId};
use matching_persistence::WalRecord;
use std::time::{Duration, Instant};

fn log(prefix: &str) -> FencedLog {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    FencedLog::new(
        EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap(),
        prefix,
        ClusterId(191),
        ShardId(192),
    )
    .unwrap()
}
fn record(epoch: u64, seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 191,
        shard_id: 192,
        flags: 0,
        epoch,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![7],
    }
}

#[test]
#[ignore = "仅供跨进程测试启动"]
fn child_holds_committed_writer_until_termination() {
    let Some(root) = std::env::var_os("TIDE_FENCED_PROCESS_DIR") else {
        return;
    };
    let prefix = std::env::var("TIDE_FENCED_PROCESS_PREFIX").unwrap();
    let log = log(&prefix);
    let mut writer = log
        .acquire_bound(30, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    writer.append(&record(writer.epoch(), 1)).unwrap();
    writer
        .checkpoint_digest(1, [1; 32], [2; 32], Instant::now() + Duration::from_secs(5))
        .unwrap();
    std::fs::write(
        std::path::PathBuf::from(root).join("ready"),
        writer.replica_identity().lease_id.to_string(),
    )
    .unwrap();
    std::thread::sleep(Duration::from_secs(20));
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn killed_writer_requires_revocation_before_new_epoch_and_keeps_prefix() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-fenced-process-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let prefix = format!("/tide-test/fenced-process/{}-{stamp}", std::process::id());
    let log = log(&prefix);
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "child_holds_committed_writer_until_termination",
            "--ignored",
        ])
        .env("TIDE_FENCED_PROCESS_DIR", &root)
        .env("TIDE_FENCED_PROCESS_PREFIX", &prefix)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !root.join("ready").exists() && Instant::now() < deadline {
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready = root.join("ready").exists();
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(ready);
    let lease = std::fs::read_to_string(root.join("ready")).unwrap();
    assert_eq!(log.committed().unwrap(), 1);
    assert!(
        log.acquire_bound(10, include_bytes!("fixtures/test-client.der"))
            .is_err()
    );
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    ureq::post(&format!(
        "{}/v3/lease/revoke",
        endpoints.split(',').next().unwrap()
    ))
    .set("Content-Type", "application/json")
    .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
    .unwrap();
    let mut next = log
        .acquire_bound(10, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    assert_eq!(next.epoch(), 2);
    let checkpoint = next
        .load_current_digest(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        (
            checkpoint.shard_seq,
            checkpoint.epoch,
            checkpoint.state_digest,
            checkpoint.event_hash
        ),
        (1, 1, [1; 32], [2; 32])
    );
    next.append(&record(2, 2)).unwrap();
    assert_eq!(log.read(1).unwrap(), Some(record(1, 1)));
    assert_eq!(log.read(2).unwrap(), Some(record(2, 2)));
    next.revoke().unwrap();
    let target = root.canonicalize().unwrap();
    assert_eq!(
        target.parent(),
        Some(std::env::temp_dir().canonicalize().unwrap().as_path())
    );
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-fenced-process-")
    );
    std::fs::remove_dir_all(target).unwrap();
}
