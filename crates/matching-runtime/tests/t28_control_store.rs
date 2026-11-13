//! 验证文件租约的排他性、持久纪元和损坏时失败关闭。
mod t22_support;
use matching_domain::{ClusterId, ShardId};
use matching_runtime::{ControlCoordinator, ControlStore, LeaseError};

#[test]
fn lease_is_exclusive_and_epochs_survive_reopen() {
    let temp = t22_support::TestDirectory::new();
    let path = temp.0.join("control.lease");
    let store = ControlStore::new(&path, ClusterId(2), ShardId(3));
    let lease = store.acquire_primary().unwrap();
    assert_eq!(lease.epoch(), 1);
    assert!(lease.verify(ClusterId(2), ShardId(3), 1).is_ok());
    assert!(lease.verify(ClusterId(2), ShardId(3), 2).is_err());
    assert!(matches!(store.acquire_primary(), Err(LeaseError::Busy)));
    drop(lease);
    let next = store.acquire_primary().unwrap();
    assert_eq!(next.epoch(), 2);
    assert!(next.verify(ClusterId(2), ShardId(3), 1).is_err());
}

#[test]
fn lease_rejects_wrong_identity_and_truncated_journal() {
    use std::io::Write;
    let temp = t22_support::TestDirectory::new();
    let path = temp.0.join("control.lease");
    let store = ControlStore::new(&path, ClusterId(2), ShardId(3));
    drop(store.acquire_primary().unwrap());
    let wrong = ControlStore::new(&path, ClusterId(2), ShardId(4));
    assert!(matches!(wrong.acquire_primary(), Err(LeaseError::Corrupt)));
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[1])
        .unwrap();
    assert!(matches!(store.acquire_primary(), Err(LeaseError::Corrupt)));
}

#[test]
fn lease_rejects_corrupt_record_and_existing_empty_file() {
    let temp = t22_support::TestDirectory::new();
    let empty_path = temp.0.join("empty.lease");
    std::fs::write(&empty_path, []).unwrap();
    assert!(matches!(
        ControlStore::new(&empty_path, ClusterId(2), ShardId(3)).acquire_primary(),
        Err(LeaseError::Corrupt)
    ));
    let path = temp.0.join("corrupt.lease");
    let store = ControlStore::new(&path, ClusterId(2), ShardId(3));
    drop(store.acquire_primary().unwrap());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[15] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(matches!(store.acquire_primary(), Err(LeaseError::Corrupt)));
}

#[test]
fn lease_child_probe() {
    let Ok(path) = std::env::var("T28_LEASE_PROBE_PATH") else {
        return;
    };
    let store = ControlStore::new(path, ClusterId(2), ShardId(3));
    assert!(matches!(store.acquire_primary(), Err(LeaseError::Busy)));
}

#[test]
fn lease_excludes_other_processes() {
    let temp = t22_support::TestDirectory::new();
    let path = temp.0.join("process.lease");
    let store = ControlStore::new(&path, ClusterId(2), ShardId(3));
    let _lease = store.acquire_primary().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lease_child_probe"])
        .env("T28_LEASE_PROBE_PATH", path)
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn coordinator_derives_stable_scoped_subcommand_from_current_lease() {
    let temp = t22_support::TestDirectory::new();
    let store = ControlStore::new(temp.0.join("coordinator.lease"), ClusterId(2), ShardId(3));
    let lease = store.acquire_primary().unwrap();
    let first = ControlCoordinator::shard_kill_subcommand(
        &lease,
        matching_domain::CommandId(8),
        true,
        true,
    );
    let retry = ControlCoordinator::shard_kill_subcommand(
        &lease,
        matching_domain::CommandId(8),
        true,
        true,
    );
    assert_eq!(first, retry);
    assert_eq!(
        first.0,
        matching_domain::CommandId(0x4c50e94e6fd1d973fdcada7c66a521cb)
    );
    let other = ControlCoordinator::shard_kill_subcommand(
        &lease,
        matching_domain::CommandId(8),
        false,
        false,
    );
    assert_ne!(first.0, other.0);
    assert_eq!(first.1.expected_primary_epoch, lease.epoch());
    assert_eq!(first.1.target_shard_id, ShardId(3));
}
