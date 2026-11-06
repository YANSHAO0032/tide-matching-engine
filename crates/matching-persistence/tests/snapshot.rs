//! 验证快照文件持久提交和损坏拒绝。
use matching_persistence::{
    FileManifestStorage, ManifestIntent, ManifestManager, commit_snapshot, read_snapshot,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "t24-snapshot-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::create_dir(path.join("manifest")).unwrap();
    std::fs::create_dir(path.join("snapshot")).unwrap();
    path
}
#[test]
fn snapshot_commit_roundtrips_and_corruption_is_rejected() {
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 3,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    commit_snapshot(
        root.join("snapshot"),
        &mut manager,
        7,
        3,
        b"immutable image",
    )
    .unwrap();
    assert_eq!(manager.state().latest_snapshot, Some((7, 3)));
    let file = root.join("snapshot").join("snapshot-7.bin");
    assert_eq!(
        read_snapshot(&file).unwrap(),
        (7, 3, b"immutable image".to_vec())
    );
    let mut bytes = std::fs::read(&file).unwrap();
    let i = bytes.len() - 5;
    bytes[i] ^= 1;
    std::fs::write(&file, bytes).unwrap();
    assert!(read_snapshot(&file).is_err());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn snapshot_ahead_of_durable_state_does_not_publish_or_write() {
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    assert!(commit_snapshot(root.join("snapshot"), &mut manager, 7, 1, b"image").is_err());
    assert!(manager.state().latest_snapshot.is_none());
    assert_eq!(std::fs::read_dir(root.join("snapshot")).unwrap().count(), 0);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn every_truncated_file_is_rejected() {
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 3,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    commit_snapshot(root.join("snapshot"), &mut manager, 7, 3, b"image bytes").unwrap();
    let file = root.join("snapshot").join("snapshot-7.bin");
    let bytes = std::fs::read(&file).unwrap();
    for end in 0..bytes.len() {
        std::fs::write(&file, &bytes[..end]).unwrap();
        assert!(read_snapshot(&file).is_err(), "截断位置 {end}");
    }
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_failure_leaves_only_uncommitted_snapshot() {
    use matching_persistence::{ManifestState, ManifestStorage};
    use std::io;
    struct Fails;
    impl ManifestStorage for Fails {
        fn write_inactive_tmp(&mut self, _: u64, _: &ManifestState) -> io::Result<()> {
            Err(io::Error::other("注入清单写入失败"))
        }
        fn sync_tmp(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn rename_inactive_slot(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn sync_directory(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let root = directory();
    let mut manager = ManifestManager::new(
        Fails,
        ManifestState {
            generation: 2,
            local_durable_seq: 3,
            ..ManifestState::default()
        },
    );
    assert!(commit_snapshot(root.join("snapshot"), &mut manager, 7, 3, b"image").is_err());
    assert_eq!(manager.state().latest_snapshot, None);
    assert_eq!(
        read_snapshot(root.join("snapshot").join("snapshot-7.bin")).unwrap(),
        (7, 3, b"image".to_vec())
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn committed_reader_checks_manifest_identity_and_sequence() {
    use matching_persistence::read_committed_snapshot;
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 3,
            first_seq: 1,
        })
        .unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    assert!(
        read_committed_snapshot(root.join("snapshot"), &manager)
            .unwrap()
            .is_none()
    );
    commit_snapshot(root.join("snapshot"), &mut manager, 7, 3, b"image").unwrap();
    assert_eq!(
        read_committed_snapshot(root.join("snapshot"), &manager)
            .unwrap()
            .unwrap(),
        (7, 3, b"image".to_vec())
    );
    let path = root.join("snapshot").join("snapshot-7.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[14..22].copy_from_slice(&2_u64.to_be_bytes());
    let end = bytes.len() - 4;
    let checksum = matching_persistence::crc32c(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum.to_be_bytes());
    std::fs::write(&path, bytes).unwrap();
    assert!(read_committed_snapshot(root.join("snapshot"), &manager).is_err());
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn uninitialized_manifest_cannot_acknowledge_a_zero_sequence_snapshot() {
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    assert!(commit_snapshot(root.join("snapshot"), &mut manager, 7, 0, b"image").is_err());
    assert!(manager.state().latest_snapshot.is_none());
    assert_eq!(std::fs::read_dir(root.join("snapshot")).unwrap().count(), 0);
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn committed_manifest_retains_previous_snapshot_after_later_wal_updates() {
    let root = directory();
    let mut manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    manager.submit(ManifestIntent::Initialize).unwrap();
    manager.submit(ManifestIntent::RecoveryAnchor).unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 3,
            first_seq: 1,
        })
        .unwrap();
    commit_snapshot(root.join("snapshot"), &mut manager, 7, 3, b"first").unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 4,
            first_seq: 1,
        })
        .unwrap();
    commit_snapshot(root.join("snapshot"), &mut manager, 8, 4, b"second").unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 5,
            first_seq: 1,
        })
        .unwrap();
    manager
        .submit(ManifestIntent::LocalDurableAdvanced {
            through_seq: 6,
            first_seq: 1,
        })
        .unwrap();
    drop(manager);
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    assert_eq!(manager.state().latest_snapshot, Some((8, 4)));
    assert_eq!(manager.state().previous_snapshot, Some((7, 3)));
    let prior = matching_persistence::read_previous_snapshot(root.join("snapshot"), &manager)
        .unwrap()
        .unwrap();
    assert_eq!(prior, (7, 3, b"first".to_vec()));
    drop(manager);
    std::fs::remove_dir_all(root).unwrap();
}
