//! 验证审计记录恢复、已确认前缀的损坏检测和未确认尾部清理。
mod t22_support;
use matching_runtime::{AuditAction, AuditLog, AuditOutcome, AuditRecord};

#[test]
fn audit_is_exclusive_durable_and_detects_torn_or_corrupt_records() {
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("audit.log");
    let record = AuditRecord {
        actor_id: 7,
        command_id: 42,
        action: AuditAction::MarketUpdate,
        outcome: AuditOutcome::Attempt,
        market_id: Some(9),
        route_epoch: Some(3),
        primary_epoch: None,
        scope: None,
        command_digest: [0x51; 32],
    };
    let mut audit = AuditLog::create(&path, 2).unwrap();
    assert!(AuditLog::open(&path, 2).is_err());
    audit.append(&record).unwrap();
    drop(audit);
    let audit = AuditLog::open(&path, 2).unwrap();
    assert_eq!(audit.records(), &[record]);
    drop(audit);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..4]).unwrap();
    assert!(AuditLog::open(&path, 2).is_err());
    let mut unconfirmed = bytes.clone();
    unconfirmed.extend_from_slice(&[1, 2, 3]);
    std::fs::write(&path, &unconfirmed).unwrap();
    drop(AuditLog::open(&path, 2).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let mut unconfirmed_record = bytes.clone();
    unconfirmed_record.extend_from_slice(&bytes[4..]);
    std::fs::write(&path, unconfirmed_record).unwrap();
    drop(AuditLog::open(&path, 2).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
    assert!(AuditLog::open(&path, 2).is_err());
    let mut corrupt = bytes;
    corrupt[10] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert!(AuditLog::open(&path, 2).is_err());
}

#[test]
fn audit_mass_cancel_wire_tag_and_optional_fields_are_stable() {
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("wire.log");
    let mut audit = AuditLog::create(&path, 1).unwrap();
    let record = AuditRecord {
        actor_id: 7,
        command_id: 9,
        action: AuditAction::MassCancel,
        outcome: AuditOutcome::Succeeded,
        market_id: None,
        route_epoch: Some(4),
        primary_epoch: None,
        scope: None,
        command_digest: [3; 32],
    };
    audit.append(&record).unwrap();
    drop(audit);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..4], b"AUD2");
    assert_eq!(bytes.len(), 94);
    assert_eq!((bytes[28], bytes[29], bytes[30], bytes[35]), (10, 2, 0, 1));
    assert_eq!(AuditLog::open(&path, 1).unwrap().records(), &[record]);
}

#[test]
fn primary_lease_acquisition_records_the_actual_epoch() {
    use matching_domain::{ClusterId, ShardId};
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("lease-audit.log");
    let store =
        matching_runtime::ControlStore::new(dir.0.join("lease.log"), ClusterId(3), ShardId(4));
    let mut audit = AuditLog::create(&path, 4).unwrap();
    let lease = audit.acquire_primary(7, 100, &store).unwrap();
    assert_eq!(lease.epoch(), 1);
    assert_eq!(audit.records()[0].outcome, AuditOutcome::Attempt);
    assert_eq!(audit.records()[1].action, AuditAction::Control);
    assert_eq!(audit.records()[1].primary_epoch, Some(1));
    assert_eq!(audit.records()[1].scope, Some((3, 4)));
    drop(lease);
    let lease = audit.acquire_primary(7, 101, &store).unwrap();
    assert_eq!(lease.epoch(), 2);
    assert_eq!(audit.records()[3].primary_epoch, Some(2));
    drop(lease);
    drop(audit);
    assert_eq!(
        AuditLog::open(&path, 4).unwrap().records()[3].primary_epoch,
        Some(2)
    );
}

#[test]
fn failed_lease_and_insufficient_audit_capacity_do_not_hide_outcome() {
    use matching_domain::{ClusterId, ShardId};
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("lease.log");
    let store = matching_runtime::ControlStore::new(&path, ClusterId(5), ShardId(6));
    let mut limited = AuditLog::create(dir.0.join("limited.log"), 1).unwrap();
    assert!(matches!(
        limited.acquire_primary(1, 1, &store),
        Err(matching_runtime::AuditDispatchError::Audit(_))
    ));
    assert!(!path.exists());
    let held = store.acquire_primary().unwrap();
    let mut audit = AuditLog::create(dir.0.join("busy.log"), 2).unwrap();
    assert!(matches!(
        audit.acquire_primary(1, 2, &store),
        Err(matching_runtime::AuditDispatchError::Lease(
            matching_runtime::LeaseError::Busy
        ))
    ));
    assert_eq!(audit.records()[1].outcome, AuditOutcome::Rejected);
    assert_eq!(audit.records()[1].primary_epoch, None);
    drop(held);
    assert_eq!(store.acquire_primary().unwrap().epoch(), 2);
    let missing = matching_runtime::ControlStore::new(
        dir.0.join("missing").join("lease.log"),
        ClusterId(5),
        ShardId(6),
    );
    let mut io_audit = AuditLog::create(dir.0.join("io.log"), 2).unwrap();
    assert!(matches!(
        io_audit.acquire_primary(1, 3, &missing),
        Err(matching_runtime::AuditDispatchError::Lease(
            matching_runtime::LeaseError::Io(_)
        ))
    ));
    assert_eq!(io_audit.records()[1].outcome, AuditOutcome::Unknown);
}
