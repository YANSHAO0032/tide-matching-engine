//! 验证持久性配置与连续提交水位。
use matching_persistence::{
    CommitWatermark, DurabilityConfig, DurabilityMode, Profile, ReplicationMode,
};

#[test]
fn memory_only_rejects_production_recovery_and_ha() {
    let base = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::MemoryOnly,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: false,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    assert!(base.validate().is_ok());
    assert!(
        DurabilityConfig {
            profile: Profile::Production,
            ..base
        }
        .validate()
        .is_err()
    );
    assert!(
        DurabilityConfig {
            crash_recovery_required: true,
            ..base
        }
        .validate()
        .is_err()
    );
    assert!(
        DurabilityConfig {
            ha_enabled: true,
            ..base
        }
        .validate()
        .is_err()
    );
}

#[test]
fn ha_local_durability_requires_explicit_data_loss_opt_in() {
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    assert!(config.validate().is_err());
    assert!(
        DurabilityConfig {
            allow_data_loss_on_failover: true,
            ..config
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn group_configuration_requires_positive_capacity_and_deadline() {
    let base = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    assert!(
        DurabilityConfig {
            mode: DurabilityMode::GroupCommit {
                max_batch: 0,
                max_delay_micros: 1
            },
            ..base
        }
        .validate()
        .is_err()
    );
    assert!(
        DurabilityConfig {
            mode: DurabilityMode::GroupCommit {
                max_batch: 1,
                max_delay_micros: 0
            },
            ..base
        }
        .validate()
        .is_err()
    );
    assert!(
        DurabilityConfig {
            mode: DurabilityMode::GroupCommit {
                max_batch: 1,
                max_delay_micros: 1
            },
            ..base
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn standby_commit_requires_contiguous_local_and_remote_prefixes() {
    let mut state = CommitWatermark::new(ReplicationMode::StandbyDurable, 0);
    assert!(state.advance_local(2, 3).is_err());
    assert_eq!(state.committed(), 0);
    state.advance_local(1, 3).unwrap();
    assert_eq!(state.committed(), 0);
    assert!(state.advance_standby(2, 3).is_err());
    state.advance_standby(1, 2).unwrap();
    assert_eq!(state.committed(), 2);
    state.advance_standby(3, 3).unwrap();
    assert_eq!(state.committed(), 3);
}
