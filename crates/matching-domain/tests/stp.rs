//! 自成交防护身份和模式决策的领域契约。

use matching_domain::{CancelReason, StpDecision, StpKey, StpMode, UserId, decide_stp};

fn user(value: u64) -> UserId {
    UserId::new(value)
}

#[test]
fn none_mode_never_reports_a_conflict() {
    assert_eq!(
        decide_stp(
            user(1),
            StpKey::new(7),
            StpMode::None,
            user(1),
            StpKey::new(7)
        ),
        StpDecision::NoConflict
    );
}

#[test]
fn same_user_conflicts_even_when_both_keys_are_zero() {
    assert_eq!(
        decide_stp(
            user(1),
            StpKey::new(0),
            StpMode::CancelTaker,
            user(1),
            StpKey::new(0),
        ),
        StpDecision::CancelTaker
    );
}

#[test]
fn non_zero_matching_group_keys_conflict_across_users() {
    assert_eq!(
        decide_stp(
            user(1),
            StpKey::new(7),
            StpMode::CancelMaker,
            user(2),
            StpKey::new(7),
        ),
        StpDecision::CancelMaker
    );
}

#[test]
fn zero_group_keys_do_not_conflict_across_users() {
    assert_eq!(
        decide_stp(
            user(1),
            StpKey::new(0),
            StpMode::CancelBoth,
            user(2),
            StpKey::new(0),
        ),
        StpDecision::NoConflict
    );
}

#[test]
fn different_non_zero_group_keys_do_not_conflict() {
    assert_eq!(
        decide_stp(
            user(1),
            StpKey::new(7),
            StpMode::CancelTaker,
            user(2),
            StpKey::new(8),
        ),
        StpDecision::NoConflict
    );
}

#[test]
fn decrement_and_cancel_is_rejected_at_submission_boundary() {
    assert_eq!(
        StpMode::DecrementAndCancel.submission_rejection(),
        Some(matching_domain::RejectReason::InvalidOrderCombination)
    );
}

#[test]
#[should_panic(expected = "unsupported STP mode reached matching")]
fn unsupported_mode_cannot_silently_disable_stp_in_the_matcher() {
    decide_stp(
        user(1),
        StpKey::new(0),
        StpMode::DecrementAndCancel,
        user(2),
        StpKey::new(0),
    );
}

#[test]
fn self_trade_cancel_reasons_keep_their_stable_protocol_codes() {
    assert_eq!(CancelReason::SelfTradePreventionMaker as u16, 1106);
    assert_eq!(CancelReason::SelfTradePreventionTaker as u16, 1107);
    assert_eq!(CancelReason::SelfTradePreventionBoth as u16, 1108);
}
