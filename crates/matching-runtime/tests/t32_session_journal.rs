//! 验证断连控制意图在本机文件中的恢复和失败关闭。
mod t22_support;
use matching_domain::{CommandId, MarketId, OriginSessionId, UserId};
use matching_runtime::{SessionControlJournal, SessionTarget};
use std::io::{Seek, SeekFrom, Write};

fn targets() -> Vec<SessionTarget> {
    vec![
        SessionTarget {
            market: MarketId(1),
            route_epoch: 2,
            command_id: CommandId(10),
        },
        SessionTarget {
            market: MarketId(2),
            route_epoch: 3,
            command_id: CommandId(11),
        },
    ]
}

#[test]
fn restart_disconnects_and_preserves_pending_targets_and_acknowledgements() {
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("session.log");
    let mut session =
        SessionControlJournal::create(&path, UserId(7), OriginSessionId(9), 88, targets(), 4)
            .unwrap();
    assert!(session.is_open());
    assert!(session.pending().unwrap().is_empty());
    assert!(SessionControlJournal::open(&path, 4).is_err());
    session.disconnect().unwrap();
    assert_eq!(session.pending().unwrap(), targets());
    session.acknowledge(MarketId(1)).unwrap();
    drop(session);
    let session = SessionControlJournal::open(&path, 4).unwrap();
    assert!(!session.is_open());
    assert_eq!(session.pending().unwrap(), vec![targets()[1]]);
    assert_eq!(session.user(), UserId(7));
    assert_eq!(session.session(), OriginSessionId(9));
}

#[test]
fn restart_of_open_session_closes_it_and_corrupt_tail_fails_closed() {
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("session.log");
    drop(
        SessionControlJournal::create(&path, UserId(7), OriginSessionId(9), 88, targets(), 4)
            .unwrap(),
    );
    let session = SessionControlJournal::open(&path, 4).unwrap();
    assert!(!session.is_open());
    assert_eq!(session.pending().unwrap(), targets());
    drop(session);
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    file.write_all(&[1]).unwrap();
    file.sync_all().unwrap();
    assert!(SessionControlJournal::open(&path, 4).is_err());
}

#[test]
fn invalid_lists_and_premature_or_unknown_acknowledgements_are_rejected() {
    let dir = t22_support::TestDirectory::new();
    let mut duplicate = targets();
    duplicate[1].command_id = duplicate[0].command_id;
    assert!(
        SessionControlJournal::create(
            dir.0.join("duplicate.log"),
            UserId(7),
            OriginSessionId(9),
            88,
            duplicate,
            4
        )
        .is_err()
    );
    let mut unsorted = targets();
    unsorted.reverse();
    assert!(
        SessionControlJournal::create(
            dir.0.join("unsorted.log"),
            UserId(7),
            OriginSessionId(9),
            88,
            unsorted,
            4
        )
        .is_err()
    );
    let mut session = SessionControlJournal::create(
        dir.0.join("good.log"),
        UserId(7),
        OriginSessionId(9),
        88,
        targets(),
        4,
    )
    .unwrap();
    assert!(session.acknowledge(MarketId(1)).is_err());
    session.disconnect().unwrap();
    assert!(session.acknowledge(MarketId(9)).is_err());
    session.acknowledge(MarketId(1)).unwrap();
    session.acknowledge(MarketId(1)).unwrap();
    assert_eq!(session.pending().unwrap(), vec![targets()[1]]);
}

#[test]
fn each_truncated_transition_is_rejected() {
    let dir = t22_support::TestDirectory::new();
    let source = dir.0.join("source.log");
    let mut session =
        SessionControlJournal::create(&source, UserId(7), OriginSessionId(9), 88, targets(), 4)
            .unwrap();
    session.disconnect().unwrap();
    session.acknowledge(MarketId(1)).unwrap();
    drop(session);
    let bytes = std::fs::read(source).unwrap();
    for cut in 1..9 {
        let path = dir.0.join(format!("cut-{cut}.log"));
        std::fs::write(&path, &bytes[..bytes.len() - cut]).unwrap();
        assert!(SessionControlJournal::open(path, 4).is_err(), "cut={cut}");
    }
}

#[test]
fn pending_command_uses_persisted_identity_and_session_scope() {
    use matching_domain::{Command, ShardCancelScope};
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("command.log");
    let mut session =
        SessionControlJournal::create(&path, UserId(7), OriginSessionId(9), 88, targets(), 4)
            .unwrap();
    session.disconnect().unwrap();
    let before = session.command(MarketId(2)).unwrap();
    assert_eq!(before.command_id, CommandId(11));
    assert_eq!(before.session_id, 88);
    assert_eq!(before.client_seq, 1);
    assert_eq!(before.route_epoch, Some(3));
    assert!(
        matches!(before.payload, Command::MassCancelShard(value) if value.scope == ShardCancelScope::Session { user_id: UserId(7), session_id: OriginSessionId(9) })
    );
    drop(session);
    let reopened = SessionControlJournal::open(path, 4).unwrap();
    assert_eq!(before, reopened.command(MarketId(2)).unwrap());
}

#[test]
fn dispatch_refuses_non_durable_shard_without_confirming_target() {
    use matching_domain::{ClusterId, ShardId};
    use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
    use matching_runtime::{
        MarketRoute, RouteState, RouteTable, SessionDispatchError, Shard, dispatch_session_cancel,
    };
    let dir = t22_support::TestDirectory::new();
    let mut journal = SessionControlJournal::create(
        dir.0.join("dispatch.log"),
        UserId(7),
        OriginSessionId(9),
        88,
        targets(),
        4,
    )
    .unwrap();
    journal.disconnect().unwrap();
    let routes = RouteTable::new([MarketRoute {
        market_id: MarketId(1),
        shard_id: ShardId(99),
        route_epoch: 2,
        state: RouteState::Active,
    }])
    .unwrap();
    let book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(100).unwrap());
    let mut shard = Shard::start(
        ShardId(99),
        ClusterId(99),
        routes,
        vec![(MarketId(1), book)],
    )
    .unwrap();
    assert!(matches!(
        dispatch_session_cancel(
            &mut journal,
            MarketId(1),
            &shard.handle(),
            std::time::Duration::from_secs(2)
        ),
        Err(SessionDispatchError::NonDurable)
    ));
    assert_eq!(journal.pending().unwrap(), targets());
    shard.shutdown(std::time::Duration::from_secs(2)).unwrap();
}

#[test]
fn fanout_does_not_acknowledge_when_no_durable_route_exists() {
    let dir = t22_support::TestDirectory::new();
    let mut session = SessionControlJournal::create(
        dir.0.join("fanout.log"),
        UserId(7),
        OriginSessionId(9),
        88,
        targets(),
        4,
    )
    .unwrap();
    session.disconnect().unwrap();
    assert!(matches!(
        matching_runtime::dispatch_session_cancels(
            &mut session,
            &[],
            std::time::Duration::from_secs(1)
        ),
        Err(matching_runtime::SessionDispatchError::Route)
    ));
    assert_eq!(session.pending().unwrap(), targets());
}

#[test]
fn disconnect_policy_disabled_never_schedules_cancels_after_restart() {
    let dir = t22_support::TestDirectory::new();
    let path = dir.0.join("disabled.log");
    let mut session = SessionControlJournal::create_with_policy(
        &path,
        UserId(7),
        OriginSessionId(9),
        88,
        targets(),
        4,
        false,
    )
    .unwrap();
    session.disconnect().unwrap();
    assert!(session.pending().unwrap().is_empty());
    assert!(session.command(MarketId(1)).is_err());
    drop(session);
    let recovered = SessionControlJournal::open(path, 4).unwrap();
    assert!(recovered.pending().unwrap().is_empty());
    assert!(recovered.command(MarketId(1)).is_err());
}
