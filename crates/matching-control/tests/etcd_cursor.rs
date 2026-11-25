//! 使用隔离集群验证持久游标和旧发布者围栏。
use matching_control::{CursorPosition, CursorStore, EtcdClient, PublisherLease};
use matching_domain::{ClusterId, ShardId};
use std::time::Duration;

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn checkpoint_is_durable_namespaced_and_fenced_by_publisher_epoch() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/cursor/{}", std::process::id());
    let store = CursorStore::new(client.clone(), &prefix, ClusterId(11), ShardId(12)).unwrap();
    let other = CursorStore::new(client.clone(), &prefix, ClusterId(11), ShardId(13)).unwrap();
    let mut first =
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(11), ShardId(12), 10).unwrap();
    let old_token = first.token();
    assert!(!old_token.is_locally_invalidated());
    assert_eq!(old_token.epoch(), 1);
    first.verify().unwrap();
    first.keep_alive().unwrap();
    assert!(
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(11), ShardId(12), 10).is_err()
    );
    assert!(store.load(7).unwrap().is_none());
    assert!(other.load(7).unwrap().is_none());
    let position = CursorPosition {
        shard_seq: 5,
        event_index: 2,
    };
    let checkpoint = store.advance(&old_token, 7, None, position).unwrap();
    assert_eq!(
        (
            checkpoint.generation,
            checkpoint.publisher_epoch,
            checkpoint.position
        ),
        (1, 1, position)
    );
    assert!(store.advance(&old_token, 7, None, position).is_err());
    assert!(
        store
            .advance(
                &old_token,
                7,
                Some(1),
                CursorPosition {
                    shard_seq: 4,
                    event_index: 9
                }
            )
            .is_err()
    );
    first.revoke().unwrap();
    assert!(old_token.is_locally_invalidated());
    assert!(first.verify().is_err());
    let mut next =
        PublisherLease::acquire(client, &prefix, ClusterId(11), ShardId(12), 10).unwrap();
    assert_eq!(next.token().epoch(), 2);
    assert!(
        store
            .advance(
                &old_token,
                7,
                Some(1),
                CursorPosition {
                    shard_seq: 6,
                    event_index: 0
                }
            )
            .is_err()
    );
    assert_eq!(store.load(7).unwrap(), Some(checkpoint));
    let advanced = store
        .advance(
            &next.token(),
            7,
            Some(1),
            CursorPosition {
                shard_seq: 6,
                event_index: 0,
            },
        )
        .unwrap();
    assert_eq!((advanced.generation, advanced.publisher_epoch), (2, 2));
    assert!(other.load(7).unwrap().is_none());
    next.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn expired_publisher_cannot_advance_after_successor_acquires() {
    use std::time::Instant;
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/expiry/{}", std::process::id());
    let store = CursorStore::new(client.clone(), &prefix, ClusterId(21), ShardId(22)).unwrap();
    let first =
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(21), ShardId(22), 1).unwrap();
    let old = first.token();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut successor = loop {
        match PublisherLease::acquire(client.clone(), &prefix, ClusterId(21), ShardId(22), 5) {
            Ok(lease) => break lease,
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100))
            }
            Err(error) => panic!("无法获取后继租约: {error}"),
        }
    };
    assert_eq!(successor.token().epoch(), 2);
    assert!(!old.is_locally_invalidated());
    assert!(
        store
            .advance(
                &old,
                1,
                None,
                CursorPosition {
                    shard_seq: 1,
                    event_index: 0
                }
            )
            .is_err()
    );
    assert!(old.is_locally_invalidated());
    assert!(first.verify().is_err());
    assert!(store.load(1).unwrap().is_none());
    store
        .advance(
            &successor.token(),
            1,
            None,
            CursorPosition {
                shard_seq: 1,
                event_index: 0,
            },
        )
        .unwrap();
    successor.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn newer_checkpoint_invalidates_old_publisher_even_before_cas() {
    use std::time::Instant;
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/newer-cursor/{}", std::process::id());
    let store = CursorStore::new(client.clone(), &prefix, ClusterId(24), ShardId(25)).unwrap();
    let old =
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(24), ShardId(25), 1).unwrap();
    let token = old.token();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut next = loop {
        match PublisherLease::acquire(client.clone(), &prefix, ClusterId(24), ShardId(25), 5) {
            Ok(lease) => break lease,
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100))
            }
            Err(error) => panic!("后继租约失败: {error}"),
        }
    };
    let position = CursorPosition {
        shard_seq: 1,
        event_index: 0,
    };
    store.advance(&next.token(), 1, None, position).unwrap();
    assert!(store.advance(&token, 1, Some(1), position).is_err());
    assert!(token.is_locally_invalidated());
    next.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn concurrent_initial_checkpoints_have_one_winner() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/race/{}", std::process::id());
    let mut publisher =
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(31), ShardId(32), 10).unwrap();
    let token = publisher.token();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for sequence in [1, 2] {
            let store =
                CursorStore::new(client.clone(), &prefix, ClusterId(31), ShardId(32)).unwrap();
            let token = token.clone();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                barrier.wait();
                store.advance(
                    &token,
                    7,
                    None,
                    CursorPosition {
                        shard_seq: sequence,
                        event_index: 0,
                    },
                )
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let store = CursorStore::new(client, &prefix, ClusterId(31), ShardId(32)).unwrap();
    assert_eq!(store.load(7).unwrap().unwrap().generation, 1);
    publisher.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn corrupt_checkpoint_and_cross_shard_token_fail_closed() {
    use matching_control::KvVersion;
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/corrupt/{}", std::process::id());
    let mut publisher =
        PublisherLease::acquire(client.clone(), &prefix, ClusterId(41), ShardId(42), 10).unwrap();
    let other = CursorStore::new(client.clone(), &prefix, ClusterId(41), ShardId(43)).unwrap();
    assert!(
        other
            .advance(
                &publisher.token(),
                1,
                None,
                CursorPosition {
                    shard_seq: 1,
                    event_index: 0
                }
            )
            .is_err()
    );
    let store = CursorStore::new(client.clone(), &prefix, ClusterId(41), ShardId(42)).unwrap();
    let checkpoint = store
        .advance(
            &publisher.token(),
            1,
            None,
            CursorPosition {
                shard_seq: 2,
                event_index: 0,
            },
        )
        .unwrap();
    assert_eq!(checkpoint.generation, 1);
    let key = format!("{prefix}/cluster/0029/shard/002a/cursor/0000000000000001");
    let (revision, mut value) = client.get(key.as_bytes()).unwrap().unwrap();
    value[10] ^= 1;
    client
        .compare_and_swap(key.as_bytes(), KvVersion::Revision(revision), &value)
        .unwrap()
        .unwrap();
    assert_eq!(
        store.load(1).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(
        store
            .advance(
                &publisher.token(),
                1,
                Some(1),
                CursorPosition {
                    shard_seq: 3,
                    event_index: 0
                }
            )
            .is_err()
    );
    assert!(publisher.token().is_locally_invalidated());
    publisher.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn required_sinks_are_durable_and_updates_are_versioned() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/sinks/{}", std::process::id());
    let store = CursorStore::new(client.clone(), &prefix, ClusterId(51), ShardId(52)).unwrap();
    let other = CursorStore::new(client, &prefix, ClusterId(51), ShardId(52)).unwrap();
    assert!(store.load_required_sinks().unwrap().is_none());
    let first = store.set_required_sinks(None, &[3, 7]).unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(other.load_required_sinks().unwrap(), Some(first));
    assert!(other.set_required_sinks(None, &[3]).is_err());
    assert!(other.set_required_sinks(Some(1), &[7, 3]).is_err());
    assert!(other.set_required_sinks(Some(1), &[3, 3]).is_err());
    let updated = other.set_required_sinks(Some(1), &[7]).unwrap();
    assert_eq!(updated.generation, 2);
    assert_eq!(store.load_required_sinks().unwrap(), Some(updated));
}
