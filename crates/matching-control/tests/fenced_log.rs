//! 使用真实控制集群验证分片连续日志与写权围栏。
use matching_control::{EtcdClient, FencedLog};
use matching_domain::{ClusterId, ShardId};
use matching_persistence::WalRecord;
use std::time::Duration;

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn genesis_protocol_requires_empty_log_before_first_write() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("/tide-test/genesis-proof/{}-{stamp}", std::process::id());
    let log = FencedLog::new(client, &prefix, ClusterId(193), ShardId(194)).unwrap();
    let cert = include_bytes!("fixtures/test-client.der");
    let deadline = || std::time::Instant::now() + Duration::from_secs(5);
    let mut writer = log.acquire_bound(30, cert).unwrap();
    writer.initialize_genesis_protocol(deadline()).unwrap();
    writer.initialize_genesis_protocol(deadline()).unwrap();
    writer
        .append(&WalRecord {
            cluster_id: 193,
            shard_id: 194,
            flags: 0,
            epoch: 1,
            shard_seq: 1,
            engine_time: 0,
            command_id: 1,
            payload: vec![7],
        })
        .unwrap();
    writer.revoke().unwrap();
    let mut next = log.acquire_bound(30, cert).unwrap();
    assert!(next.verify_genesis_protocol(deadline()).is_ok());
    assert!(next.initialize_genesis_protocol(deadline()).is_err());
    next.revoke().unwrap();
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let unmarked = FencedLog::new(
        EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap(),
        &format!("{prefix}/unmarked"),
        ClusterId(193),
        ShardId(194),
    )
    .unwrap();
    let mut legacy = unmarked.acquire_bound(30, cert).unwrap();
    legacy
        .append(&WalRecord {
            cluster_id: 193,
            shard_id: 194,
            flags: 0,
            epoch: 1,
            shard_seq: 1,
            engine_time: 0,
            command_id: 1,
            payload: vec![7],
        })
        .unwrap();
    assert!(legacy.verify_genesis_protocol(deadline()).is_err());
    assert!(legacy.is_locally_invalidated());
    legacy.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn digest_checkpoint_is_bound_to_live_writer_and_exact_committed_head() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let log = FencedLog::new(
        client,
        &format!("/tide-test/digest/{}-{stamp}", std::process::id()),
        ClusterId(181),
        ShardId(182),
    )
    .unwrap();
    let cert = include_bytes!("fixtures/test-client.der");
    let deadline = || std::time::Instant::now() + Duration::from_secs(5);
    let mut first = log.acquire_bound(30, cert).unwrap();
    let record = |epoch, seq| WalRecord {
        cluster_id: 181,
        shard_id: 182,
        flags: 0,
        epoch,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![7],
    };
    first.append(&record(first.epoch(), 1)).unwrap();
    let expected = first
        .checkpoint_digest(1, [1; 32], [2; 32], deadline())
        .unwrap();
    assert_eq!(
        first
            .checkpoint_digest(1, [1; 32], [2; 32], deadline())
            .unwrap(),
        expected
    );
    assert_eq!(log.read_digest_checkpoint(1).unwrap(), Some(expected));
    first.revoke().unwrap();
    assert!(
        first
            .checkpoint_digest(1, [1; 32], [2; 32], deadline())
            .is_err()
    );
    let mut second = log.acquire_bound(30, cert).unwrap();
    assert_eq!(second.load_current_digest(deadline()).unwrap(), expected);
    second.append(&record(second.epoch(), 2)).unwrap();
    assert_eq!(
        second.load_latest_digest(deadline()).unwrap(),
        Some(expected)
    );
    assert_eq!(log.read_digest_checkpoint(2).unwrap(), None);
    let current = second
        .checkpoint_digest(2, [3; 32], [4; 32], deadline())
        .unwrap();
    assert_eq!(log.read_digest_checkpoint(2).unwrap(), Some(current));
    assert_eq!(
        second.load_latest_digest(deadline()).unwrap(),
        Some(current)
    );
    assert!(
        second
            .checkpoint_digest(2, [9; 32], [4; 32], deadline())
            .is_err()
    );
    assert!(second.is_locally_invalidated());
    assert_eq!(log.read_digest_checkpoint(2).unwrap(), Some(current));
    second.revoke().unwrap();
    let mut stale = log.acquire_bound(30, cert).unwrap();
    assert_eq!(stale.load_current_digest(deadline()).unwrap(), current);
    assert!(
        stale
            .checkpoint_digest(1, [1; 32], [2; 32], deadline())
            .is_err()
    );
    assert!(stale.is_locally_invalidated());
    stale.revoke().unwrap();
    let mut unbound = log.acquire(30).unwrap();
    assert!(
        unbound
            .checkpoint_digest(2, [3; 32], [4; 32], deadline())
            .is_err()
    );
    assert!(unbound.is_locally_invalidated());
    unbound.revoke().unwrap();
    let mut revoked = log.acquire_bound(30, cert).unwrap();
    let endpoint = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    ureq::post(&format!(
        "{}/v3/lease/revoke",
        endpoint.split(',').next().unwrap()
    ))
    .set("Content-Type", "application/json")
    .send_string(&format!(
        "{{\"ID\":\"{}\"}}",
        revoked.replica_identity().lease_id
    ))
    .unwrap();
    assert!(
        revoked
            .checkpoint_digest(2, [3; 32], [4; 32], deadline())
            .is_err()
    );
    assert!(revoked.is_locally_invalidated());
    assert_eq!(log.read_digest_checkpoint(2).unwrap(), Some(current));
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn certificate_binding_is_atomic_with_writer_lease() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let log = FencedLog::new(
        client,
        &format!("/tide-test/bound-owner/{}-{stamp}", std::process::id()),
        ClusterId(101),
        ShardId(102),
    )
    .unwrap();
    let certificate = include_bytes!("fixtures/test-client.der");
    let other = include_bytes!("fixtures/test-other.der");
    let mut writer = log.acquire_bound(10, certificate).unwrap();
    let identity = writer.replica_identity();
    let deadline = || std::time::Instant::now() + Duration::from_secs(3);
    log.verify_bound_replica_writer(identity, certificate, deadline())
        .unwrap();
    assert!(
        log.verify_bound_replica_writer(identity, other, deadline())
            .is_err()
    );
    writer.revoke().unwrap();
    assert!(
        log.verify_bound_replica_writer(identity, certificate, deadline())
            .is_err()
    );
    let mut next = log.acquire_bound(10, other).unwrap();
    assert!(
        log.verify_bound_replica_writer(next.replica_identity(), certificate, deadline())
            .is_err()
    );
    log.verify_bound_replica_writer(next.replica_identity(), other, deadline())
        .unwrap();
    next.revoke().unwrap();
    let mut unbound = log.acquire(10).unwrap();
    assert!(
        log.verify_bound_replica_writer(unbound.replica_identity(), other, deadline())
            .is_err()
    );
    unbound.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn standby_verifies_exact_live_writer_identity_before_epoch_barrier() {
    use matching_control::activate_replica_writer;
    use matching_persistence::{
        DurabilityConfig, DurabilityMode, DurableBatcher, FencedStandbyReplica,
        FileManifestStorage, ManifestManager, Profile, RegisteredAppender, ReplicaEpochStore,
        ReplicationMode, SegmentWriter, StandbyReplica,
    };
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!(
        "/tide-test/fenced-log-barrier/{}-{stamp}",
        std::process::id()
    );
    let log = FencedLog::new(client, &prefix, ClusterId(91), ShardId(92)).unwrap();
    let mut old = log.acquire(10).unwrap();
    let first = old.replica_identity();
    let deadline = || std::time::Instant::now() + Duration::from_secs(3);
    log.verify_replica_writer(first, deadline()).unwrap();
    let root =
        std::env::temp_dir().join(format!("tide-owner-barrier-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let manager = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::new(SegmentWriter::new(root.join("wal"), 4096).unwrap(), manager)
            .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(appender, config, first.epoch).unwrap();
    let replica = StandbyReplica::new(batcher, 91, 92, first.epoch, 8).unwrap();
    let epochs = ReplicaEpochStore::create(root.join("epoch"), 91, 92, first.epoch, 0).unwrap();
    let mut replica = FencedStandbyReplica::new(replica, epochs).unwrap();
    assert_eq!(
        activate_replica_writer(&log, &mut replica, first, deadline()).unwrap(),
        0
    );
    replica
        .confirm_reconciled(0, |_, _, _| log.verify_replica_writer(first, deadline()))
        .unwrap();
    let record = WalRecord {
        cluster_id: 91,
        shard_id: 92,
        flags: 0,
        epoch: first.epoch,
        shard_seq: 1,
        engine_time: 0,
        command_id: 1,
        payload: vec![3],
    };
    old.prepare(&record).unwrap();
    replica
        .append_batch(std::slice::from_ref(&record), std::time::Instant::now())
        .unwrap();
    let mut wrong = first;
    wrong.shard_id += 1;
    assert!(log.verify_replica_writer(wrong, deadline()).is_err());
    wrong = first;
    wrong.lease_id += 1;
    assert!(log.verify_replica_writer(wrong, deadline()).is_err());
    old.revoke().unwrap();
    assert!(log.verify_replica_writer(first, deadline()).is_err());
    assert!(activate_replica_writer(&log, &mut replica, first, deadline()).is_err());
    let mut next = log.acquire(10).unwrap();
    let interrupted = next.replica_identity();
    assert!(log.verify_replica_writer(first, deadline()).is_err());
    assert!(log.verify_replica_writer(interrupted, deadline()).is_ok());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            assert_eq!(
                matching_control::receive_replica_fence(
                    &mut stream,
                    &log,
                    &mut replica,
                    interrupted,
                    deadline()
                )
                .unwrap(),
                1
            );
            assert!(
                matching_control::confirm_authoritative_replica(
                    &log,
                    &mut replica,
                    interrupted,
                    0,
                    deadline()
                )
                .is_err()
            );
            stream.write_all(b"RFR1\0").unwrap();
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        assert!(
            next.reconcile_remote_prefix(&mut stream, 0, deadline())
                .is_err()
        );
        server.join().unwrap();
    });
    assert!(next.is_locally_invalidated());
    assert_eq!(log.committed().unwrap(), 0);
    next.revoke().unwrap();
    let mut next = log
        .acquire_bound(10, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    let second = next.replica_identity();
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    let ca = include_bytes!("fixtures/test-ca.der");
    let client_cert = include_bytes!("fixtures/test-client.der");
    let server_cert = include_bytes!("fixtures/test-server.der");
    let client_config = std::sync::Arc::new(
        matching_control::replica_client_config(
            &[ca],
            vec![CertificateDer::from(client_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-client-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let server_config = std::sync::Arc::new(
        matching_control::replica_server_config(
            &[ca],
            vec![CertificateDer::from(server_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-server-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let other_cert = include_bytes!("fixtures/test-other.der");
    let other_config = std::sync::Arc::new(
        matching_control::replica_client_config(
            &[ca],
            vec![CertificateDer::from(other_cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/test-other-key.der").to_vec(),
            )),
        )
        .unwrap(),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                other_cert,
                deadline(),
            )
            .unwrap();
            assert!(
                matching_control::serve_authenticated_fenced_prefix(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .is_err()
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let _stream = matching_control::TlsReplicaStream::connect(
            socket,
            other_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        server.join().unwrap();
    });
    assert_eq!(log.committed().unwrap(), 0);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                deadline(),
            )
            .unwrap();
            assert_eq!(
                matching_control::serve_authenticated_fenced_prefix(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .unwrap(),
                1
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        assert_eq!(
            next.reconcile_authenticated_remote_prefix(&mut stream, 0, deadline())
                .unwrap(),
            1
        );
        server.join().unwrap();
    });
    let mut pending = record.clone();
    pending.epoch = second.epoch;
    pending.shard_seq = 2;
    pending.command_id = 2;
    assert!(
        replica
            .append_batch(std::slice::from_ref(&pending), std::time::Instant::now())
            .is_err()
    );
    assert_eq!(log.committed().unwrap(), 1);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                other_cert,
                deadline(),
            )
            .unwrap();
            assert!(
                matching_control::receive_authenticated_replica_resume(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .is_err()
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let _stream = matching_control::TlsReplicaStream::connect(
            socket,
            other_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        server.join().unwrap();
    });
    assert!(
        replica
            .append_batch(std::slice::from_ref(&pending), std::time::Instant::now())
            .is_err()
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            assert!(
                matching_control::receive_replica_resume(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        assert!(matching_control::send_replica_resume(&mut stream, second, 2, deadline()).is_err());
        server.join().unwrap();
    });
    assert!(
        replica
            .append_batch(std::slice::from_ref(&pending), std::time::Instant::now())
            .is_err()
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            use std::io::{Read, Write};
            struct FailReply(/** 用于模拟回复失败的实际连接。 */ std::net::TcpStream);
            impl Read for FailReply {
                fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                    self.0.read(bytes)
                }
            }
            impl Write for FailReply {
                fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                    Err(std::io::Error::other("模拟确认发送失败"))
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            impl matching_control::ReplicationStream for FailReply {
                fn set_io_timeout(&mut self, timeout: Duration) -> std::io::Result<()> {
                    self.0.set_read_timeout(Some(timeout))?;
                    self.0.set_write_timeout(Some(timeout))
                }
            }
            let (stream, _) = listener.accept().unwrap();
            assert!(
                matching_control::receive_replica_resume(
                    &mut FailReply(stream),
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        assert!(matching_control::send_replica_resume(&mut stream, second, 1, deadline()).is_err());
        server.join().unwrap();
    });
    assert!(
        replica
            .append_batch(std::slice::from_ref(&pending), std::time::Instant::now())
            .is_err()
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                deadline(),
            )
            .unwrap();
            assert_eq!(
                matching_control::receive_authenticated_replica_resume(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .unwrap(),
                1
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        matching_control::send_replica_resume(&mut stream, second, 1, deadline()).unwrap();
        server.join().unwrap();
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            assert_eq!(
                matching_control::receive_replica_resume(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .unwrap(),
                1
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        matching_control::send_replica_resume(&mut stream, second, 1, deadline()).unwrap();
        server.join().unwrap();
    });
    assert_eq!(log.committed().unwrap(), 1);
    assert_eq!(log.read(1).unwrap(), Some(record.clone()));
    let mut stale = record.clone();
    stale.shard_seq = 2;
    assert!(
        replica
            .append_batch(&[stale], std::time::Instant::now())
            .is_err()
    );
    next.prepare(&pending).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                other_cert,
                deadline(),
            )
            .unwrap();
            assert!(
                matching_control::receive_authenticated_replication_batch(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(3)
                )
                .is_err()
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            other_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        assert!(
            matching_control::send_replication_batch(
                &mut stream,
                std::slice::from_ref(&pending),
                32 * 1024 * 1024
            )
            .is_err()
        );
        server.join().unwrap();
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                deadline(),
            )
            .unwrap();
            let ack = matching_control::receive_authenticated_replication_batch(
                &mut stream,
                &log,
                &mut replica,
                second,
                8,
                32 * 1024 * 1024,
                Duration::from_secs(3),
            )
            .unwrap();
            assert_eq!(ack.through_seq, 2);
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        assert_eq!(
            matching_control::send_replication_batch(
                &mut stream,
                std::slice::from_ref(&pending),
                32 * 1024 * 1024
            )
            .unwrap()
            .through_seq,
            2
        );
        server.join().unwrap();
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            assert!(
                matching_control::receive_replica_resume(
                    &mut stream,
                    &log,
                    &mut replica,
                    second,
                    0,
                    deadline()
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        assert!(matching_control::send_replica_resume(&mut stream, second, 1, deadline()).is_err());
        server.join().unwrap();
    });
    let mut newer = pending.clone();
    newer.shard_seq = 3;
    assert!(
        replica
            .append_batch(&[newer], std::time::Instant::now())
            .is_err()
    );
    next.revoke().unwrap();
    let mut third = log.acquire(10).unwrap();
    assert_eq!(
        third
            .reconcile_local_standby(&mut replica, 0, deadline())
            .unwrap(),
        2
    );
    assert_eq!(log.read(2).unwrap(), Some(pending.clone()));
    let mut future = pending.clone();
    future.epoch = third.epoch();
    future.shard_seq = 3;
    future.command_id = 3;
    third.prepare(&future).unwrap();
    replica
        .append_batch(std::slice::from_ref(&future), std::time::Instant::now())
        .unwrap();
    let mut absent_intent = future.clone();
    absent_intent.shard_seq = 4;
    absent_intent.command_id = 4;
    replica
        .append_batch(
            std::slice::from_ref(&absent_intent),
            std::time::Instant::now(),
        )
        .unwrap();
    third.revoke().unwrap();
    let mut fourth = log.acquire(10).unwrap();
    assert!(
        fourth
            .reconcile_local_standby(&mut replica, 0, deadline())
            .is_err()
    );
    assert_eq!(log.committed().unwrap(), 2);
    assert!(fourth.is_locally_invalidated());
    fourth.revoke().unwrap();
    let mut unbound = log.acquire(10).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            use std::io::Read;
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                deadline(),
            )
            .unwrap();
            let mut byte = [0_u8; 1];
            assert!(!matches!(stream.read(&mut byte), Ok(count) if count > 0));
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            deadline(),
        )
        .unwrap();
        assert_eq!(
            unbound
                .reconcile_authenticated_remote_prefix(&mut stream, 0, deadline())
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert!(unbound.is_locally_invalidated());
        drop(stream);
        server.join().unwrap();
    });
    assert_eq!(log.committed().unwrap(), 2);
    unbound.revoke().unwrap();
    drop(replica);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 91, 92)
            .unwrap(),
        vec![record, pending, future, absent_intent]
    );
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
            .starts_with("tide-owner-barrier-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn durable_prefix_survives_reacquisition_and_rejects_old_writer() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/fenced-log/{}", std::process::id());
    let log = FencedLog::new(client, &prefix, ClusterId(31), ShardId(32)).unwrap();
    let mut old = log.acquire(10).unwrap();
    assert!(log.acquire(10).is_err());
    let make = |epoch, seq, size| WalRecord {
        cluster_id: 31,
        shard_id: 32,
        flags: 0,
        epoch,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![0x5a; size],
    };
    assert_eq!(old.epoch(), 1);
    assert!(old.append(&make(1, 2, 2)).is_err());
    let large = make(1, 1, 300_000);
    assert_eq!(old.append(&large).unwrap(), 1);
    assert_eq!(log.committed().unwrap(), 1);
    assert_eq!(log.read(1).unwrap(), Some(large));
    old.revoke().unwrap();
    let mut next = log.acquire(10).unwrap();
    assert_eq!(next.epoch(), 2);
    assert!(old.append(&make(1, 2, 4)).is_err());
    let second = make(2, 2, 4);
    assert_eq!(next.append(&second).unwrap(), 2);
    assert_eq!(log.read(2).unwrap(), Some(second));
    assert_eq!(log.committed().unwrap(), 2);
    next.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn remote_lease_revocation_fences_old_writer_and_corruption_fails_closed() {
    use matching_control::KvVersion;
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let prefix = format!("/tide-test/fenced-log-revoke/{}", std::process::id());
    let log = FencedLog::new(client.clone(), &prefix, ClusterId(41), ShardId(42)).unwrap();
    let mut old = log.acquire(10).unwrap();
    let owner = format!("{prefix}/cluster/0029/shard/002a/fenced-log/owner");
    let lease = client.get(owner.as_bytes()).unwrap().unwrap().1;
    let lease = i64::from_be_bytes(lease.try_into().unwrap());
    let endpoint = endpoints.split(',').next().unwrap();
    ureq::post(&format!("{endpoint}/v3/lease/revoke"))
        .set("Content-Type", "application/json")
        .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
        .unwrap();
    let make = |epoch, seq| WalRecord {
        cluster_id: 41,
        shard_id: 42,
        flags: 0,
        epoch,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![7; 20],
    };
    let mut next = log.acquire(10).unwrap();
    assert_eq!(next.epoch(), 2);
    assert_eq!(log.committed().unwrap(), 0);
    assert_eq!(next.append(&make(2, 1)).unwrap(), 1);
    assert!(old.append(&make(1, 1)).is_err());
    assert!(old.is_locally_invalidated());
    let chunk = format!(
        "{prefix}/cluster/0029/shard/002a/fenced-log/stage/{:016x}/{:016x}/{:08x}",
        2, 1, 0
    );
    let (revision, _) = client.get(chunk.as_bytes()).unwrap().unwrap();
    assert!(
        client
            .compare_and_swap(chunk.as_bytes(), KvVersion::Revision(revision), b"corrupt")
            .unwrap()
            .is_some()
    );
    assert!(log.read(1).is_err());
    next.revoke().unwrap();
    assert!(log.acquire(10).is_err());
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn maximum_wal_payload_is_not_reduced_by_chunking() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(5), true).unwrap();
    let prefix = format!("/tide-test/fenced-log-max/{}", std::process::id());
    let log = FencedLog::new(client, &prefix, ClusterId(51), ShardId(52)).unwrap();
    let mut writer = log.acquire(120).unwrap();
    let record = WalRecord {
        cluster_id: 51,
        shard_id: 52,
        flags: 0,
        epoch: 1,
        shard_seq: 1,
        engine_time: 0,
        command_id: 1,
        payload: vec![0xa5; 16 * 1024 * 1024],
    };
    assert_eq!(writer.append(&record).unwrap(), 1);
    assert_eq!(log.read(1).unwrap(), Some(record));
    writer.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn abandoned_staged_chunk_does_not_become_a_committed_record() {
    use matching_control::KvVersion;
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/fenced-log-tail/{}", std::process::id());
    let log = FencedLog::new(client.clone(), &prefix, ClusterId(61), ShardId(62)).unwrap();
    let mut first = log.acquire(10).unwrap();
    let staged = format!(
        "{prefix}/cluster/003d/shard/003e/fenced-log/stage/{:016x}/{:016x}/{:08x}",
        1, 1, 0
    );
    assert!(
        client
            .compare_and_swap(staged.as_bytes(), KvVersion::Absent, b"partial")
            .unwrap()
            .is_some()
    );
    let make = |epoch| WalRecord {
        cluster_id: 61,
        shard_id: 62,
        flags: 0,
        epoch,
        shard_seq: 1,
        engine_time: 0,
        command_id: 1,
        payload: vec![1; 4],
    };
    assert!(first.append(&make(1)).is_err());
    assert_eq!(log.committed().unwrap(), 0);
    assert_eq!(log.read(1).unwrap(), None);
    first.revoke().unwrap();
    let mut next = log.acquire(10).unwrap();
    assert_eq!(next.epoch(), 2);
    assert_eq!(next.append(&make(2)).unwrap(), 1);
    assert_eq!(log.read(1).unwrap(), Some(make(2)));
    next.revoke().unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn prepared_record_survives_lease_loss_without_advancing_commit() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/fenced-log-intent/{}", std::process::id());
    let log = FencedLog::new(client, &prefix, ClusterId(81), ShardId(82)).unwrap();
    let mut first = log.acquire(10).unwrap();
    let record = WalRecord {
        cluster_id: 81,
        shard_id: 82,
        flags: 0,
        epoch: 1,
        shard_seq: 1,
        engine_time: 0,
        command_id: 1,
        payload: vec![4; 300_000],
    };
    first.prepare(&record).unwrap();
    let second = WalRecord {
        shard_seq: 2,
        command_id: 2,
        payload: vec![8; 4],
        ..record.clone()
    };
    first.prepare(&second).unwrap();
    assert!(first.commit_prepared(&second).is_err());
    assert_eq!(log.committed().unwrap(), 0);
    assert_eq!(log.read(1).unwrap(), None);
    assert_eq!(log.read_prepared(1, 1).unwrap(), Some(record.clone()));
    assert_eq!(log.read_prepared(1, 2).unwrap(), Some(second));
    first.revoke().unwrap();
    assert!(first.commit_prepared(&record).is_err());
    let mut next = log.acquire(10).unwrap();
    assert_eq!(next.epoch(), 2);
    assert_eq!(log.committed().unwrap(), 0);
    assert_eq!(log.read_prepared(1, 1).unwrap(), Some(record));
    next.revoke().unwrap();
}
