//! 在真实本机连接上验证有界复制帧与备节点持久确认。
use matching_control::{receive_replication_batch, send_replication_batch};
use matching_persistence::{
    DurabilityConfig, DurabilityMode, DurableBatcher, FileManifestStorage, ManifestManager,
    Profile, RegisteredAppender, ReplicaDurableAck, ReplicationMode, SegmentWriter, StandbyReplica,
    WalRecord,
};
use std::time::Duration;

fn record(seq: u64) -> WalRecord {
    WalRecord {
        cluster_id: 1,
        shard_id: 2,
        flags: 0,
        epoch: 3,
        shard_seq: seq,
        engine_time: 0,
        command_id: u128::from(seq),
        payload: vec![9; 64],
    }
}

#[test]
fn fence_frames_reject_bad_identity_corruption_and_forged_ack() {
    use matching_control::{
        EtcdClient, FencedLog, ReplicaWriterIdentity, receive_replica_fence, send_replica_fence,
    };
    use matching_domain::{ClusterId, ShardId};
    use matching_persistence::{DurableAppender, FencedStandbyReplica, ReplicaEpochStore};
    use std::io::{Read, Write};
    struct Appender;
    impl DurableAppender for Appender {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Duplex {
        /// 供接收端读取的模拟帧数据。
        input: std::io::Cursor<Vec<u8>>,
        /// 记录发送端写出的帧数据。
        output: Vec<u8>,
    }
    impl Read for Duplex {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(out)
        }
    }
    impl Write for Duplex {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl matching_control::ReplicationStream for Duplex {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("tide-fence-wire-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(Appender, config, 3).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let epochs = ReplicaEpochStore::create(root.join("epoch"), 1, 2, 3, 0).unwrap();
    let mut standby = FencedStandbyReplica::new(replica, epochs).unwrap();
    standby.activate(3, |_, _, _| Ok(())).unwrap();
    standby.confirm_reconciled(0, |_, _, _| Ok(())).unwrap();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", probe.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = probe.accept().unwrap();
        let mut header = Vec::new();
        let mut byte = [0_u8; 1];
        while !header.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
            assert!(header.len() <= 4096);
        }
        let length: usize = std::str::from_utf8(&header)
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .unwrap()
            .1
            .trim()
            .parse()
            .unwrap();
        assert!(length <= 4096);
        let mut body = vec![0_u8; length];
        stream.read_exact(&mut body).unwrap();
        let body = b"{\"header\":{\"cluster_id\":\"1\",\"revision\":\"1\"}}";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
    });
    let client = EtcdClient::new(vec![endpoint], Duration::from_secs(2), true).unwrap();
    server.join().unwrap();
    let log = FencedLog::new(client, "/tide-test/fence-input", ClusterId(1), ShardId(2)).unwrap();
    let identity = ReplicaWriterIdentity {
        cluster_id: 1,
        shard_id: 2,
        epoch: 3,
        lease_id: 4,
    };
    let mut request = Vec::from(&b"RFE1"[..]);
    request.extend_from_slice(&1_u16.to_be_bytes());
    request.extend_from_slice(&2_u16.to_be_bytes());
    request.extend_from_slice(&3_u64.to_be_bytes());
    request.extend_from_slice(&4_i64.to_be_bytes());
    request.extend_from_slice(&matching_persistence::crc32c(&request).to_be_bytes());
    let deadline = || std::time::Instant::now() + Duration::from_secs(1);
    let mut wrong = identity;
    wrong.lease_id = 5;
    let mut stream = Duplex {
        input: std::io::Cursor::new(request.clone()),
        output: vec![],
    };
    assert_eq!(
        receive_replica_fence(&mut stream, &log, &mut standby, wrong, deadline())
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert!(stream.output.is_empty());
    request[24] ^= 1;
    let mut stream = Duplex {
        input: std::io::Cursor::new(request),
        output: vec![],
    };
    assert!(receive_replica_fence(&mut stream, &log, &mut standby, identity, deadline()).is_err());
    assert!(stream.output.is_empty());
    standby
        .append_batch(&[record(1)], std::time::Instant::now())
        .unwrap();
    let mut ack = Vec::from(&b"RFA1"[..]);
    ack.extend_from_slice(&1_u16.to_be_bytes());
    ack.extend_from_slice(&2_u16.to_be_bytes());
    ack.extend_from_slice(&3_u64.to_be_bytes());
    ack.extend_from_slice(&5_i64.to_be_bytes());
    ack.extend_from_slice(&0_u64.to_be_bytes());
    ack.extend_from_slice(&matching_persistence::crc32c(&ack).to_be_bytes());
    let mut stream = Duplex {
        input: std::io::Cursor::new(ack),
        output: vec![],
    };
    assert!(send_replica_fence(&mut stream, identity, deadline()).is_err());
    let mut stream = Duplex {
        input: std::io::Cursor::new(vec![0; 10]),
        output: vec![],
    };
    assert!(send_replica_fence(&mut stream, identity, deadline()).is_err());
    drop(standby);
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
            .starts_with("tide-fence-wire-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn fence_response_slow_drip_obeys_total_deadline() {
    use matching_control::{ReplicaWriterIdentity, ReplicationStream, send_replica_fence};
    use std::io::{Read, Write};
    struct Slow;
    impl Read for Slow {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            std::thread::sleep(Duration::from_millis(10));
            out[0] = 0;
            Ok(1)
        }
    }
    impl Write for Slow {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl ReplicationStream for Slow {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
    }
    let identity = ReplicaWriterIdentity {
        cluster_id: 1,
        shard_id: 2,
        epoch: 3,
        lease_id: 4,
    };
    let start = std::time::Instant::now();
    assert!(send_replica_fence(&mut Slow, identity, start + Duration::from_millis(35)).is_err());
    assert!(start.elapsed() < Duration::from_millis(200));
}

#[test]
fn tcp_ack_follows_real_sync_and_gap_closes_connection() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-wire-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let manifest = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        manifest,
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 8,
            max_delay_micros: 1000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let epochs =
        matching_persistence::ReplicaEpochStore::create(root.join("epoch"), 1, 2, 3, 0).unwrap();
    let mut standby = matching_persistence::FencedStandbyReplica::new(replica, epochs).unwrap();
    standby.activate(3, |_, _, _| Ok(())).unwrap();
    standby.confirm_reconciled(0, |_, _, _| Ok(())).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let ack = receive_replication_batch(
                &mut stream,
                &mut standby,
                &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
            assert_eq!(ack.through_seq, 2);
            assert!(
                receive_replication_batch(
                    &mut stream,
                    &mut standby,
                    &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(5)
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let ack =
            send_replication_batch(&mut stream, &[record(1), record(2)], 32 * 1024 * 1024).unwrap();
        assert_eq!(
            ack,
            ReplicaDurableAck {
                cluster_id: 1,
                shard_id: 2,
                epoch: 3,
                first_seq: 1,
                through_seq: 2
            }
        );
        assert!(send_replication_batch(&mut stream, &[record(4)], 32 * 1024 * 1024).is_err());
        server.join().unwrap();
    });
    drop(standby);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(1), record(2)]
    );
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-wire-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn network_ack_waits_for_standby_sync_to_finish() {
    use matching_persistence::DurableAppender;
    use std::sync::mpsc::sync_channel;
    struct BlockingAppender {
        /// 通知测试线程备节点同步已进入等待。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 等待测试线程放行备节点同步。
        release: std::sync::mpsc::Receiver<()>,
    }
    impl DurableAppender for BlockingAppender {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.entered.send(()).unwrap();
            self.release
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| std::io::Error::other("同步闸门超时"))
        }
    }
    let (entered, entered_rx) = sync_channel(1);
    let (release, release_rx) = sync_channel(1);
    let (result_tx, result_rx) = sync_channel(1);
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(
        BlockingAppender {
            entered,
            release: release_rx,
        },
        config,
        3,
    )
    .unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            receive_replication_batch(
                &mut stream,
                &mut standby,
                &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
        });
        let client = scope.spawn(move || {
            let mut stream = std::net::TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let result = send_replication_batch(&mut stream, &[record(1)], 32 * 1024 * 1024);
            result_tx.send(result).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            result_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        assert_eq!(
            result_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .through_seq,
            1
        );
        client.join().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn malformed_or_truncated_frames_never_reach_standby_wal() {
    use matching_persistence::DurableAppender;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    /// 记录持久化探针调用次数的共享计数器。
    struct Probe(Arc<AtomicUsize>);
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let writes = Arc::new(AtomicUsize::new(0));
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(Probe(writes.clone()), config, 3).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let max_frame = 32 * 1024 * 1024;
    let mut oversized = b"RPL1".to_vec();
    oversized.extend_from_slice(&u32::try_from(max_frame).unwrap().to_be_bytes());
    let mut bad_crc = b"RPL1".to_vec();
    bad_crc.extend_from_slice(&8_u32.to_be_bytes());
    bad_crc.extend_from_slice(&0_u32.to_be_bytes());
    bad_crc.extend_from_slice(&0_u32.to_be_bytes());
    let mut truncated = b"RPL1".to_vec();
    truncated.extend_from_slice(&16_u32.to_be_bytes());
    truncated.extend_from_slice(&[1, 2]);
    for bytes in [oversized, bad_crc, truncated] {
        let mut stream = std::io::Cursor::new(bytes);
        assert!(
            receive_replication_batch(
                &mut stream,
                &mut standby,
                &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                8,
                max_frame,
                Duration::from_secs(5)
            )
            .is_err()
        );
    }
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[test]
fn missing_replication_intent_is_rejected_before_standby_wal() {
    use matching_persistence::DurableAppender;
    use std::io::{Read, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    /// 记录发送端输出的帧字节。
    struct Capture(Vec<u8>);
    impl Read for Capture {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Ok(0)
        }
    }
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    /// 记录持久化探针调用次数的共享计数器。
    struct Probe(Arc<AtomicUsize>);
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut capture = Capture(Vec::new());
    assert!(send_replication_batch(&mut capture, &[record(1)], 32 * 1024 * 1024).is_err());
    let writes = Arc::new(AtomicUsize::new(0));
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(Probe(writes.clone()), config, 3).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let mut stream = std::io::Cursor::new(capture.0);
    let mut reject =
        |_: &WalRecord, _: std::time::Instant| Err(std::io::Error::other("复制意图不存在"));
    assert!(
        receive_replication_batch(
            &mut stream,
            &mut standby,
            &mut reject,
            8,
            32 * 1024 * 1024,
            Duration::from_secs(2)
        )
        .is_err()
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[test]
fn sender_rejects_validly_checksummed_ack_for_wrong_prefix() {
    use std::io::{Read, Write};
    struct FakeWire {
        /// 供发送端读取的模拟确认帧。
        reply: std::io::Cursor<Vec<u8>>,
    }
    impl Read for FakeWire {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.reply.read(out)
        }
    }
    impl Write for FakeWire {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    for (shard, through) in [(2_u16, 2_u64), (9_u16, 1_u64)] {
        let mut reply = [0_u8; 36];
        reply[..4].copy_from_slice(b"RPA1");
        reply[4..6].copy_from_slice(&1_u16.to_be_bytes());
        reply[6..8].copy_from_slice(&shard.to_be_bytes());
        reply[8..16].copy_from_slice(&3_u64.to_be_bytes());
        reply[16..24].copy_from_slice(&1_u64.to_be_bytes());
        reply[24..32].copy_from_slice(&through.to_be_bytes());
        let checksum = matching_persistence::crc32c(&reply[..32]);
        reply[32..].copy_from_slice(&checksum.to_be_bytes());
        let mut stream = FakeWire {
            reply: std::io::Cursor::new(reply.to_vec()),
        };
        assert!(send_replication_batch(&mut stream, &[record(1)], 32 * 1024 * 1024).is_err());
    }
}

#[test]
fn replication_frame_accepts_the_full_wal_payload_limit() {
    use std::io::{Read, Write};
    struct CountWire {
        /// 供发送端读取的模拟确认帧。
        reply: std::io::Cursor<Vec<u8>>,
        /// 记录写入模拟连接的字节数。
        written: usize,
    }
    impl Read for CountWire {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.reply.read(out)
        }
    }
    impl Write for CountWire {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.written = self
                .written
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("测试计数耗尽"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut reply = [0_u8; 36];
    reply[..4].copy_from_slice(b"RPA1");
    reply[4..6].copy_from_slice(&1_u16.to_be_bytes());
    reply[6..8].copy_from_slice(&2_u16.to_be_bytes());
    reply[8..16].copy_from_slice(&3_u64.to_be_bytes());
    reply[16..24].copy_from_slice(&1_u64.to_be_bytes());
    reply[24..32].copy_from_slice(&1_u64.to_be_bytes());
    let checksum = matching_persistence::crc32c(&reply[..32]);
    reply[32..].copy_from_slice(&checksum.to_be_bytes());
    let mut stream = CountWire {
        reply: std::io::Cursor::new(reply.to_vec()),
        written: 0,
    };
    let mut max = record(1);
    max.payload = vec![9; 16 * 1024 * 1024];
    let frame_limit = 16 * 1024 * 1024 + 84;
    assert!(send_replication_batch(&mut stream, &[max.clone()], frame_limit - 1).is_err());
    assert_eq!(stream.written, 0);
    assert_eq!(
        send_replication_batch(&mut stream, &[max], frame_limit)
            .unwrap()
            .through_seq,
        1
    );
    assert_eq!(stream.written, frame_limit);
}

#[test]
fn lost_ack_does_not_erase_a_synced_standby_prefix() {
    use std::io::{Read, Write};
    /// 记录发送端输出的帧字节。
    struct Capture(Vec<u8>);
    impl Read for Capture {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Ok(0)
        }
    }
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    /// 保存模拟确认帧的输入游标。
    struct LostAck(std::io::Cursor<Vec<u8>>);
    impl Read for LostAck {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(bytes)
        }
    }
    impl Write for LostAck {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "模拟确认丢失",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut capture = Capture(Vec::new());
    assert!(send_replication_batch(&mut capture, &[record(1)], 32 * 1024 * 1024).is_err());
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-ack-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let manifest = ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap();
    let appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        manifest,
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let mut stream = LostAck(std::io::Cursor::new(capture.0));
    assert!(
        receive_replication_batch(
            &mut stream,
            &mut standby,
            &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
            8,
            32 * 1024 * 1024,
            Duration::from_secs(5)
        )
        .is_err()
    );
    drop(standby);
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(1)]
    );
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-ack-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn primary_commits_only_after_local_and_standby_wal_are_durable() {
    use matching_control::{ReplicatedAppender, ReplicationStream};
    use std::io::{Read, Write};
    struct OffThread {
        /// 检查访问线程的实际 TCP 连接。
        inner: std::net::TcpStream,
        /// 调用者线程身份，用于验证网络操作在线程外执行。
        caller: std::thread::ThreadId,
    }
    impl Read for OffThread {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            assert_ne!(std::thread::current().id(), self.caller);
            self.inner.read(bytes)
        }
    }
    impl Write for OffThread {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert_ne!(std::thread::current().id(), self.caller);
            self.inner.write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            assert_ne!(std::thread::current().id(), self.caller);
            self.inner.flush()
        }
    }
    impl ReplicationStream for OffThread {
        fn set_io_timeout(&mut self, timeout: Duration) -> std::io::Result<()> {
            self.inner.set_read_timeout(Some(timeout))?;
            self.inner.set_write_timeout(Some(timeout))
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tide-replica-primary-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    for name in [
        "primary-wal",
        "primary-manifest",
        "standby-wal",
        "standby-manifest",
    ] {
        std::fs::create_dir(root.join(name)).unwrap();
    }
    let open = |wal: &str, manifest: &str| {
        RegisteredAppender::new(
            SegmentWriter::new(root.join(wal), 4096).unwrap(),
            ManifestManager::<FileManifestStorage>::open(root.join(manifest)).unwrap(),
        )
        .unwrap()
    };
    let local_config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::GroupCommit {
            max_batch: 8,
            max_delay_micros: 1_000_000,
        },
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let standby_batcher =
        DurableBatcher::new_with_config(open("standby-wal", "standby-manifest"), local_config, 3)
            .unwrap();
    let mut standby = StandbyReplica::new(standby_batcher, 1, 2, 3, 8).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            receive_replication_batch(
                &mut stream,
                &mut standby,
                &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
        });
        let stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let appender = ReplicatedAppender::new(
            open("primary-wal", "primary-manifest"),
            OffThread {
                inner: stream,
                caller: std::thread::current().id(),
            },
            |record: &WalRecord, _: std::time::Instant| Ok(record.shard_seq),
            32 * 1024 * 1024,
            Duration::from_secs(5),
        )
        .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Production,
            mode: local_config.mode,
            replication: ReplicationMode::StandbyDurable,
            crash_recovery_required: true,
            ha_enabled: true,
            allow_data_loss_on_failover: false,
        };
        let mut primary = DurableBatcher::new_with_config(appender, config, 3).unwrap();
        primary.set_standby_available(true);
        primary
            .append_at(&record(1), std::time::Instant::now())
            .unwrap();
        primary
            .append_at(&record(2), std::time::Instant::now())
            .unwrap();
        assert_eq!(primary.committed(), 0);
        primary.flush().unwrap();
        assert_eq!(primary.committed(), 2);
        drop(primary);
        server.join().unwrap();
    });
    drop(standby);
    let expected = vec![record(1), record(2)];
    assert_eq!(
        matching_persistence::recover_local(
            root.join("primary-wal"),
            root.join("primary-manifest"),
            0,
            1,
            2
        )
        .unwrap(),
        expected
    );
    assert_eq!(
        matching_persistence::recover_local(
            root.join("standby-wal"),
            root.join("standby-manifest"),
            0,
            1,
            2
        )
        .unwrap(),
        expected
    );
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-primary-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn standby_disconnect_never_advances_primary_commit() {
    use matching_control::ReplicatedAppender;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-fail-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    let appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap(),
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        drop(stream);
    });
    let stream = std::net::TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let appender = ReplicatedAppender::new(
        appender,
        stream,
        |record: &WalRecord, _: std::time::Instant| Ok(record.shard_seq),
        32 * 1024 * 1024,
        Duration::from_secs(5),
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::StandbyDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    let mut primary = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    primary.set_standby_available(true);
    assert!(
        primary
            .append_at(&record(1), std::time::Instant::now())
            .is_err()
    );
    assert_eq!(primary.committed(), 0);
    assert!(
        primary
            .append_at(&record(2), std::time::Instant::now())
            .is_err()
    );
    drop(primary);
    server.join().unwrap();
    assert_eq!(
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap(),
        vec![record(1)]
    );
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-fail-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
fn replication_worker_refuses_stream_without_io_deadlines() {
    use matching_control::{ReplicatedAppender, ReplicationStream};
    use matching_persistence::DurableAppender;
    use std::io::{Read, Write};
    struct Local;
    impl DurableAppender for Local {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            panic!("不应写入")
        }
        fn sync(&mut self) -> std::io::Result<()> {
            panic!("不应同步")
        }
    }
    struct NoTimeout;
    impl Read for NoTimeout {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("不应读取")
        }
    }
    impl Write for NoTimeout {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            panic!("不应发送")
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("不应刷新")
        }
    }
    impl ReplicationStream for NoTimeout {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Err(std::io::Error::other("无法配置期限"))
        }
    }
    assert!(
        ReplicatedAppender::new(
            Local,
            NoTimeout,
            |record: &WalRecord, _: std::time::Instant| Ok(record.shard_seq),
            32 * 1024 * 1024,
            Duration::from_secs(1)
        )
        .is_err()
    );
}

#[test]
fn failed_authority_fence_blocks_commit_after_standby_ack() {
    use matching_control::ReplicatedAppender;
    use matching_persistence::DurableAppender;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    /// 记录持久化探针调用次数的共享计数器。
    struct Probe(Arc<AtomicUsize>);
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let local_syncs = Arc::new(AtomicUsize::new(0));
    let standby_syncs = Arc::new(AtomicUsize::new(0));
    let fence_calls = Arc::new(AtomicUsize::new(0));
    let local_config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher =
        DurableBatcher::new_with_config(Probe(standby_syncs.clone()), local_config, 3).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, 3, 8).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            receive_replication_batch(
                &mut stream,
                &mut standby,
                &mut |_: &WalRecord, _: std::time::Instant| Ok(()),
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
        });
        let stream = std::net::TcpStream::connect(address).unwrap();
        let calls = fence_calls.clone();
        let fence = move |_: &WalRecord, _: std::time::Instant| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("写权已失效"))
        };
        let appender = ReplicatedAppender::new(
            Probe(local_syncs.clone()),
            stream,
            fence,
            32 * 1024 * 1024,
            Duration::from_secs(5),
        )
        .unwrap();
        let config = DurabilityConfig {
            profile: Profile::Production,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::StandbyDurable,
            crash_recovery_required: true,
            ha_enabled: true,
            allow_data_loss_on_failover: false,
        };
        let mut primary = DurableBatcher::new_with_config(appender, config, 3).unwrap();
        primary.set_standby_available(true);
        assert!(
            primary
                .append_at(&record(1), std::time::Instant::now())
                .is_err()
        );
        assert_eq!(primary.committed(), 0);
        assert!(
            primary
                .append_at(&record(2), std::time::Instant::now())
                .is_err()
        );
        drop(primary);
        server.join().unwrap();
    });
    assert_eq!(local_syncs.load(Ordering::SeqCst), 1);
    assert_eq!(standby_syncs.load(Ordering::SeqCst), 1);
    assert_eq!(fence_calls.load(Ordering::SeqCst), 1);
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn real_lease_revocation_prevents_primary_commit_after_standby_ack() {
    use matching_control::{EtcdClient, FencedLog, ReplicatedAppender};
    use matching_domain::{ClusterId, ShardId};
    use matching_persistence::DurableAppender;
    struct StandbyProbe<A> {
        /// 被包装的备节点持久化组件。
        inner: A,
        /// 记录备节点同步调用次数。
        syncs: usize,
        /// 通知测试线程备节点同步已进入等待。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 等待测试线程放行备节点同步。
        release: std::sync::mpsc::Receiver<()>,
    }
    impl<A: DurableAppender> DurableAppender for StandbyProbe<A> {
        fn would_rotate(&self, record: &WalRecord) -> std::io::Result<bool> {
            self.inner.would_rotate(record)
        }
        fn append(&mut self, record: &WalRecord) -> std::io::Result<()> {
            self.inner.append(record)
        }
        fn sync(&mut self) -> std::io::Result<()> {
            self.syncs = self.syncs.checked_add(1).unwrap();
            if self.syncs == 2 {
                self.entered.send(()).unwrap();
                self.release
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| std::io::Error::other("同步闸门超时"))?;
            }
            self.inner.sync()
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("tide-replica-fence-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("wal")).unwrap();
    std::fs::create_dir(root.join("manifest")).unwrap();
    std::fs::create_dir(root.join("primary-wal")).unwrap();
    std::fs::create_dir(root.join("primary-manifest")).unwrap();
    let primary_appender = RegisteredAppender::new(
        SegmentWriter::new(root.join("primary-wal"), 4096).unwrap(),
        ManifestManager::<FileManifestStorage>::open(root.join("primary-manifest")).unwrap(),
    )
    .unwrap();
    let inner = RegisteredAppender::new(
        SegmentWriter::new(root.join("wal"), 4096).unwrap(),
        ManifestManager::<FileManifestStorage>::open(root.join("manifest")).unwrap(),
    )
    .unwrap();
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let client = EtcdClient::new(
        endpoints.split(',').map(str::to_owned).collect(),
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let prefix = format!(
        "/tide-test/replicated-primary/{}-{stamp}",
        std::process::id()
    );
    let log = FencedLog::new(client.clone(), &prefix, ClusterId(1), ShardId(2)).unwrap();
    let writer = log
        .acquire_bound(2, include_bytes!("fixtures/test-client.der"))
        .unwrap();
    let identity = writer.replica_identity();
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
    assert_eq!(writer.epoch(), 1);
    let local_config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let batcher = DurableBatcher::new_with_config(
        StandbyProbe {
            inner,
            syncs: 0,
            entered,
            release: release_rx,
        },
        local_config,
        1,
    )
    .unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 1, 8).unwrap();
    let epochs =
        matching_persistence::ReplicaEpochStore::create(root.join("epoch"), 1, 2, 1, 0).unwrap();
    let mut standby = matching_persistence::FencedStandbyReplica::new(replica, epochs).unwrap();
    matching_control::activate_replica_writer(
        &log,
        &mut standby,
        identity,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .unwrap();
    standby
        .confirm_reconciled(0, |_, _, _| {
            log.verify_bound_replica_writer(
                identity,
                client_cert,
                std::time::Instant::now() + Duration::from_secs(5),
            )
        })
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
            matching_control::receive_authenticated_replication_batch(
                &mut stream,
                &log,
                &mut standby,
                identity,
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
            matching_control::receive_authenticated_replication_batch(
                &mut stream,
                &log,
                &mut standby,
                identity,
                8,
                32 * 1024 * 1024,
                Duration::from_secs(5),
            )
            .unwrap();
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let appender = ReplicatedAppender::new_authenticated(
            primary_appender,
            stream,
            writer,
            32 * 1024 * 1024,
            Duration::from_secs(5),
        )
        .unwrap();
        let health = appender.health_handle();
        let config = DurabilityConfig {
            profile: Profile::Production,
            mode: DurabilityMode::FsyncEach,
            replication: ReplicationMode::StandbyDurable,
            crash_recovery_required: true,
            ha_enabled: true,
            allow_data_loss_on_failover: false,
        };
        let mut primary = DurableBatcher::new_with_config(appender, config, 1).unwrap();
        primary.set_standby_available(true);
        let make = |seq| {
            let mut value = record(seq);
            value.epoch = 1;
            value
        };
        primary
            .append_at(&make(1), std::time::Instant::now())
            .unwrap();
        assert_eq!(primary.committed(), 1);
        primary.checkpoint_digest(1, [1; 32], [2; 32]).unwrap();
        let checkpoint = log.read_digest_checkpoint(1).unwrap().unwrap();
        assert_eq!(
            (
                checkpoint.epoch,
                checkpoint.state_digest,
                checkpoint.event_hash
            ),
            (1, [1; 32], [2; 32])
        );
        std::thread::sleep(Duration::from_secs(4));
        assert!(health.ready());
        assert!(log.acquire(2).is_err());
        let pending = scope.spawn(move || {
            let result = primary.append_at(&make(2), std::time::Instant::now());
            (primary, result)
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let owner = format!("{prefix}/cluster/0001/shard/0002/fenced-log/owner");
        let lease = client.get(owner.as_bytes()).unwrap().unwrap().1;
        let lease = i64::from_be_bytes(lease.try_into().unwrap());
        let endpoint = endpoints.split(',').next().unwrap();
        ureq::post(&format!("{endpoint}/v3/lease/revoke"))
            .set("Content-Type", "application/json")
            .send_string(&format!("{{\"ID\":\"{lease}\"}}"))
            .unwrap();
        release.send(()).unwrap();
        let (primary, result) = pending.join().unwrap();
        assert!(result.is_err());
        assert_eq!(primary.committed(), 1);
        assert_eq!(log.committed().unwrap(), 1);
        assert_eq!(log.read(1).unwrap(), Some(make(1)));
        assert_eq!(log.read_prepared(1, 2).unwrap(), Some(make(2)));
        drop(primary);
        server.join().unwrap();
    });
    drop(standby);
    let primary_recovered = matching_persistence::recover_local(
        root.join("primary-wal"),
        root.join("primary-manifest"),
        0,
        1,
        2,
    )
    .unwrap();
    assert_eq!(
        primary_recovered
            .iter()
            .map(|record| record.shard_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let recovered =
        matching_persistence::recover_local(root.join("wal"), root.join("manifest"), 0, 1, 2)
            .unwrap();
    assert_eq!(primary_recovered, recovered);
    assert_eq!(
        recovered
            .iter()
            .map(|record| record.shard_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(log.committed().unwrap(), 1);
    let mut next = log.acquire_bound(10, client_cert).unwrap();
    assert_eq!(next.epoch(), 2);
    let manager =
        ManifestManager::<FileManifestStorage>::open_recovery(root.join("manifest")).unwrap();
    let appender =
        RegisteredAppender::recover_local(root.join("wal"), 4096, manager, 0, 1, 2).unwrap();
    let batcher = DurableBatcher::resume_local(appender, local_config, 1).unwrap();
    let replica = StandbyReplica::new(batcher, 1, 2, 1, 8).unwrap();
    let epochs = matching_persistence::ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
    assert_eq!(epochs.state().epoch, 1);
    let mut replica = matching_persistence::FencedStandbyReplica::new(replica, epochs).unwrap();
    let identity = next.replica_identity();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = matching_control::TlsReplicaStream::accept(
                socket,
                server_config.clone(),
                client_cert,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
            matching_control::serve_authenticated_fenced_prefix(
                &mut stream,
                &log,
                &mut replica,
                identity,
                0,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let mut stream = matching_control::TlsReplicaStream::connect(
            socket,
            client_config.clone(),
            ServerName::try_from("server.test".to_string()).unwrap(),
            server_cert,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            next.reconcile_authenticated_remote_prefix(
                &mut stream,
                0,
                std::time::Instant::now() + Duration::from_secs(5)
            )
            .unwrap(),
            2
        );
        server.join().unwrap();
    });
    assert_eq!(log.committed().unwrap(), 2);
    assert_eq!(log.read(2).unwrap(), Some(recovered[1].clone()));
    next.revoke().unwrap();
    drop(replica);
    let epochs = matching_persistence::ReplicaEpochStore::open(root.join("epoch"), 1, 2).unwrap();
    assert_eq!((epochs.state().epoch, epochs.state().durable_seq), (2, 2));
    drop(epochs);
    let target = root.canonicalize().unwrap();
    let parent = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("tide-replica-fence-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn standby_rejects_wal_that_differs_from_real_authority_intent() {
    use matching_control::{EtcdClient, FencedLog};
    use matching_domain::{ClusterId, ShardId};
    use matching_persistence::DurableAppender;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    /// 记录持久化探针调用次数的共享计数器。
    struct Probe(Arc<AtomicUsize>);
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let client = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let prefix = format!("/tide-test/replica-intent/{}", std::process::id());
    let log = FencedLog::new(client, &prefix, ClusterId(1), ShardId(2)).unwrap();
    let mut writer = log.acquire(10).unwrap();
    let mut intended = record(1);
    intended.epoch = writer.epoch();
    writer.prepare(&intended).unwrap();
    let mut altered = intended.clone();
    altered.payload[0] ^= 1;
    let writes = Arc::new(AtomicUsize::new(0));
    let config = DurabilityConfig {
        profile: Profile::Development,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::LocalDurable,
        crash_recovery_required: true,
        ha_enabled: false,
        allow_data_loss_on_failover: false,
    };
    let batcher =
        DurableBatcher::new_with_config(Probe(writes.clone()), config, writer.epoch()).unwrap();
    let mut standby = StandbyReplica::new(batcher, 1, 2, writer.epoch(), 8).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert!(
                receive_replication_batch(
                    &mut stream,
                    &mut standby,
                    &mut log.clone(),
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(5)
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert!(send_replication_batch(&mut stream, &[altered], 32 * 1024 * 1024).is_err());
        server.join().unwrap();
    });
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(log.committed().unwrap(), 0);
    writer.revoke().unwrap();
    let mut next = log.acquire(10).unwrap();
    assert_eq!(next.epoch(), 2);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert!(
                receive_replication_batch(
                    &mut stream,
                    &mut standby,
                    &mut log.clone(),
                    8,
                    32 * 1024 * 1024,
                    Duration::from_secs(5)
                )
                .is_err()
            );
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert!(send_replication_batch(&mut stream, &[intended], 32 * 1024 * 1024).is_err());
        server.join().unwrap();
    });
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(log.committed().unwrap(), 0);
    next.revoke().unwrap();
}

#[test]
fn failed_idle_renewal_blocks_later_wal_append_and_marks_unready() {
    use matching_control::{ReplicaCommitFence, ReplicatedAppender, ReplicationStream};
    use matching_persistence::DurableAppender;
    use std::io::{Read, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    /// 记录持久化探针调用次数的共享计数器。
    struct Probe(Arc<AtomicUsize>);
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Idle;
    impl Read for Idle {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("不应读取")
        }
    }
    impl Write for Idle {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            panic!("不应发送")
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl ReplicationStream for Idle {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
    }
    /// 控制模拟租约失效的续约次数。
    struct Expiring(usize);
    impl ReplicaCommitFence for Expiring {
        fn prepare(&mut self, _: &[WalRecord], _: std::time::Instant) -> std::io::Result<()> {
            panic!("不应登记意图")
        }
        fn commit(&mut self, _: &WalRecord, _: std::time::Instant) -> std::io::Result<u64> {
            panic!("不应提交")
        }
        fn renewal_interval(&self) -> Option<Duration> {
            Some(Duration::from_millis(10))
        }
        fn renew(&mut self) -> std::io::Result<()> {
            self.0 += 1;
            if self.0 == 1 {
                Ok(())
            } else {
                Err(std::io::Error::other("租约失效"))
            }
        }
    }
    let writes = Arc::new(AtomicUsize::new(0));
    let appender = ReplicatedAppender::new(
        Probe(writes.clone()),
        Idle,
        Expiring(0),
        32 * 1024 * 1024,
        Duration::from_secs(1),
    )
    .unwrap();
    let health = appender.health_handle();
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::StandbyDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    batcher.set_standby_available(true);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while health.ready() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!health.ready());
    assert!(
        batcher
            .append_at(&record(1), std::time::Instant::now())
            .is_err()
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(batcher.committed(), 0);
}

#[test]
fn slow_ack_cannot_commit_after_the_batch_deadline() {
    use matching_control::{ReplicatedAppender, ReplicationStream};
    use matching_persistence::DurableAppender;
    use std::io::{Read, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Probe;
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    /// 缓慢返回模拟确认帧的输入游标。
    struct SlowAck(std::io::Cursor<Vec<u8>>);
    impl Read for SlowAck {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            std::thread::sleep(Duration::from_millis(10));
            let one = bytes.len().min(1);
            self.0.read(&mut bytes[..one])
        }
    }
    impl Write for SlowAck {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl ReplicationStream for SlowAck {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut reply = [0_u8; 36];
    reply[..4].copy_from_slice(b"RPA1");
    reply[4..6].copy_from_slice(&1_u16.to_be_bytes());
    reply[6..8].copy_from_slice(&2_u16.to_be_bytes());
    reply[8..16].copy_from_slice(&3_u64.to_be_bytes());
    reply[16..24].copy_from_slice(&1_u64.to_be_bytes());
    reply[24..32].copy_from_slice(&1_u64.to_be_bytes());
    let checksum = matching_persistence::crc32c(&reply[..32]);
    reply[32..].copy_from_slice(&checksum.to_be_bytes());
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let fence = move |record: &WalRecord, _: std::time::Instant| {
        capture.fetch_add(1, Ordering::SeqCst);
        Ok(record.shard_seq)
    };
    let appender = ReplicatedAppender::new(
        Probe,
        SlowAck(std::io::Cursor::new(reply.to_vec())),
        fence,
        32 * 1024 * 1024,
        Duration::from_millis(50),
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::FsyncEach,
        replication: ReplicationMode::StandbyDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    batcher.set_standby_available(true);
    assert!(
        batcher
            .append_at(&record(1), std::time::Instant::now())
            .is_err()
    );
    assert_eq!(batcher.committed(), 0);
    drop(batcher);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn timed_out_inflight_fence_does_not_start_the_next_commit() {
    use matching_control::{ReplicatedAppender, ReplicationStream};
    use matching_persistence::DurableAppender;
    use std::io::{Read, Write};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Probe;
    impl DurableAppender for Probe {
        fn would_rotate(&self, _: &WalRecord) -> std::io::Result<bool> {
            Ok(false)
        }
        fn append(&mut self, _: &WalRecord) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    /// 保存测试传输数据的内存游标。
    struct Wire(std::io::Cursor<Vec<u8>>);
    impl Read for Wire {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(bytes)
        }
    }
    impl Write for Wire {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl ReplicationStream for Wire {
        fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut reply = [0_u8; 36];
    reply[..4].copy_from_slice(b"RPA1");
    reply[4..6].copy_from_slice(&1_u16.to_be_bytes());
    reply[6..8].copy_from_slice(&2_u16.to_be_bytes());
    reply[8..16].copy_from_slice(&3_u64.to_be_bytes());
    reply[16..24].copy_from_slice(&1_u64.to_be_bytes());
    reply[24..32].copy_from_slice(&2_u64.to_be_bytes());
    let checksum = matching_persistence::crc32c(&reply[..32]);
    reply[32..].copy_from_slice(&checksum.to_be_bytes());
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let fence = move |record: &WalRecord, _: std::time::Instant| {
        capture.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(400));
        Ok(record.shard_seq)
    };
    let appender = ReplicatedAppender::new(
        Probe,
        Wire(std::io::Cursor::new(reply.to_vec())),
        fence,
        32 * 1024 * 1024,
        Duration::from_millis(200),
    )
    .unwrap();
    let config = DurabilityConfig {
        profile: Profile::Production,
        mode: DurabilityMode::GroupCommit {
            max_batch: 8,
            max_delay_micros: 1_000_000,
        },
        replication: ReplicationMode::StandbyDurable,
        crash_recovery_required: true,
        ha_enabled: true,
        allow_data_loss_on_failover: false,
    };
    let mut batcher = DurableBatcher::new_with_config(appender, config, 3).unwrap();
    batcher.set_standby_available(true);
    batcher
        .append_at(&record(1), std::time::Instant::now())
        .unwrap();
    batcher
        .append_at(&record(2), std::time::Instant::now())
        .unwrap();
    assert!(batcher.flush().is_err());
    assert_eq!(batcher.committed(), 0);
    drop(batcher);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
