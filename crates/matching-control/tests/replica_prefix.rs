//! 验证接任前缀传输按原序拒绝缺失或损坏记录。
use matching_control::{
    ReplicaPrefixBoundary, ReplicationStream, read_replica_prefix, send_replica_prefix,
};
use matching_persistence::WalRecord;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

struct Memory {
    /// 保存测试传输内容的内存游标。
    data: std::io::Cursor<Vec<u8>>,
}
impl Read for Memory {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.data.read(bytes)
    }
}
impl Write for Memory {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.data.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl ReplicationStream for Memory {
    fn set_io_timeout(&mut self, _: Duration) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn prefix_round_trip_rejects_gap_corruption_and_truncation() {
    let boundary = ReplicaPrefixBoundary {
        cluster_id: 1,
        shard_id: 2,
        snapshot_seq: 0,
        durable_seq: 2,
        writer_epoch: 4,
    };
    let records: Vec<_> = (1..=2)
        .map(|seq| WalRecord {
            cluster_id: 1,
            shard_id: 2,
            flags: 0,
            epoch: 3,
            shard_seq: seq,
            engine_time: 0,
            command_id: u128::from(seq),
            payload: vec![7; 8],
        })
        .collect();
    let deadline = || Instant::now() + Duration::from_secs(2);
    let mut stream = Memory {
        data: std::io::Cursor::new(Vec::new()),
    };
    send_replica_prefix(&mut stream, boundary, &records, deadline()).unwrap();
    let bytes = stream.data.into_inner();
    let mut input = Memory {
        data: std::io::Cursor::new(bytes.clone()),
    };
    let mut actual = Vec::new();
    read_replica_prefix(&mut input, boundary, deadline(), |record| {
        actual.push(record);
        Ok(())
    })
    .unwrap();
    assert_eq!(actual, records);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let sender = scope.spawn(|| {
            let (mut stream, _) = listener.accept().unwrap();
            send_replica_prefix(&mut stream, boundary, &records, deadline()).unwrap();
        });
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        let mut received = Vec::new();
        read_replica_prefix(&mut stream, boundary, deadline(), |record| {
            received.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(received, records);
        sender.join().unwrap();
    });
    let mut gap = records.clone();
    gap[1].shard_seq = 3;
    let mut output = Memory {
        data: std::io::Cursor::new(Vec::new()),
    };
    assert!(send_replica_prefix(&mut output, boundary, &gap, deadline()).is_err());
    let mut damaged = bytes.clone();
    damaged[12] ^= 1;
    let mut input = Memory {
        data: std::io::Cursor::new(damaged),
    };
    assert!(read_replica_prefix(&mut input, boundary, deadline(), |_| Ok(())).is_err());
    let mut input = Memory {
        data: std::io::Cursor::new(bytes[..bytes.len() - 1].to_vec()),
    };
    assert!(read_replica_prefix(&mut input, boundary, deadline(), |_| Ok(())).is_err());
}

#[test]
fn resume_sender_rejects_forged_or_truncated_confirmation() {
    use matching_control::{ReplicaWriterIdentity, send_replica_resume};
    struct Response {
        /// 保存模拟恢复响应的输入游标。
        input: std::io::Cursor<Vec<u8>>,
    }
    impl Read for Response {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(bytes)
        }
    }
    impl Write for Response {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl ReplicationStream for Response {
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
    let mut frame = Vec::from(&b"RRA1"[..]);
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.extend_from_slice(&2_u16.to_be_bytes());
    frame.extend_from_slice(&3_u64.to_be_bytes());
    frame.extend_from_slice(&4_i64.to_be_bytes());
    frame.extend_from_slice(&2_u64.to_be_bytes());
    frame.extend_from_slice(&matching_persistence::crc32c(&frame).to_be_bytes());
    let deadline = || Instant::now() + Duration::from_secs(1);
    let mut stream = Response {
        input: std::io::Cursor::new(frame.clone()),
    };
    assert!(send_replica_resume(&mut stream, identity, 1, deadline()).is_err());
    let mut stream = Response {
        input: std::io::Cursor::new(frame[..frame.len() - 1].to_vec()),
    };
    assert!(send_replica_resume(&mut stream, identity, 2, deadline()).is_err());
    frame[32] ^= 1;
    let mut stream = Response {
        input: std::io::Cursor::new(frame),
    };
    assert!(send_replica_resume(&mut stream, identity, 2, deadline()).is_err());
}
