//! 复制线程中的摘要登记及失败关闭。
use matching_control::{ReplicaCommitFence, ReplicatedAppender, ReplicationStream};
use matching_persistence::{DurableAppender, WalRecord};
use std::{
    io::{self, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct Local;
impl DurableAppender for Local {
    fn would_rotate(&self, _: &WalRecord) -> io::Result<bool> {
        Ok(false)
    }
    fn append(&mut self, _: &WalRecord) -> io::Result<()> {
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct UnusedStream;
impl Read for UnusedStream {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("未预期的读取"))
    }
}
impl Write for UnusedStream {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("未预期的写入"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl ReplicationStream for UnusedStream {
    fn set_io_timeout(&mut self, _: Duration) -> io::Result<()> {
        Ok(())
    }
}
struct Fence {
    /// 记录摘要登记调用次数。
    calls: Arc<AtomicUsize>,
    /// 控制摘要登记是否返回模拟错误。
    fail: Arc<AtomicBool>,
    /// 启动调用的线程身份，用于验证外围线程执行。
    caller: std::thread::ThreadId,
}
impl ReplicaCommitFence for Fence {
    fn prepare(&mut self, _: &[WalRecord], _: Instant) -> io::Result<()> {
        Ok(())
    }
    fn commit(&mut self, _: &WalRecord, _: Instant) -> io::Result<u64> {
        unreachable!()
    }
    fn checkpoint_digest(
        &mut self,
        seq: u64,
        state: [u8; 32],
        events: [u8; 32],
        _: Instant,
    ) -> io::Result<()> {
        assert_ne!(self.caller, std::thread::current().id());
        assert_eq!((seq, state, events), (7, [1; 32], [2; 32]));
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(io::Error::other("模拟检查点失败"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn checkpoint_runs_on_bounded_replication_thread_and_failure_halts() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let fence = Fence {
        calls: calls.clone(),
        fail: fail.clone(),
        caller: std::thread::current().id(),
    };
    let mut appender = ReplicatedAppender::new(
        Local,
        UnusedStream,
        fence,
        32 * 1024 * 1024,
        Duration::from_secs(3),
    )
    .unwrap();
    appender.checkpoint_digest(7, [1; 32], [2; 32]).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fail.store(true, Ordering::SeqCst);
    assert!(appender.checkpoint_digest(7, [1; 32], [2; 32]).is_err());
    assert!(!appender.health_handle().ready());
    assert!(appender.checkpoint_digest(7, [1; 32], [2; 32]).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn late_checkpoint_success_after_timeout_never_reopens_writer() {
    struct BlockedFence {
        /// 通知测试线程摘要登记已进入等待。
        entered: std::sync::mpsc::SyncSender<()>,
        /// 等待测试线程放行摘要登记。
        release: std::sync::mpsc::Receiver<()>,
    }
    impl ReplicaCommitFence for BlockedFence {
        fn prepare(&mut self, _: &[WalRecord], _: Instant) -> io::Result<()> {
            Ok(())
        }
        fn commit(&mut self, _: &WalRecord, _: Instant) -> io::Result<u64> {
            unreachable!()
        }
        fn checkpoint_digest(
            &mut self,
            _: u64,
            _: [u8; 32],
            _: [u8; 32],
            _: Instant,
        ) -> io::Result<()> {
            self.entered.send(()).map_err(io::Error::other)?;
            self.release
                .recv_timeout(Duration::from_secs(3))
                .map_err(io::Error::other)?;
            Ok(())
        }
    }
    let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let mut appender = ReplicatedAppender::new(
        Local,
        UnusedStream,
        BlockedFence {
            entered,
            release: release_rx,
        },
        32 * 1024 * 1024,
        Duration::from_millis(200),
    )
    .unwrap();
    std::thread::scope(|scope| {
        let request = scope.spawn(|| appender.checkpoint_digest(7, [1; 32], [2; 32]));
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let error = request.join().unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        release.send(()).unwrap();
    });
    assert!(!appender.health_handle().ready());
    assert!(appender.checkpoint_digest(7, [1; 32], [2; 32]).is_err());
}
