//! 独立后台快照文件写入及清单提交。
use super::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;

/// 快照后台队列不可用或已关闭。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotWriterError {
    /// 后台队列已满；未入队镜像被丢弃，调用方可以重新捕获。
    Busy,
    /// 后台写入器已经关闭。
    Closed,
}
/// 后台写入请求及其一次性结果。
struct Request {
    /// 目标快照文件及清单使用的身份。
    id: u64,
    /// 分片屏障产生的不可变状态副本。
    image: SnapshotImage,
    /// 返回文件和清单提交结果的一次性回复端。
    reply: SyncSender<std::io::Result<()>>,
}
/// 持有本分片写入器名额，线程退出时释放。
struct WriterLease(
    /// 工作线程与入口共享的写入器占用标志。
    Arc<AtomicBool>,
);
impl Drop for WriterLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
/// 独占目录、单线程写入且最多排队一个镜像的后台写入器。
pub struct SnapshotWriter {
    /// 有界请求入口。
    sender: Option<SyncSender<Request>>,
    /// 后台线程所有权。
    join: Option<JoinHandle<()>>,
}
impl SnapshotWriter {
    /// 启动独立写入线程；调用方须确保目录独占，并先关闭写入器再关闭分片。
    pub fn start(
        handle: ShardHandle,
        directory: impl AsRef<Path>,
        max_bytes: usize,
    ) -> std::io::Result<Self> {
        let directory: PathBuf = directory.as_ref().to_path_buf();
        if !directory.is_dir() || max_bytes == 0 || max_bytes > 256 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "快照目录或容量无效",
            ));
        }
        if handle
            .snapshot_writer_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "分片快照写入器已存在",
            ));
        }
        let lease = WriterLease(Arc::clone(&handle.snapshot_writer_active));
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("snapshot.lock"))?;
        lock.try_lock()
            .map_err(|error| std::io::Error::other(format!("快照目录锁不可用: {error}")))?;
        let (sender, receiver) = sync_channel::<Request>(1);
        let join = std::thread::Builder::new()
            .name("snapshot-writer".into())
            .spawn(move || {
                let _lease = lease;
                let _lock = lock;
                while let Ok(request) = receiver.recv() {
                    let result = (|| {
                        if request.image.shard != handle.shard_id
                            || request
                                .image
                                .origin
                                .as_ref()
                                .is_none_or(|origin| !Arc::ptr_eq(origin, &handle.snapshot_origin))
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "快照并非来自当前分片屏障",
                            ));
                        }
                        let payload = request.image.encode_snapshot(max_bytes).map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, "镜像编码失败")
                        })?;
                        matching_persistence::write_snapshot_file(
                            &directory,
                            request.id,
                            request.image.snapshot_seq,
                            &payload,
                        )?;
                        let (reply, receipt) = sync_channel(1);
                        handle
                            .push(
                                Message::SnapshotCommitted(
                                    request.id,
                                    request.image.snapshot_seq,
                                    request.image.cluster,
                                    request.image.shard,
                                    reply,
                                ),
                                IngressClass::Control,
                            )
                            .map_err(|error| {
                                std::io::Error::other(format!("提交快照控制消息失败: {error:?}"))
                            })?;
                        receipt
                            .recv()
                            .map_err(|_| std::io::Error::other("清单写者退出"))?
                    })();
                    let _ = request.reply.send(result);
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            join: Some(join),
        })
    }
    /// 尝试入队不可变镜像；队列满时立即返回背压。
    pub fn submit(
        &self,
        id: u64,
        image: SnapshotImage,
    ) -> Result<Receiver<std::io::Result<()>>, SnapshotWriterError> {
        let (reply, receiver) = sync_channel(1);
        let request = Request { id, image, reply };
        self.sender
            .as_ref()
            .ok_or(SnapshotWriterError::Closed)?
            .try_send(request)
            .map_err(|error| match error {
                TrySendError::Full(_) => SnapshotWriterError::Busy,
                TrySendError::Disconnected(_) => SnapshotWriterError::Closed,
            })?;
        Ok(receiver)
    }
    /// 关闭入口并等待所有已接受请求结束。
    pub fn shutdown(&mut self) -> std::io::Result<()> {
        self.sender.take();
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| std::io::Error::other("快照写入线程异常退出"))?;
        }
        Ok(())
    }
}
impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
