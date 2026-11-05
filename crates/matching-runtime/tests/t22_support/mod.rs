//! 隔离真实文件持久化测试的临时目录。
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
/// 在测试退出时清理独立临时目录。
pub struct TestDirectory(pub PathBuf);
impl TestDirectory {
    /// 创建不会复用其他测试或失败残留的目录。
    pub fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "matching-runtime-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("wal")).unwrap();
        std::fs::create_dir(path.join("manifest")).unwrap();
        Self(path)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("临时目录清理失败: {error}");
        }
    }
}
