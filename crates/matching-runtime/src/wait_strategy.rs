//! 工作线程等待策略和无业务状态的唤醒。

/// 工作线程空闲时的等待方式；只影响调度，不改变业务状态。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WaitStrategy {
    /// 使用处理器自旋提示，适用于专用核心。
    BusySpin,
    /// 让出当前线程的执行机会。
    Yield,
    /// 休眠直到其他线程发出唤醒令牌。
    Park,
    /// 先短暂自旋，再让出线程，最后等待唤醒。
    #[default]
    Hybrid,
}

/// 持有单次空闲阶段计数，不进入撮合业务状态。
pub(crate) struct Waiter {
    /// 当前工作线程的空闲等待方式。
    strategy: WaitStrategy,
    /// 混合等待策略已执行的空闲阶段计数。
    idle_count: u8,
}

impl Waiter {
    /// 创建工作线程独占的等待状态。
    pub(crate) fn new(strategy: WaitStrategy) -> Self {
        Self {
            strategy,
            idle_count: 0,
        }
    }

    /// 完成一次空闲等待。
    pub(crate) fn idle(&mut self) {
        match self.strategy {
            WaitStrategy::BusySpin => std::hint::spin_loop(),
            WaitStrategy::Yield => std::thread::yield_now(),
            WaitStrategy::Park => std::thread::park(),
            WaitStrategy::Hybrid => {
                if self.idle_count < 32 {
                    self.idle_count += 1;
                    std::hint::spin_loop();
                } else if self.idle_count < 48 {
                    self.idle_count += 1;
                    std::thread::yield_now();
                } else {
                    std::thread::park();
                }
            }
        }
    }

    /// 处理消息后重新开始空闲阶段。
    pub(crate) fn reset(&mut self) {
        self.idle_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::{WaitStrategy, Waiter};
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn park_is_woken_by_unpark_token() {
        let (ready, observed) = sync_channel(1);
        let worker = thread::spawn(move || {
            let mut waiter = Waiter::new(WaitStrategy::Park);
            ready.send(()).unwrap();
            waiter.idle();
            7
        });
        observed.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.thread().unpark();
        assert_eq!(worker.join().unwrap(), 7);
    }
}
