//! 有界队列、准入配额与关闭屏障的单消费者封装。

use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// 外部入口消息的配额类别。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IngressClass {
    /// 业务状态变更，可使用全部空闲容量。
    Mutation,
    /// 查询消息，受查询配额和变更预留空间限制。
    Query,
    /// 控制消息，受控制配额和变更预留空间限制。
    Control,
    /// 已关闭外部入口后的内部屏障，不计入外部配额。
    Internal,
}

/// 有界容量及外部查询、控制准入配额。
#[derive(Clone, Copy, Debug)]
pub(crate) struct IngressLimits {
    /// 队列固定容量。
    pub(crate) capacity: usize,
    /// 查询和控制不能占用的变更预留容量。
    pub(crate) mutation_reserved_slots: usize,
    /// 排队查询数量上限。
    pub(crate) max_query_inflight: usize,
    /// 排队控制数量上限。
    pub(crate) max_control_inflight: usize,
}

/// 入队失败时保留未接收消息。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PushError<T> {
    /// 容量、类别配额已满，或准入锁正被其他线程使用。
    Full(T),
    /// 外部入口已经关闭。
    Closed(T),
}

/// 入队、出队配额和关闭共享同一短临界区，不持有业务状态。
struct Admission {
    /// 是否已关闭外部入口。
    closed: bool,
    /// 尚在队列内的查询数量。
    query: usize,
    /// 尚在队列内的控制消息数量。
    control: usize,
    /// 创建队列时固定的容量与配额。
    limits: IngressLimits,
}

/// 生产者和唯一消费者共同持有的队列元数据。
struct Shared<T> {
    /// 有界消息及其准入类别。
    queue: ArrayQueue<(IngressClass, T)>,
    /// 入队、出队和关闭共用的元数据边界。
    admission: Mutex<Admission>,
    /// 在关闭完成后无锁识别已关闭入口。
    closed: AtomicBool,
}

/// 可克隆的有界生产者入口。
pub(crate) struct Producer<T> {
    /// 所有入口共用的有界队列及准入状态。
    shared: Arc<Shared<T>>,
}
impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<T> Producer<T> {
    /// 外部消息仅尝试一次；关闭与准入在同一临界区线性化。
    pub(crate) fn try_push_classified(
        &self,
        message: T,
        class: IngressClass,
    ) -> Result<(), PushError<T>> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(PushError::Closed(message));
        }
        let mut state = match self.shared.admission.try_lock() {
            Ok(state) => state,
            // 准入锁竞争也返回繁忙；此时队列不一定已满。
            Err(std::sync::TryLockError::WouldBlock) => {
                return if self.shared.closed.load(Ordering::Acquire) {
                    Err(PushError::Closed(message))
                } else {
                    Err(PushError::Full(message))
                };
            }
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("准入状态锁已损坏"),
        };
        if state.closed {
            return Err(PushError::Closed(message));
        }
        assert!(
            class != IngressClass::Internal,
            "internal message requires shutdown path"
        );
        let non_mutation = state.query + state.control;
        let maximum = state.limits.capacity - state.limits.mutation_reserved_slots;
        let quota_full = match class {
            IngressClass::Query => {
                state.query >= state.limits.max_query_inflight || non_mutation >= maximum
            }
            IngressClass::Control => {
                state.control >= state.limits.max_control_inflight || non_mutation >= maximum
            }
            IngressClass::Mutation => false,
            IngressClass::Internal => unreachable!(),
        };
        if quota_full {
            return Err(PushError::Full(message));
        }
        match self.shared.queue.push((class, message)) {
            Ok(()) => {
                match class {
                    IngressClass::Query => state.query += 1,
                    IngressClass::Control => state.control += 1,
                    _ => {}
                }
                Ok(())
            }
            Err((_, message)) => Err(PushError::Full(message)),
        }
    }

    /// 关闭所有外部消息入口；返回时此前成功入队的消息已经可被消费。
    pub(crate) fn close_ingress(&self) {
        let mut state = self
            .shared
            .admission
            .lock()
            .expect("admission mutex poisoned");
        state.closed = true;
        self.shared.closed.store(true, Ordering::Release);
    }

    /// 在截止时间前关闭外部入口；超时不声称入口已经关闭。
    pub(crate) fn close_ingress_until(&self, deadline: Instant) -> bool {
        loop {
            if Instant::now() >= deadline {
                return false;
            }
            match self.shared.admission.try_lock() {
                Ok(mut state) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    state.closed = true;
                    self.shared.closed.store(true, Ordering::Release);
                    return true;
                }
                Err(std::sync::TryLockError::WouldBlock) => std::thread::yield_now(),
                Err(std::sync::TryLockError::Poisoned(_)) => panic!("准入状态锁已损坏"),
            }
        }
    }

    /// 关闭后在截止时间前将内部屏障入队；超时归还消息。
    pub(crate) fn push_internal_until(&self, mut message: T, deadline: Instant) -> Result<(), T> {
        loop {
            {
                let state = match self.shared.admission.try_lock() {
                    Ok(state) => state,
                    Err(std::sync::TryLockError::WouldBlock) => {
                        if Instant::now() >= deadline {
                            return Err(message);
                        }
                        std::thread::yield_now();
                        continue;
                    }
                    Err(std::sync::TryLockError::Poisoned(_)) => panic!("准入状态锁已损坏"),
                };
                assert!(state.closed, "internal barrier requires closed ingress");
                if Instant::now() >= deadline {
                    return Err(message);
                }
                match self.shared.queue.push((IngressClass::Internal, message)) {
                    Ok(()) => return Ok(()),
                    Err((_, returned)) => message = returned,
                }
            }
            if Instant::now() >= deadline {
                return Err(message);
            }
            std::thread::yield_now();
        }
    }

    /// 简单有界变更入口；失败归还消息。
    #[cfg(test)]
    pub(crate) fn try_push(&self, message: T) -> Result<(), T> {
        self.try_push_classified(message, IngressClass::Mutation)
            .map_err(|error| match error {
                PushError::Full(message) | PushError::Closed(message) => message,
            })
    }
}

/// 不可克隆的唯一工作线程出队端。
pub(crate) struct Consumer<T> {
    /// 唯一消费者访问的有界队列及准入状态。
    shared: Arc<Shared<T>>,
}
impl<T> Consumer<T> {
    /// 在准入锁下共同观察关闭状态和队列空状态。
    pub(crate) fn is_closed_and_empty(&self) -> bool {
        let state = self.shared.admission.lock().expect("准入状态锁已损坏");
        state.closed && self.shared.queue.is_empty()
    }

    /// 按入队顺序取出消息并释放对应排队配额。
    pub(crate) fn pop(&mut self) -> Option<T> {
        let mut state = self
            .shared
            .admission
            .lock()
            .expect("admission mutex poisoned");
        let (class, message) = self.shared.queue.pop()?;
        match class {
            IngressClass::Query => state.query -= 1,
            IngressClass::Control => state.control -= 1,
            _ => {}
        }
        Some(message)
    }
}

/// 构造配额受限的队列；错误配置不构造入口。
pub(crate) fn bounded_queue_with_limits<T>(
    limits: IngressLimits,
) -> Result<(Producer<T>, Consumer<T>), ()> {
    if limits.capacity == 0
        || limits.mutation_reserved_slots == 0
        || limits.mutation_reserved_slots > limits.capacity
    {
        return Err(());
    }
    let shared = Arc::new(Shared {
        queue: ArrayQueue::new(limits.capacity),
        closed: AtomicBool::new(false),
        admission: Mutex::new(Admission {
            closed: false,
            query: 0,
            control: 0,
            limits,
        }),
    });
    Ok((
        Producer {
            shared: Arc::clone(&shared),
        },
        Consumer { shared },
    ))
}

/// 构造仅用于变更消息的固定容量队列。
#[cfg(test)]
pub(crate) fn bounded_queue<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    bounded_queue_with_limits(IngressLimits {
        capacity,
        mutation_reserved_slots: capacity,
        max_query_inflight: 0,
        max_control_inflight: 0,
    })
    .expect("queue capacity must be positive")
}

#[cfg(test)]
mod tests {
    use super::bounded_queue;

    #[test]
    fn full_queue_returns_original_message_and_keeps_fifo() {
        let (producer, mut consumer) = bounded_queue(2);
        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.clone().try_push(2), Ok(()));
        assert_eq!(producer.try_push(3), Err(3));
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), None);
    }
}

#[cfg(test)]
mod admission_tests {
    use super::{IngressClass, IngressLimits, PushError, bounded_queue_with_limits};
    use std::time::{Duration, Instant};

    #[test]
    fn reserves_capacity_and_releases_quotas_on_pop() {
        let (p, mut c) = bounded_queue_with_limits(IngressLimits {
            capacity: 3,
            mutation_reserved_slots: 1,
            max_query_inflight: 2,
            max_control_inflight: 1,
        })
        .unwrap();
        assert!(p.try_push_classified(1, IngressClass::Query).is_ok());
        assert!(p.try_push_classified(2, IngressClass::Control).is_ok());
        assert!(matches!(
            p.try_push_classified(3, IngressClass::Query),
            Err(PushError::Full(3))
        ));
        assert!(p.try_push_classified(4, IngressClass::Mutation).is_ok());
        assert_eq!(c.pop(), Some(1));
        assert!(p.try_push_classified(5, IngressClass::Query).is_ok());
    }

    #[test]
    fn closed_ingress_rejects_external_and_internal_barrier_obeys_deadline() {
        let (p, mut c) = bounded_queue_with_limits(IngressLimits {
            capacity: 1,
            mutation_reserved_slots: 1,
            max_query_inflight: 0,
            max_control_inflight: 0,
        })
        .unwrap();
        assert!(p.try_push_classified(1, IngressClass::Mutation).is_ok());
        p.close_ingress();
        assert!(matches!(
            p.try_push_classified(2, IngressClass::Mutation),
            Err(PushError::Closed(2))
        ));
        assert_eq!(
            p.push_internal_until(3, Instant::now() + Duration::from_millis(5)),
            Err(3)
        );
        assert_eq!(c.pop(), Some(1));
        assert_eq!(
            p.push_internal_until(4, Instant::now() + Duration::from_secs(1)),
            Ok(())
        );
        assert_eq!(c.pop(), Some(4));
    }
    #[test]
    fn closed_ingress_is_reported_even_during_lock_contention() {
        let (producer, _consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 2,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 0,
        })
        .unwrap();
        producer.close_ingress();
        let _held = producer.shared.admission.lock().unwrap();
        assert_eq!(
            producer.try_push_classified(9, IngressClass::Mutation),
            Err(PushError::Closed(9))
        );
    }

    #[test]
    fn producer_returns_busy_without_waiting_for_admission_lock() {
        let (producer, _consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 2,
            mutation_reserved_slots: 1,
            max_query_inflight: 1,
            max_control_inflight: 0,
        })
        .unwrap();
        let held = producer.shared.admission.lock().unwrap();
        let copy = producer.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            tx.send(copy.try_push_classified(9, IngressClass::Mutation))
                .unwrap();
        });
        let result = rx.recv_timeout(Duration::from_millis(100));
        drop(held);
        worker.join().unwrap();
        assert_eq!(result.unwrap(), Err(PushError::Full(9)));
    }

    #[test]
    fn ingress_close_times_out_if_admission_lock_is_held() {
        let (producer, _consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 1,
            mutation_reserved_slots: 1,
            max_query_inflight: 0,
            max_control_inflight: 0,
        })
        .unwrap();
        let guard = producer.shared.admission.lock().unwrap();
        assert!(!producer.close_ingress_until(Instant::now() + Duration::from_millis(5)));
        drop(guard);
        assert!(producer.close_ingress_until(Instant::now() + Duration::from_secs(1)));
        assert!(matches!(
            producer.try_push_classified(1, IngressClass::Mutation),
            Err(PushError::Closed(1))
        ));
    }

    #[test]
    fn internal_barrier_timeout_includes_wait_for_admission_lock() {
        let (producer, _consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 1,
            mutation_reserved_slots: 1,
            max_query_inflight: 0,
            max_control_inflight: 0,
        })
        .unwrap();
        producer.close_ingress();
        let held = producer.shared.admission.lock().unwrap();
        let copy = producer.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            tx.send(copy.push_internal_until(9, Instant::now() + Duration::from_millis(5)))
                .unwrap();
        });
        let result = rx.recv_timeout(Duration::from_millis(100));
        drop(held);
        worker.join().unwrap();
        assert_eq!(result.unwrap(), Err(9));
    }

    #[test]
    fn expired_internal_deadline_does_not_accept_even_when_queue_has_room() {
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 1,
            mutation_reserved_slots: 1,
            max_query_inflight: 0,
            max_control_inflight: 0,
        })
        .unwrap();
        producer.close_ingress();
        let expired = Instant::now() - Duration::from_secs(1);
        assert_eq!(producer.push_internal_until(7, expired), Err(7));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn concurrent_close_keeps_every_accepted_message_before_barrier() {
        let (producer, mut consumer) = bounded_queue_with_limits(IngressLimits {
            capacity: 101,
            mutation_reserved_slots: 1,
            max_query_inflight: 100,
            max_control_inflight: 0,
        })
        .unwrap();
        let start = std::sync::Arc::new(std::sync::Barrier::new(101));
        let mut workers = Vec::new();
        for id in 0..100 {
            let p = producer.clone();
            let start = std::sync::Arc::clone(&start);
            workers.push(std::thread::spawn(move || {
                start.wait();
                p.try_push_classified(id, IngressClass::Query).is_ok()
            }));
        }
        start.wait();
        producer.close_ingress();
        let accepted = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|accepted| *accepted)
            .count();
        producer
            .push_internal_until(100, Instant::now() + Duration::from_secs(1))
            .unwrap();
        for _ in 0..accepted {
            assert!(consumer.pop().unwrap() < 100);
        }
        assert_eq!(consumer.pop(), Some(100));
        assert_eq!(consumer.pop(), None);
    }
}
