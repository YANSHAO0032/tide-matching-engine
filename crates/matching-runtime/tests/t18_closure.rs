//! 验证运行时查询、屏障与关闭边界。
use matching_domain::{
    ClusterId, CommandId, LimitOrder, MarketId, OrderId, Price, Qty, ShardId, Side, StpKey,
    StpMode, UserId,
};
use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
use matching_runtime::{MarketRoute, Mutation, RouteState, RouteTable, Shard, ShardError};
use std::time::{Duration, Instant};

static NEXT_CLUSTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(7401);

fn shard() -> Shard {
    let market = MarketId(701);
    let routes = RouteTable::new([MarketRoute {
        market_id: market,
        shard_id: ShardId(41),
        route_epoch: 7,
        state: RouteState::Active,
    }])
    .unwrap();
    Shard::start(
        ShardId(41),
        ClusterId(NEXT_CLUSTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)),
        routes,
        vec![(
            market,
            ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap()),
        )],
    )
    .unwrap()
}

fn retry<T>(mut f: impl FnMut() -> Result<T, ShardError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match f() {
            Ok(v) => return v,
            Err(ShardError::Busy) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("入口失败: {e:?}"),
        }
    }
}

fn order(id: u128) -> LimitOrder {
    LimitOrder {
        order_id: OrderId(id),
        user_id: UserId(id as u64),
        stp_key: StpKey::new(0),
        stp_mode: StpMode::None,
        side: Side::Buy,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(1).unwrap(),
    }
}

#[test]
fn query_and_barrier_observe_ordered_execution_and_rejections() {
    let mut shard = shard();
    let h = shard.handle();
    let m1 = retry(|| h.submit(CommandId(1), MarketId(701), 7, Mutation::Place(order(1))));
    let q = retry(|| h.order(MarketId(701), OrderId(1)));
    let m2 = retry(|| h.submit(CommandId(2), MarketId(701), 7, Mutation::Cancel(OrderId(1))));
    assert_eq!(
        m1.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    assert!(
        q.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .is_some()
    );
    assert_eq!(
        m2.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .seq,
        2
    );
    assert!(
        retry(|| h.order(MarketId(701), OrderId(1)))
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert!(
        retry(|| h.submit(CommandId(3), MarketId(701), 6, Mutation::Cancel(OrderId(4))))
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .is_err()
    );
    assert_eq!(
        retry(|| h.barrier(3))
            .recv_timeout(Duration::from_secs(10))
            .unwrap(),
        (3, 2)
    );
    let rejected = retry(|| h.submit(CommandId(4), MarketId(701), 7, Mutation::Cancel(OrderId(4))))
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    assert_eq!(rejected.seq, 3);
    assert!(rejected.result.is_err());
    assert_eq!(
        retry(|| h.barrier(4))
            .recv_timeout(Duration::from_secs(10))
            .unwrap(),
        (4, 3)
    );
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn abandoned_replies_do_not_cancel_or_block_work() {
    let mut shard = shard();
    let h = shard.handle();
    drop(retry(|| {
        h.submit(CommandId(1), MarketId(701), 7, Mutation::Place(order(9)))
    }));
    drop(retry(|| h.order(MarketId(701), OrderId(9))));
    drop(retry(|| h.barrier(10)));
    assert_eq!(
        retry(|| h.barrier(11))
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        (11, 1)
    );
    assert!(
        retry(|| h.order(MarketId(701), OrderId(9)))
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .is_some()
    );
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn timeout_before_closing_admission_allows_retry_and_submission() {
    let mut shard = shard();
    assert_eq!(shard.shutdown(Duration::ZERO), Err(ShardError::Timeout));
    let h = shard.handle();
    assert_eq!(
        retry(|| h.submit(CommandId(1), MarketId(701), 7, Mutation::Place(order(1))))
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    shard.shutdown(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        h.order(MarketId(701), OrderId(1)),
        Err(ShardError::Closed)
    ));
}

#[test]
fn concurrent_mutations_queries_and_barriers_preserve_sequences() {
    let mut shard = shard();
    let h = shard.handle();
    let mut threads = Vec::new();
    for i in 0..12u128 {
        let h = h.clone();
        threads.push(std::thread::spawn(move || {
            let mut seqs = Vec::new();
            for n in 0..20u128 {
                let id = i * 100 + n + 1;
                let execution =
                    retry(|| h.submit(CommandId(id), MarketId(701), 7, Mutation::Place(order(id))))
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap()
                        .unwrap();
                assert!(execution.result.is_ok());
                seqs.push(execution.seq);
                assert!(
                    retry(|| h.order(MarketId(701), OrderId(id)))
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap()
                        .unwrap()
                        .is_some()
                );
                let _ = retry(|| h.barrier(id))
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
            seqs
        }));
    }
    let mut seqs: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=240).collect::<Vec<_>>());
    assert_eq!(
        retry(|| h.barrier(9999))
            .recv_timeout(Duration::from_secs(10))
            .unwrap(),
        (9999, 240)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn shutdown_racing_submit_drains_every_accepted_request() {
    let mut shard = shard();
    let handle = shard.handle();
    let first = retry(|| {
        handle.submit(
            CommandId(9000),
            MarketId(701),
            7,
            Mutation::Place(order(9000)),
        )
    });
    let start = std::sync::Arc::new(std::sync::Barrier::new(9));
    let mut threads = Vec::new();
    for i in 0..8u128 {
        let h = handle.clone();
        let start = start.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            let mut mutations = Vec::new();
            let mut queries = Vec::new();
            let mut barriers = Vec::new();
            for n in 0..40u128 {
                let id = i * 100 + n + 1;
                match h.submit(CommandId(id), MarketId(701), 7, Mutation::Place(order(id))) {
                    Ok(reply) => mutations.push((id, reply)),
                    Err(ShardError::Busy | ShardError::Closed) => {}
                    Err(e) => panic!("意外的提交错误: {e:?}"),
                }
                match h.order(MarketId(701), OrderId(9000)) {
                    Ok(reply) => queries.push(reply),
                    Err(ShardError::Busy | ShardError::Closed) => {}
                    Err(e) => panic!("意外的查询错误: {e:?}"),
                }
                match h.barrier(id) {
                    Ok(reply) => barriers.push((id, reply)),
                    Err(ShardError::Busy | ShardError::Closed) => {}
                    Err(e) => panic!("意外的屏障错误: {e:?}"),
                }
            }
            (mutations, queries, barriers)
        }));
    }
    start.wait();
    shard.shutdown(Duration::from_secs(5)).unwrap();
    let first = first.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    assert_eq!(first.seq, 1);
    let mut seqs = vec![first.seq];
    for thread in threads {
        let (mutations, queries, barriers) = thread.join().unwrap();
        for (id, reply) in mutations {
            let result = reply.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
            assert_eq!(result.command_id, CommandId(id));
            assert!(result.result.is_ok());
            seqs.push(result.seq);
        }
        for reply in queries {
            assert!(
                reply
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
        }
        for (id, reply) in barriers {
            assert_eq!(reply.recv_timeout(Duration::from_secs(2)).unwrap().0, id);
        }
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert!(matches!(handle.barrier(1), Err(ShardError::Closed)));
}

#[test]
fn nonreading_receivers_do_not_block_worker() {
    let mut shard = shard();
    let h = shard.handle();
    let held_query = retry(|| h.order(MarketId(701), OrderId(1)));
    let held_barrier = retry(|| h.barrier(17));
    let held_mutation =
        retry(|| h.submit(CommandId(1), MarketId(701), 7, Mutation::Place(order(1))));
    assert_eq!(
        retry(|| h.barrier(18))
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        (18, 1)
    );
    assert!(
        held_query
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        held_barrier.recv_timeout(Duration::from_secs(2)).unwrap(),
        (17, 0)
    );
    assert_eq!(
        held_mutation
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .seq,
        1
    );
    shard.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn separate_query_and_control_floods_allow_mutation_progress() {
    let mut shard = shard();
    let h = shard.handle();
    let start = std::sync::Arc::new(std::sync::Barrier::new(4));
    let mut threads = Vec::new();
    for is_query in [true, false] {
        let h = h.clone();
        let start = start.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            for n in 0..300u128 {
                if is_query {
                    let _ = retry(|| h.order(MarketId(701), OrderId(9000)))
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap()
                        .unwrap();
                } else {
                    let _ = retry(|| h.barrier(n))
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
            }
        }));
    }
    let mutation_handle = h.clone();
    let mutation_start = start.clone();
    let mutation = std::thread::spawn(move || {
        mutation_start.wait();
        for id in 1..=100u128 {
            let execution = retry(|| {
                mutation_handle.submit(CommandId(id), MarketId(701), 7, Mutation::Place(order(id)))
            })
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
            assert_eq!(execution.seq, id as u64);
            assert!(execution.result.is_ok());
        }
    });
    start.wait();
    mutation.join().unwrap();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(
        retry(|| h.barrier(999))
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        (999, 100)
    );
    shard.shutdown(Duration::from_secs(5)).unwrap();
}
