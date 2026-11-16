//! 分片状态必须反映真实入口及工作线程生命周期。
use matching_domain::{ClusterId, MarketId, ShardId};
use matching_runtime::{Health, HealthDependency, RouteTable, Shard, ShardConfig};
use std::time::Duration;

struct HttpExchange {
    /// 模拟连接中待读取的请求字节。
    request: std::io::Cursor<Vec<u8>>,
    /// 模拟连接已写出的响应字节。
    response: Vec<u8>,
}
impl std::io::Read for HttpExchange {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.request.read(buffer)
    }
}
impl std::io::Write for HttpExchange {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.response.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn shard_health_reports_running_queue_and_shutdown() {
    let config = ShardConfig {
        capacity: 8,
        ..ShardConfig::default()
    };
    let mut shard = Shard::start_with_config(
        ShardId(997),
        ClusterId(997),
        RouteTable::new([]).unwrap(),
        vec![],
        config,
    )
    .unwrap();
    let handle = shard.handle();
    let health = Health::new(vec![handle.clone()]).unwrap();
    let active = handle.observation();
    assert!(active.thread_running);
    assert!(active.ingress_open);
    assert!(!active.failed);
    assert_eq!(active.queue_capacity, 8);
    handle
        .barrier(1)
        .unwrap()
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(handle.observation().queue_depth, 0);
    assert!(
        health
            .prometheus()
            .unwrap()
            .contains("engine_queue_wait_us_count 1\n")
    );
    shard.shutdown(Duration::from_secs(1)).unwrap();
    let stopped = handle.observation();
    assert!(!stopped.thread_running);
    assert!(!stopped.ingress_open);
    assert!(!stopped.failed);
}

#[test]
fn readiness_requires_all_dependencies_and_live_open_shard() {
    let mut shard = Shard::start(
        ShardId(998),
        ClusterId(998),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let health = Health::new(vec![shard.handle()]).unwrap();
    assert!(Health::new_primary(vec![shard.handle()]).is_err());
    assert!(health.live());
    assert!(!health.ready());
    for dependency in [
        HealthDependency::Recovery,
        HealthDependency::Persistence,
        HealthDependency::Control,
    ] {
        health.set_healthy(dependency, true);
    }
    assert!(health.ready());
    health.set_healthy(HealthDependency::Persistence, false);
    assert!(health.live());
    assert!(!health.ready());
    health.set_healthy(HealthDependency::Persistence, true);
    shard.shutdown(Duration::from_secs(1)).unwrap();
    assert!(!health.ready());
    assert!(!health.live());
    assert!(Health::new(vec![]).is_err());
}

#[test]
fn metrics_expose_real_bounded_queue_and_health() {
    let mut shard = Shard::start_with_config(
        ShardId(999),
        ClusterId(999),
        RouteTable::new([]).unwrap(),
        vec![],
        ShardConfig {
            capacity: 9,
            ..ShardConfig::default()
        },
    )
    .unwrap();
    let health = Health::new(vec![shard.handle()]).unwrap();
    assert!(
        Health::new(vec![shard.handle()])
            .unwrap()
            .with_priority_markets(vec![MarketId(1), MarketId(1)], 2)
            .is_err()
    );
    let missing = Health::new(vec![shard.handle()])
        .unwrap()
        .with_priority_markets(vec![MarketId(1)], 1)
        .unwrap()
        .prometheus()
        .unwrap();
    assert!(missing.contains("engine_priority_metrics_available 0\n"));
    assert!(!missing.contains("market_queue_priority_next{"));
    let metrics = health.prometheus().unwrap();
    assert!(metrics.contains("engine_queue_depth 0\n"));
    assert!(metrics.contains("engine_queue_capacity 9\n"));
    assert!(metrics.contains("engine_live 1\n"));
    assert!(metrics.contains("engine_ready 0\n"));
    shard.shutdown(Duration::from_secs(1)).unwrap();
    let metrics = health.prometheus().unwrap();
    assert!(metrics.contains("engine_live 0\n"));
    assert!(metrics.contains("engine_book_metrics_available 0\n"));
    assert!(!metrics.contains("engine_open_orders "));
}

#[test]
fn health_http_endpoints_reflect_readiness_and_reject_oversized_headers() {
    let mut shard = Shard::start(
        ShardId(996),
        ClusterId(996),
        RouteTable::new([]).unwrap(),
        vec![],
    )
    .unwrap();
    let health = Health::new(vec![shard.handle()]).unwrap();
    let mut exchange = HttpExchange {
        request: std::io::Cursor::new(b"GET /ready HTTP/1.1\r\nHost: local\r\n\r\n".to_vec()),
        response: vec![],
    };
    health.serve_http(&mut exchange, 1024).unwrap();
    assert!(exchange.response.starts_with(b"HTTP/1.1 503 "));
    for dependency in [
        HealthDependency::Recovery,
        HealthDependency::Persistence,
        HealthDependency::Control,
    ] {
        health.set_healthy(dependency, true);
    }
    let mut exchange = HttpExchange {
        request: std::io::Cursor::new(b"GET /ready HTTP/1.1\r\nHost: local\r\n\r\n".to_vec()),
        response: vec![],
    };
    health.serve_http(&mut exchange, 1024).unwrap();
    assert!(exchange.response.starts_with(b"HTTP/1.1 200 "));
    assert!(String::from_utf8_lossy(&exchange.response).contains("durability=ephemeral\n"));
    let mut exchange = HttpExchange {
        request: std::io::Cursor::new(b"GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n".to_vec()),
        response: vec![],
    };
    health.serve_http(&mut exchange, 1024).unwrap();
    assert!(
        String::from_utf8(exchange.response)
            .unwrap()
            .contains("engine_ready 1\n")
    );
    let mut exchange = HttpExchange {
        request: std::io::Cursor::new(vec![b'x'; 32]),
        response: vec![],
    };
    assert!(health.serve_http(&mut exchange, 16).is_err());
    shard.shutdown(Duration::from_secs(1)).unwrap();
}
