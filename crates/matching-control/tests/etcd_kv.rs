//! 使用隔离的真实集群核验线性一致读写。
use matching_control::{EtcdClient, KvVersion};
use std::time::Duration;

#[test]
fn endpoint_probes_share_one_operation_deadline() {
    let mut endpoints = Vec::new();
    for _ in 0..3 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        std::thread::spawn(move || {
            if let Ok((_stream, _)) = listener.accept() {
                std::thread::sleep(Duration::from_secs(2));
            }
        });
    }
    let started = std::time::Instant::now();
    assert!(EtcdClient::new(endpoints, Duration::from_millis(200), true).is_err());
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "探测按端点重复消耗超时: {:?}",
        started.elapsed()
    );
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn authorization_failure_does_not_fall_back_to_another_endpoint() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).unwrap();
        stream
            .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
    });
    let endpoint = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .next()
        .unwrap()
        .to_owned();
    let result = EtcdClient::new(
        vec![format!("http://{address}"), endpoint],
        Duration::from_secs(3),
        true,
    );
    server.join().unwrap();
    assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied));
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn compare_and_swap_is_linearizable_across_clients() {
    let endpoints = std::env::var("TIDE_ETCD_TEST_ENDPOINTS").unwrap();
    let endpoints: Vec<_> = endpoints.split(',').map(str::to_owned).collect();
    let client = EtcdClient::new(endpoints.clone(), Duration::from_secs(3), true).unwrap();
    let other = EtcdClient::new(endpoints, Duration::from_secs(3), true).unwrap();
    let key = format!(
        "/tide-test/cas/{}/{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("case")
    );
    assert!(client.get(key.as_bytes()).unwrap().is_none());
    let first = client
        .compare_and_swap(key.as_bytes(), KvVersion::Absent, b"one")
        .unwrap()
        .unwrap();
    assert!(
        other
            .compare_and_swap(key.as_bytes(), KvVersion::Absent, b"wrong")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        other.get(key.as_bytes()).unwrap().unwrap(),
        (first, b"one".to_vec())
    );
    let second = other
        .compare_and_swap(key.as_bytes(), KvVersion::Revision(first), b"two")
        .unwrap()
        .unwrap();
    assert!(second > first);
    assert!(
        client
            .compare_and_swap(key.as_bytes(), KvVersion::Revision(first), b"wrong")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        client.get(key.as_bytes()).unwrap().unwrap(),
        (second, b"two".to_vec())
    );
}

#[test]
#[ignore = "需要独立本地 etcd 测试集群"]
fn unavailable_first_endpoint_does_not_block_quorum_operations() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().take(3) {
            drop(stream);
        }
    });
    let endpoint = std::env::var("TIDE_ETCD_TEST_ENDPOINTS")
        .unwrap()
        .split(',')
        .next()
        .unwrap()
        .to_owned();
    let client = EtcdClient::new(
        vec![unavailable, endpoint],
        Duration::from_millis(500),
        true,
    )
    .unwrap();
    let key = format!("/tide-test/failover/{}", std::process::id());
    let revision = client
        .compare_and_swap(key.as_bytes(), KvVersion::Absent, b"available")
        .unwrap()
        .unwrap();
    assert_eq!(
        client.get(key.as_bytes()).unwrap(),
        Some((revision, b"available".to_vec()))
    );
}
