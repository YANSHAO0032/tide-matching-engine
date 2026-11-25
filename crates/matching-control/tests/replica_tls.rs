//! 验证复制连接的双向 TLS、固定证书及握手期限。
use matching_control::TlsReplicaStream;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CA: &[u8] = include_bytes!("fixtures/test-ca.der");
const SERVER: &[u8] = include_bytes!("fixtures/test-server.der");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/test-server-key.der");
const CLIENT: &[u8] = include_bytes!("fixtures/test-client.der");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/test-client-key.der");
const OTHER: &[u8] = include_bytes!("fixtures/test-other.der");
const OTHER_KEY: &[u8] = include_bytes!("fixtures/test-other-key.der");

fn configs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let server = matching_control::replica_server_config(
        &[CA],
        vec![CertificateDer::from(SERVER.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(SERVER_KEY.to_vec())),
    )
    .unwrap();
    let client = matching_control::replica_client_config(
        &[CA],
        vec![CertificateDer::from(CLIENT.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(CLIENT_KEY.to_vec())),
    )
    .unwrap();
    (Arc::new(client), Arc::new(server))
}

#[test]
fn secure_configs_reject_empty_roots_and_mismatched_keys() {
    let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(CLIENT_KEY.to_vec()));
    assert!(
        matching_control::replica_client_config(
            &[],
            vec![CertificateDer::from(CLIENT.to_vec())],
            key()
        )
        .is_err()
    );
    assert!(
        matching_control::replica_server_config(
            &[],
            vec![CertificateDer::from(SERVER.to_vec())],
            key()
        )
        .is_err()
    );
    assert!(
        matching_control::replica_server_config(
            &[CA],
            vec![CertificateDer::from(SERVER.to_vec())],
            key()
        )
        .is_err()
    );
}

#[test]
fn mutual_tls_checks_peer_certificate_and_transports_bytes() {
    let (client, server) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut stream = TlsReplicaStream::accept(
                socket,
                server,
                CLIENT,
                Instant::now() + Duration::from_secs(3),
            )
            .unwrap();
            let mut bytes = [0_u8; 4];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"test");
            stream.write_all(b"okay").unwrap();
            stream.flush().unwrap();
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let mut stream = TlsReplicaStream::connect(
            socket,
            client,
            name,
            SERVER,
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
        stream.write_all(b"test").unwrap();
        stream.flush().unwrap();
        let mut bytes = [0_u8; 4];
        stream.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"okay");
        worker.join().unwrap();
    });
}

#[test]
fn wrong_peer_pin_is_rejected() {
    let (client, server) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            assert!(
                TlsReplicaStream::accept(
                    socket,
                    server,
                    OTHER,
                    Instant::now() + Duration::from_secs(3)
                )
                .is_err()
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let _result = TlsReplicaStream::connect(
            socket,
            client,
            name,
            SERVER,
            Instant::now() + Duration::from_secs(3),
        );
        worker.join().unwrap();
    });
}

#[test]
fn rotated_client_certificate_requires_explicit_pin_update() {
    for updated in [false, true] {
        let (_, server) = configs();
        let client = Arc::new(
            matching_control::replica_client_config(
                &[CA],
                vec![CertificateDer::from(OTHER.to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(OTHER_KEY.to_vec())),
            )
            .unwrap(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let (socket, _) = listener.accept().unwrap();
                let pin = if updated { OTHER } else { CLIENT };
                let result = TlsReplicaStream::accept(
                    socket,
                    server,
                    pin,
                    Instant::now() + Duration::from_secs(3),
                );
                assert_eq!(result.is_ok(), updated);
            });
            let socket = std::net::TcpStream::connect(address).unwrap();
            let name = ServerName::try_from("server.test".to_string()).unwrap();
            let _result = TlsReplicaStream::connect(
                socket,
                client,
                name,
                SERVER,
                Instant::now() + Duration::from_secs(3),
            );
            worker.join().unwrap();
        });
    }
}

#[test]
fn server_requires_client_certificate() {
    let (_, server) = configs();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(CA.to_vec())).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            assert!(
                TlsReplicaStream::accept(
                    socket,
                    server,
                    CLIENT,
                    Instant::now() + Duration::from_secs(3)
                )
                .is_err()
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let _result = TlsReplicaStream::connect(
            socket,
            client,
            name,
            SERVER,
            Instant::now() + Duration::from_secs(3),
        );
        worker.join().unwrap();
    });
}

#[test]
fn handshake_times_out_if_peer_does_not_respond() {
    let (client, _) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (_socket, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(150));
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let start = Instant::now();
        assert!(
            TlsReplicaStream::connect(
                socket,
                client,
                name,
                SERVER,
                start + Duration::from_millis(30)
            )
            .is_err()
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        worker.join().unwrap();
    });
}

#[test]
fn handshake_slow_drip_respects_total_deadline() {
    let (client, _) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (mut socket, _) = listener.accept().unwrap();
            let mut hello = [0_u8; 4096];
            let _ = socket.read(&mut hello);
            for byte in [0x16_u8, 0x03, 0x03, 0x01, 0x00]
                .into_iter()
                .chain(std::iter::repeat_n(0, 50))
            {
                if socket.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let start = Instant::now();
        assert!(
            TlsReplicaStream::connect(
                socket,
                client,
                name,
                SERVER,
                start + Duration::from_millis(35)
            )
            .is_err()
        );
        assert!(start.elapsed() < Duration::from_millis(250));
        worker.join().unwrap();
    });
}

#[test]
fn encrypted_application_record_respects_shared_read_deadline() {
    use matching_control::ReplicationStream;
    struct SlowWire {
        /// 承载测试流量的实际 TCP 连接。
        socket: std::net::TcpStream,
        /// 控制是否模拟缓慢写入。
        slow: bool,
    }
    impl Read for SlowWire {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.socket.read(bytes)
        }
    }
    impl Write for SlowWire {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.slow && !bytes.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
                self.socket.write(&bytes[..1])
            } else {
                self.socket.write(bytes)
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.socket.flush()
        }
    }
    let (client, server) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let mut wire = SlowWire {
                socket,
                slow: false,
            };
            let mut connection = rustls::ServerConnection::new(server).unwrap();
            while connection.is_handshaking() {
                connection.complete_io(&mut wire).unwrap();
            }
            wire.slow = true;
            let mut stream = rustls::StreamOwned::new(connection, wire);
            ready_tx.send(()).unwrap();
            let _result = stream
                .write_all(b"encrypted payload")
                .and_then(|()| stream.flush());
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        let mut stream = TlsReplicaStream::connect(
            socket,
            client,
            name,
            SERVER,
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
        ready_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        stream.set_io_timeout(Duration::from_millis(35)).unwrap();
        let started = Instant::now();
        let mut payload = [0_u8; 17];
        assert!(stream.read_exact(&mut payload).is_err());
        assert!(started.elapsed() < Duration::from_millis(250));
        drop(stream);
        worker.join().unwrap();
    });
}

#[test]
fn client_rejects_wrong_server_name_and_untrusted_ca() {
    for wrong_name in [true, false] {
        let (client, server) = configs();
        let client = if wrong_name {
            client
        } else {
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_client_auth_cert(
                        vec![CertificateDer::from(CLIENT.to_vec())],
                        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(CLIENT_KEY.to_vec())),
                    )
                    .unwrap(),
            )
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let (socket, _) = listener.accept().unwrap();
                assert!(
                    TlsReplicaStream::accept(
                        socket,
                        server,
                        CLIENT,
                        Instant::now() + Duration::from_secs(3)
                    )
                    .is_err()
                );
            });
            let socket = std::net::TcpStream::connect(address).unwrap();
            let name = ServerName::try_from(
                if wrong_name {
                    "other.test"
                } else {
                    "server.test"
                }
                .to_string(),
            )
            .unwrap();
            assert!(
                TlsReplicaStream::connect(
                    socket,
                    client,
                    name,
                    SERVER,
                    Instant::now() + Duration::from_secs(3)
                )
                .is_err()
            );
            worker.join().unwrap();
        });
    }
}

#[test]
fn client_rejects_wrong_server_pin() {
    let (client, server) = configs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let (socket, _) = listener.accept().unwrap();
            let _result = TlsReplicaStream::accept(
                socket,
                server,
                CLIENT,
                Instant::now() + Duration::from_secs(3),
            );
        });
        let socket = std::net::TcpStream::connect(address).unwrap();
        let name = ServerName::try_from("server.test".to_string()).unwrap();
        assert!(
            TlsReplicaStream::connect(
                socket,
                client,
                name,
                OTHER,
                Instant::now() + Duration::from_secs(3)
            )
            .is_err()
        );
        worker.join().unwrap();
    });
}
