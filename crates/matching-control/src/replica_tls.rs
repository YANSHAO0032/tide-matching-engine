//! 使用双向 TLS 和固定对端证书保护外围复制连接。
use crate::ReplicationStream;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 构造要求服务端证书链验证并提供客户端证书的配置。
pub fn replica_client_config(
    roots: &[&[u8]],
    certificate_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    private_key: rustls::pki_types::PrivateKeyDer<'static>,
) -> io::Result<rustls::ClientConfig> {
    rustls::ClientConfig::builder()
        .with_root_certificates(replica_roots(roots)?)
        .with_client_auth_cert(certificate_chain, private_key)
        .map_err(io::Error::other)
}

/// 构造强制验证客户端证书链的服务端配置。
pub fn replica_server_config(
    roots: &[&[u8]],
    certificate_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    private_key: rustls::pki_types::PrivateKeyDer<'static>,
) -> io::Result<rustls::ServerConfig> {
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(replica_roots(roots)?))
        .build()
        .map_err(io::Error::other)?;
    rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificate_chain, private_key)
        .map_err(io::Error::other)
}

/// 只接受非空且可解析的显式信任根。
fn replica_roots(roots: &[&[u8]]) -> io::Result<rustls::RootCertStore> {
    if roots.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "信任根为空"));
    }
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store
            .add(rustls::pki_types::CertificateDer::from(root.to_vec()))
            .map_err(io::Error::other)?;
    }
    Ok(store)
}

/// 已完成证书校验和对端证书绑定的复制流。
pub struct TlsReplicaStream {
    /// 握手完成后的 TLS 连接及有界阻塞套接字。
    stream: TlsStream,
    /// 握手验证并固定的对端证书。
    peer_certificate: Vec<u8>,
}

/// 区分客户端与服务端 TLS 流。
enum TlsStream {
    /// 主动连接端。
    Client(rustls::StreamOwned<rustls::ClientConnection, DeadlineSocket>),
    /// 接收连接端。
    Server(rustls::StreamOwned<rustls::ServerConnection, DeadlineSocket>),
}

/// 让 TLS 内部每次套接字读写共享上层设置的截止时间。
struct DeadlineSocket {
    /// 实际网络连接。
    socket: TcpStream,
    /// 当前操作的截止时间；空值沿用原套接字超时。
    deadline: Option<Instant>,
}
impl DeadlineSocket {
    /// 包装已经完成握手的套接字。
    fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            deadline: None,
        }
    }
    /// 设置供内部全部读写使用的共同期限。
    fn set_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS 读写期限为零",
            ));
        }
        self.deadline = Some(
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS 读写期限越界"))?,
        );
        Ok(())
    }
    /// 返回剩余期限，过期时不得再次发起读写。
    fn remaining(&self) -> io::Result<Option<Duration>> {
        self.deadline
            .map(|deadline| {
                deadline
                    .checked_duration_since(Instant::now())
                    .filter(|value| !value.is_zero())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "TLS 读写期限已过"))
            })
            .transpose()
    }
}
impl Read for DeadlineSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if let Some(timeout) = self.remaining()? {
            self.socket.set_read_timeout(Some(timeout))?;
        }
        let result = self.socket.read(bytes)?;
        self.remaining()?;
        Ok(result)
    }
}
impl Write for DeadlineSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(timeout) = self.remaining()? {
            self.socket.set_write_timeout(Some(timeout))?;
        }
        let result = self.socket.write(bytes)?;
        self.remaining()?;
        Ok(result)
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(timeout) = self.remaining()? {
            self.socket.set_write_timeout(Some(timeout))?;
        }
        self.socket.flush()?;
        self.remaining()?;
        Ok(())
    }
}

impl TlsReplicaStream {
    /// 返回本次握手验证的对端证书，用于检查控制存储中的租约绑定。
    pub fn peer_certificate(&self) -> &[u8] {
        &self.peer_certificate
    }
    /// 使用调用方验证配置完成客户端握手，并校验固定对端证书。
    pub fn connect(
        socket: TcpStream,
        config: Arc<rustls::ClientConfig>,
        server_name: rustls::pki_types::ServerName<'static>,
        peer_der: &[u8],
        deadline: Instant,
    ) -> io::Result<Self> {
        let connection =
            rustls::ClientConnection::new(config, server_name).map_err(io::Error::other)?;
        Self::handshake(
            socket,
            rustls::Connection::Client(connection),
            peer_der,
            deadline,
        )
    }

    /// 使用调用方验证配置完成服务端握手，并校验固定客户端证书；配置须要求客户端证书。
    pub fn accept(
        socket: TcpStream,
        config: Arc<rustls::ServerConfig>,
        peer_der: &[u8],
        deadline: Instant,
    ) -> io::Result<Self> {
        let connection = rustls::ServerConnection::new(config).map_err(io::Error::other)?;
        Self::handshake(
            socket,
            rustls::Connection::Server(connection),
            peer_der,
            deadline,
        )
    }

    /// 在共同截止时间内完成握手，并拒绝缺失或不匹配的对端证书。
    fn handshake(
        mut socket: TcpStream,
        mut connection: rustls::Connection,
        peer_der: &[u8],
        deadline: Instant,
    ) -> io::Result<Self> {
        if peer_der.is_empty() || peer_der.len() > 16 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "对端证书为空"));
        }
        while connection.is_handshaking() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "TLS 握手期限已过"))?;
            socket.set_read_timeout(Some(remaining))?;
            socket.set_write_timeout(Some(remaining))?;
            let mut bounded = crate::replicated_appender::DeadlineStream {
                inner: &mut socket,
                deadline,
            };
            connection.complete_io(&mut bounded)?;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "TLS 握手期限已过"));
        }
        if connection
            .peer_certificates()
            .and_then(|chain| chain.first())
            .is_none_or(|cert| cert.as_ref() != peer_der)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TLS 对端证书不匹配",
            ));
        }
        let socket = DeadlineSocket::new(socket);
        let stream = match connection {
            rustls::Connection::Client(client) => {
                TlsStream::Client(rustls::StreamOwned::new(client, socket))
            }
            rustls::Connection::Server(server) => {
                TlsStream::Server(rustls::StreamOwned::new(server, socket))
            }
        };
        Ok(Self {
            stream,
            peer_certificate: peer_der.to_vec(),
        })
    }
}

impl Read for TlsReplicaStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match &mut self.stream {
            TlsStream::Client(stream) => stream.read(bytes),
            TlsStream::Server(stream) => stream.read(bytes),
        }
    }
}
impl Write for TlsReplicaStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &mut self.stream {
            TlsStream::Client(stream) => stream.write(bytes),
            TlsStream::Server(stream) => stream.write(bytes),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match &mut self.stream {
            TlsStream::Client(stream) => stream.flush(),
            TlsStream::Server(stream) => stream.flush(),
        }
    }
}
impl ReplicationStream for TlsReplicaStream {
    fn set_io_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        let socket = match &mut self.stream {
            TlsStream::Client(stream) => &mut stream.sock,
            TlsStream::Server(stream) => &mut stream.sock,
        };
        socket.set_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underlying_socket_rechecks_shared_deadline_between_reads() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let (mut socket, _) = listener.accept().unwrap();
                for _ in 0..50 {
                    if socket.write_all(&[1]).is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            let mut socket = DeadlineSocket::new(TcpStream::connect(address).unwrap());
            socket.set_timeout(Duration::from_millis(35)).unwrap();
            let mut bytes = [0_u8; 50];
            let started = Instant::now();
            assert!(socket.read_exact(&mut bytes).is_err());
            assert!(started.elapsed() < Duration::from_millis(250));
            drop(socket);
            worker.join().unwrap();
        });
    }
}
