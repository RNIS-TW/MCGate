//! Port of `routing/BackendPinger.kt` — dials a backend right now and performs a real Server
//! List Ping, independent of any client traffic or the `PingCache`. Used for on-demand health
//! checks (the API's live `/ping` endpoint) as well as `server.rs`'s client-triggered status path
//! (which additionally validates and caches the result — see `ping_cache.rs`).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

use crate::buffered_stream::BufferedStream;
use crate::minecraft_protocol::{encode_handshake, encode_proxy_protocol_header};
use crate::varint::{read_var_int, read_string, write_var_int};

pub struct PingResult {
    pub status_json: String,
    pub latency_millis: i64,
}

#[derive(Debug)]
pub enum PingError {
    Connect(std::io::Error),
    Timeout,
    Io(std::io::Error),
    UnexpectedPacketId(i32),
}

impl std::fmt::Display for PingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PingError::Connect(e) => write!(f, "connect failed: {e}"),
            PingError::Timeout => write!(f, "ping timed out"),
            PingError::Io(e) => write!(f, "io error: {e}"),
            PingError::UnexpectedPacketId(id) => write!(f, "unexpected packet id {id} from backend"),
        }
    }
}
impl std::error::Error for PingError {}

/// Opens a fresh connection to `addr` and performs a real Server List Ping (handshake + status
/// request) right now.
pub async fn ping_backend_live(
    addr: SocketAddr,
    protocol_version: i32,
    virtual_host: &str,
    port: u16,
    timeout: Duration,
    proxy_protocol: bool,
) -> Result<PingResult, PingError> {
    let start = Instant::now();
    let connect = tokio::time::timeout(timeout, TcpStream::connect(addr)).await;
    let mut stream = match connect {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(PingError::Connect(e)),
        Err(_) => return Err(PingError::Timeout),
    };
    let _ = stream.set_nodelay(true);

    if proxy_protocol {
        // No real client behind this probe (it's a standalone health check) - a placeholder
        // loopback source is fine, backends enforcing proxyProtocol just need a syntactically
        // valid header before they'll process anything past it.
        let source: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let header = encode_proxy_protocol_header(source, addr);
        tokio::io::AsyncWriteExt::write_all(&mut stream, &header).await.map_err(PingError::Io)?;
    }
    let hs = encode_handshake(protocol_version, virtual_host, port, 1);
    tokio::io::AsyncWriteExt::write_all(&mut stream, &hs).await.map_err(PingError::Io)?;
    let mut request = Vec::new();
    write_var_int(&mut request, 1);
    write_var_int(&mut request, 0x00);
    tokio::io::AsyncWriteExt::write_all(&mut stream, &request).await.map_err(PingError::Io)?;

    let mut buffered = BufferedStream::new(stream);
    let frame = match tokio::time::timeout(timeout, buffered.read_frame(262_144)).await {
        Ok(Ok(f)) => f,
        Ok(Err(e)) => return Err(PingError::Io(e)),
        Err(_) => return Err(PingError::Timeout),
    };
    let (_length, header_len) = read_var_int(&frame).map_err(|_| PingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad frame")))?;
    let (packet_id, consumed) =
        read_var_int(&frame[header_len..]).map_err(|_| PingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad packet id")))?;
    if packet_id != 0x00 {
        return Err(PingError::UnexpectedPacketId(packet_id));
    }
    let (json, _) =
        read_string(&frame[header_len + consumed..], 262_144).map_err(|_| PingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad string")))?;
    Ok(PingResult { status_json: json, latency_millis: start.elapsed().as_millis() as i64 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn ping_backend_live_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let _ = n; // consume handshake + status request (arrives as one or two reads)
            let mut buf2 = vec![0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_millis(100), sock.read(&mut buf2)).await;
            let json = r#"{"version":{"name":"1.21","protocol":767}}"#;
            let mut payload = Vec::new();
            write_var_int(&mut payload, 0x00);
            let mut full = Vec::new();
            write_var_int(&mut full, json.len() as i32);
            full.extend_from_slice(json.as_bytes());
            payload.extend_from_slice(&full);
            let mut frame = Vec::new();
            write_var_int(&mut frame, payload.len() as i32);
            frame.extend_from_slice(&payload);
            tokio::io::AsyncWriteExt::write_all(&mut sock, &frame).await.unwrap();
        });

        let result = ping_backend_live(addr, 767, "example.com", addr.port(), Duration::from_secs(2), false).await.unwrap();
        assert!(result.status_json.contains("767"));
    }

    #[tokio::test]
    async fn ping_backend_live_connect_failure() {
        // Port 0 after binding-and-dropping is unlikely to be listening; use an address nothing
        // listens on instead for a deterministic connection refusal.
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let result = ping_backend_live(addr, 767, "example.com", 1, Duration::from_millis(500), false).await;
        assert!(result.is_err());
    }
}
