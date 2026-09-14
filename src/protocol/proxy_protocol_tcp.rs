//! TCP-stream framing around `proxy_protocol_datagram::parse_proxy_protocol_header`'s pure byte
//! parser — the streaming counterpart to Netty's `HAProxyMessageDecoder`
//! (`ProxyProtocolAttributeHandler.kt`'s upstream decoder). Reads only as many bytes as the
//! header declares before handing off to the shared parser, so it never over-reads into what
//! comes after the header (the Minecraft handshake).

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::protocol::proxy_protocol_datagram::{parse_proxy_protocol_header, ProxyProtocolFormatError, V1_PREFIX, V2_SIGNATURE};
use std::net::SocketAddr;

fn err(msg: impl Into<String>) -> ProxyProtocolFormatError {
    ProxyProtocolFormatError(msg.into())
}

async fn read_exact_more<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut Vec<u8>, target_len: usize) -> Result<(), ProxyProtocolFormatError> {
    while buf.len() < target_len {
        let mut chunk = vec![0u8; target_len - buf.len()];
        let n = reader.read(&mut chunk).await.map_err(|e| err(format!("io error reading PROXY header: {e}")))?;
        if n == 0 {
            return Err(err("connection closed before PROXY header completed"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(())
}

/// Reads a PROXY protocol v1/v2 header from the front of `reader`, consuming exactly the header's
/// bytes and no more. Returns the source address it carries, or `None` for a v2 `LOCAL` command /
/// `UNSPEC` family (health checks — no client address to route by, caller should fall back to the
/// TCP connection's own peer address).
pub async fn read_proxy_protocol_header<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<SocketAddr>, ProxyProtocolFormatError> {
    let mut buf = Vec::with_capacity(16);
    read_exact_more(reader, &mut buf, 16).await?;

    if buf.starts_with(&V2_SIGNATURE) {
        let addr_len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
        read_exact_more(reader, &mut buf, 16 + addr_len).await?;
    } else if buf.starts_with(V1_PREFIX) {
        // The line ends with CRLF and is at most 107 bytes including it - read one byte at a
        // time (a v1 header is at most 108 bytes total, so this is bounded and cheap) until
        // found, rather than guessing a length up front the way v2's binary header lets us.
        loop {
            if buf.windows(2).any(|w| w == [0x0D, 0x0A]) {
                break;
            }
            if buf.len() >= 108 {
                return Err(err("PROXY v1 header has no CRLF within 107 bytes"));
            }
            let target = buf.len() + 1;
            read_exact_more(reader, &mut buf, target).await?;
        }
    } else {
        return Err(err("no PROXY protocol v1 or v2 signature"));
    }

    let (addr, _consumed) = parse_proxy_protocol_header(&buf)?;
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn v1_header_parses_and_consumes_exactly_the_header() {
        let mut data = b"PROXY TCP4 192.168.1.1 192.168.1.2 5555 25565\r\n".to_vec();
        data.extend_from_slice(b"handshake bytes follow");
        let mut cursor = Cursor::new(data.clone());
        let addr = read_proxy_protocol_header(&mut cursor).await.unwrap();
        assert_eq!(addr, Some("192.168.1.1:5555".parse().unwrap()));
        // Whatever comes after the header must still be readable by the caller afterward.
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut cursor, &mut rest).await.unwrap();
        assert_eq!(rest, b"handshake bytes follow");
    }

    #[tokio::test]
    async fn v2_header_parses_and_consumes_exactly_the_header() {
        let mut data = V2_SIGNATURE.to_vec();
        data.push(0x21); // version 2, command PROXY
        data.push(0x11); // family INET, proto STREAM
        data.extend_from_slice(&12u16.to_be_bytes());
        data.extend_from_slice(&[10, 0, 0, 1]);
        data.extend_from_slice(&[10, 0, 0, 2]);
        data.extend_from_slice(&5555u16.to_be_bytes());
        data.extend_from_slice(&25565u16.to_be_bytes());
        data.extend_from_slice(b"handshake bytes follow");

        let mut cursor = Cursor::new(data);
        let addr = read_proxy_protocol_header(&mut cursor).await.unwrap();
        assert_eq!(addr, Some("10.0.0.1:5555".parse().unwrap()));
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut cursor, &mut rest).await.unwrap();
        assert_eq!(rest, b"handshake bytes follow");
    }

    #[tokio::test]
    async fn no_signature_errors() {
        let mut cursor = Cursor::new(b"not a proxy header at all, just garbage".to_vec());
        assert!(read_proxy_protocol_header(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn v1_split_across_many_small_reads() {
        struct OneByteAtATime(Vec<u8>, usize);
        impl AsyncRead for OneByteAtATime {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if self.1 < self.0.len() {
                    buf.put_slice(&[self.0[self.1]]);
                    self.1 += 1;
                }
                std::task::Poll::Ready(Ok(()))
            }
        }
        let data = b"PROXY TCP4 1.2.3.4 5.6.7.8 111 222\r\n".to_vec();
        let mut reader = OneByteAtATime(data, 0);
        let addr = read_proxy_protocol_header(&mut reader).await.unwrap();
        assert_eq!(addr, Some("1.2.3.4:111".parse().unwrap()));
    }
}
