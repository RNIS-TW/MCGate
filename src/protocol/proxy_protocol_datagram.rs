//! Port of `protocol/ProxyProtocolDatagram.kt` — a minimal PROXY protocol (v1 and v2) header
//! parser for the datagram case. An L4 proxy such as Cloudflare Spectrum prepends one of these to
//! *every* UDP datagram it forwards to the origin, so MCGate's UDP relays (section 6, not yet
//! ported) need to strip it per packet to recover the real client address.
//!
//! Unlike the TCP case (`netty-codec-haproxy`'s `HAProxyMessageDecoder`, or in Rust the `ppp`
//! crate), this hand-rolls the parse: it's a one-shot header at the front of a single datagram,
//! not a stream decoder, and keeping it hand-rolled matches the exact edge-case behavior
//! (LOCAL/UNSPEC handling, truncation errors) the rest of this port targets bit-for-bit against
//! the Kotlin version.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// PROXY v2 12-byte signature: `\r\n\r\n\0\r\nQUIT\n`. `pub(crate)` so `proxy_protocol_tcp.rs` can
/// reuse it for its own incremental (read-until-enough-bytes) framing.
pub(crate) const V2_SIGNATURE: [u8; 12] = [0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A];
pub(crate) const V1_PREFIX: &[u8] = b"PROXY ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyProtocolFormatError(pub String);

impl fmt::Display for ProxyProtocolFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ProxyProtocolFormatError {}

fn err(msg: impl Into<String>) -> ProxyProtocolFormatError {
    ProxyProtocolFormatError(msg.into())
}

fn starts_with(buf: &[u8], prefix: &[u8]) -> bool {
    buf.len() >= prefix.len() && &buf[..prefix.len()] == prefix
}

/// Parses a PROXY protocol header from the front of `buf`. Returns the source address the header
/// carries (or `None` for a v2 `LOCAL` command / an `UNSPEC` address family — health checks and
/// the like, no client address to route by) along with the number of bytes the header occupied,
/// so the caller can slice off the rest of the datagram as the real payload.
pub fn parse_proxy_protocol_header(buf: &[u8]) -> Result<(Option<SocketAddr>, usize), ProxyProtocolFormatError> {
    if buf.len() >= 16 && starts_with(buf, &V2_SIGNATURE) {
        return parse_v2(buf);
    }
    if starts_with(buf, V1_PREFIX) {
        return parse_v1(buf);
    }
    Err(err("no PROXY protocol v1 or v2 signature"))
}

fn parse_v2(buf: &[u8]) -> Result<(Option<SocketAddr>, usize), ProxyProtocolFormatError> {
    let ver_cmd = buf[12];
    if ver_cmd >> 4 != 0x2 {
        return Err(err("PROXY v2 version nibble != 2"));
    }
    let command = ver_cmd & 0x0F; // 0 = LOCAL, 1 = PROXY

    let fam_proto = buf[13];
    let family = fam_proto >> 4; // 0 = UNSPEC, 1 = INET, 2 = INET6

    let addr_len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    let header_len = 16 + addr_len;
    if buf.len() < header_len {
        return Err(err(format!("PROXY v2 header truncated (need {header_len} bytes)")));
    }

    let result = if command == 0 || family == 0 {
        None // LOCAL or UNSPEC - nothing to route by
    } else if family == 1 && addr_len >= 12 {
        let addr = Ipv4Addr::new(buf[16], buf[17], buf[18], buf[19]);
        let port = u16::from_be_bytes([buf[24], buf[25]]);
        Some(SocketAddr::new(IpAddr::V4(addr), port))
    } else if family == 2 && addr_len >= 36 {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&buf[16..32]);
        let addr = Ipv6Addr::from(octets);
        let port = u16::from_be_bytes([buf[48], buf[49]]);
        Some(SocketAddr::new(IpAddr::V6(addr), port))
    } else {
        return Err(err(format!("PROXY v2 address block too short for family {family}")));
    };

    Ok((result, header_len))
}

fn parse_v1(buf: &[u8]) -> Result<(Option<SocketAddr>, usize), ProxyProtocolFormatError> {
    // The line ends with CRLF and is at most 107 bytes including it.
    let limit = buf.len().min(108);
    let mut crlf = None;
    for i in 1..limit {
        if buf[i - 1] == 0x0D && buf[i] == 0x0A {
            crlf = Some(i - 1);
            break;
        }
    }
    let Some(crlf) = crlf else { return Err(err("PROXY v1 header has no CRLF within 107 bytes")) };

    let line = String::from_utf8_lossy(&buf[..crlf]).into_owned();
    let consumed = crlf + 2;

    let parts: Vec<&str> = line.split(' ').collect();
    // "PROXY UNKNOWN ..." - connection info withheld, nothing to route by.
    if parts.len() >= 2 && parts[1] == "UNKNOWN" {
        return Ok((None, consumed));
    }
    if parts.len() < 6 {
        return Err(err(format!("PROXY v1 header has too few fields: '{line}'")));
    }
    let ip: IpAddr = parts[2].parse().map_err(|_| err(format!("PROXY v1 header has an unparseable source address: '{line}'")))?;
    let port: u16 = parts[4].parse().map_err(|_| err(format!("PROXY v1 header has an unparseable source address: '{line}'")))?;
    Ok((Some(SocketAddr::new(ip, port)), consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_ipv4_parses() {
        let line = b"PROXY TCP4 192.168.1.1 192.168.1.2 5555 25565\r\nrest";
        let (addr, consumed) = parse_proxy_protocol_header(line).unwrap();
        assert_eq!(addr, Some("192.168.1.1:5555".parse().unwrap()));
        assert_eq!(&line[consumed..], b"rest");
    }

    #[test]
    fn v1_unknown_returns_none() {
        let line = b"PROXY UNKNOWN\r\nrest";
        let (addr, consumed) = parse_proxy_protocol_header(line).unwrap();
        assert_eq!(addr, None);
        assert_eq!(&line[consumed..], b"rest");
    }

    #[test]
    fn v1_missing_crlf_errors() {
        let line = b"PROXY TCP4 192.168.1.1 192.168.1.2 5555 25565 no newline here at all padding padding padding padding padding padding padding padding";
        assert!(parse_proxy_protocol_header(line).is_err());
    }

    #[test]
    fn v2_ipv4_proxy_command_parses() {
        let mut buf = V2_SIGNATURE.to_vec();
        buf.push(0x21); // version 2, command PROXY
        buf.push(0x11); // family INET, proto STREAM
        buf.extend_from_slice(&12u16.to_be_bytes()); // addr len
        buf.extend_from_slice(&[10, 0, 0, 1]); // src addr
        buf.extend_from_slice(&[10, 0, 0, 2]); // dst addr
        buf.extend_from_slice(&5555u16.to_be_bytes()); // src port
        buf.extend_from_slice(&25565u16.to_be_bytes()); // dst port
        buf.extend_from_slice(b"payload");

        let (addr, consumed) = parse_proxy_protocol_header(&buf).unwrap();
        assert_eq!(addr, Some("10.0.0.1:5555".parse().unwrap()));
        assert_eq!(&buf[consumed..], b"payload");
    }

    #[test]
    fn v2_local_command_returns_none() {
        let mut buf = V2_SIGNATURE.to_vec();
        buf.push(0x20); // version 2, command LOCAL
        buf.push(0x00); // family UNSPEC
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(b"payload");

        let (addr, consumed) = parse_proxy_protocol_header(&buf).unwrap();
        assert_eq!(addr, None);
        assert_eq!(&buf[consumed..], b"payload");
    }

    #[test]
    fn v2_truncated_header_errors() {
        let mut buf = V2_SIGNATURE.to_vec();
        buf.push(0x21);
        buf.push(0x11);
        buf.extend_from_slice(&12u16.to_be_bytes());
        // missing the 12 address bytes
        assert!(parse_proxy_protocol_header(&buf).is_err());
    }

    #[test]
    fn no_signature_errors() {
        assert!(parse_proxy_protocol_header(b"just some random bytes").is_err());
    }
}
