//! Port of the packet encoders/decoders in `protocol/MinecraftProtocol.kt` (varint/frame-length
//! primitives themselves live in `varint.rs`).

use std::net::SocketAddr;

use uuid::Uuid;

use crate::varint::{length_prefix, read_string, read_var_int, write_string, write_var_int};

/// PROXY protocol v1 preamble — required before the handshake on any backend connection when the
/// route has `proxyProtocol: true`, including status/health-check probes: a backend enforcing it
/// will silently withhold its response (never closing the connection either) for any dial that
/// skips this header, which looks identical to a hung/unreachable backend from the caller's side.
pub fn encode_proxy_protocol_header(source_addr: SocketAddr, dest_addr: SocketAddr) -> Vec<u8> {
    let proto = if source_addr.is_ipv6() { "TCP6" } else { "TCP4" };
    let header = format!(
        "PROXY {proto} {} {} {} {}\r\n",
        source_addr.ip(),
        dest_addr.ip(),
        source_addr.port(),
        dest_addr.port()
    );
    header.into_bytes()
}

/// Encodes a length-prefixed handshake packet (id 0x00) from scratch.
pub fn encode_handshake(protocol_version: i32, host: &str, port: u16, next_state: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_var_int(&mut payload, protocol_version);
    write_string(&mut payload, host);
    payload.extend_from_slice(&port.to_be_bytes());
    write_var_int(&mut payload, next_state);
    length_prefix(&payload)
}

/// Encodes a length-prefixed Status Response packet (id 0x00) carrying the given JSON.
pub fn encode_status_response(json: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_string(&mut payload, json);
    length_prefix(&payload)
}

/// Best-effort parse of a Login Start packet (id 0x00): username, and UUID if the packet has
/// exactly a trailing 16 bytes after the name (the shape used by modern protocols that send a
/// mandatory UUID with no other trailing fields). `buf` must contain the full frame starting at
/// the frame-length VarInt. Returns `None` if it isn't a complete, well-formed Login Start packet
/// yet.
pub fn parse_login_start(buf: &[u8]) -> Option<(String, Option<Uuid>)> {
    let (length, mut pos) = read_var_int(buf).ok()?;
    let payload_start = pos;
    if length < 0 {
        return None;
    }
    if buf.len() - pos < length as usize {
        return None;
    }
    let (packet_id, consumed) = read_var_int(&buf[pos..]).ok()?;
    if packet_id != 0x00 {
        return None;
    }
    pos += consumed;
    let (name, consumed) = read_string(&buf[pos..], 16).ok()?;
    pos += consumed;
    let payload_end = payload_start + length as usize;
    let remaining = payload_end.checked_sub(pos)?;
    let uuid = if remaining == 16 {
        let bytes = buf.get(pos..pos + 16)?;
        let most = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let least = i64::from_be_bytes(bytes[8..16].try_into().ok()?);
        Some(uuid_from_bits(most, least))
    } else {
        None
    };
    Some((name, uuid))
}

fn uuid_from_bits(most_significant: i64, least_significant: i64) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&most_significant.to_be_bytes());
    bytes[8..16].copy_from_slice(&least_significant.to_be_bytes());
    Uuid::from_bytes(bytes)
}

fn uuid_to_bits(uuid: Uuid) -> (i64, i64) {
    let bytes = uuid.into_bytes();
    let most = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let least = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    (most, least)
}

/// Encodes a length-prefixed Login Start packet (id 0x00) — the inverse of [`parse_login_start`],
/// used to replay a client's login against a backend without the original raw bytes on hand.
pub fn encode_login_start(name: &str, uuid: Option<Uuid>) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_string(&mut payload, name);
    if let Some(uuid) = uuid {
        let (most, least) = uuid_to_bits(uuid);
        payload.extend_from_slice(&most.to_be_bytes());
        payload.extend_from_slice(&least.to_be_bytes());
    }
    length_prefix(&payload)
}

/// Encodes a length-prefixed Pong packet (id 0x01) echoing the ping payload.
pub fn encode_pong(payload: i64) -> Vec<u8> {
    let mut body = Vec::new();
    write_var_int(&mut body, 0x01);
    body.extend_from_slice(&payload.to_be_bytes());
    length_prefix(&body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trip_shape() {
        let frame = encode_handshake(763, "play.example.com", 25565, 1);
        // frame-length varint, then packet-id 0x00, protocol varint, string, short port, state varint
        let (len, mut pos) = crate::varint::read_var_int(&frame).unwrap();
        assert_eq!(len as usize, frame.len() - pos);
        let (id, c) = crate::varint::read_var_int(&frame[pos..]).unwrap();
        pos += c;
        assert_eq!(id, 0);
        let (protocol, c) = crate::varint::read_var_int(&frame[pos..]).unwrap();
        pos += c;
        assert_eq!(protocol, 763);
        let (host, c) = crate::varint::read_string(&frame[pos..], 32767).unwrap();
        pos += c;
        assert_eq!(host, "play.example.com");
        let port = u16::from_be_bytes(frame[pos..pos + 2].try_into().unwrap());
        pos += 2;
        assert_eq!(port, 25565);
        let (next_state, _) = crate::varint::read_var_int(&frame[pos..]).unwrap();
        assert_eq!(next_state, 1);
    }

    #[test]
    fn login_start_round_trip_with_uuid() {
        let uuid = Uuid::new_v4();
        let frame = encode_login_start("Steve", Some(uuid));
        let (name, parsed_uuid) = parse_login_start(&frame).unwrap();
        assert_eq!(name, "Steve");
        assert_eq!(parsed_uuid, Some(uuid));
    }

    #[test]
    fn login_start_round_trip_without_uuid() {
        let frame = encode_login_start("Steve", None);
        let (name, parsed_uuid) = parse_login_start(&frame).unwrap();
        assert_eq!(name, "Steve");
        assert_eq!(parsed_uuid, None);
    }

    #[test]
    fn login_start_incomplete_returns_none() {
        let frame = encode_login_start("Steve", Some(Uuid::new_v4()));
        assert_eq!(parse_login_start(&frame[..frame.len() - 1]), None);
    }

    #[test]
    fn login_start_wrong_packet_id_returns_none() {
        let mut payload = Vec::new();
        write_var_int(&mut payload, 0x01); // wrong id
        write_string(&mut payload, "Steve");
        let frame = length_prefix(&payload);
        assert_eq!(parse_login_start(&frame), None);
    }

    #[test]
    fn pong_encodes_payload() {
        let frame = encode_pong(0x1234_5678_9abc_def0);
        let (_, mut pos) = crate::varint::read_var_int(&frame).unwrap();
        let (id, c) = crate::varint::read_var_int(&frame[pos..]).unwrap();
        pos += c;
        assert_eq!(id, 1);
        let payload = i64::from_be_bytes(frame[pos..pos + 8].try_into().unwrap());
        assert_eq!(payload, 0x1234_5678_9abc_def0);
    }

    #[test]
    fn proxy_protocol_header_v1_shape() {
        let src: SocketAddr = "1.2.3.4:5555".parse().unwrap();
        let dst: SocketAddr = "10.0.0.1:25565".parse().unwrap();
        let header = String::from_utf8(encode_proxy_protocol_header(src, dst)).unwrap();
        assert_eq!(header, "PROXY TCP4 1.2.3.4 10.0.0.1 5555 25565\r\n");
    }
}
