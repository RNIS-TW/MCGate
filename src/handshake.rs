//! Port of `handler/HandshakeSniffer.kt`, adapted from Netty's incremental
//! `ByteToMessageDecoder` (called again each time more bytes arrive) to a single async function
//! that reads from the stream in a loop until a complete handshake is buffered.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::varint::{read_var_int, VarIntError};

/// Upper bound on a handshake frame's declared body length. A legitimate handshake is ~30-280
/// bytes (the host field alone is capped at 255); this leaves a wide margin while still cutting
/// off the slow-loris amplification vector of a declared-huge-frame-then-dribble attack.
const MAX_HANDSHAKE_FRAME_BYTES: i32 = 512;

#[derive(Debug)]
pub enum HandshakeError {
    Io(std::io::Error),
    /// The peer closed the connection before a complete handshake arrived.
    Eof,
    /// The bytes received so far can't be a valid handshake — reason is for logging only.
    Malformed(String),
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::Io(e) => write!(f, "io error: {e}"),
            HandshakeError::Eof => write!(f, "connection closed before handshake completed"),
            HandshakeError::Malformed(reason) => write!(f, "malformed handshake: {reason}"),
        }
    }
}
impl std::error::Error for HandshakeError {}

#[derive(Debug)]
pub struct Handshake {
    pub protocol_version: i32,
    pub host: String,
    pub port: u16,
    pub next_state: i32,
    /// The raw frame bytes (length-prefix + payload) exactly as received, for forwarding
    /// untouched to a backend that doesn't need `modifyVirtualHost` rewriting.
    pub raw_frame: Vec<u8>,
}

pub(crate) enum ParseOutcome {
    Complete(Handshake),
    Incomplete,
    Malformed(String),
}

/// Reads a single handshake packet from `reader`, looping until enough bytes have arrived (or
/// rejecting outright once the declared length proves it can't be a handshake).
pub async fn read_handshake<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Handshake, HandshakeError> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    loop {
        match try_parse(&buf) {
            ParseOutcome::Complete(h) => return Ok(h),
            ParseOutcome::Malformed(reason) => return Err(HandshakeError::Malformed(reason)),
            ParseOutcome::Incomplete => {}
        }
        let mut chunk = [0u8; 512];
        let n = reader.read(&mut chunk).await.map_err(HandshakeError::Io)?;
        if n == 0 {
            return Err(HandshakeError::Eof);
        }
        buf.extend_from_slice(&chunk[..n]);
        // A well-formed handshake is bounded by MAX_HANDSHAKE_FRAME_BYTES plus a few bytes of
        // varint/length overhead; anything past that without completing is either a lying
        // declared length (already rejected in try_parse once the length varint itself is
        // readable) or a peer sending garbage one byte at a time forever - bound the buffer
        // regardless so that pathological case can't grow it unboundedly either.
        if buf.len() > MAX_HANDSHAKE_FRAME_BYTES as usize + 16 {
            return Err(HandshakeError::Malformed("handshake exceeded maximum size without completing".into()));
        }
    }
}

pub(crate) fn try_parse(buf: &[u8]) -> ParseOutcome {
    let (length, header_len) = match read_var_int(buf) {
        Ok(v) => v,
        Err(VarIntError::Incomplete) => return ParseOutcome::Incomplete,
        Err(VarIntError::TooBig) => return ParseOutcome::Malformed("bogus handshake length".into()),
    };
    if !(0..=MAX_HANDSHAKE_FRAME_BYTES).contains(&length) {
        return ParseOutcome::Malformed(format!("bogus handshake length {length}"));
    }
    let length = length as usize;
    if buf.len() - header_len < length {
        return ParseOutcome::Incomplete;
    }
    let mut pos = header_len;

    let (packet_id, consumed) = match read_var_int(&buf[pos..]) {
        Ok(v) => v,
        Err(_) => return ParseOutcome::Malformed("bad packet id".into()),
    };
    pos += consumed;
    if packet_id != 0x00 {
        return ParseOutcome::Malformed(format!("first packet id was {packet_id}, expected handshake (0)"));
    }

    let (protocol_version, consumed) = match read_var_int(&buf[pos..]) {
        Ok(v) => v,
        Err(_) => return ParseOutcome::Malformed("bad protocol version".into()),
    };
    pos += consumed;

    let (host_length, consumed) = match read_var_int(&buf[pos..]) {
        Ok(v) => v,
        Err(_) => return ParseOutcome::Malformed("bad host length".into()),
    };
    pos += consumed;
    if !(0..=255).contains(&host_length) {
        return ParseOutcome::Malformed(format!("bogus host length {host_length}"));
    }
    let host_length = host_length as usize;
    if buf.len() < pos + host_length + 2 {
        // Shouldn't happen given the frame-length check above for a well-formed frame, but a
        // lying host length inside an otherwise-short declared frame could reach past what's
        // buffered - treat as malformed rather than panicking on an out-of-bounds slice.
        return ParseOutcome::Malformed("host/port fields exceed declared frame length".into());
    }
    let mut host = String::from_utf8_lossy(&buf[pos..pos + host_length]).into_owned();
    pos += host_length;

    // Strip Forge/FML markers appended after a NUL char (e.g. "host\0FML...").
    if let Some(nul) = host.find('\0') {
        host.truncate(nul);
    }
    // Strip a trailing DNS root dot (e.g. "sv.xxx.com."), which some clients/launchers send.
    while host.ends_with('.') {
        host.pop();
    }

    let port = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
    pos += 2;

    let (next_state, _consumed) = match read_var_int(&buf[pos..]) {
        Ok(v) => v,
        Err(_) => return ParseOutcome::Malformed("bad next state".into()),
    };

    let frame_end = header_len + length;
    ParseOutcome::Complete(Handshake { protocol_version, host, port, next_state, raw_frame: buf[..frame_end].to_vec() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::minecraft_protocol::encode_handshake;
    use std::io::Cursor;

    #[tokio::test]
    async fn reads_a_complete_handshake_in_one_shot() {
        let frame = encode_handshake(763, "play.example.com", 25565, 2);
        let mut cursor = Cursor::new(frame.clone());
        let h = read_handshake(&mut cursor).await.unwrap();
        assert_eq!(h.protocol_version, 763);
        assert_eq!(h.host, "play.example.com");
        assert_eq!(h.port, 25565);
        assert_eq!(h.next_state, 2);
        assert_eq!(h.raw_frame, frame);
    }

    #[tokio::test]
    async fn reads_a_handshake_split_across_many_small_reads() {
        // A byte-at-a-time reader forces read_handshake's loop to run many times, exercising the
        // "incomplete, read more" path repeatedly rather than completing on the first read.
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
        let frame = encode_handshake(763, "a.example.com", 25565, 1);
        let mut reader = OneByteAtATime(frame.clone(), 0);
        let h = read_handshake(&mut reader).await.unwrap();
        assert_eq!(h.host, "a.example.com");
        assert_eq!(h.raw_frame, frame);
    }

    #[tokio::test]
    async fn rejects_oversized_declared_length() {
        let mut bogus = Vec::new();
        crate::varint::write_var_int(&mut bogus, 10_000); // way past MAX_HANDSHAKE_FRAME_BYTES
        let mut cursor = Cursor::new(bogus);
        let err = read_handshake(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HandshakeError::Malformed(_)));
    }

    #[tokio::test]
    async fn rejects_wrong_packet_id() {
        let mut payload = Vec::new();
        crate::varint::write_var_int(&mut payload, 0x05); // not 0x00
        let frame = crate::varint::length_prefix(&payload);
        let mut cursor = Cursor::new(frame);
        let err = read_handshake(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HandshakeError::Malformed(_)));
    }

    #[tokio::test]
    async fn strips_trailing_dot_and_fml_marker() {
        let frame = encode_handshake(763, "play.example.com.\u{0}FML", 25565, 1);
        let mut cursor = Cursor::new(frame);
        let h = read_handshake(&mut cursor).await.unwrap();
        assert_eq!(h.host, "play.example.com");
    }

    #[tokio::test]
    async fn eof_before_complete_is_an_error() {
        let frame = encode_handshake(763, "play.example.com", 25565, 1);
        let mut cursor = Cursor::new(frame[..frame.len() - 3].to_vec());
        let err = read_handshake(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HandshakeError::Eof));
    }
}
