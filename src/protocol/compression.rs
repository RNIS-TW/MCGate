//! Port of `protocol/Compression.kt` — Minecraft's packet compression framing (stable since
//! 1.8): once a connection has negotiated a compression threshold (via the Login-state Set
//! Compression packet), every packet afterward is framed as
//! `[packet length][data length][payload]`, where `data length` is 0 for a payload left
//! uncompressed (below threshold) or the decompressed size for a zlib-deflated payload.
//!
//! MCGate's raw byte relay never needs this — it just tunnels whatever bytes the backend and
//! client exchange. It only matters for packets MCGate *synthesizes* itself (the reconnect-wait
//! world, section 3), which must match whatever framing the client's decoder already expects.
//!
//! Java's `Deflater`/`Inflater` default to zlib (RFC 1950) framing, not raw DEFLATE — this uses
//! `flate2`'s `Zlib{Encoder,Decoder}` to match exactly.

use std::fmt;
use std::io::Write;

use crate::protocol::varint::{length_prefix, read_var_int, write_var_int, MAX_PACKET_BYTES};

#[derive(Debug)]
pub enum CompressionError {
    DataLengthOutOfRange(i32),
    CompressedPayloadOutOfRange(i32),
    Deflate(std::io::Error),
    Truncated,
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::DataLengthOutOfRange(n) => write!(f, "compressed data length {n} out of range (max {MAX_PACKET_BYTES})"),
            CompressionError::CompressedPayloadOutOfRange(n) => write!(f, "compressed payload length {n} out of range"),
            CompressionError::Deflate(e) => write!(f, "zlib error: {e}"),
            CompressionError::Truncated => write!(f, "truncated compressed frame"),
        }
    }
}
impl std::error::Error for CompressionError {}

/// Frames `payload` (packet id + fields, unframed) for the wire, applying compression if
/// `compression_threshold >= 0`.
pub fn frame(payload: &[u8], compression_threshold: i32) -> Vec<u8> {
    if compression_threshold < 0 {
        return length_prefix(payload);
    }

    let uncompressed_size = payload.len();
    let mut inner = Vec::new();
    if (uncompressed_size as i32) < compression_threshold {
        write_var_int(&mut inner, 0);
        inner.extend_from_slice(payload);
    } else {
        write_var_int(&mut inner, uncompressed_size as i32);
        inner.extend_from_slice(&deflate(payload));
    }
    length_prefix(&inner)
}

fn deflate(src: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(src).expect("writing to an in-memory Vec cannot fail");
    encoder.finish().expect("finishing an in-memory zlib stream cannot fail")
}

/// Reads the packet id from the front of `frame_payload` (everything after the outer
/// packet-length varint has already been stripped by the caller, i.e. this is the compression
/// frame's full contents) and returns it along with the remaining bytes (the packet's fields),
/// honoring `compression_threshold`.
pub fn read_compressed_frame(frame_payload: &[u8], compression_threshold: i32) -> Result<(i32, Vec<u8>), CompressionError> {
    if compression_threshold < 0 {
        let (id, consumed) = read_var_int(frame_payload).map_err(|_| CompressionError::Truncated)?;
        return Ok((id, frame_payload[consumed..].to_vec()));
    }
    let (data_length, consumed) = read_var_int(frame_payload).map_err(|_| CompressionError::Truncated)?;
    let rest = &frame_payload[consumed..];
    if data_length == 0 {
        let (id, id_consumed) = read_var_int(rest).map_err(|_| CompressionError::Truncated)?;
        return Ok((id, rest[id_consumed..].to_vec()));
    }
    // data_length is the peer's claim of the *decompressed* size — it drives the allocation
    // below. Unbounded, one crafted frame (data_length ~= i32::MAX) is a huge-allocation DoS.
    // Minecraft never frames a packet larger than MAX_PACKET_BYTES; anything past that is
    // malformed.
    if data_length < 0 || data_length > MAX_PACKET_BYTES {
        return Err(CompressionError::DataLengthOutOfRange(data_length));
    }
    let compressed_len = rest.len();
    if compressed_len > MAX_PACKET_BYTES as usize {
        return Err(CompressionError::CompressedPayloadOutOfRange(compressed_len as i32));
    }
    let inflated = inflate(rest, data_length as usize)?;
    let (id, id_consumed) = read_var_int(&inflated).map_err(|_| CompressionError::Truncated)?;
    Ok((id, inflated[id_consumed..].to_vec()))
}

pub fn inflate(src: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError> {
    use std::io::Read;
    let mut decoder = flate2::read::ZlibDecoder::new(src).take(expected_size as u64);
    let mut out = Vec::with_capacity(expected_size);
    decoder.read_to_end(&mut out).map_err(CompressionError::Deflate)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_with_id(id: i32, fields: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_var_int(&mut out, id);
        out.extend_from_slice(fields);
        out
    }

    #[test]
    fn uncompressed_round_trip() {
        let payload = payload_with_id(0x42, b"hello");
        let wire = frame(&payload, -1);
        let (len, pos) = crate::protocol::varint::read_var_int(&wire).unwrap();
        assert_eq!(len as usize, wire.len() - pos);
        let (id, fields) = read_compressed_frame(&wire[pos..], -1).unwrap();
        assert_eq!(id, 0x42);
        assert_eq!(fields, b"hello");
    }

    #[test]
    fn compressed_below_threshold_stays_uncompressed() {
        let payload = payload_with_id(0x01, b"short");
        let wire = frame(&payload, 100); // threshold way above payload size
        let (_, pos) = crate::protocol::varint::read_var_int(&wire).unwrap();
        let (id, fields) = read_compressed_frame(&wire[pos..], 100).unwrap();
        assert_eq!(id, 0x01);
        assert_eq!(fields, b"short");
    }

    #[test]
    fn compressed_above_threshold_round_trips() {
        let big_fields = vec![b'x'; 500];
        let payload = payload_with_id(0x05, &big_fields);
        let wire = frame(&payload, 10); // well below payload size, forces compression
        let (_, pos) = crate::protocol::varint::read_var_int(&wire).unwrap();
        let (id, fields) = read_compressed_frame(&wire[pos..], 10).unwrap();
        assert_eq!(id, 0x05);
        assert_eq!(fields, big_fields);
    }

    #[test]
    fn oversized_data_length_rejected() {
        let mut frame_payload = Vec::new();
        write_var_int(&mut frame_payload, MAX_PACKET_BYTES + 1);
        assert!(read_compressed_frame(&frame_payload, 10).is_err());
    }
}
