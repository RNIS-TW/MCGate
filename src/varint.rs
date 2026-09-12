//! Port of the VarInt/frame-length/string primitives in `protocol/MinecraftProtocol.kt`.
//!
//! Netty's `ByteBuf` has a mutable reader index that a decoder rewinds on an incomplete read;
//! the idiomatic Rust equivalent used throughout this module is a pure function over `&[u8]`
//! that returns `(value, bytes_consumed)` and lets the caller decide whether to advance — no
//! index to roll back because nothing was mutated on a partial read.

use std::fmt;

/// Minecraft's hard ceiling on a single (uncompressed) packet: 2^21 - 1 bytes. Every decoder in
/// this crate bounds the frame length it's willing to buffer against this — without it, a peer
/// that declares a huge frame length and dribbles bytes makes a cumulation buffer grow toward
/// that size, one held connection at a time: a cheap memory-amplification DDoS.
pub const MAX_PACKET_BYTES: i32 = (1 << 21) - 1;

/// Login state, clientbound — stable since the encryption handshake was introduced. Signals the
/// backend is online-mode; everything after the client's Encryption Response is AES-CFB8
/// ciphertext that a plain packet parser can no longer decode.
pub const LOGIN_ENCRYPTION_REQUEST: i32 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarIntError {
    /// `buf` ended before a terminating (high-bit-clear) byte was found — the caller should wait
    /// for more bytes and retry, not treat this as malformed input.
    Incomplete,
    /// More than 5 bytes were read without terminating — not a valid 32-bit VarInt under any
    /// circumstance, unlike `Incomplete`.
    TooBig,
}

impl fmt::Display for VarIntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VarIntError::Incomplete => write!(f, "incomplete VarInt"),
            VarIntError::TooBig => write!(f, "VarInt too big"),
        }
    }
}
impl std::error::Error for VarIntError {}

/// Reads a VarInt from the start of `buf`. Returns `(value, bytes_consumed)`.
pub fn read_var_int(buf: &[u8]) -> Result<(i32, usize), VarIntError> {
    let mut result: i32 = 0;
    let mut shift = 0u32;
    let mut i = 0usize;
    loop {
        let Some(&b) = buf.get(i) else { return Err(VarIntError::Incomplete) };
        i += 1;
        result |= ((b & 0x7F) as i32) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(VarIntError::TooBig);
        }
    }
    Ok((result, i))
}

/// Appends `value` to `out` as a VarInt.
pub fn write_var_int(out: &mut Vec<u8>, value: i32) {
    let mut v = value as u32;
    loop {
        if v & !0x7Fu32 == 0 {
            out.push(v as u8);
            return;
        }
        out.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLengthOutOfRange(pub i32);

impl fmt::Display for FrameLengthOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "frame length {} out of range (max {MAX_PACKET_BYTES})", self.0)
    }
}
impl std::error::Error for FrameLengthOutOfRange {}

/// Reads a frame-length VarInt and range-checks it. Returns `Ok(None)` if the VarInt isn't fully
/// buffered yet (caller should wait for more bytes); `Ok(Some((length, bytes_consumed)))` on
/// success; `Err` for a negative or over-[`MAX_PACKET_BYTES`] length.
pub fn read_frame_length(buf: &[u8]) -> Result<Option<(i32, usize)>, FrameLengthOutOfRange> {
    match read_var_int(buf) {
        Err(VarIntError::Incomplete) => Ok(None),
        // A VarInt that's grown past 5 bytes without terminating can never be a valid frame
        // length either way — treat the same as an out-of-range length rather than surfacing a
        // separate error variant callers would have to handle identically anyway.
        Err(VarIntError::TooBig) => Err(FrameLengthOutOfRange(-1)),
        Ok((length, consumed)) => {
            if length < 0 || length > MAX_PACKET_BYTES {
                Err(FrameLengthOutOfRange(length))
            } else {
                Ok(Some((length, consumed)))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringTooLong(pub i32);

impl fmt::Display for StringTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "String too long: {}", self.0)
    }
}
impl std::error::Error for StringTooLong {}

/// Reads a length-prefixed (VarInt byte count, not char count) UTF-8 string from the start of
/// `buf`. Returns `(value, bytes_consumed)`.
pub fn read_string(buf: &[u8], max_length: i32) -> Result<(String, usize), StringTooLong> {
    let (length, mut consumed) = read_var_int(buf).map_err(|_| StringTooLong(-1))?;
    if length < 0 || length > max_length.saturating_mul(4) {
        return Err(StringTooLong(length));
    }
    let length = length as usize;
    let Some(bytes) = buf.get(consumed..consumed + length) else { return Err(StringTooLong(length as i32)) };
    consumed += length;
    Ok((String::from_utf8_lossy(bytes).into_owned(), consumed))
}

/// Appends `value` to `out` as a length-prefixed UTF-8 string.
pub fn write_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    write_var_int(out, bytes.len() as i32);
    out.extend_from_slice(bytes);
}

/// Wraps `payload` with a VarInt length prefix — the outer packet-length frame every Minecraft
/// packet is sent with.
pub fn length_prefix(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    write_var_int(&mut out, payload.len() as i32);
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_int_round_trip_small_and_large() {
        for &v in &[0i32, 1, 127, 128, 255, 300, i32::MAX, -1, i32::MIN] {
            let mut out = Vec::new();
            write_var_int(&mut out, v);
            let (decoded, consumed) = read_var_int(&out).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, out.len());
        }
    }

    #[test]
    fn var_int_incomplete_when_truncated() {
        let mut out = Vec::new();
        write_var_int(&mut out, 300); // needs 2 bytes
        assert_eq!(read_var_int(&out[..1]), Err(VarIntError::Incomplete));
    }

    #[test]
    fn var_int_too_big_past_five_bytes() {
        let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        assert_eq!(read_var_int(&bytes), Err(VarIntError::TooBig));
    }

    #[test]
    fn frame_length_rejects_over_max() {
        let mut out = Vec::new();
        write_var_int(&mut out, MAX_PACKET_BYTES + 1);
        assert!(read_frame_length(&out).is_err());
    }

    #[test]
    fn frame_length_ok_within_range() {
        let mut out = Vec::new();
        write_var_int(&mut out, 100);
        assert_eq!(read_frame_length(&out).unwrap(), Some((100, out.len())));
    }

    #[test]
    fn frame_length_none_when_buffered_partially() {
        let mut out = Vec::new();
        write_var_int(&mut out, 300);
        assert_eq!(read_frame_length(&out[..1]).unwrap(), None);
    }

    #[test]
    fn string_round_trip() {
        let mut out = Vec::new();
        write_string(&mut out, "hello world");
        let (s, consumed) = read_string(&out, 32767).unwrap();
        assert_eq!(s, "hello world");
        assert_eq!(consumed, out.len());
    }

    #[test]
    fn string_too_long_rejected() {
        let mut out = Vec::new();
        write_var_int(&mut out, 1000);
        out.extend(std::iter::repeat(b'a').take(1000));
        assert!(read_string(&out, 16).is_err());
    }
}
