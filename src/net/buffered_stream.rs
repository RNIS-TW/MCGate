//! A per-connection read buffer that carries leftover bytes forward across separate framed
//! reads — the Rust equivalent of Netty's per-channel cumulation buffer.
//!
//! Real Minecraft clients often pipeline small packets (e.g. Handshake immediately followed by
//! Login Start) into the same TCP segment, so a single `stream.read()` call can return bytes
//! belonging to more than one packet. Reading a handshake directly off a raw `TcpStream` with a
//! throwaway buffer (as a naive port might) would silently discard whatever came after the
//! handshake in that same read — this wrapper fixes that by retaining any unconsumed bytes and
//! transparently feeding them back out through its own `AsyncRead` impl before touching the
//! underlying stream again. That means once framed reads (handshake, Login Start) are done, the
//! wrapper itself can be hard to a raw byte splice (e.g. `tokio::io::copy_bidirectional`) with no
//! special handling needed — leftover bytes just flow out as part of the first read.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::protocol::handshake::{self, Handshake, HandshakeError, ParseOutcome};
use crate::protocol::varint::read_var_int;

pub struct BufferedStream<S> {
    inner: S,
    buf: Vec<u8>,
    /// Read position within `buf` — bytes before this have already been handed to a caller
    /// (via a framed read) or are queued to be handed out via `poll_read`.
    pos: usize,
}

impl<S: AsyncRead + Unpin> BufferedStream<S> {
    pub fn new(inner: S) -> Self {
        Self { inner, buf: Vec::new(), pos: 0 }
    }

    fn unread(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    async fn fill_more(&mut self) -> std::io::Result<usize> {
        let mut chunk = [0u8; 4096];
        let n = self.inner.read(&mut chunk).await?;
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(n)
    }

    /// Reads one handshake packet, retaining any bytes read past its end for the next call
    /// (framed or raw).
    pub async fn read_handshake(&mut self) -> Result<Handshake, HandshakeError> {
        loop {
            match handshake::try_parse(self.unread()) {
                ParseOutcome::Complete(h) => {
                    self.pos += h.raw_frame.len();
                    return Ok(h);
                }
                ParseOutcome::Malformed(reason) => return Err(HandshakeError::Malformed(reason)),
                ParseOutcome::Incomplete => {}
            }
            if self.fill_more().await.map_err(HandshakeError::Io)? == 0 {
                return Err(HandshakeError::Eof);
            }
            if self.buf.len() - self.pos > 1024 {
                return Err(HandshakeError::Malformed("handshake exceeded maximum size without completing".into()));
            }
        }
    }

    /// Reads one length-prefixed frame (length varint + payload), returning the raw bytes
    /// (including the length prefix) unmodified — used for Login Start / Status Request, whose
    /// own field-level parsing lives in `minecraft_protocol.rs`.
    pub async fn read_frame(&mut self, max_len: i32) -> std::io::Result<Vec<u8>> {
        loop {
            if let Ok((length, header_len)) = read_var_int(self.unread()) {
                if !(0..=max_len).contains(&length) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("frame length {length} out of range")));
                }
                let total = header_len + length as usize;
                if self.unread().len() >= total {
                    let frame = self.unread()[..total].to_vec();
                    self.pos += total;
                    return Ok(frame);
                }
            }
            if self.fill_more().await? == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed before frame completed"));
            }
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for BufferedStream<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.pos < self.buf.len() {
            let n = out.remaining().min(self.buf.len() - self.pos);
            let start = self.pos;
            out.put_slice(&self.buf[start..start + n]);
            self.pos += n;
            // Buffer fully drained — reclaim its memory rather than holding it for the rest of
            // what's likely about to become a long-lived raw relay.
            if self.pos == self.buf.len() {
                self.buf.clear();
                self.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, out)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for BufferedStream<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::minecraft_protocol::{encode_handshake, encode_login_start};
    use std::io::Cursor;

    #[tokio::test]
    async fn leftover_bytes_after_handshake_are_preserved_for_next_read() {
        let handshake_frame = encode_handshake(763, "play.example.com", 25565, 2);
        let login_frame = encode_login_start("Steve", None);
        let mut combined = handshake_frame.clone();
        combined.extend_from_slice(&login_frame);

        // Both packets arrive in a single underlying read, as real pipelining clients do.
        let mut stream = BufferedStream::new(Cursor::new(combined));
        let h = stream.read_handshake().await.unwrap();
        assert_eq!(h.host, "play.example.com");

        // The login frame must still be there, not silently dropped.
        let frame = stream.read_frame(32767).await.unwrap();
        assert_eq!(frame, login_frame);
    }

    #[tokio::test]
    async fn leftover_bytes_after_all_framed_reads_flow_through_async_read() {
        let handshake_frame = encode_handshake(763, "play.example.com", 25565, 2);
        let mut combined = handshake_frame.clone();
        combined.extend_from_slice(b"raw relay bytes after login");

        let mut stream = BufferedStream::new(Cursor::new(combined));
        stream.read_handshake().await.unwrap();

        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"raw relay bytes after login");
    }

    #[tokio::test]
    async fn read_frame_across_split_reads() {
        struct OneByteAtATime(Vec<u8>, usize);
        impl AsyncRead for OneByteAtATime {
            fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                if self.1 < self.0.len() {
                    buf.put_slice(&[self.0[self.1]]);
                    self.1 += 1;
                }
                Poll::Ready(Ok(()))
            }
        }
        let login_frame = encode_login_start("Alex", None);
        let mut stream = BufferedStream::new(OneByteAtATime(login_frame.clone(), 0));
        let frame = stream.read_frame(32767).await.unwrap();
        assert_eq!(frame, login_frame);
    }
}
