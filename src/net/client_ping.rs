//! The player-visible "ping" in `players`/`whois` is client <-> MCGate latency, not MCGate <->
//! backend latency (that's `RouteRuntime::latency_of`, shown separately per backend). Read
//! straight from the kernel's `TCP_INFO` on the client's own socket - not measured by MCGate
//! sending its own probes - deliberately: after login, MCGate is a raw byte splice with no
//! visibility into (and, once a session is encrypted, no ability to safely inject into) the
//! Minecraft protocol stream. Sending our own Play-state Keep Alive to measure round-trip time
//! would mean intercepting and reacting to protocol bytes mid-relay, with real risk of breaking
//! the backend's own keep-alive tracking - a client answering two different keep-alive senders
//! with related-looking packet IDs looks like a protocol violation to the backend. `TCP_INFO`
//! gets the same answer (transport-level RTT) for free, from a value the kernel already tracks
//! on every open socket, no protocol involvement at all.
//!
//! Linux-only: `TCP_INFO`'s layout is Linux-specific (macOS's equivalent, `TCP_CONNECTION_INFO`,
//! is a different struct entirely under a different constant; Windows has no direct equivalent).
//! Every other target just reports no ping, the same as any other value this can't determine.
//!
//! **Meaningless under `proxyProtocol` (inbound)**: the socket `capture` is called on is then
//! MCGate <-> the fronting load balancer/L4 proxy, not MCGate <-> the real client - the PROXY
//! header only tells MCGate what the real client's address *was*, it doesn't change whose TCP
//! connection this actually is. `TCP_INFO` on it would report that hop's RTT, not the player's.
//! `server::handle_login` accounts for this by calling `unavailable()` instead of `capture()` in
//! that case, rather than reporting a plausible-looking but wrong number - never remove that
//! check without solving this differently first.

#[cfg(target_os = "linux")]
mod linux {
    use std::os::unix::io::RawFd;

    /// Captured once at login (a plain fd number, not a live socket handle) so `whois`/`players`
    /// can read this connection's current RTT later without needing a live reference to its
    /// `TcpStream` - the fd stays valid for the connection's whole lifetime regardless of how
    /// deeply the actual stream object is nested inside `relay`'s split read/write halves by then.
    #[derive(Debug, Clone, Copy)]
    pub struct ClientPingProbe(RawFd);

    impl ClientPingProbe {
        pub fn capture<S: std::os::unix::io::AsRawFd>(stream: &S) -> Self {
            Self(stream.as_raw_fd())
        }

        /// A probe with no real socket behind it (test fixtures, or a session that predates this
        /// field) - `round_trip_millis` just reports no ping, the same as a syscall failure would.
        pub fn unavailable() -> Self {
            Self(-1)
        }

        /// The kernel's current smoothed round-trip-time estimate for this connection, in
        /// milliseconds - `None` if the socket's already gone, or the syscall otherwise fails.
        pub fn round_trip_millis(&self) -> Option<u32> {
            let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
            let ret = unsafe { libc::getsockopt(self.0, libc::IPPROTO_TCP, libc::TCP_INFO, (&mut info as *mut libc::tcp_info).cast(), &mut len) };
            if ret != 0 {
                return None;
            }
            // tcpi_rtt is microseconds (the same field `ss -i` reports as "rtt") - round to the
            // nearest millisecond rather than truncating, so a healthy sub-1ms LAN link reads as
            // "1ms" rather than a misleading "0ms".
            Some((info.tcpi_rtt + 500) / 1000)
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod other {
    #[derive(Debug, Clone, Copy)]
    pub struct ClientPingProbe;

    impl ClientPingProbe {
        pub fn capture<S>(_stream: &S) -> Self {
            Self
        }

        pub fn unavailable() -> Self {
            Self
        }

        pub fn round_trip_millis(&self) -> Option<u32> {
            None
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::ClientPingProbe;
#[cfg(not(target_os = "linux"))]
pub use other::ClientPingProbe;
