//! Port of `udp/UdpThrottle.kt` — process-wide, hot-reloadable anti-abuse limits shared by every
//! UDP relay path (`voice_relay.rs` and each `udp_proxy.rs` forward). UDP has no handshake to
//! gate on and a trivially spoofable source address, so these caps are the only thing bounding
//! how much a datagram flood can make MCGate allocate (a session object plus a backend-facing
//! socket/fd per distinct source).

use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::OnceLock;

use crate::config::UdpThrottleConfig;

pub struct UdpThrottle {
    max_sessions: AtomicI32,
    max_sessions_per_ip: AtomicI32,
    pending_packets_per_session: AtomicI32,
    idle_timeout_millis: AtomicI64,
    no_reply_teardown_millis: AtomicI64,
}

fn defaults() -> UdpThrottleConfig {
    UdpThrottleConfig::default()
}

pub fn udp_throttle() -> &'static UdpThrottle {
    static INSTANCE: OnceLock<UdpThrottle> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let d = defaults();
        UdpThrottle {
            max_sessions: AtomicI32::new(d.max_sessions),
            max_sessions_per_ip: AtomicI32::new(d.max_sessions_per_ip),
            pending_packets_per_session: AtomicI32::new(d.pending_packets_per_session),
            idle_timeout_millis: AtomicI64::new(d.idle_timeout_millis),
            no_reply_teardown_millis: AtomicI64::new(d.no_reply_teardown_millis),
        }
    })
}

impl UdpThrottle {
    pub fn apply(&self, c: &UdpThrottleConfig) {
        self.max_sessions.store(c.max_sessions, Ordering::Relaxed);
        self.max_sessions_per_ip.store(c.max_sessions_per_ip, Ordering::Relaxed);
        self.pending_packets_per_session.store(c.pending_packets_per_session.max(1), Ordering::Relaxed);
        self.idle_timeout_millis.store(c.idle_timeout_millis, Ordering::Relaxed);
        self.no_reply_teardown_millis.store(c.no_reply_teardown_millis, Ordering::Relaxed);
    }

    pub fn max_sessions(&self) -> i32 {
        self.max_sessions.load(Ordering::Relaxed)
    }
    pub fn max_sessions_per_ip(&self) -> i32 {
        self.max_sessions_per_ip.load(Ordering::Relaxed)
    }
    pub fn pending_packets_per_session(&self) -> i32 {
        self.pending_packets_per_session.load(Ordering::Relaxed)
    }
    pub fn idle_timeout_millis(&self) -> i64 {
        self.idle_timeout_millis.load(Ordering::Relaxed)
    }
    pub fn no_reply_teardown_millis(&self) -> i64 {
        self.no_reply_teardown_millis.load(Ordering::Relaxed)
    }

    /// Test hook - restore the compiled-in defaults.
    #[cfg(test)]
    pub fn reset(&self) {
        self.apply(&defaults());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_updates_all_fields_and_clamps_pending_to_at_least_one() {
        let throttle = udp_throttle();
        throttle.apply(&UdpThrottleConfig {
            max_sessions: 10,
            max_sessions_per_ip: 2,
            pending_packets_per_session: 0, // should clamp to 1
            idle_timeout_millis: 5000,
            no_reply_teardown_millis: 1000,
        });
        assert_eq!(throttle.max_sessions(), 10);
        assert_eq!(throttle.max_sessions_per_ip(), 2);
        assert_eq!(throttle.pending_packets_per_session(), 1);
        assert_eq!(throttle.idle_timeout_millis(), 5000);
        assert_eq!(throttle.no_reply_teardown_millis(), 1000);
        throttle.reset();
    }
}
