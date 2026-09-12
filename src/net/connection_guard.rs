//! Port of `handler/ConnectionGuardHandler.kt`'s `PreLoginConnections` registry plus the guard
//! logic itself, adapted from Netty's handler-lifecycle model to a single async task per
//! connection: instead of a pipeline handler reacting to lifecycle callbacks
//! (`channelActive`/`userEventTriggered`/`channelInactive`), this is an RAII guard — acquire a
//! slot (and start a deadline) when a connection task begins, call `prelogin_done()` once the
//! client proves itself real (Login Start received), and let `Drop` release whatever's still
//! held if the task ends before that (dropped connection, handshake rejected, etc.) — mirroring
//! `release()`'s "runs from every path" idempotency without needing separate handlers for each
//! Netty callback.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Process-wide count of connections currently in their pre-login phase, keyed by source IP.
/// Entries self-remove when their count hits zero, so a transient flood from thousands of
/// distinct IPs doesn't leave the map permanently bloated.
#[derive(Default)]
struct PreLoginConnections {
    counts: Mutex<HashMap<String, u32>>,
}

fn pre_login_connections() -> &'static PreLoginConnections {
    static INSTANCE: OnceLock<PreLoginConnections> = OnceLock::new();
    INSTANCE.get_or_init(PreLoginConnections::default)
}

impl PreLoginConnections {
    fn try_acquire(&self, ip: &str, max: u32) -> bool {
        let mut counts = self.counts.lock().unwrap();
        let counter = counts.entry(ip.to_string()).or_insert(0);
        *counter += 1;
        if *counter > max {
            *counter -= 1;
            if *counter == 0 {
                counts.remove(ip);
            }
            false
        } else {
            true
        }
    }

    fn release(&self, ip: &str) {
        let mut counts = self.counts.lock().unwrap();
        if let Some(counter) = counts.get_mut(ip) {
            if *counter <= 1 {
                counts.remove(ip);
            } else {
                *counter -= 1;
            }
        }
    }

    #[cfg(test)]
    fn count_for_test(&self, ip: &str) -> u32 {
        *self.counts.lock().unwrap().get(ip).unwrap_or(&0)
    }
}

/// RAII guard for one connection's pre-login phase: holds a per-IP slot (if the cap is enabled
/// and was available) for as long as the connection hasn't proven itself via [`Self::done`].
/// Dropping the guard at any point (early rejection, handshake failure, client disconnect before
/// login, or an explicit `done()`) releases the slot exactly once.
pub struct PreLoginGuard {
    ip: Option<String>,
    released: bool,
}

impl PreLoginGuard {
    /// Attempts to reserve a pre-login slot for `ip`. Returns `None` if the per-IP cap is
    /// enabled and already full for this IP (caller should close the connection immediately,
    /// mirroring `ConnectionGuardHandler.channelActive`'s reject-on-full behavior). Returns
    /// `Some` holding no slot at all if `max_connections_per_ip` is 0 (disabled).
    pub fn acquire(ip: Option<&str>, max_connections_per_ip: u32) -> Option<Self> {
        if max_connections_per_ip == 0 {
            return Some(Self { ip: None, released: true });
        }
        let Some(ip) = ip else { return Some(Self { ip: None, released: true }) };
        if pre_login_connections().try_acquire(ip, max_connections_per_ip) {
            Some(Self { ip: Some(ip.to_string()), released: false })
        } else {
            None
        }
    }

    /// Signals the connection has proven itself real (Login Start received) — releases the
    /// pre-login slot immediately rather than waiting for the connection to eventually close.
    /// Matches the `PRELOGIN_DONE` user event.
    pub fn done(&mut self) {
        self.release();
    }

    fn release(&mut self) {
        if !self.released {
            self.released = true;
            if let Some(ip) = &self.ip {
                pre_login_connections().release(ip);
            }
        }
    }
}

impl Drop for PreLoginGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_cap_never_blocks() {
        let g1 = PreLoginGuard::acquire(Some("1.2.3.4"), 0);
        let g2 = PreLoginGuard::acquire(Some("1.2.3.4"), 0);
        assert!(g1.is_some());
        assert!(g2.is_some());
    }

    #[test]
    fn cap_blocks_past_limit_and_release_frees_a_slot() {
        let ip = "10.0.0.1-cap-test";
        let g1 = PreLoginGuard::acquire(Some(ip), 2);
        let g2 = PreLoginGuard::acquire(Some(ip), 2);
        assert!(g1.is_some());
        assert!(g2.is_some());
        assert_eq!(pre_login_connections().count_for_test(ip), 2);

        let g3 = PreLoginGuard::acquire(Some(ip), 2);
        assert!(g3.is_none(), "third connection should be rejected at the cap");

        drop(g1);
        assert_eq!(pre_login_connections().count_for_test(ip), 1);
        let g4 = PreLoginGuard::acquire(Some(ip), 2);
        assert!(g4.is_some(), "a freed slot should admit a new connection");
    }

    #[test]
    fn done_releases_slot_immediately_not_just_on_drop() {
        let ip = "10.0.0.2-done-test";
        let mut g = PreLoginGuard::acquire(Some(ip), 1).unwrap();
        assert_eq!(pre_login_connections().count_for_test(ip), 1);
        g.done();
        assert_eq!(pre_login_connections().count_for_test(ip), 0);
        // Dropping after done() must not double-release (would corrupt another IP's count if a
        // decrement-past-zero bug existed).
        drop(g);
        assert_eq!(pre_login_connections().count_for_test(ip), 0);
    }

    #[test]
    fn entry_is_removed_once_count_hits_zero() {
        let ip = "10.0.0.3-cleanup-test";
        let g = PreLoginGuard::acquire(Some(ip), 5).unwrap();
        drop(g);
        assert!(!pre_login_connections().counts.lock().unwrap().contains_key(ip));
    }
}
