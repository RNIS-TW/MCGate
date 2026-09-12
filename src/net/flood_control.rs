//! Port of `handler/FloodControl.kt` — process-wide flood ceilings layered on top of
//! `connection_guard`'s per-IP pre-login cap. Checked before any per-connection state (the
//! guard, a task spawn) is built, mirroring the Kotlin version being checked in the child-channel
//! initializer before any pipeline handler is added.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Hard cap on total concurrent client connections.
pub struct GlobalConnections {
    count: AtomicI32,
}

fn global_connections() -> &'static GlobalConnections {
    static INSTANCE: OnceLock<GlobalConnections> = OnceLock::new();
    INSTANCE.get_or_init(|| GlobalConnections { count: AtomicI32::new(0) })
}

impl GlobalConnections {
    /// Reserves a slot if fewer than `max` are held. Caller must call [`release`] on channel
    /// close (via the returned guard, or manually).
    pub fn try_acquire(max: i32) -> bool {
        let gc = global_connections();
        if gc.count.fetch_add(1, Ordering::SeqCst) + 1 > max {
            gc.count.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    pub fn release() {
        global_connections().count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn current() -> i32 {
        global_connections().count.load(Ordering::SeqCst)
    }
}

/// RAII guard for a [`GlobalConnections`] slot — releases on drop, matching the "always release
/// on channel close" requirement without needing an explicit close hook.
pub struct GlobalConnectionGuard {
    released: bool,
}

impl GlobalConnectionGuard {
    /// Attempts to reserve a global slot; `max <= 0` means unlimited (mirrors `maxConnections: 0`).
    pub fn acquire(max: i32) -> Option<Self> {
        if max <= 0 {
            return Some(Self { released: true });
        }
        if GlobalConnections::try_acquire(max) {
            Some(Self { released: false })
        } else {
            None
        }
    }
}

impl Drop for GlobalConnectionGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            GlobalConnections::release();
        }
    }
}

struct Window {
    start: Instant,
    count: AtomicU32,
}

/// Per-source-IP new-connection rate limit: at most `max` new connections from one IP within a
/// rolling `window`. Fixed-window (a boundary can briefly allow up to 2x), which is fine for
/// flood mitigation.
#[derive(Default)]
pub struct ConnectionRates {
    windows: Mutex<HashMap<String, Window>>,
}

/// Past this many tracked IPs the limiter fails open (the global cap is then the backstop) —
/// keeps a flood from millions of distinct sources from growing this map without bound.
const MAX_TRACKED_IPS: usize = 250_000;
const SWEEP_CUTOFF: Duration = Duration::from_secs(600);

pub fn connection_rates() -> &'static ConnectionRates {
    static INSTANCE: OnceLock<ConnectionRates> = OnceLock::new();
    INSTANCE.get_or_init(ConnectionRates::default)
}

impl ConnectionRates {
    /// True if a new connection from `ip` is within the allowed rate.
    pub fn try_acquire(&self, ip: &str, max: u32, window: Duration) -> bool {
        let mut windows = self.windows.lock().unwrap();
        if windows.len() >= MAX_TRACKED_IPS && !windows.contains_key(ip) {
            return true;
        }
        let now = Instant::now();
        let entry = windows.entry(ip.to_string()).and_modify(|w| {
            if now.duration_since(w.start) >= window {
                w.start = now;
                w.count = AtomicU32::new(0);
            }
        });
        let w = entry.or_insert_with(|| Window { start: now, count: AtomicU32::new(0) });
        w.count.fetch_add(1, Ordering::SeqCst) + 1 <= max
    }

    fn sweep(&self) {
        let cutoff = Instant::now().checked_sub(SWEEP_CUTOFF);
        let Some(cutoff) = cutoff else { return };
        self.windows.lock().unwrap().retain(|_, w| w.start >= cutoff);
    }

    #[cfg(test)]
    fn count_for_test(&self, ip: &str) -> u32 {
        self.windows.lock().unwrap().get(ip).map(|w| w.count.load(Ordering::SeqCst)).unwrap_or(0)
    }
}

/// Spawns the periodic stale-window sweep. Call once at startup, from within a tokio runtime.
pub fn spawn_connection_rate_sweeper() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            connection_rates().sweep();
        }
    });
}

/// Process-wide cap on players held in the reconnect-wait state. Each held player holds a live
/// connection plus keep-alive/animation/retry timers, so an unbounded number of them is itself a
/// resource-exhaustion vector. Not yet wired to anything — the reconnect-hold feature itself
/// (`ReconnectHandler.kt`) isn't ported (see plan.md); kept here so the counter/cap shape exists
/// ready for that.
pub struct HeldReconnectSessions {
    held: AtomicI32,
    max: AtomicI32,
}

fn held_reconnect_sessions() -> &'static HeldReconnectSessions {
    static INSTANCE: OnceLock<HeldReconnectSessions> = OnceLock::new();
    INSTANCE.get_or_init(|| HeldReconnectSessions { held: AtomicI32::new(0), max: AtomicI32::new(500) })
}

impl HeldReconnectSessions {
    pub fn set_max(max: i32) {
        held_reconnect_sessions().max.store(max, Ordering::SeqCst);
    }

    /// Reserves a hold slot. Caller must call [`exit`] exactly once when the hold ends.
    #[allow(dead_code)]
    pub fn try_enter() -> bool {
        let s = held_reconnect_sessions();
        let max = s.max.load(Ordering::SeqCst);
        let held = s.held.fetch_add(1, Ordering::SeqCst) + 1;
        if held > max && max > 0 {
            s.held.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    #[allow(dead_code)]
    pub fn exit() {
        let s = held_reconnect_sessions();
        s.held.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some((v - 1).max(0))).ok();
    }

    #[allow(dead_code)]
    pub fn current() -> i32 {
        held_reconnect_sessions().held.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_connection_guard_enforces_cap() {
        let g1 = GlobalConnectionGuard::acquire(2);
        let g2 = GlobalConnectionGuard::acquire(2);
        assert!(g1.is_some());
        assert!(g2.is_some());
        let before = GlobalConnections::current();
        let g3 = GlobalConnectionGuard::acquire(2);
        assert!(g3.is_none());
        assert_eq!(GlobalConnections::current(), before); // rejected acquire didn't leak a count

        drop(g1);
        let g4 = GlobalConnectionGuard::acquire(2);
        assert!(g4.is_some());
    }

    #[test]
    fn global_connection_guard_zero_means_unlimited() {
        let guards: Vec<_> = (0..100).map(|_| GlobalConnectionGuard::acquire(0)).collect();
        assert!(guards.iter().all(|g| g.is_some()));
    }

    #[test]
    fn connection_rate_limits_within_window() {
        let rates = ConnectionRates::default();
        let ip = "1.1.1.1";
        for _ in 0..5 {
            assert!(rates.try_acquire(ip, 5, Duration::from_secs(60)));
        }
        assert!(!rates.try_acquire(ip, 5, Duration::from_secs(60)));
        assert_eq!(rates.count_for_test(ip), 6);
    }

    #[test]
    fn connection_rate_resets_after_window_elapses() {
        let rates = ConnectionRates::default();
        let ip = "2.2.2.2";
        assert!(rates.try_acquire(ip, 1, Duration::from_millis(1)));
        assert!(!rates.try_acquire(ip, 1, Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(20));
        assert!(rates.try_acquire(ip, 1, Duration::from_millis(1)), "window should have reset");
    }

    #[test]
    fn held_reconnect_sessions_respects_max() {
        HeldReconnectSessions::set_max(2);
        assert!(HeldReconnectSessions::try_enter());
        assert!(HeldReconnectSessions::try_enter());
        assert!(!HeldReconnectSessions::try_enter());
        HeldReconnectSessions::exit();
        assert!(HeldReconnectSessions::try_enter());
        // cleanup so other tests sharing this singleton aren't affected
        HeldReconnectSessions::exit();
        HeldReconnectSessions::exit();
        HeldReconnectSessions::set_max(500);
    }
}
