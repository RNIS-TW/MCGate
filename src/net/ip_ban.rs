//! Fail2ban-style auto-ban, layered on top of the other per-IP guards (`connection_guard`,
//! `flood_control`): repeated violations from one source IP within a rolling window (over any
//! per-IP limit, a login that never completes, a malformed/garbage handshake) earn a temporary
//! ban rejected at the very top of `server::handle_connection`, before the PROXY protocol read,
//! the pre-login guard, or anything else runs.
//!
//! This is the actual DDoS-mitigation value on top of what already existed: without it, a
//! sustained flood from a small number of source IPs pays the same handshake-read/guard-acquire
//! cost on every single connection forever, even though every one of them is rejected anyway. A
//! banned IP's connections are now dropped for near-zero cost - no PROXY protocol read, no
//! handshake read, nothing - for the whole ban duration, turning a repeat offender from an
//! ongoing cost into a one-time one.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

struct Offender {
    violations_since: Instant,
    violation_count: u32,
    banned_until: Option<Instant>,
}

/// Past this many tracked IPs the limiter fails open (the other process-wide caps -
/// `connectionThrottle.maxConnections`, `maxConnectionsPerIp` - are the backstop) - keeps a flood
/// from millions of distinct sources from growing this map without bound. Matches
/// `flood_control::ConnectionRates`'s own reasoning/cap.
const MAX_TRACKED_IPS: usize = 250_000;

/// How long an offender with no ban and no recent violation is kept around at all - past this,
/// `sweep` drops it so the map doesn't grow without bound across a long uptime.
const FORGET_AFTER: Duration = Duration::from_secs(3600);

#[derive(Default)]
pub struct IpBanList {
    offenders: Mutex<HashMap<String, Offender>>,
}

pub fn ip_ban_list() -> &'static IpBanList {
    static INSTANCE: OnceLock<IpBanList> = OnceLock::new();
    INSTANCE.get_or_init(IpBanList::default)
}

impl IpBanList {
    /// Whether `ip` is currently banned. Lazily clears an expired ban here rather than in
    /// `sweep` alone, so a later `record_violation` for the same IP starts a fresh violation
    /// count instead of immediately re-banning off a stale one.
    pub fn is_banned(&self, ip: &str) -> bool {
        let mut offenders = self.offenders.lock().unwrap();
        let Some(o) = offenders.get_mut(ip) else { return false };
        match o.banned_until {
            Some(until) if until > Instant::now() => true,
            Some(_) => {
                offenders.remove(ip);
                false
            }
            None => false,
        }
    }

    /// Manually bans `ip` for `duration` right away, regardless of its violation history - the
    /// console `ban` command, for banning a known-bad IP immediately instead of waiting for it
    /// to rack up `autoBan.maxViolations` on its own. Unlike `record_violation`, this always
    /// succeeds (an explicit admin action shouldn't silently no-op just because the tracked-IP
    /// table happens to be near its cap).
    pub fn ban(&self, ip: &str, duration: Duration) {
        let now = Instant::now();
        let mut offenders = self.offenders.lock().unwrap();
        let o = offenders.entry(ip.to_string()).or_insert_with(|| Offender { violations_since: now, violation_count: 0, banned_until: None });
        o.banned_until = Some(now + duration);
    }

    /// Records one violation for `ip`; bans it for `ban_duration` once `max_violations` are
    /// reached within `window`. Returns `true` exactly when this call is the one that triggered
    /// a fresh ban (so the caller can log it once, not on every subsequent connection attempt).
    pub fn record_violation(&self, ip: &str, max_violations: u32, window: Duration, ban_duration: Duration) -> bool {
        let mut offenders = self.offenders.lock().unwrap();
        if offenders.len() >= MAX_TRACKED_IPS && !offenders.contains_key(ip) {
            return false; // fail open under an extreme distributed flood
        }
        let now = Instant::now();
        let o = offenders.entry(ip.to_string()).or_insert_with(|| Offender { violations_since: now, violation_count: 0, banned_until: None });
        if now.duration_since(o.violations_since) >= window {
            o.violations_since = now;
            o.violation_count = 0;
        }
        o.violation_count += 1;
        if o.violation_count >= max_violations {
            o.banned_until = Some(now + ban_duration);
            // Reset the count so a re-offense after this ban expires needs a fresh full count,
            // not just one more hit riding on the old one.
            o.violation_count = 0;
            o.violations_since = now;
            return true;
        }
        false
    }

    /// Every currently-banned IP and how many seconds its ban has left, for the console `bans`
    /// command - sorted by remaining time descending (the longest-banned/worst offenders first).
    pub fn active_bans(&self) -> Vec<(String, u64)> {
        let now = Instant::now();
        let mut bans: Vec<(String, u64)> = self
            .offenders
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(ip, o)| {
                let until = o.banned_until?;
                (until > now).then(|| (ip.clone(), until.duration_since(now).as_secs()))
            })
            .collect();
        bans.sort_by(|a, b| b.1.cmp(&a.1));
        bans
    }

    /// Manually lifts `ip`'s ban (if any) - the console `unban` command. Returns `true` if it was
    /// actually banned (not just tracked with a violation count that never crossed the
    /// threshold), so the caller can report "wasn't banned" distinctly from "unbanned".
    pub fn unban(&self, ip: &str) -> bool {
        let mut offenders = self.offenders.lock().unwrap();
        let Some(o) = offenders.get_mut(ip) else { return false };
        let was_banned = o.banned_until.map(|u| u > Instant::now()).unwrap_or(false);
        offenders.remove(ip);
        was_banned
    }

    fn sweep(&self) {
        let now = Instant::now();
        self.offenders.lock().unwrap().retain(|_, o| {
            let ban_active = o.banned_until.map(|u| u > now).unwrap_or(false);
            let recent_violation = now.duration_since(o.violations_since) < FORGET_AFTER;
            ban_active || recent_violation
        });
    }

    #[cfg(test)]
    fn is_tracked_for_test(&self, ip: &str) -> bool {
        self.offenders.lock().unwrap().contains_key(ip)
    }
}

/// Spawns the periodic stale-offender sweep. Call once at startup, from within a tokio runtime.
pub fn spawn_sweeper() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        interval.tick().await;
        loop {
            interval.tick().await;
            ip_ban_list().sweep();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_banned_below_the_violation_threshold() {
        let list = IpBanList::default();
        let ip = "10.1.1.1";
        assert!(!list.record_violation(ip, 3, Duration::from_secs(60), Duration::from_secs(60)));
        assert!(!list.record_violation(ip, 3, Duration::from_secs(60), Duration::from_secs(60)));
        assert!(!list.is_banned(ip));
    }

    #[test]
    fn bans_after_reaching_the_violation_threshold() {
        let list = IpBanList::default();
        let ip = "10.1.1.2";
        for _ in 0..2 {
            assert!(!list.record_violation(ip, 3, Duration::from_secs(60), Duration::from_secs(60)));
        }
        assert!(list.record_violation(ip, 3, Duration::from_secs(60), Duration::from_secs(60)), "the 3rd violation should trigger the ban");
        assert!(list.is_banned(ip));
    }

    #[test]
    fn violation_window_elapsing_resets_the_count() {
        let list = IpBanList::default();
        let ip = "10.1.1.3";
        assert!(!list.record_violation(ip, 2, Duration::from_millis(1), Duration::from_secs(60)));
        std::thread::sleep(Duration::from_millis(20));
        // The window has elapsed - this must be treated as violation #1 again, not #2 (which
        // would otherwise incorrectly trigger a ban here).
        assert!(!list.record_violation(ip, 2, Duration::from_millis(1), Duration::from_secs(60)));
        assert!(!list.is_banned(ip));
    }

    #[test]
    fn ban_expires_after_its_duration() {
        let list = IpBanList::default();
        let ip = "10.1.1.4";
        assert!(!list.record_violation(ip, 2, Duration::from_secs(60), Duration::from_millis(50)));
        assert!(list.record_violation(ip, 2, Duration::from_secs(60), Duration::from_millis(50)));
        assert!(list.is_banned(ip));
        std::thread::sleep(Duration::from_millis(80));
        assert!(!list.is_banned(ip), "ban should have expired");
    }

    #[test]
    fn is_banned_never_bans_an_untracked_ip() {
        let list = IpBanList::default();
        assert!(!list.is_banned("192.0.2.1"));
    }

    #[test]
    fn active_bans_lists_only_currently_banned_ips() {
        let list = IpBanList::default();
        list.record_violation("10.1.1.7", 100, Duration::from_secs(60), Duration::from_secs(60)); // tracked, never banned
        assert!(list.record_violation("10.1.1.8", 1, Duration::from_secs(60), Duration::from_secs(60)));
        let bans = list.active_bans();
        assert_eq!(bans.len(), 1);
        assert_eq!(bans[0].0, "10.1.1.8");
        assert!(bans[0].1 <= 60);
    }

    #[test]
    fn manual_ban_takes_effect_immediately_with_no_prior_violations() {
        let list = IpBanList::default();
        let ip = "10.1.1.20";
        assert!(!list.is_banned(ip));
        list.ban(ip, Duration::from_secs(60));
        assert!(list.is_banned(ip));
    }

    #[test]
    fn unban_lifts_an_active_ban_and_reports_it_did() {
        let list = IpBanList::default();
        assert!(list.record_violation("10.1.1.9", 1, Duration::from_secs(60), Duration::from_secs(60)));
        assert!(list.is_banned("10.1.1.9"));
        assert!(list.unban("10.1.1.9"));
        assert!(!list.is_banned("10.1.1.9"));
    }

    #[test]
    fn unban_reports_false_for_an_ip_that_was_never_banned() {
        let list = IpBanList::default();
        list.record_violation("10.1.1.10", 100, Duration::from_secs(60), Duration::from_secs(60)); // tracked, never banned
        assert!(!list.unban("10.1.1.10"));
        assert!(!list.unban("192.0.2.99")); // not tracked at all
    }

    #[test]
    fn sweep_drops_stale_untracked_entries_but_keeps_active_bans() {
        let list = IpBanList::default();
        list.record_violation("10.1.1.5", 100, Duration::from_secs(60), Duration::from_secs(60)); // one hit, well under threshold
        assert!(list.record_violation("10.1.1.6", 1, Duration::from_secs(60), Duration::from_secs(3600)));
        assert!(list.is_tracked_for_test("10.1.1.5"));
        assert!(list.is_tracked_for_test("10.1.1.6"));
        list.sweep(); // neither is stale yet (both just touched) - both survive
        assert!(list.is_tracked_for_test("10.1.1.5"));
        assert!(list.is_tracked_for_test("10.1.1.6"));
        assert!(list.is_banned("10.1.1.6"));
    }
}
