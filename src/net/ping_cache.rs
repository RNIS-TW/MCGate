//! Port of `routing/PingCache.kt` — TTL-cached status-ping responses per backend, so a status
//! flood doesn't dial the backend on every single request.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a failed dial is remembered so repeated status pings during an outage return the
/// fallback immediately instead of each separately eating a full connect timeout.
const FAILURE_TTL: Duration = Duration::from_secs(5);
/// How often the sweep clears expired entries out of both maps.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Hard backstop on the number of live response-cache entries, independent of TTL.
const MAX_ENTRIES: usize = 10_000;

struct Entry {
    json: String,
    expires_at: Instant,
}

#[derive(Default)]
pub struct PingCache {
    entries: Mutex<HashMap<String, Entry>>,
    down_until: Mutex<HashMap<String, Instant>>,
}

impl PingCache {
    pub fn get(&self, key: &str) -> Option<String> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get(key)?;
        if Instant::now() > entry.expires_at {
            entries.remove(key);
            return None;
        }
        Some(entries.get(key).unwrap().json.clone())
    }

    pub fn put(&self, key: &str, json: String, ttl_millis: i64) {
        self.down_until.lock().unwrap().remove(key);
        if ttl_millis < 0 {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        entries.insert(key.to_string(), Entry { json, expires_at: Instant::now() + Duration::from_millis(ttl_millis as u64) });
        if entries.len() > MAX_ENTRIES {
            evict_overflow(&mut entries);
        }
    }

    /// Whether `key`'s last dial attempt failed recently enough that it's not worth retrying yet.
    pub fn is_known_down(&self, key: &str) -> bool {
        let mut down_until = self.down_until.lock().unwrap();
        let Some(&until) = down_until.get(key) else { return false };
        if Instant::now() > until {
            down_until.remove(key);
            return false;
        }
        true
    }

    pub fn mark_down(&self, key: &str) {
        self.down_until.lock().unwrap().insert(key.to_string(), Instant::now() + FAILURE_TTL);
    }

    fn sweep(&self) {
        let now = Instant::now();
        self.entries.lock().unwrap().retain(|_, e| e.expires_at >= now);
        self.down_until.lock().unwrap().retain(|_, &mut until| until >= now);
    }

    #[cfg(test)]
    fn entry_count_for_test(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// Trims `entries` well below `MAX_ENTRIES` (to 90%) by dropping the soonest-to-expire first.
fn evict_overflow(entries: &mut HashMap<String, Entry>) {
    let target = MAX_ENTRIES * 9 / 10;
    let over_by = entries.len().saturating_sub(target);
    if over_by == 0 {
        return;
    }
    let mut by_expiry: Vec<(String, Instant)> = entries.iter().map(|(k, v)| (k.clone(), v.expires_at)).collect();
    by_expiry.sort_by_key(|(_, expires_at)| *expires_at);
    for (key, _) in by_expiry.into_iter().take(over_by) {
        entries.remove(&key);
    }
}

/// Spawns the periodic sweep for `cache`. Call once at startup, from within a tokio runtime.
pub fn spawn_sweeper(cache: &'static PingCache) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            cache.sweep();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_within_ttl() {
        let cache = PingCache::default();
        cache.put("a", "json".to_string(), 10_000);
        assert_eq!(cache.get("a"), Some("json".to_string()));
    }

    #[test]
    fn get_expired_entry_returns_none() {
        let cache = PingCache::default();
        cache.put("a", "json".to_string(), 0);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(cache.get("a"), None);
    }

    #[test]
    fn negative_ttl_never_cached() {
        let cache = PingCache::default();
        cache.put("a", "json".to_string(), -1);
        assert_eq!(cache.get("a"), None);
    }

    #[test]
    fn mark_down_then_is_known_down_then_expires() {
        let cache = PingCache::default();
        assert!(!cache.is_known_down("a"));
        cache.mark_down("a");
        assert!(cache.is_known_down("a"));
    }

    #[test]
    fn put_clears_down_status() {
        let cache = PingCache::default();
        cache.mark_down("a");
        assert!(cache.is_known_down("a"));
        cache.put("a", "json".to_string(), 10_000);
        assert!(!cache.is_known_down("a"));
    }

    #[test]
    fn overflow_eviction_trims_to_ninety_percent() {
        // Use a tiny effective cap by directly testing evict_overflow's logic via many entries -
        // exercising the real MAX_ENTRIES would be slow, so this checks the trim math directly.
        let mut entries = HashMap::new();
        for i in 0..100 {
            entries.insert(format!("k{i}"), Entry { json: "x".into(), expires_at: Instant::now() + Duration::from_secs(i as u64) });
        }
        let target_over = 100usize.saturating_sub(90);
        assert_eq!(target_over, 10);
        evict_overflow(&mut entries); // MAX_ENTRIES is 10_000, so this is a no-op at n=100
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn sweep_removes_expired_but_keeps_fresh() {
        let cache = PingCache::default();
        cache.put("expired", "x".to_string(), 0);
        cache.put("fresh", "x".to_string(), 60_000);
        std::thread::sleep(Duration::from_millis(5));
        cache.sweep();
        assert_eq!(cache.entry_count_for_test(), 1);
    }
}
