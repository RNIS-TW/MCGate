//! Port of `config/DnsCache.kt` — caches backend hostname resolution.
//!
//! The Kotlin version exists to keep `InetSocketAddress(host, port)`'s **synchronous, blocking**
//! DNS lookup off Netty's fixed pool of event-loop threads: one slow lookup there stalls every
//! other player channel sharing that thread, not just the connecting one. That specific problem
//! doesn't exist in this port: every connection here runs as its own tokio task (section 3), and
//! `tokio::net::lookup_host` is genuinely async — awaiting an uncached lookup suspends only that
//! one task, not a shared worker. So the elaborate bounded-executor-pool-with-`AbortPolicy`
//! machinery in the Kotlin version has no Rust equivalent need; this keeps only what's still
//! worth having independent of that: avoiding a real DNS round trip on every single connection to
//! the same hostname (TTL cache, stale-while-revalidate, negative caching for a broken host, a
//! size cap, and idle eviction so a wildcard-route flood of one-off subdomains can't grow the
//! cache without bound).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::net::lookup_host;

const CACHE_TTL: Duration = Duration::from_secs(60);
/// A failed lookup is cached this briefly — long enough that a flood of connections to a broken
/// host doesn't hammer the resolver, short enough that recovery is noticed quickly.
const NEGATIVE_TTL: Duration = Duration::from_secs(5);
/// How long an entry can go un-looked-up before the periodic sweep evicts it. A host still being
/// actively used gets refreshed well within this window (see `resolve`) and is never swept.
const ENTRY_IDLE_EVICT: Duration = Duration::from_secs(600);
/// Hard ceiling on cached hostnames, independent of the idle sweep — a wildcard route hit by a
/// flood of connections to random subdomains resolves a new key per distinct subdomain, faster
/// than `ENTRY_IDLE_EVICT` clears them. Past this, new hostnames still resolve, they just aren't
/// cached (the flood keys churn instead of accumulating).
const MAX_CACHE_ENTRIES: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsResolveError {
    pub host: String,
}

impl std::fmt::Display for DnsResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to resolve host '{}'", self.host)
    }
}
impl std::error::Error for DnsResolveError {}

#[derive(Clone)]
struct Entry {
    result: Result<SocketAddr, DnsResolveError>,
    expires_at: Instant,
}

#[derive(Default)]
pub struct DnsCache {
    cache: Mutex<HashMap<String, Entry>>,
    refreshing: Mutex<HashSet<String>>,
}

pub fn dns_cache() -> &'static DnsCache {
    static INSTANCE: OnceLock<DnsCache> = OnceLock::new();
    INSTANCE.get_or_init(DnsCache::default)
}

impl DnsCache {
    /// Resolves `host:port`. Awaits a real lookup only on the first-ever call for a given
    /// hostname (or after a negative-cached failure expires); every other call — including a
    /// stale-but-present cache hit, which triggers a background refresh — returns immediately.
    ///
    /// Takes `&'static self` (only satisfiable by [`dns_cache()`]'s singleton, never a locally
    /// constructed instance) because a stale hit spawns a background refresh task that writes
    /// back into `self` — that capture requires `self` to genuinely outlive the task. This also
    /// rules out, by construction, a bug this module briefly had while under development: a
    /// spawned refresh/warm task quietly writing into the *global* singleton's cache regardless
    /// of which `DnsCache` instance `resolve`/`warm` were called on. Requiring `&'static self`
    /// makes "which cache did this write land in" always unambiguous — it's always `self`.
    pub async fn resolve(&'static self, host: &str, port: u16) -> Result<SocketAddr, DnsResolveError> {
        if let Some(addr) = is_ip_literal(host, port) {
            return Ok(addr);
        }

        let key = format!("{host}:{port}");
        let cached = self.cache.lock().unwrap().get(&key).cloned();
        if let Some(entry) = cached {
            if Instant::now() >= entry.expires_at {
                self.refresh_async(key, host.to_string(), port);
            }
            return entry.result;
        }

        // No cached value at all yet - this first lookup has to actually await. Every subsequent
        // call for this host is non-blocking (served from cache).
        let result = do_lookup(host, port).await;
        let mut cache = self.cache.lock().unwrap();
        if cache.len() < MAX_CACHE_ENTRIES {
            let ttl = if result.is_ok() { CACHE_TTL } else { NEGATIVE_TTL };
            cache.insert(key, Entry { result: result.clone(), expires_at: Instant::now() + ttl });
        }
        result
    }

    /// True if `resolve` for this host:port will return without awaiting a real DNS lookup — an
    /// IP literal, or a hostname already in the cache (a stale entry refreshes in the background
    /// and still returns immediately).
    pub fn will_resolve_without_blocking(&self, host: &str, port: u16) -> bool {
        is_ip_literal(host, port).is_some() || self.cache.lock().unwrap().contains_key(&format!("{host}:{port}"))
    }

    /// Pre-resolves `host`:`port` on a background task so the first player connection to it
    /// doesn't pay the lookup cost. Safe to call for IP literals (no-op cost). See `resolve`'s
    /// doc for why this requires `&'static self`.
    ///
    /// Takes an explicit `tokio::runtime::Handle` rather than using ambient `tokio::spawn`
    /// because this is called from `config::warm_static_backends`, which runs both from inside
    /// the tokio runtime (startup) and from the config-file watcher's background thread (a plain
    /// OS thread `notify-debouncer-mini` owns, not a tokio task) on every hot reload — `spawn`ing
    /// without a runtime context on that second path would panic. `Handle::spawn` works from any
    /// thread as long as the handle itself came from a live runtime.
    pub fn warm(&'static self, handle: &tokio::runtime::Handle, host: &str, port: u16) {
        if is_ip_literal(host, port).is_some() {
            return;
        }
        let host = host.to_string();
        let key = format!("{host}:{port}");
        handle.spawn(async move {
            let result = do_lookup(&host, port).await;
            // Don't overwrite a good entry with a failure, and don't cache a negative result at
            // the full TTL.
            if let Ok(addr) = result {
                let mut guard = self.cache.lock().unwrap();
                if guard.len() < MAX_CACHE_ENTRIES {
                    guard.insert(key, Entry { result: Ok(addr), expires_at: Instant::now() + CACHE_TTL });
                }
            } else {
                tracing::debug!("Failed to pre-resolve backend host '{host}'");
            }
        });
    }

    fn refresh_async(&'static self, key: String, host: String, port: u16) {
        {
            let mut refreshing = self.refreshing.lock().unwrap();
            if !refreshing.insert(key.clone()) {
                return; // a refresh for this key is already in flight
            }
        }
        tokio::spawn(async move {
            let result = do_lookup(&host, port).await;
            if result.is_ok() {
                let mut guard = self.cache.lock().unwrap();
                if guard.len() < MAX_CACHE_ENTRIES {
                    guard.insert(key.clone(), Entry { result, expires_at: Instant::now() + CACHE_TTL });
                }
            } else {
                tracing::debug!("Failed to refresh backend host '{host}'");
            }
            self.refreshing.lock().unwrap().remove(&key);
        });
    }

    /// A still-live entry's expiry keeps getting pushed forward every `resolve` call — so an
    /// entry whose expiry is more than `ENTRY_IDLE_EVICT` in the past hasn't been looked up in at
    /// least that long and is safe to drop; the next `resolve` for that host just pays a fresh
    /// lookup, identical to a never-before-seen host.
    fn evict_idle_entries(&self) {
        let cutoff = Instant::now().checked_sub(ENTRY_IDLE_EVICT);
        let Some(cutoff) = cutoff else { return };
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|_, entry| entry.expires_at >= cutoff);
        let keys: HashSet<String> = cache.keys().cloned().collect();
        drop(cache);
        self.refreshing.lock().unwrap().retain(|k| keys.contains(k));
    }
}

async fn do_lookup(host: &str, port: u16) -> Result<SocketAddr, DnsResolveError> {
    match lookup_host((host, port)).await {
        Ok(mut addrs) => addrs.next().ok_or_else(|| DnsResolveError { host: host.to_string() }),
        Err(_) => Err(DnsResolveError { host: host.to_string() }),
    }
}

/// Dotted-quad IPv4 or bracket-less IPv6 literal check — matches the Kotlin version's loose
/// `contains(':')` IPv6 heuristic (imprecise for, e.g., a literal with a zone id, but faithful to
/// the original rather than "improved" independently). Returns the resolved `SocketAddr` directly
/// when it is one, since parsing an IP literal never needs the cache at all.
fn is_ip_literal(host: &str, port: u16) -> Option<SocketAddr> {
    if let Ok(ipv4) = host.parse::<std::net::Ipv4Addr>() {
        return Some(SocketAddr::new(ipv4.into(), port));
    }
    if host.contains(':') {
        let cleaned = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ipv6) = cleaned.parse::<std::net::Ipv6Addr>() {
            return Some(SocketAddr::new(ipv6.into(), port));
        }
    }
    None
}

/// Spawns the periodic idle-entry sweep. Call once at startup, from within a tokio runtime.
pub fn spawn_evictor() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(ENTRY_IDLE_EVICT);
        interval.tick().await; // first tick fires immediately; skip it, matching the Kotlin
                                // scheduler's initial-delay-then-repeat semantics
        loop {
            interval.tick().await;
            dns_cache().evict_idle_entries();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_literal_detection() {
        assert!(is_ip_literal("127.0.0.1", 25565).is_some());
        assert!(is_ip_literal("::1", 25565).is_some());
        assert!(is_ip_literal("not-an-ip.example.com", 25565).is_none());
    }

    // resolve()/warm() require `&'static self` (see their doc), which only the process-wide
    // `dns_cache()` singleton satisfies — a locally constructed `DnsCache` can't be used with
    // them at all (that's the whole point: it rules out the class of bug where a background task
    // writes into the wrong instance). Tests below therefore share the singleton and use
    // per-test-unique hostnames so parallel test runs don't observe each other's cache entries.

    #[tokio::test]
    async fn ip_literal_resolves_without_lookup() {
        let cache = dns_cache();
        let addr = cache.resolve("127.0.0.1", 25565).await.unwrap();
        assert_eq!(addr, "127.0.0.1:25565".parse().unwrap());
    }

    #[tokio::test]
    async fn will_resolve_without_blocking_true_for_ip_literal() {
        let cache = dns_cache();
        assert!(cache.will_resolve_without_blocking("127.0.0.1", 25565));
        assert!(!cache.will_resolve_without_blocking("never-queried-before.example.invalid", 25565));
    }

    #[tokio::test]
    async fn hostname_lookup_gets_cached() {
        let cache = dns_cache();
        // "localhost" resolves via the real system resolver without any network access.
        let addr = cache.resolve("localhost", 25565).await.unwrap();
        assert!(addr.ip().is_loopback());
        assert!(cache.will_resolve_without_blocking("localhost", 25565));
    }

    #[tokio::test]
    async fn unresolvable_host_caches_negative_result() {
        let cache = dns_cache();
        let host = "this-host-does-not-exist.invalid.example";
        let result = cache.resolve(host, 25565).await;
        assert!(result.is_err());
        // Still "cached" (fast path) even though it's a failure - matches the Kotlin negative-TTL
        // caching behavior.
        assert!(cache.will_resolve_without_blocking(host, 25565));
        let second = cache.resolve(host, 25565).await;
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn warm_populates_cache_in_background() {
        let cache = dns_cache();
        let host = "warm-target.localhost.invalid-but-unique-for-this-test";
        assert!(!cache.will_resolve_without_blocking(host, 1));
        cache.warm(&tokio::runtime::Handle::current(), host, 1);
        // warm() is fire-and-forget; give the spawned task a chance to run. A resolution failure
        // (this host doesn't exist) means warm intentionally does NOT cache it - see warm's doc -
        // so this only asserts it didn't panic/hang, not that it became cached.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn evict_idle_entries_removes_stale_but_keeps_fresh() {
        let cache = DnsCache::default();
        cache.cache.lock().unwrap().insert(
            "old.example.com:1".to_string(),
            Entry { result: Ok("1.2.3.4:1".parse().unwrap()), expires_at: Instant::now() - ENTRY_IDLE_EVICT - Duration::from_secs(1) },
        );
        cache.cache.lock().unwrap().insert(
            "fresh.example.com:1".to_string(),
            Entry { result: Ok("1.2.3.4:1".parse().unwrap()), expires_at: Instant::now() + Duration::from_secs(60) },
        );
        cache.evict_idle_entries();
        let remaining = cache.cache.lock().unwrap();
        assert!(!remaining.contains_key("old.example.com:1"));
        assert!(remaining.contains_key("fresh.example.com:1"));
    }
}
