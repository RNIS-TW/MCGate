//! Live state for the console REPL, API server, and (as of sections 3-5) the actual relay
//! pipeline: `PlayerSessions`/`PlayerSession` (from `routing/PlayerSessions.kt`), `RouteRuntime`
//! (from `routing/BackendSelector.kt`), and `MetricsSnapshot`/`collect_metrics` (from
//! `routing/LiveMetrics.kt`). `server.rs` now populates `PlayerSessions` on every real login and
//! bumps `RouteRuntime` on every real connect/disconnect — this is live data, not a placeholder.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU32};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::Route;

/// One connected player. Mirrors `PlayerSession` in `PlayerSessions.kt`, minus `channel: Channel`
/// (Netty-specific) — replaced by `disconnect`, a `Notify` the owning relay task (`server.rs`)
/// awaits alongside the byte splice, so the console `kick`/API-triggered disconnect can end a
/// live connection. Unlike the Kotlin version, this can't carry a kick *message* yet: doing so
/// correctly requires knowing the connection's negotiated compression threshold, which needs
/// backend login sniffing (deliberately not ported — see plan.md section 3) since writing an
/// unframed packet into a compressed/encrypted stream would corrupt it. A kicked player is simply
/// disconnected, same as Kotlin's own fallback path for a connection it can't safely message.
pub struct PlayerSession {
    pub name: String,
    pub uuid: Uuid,
    pub host: String,
    pub remote_address: String,
    /// The backend currently relaying this session, or `None` while held waiting for reconnect.
    pub backend: Option<SocketAddr>,
    pub connected_at_millis: i64,
    pub protocol_version: i32,
    pub compression_threshold: i32,
    pub encrypted: bool,
    pub login_attempts: AtomicU32,
    pub packets_sent: AtomicI64,
    pub packets_received: AtomicI64,
    pub bytes_sent: AtomicI64,
    pub bytes_received: AtomicI64,
    pub disconnect: Notify,
}

/// Process-wide registry of currently connected players, keyed by UUID — mirrors
/// `PlayerSessions.kt`'s `object`.
#[derive(Default)]
pub struct PlayerSessions {
    sessions: Mutex<HashMap<Uuid, std::sync::Arc<PlayerSession>>>,
}

pub fn player_sessions() -> &'static PlayerSessions {
    static INSTANCE: OnceLock<PlayerSessions> = OnceLock::new();
    INSTANCE.get_or_init(PlayerSessions::default)
}

impl PlayerSessions {
    pub fn put(&self, session: std::sync::Arc<PlayerSession>) {
        self.sessions.lock().unwrap().insert(session.uuid, session);
    }

    pub fn remove(&self, uuid: Uuid) {
        self.sessions.lock().unwrap().remove(&uuid);
    }

    /// All sessions, sorted by name (case-insensitive) — matches `PlayerSessions.all()`.
    pub fn all(&self) -> Vec<std::sync::Arc<PlayerSession>> {
        let mut v: Vec<_> = self.sessions.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|s| s.name.to_lowercase());
        v
    }

    pub fn find_by_name(&self, name: &str) -> Option<std::sync::Arc<PlayerSession>> {
        self.sessions.lock().unwrap().values().find(|s| s.name.eq_ignore_ascii_case(name)).cloned()
    }

    /// Currently connected players grouped by literal virtual host — matches
    /// `PlayerSessions.onlineByHost()`.
    pub fn online_by_host(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for s in self.sessions.lock().unwrap().values() {
            *counts.entry(s.host.clone()).or_insert(0) += 1;
        }
        counts
    }

    pub fn len(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

/// Per-backend runtime state for one route: active-connection counts, last-observed latency,
/// round-robin cursor, and in-flight status-dial counts — port of `BackendSelector.kt`'s
/// `RouteRuntime`, keyed by resolved backend address.
pub struct RouteRuntime {
    active_connections: Mutex<HashMap<SocketAddr, i64>>,
    /// addr -> (latency millis, measured-at). TTL-expired entries are evicted lazily by
    /// `latency_of`/`record_latency`, same as `BackendSelector.kt`'s `RouteRuntime`.
    latencies: Mutex<HashMap<SocketAddr, (i64, Instant)>>,
    pub round_robin_counter: AtomicI64,
    status_dials_in_flight: Mutex<HashMap<SocketAddr, i32>>,
}

/// How long a latency reading stays valid before `latency_of` treats it as absent — mirrors
/// `BackendSelector.kt`'s `LATENCY_TTL_MILLIS`.
const LATENCY_TTL: std::time::Duration = std::time::Duration::from_secs(3 * 60);

/// Max concurrent live status/ping dials to a single backend address — mirrors
/// `BackendSelector.kt`'s `MAX_CONCURRENT_STATUS_DIALS`.
pub const MAX_CONCURRENT_STATUS_DIALS: i32 = 8;

impl Default for RouteRuntime {
    fn default() -> Self {
        Self {
            active_connections: Mutex::new(HashMap::new()),
            latencies: Mutex::new(HashMap::new()),
            round_robin_counter: AtomicI64::new(0),
            status_dials_in_flight: Mutex::new(HashMap::new()),
        }
    }
}

impl RouteRuntime {
    pub fn active_connections_of(&self, addr: SocketAddr) -> i64 {
        *self.active_connections.lock().unwrap().get(&addr).unwrap_or(&0)
    }

    pub fn record_connect_opened(&self, addr: SocketAddr) {
        *self.active_connections.lock().unwrap().entry(addr).or_insert(0) += 1;
    }

    /// Removes `addr`'s entry once its count drops to zero rather than leaving a permanent
    /// zero-valued entry behind — matters for wildcard routes, where every distinct captured
    /// value resolves to a different address.
    pub fn record_connect_closed(&self, addr: SocketAddr) {
        let mut map = self.active_connections.lock().unwrap();
        if let Some(count) = map.get_mut(&addr) {
            *count -= 1;
            if *count <= 0 {
                map.remove(&addr);
            }
        }
    }

    pub fn record_latency(&self, addr: SocketAddr, millis: i64) {
        let now = Instant::now();
        let mut map = self.latencies.lock().unwrap();
        map.insert(addr, (millis, now));
        // Opportunistic sweep: latency_of only evicts entries it's actually asked for, so a
        // wildcard route's one-off captured backends that never get read back would linger.
        map.retain(|_, (_, measured_at)| now.duration_since(*measured_at) <= LATENCY_TTL);
    }

    pub fn latency_of(&self, addr: SocketAddr) -> Option<i64> {
        let mut map = self.latencies.lock().unwrap();
        let (millis, measured_at) = *map.get(&addr)?;
        if Instant::now().duration_since(measured_at) > LATENCY_TTL {
            map.remove(&addr);
            return None;
        }
        Some(millis)
    }

    /// Reserves a status-dial slot for `addr` if fewer than `max` are already in flight.
    pub fn try_begin_status_dial(&self, addr: SocketAddr, max: i32) -> bool {
        let mut map = self.status_dials_in_flight.lock().unwrap();
        let count = map.entry(addr).or_insert(0);
        *count += 1;
        if *count > max {
            *count -= 1;
            if *count <= 0 {
                map.remove(&addr);
            }
            false
        } else {
            true
        }
    }

    pub fn end_status_dial(&self, addr: SocketAddr) {
        let mut map = self.status_dials_in_flight.lock().unwrap();
        if let Some(count) = map.get_mut(&addr) {
            *count -= 1;
            if *count <= 0 {
                map.remove(&addr);
            }
        }
    }
}

/// Live per-backend stats for one route/backend pair — mirrors `BackendMetric`.
pub struct BackendMetric {
    pub route_index: usize,
    pub hosts: Vec<String>,
    pub backend: String,
    pub active: i64,
    pub latency_millis: Option<i64>,
}

/// One route's cumulative upload/download byte accounting — mirrors `RouteMetric`. Never
/// populated yet: `RouteMetricsStore` (section 5) doesn't exist, so `collect_metrics` always
/// yields an empty `route_metrics` list — correct today (no route can have accrued usage with no
/// relay pipeline running) rather than a stub.
pub struct RouteMetric {
    pub route_index: usize,
    pub hosts: Vec<String>,
    pub file: Option<String>,
    pub upload_enabled: bool,
    pub upload_bytes: i64,
    pub upload_limit: i64,
    pub download_enabled: bool,
    pub download_bytes: i64,
    pub download_limit: i64,
    pub reset_interval_millis: i64,
    pub reset_at: i64,
}

/// One point-in-time read of everything MCGate's live stats surface — mirrors `MetricsSnapshot`.
pub struct MetricsSnapshot {
    pub timestamp_millis: i64,
    pub players_online: usize,
    pub online_by_host: HashMap<String, usize>,
    pub uptime_seconds: f64,
    pub backends: Vec<BackendMetric>,
    pub players: Vec<std::sync::Arc<PlayerSession>>,
    pub tracking_enabled: bool,
    pub tracking_queued_records: i64,
    pub tracking_dropped_records_total: i64,
    pub route_metrics: Vec<RouteMetric>,
}

fn process_start() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// Call once, as early as possible in `main`, so `uptime_seconds` below is accurate — mirrors the
/// JVM's own process-start timestamp being implicitly available via `RuntimeMXBean`.
pub fn mark_process_start() {
    process_start();
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Gathers a `MetricsSnapshot` right now — mirrors `collectMetrics`. `runtime_supplier` looks up
/// a route's `RouteRuntime` the same way the Kotlin version's `GateState.routeRuntimes` does.
pub async fn collect_metrics(routes: &[Route], runtime_supplier: impl Fn(&Route) -> Option<std::sync::Arc<RouteRuntime>>) -> MetricsSnapshot {
    let mut backends = Vec::new();
    for (index, route) in routes.iter().enumerate() {
        let runtime = runtime_supplier(route);
        let hosts: Vec<String> = route.host_patterns.iter().map(|p| p.raw.clone()).collect();
        // Resolving a wildcard route's backend needs a captured value from an actual connection,
        // which isn't available here - only non-wildcard routes get live active/latency data,
        // matching the same caveat the Kotlin API/console surfaces carry.
        let resolved = if route.host_patterns.iter().all(|p| p.wildcard_count == 0) {
            route.resolve_backends(&[]).await.ok()
        } else {
            None
        };
        for (i, template) in route.backend_templates.iter().enumerate() {
            let addr = resolved.as_ref().and_then(|v| v.get(i).copied());
            let active = addr.and_then(|a| runtime.as_ref().map(|r| r.active_connections_of(a))).unwrap_or(0);
            let latency = addr.and_then(|a| runtime.as_ref().and_then(|r| r.latency_of(a)));
            backends.push(BackendMetric { route_index: index, hosts: hosts.clone(), backend: template.clone(), active, latency_millis: latency });
        }
    }

    let sessions = player_sessions();
    let players = sessions.all();

    let route_metrics = routes
        .iter()
        .enumerate()
        .filter_map(|(index, route)| {
            let counter = crate::route_metrics_store::route_metrics_store().handle(route)?;
            use std::sync::atomic::Ordering;
            let file = counter.file.lock().unwrap().clone();
            Some(RouteMetric {
                route_index: index,
                hosts: route.host_patterns.iter().map(|p| p.raw.clone()).collect(),
                file,
                upload_enabled: counter.upload_enabled.load(Ordering::Relaxed),
                upload_bytes: counter.upload_bytes.load(Ordering::Relaxed),
                upload_limit: counter.upload_limit.load(Ordering::Relaxed),
                download_enabled: counter.download_enabled.load(Ordering::Relaxed),
                download_bytes: counter.download_bytes.load(Ordering::Relaxed),
                download_limit: counter.download_limit.load(Ordering::Relaxed),
                reset_interval_millis: counter.reset_interval_millis.load(Ordering::Relaxed),
                reset_at: counter.reset_at.load(Ordering::Relaxed),
            })
        })
        .collect();

    MetricsSnapshot {
        timestamp_millis: now_millis(),
        players_online: players.len(),
        online_by_host: sessions.online_by_host(),
        uptime_seconds: process_start().elapsed().as_secs_f64(),
        backends,
        players,
        tracking_enabled: crate::connection_tracker::connection_tracker().is_enabled(),
        tracking_queued_records: crate::connection_tracker::connection_tracker().queued_records(),
        tracking_dropped_records_total: crate::connection_tracker::connection_tracker().dropped_records_total(),
        route_metrics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_session(name: &str, host: &str) -> std::sync::Arc<PlayerSession> {
        std::sync::Arc::new(PlayerSession {
            name: name.to_string(),
            uuid: Uuid::new_v4(),
            host: host.to_string(),
            remote_address: "127.0.0.1:12345".to_string(),
            backend: None,
            connected_at_millis: 0,
            protocol_version: 763,
            compression_threshold: -1,
            encrypted: false,
            login_attempts: AtomicU32::new(1),
            packets_sent: AtomicI64::new(0),
            packets_received: AtomicI64::new(0),
            bytes_sent: AtomicI64::new(0),
            bytes_received: AtomicI64::new(0),
            disconnect: Notify::new(),
        })
    }

    #[test]
    fn put_get_remove_roundtrip() {
        let registry = PlayerSessions::default();
        let s = sample_session("Steve", "play.example.com");
        let uuid = s.uuid;
        registry.put(s);
        assert_eq!(registry.len(), 1);
        assert!(registry.find_by_name("steve").is_some()); // case-insensitive
        registry.remove(uuid);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn all_sorted_by_name_case_insensitive() {
        let registry = PlayerSessions::default();
        registry.put(sample_session("bob", "a"));
        registry.put(sample_session("Alice", "a"));
        let names: Vec<String> = registry.all().iter().map(|s| s.name.clone()).collect();
        assert_eq!(names, vec!["Alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn online_by_host_groups_correctly() {
        let registry = PlayerSessions::default();
        registry.put(sample_session("a", "host1"));
        registry.put(sample_session("b", "host1"));
        registry.put(sample_session("c", "host2"));
        let counts = registry.online_by_host();
        assert_eq!(counts.get("host1"), Some(&2));
        assert_eq!(counts.get("host2"), Some(&1));
    }

    #[test]
    fn route_runtime_reports_no_data_when_empty() {
        let runtime = RouteRuntime::default();
        let addr: SocketAddr = "127.0.0.1:25565".parse().unwrap();
        assert_eq!(runtime.active_connections_of(addr), 0);
        assert_eq!(runtime.latency_of(addr), None);
    }

    #[tokio::test]
    async fn collect_metrics_empty_state() {
        let snapshot = collect_metrics(&[], |_| None).await;
        assert_eq!(snapshot.players_online, 0);
        assert!(snapshot.route_metrics.is_empty());
        assert!(!snapshot.tracking_enabled);
    }
}
