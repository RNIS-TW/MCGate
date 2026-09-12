//! Port of `tracking/RouteMetricsStore.kt` — optional per-route upload/download byte accounting.
//! In memory it's just two atomics per route; persistence is a periodic JSON flush (atomic
//! write-then-rename) off any hot path, on a dedicated tokio task rather than a Netty/relay
//! thread. Limits are enforced on the same tick (kicking players already over, refusing new
//! logins is checked inline by the caller via `exceeded()`).

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::{Route, RouteMetricsConfig};

pub struct RouteTrafficCounter {
    pub key: String,
    pub upload_bytes: AtomicI64,
    pub download_bytes: AtomicI64,
    pub file: Mutex<Option<String>>,
    pub upload_enabled: std::sync::atomic::AtomicBool,
    pub download_enabled: std::sync::atomic::AtomicBool,
    pub upload_limit: AtomicI64,
    pub download_limit: AtomicI64,
    pub reset_interval_millis: AtomicI64,
    pub reset_at: AtomicI64,
    dirty: AtomicBool,
}

impl RouteTrafficCounter {
    fn new(key: String) -> Self {
        Self {
            key,
            upload_bytes: AtomicI64::new(0),
            download_bytes: AtomicI64::new(0),
            file: Mutex::new(None),
            upload_enabled: AtomicBool::new(false),
            download_enabled: AtomicBool::new(false),
            upload_limit: AtomicI64::new(-1),
            download_limit: AtomicI64::new(-1),
            reset_interval_millis: AtomicI64::new(0),
            reset_at: AtomicI64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    pub fn add_upload(&self, n: i64) {
        if self.upload_enabled.load(Ordering::Relaxed) && n > 0 {
            self.upload_bytes.fetch_add(n, Ordering::Relaxed);
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    pub fn add_download(&self, n: i64) {
        if self.download_enabled.load(Ordering::Relaxed) && n > 0 {
            self.download_bytes.fetch_add(n, Ordering::Relaxed);
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    pub fn upload_exceeded(&self) -> bool {
        self.upload_enabled.load(Ordering::Relaxed) && self.upload_limit.load(Ordering::Relaxed) >= 0 && self.upload_bytes.load(Ordering::Relaxed) >= self.upload_limit.load(Ordering::Relaxed)
    }
    pub fn download_exceeded(&self) -> bool {
        self.download_enabled.load(Ordering::Relaxed)
            && self.download_limit.load(Ordering::Relaxed) >= 0
            && self.download_bytes.load(Ordering::Relaxed) >= self.download_limit.load(Ordering::Relaxed)
    }
    pub fn exceeded(&self) -> bool {
        self.upload_exceeded() || self.download_exceeded()
    }
}

#[derive(Default)]
pub struct RouteMetricsStore {
    counters: Mutex<HashMap<String, Arc<RouteTrafficCounter>>>,
    flush_interval: Mutex<Duration>,
}

pub fn route_metrics_store() -> &'static RouteMetricsStore {
    static INSTANCE: OnceLock<RouteMetricsStore> = OnceLock::new();
    INSTANCE.get_or_init(|| RouteMetricsStore { counters: Mutex::new(HashMap::new()), flush_interval: Mutex::new(Duration::from_secs(10)) })
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn key_for(route: &Route) -> Option<String> {
    let m = route.metrics.as_ref()?;
    if !m.active() {
        return None;
    }
    Some(m.file.clone().unwrap_or_else(|| format!("host:{}", route.host_patterns.iter().map(|p| p.raw.as_str()).collect::<Vec<_>>().join("|"))))
}

impl RouteMetricsStore {
    /// The live counter for `route`, or `None` when it has no active `metrics:` block.
    pub fn handle(&self, route: &Route) -> Option<Arc<RouteTrafficCounter>> {
        let key = key_for(route)?;
        self.counters.lock().unwrap().get(&key).cloned()
    }

    pub fn is_enabled(&self) -> bool {
        !self.counters.lock().unwrap().is_empty()
    }

    /// Applies (or re-applies) the route set: creates counters for newly-metriced routes
    /// (loading any persisted total from disk), updates limits/enabled flags in place for
    /// existing ones, drops counters no longer referenced (flushing first). Idempotent.
    pub fn apply_config(&self, routes: &[Route], flush_interval: Duration) {
        *self.flush_interval.lock().unwrap() = flush_interval;
        let mut desired: HashMap<String, &RouteMetricsConfig> = HashMap::new();
        for route in routes {
            let Some(key) = key_for(route) else { continue };
            desired.entry(key).or_insert_with(|| route.metrics.as_ref().unwrap());
        }

        let mut counters = self.counters.lock().unwrap();
        counters.retain(|key, counter| {
            if desired.contains_key(key) {
                true
            } else {
                flush_counter(counter, true);
                false
            }
        });

        for (key, m) in desired {
            let counter = counters.entry(key.clone()).or_insert_with(|| {
                let c = Arc::new(RouteTrafficCounter::new(key));
                load_persisted(&c, m.file.as_deref());
                c
            });
            *counter.file.lock().unwrap() = m.file.clone();
            counter.upload_enabled.store(m.upload.enabled, Ordering::Relaxed);
            counter.download_enabled.store(m.download.enabled, Ordering::Relaxed);
            counter.upload_limit.store(m.upload.limit, Ordering::Relaxed);
            counter.download_limit.store(m.download.limit, Ordering::Relaxed);
            counter.reset_interval_millis.store(m.reset_interval_millis, Ordering::Relaxed);
            if m.reset_interval_millis > 0 {
                let now = now_millis();
                let reset_at = counter.reset_at.load(Ordering::Relaxed);
                if reset_at == 0 || reset_at > now + m.reset_interval_millis {
                    counter.reset_at.store(now + m.reset_interval_millis, Ordering::Relaxed);
                    counter.dirty.store(true, Ordering::Relaxed);
                }
            } else if counter.reset_at.load(Ordering::Relaxed) != 0 {
                counter.reset_at.store(0, Ordering::Relaxed);
                counter.dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Zeroes `route`'s counters and immediately persists the reset. `None` if the route has no
    /// active counter.
    pub fn reset(&self, route: &Route) -> Option<Arc<RouteTrafficCounter>> {
        let counter = self.handle(route)?;
        zero(&counter);
        flush_counter(&counter, true);
        Some(counter)
    }

    /// Zeroes and persists every counter. Returns how many were reset.
    pub fn reset_all(&self) -> usize {
        let counters = self.counters.lock().unwrap();
        for counter in counters.values() {
            zero(counter);
            flush_counter(counter, true);
        }
        counters.len()
    }

    fn all_counters(&self) -> Vec<Arc<RouteTrafficCounter>> {
        self.counters.lock().unwrap().values().cloned().collect()
    }
}

fn zero(counter: &RouteTrafficCounter) {
    counter.upload_bytes.store(0, Ordering::Relaxed);
    counter.download_bytes.store(0, Ordering::Relaxed);
    let interval = counter.reset_interval_millis.load(Ordering::Relaxed);
    if interval > 0 {
        counter.reset_at.store(now_millis() + interval, Ordering::Relaxed);
    }
    counter.dirty.store(true, Ordering::Relaxed);
}

fn flush_counter(counter: &RouteTrafficCounter, force: bool) {
    let path = counter.file.lock().unwrap().clone();
    let Some(path) = path else { return };
    let was_dirty = counter.dirty.swap(false, Ordering::Relaxed);
    if !force && !was_dirty {
        return;
    }
    let json = serde_json::json!({
        "uploadBytes": counter.upload_bytes.load(Ordering::Relaxed),
        "downloadBytes": counter.download_bytes.load(Ordering::Relaxed),
        "uploadLimit": counter.upload_limit.load(Ordering::Relaxed),
        "downloadLimit": counter.download_limit.load(Ordering::Relaxed),
        "resetAt": counter.reset_at.load(Ordering::Relaxed),
        "updatedAt": now_millis(),
    });
    let target = Path::new(&path);
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }
    let tmp = target.with_extension("tmp");
    match fs::write(&tmp, json.to_string()) {
        Ok(()) => {
            if fs::rename(&tmp, target).is_err() {
                tracing::warn!("Failed to persist route metrics to {path}");
                counter.dirty.store(true, Ordering::Relaxed);
            }
        }
        Err(e) => {
            tracing::warn!("Failed to persist route metrics to {path}: {e}");
            counter.dirty.store(true, Ordering::Relaxed);
        }
    }
}

fn load_persisted(counter: &RouteTrafficCounter, file: Option<&str>) {
    let Some(file) = file else { return };
    let Ok(text) = fs::read_to_string(file) else { return };
    let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) else {
        tracing::warn!("Route metrics file {file} is not valid JSON, starting its counters at 0");
        return;
    };
    counter.upload_bytes.store(data.get("uploadBytes").and_then(|v| v.as_i64()).unwrap_or(0), Ordering::Relaxed);
    counter.download_bytes.store(data.get("downloadBytes").and_then(|v| v.as_i64()).unwrap_or(0), Ordering::Relaxed);
    counter.reset_at.store(data.get("resetAt").and_then(|v| v.as_i64()).unwrap_or(0), Ordering::Relaxed);
    tracing::info!(
        "Loaded route metrics from {file} (upload={}B, download={}B)",
        counter.upload_bytes.load(Ordering::Relaxed),
        counter.download_bytes.load(Ordering::Relaxed)
    );
}

/// Zeroes any counter whose rolling auto-reset deadline has passed and advances it to the next
/// future boundary in one step (even after long downtime, matching `applyScheduledResets`).
fn apply_scheduled_resets(store: &RouteMetricsStore) {
    let now = now_millis();
    for counter in store.all_counters() {
        let interval = counter.reset_interval_millis.load(Ordering::Relaxed);
        let reset_at = counter.reset_at.load(Ordering::Relaxed);
        if interval <= 0 || reset_at <= 0 || now < reset_at {
            continue;
        }
        counter.upload_bytes.store(0, Ordering::Relaxed);
        counter.download_bytes.store(0, Ordering::Relaxed);
        let missed = (now - reset_at) / interval;
        counter.reset_at.store(reset_at + (missed + 1) * interval, Ordering::Relaxed);
        counter.dirty.store(true, Ordering::Relaxed);
        tracing::info!("Route metrics counter '{}' auto-reset; next reset at {}", counter.key, counter.reset_at.load(Ordering::Relaxed));
    }
}

/// Kicks any currently-connected player whose route's cumulative usage has reached its limit.
/// `routes` and `kick_message` are read fresh each tick so a config/messages reload takes effect
/// without restarting the ticker.
fn enforce_limits(store: &RouteMetricsStore, routes: &[Route], kick_message: &str) {
    for session in crate::state::player_sessions().all() {
        let Some(route) = routes.iter().find(|r| r.match_host(&session.host).is_some()) else { continue };
        let Some(counter) = store.handle(route) else { continue };
        if !counter.exceeded() {
            continue;
        }
        tracing::warn!(
            "Route metrics limit reached on '{}' (upload {}/{}, download {}/{}) - kicking '{}'",
            session.host,
            counter.upload_bytes.load(Ordering::Relaxed),
            counter.upload_limit.load(Ordering::Relaxed),
            counter.download_bytes.load(Ordering::Relaxed),
            counter.download_limit.load(Ordering::Relaxed),
            session.name
        );
        session.disconnect.notify_waiters();
        let _ = kick_message; // message-carrying kick needs backend login sniffing - see plan.md
    }
}

/// Spawns the periodic flush/enforce/auto-reset tick. Call once at startup, from within a tokio
/// runtime. `state` supplies the current routes/kick-message fresh on every tick.
pub fn spawn_ticker(state: Arc<crate::app_state::AppState>) {
    tokio::spawn(async move {
        loop {
            let interval = *route_metrics_store().flush_interval.lock().unwrap();
            tokio::time::sleep(interval).await;
            let store = route_metrics_store();
            apply_scheduled_resets(store);
            for counter in store.all_counters() {
                flush_counter(&counter, false);
            }
            let cfg = state.config();
            let messages = state.messages();
            enforce_limits(store, &cfg.routes, &messages.metrics_limit_kick_message);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Route, RouteMetricUsageConfig};

    fn route_with_metrics(file: Option<&str>, upload_limit: i64) -> Route {
        Route {
            host_patterns: vec![crate::host_pattern::HostPattern::new("a.example.com")],
            backend_templates: vec!["127.0.0.1:1".into()],
            strategy: crate::config::Strategy::Sequential,
            cache_ping_ttl_millis: 10_000,
            fallback: None,
            modify_virtual_host: false,
            proxy_protocol: false,
            priority: 0,
            reconnect: crate::config::ReconnectConfig::default(),
            kick_message: "kick".into(),
            voicechat_templates: vec![],
            metrics: Some(RouteMetricsConfig {
                file: file.map(|s| s.to_string()),
                upload: RouteMetricUsageConfig { enabled: true, limit: upload_limit },
                download: RouteMetricUsageConfig::default(),
                reset_interval_millis: 0,
            }),
        }
    }

    #[test]
    fn apply_config_creates_and_tracks_counter() {
        let store = RouteMetricsStore::default();
        let route = route_with_metrics(None, -1);
        store.apply_config(&[route.clone()], Duration::from_secs(10));
        let counter = store.handle(&route).unwrap();
        counter.add_upload(100);
        assert_eq!(counter.upload_bytes.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn exceeded_when_over_limit() {
        let store = RouteMetricsStore::default();
        let route = route_with_metrics(None, 100);
        store.apply_config(&[route.clone()], Duration::from_secs(10));
        let counter = store.handle(&route).unwrap();
        counter.add_upload(50);
        assert!(!counter.exceeded());
        counter.add_upload(60);
        assert!(counter.exceeded());
    }

    #[test]
    fn route_without_metrics_block_has_no_counter() {
        let store = RouteMetricsStore::default();
        let route = crate::config::Route {
            host_patterns: vec![],
            backend_templates: vec![],
            strategy: crate::config::Strategy::Sequential,
            cache_ping_ttl_millis: 10_000,
            fallback: None,
            modify_virtual_host: false,
            proxy_protocol: false,
            priority: 0,
            reconnect: crate::config::ReconnectConfig::default(),
            kick_message: String::new(),
            voicechat_templates: vec![],
            metrics: None,
        };
        store.apply_config(&[route.clone()], Duration::from_secs(10));
        assert!(store.handle(&route).is_none());
    }

    #[test]
    fn persists_and_reloads_from_file() {
        let dir = std::env::temp_dir().join(format!("mcgate-metrics-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("usage.json");
        let file_str = file_path.to_str().unwrap();

        let store = RouteMetricsStore::default();
        let route = route_with_metrics(Some(file_str), -1);
        store.apply_config(&[route.clone()], Duration::from_secs(10));
        let counter = store.handle(&route).unwrap();
        counter.add_upload(12345);
        flush_counter(&counter, true);
        assert!(file_path.exists());

        // A fresh store loading the same route/file should pick up the persisted total.
        let store2 = RouteMetricsStore::default();
        store2.apply_config(&[route.clone()], Duration::from_secs(10));
        let counter2 = store2.handle(&route).unwrap();
        assert_eq!(counter2.upload_bytes.load(Ordering::Relaxed), 12345);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reset_zeroes_and_persists() {
        let dir = std::env::temp_dir().join(format!("mcgate-metrics-reset-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("usage.json");
        let store = RouteMetricsStore::default();
        let route = route_with_metrics(Some(file_path.to_str().unwrap()), -1);
        store.apply_config(&[route.clone()], Duration::from_secs(10));
        let counter = store.handle(&route).unwrap();
        counter.add_upload(500);
        store.reset(&route);
        assert_eq!(counter.upload_bytes.load(Ordering::Relaxed), 0);
        fs::remove_dir_all(&dir).unwrap();
    }
}
