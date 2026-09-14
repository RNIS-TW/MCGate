//! Shared live state for the console REPL and API server — the Rust analogue of `Main.kt`'s
//! `GateState` + `stateRef: AtomicReference<GateState>` + the `applyConfig`/`reloadAll` closures.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::GateConfig;
use crate::logging::LogLevelHandle;
use crate::config::messages::GateMessages;
use crate::state::RouteRuntime;

pub struct AppState {
    pub config_path: String,
    pub messages_path: String,
    config: Mutex<GateConfig>,
    messages: Mutex<GateMessages>,
    /// Keyed by index into `config.routes`. Kotlin carries `RouteRuntime` across a reload via
    /// structural `Route` equality so per-backend counters/cursors survive an unrelated route
    /// being added/removed elsewhere in the file; since nothing populates these yet (section 3),
    /// this currently just recreates an empty map on every reload — revisit once route dialing
    /// exists and carrying real counters across a reload actually matters.
    route_runtimes: Mutex<HashMap<usize, Arc<RouteRuntime>>>,
    pub started_at: Instant,
    pub log_level: LogLevelHandle,
    /// Captured once at construction (always from inside the tokio runtime — see `main.rs`).
    /// Threaded through to `dns_cache`'s warming on every reload since the config-file watcher's
    /// callback runs on its own background thread, not a tokio task — see
    /// `config::warm_static_backends`'s doc.
    runtime_handle: tokio::runtime::Handle,
    /// A `Weak` reference to this same instance, set immediately after construction — lets
    /// `set_config` (which only has `&self`) hand `stats_logger` an `Arc<AppState>` when it needs
    /// one, without every caller having to thread an `Arc` through by hand. Standard
    /// self-referential-`Arc` pattern.
    self_weak: OnceLock<Weak<AppState>>,
}

impl AppState {
    pub fn new(
        config_path: String,
        messages_path: String,
        config: GateConfig,
        messages: GateMessages,
        log_level: LogLevelHandle,
        runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let route_runtimes = (0..config.routes.len()).map(|i| (i, Arc::new(RouteRuntime::default()))).collect();
        crate::config::warm_static_backends(&config.routes, &runtime_handle);
        crate::state::route_metrics_store::route_metrics_store().apply_config(&config.routes, Duration::from_millis(10_000));
        crate::state::connection_tracker::connection_tracker().apply_config(config.connection_tracking.clone());
        crate::udp::throttle::udp_throttle().apply(&config.udp_throttle);
        let state = Arc::new(Self {
            config_path,
            messages_path,
            config: Mutex::new(config.clone()),
            messages: Mutex::new(messages),
            route_runtimes: Mutex::new(route_runtimes),
            started_at: Instant::now(),
            log_level,
            runtime_handle,
            self_weak: OnceLock::new(),
        });
        let _ = state.self_weak.set(Arc::downgrade(&state));
        crate::state::stats_logger::stats_logger().apply_config(config.stats_logging, state.clone());
        state
    }

    pub fn config(&self) -> GateConfig {
        self.config.lock().unwrap().clone()
    }

    pub fn messages(&self) -> GateMessages {
        self.messages.lock().unwrap().clone()
    }

    pub fn route_runtime(&self, index: usize) -> Option<Arc<RouteRuntime>> {
        self.route_runtimes.lock().unwrap().get(&index).cloned()
    }

    /// The tokio runtime handle captured at startup — for callers on a non-tokio thread (the
    /// console's plain `std::thread`) that need to `block_on` a small async operation.
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        &self.runtime_handle
    }

    fn set_config(&self, new_config: GateConfig) {
        // Carry per-backend runtime state (active-connection counts, round-robin cursor, latency
        // readings) forward for routes that survived the reload unchanged - otherwise a reload
        // resets every counter to zero, wrongly zeroing active-connection counts for routes that
        // didn't even change, and briefly mis-routing `least-connections`/`lowest-latency` until
        // players reconnect. Structural `Route` equality (see `config.rs`'s `PartialEq` impl)
        // makes this work across reloads, matching `GateState` in the Kotlin version.
        let old_routes = self.config.lock().unwrap().routes.clone();
        let old_runtimes = self.route_runtimes.lock().unwrap().clone();
        let new_runtimes: HashMap<usize, Arc<RouteRuntime>> = new_config
            .routes
            .iter()
            .enumerate()
            .map(|(new_index, route)| {
                let carried = old_routes.iter().position(|r| r == route).and_then(|old_index| old_runtimes.get(&old_index).cloned());
                (new_index, carried.unwrap_or_default())
            })
            .collect();
        *self.route_runtimes.lock().unwrap() = new_runtimes;

        self.log_level.apply(&new_config.log_level);
        crate::config::warm_static_backends(&new_config.routes, &self.runtime_handle);
        crate::state::route_metrics_store::route_metrics_store().apply_config(&new_config.routes, Duration::from_millis(10_000));
        crate::state::connection_tracker::connection_tracker().apply_config(new_config.connection_tracking.clone());
        crate::udp::throttle::udp_throttle().apply(&new_config.udp_throttle);
        if let Some(state) = self.self_weak.get().and_then(Weak::upgrade) {
            crate::state::stats_logger::stats_logger().apply_config(new_config.stats_logging.clone(), state);
        }
        *self.config.lock().unwrap() = new_config;
    }

    fn set_messages(&self, new_messages: GateMessages) {
        *self.messages.lock().unwrap() = new_messages;
    }

    /// Re-reads both files from disk regardless of which one changed, since a route can depend
    /// on either — matches `reloadAll` in `Main.kt`. Used by both the file watchers and the
    /// console `reload` command.
    pub fn reload_all(&self, reason: &str) {
        match GateMessages::load(&self.messages_path) {
            Ok(new_messages) => {
                self.set_messages(new_messages.clone());
                match crate::config::load_config(&self.config_path, &new_messages) {
                    Ok(new_config) => {
                        let route_count = new_config.routes.len();
                        self.set_config(new_config);
                        tracing::info!(
                            "{reason}: reloaded {route_count} route(s) from {} and messages from {}",
                            self.config_path,
                            self.messages_path
                        );
                    }
                    Err(e) => tracing::error!("{reason}: reload failed, keeping previous config/messages: {e}"),
                }
            }
            Err(e) => tracing::error!("{reason}: reload failed, keeping previous config/messages: {e}"),
        }
    }

    /// Applies a config-file-only change (config watcher fired, messages unchanged) — matches the
    /// `applyConfig` call in `ConfigLoader.watch`'s callback.
    pub fn apply_config_change(&self, new_config: GateConfig) {
        let route_count = new_config.routes.len();
        self.set_config(new_config);
        tracing::info!("Config changed, loaded {route_count} route(s)");
    }

    /// Applies a messages-file-only change, re-parsing config.yml against the new defaults —
    /// matches `MessagesLoader.watch`'s callback in `Main.kt`.
    pub fn apply_messages_change(&self, new_messages: GateMessages) -> Result<()> {
        self.set_messages(new_messages.clone());
        let new_config = crate::config::load_config(&self.config_path, &new_messages)?;
        let route_count = new_config.routes.len();
        self.set_config(new_config);
        tracing::info!("Messages changed, reloading {} with new defaults ({route_count} route(s))", self.config_path);
        Ok(())
    }
}
