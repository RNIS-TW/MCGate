//! Port of `tracking/StatsLogger.kt` — optional, off-by-default local time-series log of
//! MCGate's live stats, written to SQLite on a fixed interval.
//!
//! Unlike `connection_tracker.rs` (which can see a burst of records at once, from many
//! concurrent tasks, and needs a bounded queue + batched writer to survive that), this only ever
//! writes one snapshot every `interval_millis` from a single dedicated thread — no
//! concurrent-producer backpressure problem to solve, so a plain timed loop doing a direct write
//! is enough.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rusqlite::Connection;

use crate::state::app_state::AppState;
use crate::config::StatsLoggingConfig;
use crate::state::MetricsSnapshot;

struct Inner {
    shutdown: crossbeam_channel::Sender<()>,
    handle: std::thread::JoinHandle<()>,
}

#[derive(Default)]
pub struct StatsLogger {
    inner: Mutex<Option<Inner>>,
    current_config: Mutex<StatsLoggingConfig>,
}

pub fn stats_logger() -> &'static StatsLogger {
    static INSTANCE: OnceLock<StatsLogger> = OnceLock::new();
    INSTANCE.get_or_init(StatsLogger::default)
}

impl StatsLogger {
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }

    /// Applies (or re-applies) `new_config`, starting/stopping/restarting the scheduled snapshot
    /// task as needed — same idempotent pattern as `ConnectionTracker::apply_config`. `state`
    /// supplies both the routes/runtime `collect_metrics` needs and the tokio `Handle` the
    /// (plain `std::thread`) writer bridges into it with.
    pub fn apply_config(&'static self, new_config: StatsLoggingConfig, state: Arc<AppState>) {
        {
            let current = self.current_config.lock().unwrap();
            if *current == new_config {
                return;
            }
        }
        self.stop_internal();
        *self.current_config.lock().unwrap() = new_config.clone();
        if new_config.enabled {
            self.start_internal(new_config, state);
        }
    }

    pub fn shutdown(&self) {
        self.stop_internal();
    }

    fn start_internal(&'static self, cfg: StatsLoggingConfig, state: Arc<AppState>) {
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);
        let db_path = cfg.db_path.clone();
        let handle = match open_connection(&db_path) {
            Ok(conn) => {
                let handle = std::thread::Builder::new()
                    .name("stats-logger".into())
                    .spawn(move || writer_loop(conn, shutdown_rx, cfg, state))
                    .expect("failed to spawn stats-logger thread");
                tracing::info!("Stats logging enabled, writing to {db_path}");
                handle
            }
            Err(e) => {
                tracing::error!("Failed to start stats logging, feature stays disabled: {e}");
                return;
            }
        };
        *self.inner.lock().unwrap() = Some(Inner { shutdown: shutdown_tx, handle });
    }

    fn stop_internal(&self) {
        let inner = self.inner.lock().unwrap().take();
        if let Some(inner) = inner {
            let _ = inner.shutdown.send(());
            let _ = inner.handle.join();
        }
    }
}

fn open_connection(db_path: &str) -> rusqlite::Result<Connection> {
    if let Some(parent) = std::path::Path::new(db_path).parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA auto_vacuum=INCREMENTAL;
         PRAGMA foreign_keys=ON;
         CREATE TABLE IF NOT EXISTS stats_snapshots (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             timestamp INTEGER NOT NULL,
             players_online INTEGER NOT NULL,
             uptime_seconds REAL NOT NULL,
             tracking_enabled INTEGER NOT NULL,
             tracking_queued_records INTEGER NOT NULL,
             tracking_dropped_records_total INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_stats_snapshots_timestamp ON stats_snapshots(timestamp);
         CREATE TABLE IF NOT EXISTS stats_host_snapshots (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             snapshot_id INTEGER NOT NULL REFERENCES stats_snapshots(id) ON DELETE CASCADE,
             host TEXT NOT NULL,
             players_online INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_stats_host_snapshots_snapshot_id ON stats_host_snapshots(snapshot_id);
         CREATE TABLE IF NOT EXISTS stats_backend_snapshots (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             snapshot_id INTEGER NOT NULL REFERENCES stats_snapshots(id) ON DELETE CASCADE,
             route_index INTEGER NOT NULL,
             hosts TEXT NOT NULL,
             backend TEXT NOT NULL,
             active_connections INTEGER NOT NULL,
             latency_ms INTEGER
         );
         CREATE INDEX IF NOT EXISTS idx_stats_backend_snapshots_snapshot_id ON stats_backend_snapshots(snapshot_id);",
    )?;
    Ok(conn)
}

fn writer_loop(mut conn: Connection, shutdown_rx: crossbeam_channel::Receiver<()>, cfg: StatsLoggingConfig, state: Arc<AppState>) {
    let interval = Duration::from_millis(cfg.interval_millis.max(1) as u64);
    let handle = state.runtime_handle().clone();
    loop {
        crossbeam_channel::select! {
            recv(shutdown_rx) -> _ => break,
            default(interval) => {}
        }
        let snapshot = handle.block_on(collect_snapshot(&state));
        if let Err(e) = write_snapshot(&mut conn, &snapshot) {
            tracing::warn!("Stats logging write failed: {e}");
        }
        if let Err(e) = prune(&conn, &cfg) {
            tracing::warn!("Stats logging prune failed: {e}");
        }
    }
}

async fn collect_snapshot(state: &Arc<AppState>) -> MetricsSnapshot {
    let cfg = state.config();
    let state = state.clone();
    crate::state::collect_metrics(&cfg.routes, move |r| cfg_route_runtime(&state, r)).await
}

fn cfg_route_runtime(state: &Arc<AppState>, route: &crate::config::Route) -> Option<Arc<crate::state::RouteRuntime>> {
    let cfg = state.config();
    cfg.routes.iter().position(|r| r == route).and_then(|i| state.route_runtime(i))
}

fn write_snapshot(conn: &mut Connection, s: &MetricsSnapshot) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    let snapshot_id: i64 = {
        tx.execute(
            "INSERT INTO stats_snapshots
             (timestamp, players_online, uptime_seconds, tracking_enabled, tracking_queued_records, tracking_dropped_records_total)
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![s.timestamp_millis, s.players_online as i64, s.uptime_seconds, s.tracking_enabled as i32, s.tracking_queued_records, s.tracking_dropped_records_total],
        )?;
        tx.last_insert_rowid()
    };

    if !s.online_by_host.is_empty() {
        let mut stmt = tx.prepare("INSERT INTO stats_host_snapshots (snapshot_id, host, players_online) VALUES (?1,?2,?3)")?;
        for (host, count) in &s.online_by_host {
            stmt.execute(rusqlite::params![snapshot_id, host, *count as i64])?;
        }
    }

    if !s.backends.is_empty() {
        let mut stmt = tx.prepare(
            "INSERT INTO stats_backend_snapshots (snapshot_id, route_index, hosts, backend, active_connections, latency_ms) VALUES (?1,?2,?3,?4,?5,?6)",
        )?;
        for b in &s.backends {
            stmt.execute(rusqlite::params![snapshot_id, b.route_index as i64, b.hosts.join(","), b.backend, b.active, b.latency_millis])?;
        }
    }

    tx.commit()
}

fn prune(conn: &Connection, cfg: &StatsLoggingConfig) -> rusqlite::Result<()> {
    let cutoff = now_millis() - cfg.retention_days as i64 * 86_400_000;
    conn.execute("DELETE FROM stats_snapshots WHERE timestamp < ?1", rusqlite::params![cutoff])?;
    conn.execute(
        "DELETE FROM stats_snapshots WHERE id IN (SELECT id FROM stats_snapshots ORDER BY id DESC LIMIT -1 OFFSET ?1)",
        rusqlite::params![cfg.max_records],
    )?;
    // stats_host_snapshots/stats_backend_snapshots rows are cleaned up automatically by the
    // ON DELETE CASCADE foreign keys above (PRAGMA foreign_keys=ON, set at connection open).
    conn.execute_batch("PRAGMA incremental_vacuum(2000)")
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            // A genuinely "now" timestamp - see connection_tracker.rs's identical fix for why a
            // small constant would already look ancient to retention pruning regardless of the
            // configured window.
            timestamp_millis: now_millis(),
            players_online: 3,
            online_by_host: std::collections::HashMap::from([("a.example.com".to_string(), 3usize)]),
            uptime_seconds: 12.5,
            backends: vec![],
            players: vec![],
            tracking_enabled: false,
            tracking_queued_records: 0,
            tracking_dropped_records_total: 0,
            route_metrics: vec![],
        }
    }

    #[test]
    fn schema_creation_and_write_round_trip() {
        let dir = std::env::temp_dir().join(format!("mcgate-statslogger-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("stats.db");

        let mut conn = open_connection(db_path.to_str().unwrap()).unwrap();
        write_snapshot(&mut conn, &sample_snapshot()).unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM stats_snapshots", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
        let host_count: i64 = conn.query_row("SELECT COUNT(*) FROM stats_host_snapshots", [], |r| r.get(0)).unwrap();
        assert_eq!(host_count, 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn prune_deletes_old_snapshots_and_cascades_host_rows() {
        let dir = std::env::temp_dir().join(format!("mcgate-statslogger-prune-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("stats.db");

        let mut conn = open_connection(db_path.to_str().unwrap()).unwrap();
        let mut old = sample_snapshot();
        old.timestamp_millis = 0;
        write_snapshot(&mut conn, &old).unwrap();
        write_snapshot(&mut conn, &sample_snapshot()).unwrap();

        let cfg = StatsLoggingConfig { retention_days: 1, max_records: 200_000, ..StatsLoggingConfig::default() };
        prune(&conn, &cfg).unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM stats_snapshots", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
        let host_count: i64 = conn.query_row("SELECT COUNT(*) FROM stats_host_snapshots", [], |r| r.get(0)).unwrap();
        assert_eq!(host_count, 1, "cascade delete should have removed the pruned snapshot's host rows too");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
