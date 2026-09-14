//! Port of `tracking/ConnectionTracker.kt` — optional, off-by-default persistence of completed
//! player sessions to a local SQLite database.
//!
//! Three things keep this from becoming a memory leak or a source of backpressure on the async
//! tasks that call `record`, matching the Kotlin version's reasoning exactly:
//!
//!  1. `record` only ever `try_send`s to a bounded channel and returns immediately. A full
//!     channel (the writer falling behind) drops the record — counted — rather than blocking the
//!     caller or growing without bound.
//!  2. A single dedicated writer thread (a plain `std::thread`, not a tokio task — SQLite access
//!     is blocking I/O, and this thread owns the one `rusqlite::Connection` for the file's
//!     lifetime) drains the channel in batches, each committed as one transaction.
//!  3. The same thread periodically deletes rows past the retention window and caps total row
//!     count, then runs an incremental vacuum — interleaved into its own loop rather than a
//!     second thread, since it needs the same `Connection` and SQLite only allows one writer.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use uuid::Uuid;

use crate::config::ConnectionTrackingConfig;

pub struct ConnectionRecord {
    pub uuid: Uuid,
    pub name: String,
    pub ip: String,
    pub host: String,
    pub protocol_version: i32,
    pub login_attempts: i32,
    pub packets_sent: i64,
    pub packets_received: i64,
    pub bytes_sent: i64,
    pub bytes_received: i64,
    pub compression_threshold: i32,
    pub encrypted: bool,
    pub connected_at: i64,
    pub disconnected_at: i64,
}

struct Inner {
    sender: crossbeam_channel::Sender<ConnectionRecord>,
    shutdown: crossbeam_channel::Sender<()>,
    handle: std::thread::JoinHandle<()>,
}

#[derive(Default)]
pub struct ConnectionTracker {
    inner: Mutex<Option<Inner>>,
    current_config: Mutex<ConnectionTrackingConfig>,
    dropped_since_start: AtomicU64,
    queued_hint: AtomicI64,
}

pub fn connection_tracker() -> &'static ConnectionTracker {
    static INSTANCE: OnceLock<ConnectionTracker> = OnceLock::new();
    INSTANCE.get_or_init(ConnectionTracker::default)
}

impl ConnectionTracker {
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }

    pub fn dropped_records_total(&self) -> i64 {
        self.dropped_since_start.load(Ordering::Relaxed) as i64
    }

    pub fn queued_records(&self) -> i64 {
        self.queued_hint.load(Ordering::Relaxed).max(0)
    }

    /// Queues `record` for durable persistence. No-op (and effectively free) when tracking is
    /// disabled. Never blocks.
    pub fn record(&self, rec: ConnectionRecord) {
        let guard = self.inner.lock().unwrap();
        let Some(inner) = guard.as_ref() else { return };
        if inner.sender.try_send(rec).is_err() {
            let dropped = self.dropped_since_start.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped % 500 == 0 {
                tracing::warn!("Connection tracking queue full, dropping records ({dropped} dropped since start)");
            }
        } else {
            self.queued_hint.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Applies (or re-applies) `new_config`: starts/stops/restarts the writer thread as needed.
    /// A no-op unless something actually changed, so this is safe to call on every config
    /// (re)load.
    pub fn apply_config(&'static self, new_config: ConnectionTrackingConfig) {
        {
            let current = self.current_config.lock().unwrap();
            if *current == new_config {
                return;
            }
        }
        self.stop_internal();
        *self.current_config.lock().unwrap() = new_config.clone();
        if new_config.enabled {
            self.start_internal(new_config);
        }
    }

    pub fn shutdown(&self) {
        self.stop_internal();
    }

    fn start_internal(&'static self, cfg: ConnectionTrackingConfig) {
        let (tx, rx) = crossbeam_channel::bounded::<ConnectionRecord>(cfg.queue_capacity.max(1) as usize);
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);
        self.dropped_since_start.store(0, Ordering::Relaxed);
        self.queued_hint.store(0, Ordering::Relaxed);

        let db_path = cfg.db_path.clone();
        // Capture `self` (a genuine `&'static` reference) into the writer thread rather than
        // re-deriving it via the `connection_tracker()` singleton accessor inside the thread -
        // otherwise a non-singleton instance (as tests construct) would have its writer thread
        // silently update the *global* singleton's `queued_hint` instead of its own, the same
        // class of bug `dns_cache.rs` had to be redesigned around.
        let handle = match open_connection(&db_path) {
            Ok(conn) => {
                let handle = std::thread::Builder::new()
                    .name("connection-tracker-writer".into())
                    .spawn(move || writer_loop(self, conn, rx, shutdown_rx, cfg))
                    .expect("failed to spawn connection-tracker-writer thread");
                tracing::info!("Connection tracking enabled, writing to {db_path}");
                handle
            }
            Err(e) => {
                tracing::error!("Failed to start connection tracking, feature stays disabled: {e}");
                return;
            }
        };
        *self.inner.lock().unwrap() = Some(Inner { sender: tx, shutdown: shutdown_tx, handle });
    }

    fn stop_internal(&self) {
        let inner = self.inner.lock().unwrap().take();
        if let Some(inner) = inner {
            let _ = inner.shutdown.send(());
            drop(inner.sender);
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
         CREATE TABLE IF NOT EXISTS connections (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             uuid TEXT NOT NULL,
             name TEXT NOT NULL,
             ip TEXT NOT NULL,
             host TEXT NOT NULL,
             protocol_version INTEGER NOT NULL,
             login_attempts INTEGER NOT NULL,
             packets_sent INTEGER NOT NULL,
             packets_received INTEGER NOT NULL,
             bytes_sent INTEGER NOT NULL,
             bytes_received INTEGER NOT NULL,
             compression_threshold INTEGER NOT NULL,
             encrypted INTEGER NOT NULL,
             connected_at INTEGER NOT NULL,
             disconnected_at INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_connections_uuid ON connections(uuid);
         CREATE INDEX IF NOT EXISTS idx_connections_connected_at ON connections(connected_at);",
    )?;
    Ok(conn)
}

fn writer_loop(tracker: &'static ConnectionTracker, mut conn: Connection, rx: crossbeam_channel::Receiver<ConnectionRecord>, shutdown_rx: crossbeam_channel::Receiver<()>, cfg: ConnectionTrackingConfig) {
    let flush_interval = Duration::from_millis(cfg.flush_interval_millis.max(1) as u64);
    let prune_interval = Duration::from_millis(cfg.prune_interval_millis.max(1) as u64);
    let mut last_prune = Instant::now();
    let mut batch = Vec::with_capacity(cfg.batch_size.max(1) as usize);

    loop {
        crossbeam_channel::select! {
            recv(rx) -> msg => {
                match msg {
                    Ok(rec) => {
                        batch.push(rec);
                        while batch.len() < cfg.batch_size.max(1) as usize {
                            match rx.try_recv() {
                                Ok(rec) => batch.push(rec),
                                Err(_) => break,
                            }
                        }
                        let n = batch.len() as i64;
                        if let Err(e) = write_batch(&mut conn, &batch) {
                            tracing::warn!("Connection tracking write failed, {} record(s) lost: {e}", batch.len());
                        }
                        tracker.queued_hint.fetch_sub(n, Ordering::Relaxed);
                        batch.clear();
                    }
                    Err(_) => break, // sender dropped - shutting down
                }
            }
            recv(shutdown_rx) -> _ => break,
            default(flush_interval) => {}
        }

        if last_prune.elapsed() >= prune_interval {
            if let Err(e) = prune(&conn, &cfg) {
                tracing::warn!("Connection tracking prune failed: {e}");
            }
            last_prune = Instant::now();
        }
    }

    // Shutting down - flush whatever is still queued rather than silently dropping it.
    let mut remaining = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        remaining.push(rec);
    }
    if !remaining.is_empty() {
        if let Err(e) = write_batch(&mut conn, &remaining) {
            tracing::warn!("Connection tracking final flush failed, {} record(s) lost: {e}", remaining.len());
        }
    }
}

fn write_batch(conn: &mut Connection, batch: &[ConnectionRecord]) -> rusqlite::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO connections
             (uuid, name, ip, host, protocol_version, login_attempts, packets_sent, packets_received,
              bytes_sent, bytes_received, compression_threshold, encrypted, connected_at, disconnected_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        )?;
        for rec in batch {
            stmt.execute(rusqlite::params![
                rec.uuid.to_string(),
                rec.name,
                rec.ip,
                rec.host,
                rec.protocol_version,
                rec.login_attempts,
                rec.packets_sent,
                rec.packets_received,
                rec.bytes_sent,
                rec.bytes_received,
                rec.compression_threshold,
                rec.encrypted as i32,
                rec.connected_at,
                rec.disconnected_at,
            ])?;
        }
    }
    tx.commit()
}

fn prune(conn: &Connection, cfg: &ConnectionTrackingConfig) -> rusqlite::Result<()> {
    let cutoff = now_millis() - cfg.retention_days as i64 * 86_400_000;
    conn.execute("DELETE FROM connections WHERE connected_at < ?1", rusqlite::params![cutoff])?;
    conn.execute(
        "DELETE FROM connections WHERE id IN (SELECT id FROM connections ORDER BY id DESC LIMIT -1 OFFSET ?1)",
        rusqlite::params![cfg.max_records],
    )?;
    conn.execute_batch("PRAGMA incremental_vacuum(2000)")
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(name: &str) -> ConnectionRecord {
        ConnectionRecord {
            uuid: Uuid::new_v4(),
            name: name.to_string(),
            ip: "127.0.0.1".to_string(),
            host: "test.example.com".to_string(),
            protocol_version: 767,
            login_attempts: 1,
            packets_sent: 10,
            packets_received: 20,
            bytes_sent: 100,
            bytes_received: 200,
            compression_threshold: -1,
            encrypted: false,
            // A genuinely "now" timestamp, not a small constant - retention pruning compares
            // against the real wall-clock `now_millis()`, so a fixed epoch-relative value like
            // 1000 would already look ancient regardless of the configured retention window.
            connected_at: now_millis(),
            disconnected_at: now_millis(),
        }
    }

    #[test]
    fn disabled_by_default_record_is_a_noop() {
        let tracker = ConnectionTracker::default();
        assert!(!tracker.is_enabled());
        tracker.record(sample_record("Steve")); // must not panic
        assert_eq!(tracker.dropped_records_total(), 0);
    }

    #[test]
    fn schema_creation_and_batch_write_round_trip() {
        let dir = std::env::temp_dir().join(format!("mcgate-tracker-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("connections.db");

        let mut conn = open_connection(db_path.to_str().unwrap()).unwrap();
        let batch = vec![sample_record("Steve"), sample_record("Alex")];
        write_batch(&mut conn, &batch).unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM connections", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 2);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn prune_deletes_rows_past_retention_and_row_cap() {
        let dir = std::env::temp_dir().join(format!("mcgate-tracker-prune-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("connections.db");

        let mut conn = open_connection(db_path.to_str().unwrap()).unwrap();
        let mut old = sample_record("Old");
        old.connected_at = 0; // far in the past - past any real retention window
        write_batch(&mut conn, &[old]).unwrap();
        write_batch(&mut conn, &[sample_record("Recent")]).unwrap();

        let cfg = ConnectionTrackingConfig { retention_days: 1, max_records: 200_000, ..ConnectionTrackingConfig::default() };
        prune(&conn, &cfg).unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM connections", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1, "the far-past record should have been pruned by retention");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
