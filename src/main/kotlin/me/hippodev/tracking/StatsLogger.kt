package me.hippodev.tracking

import me.hippodev.config.StatsLoggingConfig
import me.hippodev.routing.MetricsSnapshot
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Optional, off-by-default local time-series log of MCGate's live stats - a periodic
 * [MetricsSnapshot] (players online, per-host player counts, per-backend active connections/
 * latency, connection-tracking queue/drop counters) written to a local SQLite database with a
 * timestamp, distinct from [ConnectionTracker]'s per-session history. Lets traffic trends be
 * queried locally (e.g. "players online per host, over the last week") without standing up a
 * separate Prometheus server against the `/metrics` API endpoint.
 *
 * Unlike [ConnectionTracker] (which can see a burst of hundreds of records at once, from many
 * Netty threads, and needs a bounded queue + batched writer to survive that), this only ever
 * writes one snapshot every [StatsLoggingConfig.intervalMillis] (60s by default) from a single
 * dedicated scheduled-executor thread - there's no concurrent-producer backpressure problem to
 * solve here, so a plain scheduled task doing a direct write is enough. It still can't ever touch
 * a Netty event-loop thread: gathering a snapshot ([me.hippodev.routing.collectMetrics]) and
 * writing it both happen entirely on this dedicated thread.
 */
object StatsLogger {
    private val log = LoggerFactory.getLogger(StatsLogger::class.java)

    @Volatile private var config: StatsLoggingConfig = StatsLoggingConfig()
    @Volatile private var connection: Connection? = null
    private var scheduler: ScheduledExecutorService? = null

    val enabled: Boolean get() = connection != null

    /** Applies (or re-applies) [newConfig], starting/stopping/restarting the scheduled snapshot
     *  task as needed - same idempotent pattern as [ConnectionTracker.applyConfig], safe to call
     *  on every config (re)load. [snapshotSupplier] is called fresh on every tick, not cached, so
     *  it always reflects whatever routes/runtime are current at that moment even across a config
     *  reload that swaps them out. */
    @Synchronized
    fun applyConfig(newConfig: StatsLoggingConfig, snapshotSupplier: () -> MetricsSnapshot) {
        if (newConfig == config) return
        stopInternal()
        config = newConfig
        if (newConfig.enabled) startInternal(newConfig, snapshotSupplier)
    }

    @Synchronized
    fun shutdown() {
        stopInternal()
    }

    private fun startInternal(cfg: StatsLoggingConfig, snapshotSupplier: () -> MetricsSnapshot) {
        try {
            val file = File(cfg.dbPath)
            file.absoluteFile.parentFile?.mkdirs()
            val conn = DriverManager.getConnection("jdbc:sqlite:${file.path}")
            conn.createStatement().use { st ->
                st.execute("PRAGMA journal_mode=WAL")
                st.execute("PRAGMA synchronous=NORMAL")
                st.execute("PRAGMA auto_vacuum=INCREMENTAL")
                st.execute("PRAGMA foreign_keys=ON")
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stats_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        players_online INTEGER NOT NULL,
                        uptime_seconds REAL NOT NULL,
                        tracking_enabled INTEGER NOT NULL,
                        tracking_queued_records INTEGER NOT NULL,
                        tracking_dropped_records_total INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                st.execute("CREATE INDEX IF NOT EXISTS idx_stats_snapshots_timestamp ON stats_snapshots(timestamp)")
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stats_host_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_id INTEGER NOT NULL REFERENCES stats_snapshots(id) ON DELETE CASCADE,
                        host TEXT NOT NULL,
                        players_online INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                st.execute("CREATE INDEX IF NOT EXISTS idx_stats_host_snapshots_snapshot_id ON stats_host_snapshots(snapshot_id)")
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stats_backend_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_id INTEGER NOT NULL REFERENCES stats_snapshots(id) ON DELETE CASCADE,
                        route_index INTEGER NOT NULL,
                        hosts TEXT NOT NULL,
                        backend TEXT NOT NULL,
                        active_connections INTEGER NOT NULL,
                        latency_ms INTEGER
                    )
                    """.trimIndent()
                )
                st.execute("CREATE INDEX IF NOT EXISTS idx_stats_backend_snapshots_snapshot_id ON stats_backend_snapshots(snapshot_id)")
            }
            connection = conn

            val exec = Executors.newSingleThreadScheduledExecutor { r ->
                Thread(r, "stats-logger").apply { isDaemon = true }
            }
            // scheduleWithFixedDelay, not AtFixedRate: if a single write ever takes longer than
            // intervalMillis (a slow disk), this waits for it to finish rather than piling up
            // back-to-back ticks queued on the same single thread. Pruning runs every tick too,
            // right after the write - both are cheap (one snapshot's worth of rows; two DELETEs
            // and an incremental vacuum), and snapshots are already infrequent by design, so
            // there's no need for a separate, more complex "prune roughly hourly" schedule.
            exec.scheduleWithFixedDelay(
                {
                    runCatching { writeSnapshot(snapshotSupplier()) }.onFailure { log.warn("Stats logging write failed", it) }
                    runCatching { prune(cfg) }.onFailure { log.warn("Stats logging prune failed", it) }
                },
                cfg.intervalMillis, cfg.intervalMillis, TimeUnit.MILLISECONDS
            )
            scheduler = exec

            log.info("Stats logging enabled, writing to {} every {}ms", file.path, cfg.intervalMillis)
        } catch (e: Exception) {
            log.error("Failed to start stats logging, feature stays disabled", e)
            connection?.let { runCatching { it.close() } }
            connection = null
        }
    }

    private fun stopInternal() {
        scheduler?.let { exec ->
            exec.shutdown()
            // Bounded wait, same reasoning as ConnectionTracker.stopInternal - a graceful
            // shutdown must still terminate in finite time even if a write is stuck.
            runCatching { exec.awaitTermination(5, TimeUnit.SECONDS) }
        }
        scheduler = null
        connection?.let { runCatching { it.close() } }
        connection = null
    }

    private fun writeSnapshot(s: MetricsSnapshot) {
        val conn = connection ?: return
        conn.autoCommit = false
        try {
            val snapshotId = conn.prepareStatement(
                """INSERT INTO stats_snapshots
                   (timestamp, players_online, uptime_seconds, tracking_enabled, tracking_queued_records, tracking_dropped_records_total)
                   VALUES (?,?,?,?,?,?)""",
                java.sql.Statement.RETURN_GENERATED_KEYS
            ).use { ps ->
                ps.setLong(1, s.timestamp)
                ps.setInt(2, s.playersOnline)
                ps.setDouble(3, s.uptimeSeconds)
                ps.setInt(4, if (s.trackingEnabled) 1 else 0)
                ps.setInt(5, s.trackingQueuedRecords)
                ps.setLong(6, s.trackingDroppedRecordsTotal)
                ps.executeUpdate()
                ps.generatedKeys.use { rs -> rs.next(); rs.getLong(1) }
            }

            if (s.onlineByHost.isNotEmpty()) {
                conn.prepareStatement("INSERT INTO stats_host_snapshots (snapshot_id, host, players_online) VALUES (?,?,?)").use { ps ->
                    for ((host, count) in s.onlineByHost) {
                        ps.setLong(1, snapshotId)
                        ps.setString(2, host)
                        ps.setInt(3, count)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
            }

            if (s.backends.isNotEmpty()) {
                conn.prepareStatement(
                    """INSERT INTO stats_backend_snapshots
                       (snapshot_id, route_index, hosts, backend, active_connections, latency_ms) VALUES (?,?,?,?,?,?)"""
                ).use { ps ->
                    for (b in s.backends) {
                        ps.setLong(1, snapshotId)
                        ps.setInt(2, b.routeIndex)
                        ps.setString(3, b.hosts.joinToString(","))
                        ps.setString(4, b.backend)
                        ps.setInt(5, b.active)
                        if (b.latencyMillis != null) ps.setLong(6, b.latencyMillis) else ps.setNull(6, java.sql.Types.INTEGER)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
            }

            conn.commit()
        } catch (e: Exception) {
            runCatching { conn.rollback() }
            throw e
        } finally {
            conn.autoCommit = true
        }
    }

    private fun prune(cfg: StatsLoggingConfig) {
        val conn = connection ?: return
        val cutoff = System.currentTimeMillis() - cfg.retentionDays * 86_400_000L
        conn.prepareStatement("DELETE FROM stats_snapshots WHERE timestamp < ?").use { ps ->
            ps.setLong(1, cutoff)
            ps.executeUpdate()
        }
        conn.prepareStatement(
            "DELETE FROM stats_snapshots WHERE id IN (SELECT id FROM stats_snapshots ORDER BY id DESC LIMIT -1 OFFSET ?)"
        ).use { ps ->
            ps.setInt(1, cfg.maxRecords)
            ps.executeUpdate()
        }
        // stats_host_snapshots/stats_backend_snapshots rows are cleaned up automatically by the
        // ON DELETE CASCADE foreign keys above (PRAGMA foreign_keys=ON, set at connection open).
        conn.createStatement().use { st -> st.execute("PRAGMA incremental_vacuum(2000)") }
    }
}
