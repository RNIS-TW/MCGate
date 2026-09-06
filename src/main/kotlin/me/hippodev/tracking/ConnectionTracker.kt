package me.hippodev.tracking

import me.hippodev.config.ConnectionTrackingConfig
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** One completed player session, captured once (at disconnect) rather than per-packet - see
 *  [ConnectionTracker] for why. */
data class ConnectionRecord(
    val uuid: UUID,
    val name: String,
    val ip: String,
    val host: String,
    val protocolVersion: Int,
    /** Number of backend dial attempts this session made (failover across a route's backend
     *  list counts as one attempt each) before either connecting or giving up. */
    val loginAttempts: Int,
    val packetsSent: Long,
    val packetsReceived: Long,
    val bytesSent: Long,
    val bytesReceived: Long,
    val compressionThreshold: Int,
    val encrypted: Boolean,
    val connectedAt: Long,
    val disconnectedAt: Long
)

/**
 * Optional, off-by-default persistence of [ConnectionRecord]s to a local SQLite database - a
 * single compact binary file, not a plain-text log that grows forever.
 *
 * Three things keep this from becoming a memory leak or a source of backpressure on the Netty
 * event-loop threads that call [record], even with 500+ concurrent players disconnecting at once:
 *
 *  1. [record] only ever offers to a fixed-capacity in-memory queue and returns immediately. A
 *     full queue (the writer thread falling behind) drops the record - counted and logged - rather
 *     than blocking the caller or growing without bound.
 *  2. A single dedicated writer thread drains the queue in batches, each committed as one SQLite
 *     transaction, so a burst of disconnects costs a handful of commits, not one fsync per player.
 *  3. A periodic janitor deletes rows past the configured retention window and caps total row
 *     count, then runs an incremental vacuum to hand freed pages back to the OS - so the .db
 *     file's contents (and eventually its on-disk size) stay bounded no matter how long the
 *     process runs.
 */
object ConnectionTracker {
    private val log = LoggerFactory.getLogger(ConnectionTracker::class.java)

    @Volatile private var config: ConnectionTrackingConfig = ConnectionTrackingConfig()
    @Volatile private var queue: LinkedBlockingQueue<ConnectionRecord>? = null
    @Volatile private var connection: Connection? = null
    private var writerThread: Thread? = null
    private var janitor: ScheduledExecutorService? = null
    private val droppedSinceLastNotice = AtomicLong(0)

    /** Cheap, lock-free check every caller (hot path: every player channel) uses to skip all
     *  tracking overhead entirely while the feature is off - the default. */
    val enabled: Boolean get() = queue != null

    /** Records dropped since tracking last (re)started, because the queue was full - see
     *  [record]. Exposed for observability (e.g. the `/metrics` endpoint) so sustained drops
     *  under load are visible externally, not just as periodic log warnings. */
    val droppedRecordsTotal: Long get() = droppedSinceLastNotice.get()

    /** Records currently queued, waiting on the writer thread. */
    val queuedRecords: Int get() = queue?.size ?: 0

    /** Applies (or re-applies) [newConfig]: starts/stops/restarts the writer thread, DB
     *  connection and janitor as needed. Safe to call repeatedly (initial load and every config
     *  reload) - a no-op unless something actually changed, so toggling the feature on/off, or
     *  changing dbPath/retention/etc, takes effect live without a process restart. */
    @Synchronized
    fun applyConfig(newConfig: ConnectionTrackingConfig) {
        if (newConfig == config) return
        stopInternal()
        config = newConfig
        if (newConfig.enabled) startInternal(newConfig)
    }

    /** Queues [rec] for durable persistence. No-op (and effectively free) when tracking is
     *  disabled. Never blocks - see the class doc. */
    fun record(rec: ConnectionRecord) {
        val q = queue ?: return
        if (!q.offer(rec)) {
            val dropped = droppedSinceLastNotice.incrementAndGet()
            // Only warn occasionally under sustained backpressure - logging every single drop
            // would itself become a source of load on the very system that's already falling
            // behind.
            if (dropped == 1L || dropped % 500 == 0L) {
                log.warn(
                    "Connection tracking queue full, dropping records ({} dropped since last notice)",
                    dropped
                )
            }
        }
    }

    @Synchronized
    fun shutdown() {
        stopInternal()
    }

    private fun startInternal(cfg: ConnectionTrackingConfig) {
        try {
            val file = File(cfg.dbPath)
            file.absoluteFile.parentFile?.mkdirs()
            val conn = DriverManager.getConnection("jdbc:sqlite:${file.path}")
            conn.createStatement().use { st ->
                // WAL + NORMAL sync: durable enough for connection-history data (survives a
                // process crash losing at most the last unflushed batch) without paying a full
                // fsync per commit - important once batches are landing every couple of seconds
                // under real traffic.
                st.execute("PRAGMA journal_mode=WAL")
                st.execute("PRAGMA synchronous=NORMAL")
                st.execute("PRAGMA auto_vacuum=INCREMENTAL")
                st.execute(
                    """
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
                    )
                    """.trimIndent()
                )
                st.execute("CREATE INDEX IF NOT EXISTS idx_connections_uuid ON connections(uuid)")
                st.execute("CREATE INDEX IF NOT EXISTS idx_connections_connected_at ON connections(connected_at)")
            }
            connection = conn
            droppedSinceLastNotice.set(0)

            val q = LinkedBlockingQueue<ConnectionRecord>(cfg.queueCapacity)
            queue = q

            val writer = Thread({ writerLoop(q, cfg) }, "connection-tracker-writer")
            writer.isDaemon = true
            writer.start()
            writerThread = writer

            val scheduler = Executors.newSingleThreadScheduledExecutor { r ->
                Thread(r, "connection-tracker-janitor").apply { isDaemon = true }
            }
            scheduler.scheduleWithFixedDelay(
                { runCatching { prune(cfg) }.onFailure { log.warn("Connection tracking prune failed", it) } },
                cfg.pruneIntervalMillis, cfg.pruneIntervalMillis, TimeUnit.MILLISECONDS
            )
            janitor = scheduler

            log.info("Connection tracking enabled, writing to {}", file.path)
        } catch (e: Exception) {
            log.error("Failed to start connection tracking, feature stays disabled", e)
            queue = null
            connection?.let { runCatching { it.close() } }
            connection = null
        }
    }

    private fun stopInternal() {
        janitor?.shutdownNow()
        janitor = null
        val writer = writerThread
        writerThread = null
        // Stop accepting new records immediately - record() becomes a no-op the instant this
        // runs, same as if tracking were never enabled. The writer thread below still has the
        // queue via its own captured reference, so it can drain and persist whatever was already
        // waiting before this call started.
        queue = null
        if (writer != null) {
            // Unblocks the writer thread's poll() wait so it notices the shutdown promptly
            // instead of lingering until the next flushInterval tick - writerLoop treats the
            // resulting InterruptedException as "drain what's left and exit", not "discard it".
            writer.interrupt()
            // Bounded wait: a graceful shutdown (SIGTERM/console `stop`) must still terminate in
            // finite time even if, say, the disk is wedged and the final flush's DB write hangs.
            runCatching { writer.join(5000) }
        }
        connection?.let { runCatching { it.close() } }
        connection = null
    }

    private fun writerLoop(q: LinkedBlockingQueue<ConnectionRecord>, cfg: ConnectionTrackingConfig) {
        val batch = ArrayList<ConnectionRecord>(cfg.batchSize)
        // A plain `while (!isInterrupted())` would miss records: interrupt() only actually
        // throws if this thread happens to be blocked inside poll() at that exact instant - if
        // it's mid-writeBatch (the common case once there's any real load), the flag just gets
        // set and the *next* loop condition check exits silently, skipping the drain-and-flush
        // below entirely. Looping unconditionally and always running the final flush after the
        // loop - regardless of which of the three ways below it was left - closes that gap.
        while (true) {
            try {
                val first = q.poll(cfg.flushIntervalMillis, TimeUnit.MILLISECONDS)
                if (first == null) {
                    if (Thread.currentThread().isInterrupted) break
                    continue
                }
                batch.add(first)
                q.drainTo(batch, cfg.batchSize - 1)
                writeBatch(batch)
                batch.clear()
                if (Thread.currentThread().isInterrupted) break
            } catch (e: InterruptedException) {
                break
            } catch (e: Exception) {
                log.warn("Connection tracking write failed, {} record(s) lost", batch.size, e)
                batch.clear()
            }
        }

        // Shutting down (see stopInternal) - flush whatever is still queued rather than
        // silently dropping it. Clear the interrupt flag first: a still-set flag would make the
        // JDBC calls inside writeBatch fail immediately.
        Thread.interrupted()
        val remaining = ArrayList<ConnectionRecord>()
        q.drainTo(remaining)
        if (remaining.isNotEmpty()) {
            try {
                writeBatch(remaining)
            } catch (ex: Exception) {
                log.warn("Connection tracking final flush failed, {} record(s) lost", remaining.size, ex)
            }
        }
    }

    private fun writeBatch(batch: List<ConnectionRecord>) {
        if (batch.isEmpty()) return
        val conn = connection ?: return
        conn.autoCommit = false
        try {
            conn.prepareStatement(
                """INSERT INTO connections
                   (uuid, name, ip, host, protocol_version, login_attempts, packets_sent, packets_received,
                    bytes_sent, bytes_received, compression_threshold, encrypted, connected_at, disconnected_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            ).use { ps ->
                for (rec in batch) {
                    ps.setString(1, rec.uuid.toString())
                    ps.setString(2, rec.name)
                    ps.setString(3, rec.ip)
                    ps.setString(4, rec.host)
                    ps.setInt(5, rec.protocolVersion)
                    ps.setInt(6, rec.loginAttempts)
                    ps.setLong(7, rec.packetsSent)
                    ps.setLong(8, rec.packetsReceived)
                    ps.setLong(9, rec.bytesSent)
                    ps.setLong(10, rec.bytesReceived)
                    ps.setInt(11, rec.compressionThreshold)
                    ps.setInt(12, if (rec.encrypted) 1 else 0)
                    ps.setLong(13, rec.connectedAt)
                    ps.setLong(14, rec.disconnectedAt)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        } catch (e: Exception) {
            runCatching { conn.rollback() }
            throw e
        } finally {
            conn.autoCommit = true
        }
    }

    private fun prune(cfg: ConnectionTrackingConfig) {
        val conn = connection ?: return
        val cutoff = System.currentTimeMillis() - cfg.retentionDays * 86_400_000L
        conn.prepareStatement("DELETE FROM connections WHERE connected_at < ?").use { ps ->
            ps.setLong(1, cutoff)
            ps.executeUpdate()
        }
        // Row-count cap: independent of age, so sustained high traffic that would outrun the
        // retention window on its own still can't grow the table without bound.
        conn.prepareStatement(
            "DELETE FROM connections WHERE id IN (SELECT id FROM connections ORDER BY id DESC LIMIT -1 OFFSET ?)"
        ).use { ps ->
            ps.setInt(1, cfg.maxRecords)
            ps.executeUpdate()
        }
        conn.createStatement().use { st -> st.execute("PRAGMA incremental_vacuum(2000)") }
    }
}
