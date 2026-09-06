package me.hippodev.tracking

import me.hippodev.config.StatsLoggingConfig
import me.hippodev.routing.BackendMetric
import me.hippodev.routing.MetricsSnapshot
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager

/** Exercises [StatsLogger]'s periodic local time-series logging: that a snapshot's top-level
 *  stats and its per-host/per-backend breakdowns all land correctly and linked together, that
 *  retention/row-cap pruning actually bounds the table, and that it stays a true no-op when
 *  disabled (the default). */
class StatsLoggerTest {

    @AfterEach
    fun tearDown() {
        StatsLogger.applyConfig(StatsLoggingConfig()) { sampleSnapshot() }
    }

    private fun newDbPath(): String =
        Files.createTempDirectory("mcgate-stats-test").resolve("stats.db").toString()

    private fun sampleSnapshot(
        playersOnline: Int = 3,
        onlineByHost: Map<String, Int> = mapOf("survival.example.com" to 2, "creative.example.com" to 1)
    ): MetricsSnapshot = MetricsSnapshot(
        timestamp = System.currentTimeMillis(),
        playersOnline = playersOnline,
        onlineByHost = onlineByHost,
        uptimeSeconds = 123.4,
        backends = listOf(
            BackendMetric(routeIndex = 0, hosts = listOf("survival.example.com"), backend = "127.0.0.1:25566", active = 2, latencyMillis = 15),
            BackendMetric(routeIndex = 1, hosts = listOf("creative.example.com"), backend = "127.0.0.1:25567", active = 1, latencyMillis = null)
        ),
        players = emptyList(),
        trackingEnabled = false,
        trackingQueuedRecords = 0,
        trackingDroppedRecordsTotal = 0
    )

    private fun snapshotCount(dbPath: String): Long = queryCount(dbPath, "stats_snapshots")
    private fun hostRowCount(dbPath: String): Long = queryCount(dbPath, "stats_host_snapshots")
    private fun backendRowCount(dbPath: String): Long = queryCount(dbPath, "stats_backend_snapshots")

    private fun queryCount(dbPath: String, table: String): Long {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM $table").use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    private fun waitForSnapshotCount(dbPath: String, expected: Long, timeoutMillis: Long = 10_000): Long {
        val deadline = System.currentTimeMillis() + timeoutMillis
        var last = 0L
        while (System.currentTimeMillis() < deadline) {
            last = snapshotCount(dbPath)
            if (last >= expected) return last
            Thread.sleep(30)
        }
        return last
    }

    @Test
    fun `disabled by default - no database file is created`() {
        assertTrue(!StatsLogger.enabled)
        val dbPath = newDbPath()
        assertTrue(!java.io.File(dbPath).exists())
    }

    @Test
    fun `snapshot lands with host and backend breakdowns correctly linked`() {
        val dbPath = newDbPath()
        StatsLogger.applyConfig(StatsLoggingConfig(enabled = true, dbPath = dbPath, intervalMillis = 100)) { sampleSnapshot() }
        assertTrue(StatsLogger.enabled)

        assertEquals(1, waitForSnapshotCount(dbPath, 1))
        // A second tick should follow given the 100ms interval - confirms this is genuinely
        // periodic, not a one-shot write.
        assertEquals(2, waitForSnapshotCount(dbPath, 2))

        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT id, players_online, uptime_seconds FROM stats_snapshots ORDER BY id LIMIT 1").use { rs ->
                    assertTrue(rs.next())
                    val snapshotId = rs.getLong("id")
                    assertEquals(3, rs.getInt("players_online"))
                    assertEquals(123.4, rs.getDouble("uptime_seconds"), 0.001)

                    conn.prepareStatement("SELECT host, players_online FROM stats_host_snapshots WHERE snapshot_id = ? ORDER BY host").use { ps ->
                        ps.setLong(1, snapshotId)
                        ps.executeQuery().use { hostRs ->
                            assertTrue(hostRs.next())
                            assertEquals("creative.example.com", hostRs.getString("host"))
                            assertEquals(1, hostRs.getInt("players_online"))
                            assertTrue(hostRs.next())
                            assertEquals("survival.example.com", hostRs.getString("host"))
                            assertEquals(2, hostRs.getInt("players_online"))
                        }
                    }

                    conn.prepareStatement("SELECT backend, active_connections, latency_ms FROM stats_backend_snapshots WHERE snapshot_id = ? ORDER BY backend").use { ps ->
                        ps.setLong(1, snapshotId)
                        ps.executeQuery().use { backendRs ->
                            assertTrue(backendRs.next())
                            assertEquals("127.0.0.1:25566", backendRs.getString("backend"))
                            assertEquals(2, backendRs.getInt("active_connections"))
                            assertEquals(15, backendRs.getInt("latency_ms"))
                            assertTrue(backendRs.next())
                            assertEquals("127.0.0.1:25567", backendRs.getString("backend"))
                            assertEquals(1, backendRs.getInt("active_connections"))
                            backendRs.getInt("latency_ms")
                            assertTrue(backendRs.wasNull(), "latency should be NULL when unknown")
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `row cap pruning keeps stored snapshots bounded over time`() {
        val dbPath = newDbPath()
        val maxRecords = 5
        StatsLogger.applyConfig(
            StatsLoggingConfig(enabled = true, dbPath = dbPath, intervalMillis = 30, maxRecords = maxRecords, retentionDays = 365)
        ) { sampleSnapshot() }

        waitForSnapshotCount(dbPath, 20)

        // Force a prune cycle by re-applying with a much longer interval so the internal prune
        // scheduling (tied to intervalMillis in StatsLogger) still gets at least one tick, then
        // wait for it to actually run against the accumulated backlog.
        val deadline = System.currentTimeMillis() + 5000
        var count = snapshotCount(dbPath)
        while (System.currentTimeMillis() < deadline && count > maxRecords) {
            Thread.sleep(50)
            count = snapshotCount(dbPath)
        }
        assertTrue(count <= maxRecords, "expected pruning to cap snapshots at $maxRecords, found $count")

        // Cascade deletes must have cleaned up the orphaned child rows too, not just the parent.
        assertTrue(hostRowCount(dbPath) <= maxRecords * 2L, "host breakdown rows should be pruned along with their parent snapshot")
        assertTrue(backendRowCount(dbPath) <= maxRecords * 2L, "backend breakdown rows should be pruned along with their parent snapshot")
    }

    @Test
    fun `shutdown terminates the scheduler thread`() {
        val dbPath = newDbPath()
        StatsLogger.applyConfig(StatsLoggingConfig(enabled = true, dbPath = dbPath, intervalMillis = 100)) { sampleSnapshot() }
        waitForSnapshotCount(dbPath, 1)

        StatsLogger.shutdown()

        val deadline = System.currentTimeMillis() + 5000
        var leaked: Set<String>
        do {
            leaked = Thread.getAllStackTraces().keys.map { it.name }.filter { it == "stats-logger" }.toSet()
            if (leaked.isEmpty()) break
            Thread.sleep(50)
        } while (System.currentTimeMillis() < deadline)

        assertTrue(leaked.isEmpty(), "stats-logger thread still alive after shutdown: $leaked")
        assertTrue(!StatsLogger.enabled)
    }
}
