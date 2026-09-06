package me.hippodev.tracking

import me.hippodev.config.ConnectionTrackingConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Exercises [ConnectionTracker] under conditions meant to mirror "500+ players connecting and
 *  disconnecting" - many concurrent producers racing to persist a record at once - and verifies
 *  the three properties the feature was built around: nothing blocks the caller, nothing grows
 *  without bound, and the data that does fit still lands durably in the SQLite file. */
class ConnectionTrackerStressTest {

    @AfterEach
    fun tearDown() {
        // Reset the singleton back to disabled between tests so state from one test can't leak
        // into the next (ConnectionTracker is process-wide, same as it is in the running gate).
        ConnectionTracker.applyConfig(ConnectionTrackingConfig())
    }

    private fun newDbPath(): String =
        Files.createTempDirectory("mcgate-tracking-test").resolve("connections.db").toString()

    private fun rowCount(dbPath: String): Long {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM connections").use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    private fun waitForRowCount(dbPath: String, expected: Long, timeoutMillis: Long = 15_000): Long {
        val deadline = System.currentTimeMillis() + timeoutMillis
        var last = 0L
        while (System.currentTimeMillis() < deadline) {
            last = rowCount(dbPath)
            if (last >= expected) return last
            Thread.sleep(50)
        }
        return last
    }

    private fun sampleRecord(uuid: UUID = UUID.randomUUID(), name: String = "player-$uuid"): ConnectionRecord {
        val now = System.currentTimeMillis()
        return ConnectionRecord(
            uuid = uuid,
            name = name,
            ip = "127.0.0.1",
            host = "survival.example.com",
            protocolVersion = 767,
            loginAttempts = 1,
            packetsSent = 42,
            packetsReceived = 84,
            bytesSent = 4096,
            bytesReceived = 8192,
            compressionThreshold = 256,
            encrypted = true,
            connectedAt = now - 60_000,
            disconnectedAt = now
        )
    }

    @Test
    fun `disabled by default - record is a no-op`() {
        assertTrue(!ConnectionTracker.enabled)
        // Must not throw even though nothing was ever started.
        ConnectionTracker.record(sampleRecord())
    }

    @Test
    fun `single record round-trips to the database`() {
        val dbPath = newDbPath()
        ConnectionTracker.applyConfig(ConnectionTrackingConfig(enabled = true, dbPath = dbPath, flushIntervalMillis = 100))
        assertTrue(ConnectionTracker.enabled)

        val rec = sampleRecord(name = "Notch")
        ConnectionTracker.record(rec)

        assertEquals(1, waitForRowCount(dbPath, 1))
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT uuid, name, packets_sent, bytes_received, encrypted FROM connections").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(rec.uuid.toString(), rs.getString("uuid"))
                    assertEquals("Notch", rs.getString("name"))
                    assertEquals(42L, rs.getLong("packets_sent"))
                    assertEquals(8192L, rs.getLong("bytes_received"))
                    assertEquals(1, rs.getInt("encrypted"))
                }
            }
        }
    }

    /** Simulates 500+ players disconnecting at effectively the same moment (the worst case for
     *  the bounded queue -> single writer thread -> batched-commit pipeline) from many concurrent
     *  threads, the way many Netty event-loop threads each flushing one player's session would
     *  look. Every record must still make it to disk since the queue capacity comfortably exceeds
     *  the player count here - this asserts the "won't clog under normal 500+ load" property. */
    @Test
    fun `500+ concurrent disconnects all persist without blocking or dropping`() {
        val dbPath = newDbPath()
        val playerCount = 750
        ConnectionTracker.applyConfig(
            ConnectionTrackingConfig(
                enabled = true, dbPath = dbPath, queueCapacity = 5000, batchSize = 200, flushIntervalMillis = 100
            )
        )

        val pool = Executors.newFixedThreadPool(64)
        val startLatch = CountDownLatch(1)
        val doneLatch = CountDownLatch(playerCount)
        val maxRecordCallMillis = java.util.concurrent.atomic.AtomicLong(0)

        repeat(playerCount) {
            pool.submit {
                startLatch.await()
                val t0 = System.nanoTime()
                ConnectionTracker.record(sampleRecord())
                val elapsedMillis = (System.nanoTime() - t0) / 1_000_000
                maxRecordCallMillis.updateAndGet { cur -> maxOf(cur, elapsedMillis) }
                doneLatch.countDown()
            }
        }
        startLatch.countDown()
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "producers did not finish in time")
        pool.shutdown()

        // record() must be essentially instant (queue offer, not a DB write) even under this much
        // concurrent contention - a slow call here would mean a Netty event-loop thread could be
        // stalled by this feature, which is exactly what the bounded-queue design avoids.
        assertTrue(maxRecordCallMillis.get() < 1000, "record() call took ${maxRecordCallMillis.get()}ms - should never block")

        assertEquals(playerCount.toLong(), waitForRowCount(dbPath, playerCount.toLong()))
    }

    /** Floods the queue with more records than its capacity in one burst (no draining time given)
     *  to prove the "won't clog" backpressure behavior: excess records are dropped rather than
     *  the caller blocking or the queue growing past its configured bound. */
    @Test
    fun `queue backpressure drops excess records instead of blocking`() {
        val dbPath = newDbPath()
        ConnectionTracker.applyConfig(
            ConnectionTrackingConfig(enabled = true, dbPath = dbPath, queueCapacity = 20, batchSize = 5, flushIntervalMillis = 5000)
        )

        val submitted = 2000
        val start = System.currentTimeMillis()
        repeat(submitted) { ConnectionTracker.record(sampleRecord()) }
        val elapsed = System.currentTimeMillis() - start

        // 2000 non-blocking offer() calls against a 20-slot queue must finish essentially
        // instantly - if this were anywhere near as slow as the submission count, record() would
        // be blocking, which is the exact failure mode this design avoids.
        assertTrue(elapsed < 2000, "submitting $submitted records took ${elapsed}ms - record() appears to be blocking")

        // Give the writer time to fully drain whatever did make it into the bounded queue before
        // the queue filled up. The writer can keep up well enough that the persisted count ends
        // up higher than the raw queue capacity (it keeps draining while the burst is still being
        // submitted) - the property this asserts isn't a specific count, it's that the queue's
        // bound actually shed load: most of the 2000 offered records never made it in at all,
        // proving offer() dropped rather than every single one landing durably.
        waitForRowCount(dbPath, 1, timeoutMillis = 3000)
        Thread.sleep(500)
        val count = rowCount(dbPath)
        assertTrue(count in 1L until submitted.toLong(), "expected some but not all $submitted records to persist (proving drops occurred), got $count")
    }

    /** Verifies the periodic janitor enforces [ConnectionTrackingConfig.maxRecords] so the table's
     *  row count - and therefore the .db file's contents - stays bounded even when far more
     *  sessions have been recorded than the configured cap, simulating long-running accumulation
     *  well past what a single burst of 500 players would produce. */
    @Test
    fun `row cap pruning keeps stored rows bounded over time`() {
        val dbPath = newDbPath()
        val maxRecords = 50
        ConnectionTracker.applyConfig(
            ConnectionTrackingConfig(
                enabled = true, dbPath = dbPath, maxRecords = maxRecords, queueCapacity = 5000,
                batchSize = 200, flushIntervalMillis = 100, pruneIntervalMillis = 300
            )
        )

        repeat(300) { ConnectionTracker.record(sampleRecord()) }
        waitForRowCount(dbPath, 300)

        // Wait past at least one janitor cycle for the row-cap delete to run.
        val deadline = System.currentTimeMillis() + 5000
        var count = rowCount(dbPath)
        while (System.currentTimeMillis() < deadline && count > maxRecords) {
            Thread.sleep(100)
            count = rowCount(dbPath)
        }
        assertTrue(count <= maxRecords, "expected pruning to cap rows at $maxRecords, found $count")
    }

    /** A graceful shutdown (SIGTERM / console `stop`, both of which call [ConnectionTracker.shutdown]
     *  via the shutdown hook in Main.kt) must not lose records still sitting in the queue - only an
     *  actual `kill -9` (no shutdown hooks run at all) can do that. batchSize=1 forces one commit per
     *  record, so with enough records queued near-instantly the writer thread is still working through
     *  a real backlog - not idle - the moment shutdown() is called; that backlog must still all land. */
    @Test
    fun `shutdown flushes records still queued, not just already-committed ones`() {
        val dbPath = newDbPath()
        val total = 3000
        ConnectionTracker.applyConfig(
            ConnectionTrackingConfig(enabled = true, dbPath = dbPath, batchSize = 1, queueCapacity = total)
        )

        repeat(total) { ConnectionTracker.record(sampleRecord()) }
        val committedBeforeShutdown = rowCount(dbPath)
        assertTrue(
            committedBeforeShutdown < total,
            "sanity check: writer should still be working through the backlog when shutdown() is called, " +
                "got $committedBeforeShutdown/$total already committed - test isn't exercising the flush path"
        )

        ConnectionTracker.shutdown()

        assertEquals(total.toLong(), rowCount(dbPath), "records still queued at shutdown time must still be persisted, not dropped")
    }

    @Test
    fun `shutdown terminates the writer and janitor threads`() {
        val dbPath = newDbPath()
        ConnectionTracker.applyConfig(ConnectionTrackingConfig(enabled = true, dbPath = dbPath))
        ConnectionTracker.record(sampleRecord())
        Thread.sleep(200)

        ConnectionTracker.shutdown()

        val deadline = System.currentTimeMillis() + 5000
        var leaked: Set<String>
        do {
            leaked = Thread.getAllStackTraces().keys
                .map { it.name }
                .filter { it == "connection-tracker-writer" || it == "connection-tracker-janitor" }
                .toSet()
            if (leaked.isEmpty()) break
            Thread.sleep(50)
        } while (System.currentTimeMillis() < deadline)

        assertTrue(leaked.isEmpty(), "connection tracker threads still alive after shutdown: $leaked")
        assertTrue(!ConnectionTracker.enabled)
    }
}
