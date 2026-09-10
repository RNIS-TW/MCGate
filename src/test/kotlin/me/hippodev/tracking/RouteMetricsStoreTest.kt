package me.hippodev.tracking

import me.hippodev.config.ReconnectConfig
import me.hippodev.config.Route
import me.hippodev.config.RouteMetricUsageConfig
import me.hippodev.config.RouteMetricsConfig
import me.hippodev.config.Strategy
import me.hippodev.protocol.HostPattern
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.yaml.snakeyaml.Yaml
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** Exercises [RouteMetricsStore]: that byte accounting accumulates correctly, persists to its JSON
 *  file off the calling thread, survives a "restart" (re-applyConfig loads the file back), enforces
 *  cumulative limits, drops counters for routes that go away (no unbounded growth), and shuts its
 *  background thread down cleanly. */
class RouteMetricsStoreTest {

    @AfterEach
    fun tearDown() {
        RouteMetricsStore.shutdown()
    }

    private fun tempFile(name: String = "metrics.json"): String =
        Files.createTempDirectory("mcgate-route-metrics").resolve(name).toString()

    private fun route(
        host: String,
        file: String?,
        upload: RouteMetricUsageConfig = RouteMetricUsageConfig(enabled = true),
        download: RouteMetricUsageConfig = RouteMetricUsageConfig(enabled = true),
        resetIntervalMillis: Long = 0
    ) = Route(
        hostPatterns = listOf(HostPattern(host)),
        backendTemplates = listOf("127.0.0.1:25566"),
        strategy = Strategy.SEQUENTIAL,
        cachePingTTLMillis = 0,
        fallback = null,
        modifyVirtualHost = false,
        proxyProtocol = false,
        priority = 0,
        reconnect = ReconnectConfig(),
        kickMessage = "Offline",
        metrics = RouteMetricsConfig(
            file = file, upload = upload, download = download, resetIntervalMillis = resetIntervalMillis
        )
    )

    private fun readJson(path: String): Map<String, Any> {
        @Suppress("UNCHECKED_CAST")
        return Yaml().load<Map<String, Any>>(java.io.File(path).readText())
    }

    private fun waitFor(timeoutMillis: Long = 5000, cond: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return true
            Thread.sleep(25)
        }
        return cond()
    }

    @Test
    fun `no routes - store stays disabled and creates nothing`() {
        RouteMetricsStore.applyConfig(emptyList())
        assertFalse(RouteMetricsStore.enabled)
        assertNull(RouteMetricsStore.handle(route("a.example.com", null)))
    }

    @Test
    fun `an inert metrics block (no direction enabled) produces no counter`() {
        val r = route(
            "a.example.com", tempFile(),
            upload = RouteMetricUsageConfig(enabled = false),
            download = RouteMetricUsageConfig(enabled = false)
        )
        RouteMetricsStore.applyConfig(listOf(r))
        assertFalse(RouteMetricsStore.enabled)
        assertNull(RouteMetricsStore.handle(r))
    }

    @Test
    fun `counts accumulate and persist to the json file`() {
        val file = tempFile()
        val r = route("survival.example.com", file)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 100)
        assertTrue(RouteMetricsStore.enabled)

        val c = RouteMetricsStore.handle(r)!!
        c.addUpload(1000)
        c.addUpload(500)
        c.addDownload(9000)
        assertEquals(1500, c.uploadBytes.get())
        assertEquals(9000, c.downloadBytes.get())

        assertTrue(waitFor { java.io.File(file).exists() && readJson(file)["uploadBytes"] == 1500 })
        val json = readJson(file)
        assertEquals(1500, (json["uploadBytes"] as Number).toInt())
        assertEquals(9000, (json["downloadBytes"] as Number).toInt())
        // No temp file left behind by the atomic rename.
        assertFalse(java.io.File("$file.tmp").exists())
    }

    @Test
    fun `disabled directions are not counted`() {
        val r = route(
            "a.example.com", tempFile(),
            upload = RouteMetricUsageConfig(enabled = true),
            download = RouteMetricUsageConfig(enabled = false)
        )
        RouteMetricsStore.applyConfig(listOf(r))
        val c = RouteMetricsStore.handle(r)!!
        c.addUpload(100)
        c.addDownload(100)
        assertEquals(100, c.uploadBytes.get())
        assertEquals(0, c.downloadBytes.get())
    }

    @Test
    fun `persisted totals are loaded back on a restart`() {
        val file = tempFile()
        val r = route("survival.example.com", file)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 100)
        RouteMetricsStore.handle(r)!!.addUpload(4096)
        RouteMetricsStore.handle(r)!!.addDownload(8192)
        RouteMetricsStore.shutdown() // flushes on the way out

        // Fresh "process": re-apply the same config, counter should resume from the file.
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 100)
        val c = RouteMetricsStore.handle(r)!!
        assertEquals(4096, c.uploadBytes.get())
        assertEquals(8192, c.downloadBytes.get())
        c.addUpload(4)
        assertEquals(4100, c.uploadBytes.get())
    }

    @Test
    fun `hot reload keeps the same counter accumulating and updates limits in place`() {
        val file = tempFile()
        RouteMetricsStore.applyConfig(listOf(route("a.example.com", file)))
        val c1 = RouteMetricsStore.handle(route("a.example.com", file))!!
        c1.addUpload(700)

        // Reload with a changed limit - same file key, so the same counter object survives.
        RouteMetricsStore.applyConfig(
            listOf(route("a.example.com", file, upload = RouteMetricUsageConfig(enabled = true, limit = 1000)))
        )
        val c2 = RouteMetricsStore.handle(route("a.example.com", file))!!
        assertTrue(c1 === c2, "counter identity must survive a reload")
        assertEquals(700, c2.uploadBytes.get())
        assertEquals(1000, c2.uploadLimit)
        assertFalse(c2.exceeded())
        c2.addUpload(400)
        assertTrue(c2.uploadExceeded())
    }

    @Test
    fun `limit detection - exceeded flips once cumulative usage reaches the cap`() {
        val r = route(
            "a.example.com", null,
            download = RouteMetricUsageConfig(enabled = true, limit = 5000)
        )
        RouteMetricsStore.applyConfig(listOf(r))
        val c = RouteMetricsStore.handle(r)!!
        c.addDownload(4999)
        assertFalse(c.exceeded())
        c.addDownload(1)
        assertTrue(c.exceeded())
        assertTrue(c.downloadExceeded())
        assertFalse(c.uploadExceeded())
    }

    @Test
    fun `counters for routes removed on reload are flushed then dropped`() {
        val fileA = tempFile("a.json")
        val fileB = tempFile("b.json")
        RouteMetricsStore.applyConfig(listOf(route("a.example.com", fileA), route("b.example.com", fileB)))
        RouteMetricsStore.handle(route("a.example.com", fileA))!!.addUpload(1234)
        assertEquals(2, RouteMetricsStore.counters().size)

        // Reload without route A.
        RouteMetricsStore.applyConfig(listOf(route("b.example.com", fileB)))
        assertEquals(1, RouteMetricsStore.counters().size, "dropped route's counter must not linger")
        assertNull(RouteMetricsStore.handle(route("a.example.com", fileA)))
        // A's final value was still flushed before it was dropped.
        assertEquals(1234, (readJson(fileA)["uploadBytes"] as Number).toInt())
    }

    @Test
    fun `addUpload is non-blocking under heavy concurrency and totals are exact`() {
        val r = route("a.example.com", tempFile())
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 50)
        val c = RouteMetricsStore.handle(r)!!

        val threads = 32
        val perThread = 50_000
        val pool = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        val maxCallNanos = AtomicLong(0)
        repeat(threads) {
            pool.submit {
                start.await()
                repeat(perThread) {
                    val t0 = System.nanoTime()
                    c.addUpload(7)
                    val e = System.nanoTime() - t0
                    maxCallNanos.updateAndGet { cur -> maxOf(cur, e) }
                }
                done.countDown()
            }
        }
        start.countDown()
        assertTrue(done.await(30, TimeUnit.SECONDS))
        pool.shutdown()

        assertEquals(threads.toLong() * perThread * 7, c.uploadBytes.get())
        // A single addAndGet + a volatile set: even a bad outlier (GC, JIT, thread scheduling)
        // stays well under a second. Real blocking here (a lock held across the flush's disk I/O)
        // would be orders of magnitude worse. Same bar the ConnectionTracker stress test uses.
        assertTrue(maxCallNanos.get() < 1_000_000_000, "addUpload took ${maxCallNanos.get()}ns - must never block")
    }

    @Test
    fun `shutdown flushes final state and terminates the background thread`() {
        val file = tempFile()
        val r = route("a.example.com", file)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 60_000) // long, so only shutdown flushes
        RouteMetricsStore.handle(r)!!.addUpload(555)

        RouteMetricsStore.shutdown()

        assertEquals(555, (readJson(file)["uploadBytes"] as Number).toInt(), "shutdown must flush the final value")
        assertFalse(RouteMetricsStore.enabled)

        val gone = waitFor { Thread.getAllStackTraces().keys.none { it.name == "route-metrics" } }
        assertTrue(gone, "route-metrics thread still alive after shutdown")
    }

    @Test
    fun `reset zeroes a counter and persists the reset so a restart does not reload the old total`() {
        val file = tempFile()
        val r = route("survival.example.com", file)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 60_000)
        val c = RouteMetricsStore.handle(r)!!
        c.addUpload(5000)
        c.addDownload(9000)

        val returned = RouteMetricsStore.reset(r)
        assertTrue(returned === c)
        assertEquals(0, c.uploadBytes.get())
        assertEquals(0, c.downloadBytes.get())
        // Persisted immediately (not waiting for the 60s tick).
        assertEquals(0, (readJson(file)["uploadBytes"] as Number).toInt())
        assertEquals(0, (readJson(file)["downloadBytes"] as Number).toInt())

        // Simulate a restart - the file must load back as zero.
        RouteMetricsStore.shutdown()
        RouteMetricsStore.applyConfig(listOf(r))
        assertEquals(0, RouteMetricsStore.handle(r)!!.uploadBytes.get())
    }

    @Test
    fun `rolling auto-reset zeroes the counter on schedule and advances the deadline`() {
        val file = tempFile()
        val r = route("survival.example.com", file, resetIntervalMillis = 300)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 50)
        val c = RouteMetricsStore.handle(r)!!
        c.addUpload(12_345)
        val firstDeadline = c.resetAt
        assertTrue(firstDeadline > System.currentTimeMillis())

        // Within a few ticks past the 300ms window the counter must be back to zero...
        assertTrue(waitFor { c.uploadBytes.get() == 0L }, "counter should auto-reset after its interval")
        // ...and the deadline moved forward, not left in the past.
        assertTrue(c.resetAt > firstDeadline)
        assertTrue(c.resetAt > System.currentTimeMillis())
        // The zeroed value is persisted, and so is the new deadline.
        assertTrue(waitFor { (readJson(file)["uploadBytes"] as Number).toInt() == 0 })
        assertEquals(c.resetAt, (readJson(file)["resetAt"] as Number).toLong())
    }

    @Test
    fun `auto-reset schedule is anchored in the file and survives a restart`() {
        val file = tempFile()
        val r = route("a.example.com", file, resetIntervalMillis = 60_000)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 40)
        val deadline = RouteMetricsStore.handle(r)!!.resetAt
        assertTrue(deadline > 0)
        assertTrue(waitFor { java.io.File(file).exists() && (readJson(file)["resetAt"] as Number).toLong() == deadline })

        RouteMetricsStore.shutdown()
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 40)
        // Same deadline, not pushed back by the restart.
        assertEquals(deadline, RouteMetricsStore.handle(r)!!.resetAt)
    }

    @Test
    fun `a deadline already in the past (downtime) triggers exactly one reset on the next tick`() {
        val file = tempFile()
        // Pre-seed with usage and a resetAt of 1ms-past-epoch: decades overdue, so a naive
        // "loop until future" advance would spin billions of times. The advance must be O(1).
        java.io.File(file).writeText("""{"uploadBytes":999,"downloadBytes":999,"resetAt":1}""")
        val intervalMs = 3_600_000L
        val r = route("a.example.com", file, resetIntervalMillis = intervalMs)
        RouteMetricsStore.applyConfig(listOf(r), flushIntervalMillis = 40)
        val c = RouteMetricsStore.handle(r)!!

        assertTrue(waitFor { c.uploadBytes.get() == 0L }, "overdue reset should fire once on startup")
        // Advanced to exactly one on-cadence boundary in the future - not left in the past, and
        // not spun far ahead. The boundary stays aligned to the original resetAt (1ms) cadence,
        // so it lands within one interval of now.
        assertTrue(c.resetAt > System.currentTimeMillis(), "deadline must move into the future")
        assertTrue(c.resetAt <= System.currentTimeMillis() + intervalMs, "deadline must be at most one interval ahead")
        assertEquals(1L, c.resetAt % intervalMs, "deadline stays aligned to the original cadence")
    }

    @Test
    fun `reset returns null for a route with no active counter`() {
        RouteMetricsStore.applyConfig(emptyList())
        assertNull(RouteMetricsStore.reset(route("a.example.com", null)))
    }

    @Test
    fun `resetAll zeroes every counter and reports the count`() {
        RouteMetricsStore.applyConfig(listOf(route("a.example.com", tempFile("a.json")), route("b.example.com", tempFile("b.json"))))
        RouteMetricsStore.counters().values.forEach { it.addUpload(100) }

        assertEquals(2, RouteMetricsStore.resetAll())
        assertTrue(RouteMetricsStore.counters().values.all { it.uploadBytes.get() == 0L })
    }

    @Test
    fun `limit kick message comes from the registered supplier and tracks reloads`() {
        var text = "first"
        RouteMetricsStore.setLimitKickMessageSupplier { text }
        assertEquals("first", RouteMetricsStore.limitKickMessage())
        text = "reloaded"
        assertEquals("reloaded", RouteMetricsStore.limitKickMessage())
    }

    @Test
    fun `a corrupt metrics file does not crash - counters start at zero`() {
        val file = tempFile()
        java.io.File(file).writeText("this is not json {{{")
        val r = route("a.example.com", file)
        RouteMetricsStore.applyConfig(listOf(r))
        val c = RouteMetricsStore.handle(r)!!
        assertEquals(0, c.uploadBytes.get())
        assertEquals(0, c.downloadBytes.get())
    }
}
