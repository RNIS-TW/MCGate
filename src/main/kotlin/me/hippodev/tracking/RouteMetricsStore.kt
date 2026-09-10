package me.hippodev.tracking

import me.hippodev.config.Route
import me.hippodev.routing.PlayerSessions
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Optional per-route upload/download byte accounting (config.yml's per-route `metrics:` block).
 *
 * Design, mirroring [ConnectionTracker]/[StatsLogger] and the "no RAM leak, never touch a Netty
 * event loop" constraints:
 *
 *  1. **In memory it's just two [AtomicLong]s per route** ([RouteTrafficCounter]). The relay
 *     handlers already keep per-session byte atomics; this only adds one extra `addAndGet` on a
 *     process-wide counter on the same event-loop thread already handling that packet - a few
 *     nanoseconds, no allocation, no retained per-connection state. When a player disconnects
 *     nothing here holds a reference to them, so the heap footprint is O(routes), not O(players
 *     ever seen).
 *  2. **Persistence is a single background thread** ("route-metrics"), never a Netty thread. It
 *     flushes each counter to its JSON [file] atomically (write `*.tmp`, then atomic rename) every
 *     `flushIntervalMillis`, and only when that counter actually changed since the last flush.
 *  3. **Totals survive a restart**: a counter's `file` is read back on creation, so accounting is
 *     cumulative across process lifetimes.
 *  4. **Limits are enforced off the event loop too**: the same background tick kicks any player
 *     currently on a route whose cumulative usage has reached its configured `limit`; new logins
 *     are refused inline at dispatch time (that check is just an atomic read).
 *  5. **Optional rolling auto-reset** (`metrics.resetInterval`): the background tick zeroes a
 *     counter every configured period. The next-reset timestamp is persisted in the same JSON
 *     file, so the schedule is anchored to wall-clock time and survives restarts without drifting;
 *     no external cron needed.
 */
object RouteMetricsStore {
    private val log = LoggerFactory.getLogger(RouteMetricsStore::class.java)

    /** One route's (or one shared `file`'s) cumulative counters. Config fields are `@Volatile` and
     *  updated in place on a hot reload so the same atomics keep accumulating across reloads. */
    class RouteTrafficCounter internal constructor(val key: String) {
        val uploadBytes = AtomicLong(0)
        val downloadBytes = AtomicLong(0)

        @Volatile var file: String? = null
        @Volatile var uploadEnabled = false
        @Volatile var downloadEnabled = false
        @Volatile var uploadLimit = -1L
        @Volatile var downloadLimit = -1L
        /** Rolling auto-reset period in millis (0 = off). See [me.hippodev.config.RouteMetricsConfig]. */
        @Volatile var resetIntervalMillis = 0L
        /** Epoch millis of the next scheduled auto-reset (0 = none). Persisted to [file] so the
         *  schedule is anchored to wall-clock time, not process uptime. */
        @Volatile var resetAt = 0L

        internal val dirty = AtomicBoolean(false)

        fun addUpload(n: Long) {
            if (uploadEnabled && n > 0) { uploadBytes.addAndGet(n); dirty.set(true) }
        }

        fun addDownload(n: Long) {
            if (downloadEnabled && n > 0) { downloadBytes.addAndGet(n); dirty.set(true) }
        }

        fun uploadExceeded(): Boolean = uploadEnabled && uploadLimit >= 0 && uploadBytes.get() >= uploadLimit
        fun downloadExceeded(): Boolean = downloadEnabled && downloadLimit >= 0 && downloadBytes.get() >= downloadLimit
        fun exceeded(): Boolean = uploadExceeded() || downloadExceeded()
    }

    private val counters = ConcurrentHashMap<String, RouteTrafficCounter>()

    @Volatile private var routes: List<Route> = emptyList()
    @Volatile private var flushIntervalMillis: Long = 10_000
    /** Read fresh on every enforce tick / login refusal so a messages.yml hot reload takes effect
     *  without restarting the store. Defaults to a sane string until Main wires in the real one. */
    @Volatile private var limitKickMessageSupplier: () -> String =
        { "This server has reached its data-transfer limit. Please try again later." }
    private var scheduler: ScheduledExecutorService? = null

    /** Registers the source of the player-facing kick text for a metrics-limit disconnect
     *  (messages.yml's `metricsLimitKickMessage`). Called once at startup - the supplier itself
     *  re-reads the current messages, so it stays correct across hot reloads. */
    fun setLimitKickMessageSupplier(supplier: () -> String) {
        limitKickMessageSupplier = supplier
    }

    /** Current metrics-limit kick text - used by the dispatch-time login refusal in Main too. */
    fun limitKickMessage(): String = limitKickMessageSupplier()

    /** True while the background flush/enforce thread is running (i.e. at least one route counts
     *  traffic). Cheap lock-free check for callers that want to skip work entirely when off. */
    val enabled: Boolean get() = scheduler != null

    private fun keyFor(route: Route): String? {
        val m = route.metrics ?: return null
        if (!m.active) return null
        return m.file ?: ("host:" + route.hostPatterns.joinToString("|") { it.raw })
    }

    /** The live counter for [route], or null when the route has no active `metrics:` block. Safe to
     *  call from any thread (a Netty event loop included) - a single ConcurrentHashMap lookup. */
    fun handle(route: Route): RouteTrafficCounter? = keyFor(route)?.let { counters[it] }

    /** Current counters, keyed as in [keyFor] - for the API's `/metrics` surfaces. */
    fun counters(): Map<String, RouteTrafficCounter> = counters.toMap()

    /** Zeroes [route]'s upload/download counters and immediately persists the reset to its file
     *  (so a restart doesn't reload the pre-reset total). Returns false when the route has no
     *  active counter. Counters shared via a common `file:` are reset together - they're one
     *  object. */
    @Synchronized
    fun reset(route: Route): RouteTrafficCounter? {
        val counter = handle(route) ?: return null
        zero(counter)
        flushCounter(counter, force = true)
        log.info("Route metrics counter '{}' reset to zero", counter.key)
        return counter
    }

    /** Zeroes and persists every counter. Returns how many were reset. */
    @Synchronized
    fun resetAll(): Int {
        for (counter in counters.values) {
            zero(counter)
            flushCounter(counter, force = true)
        }
        if (counters.isNotEmpty()) log.info("Reset {} route metrics counter(s) to zero", counters.size)
        return counters.size
    }

    /** Zeroes a counter's bytes and re-anchors its rolling auto-reset window (if it has one) to
     *  start from now. Marks it dirty; the caller flushes. */
    private fun zero(counter: RouteTrafficCounter) {
        counter.uploadBytes.set(0)
        counter.downloadBytes.set(0)
        if (counter.resetIntervalMillis > 0) counter.resetAt = System.currentTimeMillis() + counter.resetIntervalMillis
        counter.dirty.set(true)
    }

    /** Applies (or re-applies) the route set: creates counters for newly-metriced routes (loading
     *  any persisted total from disk), updates limits/enabled flags in place for existing ones, and
     *  flushes-then-drops counters no longer referenced so nothing lingers. Idempotent - safe to
     *  call on initial load and every hot reload. */
    @Synchronized
    fun applyConfig(newRoutes: List<Route>, flushIntervalMillis: Long = 10_000) {
        this.routes = newRoutes
        this.flushIntervalMillis = flushIntervalMillis

        val desired = HashMap<String, Route>()
        for (route in newRoutes) {
            val key = keyFor(route) ?: continue
            desired.putIfAbsent(key, route)
        }

        val it = counters.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.key !in desired) {
                flushCounter(entry.value, force = true)
                it.remove()
            }
        }

        for ((key, route) in desired) {
            val m = route.metrics!!
            val counter = counters.computeIfAbsent(key) { k -> RouteTrafficCounter(k).also { loadPersisted(it, m.file) } }
            counter.file = m.file
            counter.uploadEnabled = m.upload.enabled
            counter.downloadEnabled = m.download.enabled
            counter.uploadLimit = m.upload.limit
            counter.downloadLimit = m.download.limit
            counter.resetIntervalMillis = m.resetIntervalMillis
            if (m.resetIntervalMillis > 0) {
                val now = System.currentTimeMillis()
                // Anchor the schedule only when there's no deadline at all (brand-new counter or
                // the feature just turned on) or the interval was shortened past the stored
                // deadline. A deadline already in the past (long downtime) is left alone - the
                // next background tick zeroes the counter and advances it, so a missed window
                // still triggers exactly one reset rather than being silently skipped.
                if (counter.resetAt == 0L || counter.resetAt > now + m.resetIntervalMillis) {
                    counter.resetAt = now + m.resetIntervalMillis
                    counter.dirty.set(true)
                }
            } else if (counter.resetAt != 0L) {
                counter.resetAt = 0L
                counter.dirty.set(true)
            }
        }

        if (counters.isEmpty()) stopScheduler() else startScheduler()
    }

    @Synchronized
    fun shutdown() {
        stopScheduler()
        flushAll(force = true)
        counters.clear()
        routes = emptyList()
    }

    private fun startScheduler() {
        if (scheduler != null) return
        val exec = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "route-metrics").apply { isDaemon = true }
        }
        exec.scheduleWithFixedDelay(
            {
                runCatching { applyScheduledResets() }.onFailure { log.warn("Route metrics scheduled reset failed", it) }
                runCatching { flushAll() }.onFailure { log.warn("Route metrics flush failed", it) }
                runCatching { enforceLimits() }.onFailure { log.warn("Route metrics limit enforcement failed", it) }
            },
            flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS
        )
        scheduler = exec
        log.info("Route metrics accounting enabled for {} route counter(s)", counters.size)
    }

    private fun stopScheduler() {
        scheduler?.let { exec ->
            exec.shutdown()
            // Bounded wait - a graceful shutdown must still terminate even if a flush write hangs.
            runCatching { exec.awaitTermination(5, TimeUnit.SECONDS) }
        }
        scheduler = null
    }

    /** Zeroes any counter whose rolling auto-reset deadline has passed and advances the deadline
     *  to the next future boundary (one step, even after long downtime). Runs on the background
     *  thread just before the flush, so the zeroed value is persisted the same tick. */
    private fun applyScheduledResets() {
        val now = System.currentTimeMillis()
        for (counter in counters.values) {
            val interval = counter.resetIntervalMillis
            if (interval <= 0 || counter.resetAt <= 0L || now < counter.resetAt) continue
            counter.uploadBytes.set(0)
            counter.downloadBytes.set(0)
            var next = counter.resetAt
            while (next <= now) next += interval
            counter.resetAt = next
            counter.dirty.set(true)
            log.info("Route metrics counter '{}' auto-reset; next reset at {}", counter.key, next)
        }
    }

    private fun flushAll(force: Boolean = false) {
        for (counter in counters.values) flushCounter(counter, force)
    }

    private fun flushCounter(counter: RouteTrafficCounter, force: Boolean = false) {
        val path = counter.file ?: return
        val wasDirty = counter.dirty.getAndSet(false)
        if (!force && !wasDirty) return
        try {
            val target = File(path)
            target.absoluteFile.parentFile?.mkdirs()
            val tmp = File(target.absolutePath + ".tmp")
            val json = buildString {
                append("{\"uploadBytes\":").append(counter.uploadBytes.get())
                append(",\"downloadBytes\":").append(counter.downloadBytes.get())
                append(",\"uploadLimit\":").append(counter.uploadLimit)
                append(",\"downloadLimit\":").append(counter.downloadLimit)
                append(",\"resetAt\":").append(counter.resetAt)
                append(",\"updatedAt\":").append(System.currentTimeMillis())
                append("}")
            }
            tmp.writeText(json)
            try {
                Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (e: Exception) {
                // Some filesystems don't support ATOMIC_MOVE across the same directory - fall back.
                Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        } catch (e: Exception) {
            log.warn("Failed to persist route metrics to {}: {}", path, e.toString())
            counter.dirty.set(true) // retry on the next tick
        }
    }

    private fun loadPersisted(counter: RouteTrafficCounter, file: String?) {
        if (file == null) return
        val f = File(file)
        if (!f.exists()) return
        try {
            val data = Yaml().load<Any?>(f.readText()) as? Map<*, *>
            if (data == null) {
                log.warn("Route metrics file {} is not a JSON object, starting its counters at 0", file)
                return
            }
            counter.uploadBytes.set((data["uploadBytes"] as? Number)?.toLong() ?: 0L)
            counter.downloadBytes.set((data["downloadBytes"] as? Number)?.toLong() ?: 0L)
            counter.resetAt = (data["resetAt"] as? Number)?.toLong() ?: 0L
            log.info(
                "Loaded route metrics from {} (upload={}B, download={}B)",
                file, counter.uploadBytes.get(), counter.downloadBytes.get()
            )
        } catch (e: Exception) {
            log.warn("Failed to read route metrics file {}, starting its counters at 0: {}", file, e.toString())
        }
    }

    /** Kicks any currently-connected player whose route's cumulative usage has reached its limit.
     *  Runs on the background thread; [PlayerSessions.kick] itself hands the write off to the
     *  player's own event loop. */
    private fun enforceLimits() {
        val overLimit = counters.values.filter { it.exceeded() }
        if (overLimit.isEmpty()) return
        val currentRoutes = routes
        val kickMessage = limitKickMessageSupplier()
        for (session in PlayerSessions.all()) {
            val route = currentRoutes.firstOrNull { it.match(session.host) != null } ?: continue
            val counter = keyFor(route)?.let { counters[it] } ?: continue
            if (counter !in overLimit) continue
            log.warn(
                "Route metrics limit reached on '{}' (upload {}/{}, download {}/{}) - kicking '{}'",
                session.host, counter.uploadBytes.get(), counter.uploadLimit,
                counter.downloadBytes.get(), counter.downloadLimit, session.name
            )
            PlayerSessions.kick(session.uuid, kickMessage)
        }
    }
}
