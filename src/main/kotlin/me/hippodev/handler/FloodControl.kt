package me.hippodev.handler

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Process-wide flood ceilings layered on top of [ConnectionGuardHandler]'s per-IP pre-login cap.
 * Checked in the child-channel initializer (see Main.kt) so a rejected connection is closed
 * before any per-connection state (pipeline handlers, the login-deadline timer) is built.
 */

/** Hard cap on total concurrent client connections. */
object GlobalConnections {
    private val count = AtomicInteger(0)

    /** Reserves a slot if fewer than [max] are held; caller must [release] on channel close. */
    fun tryAcquire(max: Int): Boolean {
        if (count.incrementAndGet() > max) {
            count.decrementAndGet()
            return false
        }
        return true
    }

    fun release() {
        count.decrementAndGet()
    }

    val current: Int get() = count.get()
}

/**
 * Per-source-IP new-connection rate limit: at most `max` new connections from one IP within a
 * rolling `windowMillis`. Fixed-window (a boundary can briefly allow up to 2x), which is fine for
 * flood mitigation - a real player reconnecting a handful of times is unaffected, a single-source
 * connect storm is throttled hard.
 */
object ConnectionRates {
    private val log = LoggerFactory.getLogger(ConnectionRates::class.java)

    private class Window(@Volatile var start: Long, val count: AtomicInteger)

    private val windows = ConcurrentHashMap<String, Window>()
    /** Past this many tracked IPs the limiter fails open (the global cap is then the backstop) -
     *  keeps a flood from millions of distinct sources from growing this map without bound. */
    private const val MAX_TRACKED_IPS = 250_000

    init {
        val reaper = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "conn-rate-reaper").apply { isDaemon = true }
        }
        reaper.scheduleAtFixedRate({ sweep() }, 60_000, 60_000, TimeUnit.MILLISECONDS)
    }

    /** True if a new connection from [ip] is within the allowed rate. */
    fun tryAcquire(ip: String, max: Int, windowMillis: Long): Boolean {
        if (windows.size >= MAX_TRACKED_IPS && !windows.containsKey(ip)) return true
        val now = System.currentTimeMillis()
        val w = windows.compute(ip) { _, existing ->
            if (existing == null || now - existing.start >= windowMillis) Window(now, AtomicInteger(0))
            else existing
        }!!
        return w.count.incrementAndGet() <= max
    }

    private fun sweep() {
        try {
            val cutoff = System.currentTimeMillis() - 600_000
            windows.entries.removeIf { it.value.start < cutoff }
        } catch (e: Exception) {
            log.debug("Connection-rate sweep failed", e)
        }
    }

    /** Current window count for [ip] - for tests. */
    fun countForTest(ip: String): Int = windows[ip]?.count?.get() ?: 0
}

/**
 * Process-wide cap on players held in the reconnect-wait state (see [ReconnectHandler]). Each held
 * player holds a live connection plus keep-alive / animation / retry timers, so an unbounded number
 * of them - e.g. a login flood arriving while a backend is briefly down on an offline-mode server -
 * is itself a resource-exhaustion vector. [max] is applied from config (0 = unlimited).
 */
object HeldReconnectSessions {
    @Volatile var max: Int = 500
    private val held = AtomicInteger(0)

    /** Reserves a hold slot. Caller ([ReconnectHandler]) must [exit] exactly once when the hold
     *  ends (client gone, or transferred onto a real backend). */
    fun tryEnter(): Boolean {
        if (held.incrementAndGet() > max && max > 0) {
            held.decrementAndGet()
            return false
        }
        return true
    }

    fun exit() {
        held.updateAndGet { if (it > 0) it - 1 else 0 }
    }

    val current: Int get() = held.get()
}
