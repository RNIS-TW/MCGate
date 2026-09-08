package me.hippodev.routing

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** How long a failed dial is remembered so repeated status pings during an outage return the
 *  fallback immediately instead of each separately eating a full connect timeout. Deliberately
 *  short and independent of the route's (often much longer) `cachePingTTL`, so a backend coming
 *  back up is noticed quickly. */
private const val FAILURE_TTL_MILLIS = 5_000L

/** How often the reaper sweeps expired entries out of both maps. Without this, entries are only
 *  ever removed lazily when their *exact* key is looked up again after expiry - and for a wildcard
 *  route (`*.example.com`) hit by server-list scanners with random subdomains and protocol
 *  versions, almost every key is unique and never queried twice, so both maps would grow without
 *  bound over the process's lifetime and eventually OOM. Mirrors [me.hippodev.config.DnsCache] and
 *  [me.hippodev.voice.VoiceRouting], which guard the same risk the same way. */
private const val SWEEP_INTERVAL_MILLIS = 60_000L

/** Hard backstop on the number of live response-cache entries, independent of TTL - a scanning
 *  wave can create keys far faster than [SWEEP_INTERVAL_MILLIS] clears them. Once exceeded, the
 *  oldest-expiring entries are dropped on the next write. Generous: a real deployment has a
 *  handful of backends * a handful of client protocol versions, nowhere near this. */
private const val MAX_ENTRIES = 10_000

class PingCache {
    private val log = LoggerFactory.getLogger(PingCache::class.java)

    private data class Entry(val json: String, val expiresAt: Long)

    private val entries = ConcurrentHashMap<String, Entry>()
    private val downUntil = ConcurrentHashMap<String, Long>()

    init {
        val reaper = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "ping-cache-reaper").apply { isDaemon = true }
        }
        reaper.scheduleAtFixedRate(
            { sweepExpired() }, SWEEP_INTERVAL_MILLIS, SWEEP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS
        )
    }

    fun get(key: String): String? {
        val entry = entries[key] ?: return null
        if (System.currentTimeMillis() > entry.expiresAt) {
            entries.remove(key)
            return null
        }
        return entry.json
    }

    fun put(key: String, json: String, ttlMillis: Long) {
        downUntil.remove(key)
        if (ttlMillis < 0) return
        entries[key] = Entry(json, System.currentTimeMillis() + ttlMillis)
        if (entries.size > MAX_ENTRIES) evictOverflow()
    }

    /** Whether [key]'s last dial attempt failed recently enough that it's not worth retrying yet. */
    fun isKnownDown(key: String): Boolean {
        val until = downUntil[key] ?: return false
        if (System.currentTimeMillis() > until) {
            downUntil.remove(key)
            return false
        }
        return true
    }

    fun markDown(key: String) {
        downUntil[key] = System.currentTimeMillis() + FAILURE_TTL_MILLIS
    }

    /** Live response-cache entry count - for tests asserting the map stays bounded. */
    internal fun entryCountForTest(): Int = entries.size

    private fun sweepExpired() {
        try {
            val now = System.currentTimeMillis()
            entries.entries.removeIf { it.value.expiresAt < now }
            downUntil.entries.removeIf { it.value < now }
        } catch (e: Exception) {
            log.debug("Ping cache sweep failed", e)
        }
    }

    /** Trims [entries] well below [MAX_ENTRIES] (to 90%) by dropping the soonest-to-expire first -
     *  called only on a write that pushes past the cap. Overshooting the trim keeps this off the
     *  hot path during a sustained scanning burst (one sort per ~1k writes, not one per write). */
    private fun evictOverflow() {
        val target = MAX_ENTRIES * 9 / 10
        val overBy = entries.size - target
        if (overBy <= 0) return
        entries.entries
            .sortedBy { it.value.expiresAt }
            .take(overBy)
            .forEach { entries.remove(it.key, it.value) }
    }
}
