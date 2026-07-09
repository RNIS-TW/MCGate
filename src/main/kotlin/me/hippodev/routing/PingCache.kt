package me.hippodev.routing

import java.util.concurrent.ConcurrentHashMap

/** How long a failed dial is remembered so repeated status pings during an outage return the
 *  fallback immediately instead of each separately eating a full connect timeout. Deliberately
 *  short and independent of the route's (often much longer) `cachePingTTL`, so a backend coming
 *  back up is noticed quickly. */
private const val FAILURE_TTL_MILLIS = 5_000L

class PingCache {
    private data class Entry(val json: String, val expiresAt: Long)

    private val entries = ConcurrentHashMap<String, Entry>()
    private val downUntil = ConcurrentHashMap<String, Long>()

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
}
