package me.hippodev.config

import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

private const val CACHE_TTL_MILLIS = 60_000L
/** How long an entry can go un-looked-up before the periodic sweep evicts it. Matters for
 *  wildcard routes with hostname (not IP-literal) backend templates: every distinct captured
 *  value resolves to its own cache key, and without this, a hostname nobody connects to anymore
 *  (an old/rotated subdomain, a one-off captured value) would sit in [DnsCache.cache] forever -
 *  unbounded growth over the process's lifetime. A host still being actively used gets refreshed
 *  well within this window (see [DnsCache.resolve]) and is never swept. */
private const val ENTRY_IDLE_EVICT_MILLIS = 10 * 60_000L
private val ipLiteralPattern = Regex("""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

/**
 * Resolves backend hostnames off the Netty event-loop threads.
 *
 * `InetSocketAddress(host, port)` performs a synchronous DNS lookup for anything that isn't an
 * IP literal. [Route.resolveBackends] runs on the event-loop thread that accepted the connection
 * (see `dispatch()` in Main.kt) for *every* connecting player - Netty multiplexes many player
 * channels onto a small, fixed pool of these threads, so a single slow/hanging lookup there
 * doesn't just delay the connecting player, it stalls every other channel sharing that thread
 * until it returns (a visible ping/lag spike for unrelated already-connected players).
 *
 * IP-literal backends (the common case for internal routing) skip this entirely - they never
 * trigger a real lookup. Hostname backends are cached with a short TTL and resolved
 * stale-while-revalidate: once a hostname has resolved once, later calls return the cached
 * address immediately and kick off a background refresh, so only the very first connection to a
 * newly-seen backend host can block on a real lookup.
 */
object DnsCache {
    private val log = LoggerFactory.getLogger(DnsCache::class.java)

    private data class Entry(val addr: InetSocketAddress, val expiresAt: Long)

    private val executor = Executors.newFixedThreadPool(4) { r ->
        Thread(r, "dns-resolver").apply { isDaemon = true }
    }
    private val cache = ConcurrentHashMap<String, Entry>()
    private val refreshing = ConcurrentHashMap<String, AtomicBoolean>()

    init {
        val reaper = java.util.concurrent.Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "dns-cache-reaper").apply { isDaemon = true }
        }
        reaper.scheduleAtFixedRate(
            { evictIdleEntries() }, ENTRY_IDLE_EVICT_MILLIS, ENTRY_IDLE_EVICT_MILLIS,
            java.util.concurrent.TimeUnit.MILLISECONDS
        )
    }

    /** A still-live entry's [Entry.expiresAt] keeps getting pushed forward every [resolve] call
     *  (directly on first lookup, via [refreshAsync] afterward) - so an entry whose expiry is more
     *  than [ENTRY_IDLE_EVICT_MILLIS] in the past hasn't been looked up in at least that long and
     *  is safe to drop; the next [resolve] for that host just pays a fresh lookup, identical to a
     *  never-before-seen host. */
    private fun evictIdleEntries() {
        val cutoff = System.currentTimeMillis() - ENTRY_IDLE_EVICT_MILLIS
        cache.entries.removeIf { it.value.expiresAt < cutoff }
        refreshing.keys.retainAll(cache.keys)
    }

    /** Resolves `host:port`. May block the calling thread on the first-ever lookup for a given
     *  hostname; never blocks for IP literals or once a value is cached. */
    fun resolve(host: String, port: Int): InetSocketAddress {
        if (isIpLiteral(host)) return InetSocketAddress(host, port)

        val key = "$host:$port"
        val cached = cache[key]
        if (cached != null) {
            if (System.currentTimeMillis() >= cached.expiresAt) refreshAsync(key, host, port)
            return cached.addr
        }

        // No cached value at all yet - nothing to return in the meantime, so this first lookup
        // has to be synchronous. Every subsequent call for this host is non-blocking.
        val addr = InetSocketAddress(host, port)
        cache[key] = Entry(addr, System.currentTimeMillis() + CACHE_TTL_MILLIS)
        return addr
    }

    /** Pre-resolves [host]:[port] on a background thread so the first player connection to it
     *  doesn't pay the lookup cost on the event loop. Safe to call for IP literals (no-op cost). */
    fun warm(host: String, port: Int) {
        if (isIpLiteral(host)) return
        executor.execute {
            try {
                val addr = InetSocketAddress(host, port)
                cache["$host:$port"] = Entry(addr, System.currentTimeMillis() + CACHE_TTL_MILLIS)
            } catch (e: Exception) {
                log.debug("Failed to pre-resolve backend host '{}': {}", host, e.message)
            }
        }
    }

    private fun refreshAsync(key: String, host: String, port: Int) {
        val inProgress = refreshing.computeIfAbsent(key) { AtomicBoolean(false) }
        if (!inProgress.compareAndSet(false, true)) return
        executor.execute {
            try {
                val addr = InetSocketAddress(host, port)
                cache[key] = Entry(addr, System.currentTimeMillis() + CACHE_TTL_MILLIS)
            } catch (e: Exception) {
                log.debug("Failed to refresh backend host '{}': {}", host, e.message)
            } finally {
                inProgress.set(false)
            }
        }
    }

    private fun isIpLiteral(host: String): Boolean =
        host.matches(ipLiteralPattern) || host.contains(':') // dotted-quad IPv4 or bracket-less IPv6
}
