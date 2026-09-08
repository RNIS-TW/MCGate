package me.hippodev.routing

import me.hippodev.config.Route
import me.hippodev.config.Strategy
import java.net.InetSocketAddress
import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

private const val LATENCY_TTL_MILLIS = 3 * 60_000L

/** Per-route mutable state backing the load balancing strategies. */
class RouteRuntime {
    val roundRobinCounter = AtomicInteger(0)
    val activeConnections = ConcurrentHashMap<InetSocketAddress, AtomicInteger>()
    private val latency = ConcurrentHashMap<InetSocketAddress, Pair<Long, Long>>() // addr -> (millis, measuredAt)

    fun recordConnectOpened(addr: InetSocketAddress) {
        activeConnections.computeIfAbsent(addr) { AtomicInteger(0) }.incrementAndGet()
    }

    /** Removes [addr]'s entry once its count drops to zero rather than leaving a permanent
     *  zero-valued entry behind - matters for wildcard routes (`$N`-templated backends), where
     *  every distinct captured value resolves to a different address and, without this, would
     *  leave one entry here forever for every backend ever dialed over the process's lifetime. */
    fun recordConnectClosed(addr: InetSocketAddress) {
        activeConnections.computeIfPresent(addr) { _, count ->
            if (count.decrementAndGet() <= 0) null else count
        }
    }

    fun recordLatency(addr: InetSocketAddress, millis: Long) {
        val now = System.currentTimeMillis()
        latency[addr] = millis to now
        // Opportunistic sweep: `latencyOf` only evicts entries it's actually asked for, so a
        // wildcard route's one-off captured backends that never get read back would linger. A dial
        // isn't a hot path and `latency` holds one entry per distinct backend, so scanning it here
        // is cheap and keeps the map bounded without a dedicated reaper thread.
        latency.entries.removeIf { now - it.value.second > LATENCY_TTL_MILLIS }
    }

    /** Same unbounded-growth concern as [recordConnectClosed] applies here - an expired reading
     *  is actively evicted rather than just reported as absent, so a backend address that stops
     *  being dialed (e.g. a wildcard route's captured value nobody uses anymore) doesn't leave its
     *  latency entry in the map forever. */
    fun latencyOf(addr: InetSocketAddress): Long? {
        val (millis, measuredAt) = latency[addr] ?: return null
        if (System.currentTimeMillis() - measuredAt > LATENCY_TTL_MILLIS) {
            latency.remove(addr)
            return null
        }
        return millis
    }
}

private val secureRandom = SecureRandom()

/** Orders [backends] by the route's strategy; caller should try each in order until one connects. */
fun orderBackends(route: Route, runtime: RouteRuntime, backends: List<InetSocketAddress>): List<InetSocketAddress> {
    if (backends.size <= 1) return backends
    return when (route.strategy) {
        Strategy.SEQUENTIAL -> backends
        Strategy.RANDOM -> backends.shuffled(java.util.Random(secureRandom.nextLong()))
        Strategy.ROUND_ROBIN -> {
            val start = Math.floorMod(runtime.roundRobinCounter.getAndIncrement(), backends.size)
            backends.subList(start, backends.size) + backends.subList(0, start)
        }
        Strategy.LEAST_CONNECTIONS -> backends.sortedBy { runtime.activeConnections[it]?.get() ?: 0 }
        Strategy.LOWEST_LATENCY -> backends.sortedBy { runtime.latencyOf(it) ?: Long.MAX_VALUE }
    }
}
