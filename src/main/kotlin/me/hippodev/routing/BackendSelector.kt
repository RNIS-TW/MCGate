package me.hippodev.routing

import me.hippodev.config.Route
import me.hippodev.config.Strategy
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

private const val LATENCY_TTL_MILLIS = 3 * 60_000L

/** Max concurrent live status/ping dials to a single backend address. A status flood that
 *  bypasses the response cache (e.g. by varying the client protocol version) would otherwise
 *  open an unbounded number of 5s-timeout probe sockets to the backend - a backend-amplification
 *  DDoS through MCGate. Past this, the status request fails over / serves fallback instead. */
const val MAX_CONCURRENT_STATUS_DIALS = 8

/** Per-route mutable state backing the load balancing strategies. */
class RouteRuntime {
    val roundRobinCounter = AtomicInteger(0)
    val activeConnections = ConcurrentHashMap<InetSocketAddress, AtomicInteger>()
    private val latency = ConcurrentHashMap<InetSocketAddress, Pair<Long, Long>>() // addr -> (millis, measuredAt)
    private val statusDialsInFlight = ConcurrentHashMap<InetSocketAddress, AtomicInteger>()

    /** Reserves a status-dial slot for [addr] if fewer than [max] are already in flight. */
    fun tryBeginStatusDial(addr: InetSocketAddress, max: Int = MAX_CONCURRENT_STATUS_DIALS): Boolean {
        val counter = statusDialsInFlight.computeIfAbsent(addr) { AtomicInteger(0) }
        if (counter.incrementAndGet() > max) {
            if (counter.decrementAndGet() == 0) statusDialsInFlight.remove(addr, counter)
            return false
        }
        return true
    }

    fun endStatusDial(addr: InetSocketAddress) {
        statusDialsInFlight.computeIfPresent(addr) { _, counter ->
            if (counter.decrementAndGet() <= 0) null else counter
        }
    }

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

/** Orders [backends] by the route's strategy; caller should try each in order until one connects. */
fun orderBackends(route: Route, runtime: RouteRuntime, backends: List<InetSocketAddress>): List<InetSocketAddress> {
    if (backends.size <= 1) return backends
    return when (route.strategy) {
        Strategy.SEQUENTIAL -> backends
        // ThreadLocalRandom, not SecureRandom: load-balancer ordering needs no cryptographic
        // strength, and SecureRandom is a single synchronized instance that serializes every
        // connection dispatch across all event loops under a login flood.
        Strategy.RANDOM -> backends.shuffled(ThreadLocalRandom.current())
        Strategy.ROUND_ROBIN -> {
            val start = Math.floorMod(runtime.roundRobinCounter.getAndIncrement(), backends.size)
            backends.subList(start, backends.size) + backends.subList(0, start)
        }
        Strategy.LEAST_CONNECTIONS -> backends.sortedBy { runtime.activeConnections[it]?.get() ?: 0 }
        Strategy.LOWEST_LATENCY -> backends.sortedBy { runtime.latencyOf(it) ?: Long.MAX_VALUE }
    }
}
