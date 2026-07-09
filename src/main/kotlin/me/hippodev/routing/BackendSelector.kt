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

    fun recordConnectClosed(addr: InetSocketAddress) {
        activeConnections[addr]?.decrementAndGet()
    }

    fun recordLatency(addr: InetSocketAddress, millis: Long) {
        latency[addr] = millis to System.currentTimeMillis()
    }

    fun latencyOf(addr: InetSocketAddress): Long? {
        val (millis, measuredAt) = latency[addr] ?: return null
        if (System.currentTimeMillis() - measuredAt > LATENCY_TTL_MILLIS) return null
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
