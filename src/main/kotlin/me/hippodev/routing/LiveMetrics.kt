package me.hippodev.routing

import me.hippodev.config.Route
import me.hippodev.tracking.ConnectionTracker
import java.lang.management.ManagementFactory

/** Live per-backend stats for one route/backend pair - shared across every host alias of that
 *  route/backend, since that's how active-connection/latency tracking actually works (see
 *  [RouteRuntime]). [hosts] lists every alias so a consumer can still see which hostnames this
 *  covers, even though the counters themselves can't be split further per host. */
data class BackendMetric(val routeIndex: Int, val hosts: List<String>, val backend: String, val active: Int, val latencyMillis: Long?)

/** One point-in-time read of everything MCGate's live stats surfaces expose - the API's
 *  `/metrics`/`/v1/players`, and [me.hippodev.tracking.StatsLogger]'s periodic local time-series
 *  log. Gathering this touches only in-memory ConcurrentHashMap/AtomicInteger reads (PlayerSessions,
 *  RouteRuntime, ConnectionTracker's counters) - no disk I/O, no backend dial - so it's cheap
 *  enough to call on every API request AND on every stats-logging tick without adding load
 *  anywhere that matters. */
data class MetricsSnapshot(
    val timestamp: Long,
    val playersOnline: Int,
    /** Live player count per literal virtual host - see [PlayerSessions.onlineByHost] for why
     *  this, not the backend-level [BackendMetric], is what actually distinguishes hosts that
     *  share one route/backend. */
    val onlineByHost: Map<String, Int>,
    val uptimeSeconds: Double,
    val backends: List<BackendMetric>,
    val players: List<PlayerSession>,
    val trackingEnabled: Boolean,
    val trackingQueuedRecords: Int,
    val trackingDroppedRecordsTotal: Long
)

/** Gathers a [MetricsSnapshot] right now. Safe to call from any thread - none of the sources read
 *  here (PlayerSessions, RouteRuntime, ConnectionTracker) are tied to a Netty event loop. */
fun collectMetrics(routes: List<Route>, runtimeSupplier: (Route) -> RouteRuntime?): MetricsSnapshot {
    val backends = routes.flatMapIndexed { index, route ->
        val runtime = runtimeSupplier(route)
        // All hostnames this route matches, not just the first - a route can list several
        // (`host: [a.example.com, b.example.com]`), and every one of them shares the same
        // backend/active-connection counters, so dropping any but the first here previously
        // made per-host stats silently disappear for every alias but the one shown.
        val hosts = route.hostPatterns.map { it.raw }
        val resolved = if (route.hostPatterns.all { it.wildcardCount == 0 }) {
            try { route.resolveBackends(emptyList()) } catch (e: Exception) { null }
        } else null
        route.backendTemplates.mapIndexedNotNull { i, template ->
            val addr = resolved?.getOrNull(i) ?: return@mapIndexedNotNull null
            BackendMetric(
                routeIndex = index, hosts = hosts, backend = template,
                active = runtime?.activeConnections?.get(addr)?.get() ?: 0,
                latencyMillis = runtime?.latencyOf(addr)
            )
        }
    }
    val players = PlayerSessions.all()
    return MetricsSnapshot(
        timestamp = System.currentTimeMillis(),
        playersOnline = players.size,
        onlineByHost = PlayerSessions.onlineByHost(),
        uptimeSeconds = ManagementFactory.getRuntimeMXBean().uptime / 1000.0,
        backends = backends,
        players = players,
        trackingEnabled = ConnectionTracker.enabled,
        trackingQueuedRecords = ConnectionTracker.queuedRecords,
        trackingDroppedRecordsTotal = ConnectionTracker.droppedRecordsTotal
    )
}
