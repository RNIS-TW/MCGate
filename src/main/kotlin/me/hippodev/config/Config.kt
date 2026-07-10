package me.hippodev.config

import org.yaml.snakeyaml.Yaml
import java.io.File
import java.net.InetSocketAddress
import java.util.Base64
import java.util.regex.Pattern
import me.hippodev.protocol.*

enum class Strategy {
    SEQUENTIAL, RANDOM, ROUND_ROBIN, LEAST_CONNECTIONS, LOWEST_LATENCY;

    companion object {
        fun parse(value: String?): Strategy {
            if (value == null) return SEQUENTIAL
            return when (value.lowercase()) {
                "sequential" -> SEQUENTIAL
                "random" -> RANDOM
                "round-robin", "round_robin" -> ROUND_ROBIN
                "least-connections", "least_connections" -> LEAST_CONNECTIONS
                "lowest-latency", "lowest_latency" -> LOWEST_LATENCY
                else -> error("Unknown load balancing strategy: $value")
            }
        }
    }
}

data class VersionInfo(val name: String, val protocol: Int)
data class PlayersInfo(val online: Int, val max: Int)

data class FallbackStatus(
    val motd: String,
    val version: VersionInfo,
    val players: PlayersInfo?,
    val faviconDataUri: String?
)

/**
 * Auto-reconnect behavior for when a route's backends are all unreachable - keeps the player
 * connected to MCGate itself (no separate holding server needed) and transfers them onto the real
 * backend once it's reachable again. Only supported for clients on protocol versions covered by
 * [me.hippodev.protocol.ReconnectProtocol] (currently just the latest bracket - see that file);
 * other clients always get the plain [kickMessage] disconnect.
 */
data class ReconnectConfig(
    val enabled: Boolean = false,
    val onMidSessionDrop: Boolean = enabled,
    val retryIntervalMillis: Long = 5000,
    val maxRetryIntervalMillis: Long = 30000,
    val backoffMultiplier: Double = 2.0,
    val maxWaitMillis: Long = 0, // 0 = unlimited
    val title: String = "&eServer is currently offline.",
    val subtitle: String = "&7Waiting to reconnect...",
    val actionBarFrames: List<String> = listOf("&7Reconnecting.", "&7Reconnecting..", "&7Reconnecting..."),
    val animationIntervalMillis: Long = 500,
    val kickMessage: String = "&cServer is offline. Please reconnect shortly."
)

data class Route(
    val hostPatterns: List<HostPattern>,
    val backendTemplates: List<String>,
    val strategy: Strategy,
    val cachePingTTLMillis: Long,
    val fallback: FallbackStatus?,
    val modifyVirtualHost: Boolean,
    val proxyProtocol: Boolean,
    val priority: Int,
    val reconnect: ReconnectConfig
) {
    /** Returns the wildcard captures of the first matching host pattern, or null if none match. */
    fun match(hostname: String): List<String>? {
        for (pattern in hostPatterns) {
            val captures = pattern.match(hostname)
            if (captures != null) return captures
        }
        return null
    }

    fun resolveBackends(captures: List<String>): List<InetSocketAddress> =
        backendTemplates.map { parseHostPort(substituteParams(it, captures)) }
}

data class ApiConfig(
    val enabled: Boolean = false,
    val bind: String = "localhost:8080"
) {
    val bindAddress: InetSocketAddress by lazy { parseHostPort(bind, defaultPort = 8080) }
}

data class GateConfig(
    val bind: String = "0.0.0.0:25565",
    val routes: List<Route> = emptyList(),
    val api: ApiConfig = ApiConfig()
) {
    val bindAddress: InetSocketAddress by lazy { parseHostPort(bind) }

    companion object {
        private val log = org.slf4j.LoggerFactory.getLogger(GateConfig::class.java)

        @Suppress("UNCHECKED_CAST")
        fun load(path: String): GateConfig {
            val file = File(path)
            if (!file.exists()) {
                error("Config file not found: $path")
            }
            val yaml = Yaml()
            val root = file.inputStream().use { yaml.load<Map<String, Any>>(it) } ?: emptyMap()
            val configSection = (root["config"] as? Map<String, Any>) ?: root

            val bind = configSection["bind"] as? String ?: "0.0.0.0:25565"
            val rawRoutes = configSection["routes"] as? List<Map<String, Any>> ?: emptyList()

            // Higher priority routes are matched first; ties keep config file order.
            val routes = rawRoutes.mapIndexed { index, r -> parseRoute(r, index) }
                .sortedWith(compareByDescending<Route> { it.priority })
            val api = parseApi(configSection["api"] as? Map<String, Any>)

            return GateConfig(bind = bind, routes = routes, api = api)
        }

        private fun parseApi(a: Map<String, Any>?): ApiConfig {
            if (a == null) return ApiConfig()
            return ApiConfig(
                enabled = a["enabled"] as? Boolean ?: false,
                bind = a["bind"] as? String ?: "localhost:8080"
            )
        }

        private fun parseRoute(r: Map<String, Any>, index: Int): Route {
            val hostRaw = r["host"]
            val hosts = when (hostRaw) {
                is List<*> -> hostRaw.map { it.toString() }
                is String -> listOf(hostRaw)
                else -> error("Route #$index missing 'host'")
            }
            val hostPatterns = hosts.map { HostPattern(it) }

            val backendRaw = r["backend"]
            val backends = when (backendRaw) {
                is List<*> -> backendRaw.map { it.toString() }
                is String -> listOf(backendRaw)
                else -> error("Route #$index missing 'backend'")
            }

            validateParams(hosts, backends, index)

            val strategy = Strategy.parse(r["strategy"] as? String)
            val ttl = parseDuration(r["cachePingTTL"] as? String ?: "10s")
            val fallback = parseFallback(r["fallback"] as? Map<String, Any>)
            val modifyVirtualHost = r["modifyVirtualHost"] as? Boolean ?: false
            val proxyProtocol = r["proxyProtocol"] as? Boolean ?: false
            val priority = r["priority"] as? Int ?: 0
            val reconnect = parseReconnect(r["reconnect"] as? Map<String, Any>)

            return Route(
                hostPatterns = hostPatterns,
                backendTemplates = backends,
                strategy = strategy,
                cachePingTTLMillis = ttl,
                fallback = fallback,
                modifyVirtualHost = modifyVirtualHost,
                proxyProtocol = proxyProtocol,
                priority = priority,
                reconnect = reconnect
            )
        }

        @Suppress("UNCHECKED_CAST")
        private fun parseReconnect(r: Map<String, Any>?): ReconnectConfig {
            val defaults = ReconnectConfig()
            if (r == null) return defaults
            val enabled = r["enabled"] as? Boolean ?: defaults.enabled
            val frames = (r["actionBarFrames"] as? List<*>)?.map { it.toString() }
            return ReconnectConfig(
                enabled = enabled,
                onMidSessionDrop = r["onMidSessionDrop"] as? Boolean ?: enabled,
                retryIntervalMillis = (r["retryInterval"] as? String)?.let { parseDuration(it) } ?: defaults.retryIntervalMillis,
                maxRetryIntervalMillis = (r["maxRetryInterval"] as? String)?.let { parseDuration(it) } ?: defaults.maxRetryIntervalMillis,
                backoffMultiplier = (r["backoffMultiplier"] as? Number)?.toDouble() ?: defaults.backoffMultiplier,
                maxWaitMillis = (r["maxWait"] as? String)?.let { parseDuration(it) } ?: defaults.maxWaitMillis,
                title = r["title"] as? String ?: defaults.title,
                subtitle = r["subtitle"] as? String ?: defaults.subtitle,
                actionBarFrames = frames ?: defaults.actionBarFrames,
                animationIntervalMillis = (r["animationInterval"] as? String)?.let { parseDuration(it) } ?: defaults.animationIntervalMillis,
                kickMessage = r["kickMessage"] as? String ?: defaults.kickMessage
            )
        }

        private fun validateParams(hosts: List<String>, backends: List<String>, index: Int) {
            val maxWildcards = hosts.maxOfOrNull { HostPattern(it).wildcardCount } ?: 0
            for (backend in backends) {
                val matcher = paramRefPattern.matcher(backend)
                while (matcher.find()) {
                    val n = matcher.group(1).toInt()
                    if (maxWildcards == 0) {
                        log.warn(
                            "Route #{}: backend '{}' references \${}, but host pattern has no wildcards - it won't be substituted",
                            index, backend, n
                        )
                    } else if (n > maxWildcards) {
                        log.warn(
                            "Route #{}: backend '{}' references \${}, but only {} wildcard(s) are captured",
                            index, backend, n, maxWildcards
                        )
                    }
                }
            }
        }

        @Suppress("UNCHECKED_CAST")
        private fun parseFallback(f: Map<String, Any>?): FallbackStatus? {
            if (f == null) return null
            val motd = f["motd"] as? String ?: ""
            val versionMap = f["version"] as? Map<String, Any>
            val version = VersionInfo(
                name = versionMap?.get("name") as? String ?: "",
                protocol = (versionMap?.get("protocol") as? Int) ?: -1
            )
            val playersMap = f["players"] as? Map<String, Any>
            val players = playersMap?.let {
                PlayersInfo(online = (it["online"] as? Int) ?: 0, max = (it["max"] as? Int) ?: 0)
            }
            val favicon = (f["favicon"] as? String)?.let { resolveFavicon(it) }
            return FallbackStatus(motd = motd, version = version, players = players, faviconDataUri = favicon)
        }

        private fun resolveFavicon(value: String): String? {
            if (value.startsWith("data:image")) return value
            val file = File(value)
            if (!file.exists()) {
                log.warn("Favicon file not found: {}", value)
                return null
            }
            val bytes = file.readBytes()
            return "data:image/png;base64," + Base64.getEncoder().encodeToString(bytes)
        }
    }
}

private val paramRefPattern = Pattern.compile("\\$(\\d+)")

fun parseHostPort(value: String, defaultPort: Int = 25565): InetSocketAddress {
    val idx = value.lastIndexOf(':')
    return if (idx >= 0) {
        val hostPart = value.substring(0, idx)
        val portPart = value.substring(idx + 1).toInt()
        InetSocketAddress(hostPart, portPart)
    } else {
        InetSocketAddress(value, defaultPort)
    }
}

private val durationPattern = Pattern.compile("(-?\\d+)(ms|s|m|h)")

/** Parses durations like "3m", "60s", "500ms", "-1s" into milliseconds. */
fun parseDuration(value: String): Long {
    val m = durationPattern.matcher(value.trim())
    if (!m.matches()) error("Invalid duration: $value")
    val amount = m.group(1).toLong()
    return when (m.group(2)) {
        "ms" -> amount
        "s" -> amount * 1000
        "m" -> amount * 60_000
        "h" -> amount * 3_600_000
        else -> error("Invalid duration unit: $value")
    }
}
