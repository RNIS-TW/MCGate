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
    val attemptSuffix: String = " (attempt {attempt})",
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
    val reconnect: ReconnectConfig,
    /** Optional UDP backend (Simple Voice Chat or similar mods) for this route, relayed by
     *  [me.hippodev.voice.VoiceRelay] on the same port as the Minecraft TCP listener - no
     *  separate port to open. Empty when the route has no `voicechat:` entry. Only the first
     *  template is used: unlike `backend`, there's no failover/load-balancing concept for a UDP
     *  relay session once it's been handed off to a backend. */
    val voicechatTemplates: List<String> = emptyList()
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
        backendTemplates.map { resolveBackendAddress(substituteParams(it, captures)) }

    /** Same resolution (param substitution + [DnsCache]) as [resolveBackends], for the optional
     *  `voicechat:` backend. Null when this route has none configured. */
    fun resolveVoicechat(captures: List<String>): InetSocketAddress? =
        voicechatTemplates.firstOrNull()?.let { resolveBackendAddress(substituteParams(it, captures)) }
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
    val api: ApiConfig = ApiConfig(),
    /** Netty worker event-loop thread count. 0 = auto (max(4, 2x CPU cores)).
     *
     * Every client channel is pinned to exactly one of these threads for its whole connection -
     * if a small VPS only gets Netty's CPU-count-based default (as few as 2 threads on a 1-2
     * vCPU box), players split roughly evenly across them, and anything that stalls one thread
     * (a GC pause, a slow log write, etc.) stalls every player pinned to it while the rest are
     * unaffected - the "half the players lag" symptom. MCGate is I/O-bound, not CPU-bound, so
     * more threads than cores is fine and just spreads players thinner across them. */
    val workerThreads: Int = 0,
    /** Logs every incoming connection at INFO (host, remote address, protocol version, and
     *  whether it's a status ping or a login) as soon as the handshake is read - including status
     *  pings, which otherwise aren't logged at all. Off by default since server-list pingers/
     *  scanners can hit a public port frequently enough to be noisy. */
    val logConnections: Boolean = false,
    /** Accept a PROXY protocol (v1/v2) header at the start of every incoming connection, before
     *  the Minecraft handshake - for when MCGate itself sits behind another load balancer/proxy
     *  that needs to hand it the real client address. Distinct from a [Route]'s own
     *  `proxyProtocol`, which is MCGate *sending* that header onward to its backend; this is
     *  MCGate *receiving* one from whatever's in front of it. Off by default: a plain client
     *  connecting straight to MCGate does not send this header, so turning it on when nothing
     *  upstream actually sends one just makes every real connection look like garbage and get
     *  dropped. */
    val proxyProtocol: Boolean = false
) {
    val bindAddress: InetSocketAddress by lazy { parseHostPort(bind) }

    companion object {
        private val log = org.slf4j.LoggerFactory.getLogger(GateConfig::class.java)

        @Suppress("UNCHECKED_CAST")
        fun load(path: String, messages: GateMessages = GateMessages()): GateConfig {
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
            val routes = rawRoutes.mapIndexed { index, r -> parseRoute(r, index, messages) }
                .sortedWith(compareByDescending<Route> { it.priority })
            warmStaticBackends(routes)
            val api = parseApi(configSection["api"] as? Map<String, Any>)
            val workerThreads = configSection["workerThreads"] as? Int ?: 0
            val logConnections = configSection["logConnections"] as? Boolean ?: false
            val proxyProtocol = configSection["proxyProtocol"] as? Boolean ?: false

            return GateConfig(
                bind = bind, routes = routes, api = api,
                workerThreads = workerThreads, logConnections = logConnections, proxyProtocol = proxyProtocol
            )
        }

        private fun parseApi(a: Map<String, Any>?): ApiConfig {
            if (a == null) return ApiConfig()
            return ApiConfig(
                enabled = a["enabled"] as? Boolean ?: false,
                bind = a["bind"] as? String ?: "localhost:8080"
            )
        }

        private fun parseRoute(r: Map<String, Any>, index: Int, messages: GateMessages): Route {
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

            val voicechatRaw = r["voicechat"]
            val voicechatBackends = when (voicechatRaw) {
                is List<*> -> voicechatRaw.map { it.toString() }
                is String -> listOf(voicechatRaw)
                else -> emptyList()
            }
            if (voicechatBackends.isNotEmpty()) validateParams(hosts, voicechatBackends, index, "voicechat")

            val strategy = Strategy.parse(r["strategy"] as? String)
            val ttl = parseDuration(r["cachePingTTL"] as? String ?: "10s")
            val fallback = parseFallback(r["fallback"] as? Map<String, Any>)
            val modifyVirtualHost = r["modifyVirtualHost"] as? Boolean ?: false
            val proxyProtocol = r["proxyProtocol"] as? Boolean ?: false
            val priority = r["priority"] as? Int ?: 0
            val reconnect = renderReconnectText(parseReconnect(r["reconnect"] as? Map<String, Any>, messages.reconnect))

            return Route(
                hostPatterns = hostPatterns,
                backendTemplates = backends,
                strategy = strategy,
                cachePingTTLMillis = ttl,
                fallback = fallback,
                modifyVirtualHost = modifyVirtualHost,
                proxyProtocol = proxyProtocol,
                priority = priority,
                reconnect = reconnect,
                voicechatTemplates = voicechatBackends
            )
        }

        @Suppress("UNCHECKED_CAST")
        private fun parseReconnect(r: Map<String, Any>?, messageDefaults: ReconnectMessages): ReconnectConfig {
            // Message text defaults to messages.yml (hot-reloadable, shared across routes);
            // a route can still override any of it individually via config.yml.
            val defaults = ReconnectConfig(
                title = messageDefaults.title,
                subtitle = messageDefaults.subtitle,
                attemptSuffix = messageDefaults.attemptSuffix,
                actionBarFrames = messageDefaults.actionBarFrames,
                kickMessage = messageDefaults.kickMessage
            )
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
                attemptSuffix = r["attemptSuffix"] as? String ?: defaults.attemptSuffix,
                actionBarFrames = frames ?: defaults.actionBarFrames,
                animationIntervalMillis = (r["animationInterval"] as? String)?.let { parseDuration(it) } ?: defaults.animationIntervalMillis,
                kickMessage = r["kickMessage"] as? String ?: defaults.kickMessage
            )
        }

        /** Pre-renders `title`/`subtitle`/`attemptSuffix`/`actionBarFrames` (legacy `&`-codes and
         *  MiniMessage tags both parsed - see TextFormat.kt) once at config load, rather than
         *  every single packet send. `title`/`subtitle` are sent at most a couple of times per
         *  reconnect-wait session, but `actionBarFrames` is on the animation timer's hot path
         *  (every `animationInterval`, 500ms by default, for every player currently waiting to
         *  reconnect) - re-parsing MiniMessage there on every tick was real, needless CPU work on
         *  the event-loop thread. `kickMessage` is deliberately left raw here: it's used via two
         *  different renderers (JSON for the Login-state kick, legacy for the Play-state one) and
         *  is only ever sent once per connection at most, so rendering it at each of those two
         *  call sites instead is simpler and not a hot path either way. */
        private fun renderReconnectText(reconnect: ReconnectConfig): ReconnectConfig = reconnect.copy(
            title = toLegacyText(reconnect.title),
            subtitle = toLegacyText(reconnect.subtitle),
            attemptSuffix = toLegacyText(reconnect.attemptSuffix),
            actionBarFrames = reconnect.actionBarFrames.map { toLegacyText(it) }
        )

        /** Pre-resolves backend hostnames that don't depend on a wildcard capture, so the DNS
         *  cache is already warm before the first player connects - see [DnsCache]. Templated
         *  backends (containing `$N`) can't be pre-warmed since the real hostname isn't known
         *  until a matching connection arrives. */
        private fun warmStaticBackends(routes: List<Route>) {
            for (route in routes) {
                for (template in route.backendTemplates + route.voicechatTemplates) {
                    if (template.contains('$')) continue
                    val idx = template.lastIndexOf(':')
                    if (idx < 0) continue
                    DnsCache.warm(template.substring(0, idx), template.substring(idx + 1).toIntOrNull() ?: continue)
                }
            }
        }

        private fun validateParams(hosts: List<String>, backends: List<String>, index: Int, label: String = "backend") {
            val maxWildcards = hosts.maxOfOrNull { HostPattern(it).wildcardCount } ?: 0
            for (backend in backends) {
                val matcher = paramRefPattern.matcher(backend)
                while (matcher.find()) {
                    val n = matcher.group(1).toInt()
                    if (maxWildcards == 0) {
                        log.warn(
                            "Route #{}: {} '{}' references \${}, but host pattern has no wildcards - it won't be substituted",
                            index, label, backend, n
                        )
                    } else if (n > maxWildcards) {
                        log.warn(
                            "Route #{}: {} '{}' references \${}, but only {} wildcard(s) are captured",
                            index, label, backend, n, maxWildcards
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

/** Like [parseHostPort] but resolves the hostname through [DnsCache] instead of blocking
 *  directly - see [DnsCache] for why that matters on the connection dispatch hot path. */
fun resolveBackendAddress(value: String, defaultPort: Int = 25565): InetSocketAddress {
    val idx = value.lastIndexOf(':')
    return if (idx >= 0) {
        DnsCache.resolve(value.substring(0, idx), value.substring(idx + 1).toInt())
    } else {
        DnsCache.resolve(value, defaultPort)
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
