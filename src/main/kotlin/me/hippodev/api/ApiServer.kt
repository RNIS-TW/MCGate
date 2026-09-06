package me.hippodev.api

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http.HttpVersion
import me.hippodev.config.GateConfig
import me.hippodev.config.Route
import me.hippodev.routing.MetricsSnapshot
import me.hippodev.routing.PlayerSession
import me.hippodev.routing.PlayerSessions
import me.hippodev.routing.RouteRuntime
import me.hippodev.routing.collectMetrics
import me.hippodev.routing.pingBackendLive
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture

/**
 * A small read-only JSON-over-HTTP admin/status API. Exposes the routing
 * table and live backend/connection state for observability and external
 * health checks.
 *
 * Endpoints:
 *   GET /v1/routes                 - all routes, their hosts, backends and strategy
 *   GET /v1/routes/{index}         - a single route by its index in the config
 *   GET /v1/routes/{index}/backends - last-known per-backend stats (active connections, latency)
 *   GET /v1/routes/{index}/ping     - dials each backend right now and returns a live status/latency reading
 *   GET /v1/players                 - every currently-connected player: IP, UUID, host, login attempts,
 *                                      packets/bytes sent+received, compression/encryption state
 *   GET /metrics                   - Prometheus text-format metrics, for scraping into Grafana
 *   GET /metrics?type=json         - the same metrics as JSON, plus the full /v1/players detail
 */
class ApiServer(
    private val configSupplier: () -> GateConfig,
    private val runtimeSupplier: (Route) -> RouteRuntime?
) {
    private val log = LoggerFactory.getLogger(ApiServer::class.java)

    private var bossGroup: NioEventLoopGroup? = null
    private var workerGroup: NioEventLoopGroup? = null
    private var channel: Channel? = null

    fun start(bindAddress: InetSocketAddress) {
        val boss = NioEventLoopGroup(1)
        val worker = NioEventLoopGroup()
        bossGroup = boss
        workerGroup = worker

        val bootstrap = ServerBootstrap()
            .group(boss, worker)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline()
                        .addLast(HttpServerCodec())
                        .addLast(HttpObjectAggregator(1 shl 16))
                        .addLast(ApiHandler(configSupplier, runtimeSupplier, worker))
                }
            })

        channel = bootstrap.bind(bindAddress).sync().channel()
        log.info("API listening on {}", bindAddress)
    }

    fun shutdown() {
        channel?.close()
        bossGroup?.shutdownGracefully()
        workerGroup?.shutdownGracefully()
    }
}

private class ApiHandler(
    private val configSupplier: () -> GateConfig,
    private val runtimeSupplier: (Route) -> RouteRuntime?,
    private val pingGroup: EventLoopGroup
) : SimpleChannelInboundHandler<FullHttpRequest>() {

    private val log = LoggerFactory.getLogger(ApiHandler::class.java)

    override fun channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest) {
        if (req.method() != HttpMethod.GET) {
            respond(ctx, req, HttpResponseStatus.METHOD_NOT_ALLOWED, "{\"error\":\"method not allowed\"}")
            return
        }

        val path = req.uri().substringBefore('?').trimEnd('/')
        val routes = configSupplier().routes
        val segments = path.split('/').filter { it.isNotEmpty() }

        when {
            segments == listOf("metrics") -> {
                val snapshot = collectMetrics(routes, runtimeSupplier)
                if (req.uri().substringAfter('?', "").split('&').any { it == "type=json" }) {
                    respond(ctx, req, HttpResponseStatus.OK, metricsJson(snapshot))
                } else {
                    respond(ctx, req, HttpResponseStatus.OK, metricsText(snapshot), contentType = "text/plain; version=0.0.4; charset=utf-8")
                }
            }

            segments == listOf("v1", "routes") ->
                respond(ctx, req, HttpResponseStatus.OK, routesJson(routes))

            segments == listOf("v1", "players") ->
                respond(ctx, req, HttpResponseStatus.OK, playersJson())

            segments.size == 3 && segments[0] == "v1" && segments[1] == "routes" && segments[2].toIntOrNull() != null -> {
                val route = routes.getOrNull(segments[2].toInt())
                if (route == null) notFound(ctx, req) else respond(ctx, req, HttpResponseStatus.OK, routeJson(segments[2].toInt(), route))
            }

            segments.size == 4 && segments[0] == "v1" && segments[1] == "routes" && segments[2].toIntOrNull() != null && segments[3] == "backends" -> {
                val index = segments[2].toInt()
                val route = routes.getOrNull(index)
                if (route == null) notFound(ctx, req) else respond(ctx, req, HttpResponseStatus.OK, backendsJson(route))
            }

            segments.size == 4 && segments[0] == "v1" && segments[1] == "routes" && segments[2].toIntOrNull() != null && segments[3] == "ping" -> {
                val index = segments[2].toInt()
                val route = routes.getOrNull(index)
                if (route == null) notFound(ctx, req) else pingRoute(ctx, req, index, route)
            }

            else -> notFound(ctx, req)
        }
    }

    private fun notFound(ctx: ChannelHandlerContext, req: FullHttpRequest) {
        respond(ctx, req, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}")
    }

    private fun routesJson(routes: List<Route>): String {
        val sb = StringBuilder("{\"routes\":[")
        routes.forEachIndexed { index, route ->
            if (index > 0) sb.append(',')
            sb.append(routeJson(index, route))
        }
        sb.append("]}")
        return sb.toString()
    }

    private fun routeJson(index: Int, route: Route): String {
        val sb = StringBuilder()
        sb.append("{\"index\":").append(index)
        sb.append(",\"hosts\":[")
        route.hostPatterns.forEachIndexed { i, p ->
            if (i > 0) sb.append(',')
            sb.append('"').append(escapeJson(p.raw)).append('"')
        }
        sb.append("],\"backends\":[")
        route.backendTemplates.forEachIndexed { i, b ->
            if (i > 0) sb.append(',')
            sb.append('"').append(escapeJson(b)).append('"')
        }
        sb.append("],\"strategy\":\"").append(route.strategy.name.lowercase()).append('"')
        sb.append(",\"priority\":").append(route.priority)
        sb.append(",\"cachePingTtlMillis\":").append(route.cachePingTTLMillis)
        sb.append(",\"modifyVirtualHost\":").append(route.modifyVirtualHost)
        sb.append(",\"proxyProtocol\":").append(route.proxyProtocol)
        sb.append(",\"hasFallback\":").append(route.fallback != null)
        sb.append('}')
        return sb.toString()
    }

    private fun backendsJson(route: Route): String {
        val runtime = runtimeSupplier(route)
        // Wildcard routes have no fixed backend address until a client connects and
        // supplies the captured segments, so live stats are only resolvable for
        // routes whose backend templates have no $N placeholders.
        val resolved = if (route.hostPatterns.all { it.wildcardCount == 0 }) {
            try { route.resolveBackends(emptyList()) } catch (e: Exception) { null }
        } else null

        val sb = StringBuilder("{\"backends\":[")
        route.backendTemplates.forEachIndexed { i, template ->
            if (i > 0) sb.append(',')
            val addr = resolved?.getOrNull(i)
            sb.append("{\"address\":\"").append(escapeJson(template)).append('"')
            sb.append(",\"activeConnections\":").append(
                if (addr != null && runtime != null) runtime.activeConnections[addr]?.get() ?: 0 else null
            )
            val latency = if (addr != null && runtime != null) runtime.latencyOf(addr) else null
            sb.append(",\"latencyMillis\":").append(latency ?: "null")
            sb.append('}')
        }
        sb.append("]}")
        return sb.toString()
    }

    /** Every currently-connected player, each with the connection detail requested for
     *  monitoring: IP address, UUID, which host they connected through, login attempts (backend
     *  dial attempts across this session's lifetime, including any reconnect hand-offs),
     *  packets/bytes sent and received, and "data control" state - the compression threshold and
     *  encryption flag governing how their traffic is framed. Reads a handful of AtomicLong/
     *  AtomicInteger values already being maintained live per-session (see PlayerSession) - no
     *  extra bookkeeping is done just to serve this endpoint, and nothing here blocks on I/O. */
    private fun playersJson(): String {
        val sb = StringBuilder("{\"players\":[")
        PlayerSessions.all().forEachIndexed { i, s ->
            if (i > 0) sb.append(',')
            sb.append(playerJson(s))
        }
        sb.append("]}")
        return sb.toString()
    }

    private fun playerJson(s: PlayerSession): String {
        val sb = StringBuilder()
        sb.append("{\"uuid\":\"").append(s.uuid).append('"')
        sb.append(",\"name\":\"").append(escapeJson(s.name)).append('"')
        sb.append(",\"ip\":\"").append(escapeJson(s.remoteAddress)).append('"')
        sb.append(",\"host\":\"").append(escapeJson(s.host)).append('"')
        sb.append(",\"backend\":").append(if (s.backend != null) "\"${escapeJson(s.backend.toString())}\"" else "null")
        sb.append(",\"protocolVersion\":").append(s.protocolVersion)
        sb.append(",\"connectedAt\":").append(s.connectedAt)
        sb.append(",\"loginAttempts\":").append(s.loginAttempts.get())
        sb.append(",\"packetsSent\":").append(s.packetsSent.get())
        sb.append(",\"packetsReceived\":").append(s.packetsReceived.get())
        sb.append(",\"bytesSent\":").append(s.bytesSent.get())
        sb.append(",\"bytesReceived\":").append(s.bytesReceived.get())
        sb.append(",\"compressionThreshold\":").append(s.compressionThreshold)
        sb.append(",\"encrypted\":").append(s.encrypted)
        sb.append('}')
        return sb.toString()
    }

    /** Dials every backend of [route] right now (bypassing the ping cache) and reports live results. */
    private fun pingRoute(ctx: ChannelHandlerContext, req: FullHttpRequest, index: Int, route: Route) {
        if (route.hostPatterns.any { it.wildcardCount > 0 }) {
            respond(
                ctx, req, HttpResponseStatus.BAD_REQUEST,
                "{\"error\":\"cannot ping a wildcard route without a concrete host\"}"
            )
            return
        }
        val addrs = try {
            route.resolveBackends(emptyList())
        } catch (e: Exception) {
            respond(ctx, req, HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"error\":\"failed to resolve backends\"}")
            return
        }
        val virtualHost = route.hostPatterns.firstOrNull()?.raw ?: ""
        val runtime = runtimeSupplier(route)

        val pings = route.backendTemplates.mapIndexed { i, template ->
            val addr = addrs[i]
            pingBackendLive(
                pingGroup, addr, protocolVersion = -1, virtualHost = virtualHost, port = addr.port,
                proxyProtocol = route.proxyProtocol
            )
                .handle { result, err -> Triple(template, result, err) }
        }

        CompletableFuture.allOf(*pings.toTypedArray()).whenComplete { _, _ ->
            val sb = StringBuilder("{\"index\":").append(index).append(",\"pings\":[")
            pings.forEachIndexed { i, f ->
                if (i > 0) sb.append(',')
                val (template, result, err) = f.join()
                sb.append("{\"address\":\"").append(escapeJson(template)).append('"')
                if (result != null) {
                    runtime?.recordLatency(addrs[i], result.latencyMillis)
                    sb.append(",\"online\":true,\"latencyMillis\":").append(result.latencyMillis)
                    sb.append(",\"status\":").append(result.statusJson)
                } else {
                    sb.append(",\"online\":false,\"error\":\"")
                        .append(escapeJson(err?.message ?: "unknown error")).append('"')
                }
                sb.append('}')
            }
            sb.append("]}")
            respond(ctx, req, HttpResponseStatus.OK, sb.toString())
        }
    }

    /** Prometheus text-exposition format (0.0.4) - a `scrape_configs` target pointed at
     *  `GET /metrics` is all that's needed on the Prometheus/Grafana side, no MCGate-specific
     *  plugin required. Labels are kept low-cardinality (route index/host, backend address) -
     *  never per-player - since per-player labels would make the metric set grow without bound
     *  as players come and go. */
    private fun metricsText(s: MetricsSnapshot): String {
        val sb = StringBuilder()

        sb.append("# HELP mcgate_players_online Currently connected players.\n")
        sb.append("# TYPE mcgate_players_online gauge\n")
        sb.append("mcgate_players_online ").append(s.playersOnline).append('\n')

        // One series per literal hostname actually in use - unlike mcgate_route_backend_active_-
        // connections below (necessarily shared across every host alias of a route, since that's
        // tracked per backend address), this comes from live sessions' own `host` field, so hosts
        // sharing a route/backend are still distinguishable here.
        sb.append("# HELP mcgate_host_players_online Currently connected players per virtual host.\n")
        sb.append("# TYPE mcgate_host_players_online gauge\n")
        for ((host, count) in s.onlineByHost) {
            sb.append("mcgate_host_players_online{host=\"").append(escapeLabel(host)).append("\"} ").append(count).append('\n')
        }

        sb.append("# HELP mcgate_uptime_seconds Seconds since the JVM process started.\n")
        sb.append("# TYPE mcgate_uptime_seconds gauge\n")
        sb.append("mcgate_uptime_seconds ").append(s.uptimeSeconds).append('\n')

        sb.append("# HELP mcgate_route_backend_active_connections Active relayed connections per backend.\n")
        sb.append("# TYPE mcgate_route_backend_active_connections gauge\n")
        sb.append("# HELP mcgate_route_backend_latency_ms Last-observed backend connect/ping latency.\n")
        sb.append("# TYPE mcgate_route_backend_latency_ms gauge\n")
        for (b in s.backends) {
            // A single "hosts" label carrying every hostname joined together, rather than one
            // time series per host - emitting a separate series per host would multiply-count
            // the same active-connection value (it's tracked per backend/route, not per host),
            // which would silently inflate any Prometheus sum() across this metric.
            val hostsLabel = escapeLabel(b.hosts.joinToString(","))
            val labels = "route=\"${b.routeIndex}\",hosts=\"$hostsLabel\",backend=\"${escapeLabel(b.backend)}\""
            sb.append("mcgate_route_backend_active_connections{").append(labels).append("} ").append(b.active).append('\n')
            if (b.latencyMillis != null) {
                sb.append("mcgate_route_backend_latency_ms{").append(labels).append("} ").append(b.latencyMillis).append('\n')
            }
        }

        sb.append("# HELP mcgate_connection_tracking_enabled Whether connection-history tracking (SQLite) is enabled.\n")
        sb.append("# TYPE mcgate_connection_tracking_enabled gauge\n")
        sb.append("mcgate_connection_tracking_enabled ").append(if (s.trackingEnabled) 1 else 0).append('\n')

        if (s.trackingEnabled) {
            sb.append("# HELP mcgate_connection_tracking_queued_records Records waiting to be written to the tracking database.\n")
            sb.append("# TYPE mcgate_connection_tracking_queued_records gauge\n")
            sb.append("mcgate_connection_tracking_queued_records ").append(s.trackingQueuedRecords).append('\n')

            sb.append("# HELP mcgate_connection_tracking_dropped_records_total Records dropped because the write queue was full.\n")
            sb.append("# TYPE mcgate_connection_tracking_dropped_records_total counter\n")
            sb.append("mcgate_connection_tracking_dropped_records_total ").append(s.trackingDroppedRecordsTotal).append('\n')
        }

        return sb.toString()
    }

    /** Same data as [metricsText], as JSON - for `GET /metrics?type=json`, e.g. for a dashboard
     *  or script that would rather not parse Prometheus text format. Additionally includes the
     *  full per-player detail ([playerJson]) and a per-host player-count breakdown, neither of
     *  which [metricsText] emits as Prometheus series (per-player labels would be unbounded
     *  cardinality - see the doc there) but which cost nothing extra to include here since
     *  [collectMetrics] already gathered them. */
    private fun metricsJson(s: MetricsSnapshot): String {
        val sb = StringBuilder()
        sb.append("{\"playersOnline\":").append(s.playersOnline)
        sb.append(",\"onlineByHost\":{")
        s.onlineByHost.entries.forEachIndexed { i, (host, count) ->
            if (i > 0) sb.append(',')
            sb.append('"').append(escapeJson(host)).append("\":").append(count)
        }
        sb.append('}')
        sb.append(",\"uptimeSeconds\":").append(s.uptimeSeconds)
        sb.append(",\"backends\":[")
        s.backends.forEachIndexed { i, b ->
            if (i > 0) sb.append(',')
            sb.append("{\"route\":").append(b.routeIndex)
            sb.append(",\"hosts\":[")
            b.hosts.forEachIndexed { hi, h ->
                if (hi > 0) sb.append(',')
                sb.append('"').append(escapeJson(h)).append('"')
            }
            sb.append(']')
            sb.append(",\"backend\":\"").append(escapeJson(b.backend)).append('"')
            sb.append(",\"activeConnections\":").append(b.active)
            sb.append(",\"latencyMillis\":").append(b.latencyMillis ?: "null")
            sb.append('}')
        }
        sb.append("],\"players\":[")
        s.players.forEachIndexed { i, p ->
            if (i > 0) sb.append(',')
            sb.append(playerJson(p))
        }
        sb.append("]")
        sb.append(",\"connectionTracking\":{\"enabled\":").append(s.trackingEnabled)
        if (s.trackingEnabled) {
            sb.append(",\"queuedRecords\":").append(s.trackingQueuedRecords)
            sb.append(",\"droppedRecordsTotal\":").append(s.trackingDroppedRecordsTotal)
        }
        sb.append("}}")
        return sb.toString()
    }

    private fun respond(
        ctx: ChannelHandlerContext, req: FullHttpRequest, status: HttpResponseStatus, body: String,
        contentType: String = "application/json; charset=UTF-8"
    ) {
        val content = Unpooled.copiedBuffer(body, Charsets.UTF_8)
        val response = DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content)
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, contentType)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
        val keepAlive = io.netty.handler.codec.http.HttpUtil.isKeepAlive(req)
        val future = ctx.writeAndFlush(response)
        if (!keepAlive) future.addListener(ChannelFutureListener.CLOSE)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.warn("API request failed", cause)
        ctx.close()
    }
}

/** Prometheus label-value escaping: backslash and quote are escaped, newlines become `\n` -
 *  order matters (backslash first, or the newline replacement's own backslash gets re-escaped). */
private fun escapeLabel(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

private fun escapeJson(value: String): String {
    val sb = StringBuilder(value.length)
    for (c in value) {
        when (c) {
            '"' -> sb.append("\\\"")
            '\\' -> sb.append("\\\\")
            '\n' -> sb.append("\\n")
            else -> sb.append(c)
        }
    }
    return sb.toString()
}
