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
import me.hippodev.routing.RouteRuntime
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
            segments == listOf("v1", "routes") ->
                respond(ctx, req, HttpResponseStatus.OK, routesJson(routes))

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

    private fun respond(ctx: ChannelHandlerContext, req: FullHttpRequest, status: HttpResponseStatus, body: String) {
        val content = Unpooled.copiedBuffer(body, Charsets.UTF_8)
        val response = DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content)
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8")
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
