package me.hippodev

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import me.hippodev.api.ApiServer
import me.hippodev.config.*
import me.hippodev.handler.*
import me.hippodev.routing.*
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

private val log = LoggerFactory.getLogger("MCGate")

private const val BANNER = """
  __  __  ____  ____       _
 |  \/  |/ ___|/ ___| __ _| |_ ___
 | |\/| | |   | |  _ / _` | __/ _ \
 | |  | | |___| |_| | (_| | ||  __/
 |_|  |_|\____|\____|\__,_|\__\___|
"""

/** Everything that gets swapped together on a config reload. */
private class GateState(val config: GateConfig) {
    val routeRuntimes = ConcurrentHashMap<Route, RouteRuntime>()
}

/** Falls back to "dev" when run outside a packaged jar (e.g. from an IDE run config or
 *  `mvn exec`), since there's no manifest for [Package.implementationVersion] to read. */
private val version: String =
    object {}.javaClass.`package`.implementationVersion ?: "dev"

fun main(args: Array<String>) {
    val startedAt = System.currentTimeMillis()
    println(BANNER)
    log.info("MCGate v{}", version)

    val configPath = args.firstOrNull() ?: "config.yml"
    val initialConfig = ConfigLoader.loadOrCreateDefault(configPath)
    log.info("Loaded {} route(s) from {}", initialConfig.routes.size, configPath)

    val stateRef = AtomicReference(GateState(initialConfig))
    val pingCache = PingCache()

    ConfigLoader.watch(configPath) { newConfig ->
        log.info("Config changed, loaded {} route(s)", newConfig.routes.size)
        stateRef.set(GateState(newConfig))
    }

    if (initialConfig.api.enabled) {
        val apiServer = ApiServer(
            configSupplier = { stateRef.get().config },
            runtimeSupplier = { route -> stateRef.get().routeRuntimes[route] }
        )
        apiServer.start(initialConfig.api.bindAddress)
        Runtime.getRuntime().addShutdownHook(Thread { apiServer.shutdown() })
    }

    val bossGroup = NioEventLoopGroup(1)
    val workerGroup = NioEventLoopGroup()

    run {
        val bootstrap = ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel::class.java)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    val pipeline = ch.pipeline()
                    pipeline.addLast(HandshakeSniffer { ctx, protocolVersion, host, port, nextState, rawFrame ->
                        dispatch(ctx, stateRef.get(), pingCache, protocolVersion, host, port, nextState, rawFrame)
                    })
                }
            })

        val channel = bootstrap.bind(initialConfig.bindAddress).sync().channel()
        log.info("Listening on {}", initialConfig.bindAddress)

        val startupSeconds = (System.currentTimeMillis() - startedAt) / 1000.0
        log.info("Done (%.3fs)! For help, type \"help\"".format(startupSeconds))

        // Both the console "stop" path and a Ctrl+C (SIGINT) / `kill` (SIGTERM) shutdown hook
        // can trigger this. The JVM halts as soon as every shutdown hook returns, so the hook
        // must do the full close-and-shutdown sequence itself rather than just nudging the
        // channel and hoping main() finishes in time. The AtomicBoolean guard keeps the two
        // paths from racing to shut down the same event loop groups twice, which previously
        // threw RejectedExecutionException when one path's close() ran after the other had
        // already terminated the executor.
        val stopped = AtomicBoolean(false)
        val shutdown = {
            if (stopped.compareAndSet(false, true)) {
                log.info("Shutdown signal received, stopping...")
                channel.close().sync()
                bossGroup.shutdownGracefully().sync()
                workerGroup.shutdownGracefully().sync()
                log.info("Stopped.")
            }
        }
        Runtime.getRuntime().addShutdownHook(Thread(shutdown, "shutdown"))

        startConsole(channel, stateRef)

        channel.closeFuture().sync()
        shutdown()
    }
}

/** Reads console commands from stdin on a daemon thread. */
private fun startConsole(serverChannel: io.netty.channel.Channel, stateRef: AtomicReference<GateState>) {
    val thread = Thread({
        val reader = System.`in`.bufferedReader()
        while (true) {
            val line = reader.readLine() ?: break
            val command = line.trim().substringBefore(' ').lowercase()
            when (command) {
                "stop", "shutdown", "exit" -> {
                    log.info("Stop command received, shutting down...")
                    serverChannel.close()
                    return@Thread
                }
                "help", "?" -> printHelp()
                "players", "list", "playerlist" -> printPlayers()
                "routes" -> printRoutes(stateRef.get())
                "version" -> log.info("MCGate v{}", version)
                "" -> {}
                else -> log.info("Unknown command: '{}' (try 'help')", line.trim())
            }
        }
    }, "console")
    thread.isDaemon = true
    thread.start()
}

private fun printHelp() {
    log.info("Commands:")
    log.info("  help                     - show this list")
    log.info("  players, list, playerlist - list connected players")
    log.info("  routes                   - list configured routes and backend status")
    log.info("  version                  - show the running MCGate version")
    log.info("  stop, shutdown, exit     - stop the server")
}

private fun printPlayers() {
    val sessions = PlayerSessions.all()
    if (sessions.isEmpty()) {
        log.info("No players connected.")
        return
    }
    val now = System.currentTimeMillis()
    log.info("{} player(s) connected:", sessions.size)
    for (s in sessions) {
        val connectedSec = (now - s.connectedAt) / 1000
        val status = s.backend?.toString() ?: "waiting to reconnect"
        log.info("  {} ({}) from {} on '{}' -> {} [{}s]", s.name, s.uuid, s.remoteAddress, s.host, status, connectedSec)
    }
}

private fun printRoutes(state: GateState) {
    if (state.config.routes.isEmpty()) {
        log.info("No routes configured.")
        return
    }
    state.config.routes.forEachIndexed { index, route ->
        val hosts = route.hostPatterns.joinToString(", ") { it.raw }
        val runtime = state.routeRuntimes[route]
        log.info("[{}] {} (priority {})", index, hosts, route.priority)
        val resolved = if (route.hostPatterns.all { it.wildcardCount == 0 }) {
            try { route.resolveBackends(emptyList()) } catch (e: Exception) { null }
        } else null
        route.backendTemplates.forEachIndexed { i, template ->
            val addr = resolved?.getOrNull(i)
            val active = if (addr == null) "?" else runtime?.activeConnections?.get(addr)?.get() ?: 0
            val latency = (addr?.let { runtime?.latencyOf(it) })?.let { "${it}ms" } ?: "n/a"
            log.info("      -> {} (active={}, latency={})", template, active, latency)
        }
    }
}

private fun dispatch(
    ctx: ChannelHandlerContext,
    state: GateState,
    pingCache: PingCache,
    protocolVersion: Int,
    host: String,
    port: Int,
    nextState: Int,
    rawFrame: ByteBuf
) {
    var route: Route? = null
    var captures: List<String> = emptyList()
    for (r in state.config.routes) {
        val m = r.match(host)
        if (m != null) {
            route = r
            captures = m
            break
        }
    }

    if (route == null) {
        log.info("No route for host '{}', closing connection from {}", host, ctx.channel().remoteAddress())
        rawFrame.release()
        ctx.close()
        return
    }

    val backends = route.resolveBackends(captures)
    val runtime = state.routeRuntimes.computeIfAbsent(route) { RouteRuntime() }

    val handlerName = ctx.name()
    when (nextState) {
        1 -> {
            rawFrame.release()
            ctx.pipeline().addAfter(
                handlerName, "status",
                StatusHandler(route, runtime, backends, protocolVersion, host, port, pingCache)
            )
        }
        else -> {
            val relay = LoginRelayHandler(route, runtime, backends, protocolVersion, host, port, rawFrame)
            ctx.pipeline().addAfter(handlerName, "relay", relay)
            relay.start(ctx)
        }
    }
}
