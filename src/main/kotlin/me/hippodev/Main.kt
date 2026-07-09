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

fun main(args: Array<String>) {
    println(BANNER)

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

    try {
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

        startConsole(channel)

        channel.closeFuture().sync()
        log.info("Stopped.")
    } finally {
        bossGroup.shutdownGracefully()
        workerGroup.shutdownGracefully()
    }
}

/** Reads console commands from stdin on a daemon thread; "stop" closes the listening channel to trigger shutdown. */
private fun startConsole(serverChannel: io.netty.channel.Channel) {
    val thread = Thread({
        val reader = System.`in`.bufferedReader()
        while (true) {
            val line = reader.readLine() ?: break
            when (line.trim().lowercase()) {
                "stop", "shutdown", "exit" -> {
                    log.info("Stop command received, shutting down...")
                    serverChannel.close()
                    return@Thread
                }
                "" -> {}
                else -> log.info("Unknown command: '{}' (try 'stop')", line.trim())
            }
        }
    }, "console")
    thread.isDaemon = true
    thread.start()
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
