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

// Must run before the first Logger is created (see archivePreviousLog's and
// installColorConsole's doc).
private val colorConsoleInstalled = run { archivePreviousLog(); installColorConsole(); true }
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
    // Netty already isolates most per-connection exceptions to that one channel (see the
    // exceptionCaught overrides in the handler package), but this is the last-resort net for
    // anything that still slips through uncaught on any thread - console, watcher, scheduled
    // reconnect tasks, DNS resolver pool, etc. Without it, an uncaught exception on a thread just
    // silently kills that thread (console stops responding, a player's keepalive loop goes dead)
    // with nothing in the logs to explain why.
    Thread.setDefaultUncaughtExceptionHandler { thread, cause ->
        log.error("Uncaught exception on thread '{}'", thread.name, cause)
    }

    val startedAt = System.currentTimeMillis()
    println(BANNER)
    log.info("MCGate v{}", version)

    val configPath = args.firstOrNull() ?: "config.yml"
    val messagesPath = args.getOrNull(1) ?: "messages.yml"

    val messagesRef = AtomicReference(MessagesLoader.loadOrCreateDefault(messagesPath))
    log.info("Loaded messages from {}", messagesPath)

    val initialConfig = ConfigLoader.loadOrCreateDefault(configPath, messagesRef.get())
    log.info("Loaded {} route(s) from {}", initialConfig.routes.size, configPath)

    val stateRef = AtomicReference(GateState(initialConfig))
    val pingCache = PingCache()

    // Shared by the file watchers below and the console `reload` command, so a manual reload
    // behaves identically to a file save - re-reads both files fresh from disk regardless of
    // which one triggered it, since a route can depend on either.
    fun reloadAll(reason: String) {
        try {
            val newMessages = GateMessages.load(messagesPath)
            messagesRef.set(newMessages)
            val newConfig = GateConfig.load(configPath, newMessages)
            stateRef.set(GateState(newConfig))
            log.info("{}: reloaded {} route(s) from {} and messages from {}", reason, newConfig.routes.size, configPath, messagesPath)
        } catch (e: Exception) {
            log.error("{}: reload failed, keeping previous config/messages", reason, e)
        }
    }

    ConfigLoader.watch(configPath, { messagesRef.get() }) { newConfig ->
        log.info("Config changed, loaded {} route(s)", newConfig.routes.size)
        stateRef.set(GateState(newConfig))
    }

    // Reload config.yml too so routes that don't override a message pick up the new default
    // immediately, without needing to touch config.yml themselves.
    MessagesLoader.watch(messagesPath) { newMessages ->
        messagesRef.set(newMessages)
        log.info("Messages changed, reloading {} with new defaults", configPath)
        val newConfig = GateConfig.load(configPath, newMessages)
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

    // MCGate is I/O-bound (relaying bytes, not crunching them), so more worker threads than CPU
    // cores is fine - it just spreads player channels thinner across threads. That matters
    // because Netty pins each channel to one worker thread for its whole connection: with too
    // few threads (Netty's own default is just 2x cores - as low as 2 on a small VPS), players
    // split into large groups per thread, and anything that stalls one thread stalls every
    // player pinned to it while the rest are unaffected. See GateConfig.workerThreads.
    val workerThreadCount = if (initialConfig.workerThreads > 0) {
        initialConfig.workerThreads
    } else {
        maxOf(4, Runtime.getRuntime().availableProcessors() * 2)
    }
    val bossGroup = NioEventLoopGroup(1)
    val workerGroup = NioEventLoopGroup(workerThreadCount)
    log.info("Using {} worker thread(s)", workerThreadCount)

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

        startConsole(channel, stateRef, startedAt, ::reloadAll)

        channel.closeFuture().sync()
        shutdown()
    }
}

/** Reads console commands from stdin on a daemon thread. Uses JLine for line editing/history
 *  (up/down arrows recall previous commands) - a plain BufferedReader has none of that. Falls
 *  back to a plain reader if JLine can't attach to a real terminal (e.g. output piped/redirected,
 *  or running under some process managers), since JLine throws in that case.
 *
 *  Some hosting-panel wrappers (Pterodactyl-style Docker setups, etc.) allocate a pseudo-terminal
 *  for the process even though the panel's actual console view just scrapes raw stdout rather
 *  than rendering a real terminal - JLine happily detects a "real" terminal there and redraws the
 *  prompt with cursor-movement escape codes the log viewer can't interpret, showing up as garbage
 *  like `>....` around every log line. Since that can't be detected reliably, set
 *  MCGATE_PLAIN_CONSOLE=true to force the plain fallback path unconditionally. */
private fun startConsole(
    serverChannel: io.netty.channel.Channel,
    stateRef: AtomicReference<GateState>,
    startedAt: Long,
    reload: (reason: String) -> Unit
) {
    val lineReader: org.jline.reader.LineReader? = if (System.getenv("MCGATE_PLAIN_CONSOLE") == "true") {
        null
    } else {
        try {
            val terminal = org.jline.terminal.TerminalBuilder.builder().system(true).build()
            org.jline.reader.LineReaderBuilder.builder().terminal(terminal).build()
        } catch (e: Exception) {
            log.debug("JLine unavailable, falling back to plain console input: {}", e.message)
            null
        }
    }
    // Lets the async console writer (see ConsoleColors.kt) route log lines through
    // LineReader.printAbove instead of a raw println, so a log arriving mid-command doesn't
    // corrupt the in-progress prompt/input line.
    activeLineReader = lineReader
    val plainReader = if (lineReader == null) System.`in`.bufferedReader() else null
    var ctrlCCount = 0

    val thread = Thread({
        while (true) {
            val line = try {
                lineReader?.readLine("> ") ?: plainReader?.readLine() ?: break
            } catch (e: org.jline.reader.EndOfFileException) {
                break
            } catch (e: org.jline.reader.UserInterruptException) {
                ctrlCCount++
                if (ctrlCCount == 1) {
                    log.info("Ctrl+C received, shutting down... press Ctrl+C again to force quit")
                    serverChannel.close()
                    // Deliberately keep looping (not return@Thread) instead of stopping right
                    // away - shutdown can take a moment (backend channels draining, event loops
                    // stopping), and this needs to still be reading to catch a second Ctrl+C
                    // during that window.
                    continue
                } else {
                    log.warn("Second Ctrl+C received, forcing immediate shutdown")
                    Runtime.getRuntime().halt(1)
                }
                return@Thread
            }
            val command = line.trim().substringBefore(' ').lowercase()
            try {
                when (command) {
                    "stop", "shutdown", "exit" -> {
                        log.info("Stop command received, shutting down...")
                        serverChannel.close()
                        return@Thread
                    }
                    "help", "?" -> printHelp()
                    "players", "list", "playerlist" -> printPlayers(stateRef.get())
                    "routes" -> printRoutes(stateRef.get())
                    "version" -> log.info("MCGate v{}", version)
                    "reload" -> reload("Manual reload")
                    "uptime" -> log.info("Uptime: {}", formatDuration(System.currentTimeMillis() - startedAt))
                    "kick" -> {
                        val rest = line.trim().substringAfter(' ', "").trim()
                        val name = rest.substringBefore(' ')
                        val message = rest.substringAfter(' ', "").trim().ifEmpty { null }
                        when {
                            name.isEmpty() -> log.info("Usage: kick <player> [message]")
                            else -> {
                                val session = PlayerSessions.findByName(name)
                                if (session == null) {
                                    log.info("No player named '{}' is connected.", name)
                                } else {
                                    PlayerSessions.kick(session.uuid, message)
                                    log.info("Kicked '{}'.", session.name)
                                }
                            }
                        }
                    }
                    "whois" -> {
                        val name = line.trim().substringAfter(' ', "").trim()
                        when {
                            name.isEmpty() -> log.info("Usage: whois <player>")
                            else -> printWhois(stateRef.get(), name)
                        }
                    }
                    "" -> {}
                    else -> log.info("Unknown command: '{}' (try 'help')", line.trim())
                }
            } catch (e: Exception) {
                // A bad command shouldn't take down the console thread - that would leave the
                // server running with no way to issue `stop` short of SIGTERM/Ctrl+C.
                log.error("Command '{}' failed", command, e)
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
    log.info("  whois <player>           - show full session detail for one player")
    log.info("  kick <player> [message]  - disconnect a player, optionally with a message")
    log.info("  routes                   - list configured routes and backend status")
    log.info("  reload                   - re-read config.yml and messages.yml now")
    log.info("  uptime                   - show how long MCGate has been running")
    log.info("  version                  - show the running MCGate version")
    log.info("  stop, shutdown, exit     - stop the server (press twice to force-kill)")
}

/** "backend dial latency" for [addr] on whichever route currently owns [host] - see the caveat
 *  in [printPlayers]: not the player's real in-game ping, since MCGate can't see that once it's
 *  relaying raw bytes past login. */
private fun backendLatency(state: GateState, host: String, addr: java.net.InetSocketAddress?): Long? {
    if (addr == null) return null
    val route = state.config.routes.firstOrNull { it.match(host) != null } ?: return null
    return state.routeRuntimes[route]?.latencyOf(addr)
}

private fun formatDuration(millis: Long): String {
    val totalSeconds = millis / 1000
    val h = totalSeconds / 3600
    val m = (totalSeconds % 3600) / 60
    val s = totalSeconds % 60
    return when {
        h > 0 -> "${h}h ${m}m ${s}s"
        m > 0 -> "${m}m ${s}s"
        else -> "${s}s"
    }
}

private fun printWhois(state: GateState, name: String) {
    val s = PlayerSessions.findByName(name)
    if (s == null) {
        log.info("No player named '{}' is connected.", name)
        return
    }
    val now = System.currentTimeMillis()
    val ping = backendLatency(state, s.host, s.backend)?.let { "${it}ms" } ?: "n/a"
    log.info("Player: {} ({})", s.name, s.uuid)
    log.info("  From:      {}", s.remoteAddress)
    log.info("  Host:      {}", s.host)
    log.info("  Status:    {}", s.backend?.toString() ?: "waiting to reconnect")
    log.info("  Ping:      {}", ping)
    log.info("  Connected: {} ago", formatDuration(now - s.connectedAt))
}

private fun printPlayers(state: GateState) {
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
        // MCGate relays raw bytes past login, so it can't see the client's real in-game ping
        // (that RTT is negotiated directly between client and backend). This is the backend
        // dial/connect latency recorded when this session's relay connection was opened - the
        // same stat the `routes` command shows per backend.
        val ping = backendLatency(state, s.host, s.backend)?.let { "${it}ms" } ?: "n/a"
        log.info("  {} ({}) from {} on '{}' -> {} [{}s] ping={}", s.name, s.uuid, s.remoteAddress, s.host, status, connectedSec, ping)
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
    if (state.config.logConnections) {
        val kind = if (nextState == 1) "status" else "login"
        log.info("Connection: host='{}' from {} ({}, protocol {})", host, ctx.channel().remoteAddress(), kind, protocolVersion)
    }

    var route: Route? = null
    var captures: List<String> = emptyList()
    val backends: List<java.net.InetSocketAddress>
    try {
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

        backends = route.resolveBackends(captures)
    } catch (e: Exception) {
        // Route matching/backend resolution is config-driven (regex, DNS, param substitution) -
        // a bug or bad edge case here must only drop this one connection, never take the rest of
        // the server down with it.
        log.warn("Failed to route connection for host '{}' from {}: {}", host, ctx.channel().remoteAddress(), e.toString())
        rawFrame.release()
        ctx.close()
        return
    }

    val resolvedRoute = route!! // non-null: the try block above returns before here otherwise
    val runtime = state.routeRuntimes.computeIfAbsent(resolvedRoute) { RouteRuntime() }

    val handlerName = ctx.name()
    when (nextState) {
        1 -> {
            rawFrame.release()
            ctx.pipeline().addAfter(
                handlerName, "status",
                StatusHandler(resolvedRoute, runtime, backends, protocolVersion, host, port, pingCache)
            )
        }
        else -> {
            val relay = LoginRelayHandler(resolvedRoute, runtime, backends, protocolVersion, host, port, rawFrame)
            ctx.pipeline().addAfter(handlerName, "relay", relay)
            relay.start(ctx)
        }
    }
}
