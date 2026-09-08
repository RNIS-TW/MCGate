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
import me.hippodev.protocol.effectiveRemoteAddress
import me.hippodev.routing.*
import me.hippodev.tracking.ConnectionTracker
import me.hippodev.tracking.StatsLogger
import me.hippodev.udp.UdpProxy
import me.hippodev.voice.VoiceRelay
import me.hippodev.voice.VoiceRouting
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

/** Netty sizes its pooled allocator (arena count, and a per-event-loop-thread buffer cache) off
 *  `Runtime.availableProcessors()`. Inside a container that isn't cgroup-aware - or on a shared
 *  hosting node where the JVM sees every physical core of the box, not the slice we're sold - that
 *  number is huge, so Netty reserves dozens of 4 MiB direct-memory arenas plus a fat thread-local
 *  cache per worker before a single player connects. That, not per-connection buffering (the write
 *  water marks in the bootstrap below bound that to ~64 KiB/connection), is what makes RSS sit near
 *  the container limit with only a few dozen players online.
 *
 *  These properties shrink that fixed overhead to what a byte-relay proxy actually needs. Every one
 *  is a no-op if already set (via `-D...` on the command line), so an operator can still override.
 *  MUST run before the first Netty class initializes - Netty reads them once, in a static
 *  initializer. */
private fun tuneNettyMemoryFootprint() {
    fun default(key: String, value: String) {
        if (System.getProperty(key) == null) System.setProperty(key, value)
    }
    // A relay is I/O-bound and never holds many buffers at once - a handful of arenas is plenty,
    // vs. Netty's cores*2 default (64+ on a 32-core host node).
    default("io.netty.allocator.numHeapArenas", "2")
    default("io.netty.allocator.numDirectArenas", "2")
    // pageSize(8 KiB) << maxOrder = chunk size. 6 -> 512 KiB chunks instead of the 4 MiB default,
    // so an arena that's barely used still only pins 512 KiB.
    default("io.netty.allocator.maxOrder", "6")
    // Only give the (few) event-loop threads a buffer cache, and keep it small; don't grow one on
    // every incidental thread that ever touches a ByteBuf.
    default("io.netty.allocator.useCacheForAllThreads", "false")
    default("io.netty.allocator.smallCacheSize", "128")
    default("io.netty.allocator.normalCacheSize", "64")
    default("io.netty.allocator.cacheTrimIntervalMillis", "60000")
    // Bound the object-recycler pools (per thread) too - the relay churns few pooled objects.
    default("io.netty.recycler.maxCapacityPerThread", "256")
    // Hard ceiling on total direct memory Netty will hand out, independent of -XX:MaxDirectMemorySize.
    // 96 MiB is far above what 46 players * ~64 KiB/connection water-marked queues can reach; a
    // genuine overshoot fails that one write instead of letting the process get OOM-killed.
    default("io.netty.maxDirectMemory", (96L * 1024 * 1024).toString())
}

fun main(args: Array<String>) {
    tuneNettyMemoryFootprint()

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
    logNetworkInterfaces()

    val configPath = args.firstOrNull() ?: "config.yml"
    val messagesPath = args.getOrNull(1) ?: "messages.yml"

    val messagesRef = AtomicReference(MessagesLoader.loadOrCreateDefault(messagesPath))
    log.info("Loaded messages from {}", messagesPath)

    val initialConfig = ConfigLoader.loadOrCreateDefault(configPath, messagesRef.get())
    log.info("Loaded {} route(s) from {}", initialConfig.routes.size, configPath)
    ConnectionTracker.applyConfig(initialConfig.connectionTracking)

    val stateRef = AtomicReference(GateState(initialConfig))
    val pingCache = PingCache()

    // Captured by reference (reads stateRef fresh on every StatsLogger tick), not the config
    // value itself - so a snapshot always reflects whichever routes/runtimes are current at that
    // moment, even across a config reload that swaps stateRef out from under it.
    fun currentSnapshot() = collectMetrics(stateRef.get().config.routes) { route -> stateRef.get().routeRuntimes[route] }
    StatsLogger.applyConfig(initialConfig.statsLogging, ::currentSnapshot)

    // Shared by the file watchers below and the console `reload` command, so a manual reload
    // behaves identically to a file save - re-reads both files fresh from disk regardless of
    // which one triggered it, since a route can depend on either.
    fun reloadAll(reason: String) {
        try {
            val newMessages = GateMessages.load(messagesPath)
            messagesRef.set(newMessages)
            val newConfig = GateConfig.load(configPath, newMessages)
            stateRef.set(GateState(newConfig))
            ConnectionTracker.applyConfig(newConfig.connectionTracking)
            StatsLogger.applyConfig(newConfig.statsLogging, ::currentSnapshot)
            log.info("{}: reloaded {} route(s) from {} and messages from {}", reason, newConfig.routes.size, configPath, messagesPath)
        } catch (e: Exception) {
            log.error("{}: reload failed, keeping previous config/messages", reason, e)
        }
    }

    ConfigLoader.watch(configPath, { messagesRef.get() }) { newConfig ->
        log.info("Config changed, loaded {} route(s)", newConfig.routes.size)
        stateRef.set(GateState(newConfig))
        ConnectionTracker.applyConfig(newConfig.connectionTracking)
        StatsLogger.applyConfig(newConfig.statsLogging, ::currentSnapshot)
    }

    // Reload config.yml too so routes that don't override a message pick up the new default
    // immediately, without needing to touch config.yml themselves.
    MessagesLoader.watch(messagesPath) { newMessages ->
        messagesRef.set(newMessages)
        log.info("Messages changed, reloading {} with new defaults", configPath)
        val newConfig = GateConfig.load(configPath, newMessages)
        stateRef.set(GateState(newConfig))
        ConnectionTracker.applyConfig(newConfig.connectionTracking)
        StatsLogger.applyConfig(newConfig.statsLogging, ::currentSnapshot)
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
        // cores*2 for spread, but capped: on a shared hosting node availableProcessors() reports
        // the whole box (32+), and every extra NioEventLoop thread is another stack + its own
        // Netty buffer cache + arena affinity - pure memory overhead well past the point where a
        // byte-relay handling a few hundred players needs more parallelism. 12 is plenty; an
        // operator who genuinely wants more sets `workerThreads` explicitly in config.yml.
        maxOf(4, Runtime.getRuntime().availableProcessors() * 2).coerceAtMost(12)
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
            // Explicit (not relying on Netty's version-dependent default) write water marks, the
            // trip points the relay's flow control keys off (see FlowControl.kt): once a client's
            // unsent-and-queued bytes pass 64 KiB, isWritable flips false and reads on its backend
            // pause until it drains back under 32 KiB. This is what bounds per-connection memory -
            // without a listener honoring it (which the relay handlers now are) a slow peer's
            // queue, and with it Netty's pooled-direct arenas, grow without limit.
            .childOption(
                ChannelOption.WRITE_BUFFER_WATER_MARK,
                io.netty.channel.WriteBufferWaterMark(32 * 1024, 64 * 1024)
            )
            // Some hosting providers put a NAT/firewall/load balancer in front of the box that
            // tracks raw TCP activity rather than Minecraft-protocol keepalives, and silently
            // drops a connection it decides looks idle (an AFK player shows up client-side as a
            // generic "connection interrupted" kick, with nothing in MCGate's own log explaining
            // why - the OS just reports the socket as reset/closed). SO_KEEPALIVE makes the OS
            // send its own periodic TCP-level probes, which is exactly what such middleboxes look
            // for to keep the mapping alive, independent of whatever the Minecraft protocol itself
            // is or isn't sending at the time.
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .applyTunedKeepalive()
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    val pipeline = ch.pipeline()
                    val cfg = stateRef.get().config
                    // Front of the pipeline: bound the pre-login phase (login deadline + per-IP
                    // concurrent cap) so a connection-flood / slow-loris can't tie up channels/fds
                    // or reach the backend. Per-IP capping is skipped under proxyProtocol - every
                    // connection would otherwise look like it came from the upstream load balancer.
                    val perIpLimit = if (cfg.proxyProtocol) 0 else cfg.maxConnectionsPerIp
                    if (cfg.loginTimeoutMillis > 0 || perIpLimit > 0) {
                        pipeline.addLast(ConnectionGuardHandler(cfg.loginTimeoutMillis, perIpLimit))
                    }
                    if (cfg.proxyProtocol) {
                        pipeline.addLast(io.netty.handler.codec.haproxy.HAProxyMessageDecoder())
                        pipeline.addLast(ProxyProtocolAttributeHandler())
                    }
                    pipeline.addLast(HandshakeSniffer { ctx, protocolVersion, host, port, nextState, rawFrame ->
                        dispatch(ctx, stateRef.get(), pingCache, protocolVersion, host, port, nextState, rawFrame)
                    })
                }
            })

        val channel = bootstrap.bind(initialConfig.bindAddress).sync().channel()
        log.info("Listening on {}", initialConfig.bindAddress)

        // Shares the TCP listener's own worker group rather than spinning up a separate one -
        // relaying UDP voice traffic is just as I/O-bound as relaying player TCP bytes, and it
        // binds to the very same address/port (see VoiceRelay's doc for why that needs a NAT-like
        // per-client socket rather than one shared backend-facing channel). Only bound at all if
        // some route actually configures a `voicechat:` backend - otherwise it'd permanently claim
        // that UDP port for a feature nobody's using, which conflicts with e.g. a same-port entry
        // in `udpProxy:` below. Like the rest of this section, this is decided once at startup from
        // initialConfig, not re-evaluated on a hot config reload.
        val voiceRelay = VoiceRelay(workerGroup)
        val hasVoicechatRoutes = initialConfig.routes.any { it.voicechatTemplates.isNotEmpty() }
        if (hasVoicechatRoutes) {
            voiceRelay.start(initialConfig.bindAddress)
        } else {
            log.info("No routes configure a voicechat backend, skipping voicechat UDP relay")
        }

        // Standalone static UDP forwards (config.yml's top-level `udpProxy:` list) - unrelated
        // to voice chat's login-gated routing, each with its own bind address/port. Not
        // reconfigured on a hot config reload (same as the main TCP bind/voice relay): changing
        // bind addresses needs a restart, only route/messages content is hot-reloadable.
        val udpProxies = initialConfig.udpProxies.map { UdpProxy(workerGroup, it) }
        udpProxies.forEach { it.start() }

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
                voiceRelay.stop()
                udpProxies.forEach { it.stop() }
                bossGroup.shutdownGracefully().sync()
                workerGroup.shutdownGracefully().sync()
                ConnectionTracker.shutdown()
                StatsLogger.shutdown()
                log.info("Stopped.")
                // Everything above is our own graceful cleanup and has already completed by this
                // point - this is only to force the process to actually exit afterward. The JVM
                // only exits on its own once every non-daemon thread has finished, and JLine's
                // system terminal (see startConsole) can leave behind a background input-reader
                // thread that isn't always daemon and is never explicitly closed - without this,
                // that stray thread silently keeps the process alive forever after "Stopped." is
                // logged (visible as a process manager never seeing the JVM actually exit, even
                // though shutdown clearly finished). halt() (not exit()) is deliberate: it skips
                // running shutdown hooks/finalizers again, which matters here since this can
                // itself be running from inside the shutdown hook below - calling exit() there
                // would just be a no-op instead of actually terminating the process.
                Runtime.getRuntime().halt(0)
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
                    "transfer" -> {
                        val rest = line.trim().substringAfter(' ', "").trim()
                        val name = rest.substringBefore(' ')
                        val target = rest.substringAfter(' ', "").trim()
                        when {
                            name.isEmpty() || target.isEmpty() -> log.info("Usage: transfer <player> <host:port>")
                            else -> {
                                val addr = try {
                                    parseHostPort(target)
                                } catch (e: Exception) {
                                    log.info("Invalid host:port '{}'", target)
                                    null
                                }
                                if (addr != null) {
                                    val session = PlayerSessions.findByName(name)
                                    if (session == null) {
                                        log.info("No player named '{}' is connected.", name)
                                    } else {
                                        val error = PlayerSessions.transfer(session.uuid, addr.hostString, addr.port)
                                        if (error == null) {
                                            log.info("Transferring '{}' to {}.", session.name, target)
                                        } else {
                                            log.info("Can't transfer '{}': {}", session.name, error)
                                        }
                                    }
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
    log.info("  transfer <player> <host:port> - send a player directly to another Minecraft server")
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

/** Logs every up network interface and its addresses at startup - mainly useful on multi-homed
 *  hosts (multiple NICs/public IPs, e.g. separate TCP and UDP egress paths) to see at a glance
 *  which local address an outbound connection would actually use, since that's otherwise picked
 *  silently by the OS's routing table and isn't visible anywhere else in MCGate's own logging. */
private fun logNetworkInterfaces() {
    try {
        val interfaces = java.net.NetworkInterface.getNetworkInterfaces()?.toList() ?: emptyList()
        for (iface in interfaces) {
            if (!iface.isUp) continue
            val addrs = iface.inetAddresses.toList().map { it.hostAddress }
            if (addrs.isEmpty()) continue
            val flags = buildList {
                if (iface.isLoopback) add("loopback")
                if (iface.isVirtual) add("virtual")
                if (iface.isPointToPoint) add("point-to-point")
            }.joinToString(", ")
            log.info("Network interface '{}'{}: {}", iface.displayName, if (flags.isEmpty()) "" else " ($flags)", addrs.joinToString(", "))
        }
    } catch (e: Exception) {
        log.warn("Failed to enumerate network interfaces: {}", e.toString())
    }
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
        log.info("Connection: host='{}' from {} ({}, protocol {})", host, ctx.channel().effectiveRemoteAddress(), kind, protocolVersion)
    }

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
        log.info("No route for host '{}', closing connection from {}", host, ctx.channel().effectiveRemoteAddress())
        rawFrame.release()
        ctx.close()
        return
    }

    val resolvedRoute = route
    val runtime = state.routeRuntimes.computeIfAbsent(resolvedRoute) { RouteRuntime() }

    val handlerName = ctx.name()
    when (nextState) {
        1 -> {
            rawFrame.release()
            // Status needs its backend list now (to dial for the MOTD). A login connection's is
            // resolved lazily inside LoginRelayHandler instead - only once the client sends Login
            // Start - so a connection flood to random subdomains on a wildcard route can't force a
            // blocking DNS lookup per bot on the event loop.
            val backends = try {
                resolvedRoute.resolveBackends(captures)
            } catch (e: Exception) {
                // Config-driven (regex, DNS, param substitution) - a bad edge case here must only
                // drop this one connection, never take the rest of the server down.
                log.warn("Failed to resolve backends for host '{}' from {}: {}", host, ctx.channel().effectiveRemoteAddress(), e.toString())
                ctx.close()
                return
            }
            ctx.pipeline().addAfter(
                handlerName, "status",
                StatusHandler(resolvedRoute, runtime, backends, protocolVersion, host, port, pingCache)
            )
        }
        else -> {
            registerVoicechatRoute(ctx, resolvedRoute, captures, host)
            val relay = LoginRelayHandler(resolvedRoute, runtime, captures, protocolVersion, host, port, rawFrame)
            ctx.pipeline().addAfter(handlerName, "relay", relay)
            relay.start(ctx)
        }
    }
}

/** Records this connecting client's IP -> voicechat backend mapping (see [VoiceRouting]) if
 *  [route] has a `voicechat:` backend configured, so [VoiceRelay] can relay this player's UDP
 *  voice traffic once it starts arriving. UDP carries no hostname to route by, so this is the
 *  only point where that association can be made - resolution/param-substitution failures here
 *  must only skip voice routing for this connection, never break the player's actual TCP login. */
private fun registerVoicechatRoute(ctx: ChannelHandlerContext, route: Route, captures: List<String>, host: String) {
    if (route.voicechatTemplates.isEmpty()) return
    try {
        val voiceBackend = route.resolveVoicechat(captures) ?: return
        val clientAddr = ctx.channel().effectiveRemoteAddress() as? java.net.InetSocketAddress ?: return
        VoiceRouting.register(clientAddr.address.hostAddress, voiceBackend)
    } catch (e: Exception) {
        log.warn("Failed to resolve voicechat backend for host '{}': {}", host, e.toString())
    }
}
