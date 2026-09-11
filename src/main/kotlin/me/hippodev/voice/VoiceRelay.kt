package me.hippodev.voice

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import me.hippodev.protocol.parseProxyProtocolHeader
import me.hippodev.udp.UdpThrottle
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** How often the reaper runs. Short so the no-reply teardown ([UdpThrottle.noReplyTeardownMillis])
 *  actually catches a spoofed-source flood promptly, rather than the sessions lingering for the
 *  full idle window. All the limits it enforces come from the hot-reloadable [UdpThrottle]. */
private const val REAPER_INTERVAL_MILLIS = 15_000L

/**
 * Relays UDP traffic (Simple Voice Chat and similar mods) on the same port MCGate's Minecraft TCP
 * listener binds - no extra port to open. UDP carries no hostname the way the Minecraft handshake
 * does, so which backend a datagram belongs to is looked up by the sender's IP via [VoiceRouting],
 * populated from the TCP side when a route with a `voicechat:` backend is resolved for a client.
 *
 * When [expectProxyProtocol] is set (the global `proxyProtocol` config - MCGate sits behind an L4
 * proxy such as Cloudflare Spectrum that also fronts this UDP port), the fronting proxy prepends a
 * PROXY protocol (v1/v2) header to the **first** datagram of each origin-facing flow (Cloudflare
 * Spectrum, notably, does not repeat it on every packet). That header is stripped and the real
 * client address it reports is used for the [VoiceRouting] lookup and as the session key -
 * otherwise the datagram would look like it came from the fronting proxy's own IP and never match
 * a routing entry. Subsequent header-less datagrams on the same flow are matched back to that
 * session by their source address ([byVia]). Backend replies go to the datagram's actual sender
 * (the fronting proxy), which de-muxes them to the real client - see [Session.via].
 *
 * Acts as a small NAT: each distinct client gets its own ephemeral backend-facing UDP socket (see
 * [Session.backendChannel]), so the backend sees every player as a distinct source address/port,
 * same as if MCGate weren't in the middle. Funnelling every client through one shared
 * backend-facing socket would make the backend unable to tell players apart at all.
 */
class VoiceRelay(
    private val group: EventLoopGroup,
    private val expectProxyProtocol: Boolean = false
) {
    private val log = LoggerFactory.getLogger(VoiceRelay::class.java)

    /** [backendChannel] starts null and is filled in once the async `connect()` in
     *  [openBackendChannel] completes; packets arriving in the meantime queue up in [pending]
     *  instead of being dropped. Every field is only ever touched under `synchronized(this)` -
     *  packets can arrive on the shared public channel's event loop while the connect completion
     *  runs on the new backend channel's own (different) event loop, so this genuinely races,
     *  unlike most other per-connection state in this codebase which is pinned to one thread. */
    /** [via] is where backend replies are sent: the datagram's actual source, which is the
     *  fronting L4 proxy's address when [expectProxyProtocol] is on (it de-muxes replies back to
     *  the real client) and the client's own address otherwise. Refreshed on every inbound
     *  datagram in case the fronting proxy rotates its source port mid-session. */
    private class Session(
        val clientIp: String,
        @Volatile var lastActive: Long,
        @Volatile var via: InetSocketAddress
    ) {
        val createdAt = lastActive
        var backendChannel: Channel? = null
        val pending = ArrayDeque<ByteBuf>()
        /** Set once this session is torn down (idle-evicted, client logged out, or its backend
         *  connect failed). A datagram that raced in on the public event loop after the teardown
         *  began must be released, not re-queued into a [pending] nothing will ever drain. */
        var dead = false
        /** Set on the first reply seen from the backend - lets [openBackendChannel] log once
         *  whether the backend ever actually answers at all, distinct from repeated per-packet
         *  logging. */
        @Volatile var repliedOnce = false
    }

    /** Keyed by the *client* address: the datagram's own source normally, or the address from the
     *  PROXY header under [expectProxyProtocol]. This is the key [disconnectClient] matches on. */
    private val sessions = ConcurrentHashMap<InetSocketAddress, Session>()

    /** [expectProxyProtocol] only: sessions indexed by their current [Session.via] (the fronting
     *  proxy's source address), so a header-less datagram - every datagram after the first, with
     *  Cloudflare Spectrum - can still be matched to the session its flow's first (headered)
     *  datagram established. Kept in lockstep with [Session.via]. */
    private val byVia = ConcurrentHashMap<InetSocketAddress, Session>()

    /** Concurrent session count per client IP, for [UdpThrottle.maxSessionsPerIp]. Entries
     *  self-remove at zero. */
    private val sessionsPerIp = ConcurrentHashMap<String, AtomicInteger>()

    private var publicChannel: Channel? = null

    /** Logs the "datagram arrived but couldn't be relayed" diagnostics below at DEBUG (unmatched
     *  inbound UDP - disconnected clients still blasting, non-voicechat routes, port scans - is
     *  routine and noisy), rate-limited to at most one line every 5s per reason so a mis-set
     *  voice_host / proxyProtocol retrying ~1/s can't flood even debug logs. Just enough to confirm
     *  from the MCGate side whether voice datagrams are reaching it, and why they're being dropped. */
    @Volatile private var lastDropLogAt = 0L
    private fun logDrop(reason: String, from: Any) {
        val now = System.currentTimeMillis()
        if (now - lastDropLogAt < 5_000L) return
        lastDropLogAt = now
        log.debug("Voicechat datagram from {} dropped: {}", from, reason)
    }
    private val reaper = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "voice-relay-reaper").apply { isDaemon = true }
    }

    fun start(bindAddress: InetSocketAddress) {
        val bootstrap = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    handleClientPacket(packet)
                }

                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    // The shared public channel is unconnected (bound, not dialed), so this is
                    // never a per-client failure - just a defensive net so a stray error here
                    // can't take the whole relay down.
                    log.debug("Voicechat public channel error", cause)
                }
            })

        publicChannel = bootstrap.bind(bindAddress).sync().channel()
        log.info("Relaying UDP (voicechat) on {}", bindAddress)

        reaper.scheduleAtFixedRate({ evictStaleSessions() }, REAPER_INTERVAL_MILLIS, REAPER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        VoiceRouting.attachRelay(this)
    }

    fun stop() {
        VoiceRouting.attachRelay(null)
        reaper.shutdownNow()
        sessions.values.forEach { closeSession(it) }
        sessions.clear()
        byVia.clear()
        sessionsPerIp.clear()
        publicChannel?.close()
    }

    /** Closes any live relay session for [clientIp] right away - called via [VoiceRouting.unregister]
     *  when a player's Minecraft connection ends, so their voicechat session doesn't linger until
     *  [SESSION_IDLE_MILLIS] notices the silence on its own. */
    fun disconnectClient(clientIp: String) {
        val toClose = sessions.entries.filter { it.key.address.hostAddress == clientIp }
        for (entry in toClose) {
            sessions.remove(entry.key, entry.value)
            closeSession(entry.value)
        }
    }

    private fun handleClientPacket(packet: DatagramPacket) {
        val via = packet.sender()
        val buf = packet.content()

        // The client address this datagram belongs to: its own source normally; under
        // proxyProtocol, the address in the PROXY header the fronting proxy prepends. Cloudflare
        // Spectrum (and others) only send that header on the *first* datagram of a flow, so a
        // parse failure isn't necessarily an error - a header-less datagram is matched back to an
        // already-open session by its source ([byVia]) instead.
        val clientAddr: InetSocketAddress = if (expectProxyProtocol) {
            val parsed = try {
                parseProxyProtocolHeader(buf)
            } catch (e: Exception) {
                val known = byVia[via]
                if (known != null && !known.dead) {
                    known.lastActive = System.currentTimeMillis()
                    forward(known, buf.retain())
                } else {
                    logDrop("header-less datagram from an unknown flow (proxyProtocol is on; the fronting proxy sends the PROXY header only on a flow's first datagram - was it lost, or is the proxy not sending one at all?)", via)
                }
                return
            }
            // A PROXY LOCAL command / UNSPEC family - a health check, nothing to route.
            parsed ?: return
        } else {
            via
        }

        val content = buf.retain()

        val existing = sessions[clientAddr]
        if (existing != null) {
            existing.lastActive = System.currentTimeMillis()
            repointVia(existing, via)
            forward(existing, content)
            return
        }

        val clientIp = clientAddr.address.hostAddress
        val backendAddr = VoiceRouting.resolve(clientIp)
        if (backendAddr == null) {
            content.release()
            logDrop(
                if (expectProxyProtocol)
                    "no voicechat route for client $clientIp (from PROXY header) - is that the player's real IP, and did they log in through a voicechat: route?"
                else
                    "no voicechat route for $clientIp - did this client log in through a voicechat: route? (if MCGate is behind an L4 proxy, enable proxyProtocol)",
                via
            )
            return
        }
        val globalCap = UdpThrottle.maxSessions
        if (globalCap > 0 && sessions.size >= globalCap) {
            content.release()
            logDrop("relay is at its session cap ($globalCap)", via)
            return
        }
        if (!acquireIpSlot(clientIp)) {
            content.release()
            logDrop("too many voicechat sessions from $clientIp (cap ${UdpThrottle.maxSessionsPerIp})", via)
            return
        }

        log.info("Opening voicechat relay session: {} (via {}) -> {}", clientAddr, via, backendAddr)
        val session = Session(clientIp, System.currentTimeMillis(), via)
        sessions[clientAddr] = session
        if (expectProxyProtocol) byVia[via] = session
        forward(session, content)
        openBackendChannel(clientAddr, backendAddr, session)
    }

    /** Points [byVia] at [session] under [newVia], clearing its previous entry - for when a later
     *  headered datagram for the same client arrives from a different fronting-proxy source. */
    private fun repointVia(session: Session, newVia: InetSocketAddress) {
        if (!expectProxyProtocol || session.via == newVia) {
            session.via = newVia
            return
        }
        byVia.remove(session.via, session)
        session.via = newVia
        byVia[newVia] = session
    }

    /** Reserves a per-IP session slot for [ip] unless [UdpThrottle.maxSessionsPerIp] is already
     *  reached (0 = unlimited, not tracked). Balanced by [releaseIpSlot] from [closeSession]. */
    private fun acquireIpSlot(ip: String): Boolean {
        val max = UdpThrottle.maxSessionsPerIp
        if (max <= 0) return true
        val c = sessionsPerIp.computeIfAbsent(ip) { AtomicInteger(0) }
        if (c.incrementAndGet() > max) {
            if (c.decrementAndGet() == 0) sessionsPerIp.remove(ip, c)
            return false
        }
        return true
    }

    private fun releaseIpSlot(ip: String) {
        sessionsPerIp.computeIfPresent(ip) { _, c -> if (c.decrementAndGet() <= 0) null else c }
    }

    /** Writes to [Session.backendChannel] if it's already connected, otherwise queues on
     *  [Session.pending] for [openBackendChannel]'s connect listener to flush once it lands. */
    private fun forward(session: Session, content: ByteBuf) {
        synchronized(session) {
            if (session.dead) {
                content.release()
                return
            }
            val channel = session.backendChannel
            if (channel != null) {
                channel.writeAndFlush(content)
                return
            }
            if (session.pending.size >= UdpThrottle.pendingPacketsPerSession) {
                session.pending.removeFirst().release()
            }
            session.pending.addLast(content)
        }
    }

    /** Connects asynchronously - [Bootstrap.connect] on a datagram channel doesn't touch the
     *  network, but it's still scheduled on an event loop drawn from the same [group] that's
     *  calling this (from the shared public channel's own event loop), so `.sync()`-ing it here
     *  would trip Netty's same-executor deadlock guard ([io.netty.util.concurrent.BlockingOperationException])
     *  instead of actually blocking. */
    private fun openBackendChannel(clientAddr: InetSocketAddress, backendAddr: InetSocketAddress, session: Session) {
        val public = publicChannel!!
        val bootstrap = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    session.lastActive = System.currentTimeMillis()
                    if (!session.repliedOnce) {
                        session.repliedOnce = true
                        log.info("Voicechat backend {} replied for the first time to {}", backendAddr, clientAddr)
                    }
                    // Reply to the datagram's actual sender (the fronting L4 proxy when
                    // proxyProtocol is on, otherwise the client itself) - see [Session.via].
                    public.writeAndFlush(DatagramPacket(packet.content().retain(), session.via))
                }

                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    // Most commonly a PortUnreachableException - an ICMP port-unreachable arriving
                    // for a *connected* datagram channel surfaces here as a real exception instead
                    // of silently dropping the packet, and without this override it falls through
                    // to Netty's default tail handler as an unhandled WARN. That's the only reason
                    // this is here: to keep it from going unhandled. It deliberately does NOT close
                    // or evict the session - a single rejected packet (the backend momentarily not
                    // ready right as the player joins, one dropped/errored datagram, etc.) doesn't
                    // mean the whole voice session is dead, and killing the session here forced a
                    // full reconnect (new ephemeral backend socket) on every subsequent packet -
                    // visible as "Opening voicechat relay session" logged over and over for the
                    // same client instead of a session actually holding. The channel stays open and
                    // keeps trying; only real idle timeout ([evictStaleSessions]) or the player
                    // logging out ([VoiceRouting.unregister]) tears a session down now.
                    log.info("Voicechat backend session error for {}: {}", clientAddr, cause.toString())
                }
            })

        bootstrap.connect(backendAddr).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.warn("Failed to open voicechat relay session for {} -> {}: {}", clientAddr, backendAddr, future.cause()?.toString())
                sessions.remove(clientAddr, session)
                closeSession(session)
                return@ChannelFutureListener
            }
            synchronized(session) {
                if (session.dead) {
                    // The session was torn down (player logged out / idle-evicted / relay stopped)
                    // while this connect was still in flight. Nothing will ever close this channel
                    // through [Session.backendChannel] now, so close it here - otherwise its socket
                    // and Netty's per-channel direct-buffer arena leak for the life of the process.
                    future.channel().close()
                    session.pending.forEach { it.release() }
                    session.pending.clear()
                    return@ChannelFutureListener
                }
                session.backendChannel = future.channel()
                while (session.pending.isNotEmpty()) {
                    future.channel().writeAndFlush(session.pending.removeFirst())
                }
            }
        })
    }

    private fun closeSession(session: Session) {
        byVia.remove(session.via, session)
        synchronized(session) {
            if (session.dead) return
            session.dead = true
            session.backendChannel?.close()
            session.pending.forEach { it.release() }
            session.pending.clear()
        }
        releaseIpSlot(session.clientIp)
    }

    /** Tears down sessions that have gone quiet for [UdpThrottle.idleTimeoutMillis], plus - the
     *  spoofed-source-flood defence - any whose backend has never once replied within
     *  [UdpThrottle.noReplyTeardownMillis] of being opened. */
    private fun evictStaleSessions() {
        val now = System.currentTimeMillis()
        val idleCutoff = UdpThrottle.idleTimeoutMillis
        val noReply = UdpThrottle.noReplyTeardownMillis
        val stale = sessions.entries.filter { (_, s) ->
            now - s.lastActive > idleCutoff ||
                (!s.repliedOnce && noReply > 0 && now - s.createdAt > noReply)
        }
        for (entry in stale) {
            if (sessions.remove(entry.key, entry.value)) closeSession(entry.value)
        }
    }
}
