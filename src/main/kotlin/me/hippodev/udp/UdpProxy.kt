package me.hippodev.udp

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import me.hippodev.config.UdpProxyConfig
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** How often the reaper runs - short, so no-reply sessions from a spoofed-source flood are torn
 *  down quickly rather than lingering for the full idle window. All the actual limits it enforces
 *  (idle / no-reply timeouts, session caps, per-session buffering) come from the hot-reloadable
 *  [UdpThrottle]. */
private const val REAPER_INTERVAL_MILLIS = 15_000L

/**
 * Plain static UDP forwarder: every datagram arriving on [UdpProxyConfig.bind] is relayed to
 * [UdpProxyConfig.backend], keyed by source `ip:port` (not just IP, unlike voice/[me.hippodev.voice.VoiceRelay]) -
 * there's no Minecraft login to gate on, so a full source address is the only thing available to
 * tell distinct clients apart. Unrelated to the `voicechat:` route feature; this has its own bind
 * address/port and doesn't touch [me.hippodev.voice.VoiceRouting].
 *
 * Same NAT-style approach as VoiceRelay: each distinct client source gets its own ephemeral
 * backend-facing socket, so the backend sees every client as a distinct address, same as if this
 * proxy weren't in the middle.
 */
class UdpProxy(private val group: EventLoopGroup, private val config: UdpProxyConfig) {
    private val log = LoggerFactory.getLogger(UdpProxy::class.java)

    private class Session(val clientIp: String, @Volatile var lastActive: Long) {
        val createdAt = lastActive
        var backendChannel: Channel? = null
        @Volatile var backendReplied = false
        val pending = ArrayDeque<ByteBuf>()
        /** Set once torn down - a datagram racing in after teardown is released, not re-queued
         *  into a [pending] nothing will drain. */
        var dead = false
    }

    private val sessions = ConcurrentHashMap<InetSocketAddress, Session>()
    private val sessionsPerIp = ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>()
    private var publicChannel: Channel? = null
    private val reaper = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "udp-proxy-reaper-${config.bindAddress.port}").apply { isDaemon = true }
    }

    fun start() {
        val bootstrap = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    handleClientPacket(packet)
                }

                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    log.debug("UDP proxy public channel error", cause)
                }
            })

        publicChannel = bootstrap.bind(config.bindAddress).sync().channel()
        log.info("Relaying UDP {} -> {}", config.bindAddress, config.backendAddress)

        reaper.scheduleAtFixedRate({ evictStaleSessions() }, REAPER_INTERVAL_MILLIS, REAPER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
    }

    fun stop() {
        reaper.shutdownNow()
        sessions.values.forEach { closeSession(it) }
        sessions.clear()
        sessionsPerIp.clear()
        publicChannel?.close()
    }

    private fun handleClientPacket(packet: DatagramPacket) {
        val sender = packet.sender()
        val content = packet.content().retain()

        val existing = sessions[sender]
        if (existing != null) {
            existing.lastActive = System.currentTimeMillis()
            forward(existing, content)
            return
        }

        val globalCap = UdpThrottle.maxSessions
        if (globalCap > 0 && sessions.size >= globalCap) {
            content.release()
            return
        }
        val ip = sender.address.hostAddress
        val perIpCap = UdpThrottle.maxSessionsPerIp
        val ipCount = if (perIpCap > 0) sessionsPerIp.computeIfAbsent(ip) { java.util.concurrent.atomic.AtomicInteger(0) } else null
        if (ipCount != null && ipCount.incrementAndGet() > perIpCap) {
            if (ipCount.decrementAndGet() == 0) sessionsPerIp.remove(ip, ipCount)
            content.release()
            return
        }

        if (config.logSessions) {
            log.info("Opening UDP relay session: {} -> {}", sender, config.backendAddress)
        } else {
            log.debug("Opening UDP relay session: {} -> {}", sender, config.backendAddress)
        }
        val session = Session(ip, System.currentTimeMillis())
        val prev = sessions.putIfAbsent(sender, session)
        if (prev != null) {
            // Raced with another datagram from the same sender - undo our per-IP reservation.
            releaseIpSlot(ip)
            prev.lastActive = System.currentTimeMillis()
            forward(prev, content)
            return
        }
        forward(session, content)
        openBackendChannel(sender, session)
    }

    private fun releaseIpSlot(ip: String) {
        sessionsPerIp.computeIfPresent(ip) { _, c -> if (c.decrementAndGet() <= 0) null else c }
    }

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

    private fun openBackendChannel(clientAddr: InetSocketAddress, session: Session) {
        val public = publicChannel!!
        val bootstrap = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    session.lastActive = System.currentTimeMillis()
                    session.backendReplied = true
                    public.writeAndFlush(DatagramPacket(packet.content().retain(), clientAddr))
                }

                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    log.info("UDP proxy backend {} session error for {}: {}", config.backendAddress, clientAddr, cause.toString())
                }
            })

        bootstrap.connect(config.backendAddress).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.warn("Failed to open UDP relay session for {} -> {}: {}", clientAddr, config.backendAddress, future.cause()?.toString())
                sessions.remove(clientAddr, session)
                closeSession(session)
                return@ChannelFutureListener
            }
            synchronized(session) {
                session.backendChannel = future.channel()
                while (session.pending.isNotEmpty()) {
                    future.channel().writeAndFlush(session.pending.removeFirst())
                }
            }
        })
    }

    private fun closeSession(session: Session) {
        synchronized(session) {
            if (session.dead) return
            session.dead = true
            session.backendChannel?.close()
            session.pending.forEach { it.release() }
            session.pending.clear()
        }
        releaseIpSlot(session.clientIp)
    }

    private fun evictStaleSessions() {
        val now = System.currentTimeMillis()
        val idleCutoff = UdpThrottle.idleTimeoutMillis
        val noReply = UdpThrottle.noReplyTeardownMillis
        val stale = sessions.entries.filter { (_, s) ->
            now - s.lastActive > idleCutoff ||
                (!s.backendReplied && noReply > 0 && now - s.createdAt > noReply)
        }
        for (entry in stale) {
            if (sessions.remove(entry.key, entry.value)) closeSession(entry.value)
        }
    }
}
