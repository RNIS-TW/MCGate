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

private const val SESSION_IDLE_MILLIS = 5 * 60_000L

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

    private class Session(@Volatile var lastActive: Long) {
        var backendChannel: Channel? = null
        val pending = ArrayDeque<ByteBuf>()
    }

    private val sessions = ConcurrentHashMap<InetSocketAddress, Session>()
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

        reaper.scheduleAtFixedRate({ evictIdleSessions() }, SESSION_IDLE_MILLIS, SESSION_IDLE_MILLIS, TimeUnit.MILLISECONDS)
    }

    fun stop() {
        reaper.shutdownNow()
        sessions.values.forEach { closeSession(it) }
        sessions.clear()
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

        log.info("Opening UDP relay session: {} -> {}", sender, config.backendAddress)
        val session = Session(System.currentTimeMillis())
        sessions[sender] = session
        forward(session, content)
        openBackendChannel(sender, session)
    }

    private fun forward(session: Session, content: ByteBuf) {
        synchronized(session) {
            val channel = session.backendChannel
            if (channel != null) channel.writeAndFlush(content) else session.pending.addLast(content)
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
                    public.writeAndFlush(DatagramPacket(packet.content().retain(), clientAddr))
                }

                override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                    log.info("UDP proxy backend session error for {}: {}", clientAddr, cause.toString())
                }
            })

        bootstrap.connect(config.backendAddress).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.warn("Failed to open UDP relay session for {} -> {}: {}", clientAddr, config.backendAddress, future.cause()?.toString())
                sessions.remove(clientAddr, session)
                synchronized(session) {
                    session.pending.forEach { it.release() }
                    session.pending.clear()
                }
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
            session.backendChannel?.close()
            session.pending.forEach { it.release() }
            session.pending.clear()
        }
    }

    private fun evictIdleSessions() {
        val cutoff = System.currentTimeMillis() - SESSION_IDLE_MILLIS
        val idle = sessions.entries.filter { it.value.lastActive < cutoff }
        for (entry in idle) {
            sessions.remove(entry.key, entry.value)
            closeSession(entry.value)
        }
    }
}
