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
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** How long a client<->backend UDP session can go without traffic in either direction before it's
 *  torn down. A voice mod only sends packets while a player is actually transmitting audio, so
 *  this has to be generous enough to survive a player just being quiet for a while - unlike TCP,
 *  there's no close/FIN to signal "this session is really over". */
private const val SESSION_IDLE_MILLIS = 5 * 60_000L

/**
 * Relays UDP traffic (Simple Voice Chat and similar mods) on the same port MCGate's Minecraft TCP
 * listener binds - no extra port to open. UDP carries no hostname the way the Minecraft handshake
 * does, so which backend a datagram belongs to is looked up by the sender's IP via [VoiceRouting],
 * populated from the TCP side when a route with a `voicechat:` backend is resolved for a client.
 *
 * Acts as a small NAT: each distinct client gets its own ephemeral backend-facing UDP socket (see
 * [Session.backendChannel]), so the backend sees every player as a distinct source address/port,
 * same as if MCGate weren't in the middle. Funnelling every client through one shared
 * backend-facing socket would make the backend unable to tell players apart at all.
 */
class VoiceRelay(private val group: EventLoopGroup) {
    private val log = LoggerFactory.getLogger(VoiceRelay::class.java)

    /** [backendChannel] starts null and is filled in once the async `connect()` in
     *  [openBackendChannel] completes; packets arriving in the meantime queue up in [pending]
     *  instead of being dropped. Every field is only ever touched under `synchronized(this)` -
     *  packets can arrive on the shared public channel's event loop while the connect completion
     *  runs on the new backend channel's own (different) event loop, so this genuinely races,
     *  unlike most other per-connection state in this codebase which is pinned to one thread. */
    private class Session(@Volatile var lastActive: Long) {
        var backendChannel: Channel? = null
        val pending = ArrayDeque<ByteBuf>()
        /** Set on the first reply seen from the backend - lets [openBackendChannel] log once
         *  whether the backend ever actually answers at all, distinct from repeated per-packet
         *  logging. */
        @Volatile var repliedOnce = false
    }

    private val sessions = ConcurrentHashMap<InetSocketAddress, Session>()
    private var publicChannel: Channel? = null
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

        reaper.scheduleAtFixedRate({ evictIdleSessions() }, SESSION_IDLE_MILLIS, SESSION_IDLE_MILLIS, TimeUnit.MILLISECONDS)
        VoiceRouting.attachRelay(this)
    }

    fun stop() {
        VoiceRouting.attachRelay(null)
        reaper.shutdownNow()
        sessions.values.forEach { closeSession(it) }
        sessions.clear()
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
        val sender = packet.sender()
        val content = packet.content().retain()

        val existing = sessions[sender]
        if (existing != null) {
            existing.lastActive = System.currentTimeMillis()
            forward(existing, content)
            return
        }

        val backendAddr = VoiceRouting.resolve(sender.address.hostAddress)
        if (backendAddr == null) {
            content.release()
            return
        }

        log.info("Opening voicechat relay session: {} -> {}", sender, backendAddr)
        val session = Session(System.currentTimeMillis())
        sessions[sender] = session
        forward(session, content)
        openBackendChannel(sender, backendAddr, session)
    }

    /** Writes to [Session.backendChannel] if it's already connected, otherwise queues on
     *  [Session.pending] for [openBackendChannel]'s connect listener to flush once it lands. */
    private fun forward(session: Session, content: ByteBuf) {
        synchronized(session) {
            val channel = session.backendChannel
            if (channel != null) channel.writeAndFlush(content) else session.pending.addLast(content)
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
                    public.writeAndFlush(DatagramPacket(packet.content().retain(), clientAddr))
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
                    // keeps trying; only real idle timeout ([evictIdleSessions]) or the player
                    // logging out ([VoiceRouting.unregister]) tears a session down now.
                    log.info("Voicechat backend session error for {}: {}", clientAddr, cause.toString())
                }
            })

        bootstrap.connect(backendAddr).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.warn("Failed to open voicechat relay session for {} -> {}: {}", clientAddr, backendAddr, future.cause()?.toString())
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
