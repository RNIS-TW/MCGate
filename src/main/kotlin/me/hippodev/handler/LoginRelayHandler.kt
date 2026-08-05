package me.hippodev.handler

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import me.hippodev.applyTunedKeepalive
import me.hippodev.config.*
import me.hippodev.routing.*
import me.hippodev.protocol.*
import me.hippodev.voice.VoiceRouting
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * Handles a connection past the handshake once we know next_state == 2 (login).
 * Dials a backend (with failover across the route's ordered backend list),
 * then splices client <-> backend as a raw byte pipe - no further protocol parsing.
 */
class LoginRelayHandler(
    private val route: Route,
    private val runtime: RouteRuntime,
    private val backends: List<InetSocketAddress>,
    private val protocolVersion: Int,
    private val host: String,
    private val port: Int,
    private val handshakeFrame: ByteBuf
) : ChannelInboundHandlerAdapter() {

    private val log = LoggerFactory.getLogger(LoginRelayHandler::class.java)
    private var backendChannel: Channel? = null
    private var backendAddr: InetSocketAddress? = null
    private val pending = ArrayDeque<ByteBuf>()
    private var connectedAt = 0L
    private val disconnectLogged = java.util.concurrent.atomic.AtomicBoolean(false)
    private var playerName: String? = null
    private var playerUuid: java.util.UUID? = null
    private var loginSniffed = false
    private var loginLogged = false
    private var awaitedLoginStart = false
    /** Compression threshold the *backend* negotiated with the client during login (-1 = none),
     *  captured by [BackendLoginSniffer] since MCGate's relay is otherwise byte-blind. Needed so
     *  [ReconnectHandler] can frame the packets it synthesizes to match what the client's decoder
     *  is already expecting if a mid-session drop puts the player into the reconnect wait. */
    private var compressionThreshold = -1
    /** True once the backend sent a Login-state Encryption Request (online-mode). From that
     *  point on the client and backend share an AES key MCGate never sees - the connection is a
     *  pure ciphertext pipe, and MCGate cannot inject any packet of its own into it without
     *  desyncing the client's stream cipher. Mid-session auto-reconnect is impossible on such a
     *  connection; see [handleBackendDrop]. */
    private var encrypted = false
    private var clientRemoteAddress: String = "?"
    private lateinit var frontendChannel: Channel
    /** Guards [handshakeFrame]/[pending] against a double-release: they're normally released once
     *  the backend dial resolves (success or all-attempts-exhausted, in [connect]), but if the
     *  client disconnects *while a dial is still in flight*, [channelInactive] releases them right
     *  away instead of leaving them held for however long the in-flight dial takes to time out -
     *  see [releasePendingBuffers]. Only ever touched on this channel's event-loop thread (every
     *  caller - channelRead, channelInactive, and the connect()/bootstrap listeners below, since
     *  the backend Bootstrap is built with `.group(clientChannel.eventLoop())` - runs on it), so
     *  a plain Boolean is enough. */
    private var pendingReleased = false

    /** Must be called explicitly right after this handler is added to the pipeline -
     *  channelActive() will not fire since the channel is already active by then. */
    fun start(ctx: ChannelHandlerContext) {
        ctx.channel().config().isAutoRead = false
        clientRemoteAddress = ctx.channel().effectiveRemoteAddress().toString()
        frontendChannel = ctx.channel()
        connect(ctx, orderBackends(route, runtime, backends), 0)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val backend = backendChannel
        val buf = msg as ByteBuf
        if (!loginSniffed) {
            loginSniffed = true
            parseLoginStart(buf)?.let { (name, uuid) ->
                playerName = name
                playerUuid = uuid
            }
            logLoginIfReady()
        }
        if (backend != null && backend.isActive) {
            backend.writeAndFlush(buf)
        } else {
            pending.addLast(buf)
        }
    }

    private fun connect(ctx: ChannelHandlerContext, ordered: List<InetSocketAddress>, attempt: Int) {
        if (attempt >= ordered.size) {
            // Login Start sniffing (channelRead) races with backend dialing; a fast "connection
            // refused" can resolve before the client's Login Start bytes have been read and
            // parsed. Give it one short grace period before falling back to a plain kick, since
            // auto-reconnect needs the name/UUID it carries.
            if (route.reconnect.enabled && reconnectSupported(protocolVersion) && playerName == null && !awaitedLoginStart) {
                awaitedLoginStart = true
                ctx.channel().eventLoop().schedule({ connect(ctx, ordered, attempt) }, 150, java.util.concurrent.TimeUnit.MILLISECONDS)
                return
            }

            releasePendingBuffers()

            val name = playerName
            val uuid = playerUuid
            if (route.reconnect.enabled && reconnectSupported(protocolVersion) && name != null && uuid != null) {
                log.info("All backends unreachable for host '{}', holding '{}' for reconnect", host, name)
                PlayerSessions.put(
                    PlayerSession(
                        name, uuid, host, clientRemoteAddress, null, System.currentTimeMillis(),
                        ctx.channel(), protocolVersion, compressionThreshold, encrypted
                    )
                )
                val reconnectHandler = ReconnectHandler(route, runtime, backends, protocolVersion, host, port, name, uuid)
                ctx.pipeline().replace(this, "reconnect", reconnectHandler)
                reconnectHandler.enter(ctx.pipeline().context(reconnectHandler))
            } else {
                log.warn("All backends unreachable for host '{}', kicking client", host)
                ctx.writeAndFlush(encodeLoginDisconnect(toJsonComponent(route.reconnect.kickMessage))).addListener(ChannelFutureListener.CLOSE)
            }
            return
        }

        val addr = ordered[attempt]
        val clientChannel = ctx.channel()
        val dialStartedAt = System.currentTimeMillis()

        val bootstrap = Bootstrap()
            .group(clientChannel.eventLoop())
            .channel(clientChannel.javaClass)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            // Same reasoning as the client-facing listener's SO_KEEPALIVE (see Main.kt) - a NAT/
            // firewall between MCGate and the backend can just as easily decide this leg looks
            // idle and drop it, especially since the backend often sits on internal/private
            // infrastructure with its own middleboxes.
            .option(ChannelOption.SO_KEEPALIVE, true)
            .applyTunedKeepalive()
            .handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(BackendLoginSniffer(clientChannel))
                }
            })

        bootstrap.connect(addr).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.debug("Failed to connect to backend {}: {}", addr, future.cause()?.message)
                connect(ctx, ordered, attempt + 1)
                return@ChannelFutureListener
            }

            val channel = future.channel()
            if (pendingReleased) {
                // Client disconnected while this dial was in flight - channelInactive already
                // released handshakeFrame/pending (see releasePendingBuffers), so there's nothing
                // left to relay to this backend. Don't touch either buffer again (would
                // double-release) and don't leave this now-pointless backend connection open.
                channel.close()
                return@ChannelFutureListener
            }
            backendChannel = channel
            backendAddr = addr
            connectedAt = System.currentTimeMillis()
            runtime.recordConnectOpened(addr)
            runtime.recordLatency(addr, connectedAt - dialStartedAt)
            log.info("Connected: '{}' from {} -> {}", host, clientChannel.effectiveRemoteAddress(), addr)
            logLoginIfReady()

            channel.closeFuture().addListener(ChannelFutureListener {
                runtime.recordConnectClosed(addr)
                logDisconnect(clientChannel.effectiveRemoteAddress(), addr)
            })

            if (route.proxyProtocol) {
                channel.writeAndFlush(buildProxyProtocolHeader(clientChannel.effectiveRemoteAddress(), addr))
            }

            if (route.modifyVirtualHost) {
                channel.writeAndFlush(encodeHandshake(protocolVersion, addr.hostString, port, 2))
                handshakeFrame.release()
            } else {
                channel.writeAndFlush(handshakeFrame)
            }
            pendingReleased = true // handshakeFrame is now spent either way - see the guard above

            clientChannel.config().isAutoRead = true
            while (pending.isNotEmpty()) {
                channel.writeAndFlush(pending.removeFirst())
            }
        })
    }

    /** Releases [handshakeFrame]/[pending] exactly once, however the dial ends up resolving - see
     *  [pendingReleased]. */
    private fun releasePendingBuffers() {
        if (pendingReleased) return
        pendingReleased = true
        handshakeFrame.release()
        pending.forEach { it.release() }
        pending.clear()
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        backendAddr?.let { logDisconnect(ctx.channel().effectiveRemoteAddress(), it) }
        playerUuid?.let { PlayerSessions.remove(it) }
        VoiceRouting.unregisterChannel(ctx.channel())
        closeOnFlush(backendChannel)
        // If a backend dial is still in flight when the client disconnects, this is what actually
        // frees handshakeFrame/pending - previously they just sat there, still referenced, until
        // whatever backend dial was in progress happened to resolve on its own (up to
        // CONNECT_TIMEOUT_MILLIS per remaining backend in the route) - a real, if bounded, delay
        // in freeing memory that shows up under Netty's leak detector as a ByteBuf whose access
        // trail is empty (this handler is a raw ChannelInboundHandlerAdapter, not a
        // ByteToMessageDecoder, so it never calls touch()) once GC finally collects it. The
        // connect()/bootstrap-listener continuation now checks [pendingReleased] and backs off
        // instead of touching either buffer again.
        releasePendingBuffers()
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.debug("Frontend connection error", cause)
        closeOnFlush(ctx.channel())
    }

    /** Called when a backend the client was already relaying to goes away mid-session. Holds the
     *  client for reconnect (matching whatever compression [BackendLoginSniffer] observed the
     *  backend negotiate) if the route opts in via `reconnect.onMidSessionDrop`; otherwise just
     *  closes the client connection like before this feature existed. */
    private fun handleBackendDrop(clientChannel: Channel) {
        val name = playerName
        val uuid = playerUuid
        if (route.reconnect.enabled && route.reconnect.onMidSessionDrop && reconnectSupported(protocolVersion) &&
            !encrypted && name != null && uuid != null && clientChannel.isActive
        ) {
            log.info("Backend dropped for '{}' ({}) on '{}', holding for reconnect", name, uuid, host)
            val existing = PlayerSessions.get(uuid)
            PlayerSessions.put(
                PlayerSession(
                    name, uuid, host, existing?.remoteAddress ?: clientRemoteAddress, null,
                    existing?.connectedAt ?: connectedAt, clientChannel, protocolVersion, compressionThreshold,
                    encrypted = false // this branch requires !encrypted (see the guard above), so always false here
                )
            )
            val reconnectHandler = ReconnectHandler(
                route, runtime, backends, protocolVersion, host, port, name, uuid, compressionThreshold
            )
            clientChannel.pipeline().replace("relay", "reconnect", reconnectHandler)
            reconnectHandler.enterFromPlay(clientChannel.pipeline().context(reconnectHandler))
        } else {
            // Only worth logging when reconnect is actually enabled for this route - otherwise
            // this fires on every ordinary disconnect and is just noise, not a diagnostic signal.
            if (route.reconnect.enabled) {
                log.info(
                    "Backend dropped for '{}' on '{}' but not holding for reconnect (enabled={}, onMidSessionDrop={}, " +
                        "protocolVersion={}, reconnectSupported={}, encrypted={}, name={}, uuid={})",
                    name, host, route.reconnect.enabled, route.reconnect.onMidSessionDrop, protocolVersion,
                    reconnectSupported(protocolVersion), encrypted, name, uuid
                )
            }
            uuid?.let { PlayerSessions.remove(it) }
            closeOnFlush(clientChannel)
        }
    }

    /** Installed on the backend channel during the original login. MCGate's relay is otherwise
     *  byte-blind, but needs to know if the backend enabled packet compression (via the Login
     *  state's Set Compression packet, id 0x03 - stable since 1.8) so a later mid-session
     *  reconnect entry can frame its own synthesized packets the way the client's decoder now
     *  expects. Forwards every frame to the client unchanged; once Login Success (id 0x02) goes
     *  by, login is over and this swaps itself out for a dumb raw pipe. */
    private inner class BackendLoginSniffer(private val clientChannel: Channel) : ByteToMessageDecoder() {
        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
            val frameStart = buf.readerIndex()
            val length = try {
                readVarInt(buf)
            } catch (e: IncompleteVarIntException) {
                buf.readerIndex(frameStart); return
            }
            val payloadStart = buf.readerIndex()
            if (buf.readableBytes() < length) {
                buf.readerIndex(frameStart); return
            }
            val frameEnd = payloadStart + length

            val packetId = try {
                val (id, payloadBuf) = readCompressedFrame(buf, frameEnd, compressionThreshold)
                if (id == 0x03 && compressionThreshold < 0 && payloadBuf === buf) {
                    // Set Compression is always sent uncompressed, right before it takes effect.
                    compressionThreshold = readVarInt(buf)
                }
                if (payloadBuf !== buf) payloadBuf.release()
                id
            } catch (e: Exception) {
                -1
            }

            buf.readerIndex(frameStart)
            val frameBytes = buf.readRetainedSlice(frameEnd - frameStart)
            if (clientChannel.isActive) clientChannel.writeAndFlush(frameBytes) else frameBytes.release()

            if (packetId == LOGIN_ENCRYPTION_REQUEST) {
                // Backend is online-mode: everything from the client's Encryption Response
                // onward is AES-encrypted ciphertext, which this decoder cannot parse - and which
                // MCGate could never safely inject synthesized packets into anyway (it would
                // desync the client's stream cipher). Stop trying to parse and mark the
                // connection as ineligible for mid-session auto-reconnect (see handleBackendDrop)
                // *and* for a kick message (see PlayerSessions.kick) - writing anything into this
                // connection from here on would corrupt the client's cipher stream, which is
                // itself indistinguishable from a generic "Connection Lost" to the player.
                encrypted = true
                refreshSession()
                ctx.pipeline().replace(this, "relay", PlainBackendRelay(clientChannel))
            } else if (packetId == 0x02) {
                // Login Success - login phase over, fall back to a dumb byte pipe. Compression
                // (if any) is always negotiated via Set Compression before this, so
                // compressionThreshold is final now - refresh the PlayerSessions record with it.
                // logLoginIfReady() runs the instant the backend TCP connect succeeds, well
                // before Set Compression could have arrived, so the value it captured was almost
                // always a stale -1; a kick sent using that stale value would frame the Disconnect
                // packet as uncompressed on a connection the client actually expects compressed,
                // producing garbage bytes the client can't parse - which shows up as a generic
                // "Connection Lost" instead of the kick message ever being seen.
                refreshSession()
                ctx.pipeline().replace(this, "relay", PlainBackendRelay(clientChannel))
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            handleBackendDrop(clientChannel)
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.debug("Backend connection error", cause)
            closeOnFlush(ctx.channel())
        }
    }

    private inner class PlainBackendRelay(private val clientChannel: Channel) : SimpleChannelInboundHandler<ByteBuf>() {
        override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
            if (clientChannel.isActive) clientChannel.writeAndFlush(msg.retain())
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            handleBackendDrop(clientChannel)
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.debug("Backend connection error", cause)
            closeOnFlush(ctx.channel())
        }
    }

    /** Logs the Login line once both the player's name and the chosen backend are known - the
     *  two events race, since login-start sniffing and backend dialing happen concurrently. */
    private fun logLoginIfReady() {
        val name = playerName ?: return
        val addr = backendAddr ?: return
        if (!loginLogged) {
            loginLogged = true
            log.info("Login: '{}'{} on '{}' -> {}", name, playerUuid?.let { " ($it)" } ?: "", host, addr)
            playerUuid?.let {
                PlayerSessions.put(
                    PlayerSession(
                        name, it, host, clientRemoteAddress, addr, connectedAt, frontendChannel,
                        protocolVersion, compressionThreshold, encrypted
                    )
                )
            }
        }
    }

    /** Re-puts the current PlayerSessions record (if any) with the now-current
     *  [compressionThreshold]/[encrypted] - see the call sites in [BackendLoginSniffer] for why
     *  this matters. */
    private fun refreshSession() {
        val uuid = playerUuid ?: return
        val existing = PlayerSessions.get(uuid) ?: return
        PlayerSessions.put(existing.copy(compressionThreshold = compressionThreshold, encrypted = encrypted))
    }

    private fun logDisconnect(clientAddr: java.net.SocketAddress, addr: InetSocketAddress) {
        if (!disconnectLogged.compareAndSet(false, true)) return
        val durationMs = System.currentTimeMillis() - connectedAt
        val name = playerName
        val tag = if (name != null) " ($name${playerUuid?.let { ", $it" } ?: ""})" else ""
        log.info("Disconnected: '{}'{} from {} -> {} (connected {} ms)", host, tag, clientAddr, addr, durationMs)
    }
}

private fun buildProxyProtocolHeader(clientAddr: java.net.SocketAddress, backendAddr: InetSocketAddress): ByteBuf =
    encodeProxyProtocolHeader(clientAddr as InetSocketAddress, backendAddr)

private fun closeOnFlush(channel: Channel?) {
    if (channel != null && channel.isActive) {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
            .addListener(ChannelFutureListener.CLOSE)
    }
}
