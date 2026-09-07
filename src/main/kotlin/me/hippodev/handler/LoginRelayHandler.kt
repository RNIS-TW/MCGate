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
import me.hippodev.tracking.ConnectionRecord
import me.hippodev.tracking.ConnectionTracker
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
    /** True once dialing ends or this handler is closed/removed. Guards [handshakeFrame] against
     *  double release and prevents further buffering when no future dial can drain [pending].
     *  All access runs on the client's event loop, including backend connect listeners. */
    private var pendingReleased = false
    /** Wall-clock start of this login attempt, captured before any backend dial - used as the
     *  connection-tracking record's start time even in the (rare) case every backend dial fails
     *  and the client is kicked before ever actually connecting. */
    private var sessionStartedAt = 0L
    /** Total backend dial attempts made this session (one per failover hop) - reported to
     *  [ConnectionTracker] as `loginAttempts`, and shared (not recreated) into every
     *  [PlayerSession] this handler puts so a live API/console lookup mid-session reflects it too. */
    private val backendDialAttempts = java.util.concurrent.atomic.AtomicInteger(0)
    /** Live packet/byte counters, shared into every [PlayerSession] this handler puts - see
     *  [PlayerSession.packetsSent] for why these are shared atomics rather than plain fields.
     *  Always maintained (not gated behind [ConnectionTracker.enabled]) since a live player-detail
     *  lookup (console `whois`, the `/metrics?type=json` API) needs current totals regardless of
     *  whether historical SQLite tracking is turned on; incrementing an AtomicLong already on the
     *  event-loop thread handling that exact packet is a handful of nanoseconds, not a cost worth
     *  gating. */
    private val packetsToBackend = java.util.concurrent.atomic.AtomicLong(0)
    private val bytesToBackend = java.util.concurrent.atomic.AtomicLong(0)
    private val packetsToClient = java.util.concurrent.atomic.AtomicLong(0)
    private val bytesToClient = java.util.concurrent.atomic.AtomicLong(0)
    private var trackingFlushed = false

    /** Must be called explicitly right after this handler is added to the pipeline -
     *  channelActive() will not fire since the channel is already active by then. */
    fun start(ctx: ChannelHandlerContext) {
        ctx.channel().config().isAutoRead = false
        clientRemoteAddress = ctx.channel().effectiveRemoteAddress().toString()
        frontendChannel = ctx.channel()
        sessionStartedAt = System.currentTimeMillis()
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
        packetsToBackend.incrementAndGet()
        bytesToBackend.addAndGet(buf.readableBytes().toLong())
        if (backend != null && backend.isActive) {
            backend.writeAndFlush(buf)
        } else if (pendingReleased) {
            // Dialing has ended; no future connection will drain this buffer.
            buf.release()
        } else {
            pending.addLast(buf)
        }
    }

    private fun connect(ctx: ChannelHandlerContext, ordered: List<InetSocketAddress>, attempt: Int) {
        if (pendingReleased || !ctx.channel().isActive || ctx.isRemoved) return
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
                        ctx.channel(), protocolVersion, compressionThreshold, encrypted,
                        backendDialAttempts, packetsToBackend, packetsToClient, bytesToBackend, bytesToClient
                    )
                )
                val reconnectHandler = ReconnectHandler(route, runtime, backends, protocolVersion, host, port, name, uuid)
                ctx.pipeline().replace(this, "reconnect", reconnectHandler)
                reconnectHandler.enter(ctx.pipeline().context(reconnectHandler))
            } else {
                log.warn("All backends unreachable for host '{}', kicking client", host)
                ctx.writeAndFlush(encodeLoginDisconnect(toJsonComponent(route.kickMessage))).addListener(ChannelFutureListener.CLOSE)
            }
            return
        }

        val addr = ordered[attempt]
        val clientChannel = ctx.channel()
        val dialStartedAt = System.currentTimeMillis()
        backendDialAttempts.incrementAndGet()

        val bootstrap = Bootstrap()
            .group(clientChannel.eventLoop())
            .channel(clientChannel.javaClass)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            // Mirror the client listener's write water marks (see Main.kt) so the backend->client
            // and client->backend legs are throttled symmetrically - a backend that stops reading
            // must be able to push back on the client just as the reverse does.
            .option(
                ChannelOption.WRITE_BUFFER_WATER_MARK,
                io.netty.channel.WriteBufferWaterMark(32 * 1024, 64 * 1024)
            )
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
            // Draining the login backlog above can itself push the backend past its write water
            // mark; make sure the client's reads reflect that right away rather than only on the
            // next writability change.
            channel.pauseOrResumeReads(clientChannel)
        })
    }

    /** Releases the handshake if still owned, and always drains any remaining queued buffers. */
    private fun releasePendingBuffers() {
        if (!pendingReleased) {
            pendingReleased = true
            handshakeFrame.release()
        }
        pending.forEach { it.release() }
        pending.clear()
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) {
        releasePendingBuffers()
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        backendAddr?.let { logDisconnect(ctx.channel().effectiveRemoteAddress(), it) }
        flushTrackingRecord()
        playerUuid?.let { PlayerSessions.remove(it) }
        VoiceRouting.unregisterChannel(ctx.channel())
        closeOnFlush(backendChannel)
        // Release owned buffers immediately, including during an in-flight backend dial.
        releasePendingBuffers()
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.debug("Frontend connection error", cause)
        closeOnFlush(ctx.channel())
    }

    /** Client-side half of the relay's flow control (see [pauseOrResumeReads]): when the client's
     *  socket can't drain what MCGate is forwarding to it, stop pulling more from the backend
     *  rather than queuing it in memory. */
    override fun channelWritabilityChanged(ctx: ChannelHandlerContext) {
        ctx.channel().pauseOrResumeReads(backendChannel)
        ctx.fireChannelWritabilityChanged()
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
                    encrypted = false, // this branch requires !encrypted (see the guard above), so always false here
                    loginAttempts = backendDialAttempts, packetsSent = packetsToBackend, packetsReceived = packetsToClient,
                    bytesSent = bytesToBackend, bytesReceived = bytesToClient
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
            packetsToClient.incrementAndGet()
            bytesToClient.addAndGet(frameBytes.readableBytes().toLong())
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
            try {
                super.channelInactive(ctx)
            } finally {
                handleBackendDrop(clientChannel)
            }
        }

        override fun channelWritabilityChanged(ctx: ChannelHandlerContext) {
            ctx.channel().pauseOrResumeReads(clientChannel)
            ctx.fireChannelWritabilityChanged()
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.debug("Backend connection error", cause)
            closeOnFlush(ctx.channel())
        }
    }

    private inner class PlainBackendRelay(private val clientChannel: Channel) : SimpleChannelInboundHandler<ByteBuf>() {
        override fun handlerAdded(ctx: ChannelHandlerContext) {
            // This handler replaces BackendLoginSniffer on an already-active channel, so
            // channelActive never fires for it - sync here instead. Adopt whatever the client's
            // current writability is the instant this raw pipe takes over, so a client that fell
            // behind during login doesn't get a free unthrottled burst.
            clientChannel.pauseOrResumeReads(ctx.channel())
        }

        override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
            packetsToClient.incrementAndGet()
            bytesToClient.addAndGet(msg.readableBytes().toLong())
            if (clientChannel.isActive) clientChannel.writeAndFlush(msg.retain())
        }

        override fun channelWritabilityChanged(ctx: ChannelHandlerContext) {
            ctx.channel().pauseOrResumeReads(clientChannel)
            ctx.fireChannelWritabilityChanged()
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
                        protocolVersion, compressionThreshold, encrypted,
                        backendDialAttempts, packetsToBackend, packetsToClient, bytesToBackend, bytesToClient
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

    /** Emits this session's final [ConnectionRecord] to [ConnectionTracker], if tracking is
     *  enabled and a player was ever identified. Guarded so a session that ends up here more than
     *  once (shouldn't happen - channelInactive only fires once per channel - but cheap to be
     *  sure) doesn't double-report. Sessions handed off to [ReconnectHandler] don't reach this at
     *  all (pipeline `replace`, not a channel close), so their post-handoff activity isn't
     *  reflected here - only what happened while this handler owned the channel. */
    private fun flushTrackingRecord() {
        if (trackingFlushed) return
        trackingFlushed = true
        if (!ConnectionTracker.enabled) return
        val uuid = playerUuid ?: return
        ConnectionTracker.record(
            ConnectionRecord(
                uuid = uuid,
                name = playerName ?: "",
                ip = clientRemoteAddress,
                host = host,
                protocolVersion = protocolVersion,
                loginAttempts = backendDialAttempts.get(),
                packetsSent = packetsToBackend.get(),
                packetsReceived = packetsToClient.get(),
                bytesSent = bytesToBackend.get(),
                bytesReceived = bytesToClient.get(),
                compressionThreshold = compressionThreshold,
                encrypted = encrypted,
                connectedAt = sessionStartedAt,
                disconnectedAt = System.currentTimeMillis()
            )
        )
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
