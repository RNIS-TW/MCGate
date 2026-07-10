package me.hippodev.handler

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.util.concurrent.ScheduledFuture
import me.hippodev.config.Route
import me.hippodev.protocol.*
import me.hippodev.routing.PlayerSession
import me.hippodev.routing.PlayerSessions
import me.hippodev.routing.RouteRuntime
import me.hippodev.routing.orderBackends
import me.hippodev.routing.pingBackendLive
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.TimeUnit

private const val KEEP_ALIVE_INTERVAL_MILLIS = 15_000L

/**
 * Auto-reconnect holding state for a player while a route's backends are unreachable - the
 * VelocityReconnect-style "keep them online, no separate holding server needed" behavior. Spawns
 * the client into a minimal empty world (see ReconnectProtocol.kt), keeps it alive with periodic
 * Keep Alive packets, shows an animated waiting screen, and polls the backend on a backoff
 * schedule. Once a backend answers, transitions the client back to the real server on the *same*
 * TCP connection (no visible reconnect) via the 1.20.2+ Start-Configuration reentry dance - see
 * [beginTransfer].
 *
 * Only installed for protocol versions [reconnectSupported] covers; callers must check that first.
 */
class ReconnectHandler(
    private val route: Route,
    private val runtime: RouteRuntime,
    private val backends: List<InetSocketAddress>,
    private val protocolVersion: Int,
    private val host: String,
    private val port: Int,
    private val playerName: String,
    private val playerUuid: UUID,
    /** The compression threshold already negotiated on this connection, or -1 if none. Must
     *  match whatever the client's decoder currently expects: -1 for a client waiting to
     *  reconnect on its very first login (see [enter]), or whatever the original backend set via
     *  [enterFromPlay]. */
    private val compressionThreshold: Int = -1
) : ByteToMessageDecoder() {

    private val log = LoggerFactory.getLogger(ReconnectHandler::class.java)
    private val ids = reconnectPacketIds(protocolVersion)

    private var keepAliveTask: ScheduledFuture<*>? = null
    private var retryTask: ScheduledFuture<*>? = null
    private var animationTask: ScheduledFuture<*>? = null
    private var animationFrame = 0
    private var currentIntervalMillis = route.reconnect.retryIntervalMillis
    private var attemptCount = 0
    private val enteredAt = System.currentTimeMillis()
    private var onConfigurationAck: ((ChannelHandlerContext) -> Unit)? = null
    private var done = false

    /** Entry point when the client hasn't logged in yet (initial connect found every backend
     *  down) - ReconnectHandler performs the client's very first Login Success on this
     *  connection. */
    fun enter(ctx: ChannelHandlerContext) {
        ctx.channel().config().isAutoRead = true
        ctx.writeAndFlush(encodeLoginSuccess(ids, playerUuid, playerName, compressionThreshold))
        sendWaitingWorld(ctx)
        startKeepAlive(ctx)
        startAnimation(ctx)
        scheduleRetry(ctx)
        log.info("Holding for reconnect: '{}' ({}) on '{}'", playerName, playerUuid, host)
    }

    /** Entry point when a real backend the client was already playing on dropped mid-session -
     *  the client already completed Login once (against the old backend) and is sitting in Play
     *  state, so this brings it back to Configuration via Start Configuration instead of resending
     *  Login Success (which a client only ever accepts once per connection). */
    fun enterFromPlay(ctx: ChannelHandlerContext) {
        ctx.channel().config().isAutoRead = true
        awaitConfigurationAck(ctx) { ackCtx ->
            sendWaitingWorld(ackCtx)
            startKeepAlive(ackCtx)
            startAnimation(ackCtx)
            scheduleRetry(ackCtx)
            log.info("Backend dropped mid-session, holding for reconnect: '{}' ({}) on '{}'", playerName, playerUuid, host)
        }
    }

    private fun awaitConfigurationAck(ctx: ChannelHandlerContext, action: (ChannelHandlerContext) -> Unit) {
        onConfigurationAck = action
        ctx.writeAndFlush(encodeStartConfiguration(ids, compressionThreshold))
    }

    private fun sendWaitingWorld(ctx: ChannelHandlerContext) {
        ctx.writeAndFlush(encodeFinishConfiguration(ids, compressionThreshold))
        ctx.writeAndFlush(encodeJoinGame(ids, entityId = 1, compressionThreshold = compressionThreshold))
        ctx.writeAndFlush(encodeSetCenterChunk(ids, compressionThreshold))
        ctx.writeAndFlush(encodeEmptyChunk(ids, compressionThreshold))
        ctx.writeAndFlush(encodeSetTitleText(ids, route.reconnect.title, compressionThreshold))
        ctx.writeAndFlush(encodeSetSubtitleText(ids, route.reconnect.subtitle, compressionThreshold))
    }

    private fun startKeepAlive(ctx: ChannelHandlerContext) {
        keepAliveTask = ctx.channel().eventLoop().scheduleAtFixedRate({
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(encodePlayKeepAlive(ids, System.currentTimeMillis(), compressionThreshold))
            }
        }, KEEP_ALIVE_INTERVAL_MILLIS, KEEP_ALIVE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
    }

    /** Cycles `route.reconnect.actionBarFrames` on `route.reconnect.animationIntervalMillis`,
     *  independent of the retry schedule - the small animated "Reconnecting..." spinner under the
     *  title/subtitle, matching VelocityReconnect's animated reconnect messages. */
    private fun startAnimation(ctx: ChannelHandlerContext) {
        val frames = route.reconnect.actionBarFrames
        if (frames.isEmpty()) return
        animationTask = ctx.channel().eventLoop().scheduleAtFixedRate({
            if (ctx.channel().isActive) {
                ctx.writeAndFlush(encodeActionBar(ids, frames[animationFrame % frames.size], compressionThreshold))
                animationFrame++
            }
        }, 0, route.reconnect.animationIntervalMillis, TimeUnit.MILLISECONDS)
    }

    private fun scheduleRetry(ctx: ChannelHandlerContext) {
        retryTask = ctx.channel().eventLoop().schedule({
            attemptCount++
            val ordered = orderBackends(route, runtime, backends)
            tryBackends(ctx, ordered, 0)
        }, currentIntervalMillis, TimeUnit.MILLISECONDS)
        currentIntervalMillis = (currentIntervalMillis * route.reconnect.backoffMultiplier)
            .toLong().coerceAtMost(route.reconnect.maxRetryIntervalMillis)
    }

    private fun tryBackends(ctx: ChannelHandlerContext, ordered: List<InetSocketAddress>, i: Int) {
        if (done) return
        if (i >= ordered.size) {
            ctx.writeAndFlush(encodeSetSubtitleText(ids, "${route.reconnect.subtitle} (attempt $attemptCount)", compressionThreshold))
            if (route.reconnect.maxWaitMillis > 0 && System.currentTimeMillis() - enteredAt > route.reconnect.maxWaitMillis) {
                kickWithMessage(ctx, route.reconnect.kickMessage)
                return
            }
            scheduleRetry(ctx)
            return
        }
        // Status-only health check, using the actual client's protocol version so the backend's
        // status response (MOTD/version fields) matches what that client would really see.
        pingBackendLive(
            ctx.channel().eventLoop(), ordered[i], protocolVersion = protocolVersion, host, port,
            proxyProtocol = route.proxyProtocol
        )
            .whenComplete { _, err ->
                ctx.channel().eventLoop().execute {
                    if (done) return@execute
                    if (err != null) tryBackends(ctx, ordered, i + 1) else beginTransfer(ctx, ordered[i])
                }
            }
    }

    /** Client -> proxy packets while waiting to reconnect: only Acknowledge Configuration (the
     *  reply to [beginTransfer]'s Start Configuration) matters; everything else (keepalive
     *  replies, chat, movement, client info) is discarded. */
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
            if (payloadBuf !== buf) payloadBuf.release()
            id
        } catch (e: Exception) {
            log.debug("Failed to decode inbound reconnect-wait packet", e)
            -1
        }
        buf.readerIndex(frameEnd)

        if (packetId == ids.playAckConfiguration) {
            val action = onConfigurationAck
            onConfigurationAck = null
            action?.invoke(ctx)
        }
    }

    private fun beginTransfer(ctx: ChannelHandlerContext, addr: InetSocketAddress) {
        cancelSchedules()
        log.info("Backend '{}' reachable again for '{}', transferring '{}'", addr, host, playerName)
        awaitConfigurationAck(ctx) { ackCtx -> connectToBackend(ackCtx, addr) }
    }

    private fun connectToBackend(ctx: ChannelHandlerContext, addr: InetSocketAddress) {
        val clientChannel = ctx.channel()
        val bootstrap = Bootstrap()
            .group(clientChannel.eventLoop())
            .channel(clientChannel.javaClass)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(BackendLoginRelay(clientChannel, addr))
                }
            })

        bootstrap.connect(addr).addListener(ChannelFutureListener { future ->
            if (done) {
                if (future.isSuccess) future.channel().close()
                return@ChannelFutureListener
            }
            if (!future.isSuccess) {
                log.debug("Transfer to {} failed: {}", addr, future.cause()?.message)
                // Client is sitting mid-Configuration with no data; resend the waiting world and
                // resume the retry loop instead of leaving it stuck.
                sendWaitingWorld(ctx)
                startKeepAlive(ctx)
                startAnimation(ctx)
                scheduleRetry(ctx)
                return@ChannelFutureListener
            }
            val backendChannel = future.channel()
            done = true
            cancelSchedules()
            backendChannel.writeAndFlush(encodeHandshake(protocolVersion, addr.hostString, port, 2))
            backendChannel.writeAndFlush(encodeLoginStart(playerName, playerUuid))
            val existing = PlayerSessions.get(playerUuid)
            PlayerSessions.put(
                PlayerSession(playerName, playerUuid, host, existing?.remoteAddress ?: "?", addr, existing?.connectedAt ?: enteredAt)
            )
            runtime.recordConnectOpened(addr)
            backendChannel.closeFuture().addListener(ChannelFutureListener { runtime.recordConnectClosed(addr) })
            // The client pipeline deliberately stays on this ReconnectHandler for now - see
            // BackendLoginRelay, which only swaps it to a raw pipe once the backend's login
            // completes cleanly (Finish Configuration), and instead aborts back to the waiting
            // world if the backend turns out to require encryption (see below).
        })
    }

    /** Intercepts the backend's fresh login: swallows its Login Success (the client already
     *  completed login once, back when it started waiting to reconnect, and can't accept a second
     *  one on this connection), forwards everything else - including its Configuration-phase
     *  packets - raw to the client, then hands off to a plain byte splice once Finish
     *  Configuration goes by.
     *
     *  If the backend sends an Encryption Request instead, this transfer can never succeed:
     *  MCGate has no way to complete real Mojang session authentication on the player's behalf,
     *  and forwarding the request would desync the client (which isn't expecting a Login-state
     *  packet while sitting in the waiting world's Configuration state). Aborts back to the
     *  waiting world instead - this backend will keep failing every retry until it's switched to
     *  offline mode. */
    private inner class BackendLoginRelay(
        private val clientChannel: Channel,
        private val backendAddr: InetSocketAddress
    ) : ByteToMessageDecoder() {
        private var aborted = false

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
            val packetId = try {
                readVarInt(buf)
            } catch (e: IncompleteVarIntException) {
                buf.readerIndex(frameStart); return
            }
            val frameEnd = payloadStart + length

            if (packetId == LOGIN_ENCRYPTION_REQUEST) {
                buf.readerIndex(frameEnd)
                log.warn(
                    "Backend '{}' requires online-mode encryption MCGate can't complete on '{}''s " +
                        "behalf; staying in reconnect wait", backendAddr, playerName
                )
                aborted = true
                ctx.close()
                abortTransfer(clientChannel)
                return
            }

            if (packetId == ids.loginSuccess) {
                buf.readerIndex(frameEnd)
                return
            }

            buf.readerIndex(frameStart)
            val frameBytes = buf.readRetainedSlice(frameEnd - frameStart)
            if (clientChannel.isActive) clientChannel.writeAndFlush(frameBytes) else frameBytes.release()

            if (packetId == ids.configFinishConfiguration) {
                clientChannel.pipeline().replace("reconnect", "relay-to-backend", RawRelayHandler(ctx.channel()))
                ctx.pipeline().replace(this, "relay", RawRelayHandler(clientChannel))
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            if (!aborted) closeOnFlush(clientChannel)
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            log.debug("Backend transfer connection error", cause)
            closeOnFlush(ctx.channel())
        }
    }

    /** Called when a transfer attempt has to be aborted after already having connected to the
     *  backend (currently just the encryption case - see [BackendLoginRelay]). The client is
     *  still sitting on this ReconnectHandler instance (the pipeline swap only happens once a
     *  transfer succeeds), so this just resumes the waiting world in place. */
    private fun abortTransfer(clientChannel: Channel) {
        done = false
        if (!clientChannel.isActive) return
        val ctx = clientChannel.pipeline().context(this@ReconnectHandler) ?: return
        sendWaitingWorld(ctx)
        startKeepAlive(ctx)
        startAnimation(ctx)
        scheduleRetry(ctx)
    }

    /** Kicks a player already waiting to reconnect (e.g. `reconnect.maxWait` expired) - by this
     *  point the client is always past Login state, so this uses the Play-state Disconnect. */
    private fun kickWithMessage(ctx: ChannelHandlerContext, message: String) {
        done = true
        cancelSchedules()
        ctx.writeAndFlush(encodePlayDisconnect(ids, message, compressionThreshold)).addListener(ChannelFutureListener.CLOSE)
    }

    private fun cancelSchedules() {
        keepAliveTask?.cancel(false)
        retryTask?.cancel(false)
        animationTask?.cancel(false)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        done = true
        cancelSchedules()
        PlayerSessions.remove(playerUuid)
        super.channelInactive(ctx)
    }

    override fun handlerRemoved0(ctx: ChannelHandlerContext) {
        cancelSchedules()
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.debug("Reconnect-wait connection error", cause)
        done = true
        cancelSchedules()
        closeOnFlush(ctx.channel())
    }
}

/** Plain raw byte pipe from whichever channel it's installed on to [target] - the same splice
 *  pattern LoginRelayHandler uses once a backend is connected. */
private class RawRelayHandler(private val target: Channel) : SimpleChannelInboundHandler<ByteBuf>() {
    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
        if (target.isActive) target.writeAndFlush(msg.retain())
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        closeOnFlush(target)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        closeOnFlush(ctx.channel())
    }
}

private fun closeOnFlush(channel: Channel?) {
    if (channel != null && channel.isActive) {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE)
    }
}
