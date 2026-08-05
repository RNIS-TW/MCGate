package me.hippodev.handler

import com.google.gson.JsonParser
import com.google.gson.JsonSyntaxException
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import me.hippodev.config.*
import me.hippodev.routing.*
import me.hippodev.protocol.*
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/** Whether [json] is a well-formed Server List Ping status response - specifically, that
 *  `version.protocol` is present and numeric, since that's the field a malformed/misbehaving
 *  backend most often gets wrong and the one whose absence produces the most confusing
 *  client-side error. Deliberately permissive about everything else in the JSON. */
private fun isValidStatusJson(json: String): Boolean {
    return try {
        @Suppress("DEPRECATION") // JsonParser().parse(...) - the transitively-pulled gson version
        // (2.8.0, via adventure-text-serializer-gson) predates the modern static
        // JsonParser.parseString API; this instance-method form still works fine on it.
        val root = JsonParser().parse(json).asJsonObject
        val version = root.getAsJsonObject("version") ?: return false
        val protocol = version.get("protocol") ?: return false
        protocol.asJsonPrimitive.isNumber
    } catch (e: JsonSyntaxException) {
        false
    } catch (e: IllegalStateException) {
        // e.g. asJsonObject/asJsonPrimitive called on something that isn't one
        false
    }
}

/**
 * Handles a connection past the handshake once we know next_state == 1 (status).
 * Serves a cached or fallback status response without touching the backend when
 * possible; otherwise dials the backend live, forwards its response, and caches it.
 */
class StatusHandler(
    private val route: Route,
    private val runtime: RouteRuntime,
    private val backends: List<InetSocketAddress>,
    private val protocolVersion: Int,
    private val host: String,
    private val port: Int,
    private val pingCache: PingCache
) : ByteToMessageDecoder() {

    private val log = LoggerFactory.getLogger(StatusHandler::class.java)

    override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
        val frameStart = buf.readerIndex()
        val length = try {
            readVarInt(buf)
        } catch (e: IncompleteVarIntException) {
            buf.readerIndex(frameStart); return
        }
        if (buf.readableBytes() < length) {
            buf.readerIndex(frameStart); return
        }
        val payloadStart = buf.readerIndex()
        val packetId = readVarInt(buf)
        if (packetId != 0x00) {
            // Not a Status Request (could be a stray Ping without a request) - ignore/close.
            ctx.close()
            return
        }
        // Status Request has no further payload; skip anything unexpected past the id.
        buf.skipBytes(length - (buf.readerIndex() - payloadStart))

        handleStatusRequest(ctx)

        // Swap in a small handler for the trailing optional Ping packet.
        ctx.pipeline().addAfter(ctx.name(), "ping", PingPongHandler())
        ctx.pipeline().remove(this)
    }

    private fun handleStatusRequest(ctx: ChannelHandlerContext) {
        // Order once per request: strategies like round-robin/random must not be
        // re-rolled on every retry, and each backend gets checked against its own cache entry.
        val ordered = orderBackends(route, runtime, backends)
        tryBackend(ctx, ordered, 0)
    }

    private fun tryBackend(ctx: ChannelHandlerContext, ordered: List<InetSocketAddress>, attempt: Int) {
        if (attempt >= ordered.size) {
            serveFallbackOrClose(ctx)
            return
        }
        val addr = ordered[attempt]
        // The response cache key includes protocolVersion - a version-multiplexing backend (e.g.
        // ViaVersion) resolves and reports a different version string/name per client, so a
        // response cached for one client's protocol version must never be served back to a
        // different one; that would silently defeat the whole point of forwarding the client's
        // real protocol version to the backend in the first place (see dialBackendForStatus).
        // Reachability (down-tracking) is different - a dead backend is dead regardless of which
        // protocol version asked, so that still just keys off host:port.
        val downKey = "${addr.hostString}:${addr.port}"
        val responseCacheKey = "$downKey:$protocolVersion"
        val cached = pingCache.get(responseCacheKey)
        if (cached != null) {
            ctx.writeAndFlush(encodeStatusResponse(cached))
            return
        }
        if (pingCache.isKnownDown(downKey)) {
            tryBackend(ctx, ordered, attempt + 1)
            return
        }
        dialBackendForStatus(ctx, ordered, attempt)
    }

    private fun dialBackendForStatus(ctx: ChannelHandlerContext, ordered: List<InetSocketAddress>, attempt: Int) {
        val addr = ordered[attempt]
        val downKey = "${addr.hostString}:${addr.port}"
        val responseCacheKey = "$downKey:$protocolVersion"
        val startTime = System.currentTimeMillis()

        val bootstrap = Bootstrap()
            .group(ctx.channel().eventLoop())
            .channel(ctx.channel().javaClass)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(BackendStatusFetcher { json ->
                        runtime.recordLatency(addr, System.currentTimeMillis() - startTime)
                        // MCGate otherwise relays this straight to the connecting client with zero
                        // validation - a backend sending a malformed response (missing/non-numeric
                        // version.protocol, truncated JSON, a misbehaving plugin, whatever) used to
                        // get forwarded as-is, which the client then fails to parse and surfaces as
                        // a confusing client-side error. Worse, it also got cached and served to
                        // every other client hitting this backend for the TTL window. Validate
                        // first and treat a bad response the same as an unreachable backend -
                        // never expose it to a player, just fail over to the next backend/fallback.
                        if (!isValidStatusJson(json)) {
                            log.warn("Backend {} sent a malformed status response, treating as down", addr)
                            pingCache.markDown(downKey)
                            if (ctx.channel().isActive) tryBackend(ctx, ordered, attempt + 1)
                            return@BackendStatusFetcher
                        }
                        pingCache.put(responseCacheKey, json, route.cachePingTTLMillis)
                        if (ctx.channel().isActive) {
                            ctx.writeAndFlush(encodeStatusResponse(json))
                        }
                    })
                }
            })

        bootstrap.connect(addr).addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.debug("Status dial to {} failed: {}", addr, future.cause()?.message)
                pingCache.markDown(downKey)
                tryBackend(ctx, ordered, attempt + 1)
                return@ChannelFutureListener
            }
            val backendChannel = future.channel()
            // Backends behind proxyProtocol expect the PROXY header on every connection,
            // including status probes - without it, a strict backend just never responds
            // (connection looks "up" but hangs forever), which looked identical to a dead
            // backend from here. Client's real remote address is available since this probe
            // rides on the same ctx as the actual player connection.
            if (route.proxyProtocol) {
                val clientAddr = ctx.channel().effectiveRemoteAddress() as? InetSocketAddress
                if (clientAddr != null) {
                    backendChannel.writeAndFlush(encodeProxyProtocolHeader(clientAddr, addr))
                }
            }
            // Forward the connecting client's real protocol version so version-multiplexing
            // backends (e.g. ViaVersion) resolve and report the version the client actually
            // asked for, instead of falling back to their own default/custom protocol name -
            // which otherwise shows up as a version mismatch on the client's server list entry.
            backendChannel.writeAndFlush(encodeHandshake(protocolVersion, host, port, 1))
            val requestFrame = Unpooled.buffer()
            writeVarInt(requestFrame, 1)
            writeVarInt(requestFrame, 0x00)
            backendChannel.writeAndFlush(requestFrame)
        })
    }

    private fun serveFallbackOrClose(ctx: ChannelHandlerContext) {
        val fallback = route.fallback
        if (fallback == null) {
            ctx.close()
            return
        }
        ctx.writeAndFlush(encodeStatusResponse(buildFallbackJson(fallback)))
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // Benign in practice - server-list pingers/scanners routinely RST instead of a clean
        // close. Log quietly instead of letting it fall through to Netty's tail-context WARN.
        log.debug("Status connection error", cause)
        ctx.close()
    }
}

/** Connects to a backend just long enough to read its Status Response JSON. */
private class BackendStatusFetcher(private val onResult: (String) -> Unit) :
    ByteToMessageDecoder() {

    private val log = LoggerFactory.getLogger(BackendStatusFetcher::class.java)

    override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
        val frameStart = buf.readerIndex()
        val length = try {
            readVarInt(buf)
        } catch (e: IncompleteVarIntException) {
            buf.readerIndex(frameStart); return
        }
        if (buf.readableBytes() < length) {
            buf.readerIndex(frameStart); return
        }
        val packetId = readVarInt(buf)
        if (packetId == 0x00) {
            val json = readString(buf, 262144)
            onResult(json)
        }
        ctx.close()
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        log.debug("Backend status connection error", cause)
        ctx.close()
    }
}

/** Echoes the client's Ping payload back as a Pong, then closes. */
private class PingPongHandler : SimpleChannelInboundHandler<ByteBuf>() {
    private val log = LoggerFactory.getLogger(PingPongHandler::class.java)

    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
        val frameStart = msg.readerIndex()
        val length = readVarInt(msg)
        if (msg.readableBytes() < length) {
            msg.readerIndex(frameStart)
            return
        }
        val packetId = readVarInt(msg)
        if (packetId == 0x01 && msg.readableBytes() >= 8) {
            val payload = msg.readLong()
            ctx.writeAndFlush(encodePong(payload)).addListener(ChannelFutureListener.CLOSE)
        } else {
            ctx.close()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // Benign in practice - clients frequently RST right after reading the pong instead of
        // a clean close.
        log.debug("Ping connection error", cause)
        ctx.close()
    }
}
