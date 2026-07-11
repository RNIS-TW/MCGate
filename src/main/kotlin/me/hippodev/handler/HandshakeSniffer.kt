package me.hippodev.handler

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import me.hippodev.config.*
import me.hippodev.routing.*
import me.hippodev.protocol.*
import org.slf4j.LoggerFactory

/**
 * Minimal Minecraft handshake decoder.
 *
 * Reads only the very first packet (handshake, packet id 0x00), extracts the
 * fields Lite mode needs to route (host, port, next state), then hands off to
 * [onHandshake] and removes itself from the pipeline. Any bytes already
 * buffered past the handshake frame are automatically re-delivered by Netty
 * to whichever handler [onHandshake] installs next.
 */
class HandshakeSniffer(
    private val onHandshake: (
        ctx: ChannelHandlerContext,
        protocolVersion: Int,
        host: String,
        port: Int,
        nextState: Int,
        rawFrame: ByteBuf
    ) -> Unit
) : ByteToMessageDecoder() {

    private val log = LoggerFactory.getLogger(HandshakeSniffer::class.java)

    override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
        val frameStart = buf.readerIndex()

        val length = try {
            readVarInt(buf)
        } catch (e: IncompleteVarIntException) {
            buf.readerIndex(frameStart)
            return
        }

        if (length < 0 || length > 1 shl 21) {
            log.debug("Rejecting connection with bogus handshake length {}", length)
            ctx.close()
            return
        }

        val headerLen = buf.readerIndex() - frameStart
        if (buf.readableBytes() < length) {
            buf.readerIndex(frameStart)
            return
        }

        val packetId = try {
            readVarInt(buf)
        } catch (e: IncompleteVarIntException) {
            ctx.close()
            return
        }

        if (packetId != 0x00) {
            log.debug("Rejecting connection: first packet id was {}, expected handshake (0)", packetId)
            ctx.close()
            return
        }

        val protocolVersion = readVarInt(buf)

        val hostLength = readVarInt(buf)
        if (hostLength < 0 || hostLength > 255) {
            ctx.close()
            return
        }
        val hostBytes = ByteArray(hostLength)
        buf.readBytes(hostBytes)
        var host = String(hostBytes, Charsets.UTF_8)

        // Strip Forge/FML markers appended after a NUL char (e.g. "host FML...").
        val nulIdx = host.indexOf('\u0000')
        if (nulIdx >= 0) host = host.substring(0, nulIdx)

        val port = buf.readUnsignedShort()
        val nextState = readVarInt(buf)

        // Re-slice the full raw frame (length varint + payload) to forward untouched
        // when we don't need to rewrite it (i.e. modifyVirtualHost is off).
        buf.readerIndex(frameStart)
        val rawFrame = buf.readRetainedSlice(headerLen + length)

        onHandshake(ctx, protocolVersion, host, port, nextState, rawFrame)
        ctx.pipeline().remove(this)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // Benign in practice - server-list pingers/scanners routinely RST instead of a clean
        // close before or during the handshake. Log quietly instead of letting it fall through
        // to Netty's tail-context WARN + full stack trace.
        log.debug("Handshake connection error", cause)
        ctx.close()
    }
}
