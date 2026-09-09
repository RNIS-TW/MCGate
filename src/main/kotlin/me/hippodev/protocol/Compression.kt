package me.hippodev.protocol

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.zip.Deflater
import java.util.zip.Inflater

/**
 * Minecraft's packet compression framing (stable since 1.8): once a connection has negotiated a
 * compression threshold (via the Login-state Set Compression packet), every packet afterward is
 * framed as `[packet length][data length][payload]`, where `data length` is 0 for a payload left
 * uncompressed (below threshold) or the decompressed size for a zlib-deflated payload.
 *
 * MCGate's raw byte relay never needed this - it just tunnels whatever bytes the backend and
 * client exchange. It only matters for packets MCGate *synthesizes* itself (the reconnect-wait world),
 * which must match whatever framing the client's decoder is already expecting for that
 * connection - see [me.hippodev.handler.LoginRelayHandler]'s backend login sniffing and
 * [me.hippodev.handler.ReconnectHandler].
 */

/** Frames [payload] (packet id + fields, unframed) for the wire, applying compression if
 *  [compressionThreshold] is >= 0. Always releases [payload]. */
fun frame(payload: ByteBuf, compressionThreshold: Int): ByteBuf {
    if (compressionThreshold < 0) {
        val out = Unpooled.buffer()
        writeVarInt(out, payload.readableBytes())
        out.writeBytes(payload)
        payload.release()
        return out
    }

    val uncompressedSize = payload.readableBytes()
    val inner = Unpooled.buffer()
    if (uncompressedSize < compressionThreshold) {
        writeVarInt(inner, 0)
        inner.writeBytes(payload)
    } else {
        val src = ByteArray(uncompressedSize)
        payload.readBytes(src)
        writeVarInt(inner, uncompressedSize)
        inner.writeBytes(deflate(src))
    }
    payload.release()

    val out = Unpooled.buffer()
    writeVarInt(out, inner.readableBytes())
    out.writeBytes(inner)
    inner.release()
    return out
}

private fun deflate(src: ByteArray): ByteArray {
    val deflater = Deflater()
    deflater.setInput(src)
    deflater.finish()
    val out = java.io.ByteArrayOutputStream(src.size)
    val buf = ByteArray(4096)
    while (!deflater.finished()) {
        val n = deflater.deflate(buf)
        out.write(buf, 0, n)
    }
    deflater.end()
    return out.toByteArray()
}

/**
 * Reads the packet id of the frame at [buf]'s current position (which must be right after the
 * outer packet-length varint, with the frame's payload ending at [frameEnd]), honoring
 * [compressionThreshold]. Returns the packet id and a ByteBuf positioned right after it, ready to
 * read the packet's fields - the uncompressed case reuses [buf] itself; the compressed case
 * returns a freshly-inflated buffer the caller must release once done (compare with `!==`).
 * Does not otherwise move [buf]'s reader index past what it consumed - callers that only need
 * the packet id (not its fields) should reset/advance [buf] to [frameEnd] themselves afterward.
 */
fun readCompressedFrame(buf: ByteBuf, frameEnd: Int, compressionThreshold: Int): Pair<Int, ByteBuf> {
    if (compressionThreshold < 0) {
        return readVarInt(buf) to buf
    }
    val dataLength = readVarInt(buf)
    if (dataLength == 0) {
        return readVarInt(buf) to buf
    }
    // dataLength is the peer's claim of the *decompressed* size - it drives the ByteArray
    // inflate() allocates below. Unbounded, one crafted frame (dataLength ~= Int.MAX) is an
    // instant OutOfMemoryError, which with -XX:+ExitOnOutOfMemoryError takes the whole proxy
    // down. Minecraft never frames a packet larger than MAX_PACKET_BYTES; anything past that
    // is malformed.
    if (dataLength < 0 || dataLength > MAX_PACKET_BYTES) {
        throw IllegalStateException("compressed data length $dataLength out of range (max $MAX_PACKET_BYTES)")
    }
    val compressedLen = frameEnd - buf.readerIndex()
    if (compressedLen < 0 || compressedLen > MAX_PACKET_BYTES) {
        throw IllegalStateException("compressed payload length $compressedLen out of range")
    }
    val compressed = ByteArray(compressedLen)
    buf.getBytes(buf.readerIndex(), compressed)
    val inflated = Unpooled.wrappedBuffer(inflate(compressed, dataLength))
    return readVarInt(inflated) to inflated
}

fun inflate(src: ByteArray, expectedSize: Int): ByteArray {
    val inflater = Inflater()
    inflater.setInput(src)
    val out = ByteArray(expectedSize)
    var written = 0
    while (written < expectedSize && !inflater.finished()) {
        val n = inflater.inflate(out, written, expectedSize - written)
        // inflate() returns 0 when it needs more input that will never come (a truncated or
        // lying frame) - without this the loop spins forever, pinning an event-loop thread.
        if (n == 0) break
        written += n
    }
    inflater.end()
    return out
}
