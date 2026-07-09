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
 * client exchange. It only matters for packets MCGate *synthesizes* itself (the limbo world),
 * which must match whatever framing the client's decoder is already expecting for that
 * connection - see [me.hippodev.handler.LoginRelayHandler]'s backend login sniffing and
 * [me.hippodev.handler.LimboHandler].
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
    val compressed = ByteArray(frameEnd - buf.readerIndex())
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
        written += inflater.inflate(out, written, expectedSize - written)
    }
    inflater.end()
    return out
}
