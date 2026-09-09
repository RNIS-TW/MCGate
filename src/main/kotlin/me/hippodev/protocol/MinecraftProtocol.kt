package me.hippodev.protocol

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.net.InetSocketAddress

class IncompleteVarIntException : Exception()

/**
 * Minecraft's hard ceiling on a single (uncompressed) packet: 2^21 - 1 bytes. The vanilla client
 * and server both reject anything larger. Every [io.netty.handler.codec.ByteToMessageDecoder] in
 * MCGate uses this to bound the frame length it's willing to buffer: without it, a peer that
 * declares a huge frame length and then dribbles bytes makes the decoder's cumulation buffer grow
 * toward that size, one held connection at a time - a cheap memory-amplification DDoS. A declared
 * length past this is malformed on its face; close the connection rather than buffer for it.
 */
const val MAX_PACKET_BYTES = (1 shl 21) - 1

/** Reads a frame-length varint and range-checks it. Returns null (leaving [buf]'s reader index
 *  reset to [frameStart]) if the varint isn't fully buffered yet; throws [IllegalStateException]
 *  for a negative or over-[MAX_PACKET_BYTES] length. */
fun readFrameLength(buf: ByteBuf, frameStart: Int): Int? {
    val length = try {
        readVarInt(buf)
    } catch (e: IncompleteVarIntException) {
        buf.readerIndex(frameStart)
        return null
    }
    if (length < 0 || length > MAX_PACKET_BYTES) {
        throw IllegalStateException("frame length $length out of range (max $MAX_PACKET_BYTES)")
    }
    return length
}

/** Login state, clientbound - stable since the encryption handshake was introduced. Signals the
 *  backend is online-mode; everything after the client's Encryption Response is AES-CFB8
 *  ciphertext that a plain packet parser (like the ones in LoginRelayHandler/ReconnectHandler) can no
 *  longer decode. */
const val LOGIN_ENCRYPTION_REQUEST = 0x01

fun readVarInt(buf: ByteBuf): Int {
    var result = 0
    var shift = 0
    while (true) {
        if (!buf.isReadable) throw IncompleteVarIntException()
        val b = buf.readByte().toInt()
        result = result or ((b and 0x7F) shl shift)
        if (b and 0x80 == 0) break
        shift += 7
        if (shift >= 35) throw IllegalStateException("VarInt too big")
    }
    return result
}

fun writeVarInt(buf: ByteBuf, valueIn: Int) {
    var value = valueIn
    while (true) {
        if (value and 0x7F.inv() == 0) {
            buf.writeByte(value)
            return
        }
        buf.writeByte((value and 0x7F) or 0x80)
        value = value ushr 7
    }
}

fun readString(buf: ByteBuf, maxLength: Int = 32767): String {
    val length = readVarInt(buf)
    if (length < 0 || length > maxLength * 4) throw IllegalStateException("String too long: $length")
    val bytes = ByteArray(length)
    buf.readBytes(bytes)
    return String(bytes, Charsets.UTF_8)
}

fun writeString(buf: ByteBuf, value: String) {
    val bytes = value.toByteArray(Charsets.UTF_8)
    writeVarInt(buf, bytes.size)
    buf.writeBytes(bytes)
}

/** Encodes a length-prefixed handshake packet (id 0x00) from scratch. */
/** PROXY protocol v1 preamble - required before the handshake on any backend connection when the
 *  route has `proxyProtocol: true`, including status/health-check probes: a backend enforcing it
 *  will silently withhold its response (never closing the connection either) for any dial that
 *  skips this header, which looks identical to a hung/unreachable backend from the caller's side. */
fun encodeProxyProtocolHeader(sourceAddr: InetSocketAddress, destAddr: InetSocketAddress): ByteBuf {
    val proto = if (sourceAddr.address is java.net.Inet6Address) "TCP6" else "TCP4"
    val header = "PROXY $proto ${sourceAddr.address.hostAddress} ${destAddr.address.hostAddress} " +
        "${sourceAddr.port} ${destAddr.port}\r\n"
    return Unpooled.wrappedBuffer(header.toByteArray(Charsets.US_ASCII))
}

fun encodeHandshake(protocolVersion: Int, host: String, port: Int, nextState: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, 0x00)
    writeVarInt(payload, protocolVersion)
    writeString(payload, host)
    payload.writeShort(port)
    writeVarInt(payload, nextState)

    val frame = Unpooled.buffer()
    writeVarInt(frame, payload.readableBytes())
    frame.writeBytes(payload)
    payload.release()
    return frame
}

/** Encodes a length-prefixed Status Response packet (id 0x00) carrying the given JSON. */
fun encodeStatusResponse(json: String): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, 0x00)
    writeString(payload, json)

    val frame = Unpooled.buffer()
    writeVarInt(frame, payload.readableBytes())
    frame.writeBytes(payload)
    payload.release()
    return frame
}

/**
 * Best-effort parse of a Login Start packet (id 0x00): username, and UUID if
 * the packet has exactly a trailing 16 bytes after the name (the shape used
 * by modern protocols that send a mandatory UUID with no other trailing
 * fields). Reads from a duplicate of [buf] so the caller's reader index -
 * and the raw bytes still being relayed untouched - are unaffected. Returns
 * null if [buf] isn't a complete, well-formed Login Start packet yet.
 */
fun parseLoginStart(buf: ByteBuf): Pair<String, java.util.UUID?>? {
    val dup = buf.duplicate()
    return try {
        val length = readVarInt(dup)
        val payloadStart = dup.readerIndex()
        if (dup.readableBytes() < length) return null
        val packetId = readVarInt(dup)
        if (packetId != 0x00) return null
        val name = readString(dup, 16)
        val payloadEnd = payloadStart + length
        val remaining = payloadEnd - dup.readerIndex()
        val uuid = if (remaining == 16) {
            val most = dup.readLong()
            val least = dup.readLong()
            java.util.UUID(most, least)
        } else null
        name to uuid
    } catch (e: Exception) {
        null
    }
}

/** Encodes a length-prefixed Login Start packet (id 0x00) - the inverse of [parseLoginStart],
 *  used to replay a client's login against a backend without the original raw bytes on hand
 *  (e.g. after the connection has moved past the point where they were buffered). */
fun encodeLoginStart(name: String, uuid: java.util.UUID?): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, 0x00)
    writeString(payload, name)
    if (uuid != null) {
        payload.writeLong(uuid.mostSignificantBits)
        payload.writeLong(uuid.leastSignificantBits)
    }

    val frame = Unpooled.buffer()
    writeVarInt(frame, payload.readableBytes())
    frame.writeBytes(payload)
    payload.release()
    return frame
}

/** Encodes a length-prefixed Pong packet (id 0x01) echoing the ping payload. */
fun encodePong(payload: Long): ByteBuf {
    val body = Unpooled.buffer()
    writeVarInt(body, 0x01)
    body.writeLong(payload)

    val frame = Unpooled.buffer()
    writeVarInt(frame, body.readableBytes())
    frame.writeBytes(body)
    body.release()
    return frame
}
