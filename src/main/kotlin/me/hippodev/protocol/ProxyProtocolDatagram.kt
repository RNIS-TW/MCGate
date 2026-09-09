package me.hippodev.protocol

import io.netty.buffer.ByteBuf
import java.net.InetAddress
import java.net.InetSocketAddress

/**
 * Minimal PROXY protocol (v1 and v2) header parser for the datagram case - an L4 proxy such as
 * Cloudflare Spectrum prepends one of these to *every* UDP datagram it forwards to the origin, so
 * MCGate's UDP relays ([me.hippodev.voice.VoiceRelay]) need to strip it per packet to recover the
 * real client address.
 *
 * Netty's `netty-codec-haproxy` only exposes this as a stream decoder (`HAProxyMessageDecoder`, a
 * `ByteToMessageDecoder`); its one-shot `HAProxyMessage.decodeHeader` helper is package-private.
 * Standing up an `EmbeddedChannel` per datagram just to reuse it would be far more overhead than
 * this handful of bytes of parsing.
 */

/** PROXY v2 12-byte signature: `\r\n\r\n\0\r\nQUIT\n`. */
private val V2_SIGNATURE = byteArrayOf(
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
)
private val V1_PREFIX = "PROXY ".toByteArray(Charsets.US_ASCII)

/** Thrown when [buf] does not start with a well-formed PROXY protocol header. */
class ProxyProtocolFormatException(message: String) : Exception(message)

/**
 * Parses a PROXY protocol header from the front of [buf], advancing its reader index to the first
 * byte after the header (the start of the real payload). Returns the source address the header
 * carries, or null for a v2 `LOCAL` command / an `UNSPEC` address family (health checks and the
 * like - no client address to route by). Throws [ProxyProtocolFormatException] if the bytes at
 * the reader index are not a valid header.
 */
fun parseProxyProtocolHeader(buf: ByteBuf): InetSocketAddress? {
    if (buf.readableBytes() >= 16 && startsWith(buf, V2_SIGNATURE)) return parseV2(buf)
    if (startsWith(buf, V1_PREFIX)) return parseV1(buf)
    throw ProxyProtocolFormatException("no PROXY protocol v1 or v2 signature")
}

private fun startsWith(buf: ByteBuf, prefix: ByteArray): Boolean {
    if (buf.readableBytes() < prefix.size) return false
    val base = buf.readerIndex()
    for (i in prefix.indices) {
        if (buf.getByte(base + i) != prefix[i]) return false
    }
    return true
}

private fun parseV2(buf: ByteBuf): InetSocketAddress? {
    val start = buf.readerIndex()
    val verCmd = buf.getUnsignedByte(start + 12).toInt()
    if (verCmd shr 4 != 0x2) throw ProxyProtocolFormatException("PROXY v2 version nibble != 2")
    val command = verCmd and 0x0F // 0 = LOCAL, 1 = PROXY

    val famProto = buf.getUnsignedByte(start + 13).toInt()
    val family = famProto shr 4 // 0 = UNSPEC, 1 = INET, 2 = INET6

    val addrLen = buf.getUnsignedShort(start + 14)
    val headerLen = 16 + addrLen
    if (buf.readableBytes() < headerLen) {
        throw ProxyProtocolFormatException("PROXY v2 header truncated (need $headerLen bytes)")
    }

    val result: InetSocketAddress? = when {
        command == 0 || family == 0 -> null // LOCAL or UNSPEC - nothing to route by
        family == 1 && addrLen >= 12 -> {
            val addr = ByteArray(4).also { buf.getBytes(start + 16, it) }
            val port = buf.getUnsignedShort(start + 24)
            InetSocketAddress(InetAddress.getByAddress(addr), port)
        }
        family == 2 && addrLen >= 36 -> {
            val addr = ByteArray(16).also { buf.getBytes(start + 16, it) }
            val port = buf.getUnsignedShort(start + 48)
            InetSocketAddress(InetAddress.getByAddress(addr), port)
        }
        else -> throw ProxyProtocolFormatException("PROXY v2 address block too short for family $family")
    }

    buf.readerIndex(start + headerLen)
    return result
}

private fun parseV1(buf: ByteBuf): InetSocketAddress? {
    val start = buf.readerIndex()
    // The line ends with CRLF and is at most 107 bytes including it.
    val limit = minOf(buf.readableBytes(), 108)
    var crlf = -1
    for (i in 1 until limit) {
        if (buf.getByte(start + i - 1) == 0x0D.toByte() && buf.getByte(start + i) == 0x0A.toByte()) {
            crlf = i - 1
            break
        }
    }
    if (crlf < 0) throw ProxyProtocolFormatException("PROXY v1 header has no CRLF within 107 bytes")

    val line = buf.toString(start, crlf, Charsets.US_ASCII)
    buf.readerIndex(start + crlf + 2)

    val parts = line.split(' ')
    // "PROXY UNKNOWN ..." - connection info withheld, nothing to route by.
    if (parts.size >= 2 && parts[1] == "UNKNOWN") return null
    if (parts.size < 6) throw ProxyProtocolFormatException("PROXY v1 header has too few fields: '$line'")
    return try {
        InetSocketAddress(InetAddress.getByName(parts[2]), parts[4].toInt())
    } catch (e: Exception) {
        throw ProxyProtocolFormatException("PROXY v1 header has an unparseable source address: '$line'")
    }
}
