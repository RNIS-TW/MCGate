package me.hippodev.protocol

import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ProxyProtocolDatagramTest {

    private fun v2Ipv4(src: String, srcPort: Int, payload: ByteArray): ByteArray {
        val sig = byteArrayOf(0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A)
        val addr = src.split('.').map { it.toInt().toByte() }.toByteArray()
        val block = addr + byteArrayOf(0, 0, 0, 0) +
            byteArrayOf((srcPort shr 8).toByte(), srcPort.toByte(), 0, 0)
        val header = sig + byteArrayOf(0x21, 0x11, (block.size shr 8).toByte(), block.size.toByte()) + block
        return header + payload
    }

    @Test
    fun `v2 IPv4 header yields source address and leaves payload`() {
        val payload = byteArrayOf(1, 2, 3, 4, 5)
        val buf = Unpooled.wrappedBuffer(v2Ipv4("128.118.12.30", 35470, payload))

        val src = parseProxyProtocolHeader(buf)!!
        assertEquals("128.118.12.30", src.address.hostAddress)
        assertEquals(35470, src.port)

        val rest = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
        assertArrayEquals(payload, rest)
    }

    @Test
    fun `v1 IPv4 header yields source address and leaves payload`() {
        val line = "PROXY TCP4 128.118.12.30 172.16.27.106 35470 25565\r\n"
        val payload = byteArrayOf(9, 8, 7)
        val buf = Unpooled.wrappedBuffer(line.toByteArray(Charsets.US_ASCII) + payload)

        val src = parseProxyProtocolHeader(buf)!!
        assertEquals("128.118.12.30", src.address.hostAddress)
        assertEquals(35470, src.port)

        val rest = ByteArray(buf.readableBytes()).also { buf.readBytes(it) }
        assertArrayEquals(payload, rest)
    }

    @Test
    fun `v2 LOCAL command returns null but still consumes the header`() {
        val sig = byteArrayOf(0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A)
        val header = sig + byteArrayOf(0x20, 0x00, 0x00, 0x00) // v2, LOCAL, UNSPEC, no address block
        val payload = byteArrayOf(42)
        val buf = Unpooled.wrappedBuffer(header + payload)

        assertNull(parseProxyProtocolHeader(buf))
        assertEquals(1, buf.readableBytes())
    }

    @Test
    fun `v1 UNKNOWN returns null`() {
        val buf = Unpooled.wrappedBuffer("PROXY UNKNOWN\r\nx".toByteArray(Charsets.US_ASCII))
        assertNull(parseProxyProtocolHeader(buf))
        assertEquals('x'.code, buf.readByte().toInt())
    }

    @Test
    fun `a datagram with no PROXY header throws`() {
        val buf = Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16))
        assertThrows(ProxyProtocolFormatException::class.java) { parseProxyProtocolHeader(buf) }
    }
}
