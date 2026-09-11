package me.hippodev.handler

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder
import me.hippodev.protocol.REAL_REMOTE_ADDRESS
import me.hippodev.protocol.effectiveRemoteAddress
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress

class ProxyProtocolAttributeHandlerTest {

    private val V2_SIG = byteArrayOf(0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A)

    private fun v2(verCmd: Int, fam: Int, block: ByteArray) =
        V2_SIG + byteArrayOf(verCmd.toByte(), fam.toByte(), (block.size shr 8).toByte(), block.size.toByte()) + block

    private fun tcp4(srcIp: String, srcPort: Int, dstIp: String = "10.0.0.1", dstPort: Int = 25565): ByteArray {
        fun ip(s: String) = s.split('.').map { it.toInt().toByte() }.toByteArray()
        return ip(srcIp) + ip(dstIp) +
            byteArrayOf((srcPort shr 8).toByte(), srcPort.toByte(), (dstPort shr 8).toByte(), dstPort.toByte())
    }

    private fun channel() = EmbeddedChannel(HAProxyMessageDecoder(), ProxyProtocolAttributeHandler())

    private fun feed(bytes: ByteArray): EmbeddedChannel =
        channel().apply { writeInbound(Unpooled.wrappedBuffer(bytes)) }

    @Test
    fun `real PROXY TCP4 header sets the forwarded client address`() {
        val ch = feed(v2(0x21, 0x11, tcp4("203.0.113.9", 51000)))
        val addr = ch.attr(REAL_REMOTE_ADDRESS).get() as InetSocketAddress
        assertEquals("203.0.113.9", addr.address.hostAddress)
        assertEquals(51000, addr.port)
    }

    @Test
    fun `v2 LOCAL command falls back to the real peer`() {
        val ch = feed(v2(0x20, 0x00, ByteArray(0)))
        assertNull(ch.attr(REAL_REMOTE_ADDRESS).get())
        assertEquals(ch.remoteAddress(), ch.effectiveRemoteAddress())
    }

    @Test
    fun `PROXY header with a loopback source falls back to the real peer`() {
        val ch = feed(v2(0x21, 0x11, tcp4("127.0.0.1", 0)))
        assertNull(ch.attr(REAL_REMOTE_ADDRESS).get())
    }

    @Test
    fun `PROXY header with a wildcard source falls back to the real peer`() {
        val ch = feed(v2(0x21, 0x11, tcp4("0.0.0.0", 0)))
        assertNull(ch.attr(REAL_REMOTE_ADDRESS).get())
    }

    @Test
    fun `PROXY header with a real source but port 0 falls back to the real peer`() {
        val ch = feed(v2(0x21, 0x11, tcp4("198.51.100.7", 0)))
        assertNull(ch.attr(REAL_REMOTE_ADDRESS).get())
    }

    @Test
    fun `the handler removes itself after the header`() {
        val ch = feed(v2(0x21, 0x11, tcp4("1.2.3.4", 1234)))
        assertNull(ch.pipeline().get(ProxyProtocolAttributeHandler::class.java))
    }
}
