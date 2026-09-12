package me.hippodev.voice

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import me.hippodev.config.UdpThrottleConfig
import me.hippodev.udp.UdpThrottle
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * End-to-end exercise of [VoiceRelay]'s datagram path without a real Simple Voice Chat client:
 * the relay treats voice traffic as an opaque NAT keyed by client IP (see its class doc), so a
 * plain UDP echo backend plus raw datagrams is enough to prove the forward path, the return path,
 * and the PROXY-protocol handling.
 */
class VoiceRelayTest {

    private lateinit var group: EventLoopGroup
    private lateinit var backend: Channel
    private val backendReceived = LinkedBlockingQueue<ByteArray>()

    /** A bound UDP socket that records every payload it receives and echoes it straight back to
     *  the sender - stands in for a Simple Voice Chat server. */
    @BeforeEach
    fun setUp() {
        group = NioEventLoopGroup(2)
        backend = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    val bytes = ByteArray(packet.content().readableBytes())
                    packet.content().readBytes(bytes)
                    backendReceived.add(bytes)
                    ctx.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer("echo:".toByteArray() + bytes), packet.sender()))
                }
            })
            .bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
    }

    @AfterEach
    fun tearDown() {
        VoiceRouting.unregister("127.0.0.1")
        UdpThrottle.reset()
        backend.close().sync()
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync()
    }

    private fun freeUdpPort(): Int = DatagramSocket(0).use { it.localPort }

    private fun backendAddress() = backend.localAddress() as InetSocketAddress

    /** Opens a client socket, sends [payload] to [target], and returns the first datagram that
     *  comes back (or null after 2s). */
    private fun sendAndAwaitReply(target: InetSocketAddress, payload: ByteArray): ByteArray? {
        val replies = LinkedBlockingQueue<ByteArray>()
        val client = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    val bytes = ByteArray(packet.content().readableBytes())
                    packet.content().readBytes(bytes)
                    replies.add(bytes)
                }
            })
            .bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
        try {
            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer(payload), target)).sync()
            return replies.poll(2, TimeUnit.SECONDS)
        } finally {
            client.close().sync()
        }
    }

    private fun v2Header(src: String, srcPort: Int): ByteArray {
        val sig = byteArrayOf(0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A)
        val addr = src.split('.').map { it.toInt().toByte() }.toByteArray()
        val block = addr + byteArrayOf(0, 0, 0, 0) +
            byteArrayOf((srcPort shr 8).toByte(), srcPort.toByte(), 0, 0)
        return sig + byteArrayOf(0x21, 0x11, (block.size shr 8).toByte(), block.size.toByte()) + block
    }

    @Test
    fun `relays a datagram to the routed backend and pipes the reply back`() {
        val relay = VoiceRelay(group)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))
        try {
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())

            val reply = sendAndAwaitReply(InetSocketAddress("127.0.0.1", relayPort), "hello-voice".toByteArray())

            assertArrayEquals("hello-voice".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            assertArrayEquals("echo:hello-voice".toByteArray(), reply)
        } finally {
            relay.stop()
        }
    }

    @Test
    fun `caps concurrent sessions per source IP`() {
        UdpThrottle.apply(UdpThrottleConfig(maxSessionsPerIp = 1))
        val relay = VoiceRelay(group)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))
        try {
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())
            val target = InetSocketAddress("127.0.0.1", relayPort)

            // First session from 127.0.0.1 is allowed and relays end-to-end.
            assertArrayEquals("echo:one".toByteArray(), sendAndAwaitReply(target, "one".toByteArray()))

            // A second concurrent session from the same IP (fresh ephemeral port) is over the
            // per-IP cap - dropped, no reply, nothing reaches the backend.
            backendReceived.clear()
            assertEquals(null, sendAndAwaitReply(target, "two".toByteArray()))
            assertEquals(null, backendReceived.poll(500, TimeUnit.MILLISECONDS))
        } finally {
            relay.stop()
        }
    }

    @Test
    fun `drops a datagram from an unregistered client`() {
        val relay = VoiceRelay(group)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))
        try {
            // No VoiceRouting.register - the relay has nowhere to send this.
            val reply = sendAndAwaitReply(InetSocketAddress("127.0.0.1", relayPort), "orphan".toByteArray())

            assertEquals(null, reply)
            assertEquals(null, backendReceived.poll(500, TimeUnit.MILLISECONDS))
        } finally {
            relay.stop()
        }
    }

    @Test
    fun `with proxyProtocol on, strips the PROXY header and routes by the real client address`() {
        val relay = VoiceRelay(group, expectProxyProtocol = true)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))
        try {
            // Routing entry is keyed by the address inside the PROXY header, not the datagram's
            // own source (which here is an ephemeral loopback port standing in for a fronting
            // proxy like Cloudflare Spectrum).
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())

            val framed = v2Header("127.0.0.1", 34567) + "hello-proxied".toByteArray()
            val reply = sendAndAwaitReply(InetSocketAddress("127.0.0.1", relayPort), framed)

            // Backend sees only the payload, header stripped.
            assertArrayEquals("hello-proxied".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            // Reply comes back to the datagram's actual sender (this test's client socket).
            assertArrayEquals("echo:hello-proxied".toByteArray(), reply)
        } finally {
            relay.stop()
        }
    }

    @Test
    fun `with proxyProtocol on, only the first datagram of a flow carries the header (Cloudflare Spectrum pattern)`() {
        val relay = VoiceRelay(group, expectProxyProtocol = true)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))

        val replies = LinkedBlockingQueue<ByteArray>()
        // One fixed client socket - its source address is the stand-in for a single Cloudflare
        // edge flow, constant across all three datagrams.
        val client = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    val bytes = ByteArray(packet.content().readableBytes())
                    packet.content().readBytes(bytes)
                    replies.add(bytes)
                }
            })
            .bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
        val target = InetSocketAddress("127.0.0.1", relayPort)
        try {
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())

            // First datagram: PROXY header + payload. Rest: payload only.
            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer(v2Header("127.0.0.1", 4962) + "one".toByteArray()), target)).sync()
            assertArrayEquals("one".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            assertArrayEquals("echo:one".toByteArray(), replies.poll(2, TimeUnit.SECONDS))

            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer("two".toByteArray()), target)).sync()
            assertArrayEquals("two".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            assertArrayEquals("echo:two".toByteArray(), replies.poll(2, TimeUnit.SECONDS))

            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer("three".toByteArray()), target)).sync()
            assertArrayEquals("three".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            assertArrayEquals("echo:three".toByteArray(), replies.poll(2, TimeUnit.SECONDS))
        } finally {
            client.close().sync()
            relay.stop()
        }
    }

    @Test
    fun `unregistering a client tears down its relay session`() {
        val relay = VoiceRelay(group, expectProxyProtocol = true)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))

        val replies = LinkedBlockingQueue<ByteArray>()
        val client = Bootstrap()
            .group(group)
            .channel(NioDatagramChannel::class.java)
            .handler(object : SimpleChannelInboundHandler<DatagramPacket>() {
                override fun channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket) {
                    val bytes = ByteArray(packet.content().readableBytes())
                    packet.content().readBytes(bytes)
                    replies.add(bytes)
                }
            })
            .bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
        val target = InetSocketAddress("127.0.0.1", relayPort)
        try {
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())

            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer(v2Header("127.0.0.1", 4962) + "up".toByteArray()), target)).sync()
            assertArrayEquals("up".toByteArray(), backendReceived.poll(2, TimeUnit.SECONDS))
            assertArrayEquals("echo:up".toByteArray(), replies.poll(2, TimeUnit.SECONDS))

            // Player's Minecraft connection ends -> VoiceRouting.unregister -> relay.disconnectClient.
            VoiceRouting.unregister("127.0.0.1")

            // Same flow keeps sending header-less datagrams; the session (and its byVia entry) is
            // gone, so nothing more reaches the backend.
            client.writeAndFlush(DatagramPacket(Unpooled.wrappedBuffer("after".toByteArray()), target)).sync()
            assertEquals(null, backendReceived.poll(500, TimeUnit.MILLISECONDS))
            assertEquals(null, replies.poll(500, TimeUnit.MILLISECONDS))
        } finally {
            client.close().sync()
            relay.stop()
        }
    }

    @Test
    fun `with proxyProtocol on, a headerless datagram is dropped`() {
        val relay = VoiceRelay(group, expectProxyProtocol = true)
        val relayPort = freeUdpPort()
        relay.start(InetSocketAddress("127.0.0.1", relayPort))
        try {
            VoiceRouting.register("127.0.0.1", "voice.test", backendAddress())

            val reply = sendAndAwaitReply(InetSocketAddress("127.0.0.1", relayPort), "no-header-here".toByteArray())

            assertEquals(null, reply)
            assertEquals(null, backendReceived.poll(500, TimeUnit.MILLISECONDS))
        } finally {
            relay.stop()
        }
    }
}
