package me.hippodev.handler

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.embedded.EmbeddedChannel
import me.hippodev.config.ReconnectConfig
import me.hippodev.config.Route
import me.hippodev.config.Strategy
import me.hippodev.routing.RouteRuntime
import java.net.InetSocketAddress
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BufferLifecycleTest {
    private val route = Route(
        emptyList(), emptyList(), Strategy.SEQUENTIAL, 0, null,
        false, false, 0, ReconnectConfig(), "Offline"
    )

    private fun login(handshake: ByteBuf) = LoginRelayHandler(
        route, RouteRuntime(), emptyList(), 774, "test", 25565, handshake
    )

    // Instantiate the actual private pipeline handlers without dialing a real TCP backend.
    private fun decoder(owner: Any, name: String, client: Channel): ChannelHandler {
        val type = owner.javaClass.declaredClasses.single { it.simpleName == name }
        val ctor = type.declaredConstructors.single().apply { isAccessible = true }
        return if (ctor.parameterCount == 2) ctor.newInstance(owner, client) as ChannelHandler
        else ctor.newInstance(owner, client, InetSocketAddress("127.0.0.1", 25565)) as ChannelHandler
    }

    private fun verifyIncompleteFrameCleanup(reconnect: Boolean) {
        for (bytes in listOf(byteArrayOf(0x80.toByte()), byteArrayOf(5, 0))) {
            val client = EmbeddedChannel()
            val handshake = Unpooled.buffer().writeByte(0)
            val owner = if (reconnect) ReconnectHandler(
                route, RouteRuntime(), emptyList(), 774, "test", 25565, "Test", UUID.randomUUID()
            ) else login(handshake)
            var inactiveForwarded = false
            val backend = EmbeddedChannel(
                decoder(owner, if (reconnect) "BackendLoginRelay" else "BackendLoginSniffer", client),
                object : ChannelInboundHandlerAdapter() {
                    override fun channelInactive(ctx: ChannelHandlerContext) {
                        inactiveForwarded = true
                        ctx.fireChannelInactive()
                    }
                }
            )
            val input = Unpooled.directBuffer().writeBytes(bytes)
            try {
                assertFalse(backend.writeInbound(input))
                assertEquals(1, input.refCnt())
                // Check at channelInactive, before pipeline destruction can mask missing cleanup.
                backend.pipeline().fireChannelInactive()
                assertEquals(0, input.refCnt(), "incomplete frame must be released on inactivity")
                assertTrue(inactiveForwarded)
                assertFalse(client.isActive, "backend drop must still close the frontend")
            } finally {
                backend.finishAndReleaseAll()
                client.finishAndReleaseAll()
                handshake.release()
            }
        }
    }

    @Test
    fun `login backend releases incomplete frames on inactivity`() = verifyIncompleteFrameCleanup(false)

    @Test
    fun `reconnect backend releases incomplete frames on inactivity`() = verifyIncompleteFrameCleanup(true)

    @Test
    fun `removing login handler releases handshake and queued client bytes`() {
        val handshake = Unpooled.directBuffer().writeByte(0)
        val handler = login(handshake)
        val client = EmbeddedChannel(handler)
        val input = Unpooled.directBuffer().writeByte(0)
        try {
            client.writeInbound(input)
            assertEquals(1, input.refCnt())
            client.pipeline().remove(handler)
            assertEquals(0, handshake.refCnt())
            assertEquals(0, input.refCnt())
        } finally {
            client.finishAndReleaseAll()
        }
    }

    @Test
    fun `late client bytes after dialing ends are released instead of queued`() {
        val handshake = Unpooled.directBuffer().writeByte(0)
        val handler = login(handshake)
        val client = EmbeddedChannel(handler)
        val input = Unpooled.directBuffer().writeByte(0)
        try {
            client.pipeline().fireChannelInactive()
            assertEquals(0, handshake.refCnt())
            client.writeInbound(input)
            assertEquals(0, input.refCnt())
        } finally {
            client.finishAndReleaseAll()
        }
    }

    @Test
    fun `connected relay transfers buffer ownership to backend`() {
        val handshake = Unpooled.directBuffer().writeByte(0)
        val handler = login(handshake)
        val client = EmbeddedChannel(handler)
        val backend = EmbeddedChannel()
        // Model a completed dial: the handshake has already been sent or released.
        handshake.release()
        handler.javaClass.getDeclaredField("pendingReleased").apply { isAccessible = true }.setBoolean(handler, true)
        handler.javaClass.getDeclaredField("backendChannel").apply { isAccessible = true }.set(handler, backend)
        val input = Unpooled.directBuffer().writeByte(0)
        try {
            client.writeInbound(input)
            val forwarded = backend.readOutbound<ByteBuf>()
            assertEquals(1, forwarded.refCnt())
            forwarded.release()
            assertEquals(0, input.refCnt())
            backend.close()
            val late = Unpooled.directBuffer().writeByte(0)
            client.writeInbound(late)
            assertEquals(0, late.refCnt())
        } finally {
            client.finishAndReleaseAll()
            backend.finishAndReleaseAll()
        }
    }
}
