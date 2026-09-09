package me.hippodev.handler

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import me.hippodev.protocol.encodeHandshake
import me.hippodev.protocol.writeVarInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HandshakeSnifferTest {

    private fun channel(): EmbeddedChannel {
        var seen: String? = null
        return EmbeddedChannel(HandshakeSniffer { ctx, _, host, _, _, rawFrame ->
            seen = host
            rawFrame.release()
            ctx.close()
        })
    }

    @Test
    fun `a normal handshake is parsed and passed on`() {
        val ch = channel()
        val hs = encodeHandshake(760, "play.example.com", 25565, 2)
        assertFalse(ch.writeInbound(hs))
        // Handler closed the channel from its callback - proves the handshake decoded.
        assertFalse(ch.isActive)
    }

    @Test
    fun `a frame declaring an absurd length is rejected without buffering it`() {
        val ch = channel()
        // Length varint = 1_000_000, then just a couple of bytes: a slow-loris shape. The old code
        // would keep this connection open, cumulating toward 1 MB; now it's closed on sight.
        val buf = Unpooled.buffer()
        writeVarInt(buf, 1_000_000)
        buf.writeByte(0x00).writeByte(0x00)
        assertFalse(ch.writeInbound(buf))
        assertFalse(ch.isActive, "an over-long handshake frame must be rejected immediately")
    }

    @Test
    fun `an incomplete but plausibly-sized frame is buffered, not dropped`() {
        val ch = channel()
        val full = encodeHandshake(760, "play.example.com", 25565, 2)
        val head = full.readSlice(4).copy() // first few bytes only
        full.release()
        assertFalse(ch.writeInbound(head))
        assertTrue(ch.isActive, "a partial handshake within the size cap must stay open awaiting the rest")
        ch.finishAndReleaseAll()
    }
}
