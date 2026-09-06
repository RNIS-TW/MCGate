package me.hippodev.handler

import io.netty.buffer.Unpooled
import io.netty.channel.WriteBufferWaterMark
import io.netty.channel.embedded.EmbeddedChannel
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The relay's memory safety rests entirely on one thing: when the channel MCGate is forwarding
 * bytes *into* stops draining, reads on the channel those bytes come *from* must stop too, so the
 * unsent data can't pile up unbounded in the outbound buffer (the actual cause of the "RAM keeps
 * climbing while CPU stays low" behavior). These tests pin that behavior down.
 *
 * [EmbeddedChannel] mechanics used here: a `write` that is never `flush`ed leaves the bytes in the
 * channel's outbound buffer, so a 64-byte write against a 16/32-byte water mark flips
 * `isWritable` to false; a later `flush` drains the buffer and flips it back.
 */
class FlowControlTest {

    private fun tinyWaterMarkChannel(): EmbeddedChannel = EmbeddedChannel().apply {
        config().writeBufferWaterMark = WriteBufferWaterMark(16, 32)
    }

    private fun EmbeddedChannel.backUp() = write(Unpooled.wrappedBuffer(ByteArray(64)))

    @Test
    fun `pauseOrResumeReads mirrors this channel's writability onto the source`() {
        val target = tinyWaterMarkChannel()
        val source = EmbeddedChannel()

        assertTrue(source.config().isAutoRead)

        target.backUp()
        assertFalse(target.isWritable)
        target.pauseOrResumeReads(source)
        assertFalse(source.config().isAutoRead, "source reads must pause while target can't drain")

        target.flush()
        assertTrue(target.isWritable)
        target.pauseOrResumeReads(source)
        assertTrue(source.config().isAutoRead, "source reads must resume once target drains")

        source.finishAndReleaseAll()
        target.finishAndReleaseAll()
    }

    @Test
    fun `null source is a no-op`() {
        val target = EmbeddedChannel()
        target.pauseOrResumeReads(null)
        target.finishAndReleaseAll()
    }

    @Test
    fun `RawRelayHandler pauses reads on its target when its own channel backs up`() {
        // RawRelayHandler(target) installed on `src` reads src -> writes target. When src itself
        // backs up (the far end of `target` is slow, filling src's write buffer via the reverse
        // pipe), target's reads must pause.
        val target = EmbeddedChannel()
        val src = tinyWaterMarkChannel().also { it.pipeline().addLast(RawRelayHandler(target)) }

        assertTrue(target.config().isAutoRead)

        src.backUp()
        assertFalse(src.isWritable)
        src.pipeline().fireChannelWritabilityChanged()
        assertFalse(target.config().isAutoRead, "target reads pause when src can't drain")

        src.flush()
        src.pipeline().fireChannelWritabilityChanged()
        assertTrue(target.config().isAutoRead, "target reads resume once src drains")

        src.finishAndReleaseAll()
        target.finishAndReleaseAll()
    }

    @Test
    fun `RawRelayHandler adopts existing backpressure when installed on an already-active channel`() {
        // A backed-up target the instant the pipe is spliced in (pipeline replace, so channelActive
        // never re-fires) must immediately pause reads on the channel feeding it.
        val target = tinyWaterMarkChannel()
        target.backUp()
        assertFalse(target.isWritable)

        val src = EmbeddedChannel()
        assertTrue(src.config().isAutoRead)
        src.pipeline().addLast(RawRelayHandler(target)) // fires handlerAdded synchronously
        assertFalse(src.config().isAutoRead, "a late-spliced pipe must adopt current backpressure at once")

        target.flush()
        src.finishAndReleaseAll()
        target.finishAndReleaseAll()
    }
}
