package me.hippodev.handler

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.ByteToMessageDecoder
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DecoderCleanupTest {
    private val cumulationField = ByteToMessageDecoder::class.java.getDeclaredField("cumulation").apply {
        isAccessible = true
    }

    private class DecoderWithSuper : ByteToMessageDecoder() {
        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) = Unit
        override fun channelInactive(ctx: ChannelHandlerContext) = super.channelInactive(ctx)
    }

    private class DecoderWithoutSuper : ByteToMessageDecoder() {
        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) = Unit
        override fun channelInactive(ctx: ChannelHandlerContext) = Unit
    }

    private fun feedPartialFrame(channel: EmbeddedChannel) {
        channel.writeInbound(Unpooled.wrappedBuffer(byteArrayOf(0x02, 0x01)))
    }

    private fun cumulation(decoder: ByteToMessageDecoder): ByteBuf? = cumulationField.get(decoder) as ByteBuf?

    @Test
    fun `calling super channelInactive clears decoder cumulation`() {
        val withSuper = DecoderWithSuper()
        val withSuperChannel = EmbeddedChannel(withSuper)
        feedPartialFrame(withSuperChannel)
        assertNotNull(cumulation(withSuper))
        withSuper.channelInactive(withSuperChannel.pipeline().context(withSuper))
        assertNull(cumulation(withSuper))
        withSuperChannel.finishAndReleaseAll()

        val withoutSuper = DecoderWithoutSuper()
        val withoutSuperChannel = EmbeddedChannel(withoutSuper)
        feedPartialFrame(withoutSuperChannel)
        assertNotNull(cumulation(withoutSuper))
        withoutSuper.channelInactive(withoutSuperChannel.pipeline().context(withoutSuper))
        assertNotNull(cumulation(withoutSuper))
        cumulation(withoutSuper)?.release()
        withoutSuperChannel.close()
    }
}
