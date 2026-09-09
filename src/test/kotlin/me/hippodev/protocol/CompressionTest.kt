package me.hippodev.protocol

import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.zip.Deflater

class CompressionTest {

    private fun deflate(bytes: ByteArray): ByteArray {
        val d = Deflater()
        d.setInput(bytes); d.finish()
        val out = java.io.ByteArrayOutputStream()
        val buf = ByteArray(256)
        while (!d.finished()) out.write(buf, 0, d.deflate(buf))
        d.end()
        return out.toByteArray()
    }

    @Test
    fun `readCompressedFrame rejects an absurd decompressed-size claim instead of allocating it`() {
        val buf = Unpooled.buffer()
        writeVarInt(buf, 2_000_000_000) // dataLength claim ~= Int.MAX
        buf.writeBytes(ByteArray(8))
        val frameEnd = buf.writerIndex()

        assertThrows(IllegalStateException::class.java) {
            readCompressedFrame(buf, frameEnd, compressionThreshold = 256)
        }
        buf.release()
    }

    @Test
    fun `readCompressedFrame round-trips a compressed frame`() {
        val payload = ByteArray(600) { (it % 7).toByte() } // > threshold, so it gets deflated
        val framed = frame(Unpooled.wrappedBuffer(payload.copyOf()), compressionThreshold = 256)
        // Skip the outer length varint the caller would have consumed.
        readVarInt(framed)
        val frameEnd = framed.writerIndex()
        val (packetId, body) = readCompressedFrame(framed, frameEnd, compressionThreshold = 256)
        assertEquals(payload[0].toInt(), packetId) // first payload byte is the "packet id" varint
        if (body !== framed) body.release()
        framed.release()
    }

    @Test
    fun `inflate does not spin forever on a truncated stream`() {
        val full = deflate("hello world, this is a test payload".toByteArray())
        val truncated = full.copyOf(full.size - 3)
        assertTimeoutPreemptively(Duration.ofSeconds(1)) {
            inflate(truncated, expectedSize = 1_000_000)
        }
    }

    @Test
    fun `inflate stops when the stream finishes short of the claimed size`() {
        val full = deflate("hi".toByteArray())
        val out = inflate(full, expectedSize = 100_000)
        assertEquals('h'.code.toByte(), out[0])
        assertEquals('i'.code.toByte(), out[1])
    }
}
