import me.hippodev.protocol.encodeLoginDisconnect
import me.hippodev.protocol.toJsonComponent
import me.hippodev.protocol.readVarInt
import io.netty.buffer.ByteBuf

fun main() {
    val json = toJsonComponent("&cServer is offline. Please reconnect shortly.")
    println("rendered json: $json")
    val buf: ByteBuf = encodeLoginDisconnect(json)
    val bytes = ByteArray(buf.readableBytes())
    buf.getBytes(buf.readerIndex(), bytes)
    println("total bytes: ${bytes.size}")
    println("hex: " + bytes.joinToString(" ") { "%02x".format(it) })

    // manually parse it back like a client would
    val totalLen = readVarInt(buf)
    println("frame length varint: $totalLen, remaining after that read: ${buf.readableBytes()}")
    val packetId = readVarInt(buf)
    println("packet id: $packetId")
    val strLen = readVarInt(buf)
    println("string length: $strLen, remaining: ${buf.readableBytes()}")
    val strBytes = ByteArray(strLen)
    buf.readBytes(strBytes)
    println("string: " + String(strBytes, Charsets.UTF_8))
    println("bytes left over (should be 0): " + buf.readableBytes())
}
