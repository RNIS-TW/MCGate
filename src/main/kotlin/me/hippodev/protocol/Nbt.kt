package me.hippodev.protocol

import io.netty.buffer.ByteBuf

/**
 * Minimal binary NBT writer covering just the tag types needed to build the tiny registry/dimension
 * blobs the limbo world sends during the Configuration phase (see LimboProtocol.kt). Not a general
 * purpose NBT library - no reader, no long/int arrays, no lists-of-lists.
 */
object Nbt {
    const val END = 0
    const val BYTE = 1
    const val SHORT = 2
    const val INT = 3
    const val LONG = 4
    const val FLOAT = 5
    const val DOUBLE = 6
    const val STRING = 8
    const val LIST = 9
    const val COMPOUND = 10

    fun writeString(buf: ByteBuf, value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        buf.writeShort(bytes.size)
        buf.writeBytes(bytes)
    }

    /** Writes a named tag header (type + name) - the name is empty for the network root compound. */
    fun writeNamedTag(buf: ByteBuf, type: Int, name: String) {
        buf.writeByte(type)
        writeString(buf, name)
    }
}

/** Builder for a single NBT compound's contents (assumes the caller already wrote the COMPOUND header). */
class NbtCompoundBuilder(private val buf: ByteBuf) {
    fun byte(name: String, value: Int) {
        Nbt.writeNamedTag(buf, Nbt.BYTE, name)
        buf.writeByte(value)
    }

    fun int(name: String, value: Int) {
        Nbt.writeNamedTag(buf, Nbt.INT, name)
        buf.writeInt(value)
    }

    fun float(name: String, value: Float) {
        Nbt.writeNamedTag(buf, Nbt.FLOAT, name)
        buf.writeFloat(value)
    }

    fun double(name: String, value: Double) {
        Nbt.writeNamedTag(buf, Nbt.DOUBLE, name)
        buf.writeDouble(value)
    }

    fun string(name: String, value: String) {
        Nbt.writeNamedTag(buf, Nbt.STRING, name)
        Nbt.writeString(buf, value)
    }

    fun compound(name: String, block: NbtCompoundBuilder.() -> Unit) {
        Nbt.writeNamedTag(buf, Nbt.COMPOUND, name)
        NbtCompoundBuilder(buf).apply(block)
        buf.writeByte(Nbt.END)
    }

    fun end() {
        buf.writeByte(Nbt.END)
    }
}

/** Writes a root, unnamed NBT compound (network NBT format used since 1.20.2 - no root name string). */
fun writeRootCompound(buf: ByteBuf, block: NbtCompoundBuilder.() -> Unit) {
    buf.writeByte(Nbt.COMPOUND)
    NbtCompoundBuilder(buf).apply(block)
    buf.writeByte(Nbt.END)
}
