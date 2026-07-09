package me.hippodev.protocol

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.UUID

/**
 * Packet IDs and encoders for the limbo "holding room" (see LimboHandler). Minecraft's
 * Configuration/Play packet IDs are NOT stable across versions - they shift release to release -
 * so unlike the handshake/status packets in MinecraftProtocol.kt, this table only covers a single
 * verified protocol bracket and must be extended (a new entry in [BRACKETS]) to support others.
 *
 * IDs below were confirmed against the current minecraft.wiki protocol packet reference at
 * implementation time. The NBT registry payload and chunk data encoding in this file are
 * best-effort from protocol knowledge and were NOT verified against a live client in this
 * environment - test against a real 1.20.2+ client before relying on this in production, per the
 * plan's testing section.
 */
data class LimboPacketIds(
    val loginSuccess: Int,
    val configFinishConfiguration: Int,
    val configRegistryData: Int,
    val configKeepAlive: Int,
    val configAckFinishConfiguration: Int,
    val playLogin: Int,
    val playKeepAliveClientbound: Int,
    val playKeepAliveServerbound: Int,
    val playSetActionBarText: Int,
    val playSetCenterChunk: Int,
    val playChunkDataAndUpdateLight: Int,
    val playStartConfiguration: Int,
    val playAckConfiguration: Int,
    val playDisconnect: Int
)

private val BRACKETS: Map<Int, LimboPacketIds> = mapOf(
    // Protocol 776 - current stable at implementation time. Verified via minecraft.wiki packet
    // tables; extend this map with additional protocol -> LimboPacketIds entries for other
    // brackets once verified.
    776 to LimboPacketIds(
        loginSuccess = 0x02,
        configFinishConfiguration = 0x03,
        configRegistryData = 0x07,
        configKeepAlive = 0x04,
        configAckFinishConfiguration = 0x03,
        playLogin = 0x31,
        playKeepAliveClientbound = 0x2C,
        playKeepAliveServerbound = 0x1C,
        playSetActionBarText = 0x57,
        playSetCenterChunk = 0x5E,
        playChunkDataAndUpdateLight = 0x2D,
        playStartConfiguration = 0x76,
        playAckConfiguration = 0x10,
        playDisconnect = 0x20
    )
)

/** Whether limbo can serve this protocol version - i.e. we have a verified packet-ID bracket for it. */
fun limboSupports(protocolVersion: Int): Boolean = BRACKETS.containsKey(protocolVersion)

fun limboPacketIds(protocolVersion: Int): LimboPacketIds =
    BRACKETS[protocolVersion] ?: error("No limbo packet table for protocol $protocolVersion")

private const val LIMBO_DIMENSION = "mcgate:limbo"
private const val LIMBO_DIMENSION_TYPE = "mcgate:limbo_type"

/** Login state (packet 0x00), stable across every protocol version - used for both the legacy
 *  kick fallback and any hard error while transferring out of limbo. Always sent before any
 *  compression is negotiated (Login state), so no [compressionThreshold] parameter needed. */
fun encodeLoginDisconnect(reasonText: String): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, 0x00)
    writeString(payload, "{\"text\":\"${jsonEscape(translateColorCodes(reasonText))}\"}")
    return frame(payload, -1)
}

/** Login state clientbound `login_finished` (formerly "Login Success"). As of the currently
 *  supported protocol bracket this has two UUID fields: the Game Profile's id, and a separate
 *  trailing Session ID - a client decoding this packet reads both, so omitting the second
 *  (an earlier version of this encoder did) leaves it short a UUID and throws a DecoderException.
 *  Session ID's exact semantics aren't verified here; reusing the player's UUID is an opaque but
 *  harmless placeholder since MCGate's synthetic limbo login has no real session to report. */
fun encodeLoginSuccess(ids: LimboPacketIds, uuid: UUID, username: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.loginSuccess)
    payload.writeLong(uuid.mostSignificantBits) // Game Profile: id
    payload.writeLong(uuid.leastSignificantBits)
    writeString(payload, username)              // Game Profile: name
    writeVarInt(payload, 0)                      // Game Profile: properties (none)
    payload.writeLong(uuid.mostSignificantBits) // Session ID
    payload.writeLong(uuid.leastSignificantBits)
    return frame(payload, compressionThreshold)
}

fun encodeFinishConfiguration(ids: LimboPacketIds, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.configFinishConfiguration)
    return frame(payload, compressionThreshold)
}

fun encodeConfigKeepAlive(ids: LimboPacketIds, id: Long, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.configKeepAlive)
    payload.writeLong(id)
    return frame(payload, compressionThreshold)
}

fun encodePlayKeepAlive(ids: LimboPacketIds, id: Long, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playKeepAliveClientbound)
    payload.writeLong(id)
    return frame(payload, compressionThreshold)
}

fun encodeStartConfiguration(ids: LimboPacketIds, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playStartConfiguration)
    return frame(payload, compressionThreshold)
}

/** Play state (not Login) Disconnect - used to kick a player already inside the limbo world
 *  (e.g. `limbo.maxWait` expiring), since by that point the client is well past Login state and
 *  a Login-state Disconnect packet would be rejected as unexpected. */
fun encodePlayDisconnect(ids: LimboPacketIds, reasonText: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playDisconnect)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, translateColorCodes(reasonText))
    return frame(payload, compressionThreshold)
}

/** Registry Data for a single minimal `minecraft:dimension_type` entry describing the void limbo
 *  world. Sent once per registry the client needs populated before Finish Configuration; a
 *  vanilla 1.20.2+ client requires at least dimension_type, worldgen/biome, chat_type,
 *  damage_type, and trim_material/trim_pattern registries to be present (even if minimal) before
 *  it will accept Finish Configuration - this encoder only ships dimension_type since that's the
 *  one the join packet references. If a live client rejects Finish Configuration with a missing
 *  registry, that's the first thing to extend here. */
fun encodeDimensionTypeRegistry(ids: LimboPacketIds, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.configRegistryData)
    writeString(payload, "minecraft:dimension_type")
    writeVarInt(payload, 1) // one entry
    writeString(payload, LIMBO_DIMENSION_TYPE)
    payload.writeBoolean(true) // has NBT data (not using the client's built-in default)
    writeRootCompound(payload) {
        byte("piglin_safe", 0)
        byte("has_raids", 0)
        int("monster_spawn_light_level", 0)
        int("monster_spawn_block_light_limit", 0)
        byte("natural", 0)
        float("ambient_light", 0.0f)
        string("infiniburn", "#minecraft:infiniburn_overworld")
        byte("respawn_anchor_works", 0)
        byte("has_skylight", 1)
        byte("bed_works", 0)
        string("effects", "minecraft:overworld")
        int("min_y", 0)
        int("height", 16)
        int("logical_height", 16)
        double("coordinate_scale", 1.0)
        byte("ultrawarm", 0)
        byte("has_ceiling", 1)
    }
    return frame(payload, compressionThreshold)
}

/** Minimal Play-state "Login" (Join Game) packet spawning the client into the void limbo world.
 *  Field list is best-effort against the current protocol's Login(play) packet - flagged as the
 *  riskiest unverified part of the join sequence (see file header). */
fun encodeJoinGame(ids: LimboPacketIds, entityId: Int, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playLogin)
    payload.writeInt(entityId)
    payload.writeBoolean(false) // is hardcore
    writeVarInt(payload, 1) // dimension names count
    writeString(payload, LIMBO_DIMENSION)
    writeVarInt(payload, 20) // max players (unused by client, vanilla ignores)
    writeVarInt(payload, 2) // view distance
    writeVarInt(payload, 2) // simulation distance
    payload.writeBoolean(false) // reduced debug info
    payload.writeBoolean(false) // enable respawn screen
    payload.writeBoolean(false) // do limited crafting
    writeString(payload, LIMBO_DIMENSION_TYPE) // dimension type (by identifier)
    writeString(payload, LIMBO_DIMENSION) // dimension name
    payload.writeLong(0L) // hashed seed
    writeVarInt(payload, 3) // gamemode: spectator, so gravity/void death don't apply
    payload.writeByte(-1) // previous gamemode: none
    payload.writeBoolean(false) // is debug
    payload.writeBoolean(true) // is flat
    payload.writeBoolean(false) // has death location
    writeVarInt(payload, 0) // portal cooldown
    writeVarInt(payload, 0) // sea level
    payload.writeBoolean(false) // enforces secure chat
    return frame(payload, compressionThreshold)
}

fun encodeSetCenterChunk(ids: LimboPacketIds, compressionThreshold: Int, chunkX: Int = 0, chunkZ: Int = 0): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetCenterChunk)
    writeVarInt(payload, chunkX)
    writeVarInt(payload, chunkZ)
    return frame(payload, compressionThreshold)
}

/** A single empty (all-air) chunk column at (0,0) with one section, no light data, so the
 *  client stops showing "Loading terrain..." at the limbo spawn point. */
fun encodeEmptyChunk(ids: LimboPacketIds, compressionThreshold: Int, chunkX: Int = 0, chunkZ: Int = 0): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playChunkDataAndUpdateLight)
    payload.writeInt(chunkX)
    payload.writeInt(chunkZ)

    // Heightmaps: empty NBT compound (no heightmaps present).
    writeRootCompound(payload) {}

    // Chunk section data: one section, all air.
    val section = Unpooled.buffer()
    section.writeShort(0) // block count = 0 (all air)
    // Block states: single-value paletted container, bits-per-entry 0 -> palette-only, value = air (0).
    section.writeByte(0)
    writeVarInt(section, 0) // palette entry: air block state id 0
    writeVarInt(section, 0) // data array length 0
    // Biomes: single-value paletted container, biome id 0 (plains, arbitrary).
    section.writeByte(0)
    writeVarInt(section, 0)
    writeVarInt(section, 0)

    val sectionBytes = ByteArray(section.readableBytes())
    section.readBytes(sectionBytes)
    section.release()
    writeVarInt(payload, sectionBytes.size)
    payload.writeBytes(sectionBytes)

    writeVarInt(payload, 0) // block entities: none

    // Light data: no sections lit, empty masks/arrays.
    writeVarInt(payload, 0) // sky light mask longs
    writeVarInt(payload, 0) // block light mask longs
    writeVarInt(payload, 0) // empty sky light mask longs
    writeVarInt(payload, 0) // empty block light mask longs
    writeVarInt(payload, 0) // sky light array count
    writeVarInt(payload, 0) // block light array count

    return frame(payload, compressionThreshold)
}

/** Action bar text (Play state). 1.20.3+ text components are network NBT rather than JSON; a bare
 *  NBT string tag is valid as a text component ("just the text"), which keeps this simple. */
fun encodeActionBar(ids: LimboPacketIds, text: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetActionBarText)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, translateColorCodes(text))
    return frame(payload, compressionThreshold)
}

/** Translates `&`-prefixed legacy color codes (e.g. `&e`) into the `§` codes Minecraft's chat
 *  renderer recognizes inline in a plain-text component. */
fun translateColorCodes(value: String): String {
    val sb = StringBuilder(value.length)
    var i = 0
    while (i < value.length) {
        val c = value[i]
        if (c == '&' && i + 1 < value.length && value[i + 1].lowercaseChar() in "0123456789abcdefklmnor") {
            sb.append('§').append(value[i + 1])
            i += 2
        } else {
            sb.append(c)
            i++
        }
    }
    return sb.toString()
}

private fun jsonEscape(value: String): String {
    val sb = StringBuilder(value.length)
    for (c in value) {
        when (c) {
            '"' -> sb.append("\\\"")
            '\\' -> sb.append("\\\\")
            '\n' -> sb.append("\\n")
            else -> sb.append(c)
        }
    }
    return sb.toString()
}
