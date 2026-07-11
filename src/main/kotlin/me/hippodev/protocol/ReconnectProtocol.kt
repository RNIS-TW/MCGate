package me.hippodev.protocol

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.UUID

/**
 * Packet IDs and encoders for the auto-reconnect holding state (see ReconnectHandler). Minecraft's
 * Configuration/Play packet IDs are NOT stable across versions in general - they shift release to
 * release - so unlike the handshake/status packets in MinecraftProtocol.kt, this table only
 * covers verified protocol brackets ([BRACKETS]: currently 774-776, i.e. 1.21.11 through 26.2 -
 * see the comment above that map for how that range was verified) and must be extended (a new
 * entry in [BRACKETS]) to support others.
 *
 * IDs below were confirmed against the current minecraft.wiki protocol packet reference at
 * implementation time. The chunk data encoding in this file is best-effort from protocol
 * knowledge and was NOT verified against a live client in this environment - test against a real
 * client before relying on this in production, per the plan's testing section.
 */
data class ReconnectPacketIds(
    val loginSuccess: Int,
    val configFinishConfiguration: Int,
    val configKeepAlive: Int,
    val configAckFinishConfiguration: Int,
    val playLogin: Int,
    val playKeepAliveClientbound: Int,
    val playKeepAliveServerbound: Int,
    val playSetActionBarText: Int,
    val playSetTitleText: Int,
    val playSetSubtitleText: Int,
    val playSetCenterChunk: Int,
    val playChunkDataAndUpdateLight: Int,
    val playStartConfiguration: Int,
    val playAckConfiguration: Int,
    val playDisconnect: Int
)

// Protocol 774 (1.21.11), 775 (26.1/26.1.1/26.1.2), and 776 (26.2) share this exact packet-ID
// table - cross-checked against minecraft.wiki's Protocol History changelog
// (Minecraft_Wiki:Projects/wiki.vg_merge/Protocol_History), which lists every packet ID change
// release to release: from protocol 767 (1.21) through 776 (26.2) there is no recorded ID change
// for any packet used here (only an unrelated field removed from Login Success at 1.21.2/768).
// Extend this map with additional protocol -> ReconnectPacketIds entries for other brackets once
// similarly verified - don't assume older versions match without checking, since packet IDs do
// shift release to release in general (see e.g. the 19w36a entry in that changelog for how much
// churn a single snapshot can have).
private val CURRENT_BRACKET = ReconnectPacketIds(
    loginSuccess = 0x02,
    configFinishConfiguration = 0x03,
    configKeepAlive = 0x04,
    configAckFinishConfiguration = 0x03,
    playLogin = 0x31,
    playKeepAliveClientbound = 0x2C,
    playKeepAliveServerbound = 0x1C,
    playSetActionBarText = 0x57,
    playSetTitleText = 0x72,
    playSetSubtitleText = 0x70,
    playSetCenterChunk = 0x5E,
    playChunkDataAndUpdateLight = 0x2D,
    playStartConfiguration = 0x76,
    playAckConfiguration = 0x10,
    playDisconnect = 0x20
)

private val BRACKETS: Map<Int, ReconnectPacketIds> = mapOf(
    774 to CURRENT_BRACKET, // 1.21.11
    775 to CURRENT_BRACKET, // 26.1, 26.1.1, 26.1.2
    776 to CURRENT_BRACKET  // 26.2
)

/** Whether auto-reconnect can serve this protocol version - i.e. we have a verified packet-ID bracket for it. */
fun reconnectSupported(protocolVersion: Int): Boolean = BRACKETS.containsKey(protocolVersion)

fun reconnectPacketIds(protocolVersion: Int): ReconnectPacketIds =
    BRACKETS[protocolVersion] ?: error("No auto-reconnect packet table for protocol $protocolVersion")

/** Login state (packet 0x00), stable across every protocol version - used for both the legacy
 *  kick fallback and any hard error while transferring out of the reconnect wait. Always sent
 *  before any compression is negotiated (Login state), so no [compressionThreshold] parameter
 *  needed.
 *
 *  Takes an already-rendered rich-text string ([toJsonComponent] applied by the caller) rather
 *  than parsing it here - the caller decides whether that's a one-off ad-hoc message (parse right
 *  before sending) or a static config value that should be pre-rendered once and reused, rather
 *  than every encoder call re-running MiniMessage parsing on every send. */
fun encodeLoginDisconnect(renderedReasonJson: String): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, 0x00)
    writeString(payload, renderedReasonJson)
    return frame(payload, -1)
}

/** Login state clientbound `login_finished` (formerly "Login Success"). As of the currently
 *  supported protocol bracket this has two UUID fields: the Game Profile's id, and a separate
 *  trailing Session ID - a client decoding this packet reads both, so omitting the second
 *  (an earlier version of this encoder did) leaves it short a UUID and throws a DecoderException.
 *  Session ID's exact semantics aren't verified here; reusing the player's UUID is an opaque but
 *  harmless placeholder since MCGate's synthetic reconnect-wait login has no real session to
 *  report. */
fun encodeLoginSuccess(ids: ReconnectPacketIds, uuid: UUID, username: String, compressionThreshold: Int): ByteBuf {
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

fun encodeFinishConfiguration(ids: ReconnectPacketIds, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.configFinishConfiguration)
    return frame(payload, compressionThreshold)
}

fun encodeConfigKeepAlive(ids: ReconnectPacketIds, id: Long, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.configKeepAlive)
    payload.writeLong(id)
    return frame(payload, compressionThreshold)
}

fun encodePlayKeepAlive(ids: ReconnectPacketIds, id: Long, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playKeepAliveClientbound)
    payload.writeLong(id)
    return frame(payload, compressionThreshold)
}

fun encodeStartConfiguration(ids: ReconnectPacketIds, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playStartConfiguration)
    return frame(payload, compressionThreshold)
}

/** Play state (not Login) Disconnect - used to kick a player already waiting for reconnect
 *  (e.g. `reconnect.maxWait` expiring), since by that point the client is well past Login state
 *  and a Login-state Disconnect packet would be rejected as unexpected.
 *
 *  Takes already-rendered legacy text ([toLegacyText] applied by the caller) - see
 *  [encodeLoginDisconnect]'s doc for why the parsing isn't done here. */
fun encodePlayDisconnect(ids: ReconnectPacketIds, renderedReasonText: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playDisconnect)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, renderedReasonText)
    return frame(payload, compressionThreshold)
}

/** Minimal Play-state "Login" (Join Game) packet spawning the client into a held, empty world
 *  while waiting to reconnect. Reuses the client's own built-in `minecraft:overworld` dimension
 *  type rather than registering a custom one, so no Registry Data packet is needed first - one
 *  less packet, and no hand-rolled NBT dimension description to keep in sync with the client.
 *  Field list is best-effort against the current protocol's Login(play) packet - flagged as the
 *  riskiest unverified part of the join sequence (see file header). */
fun encodeJoinGame(ids: ReconnectPacketIds, entityId: Int, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playLogin)
    payload.writeInt(entityId)
    payload.writeBoolean(false) // is hardcore
    writeVarInt(payload, 1) // dimension names count
    writeString(payload, "minecraft:overworld")
    writeVarInt(payload, 20) // max players (unused by client, vanilla ignores)
    writeVarInt(payload, 2) // view distance
    writeVarInt(payload, 2) // simulation distance
    payload.writeBoolean(false) // reduced debug info
    payload.writeBoolean(false) // enable respawn screen
    payload.writeBoolean(false) // do limited crafting
    writeString(payload, "minecraft:overworld") // dimension type (by identifier)
    writeString(payload, "minecraft:overworld") // dimension name
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

fun encodeSetCenterChunk(ids: ReconnectPacketIds, compressionThreshold: Int, chunkX: Int = 0, chunkZ: Int = 0): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetCenterChunk)
    writeVarInt(payload, chunkX)
    writeVarInt(payload, chunkZ)
    return frame(payload, compressionThreshold)
}

/** A single empty (all-air) chunk column at (0,0) with one section, no light data, so the
 *  client stops showing "Loading terrain..." at the held spawn point. */
fun encodeEmptyChunk(ids: ReconnectPacketIds, compressionThreshold: Int, chunkX: Int = 0, chunkZ: Int = 0): ByteBuf {
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
 *  NBT string tag is valid as a text component ("just the text"), which keeps this simple.
 *
 *  Takes already-rendered legacy text ([toLegacyText] applied by the caller), since this is on
 *  the animation timer's hot path (fires every `animationInterval`, 500ms by default, for every
 *  player waiting to reconnect) - re-running MiniMessage parsing on every single tick was real,
 *  needless work piling onto the event-loop thread. Callers should render once (e.g. at config
 *  load) and reuse the result, not parse per-send. */
fun encodeActionBar(ids: ReconnectPacketIds, renderedText: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetActionBarText)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, renderedText)
    return frame(payload, compressionThreshold)
}

/** Title text (Play state) - the large centered text shown while waiting to reconnect. Takes
 *  already-rendered legacy text - see [encodeActionBar]'s doc for why. */
fun encodeSetTitleText(ids: ReconnectPacketIds, renderedText: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetTitleText)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, renderedText)
    return frame(payload, compressionThreshold)
}

/** Subtitle text (Play state) - the smaller text shown under the title. Takes already-rendered
 *  legacy text - see [encodeActionBar]'s doc for why. */
fun encodeSetSubtitleText(ids: ReconnectPacketIds, renderedText: String, compressionThreshold: Int): ByteBuf {
    val payload = Unpooled.buffer()
    writeVarInt(payload, ids.playSetSubtitleText)
    payload.writeByte(Nbt.STRING)
    Nbt.writeString(payload, renderedText)
    return frame(payload, compressionThreshold)
}

