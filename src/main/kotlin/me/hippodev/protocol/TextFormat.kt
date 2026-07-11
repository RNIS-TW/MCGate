package me.hippodev.protocol

import net.kyori.adventure.text.Component
import net.kyori.adventure.text.minimessage.MiniMessage
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer

private val miniMessage = MiniMessage.miniMessage()
private val jsonSerializer = GsonComponentSerializer.gson()
private val legacySerializer = LegacyComponentSerializer.builder().hexColors().useUnusualXRepeatedCharacterHexFormat().build()

private val LEGACY_TAGS = mapOf(
    '0' to "black", '1' to "dark_blue", '2' to "dark_green", '3' to "dark_aqua",
    '4' to "dark_red", '5' to "dark_purple", '6' to "gold", '7' to "gray",
    '8' to "dark_gray", '9' to "blue", 'a' to "green", 'b' to "aqua",
    'c' to "red", 'd' to "light_purple", 'e' to "yellow", 'f' to "white",
    'k' to "obfuscated", 'l' to "bold", 'm' to "strikethrough", 'n' to "underlined",
    'o' to "italic", 'r' to "reset"
)

/** Rewrites `&`-prefixed legacy color codes (e.g. `&e`) into their MiniMessage tag equivalent
 *  (`<yellow>`). MiniMessage's parser deliberately rejects raw `&`/`§` codes in its input (to
 *  avoid ambiguous mixing) - converting them to real tags first means legacy-coded config values
 *  keep working exactly as before, while genuine MiniMessage tags (`<red>`, `<gradient:...>`,
 *  `<bold>`, `<#ff0000>`, etc. - see https://docs.advntr.dev/minimessage/format.html) elsewhere in
 *  the same string are parsed on top, so config authors can freely mix both or use whichever they
 *  prefer without any existing `&`-coded config breaking. */
private fun legacyCodesToTags(value: String): String {
    val sb = StringBuilder(value.length)
    var i = 0
    while (i < value.length) {
        val c = value[i]
        val tag = if (c == '&' && i + 1 < value.length) LEGACY_TAGS[value[i + 1].lowercaseChar()] else null
        if (tag != null) {
            sb.append('<').append(tag).append('>')
            i += 2
        } else {
            sb.append(c)
            i++
        }
    }
    return sb.toString()
}

/** Parses [input] as rich text - see [legacyCodesToTags] for how legacy `&`-codes and MiniMessage
 *  tags coexist in the same string. */
fun parseRichText(input: String): Component = miniMessage.deserialize(legacyCodesToTags(input))

/** Serializes to the Minecraft JSON chat component format used by e.g. the status/MOTD response
 *  and the Login Disconnect packet - full fidelity, including MiniMessage features a flat legacy
 *  string can't express (hover text, click events, etc.). */
fun toJsonComponent(input: String): String = jsonSerializer.serialize(parseRichText(input))

/** Serializes to a flat `§`-coded string, with hex colors (including MiniMessage gradients, which
 *  get sampled into a run of per-character hex codes) written as the `§x§f§f...` extension every
 *  client since 1.16 understands. For packets that only carry a plain NBT string rather than a
 *  full structured component - Title/Subtitle/ActionBar/PlayDisconnect (see ReconnectProtocol.kt)
 *  - which can still show colors and basic formatting, just not hover/click/etc. */
fun toLegacyText(input: String): String = legacySerializer.serialize(parseRichText(input))
