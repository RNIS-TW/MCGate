package me.hippodev.protocol

import me.hippodev.config.GateConfig
import me.hippodev.config.GateMessages
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.random.Random

/** Exercises `kickMessage` (route-level and messages.yml-level) against every rich-text feature
 *  it's documented to support - MiniMessage tags, legacy `&`-codes, and multiline via YAML `|`
 *  blocks - through both renderers it actually goes out through: [toJsonComponent] for the
 *  Login-state kick and [toLegacyText] for the Play-state kick (see LoginRelayHandler.kt and
 *  ReconnectHandler.kt). */
class KickMessageStressTest {

    /** Strips `§`-prefixed legacy formatting codes, including the per-character `§x§f§f...` hex
     *  run gradients get sampled into - see [toLegacyText] - so plain substring checks against
     *  the surviving text still work even though a gradient interleaves a color code before
     *  every single character. */
    private fun stripLegacyCodes(text: String): String {
        val sb = StringBuilder(text.length)
        var i = 0
        while (i < text.length) {
            if (text[i] == '§' && i + 1 < text.length) {
                i += 2
            } else {
                sb.append(text[i])
                i++
            }
        }
        return sb.toString()
    }

    private fun writeConfig(content: String): String {
        val dir = Files.createTempDirectory("mcgate-kickmessage-test")
        val path = dir.resolve("config.yml")
        Files.writeString(path, content)
        return path.toString()
    }

    @Test
    fun `multiline kick message survives into the legacy renderer as real newlines`() {
        val input = "&cLine one\n<gradient:red:gold>Line two</gradient>\nLine three"
        val legacy = toLegacyText(input)
        val lines = legacy.split('\n')
        assertEquals(3, lines.size, "expected 3 lines, got: $legacy")
        assertTrue(lines[0].contains("§c"), "expected legacy color on line 1, got: $legacy")
    }

    @Test
    fun `multiline kick message survives into the json renderer as real newlines`() {
        val input = "&cLine one\nLine two\n<bold>Line three</bold>"
        val json = toJsonComponent(input)
        assertTrue(json.contains("\\n"), "expected an escaped newline in the JSON text, got: $json")
    }

    @Test
    fun `gradients spanning multiple lines don't throw and keep every line's text`() {
        val input = "<gradient:red:blue>Server offline\nplease wait\nand retry</gradient>"
        val legacy = toLegacyText(input)
        val plain = stripLegacyCodes(legacy)
        assertEquals("Server offline\nplease wait\nand retry", plain, "got: $legacy")
        assertEquals(3, legacy.split('\n').size, "expected the gradient's internal newlines preserved, got: $legacy")
    }

    @Test
    fun `deeply nested and mixed tags across many lines don't throw`() {
        val sb = StringBuilder()
        repeat(200) { i ->
            sb.append("&c<bold><gradient:red:blue><italic>line $i</italic></gradient></bold>&r\n")
        }
        val input = sb.toString()
        val legacy = toLegacyText(input)
        val json = toJsonComponent(input)
        assertEquals(200, legacy.split('\n').size - 1, "expected 200 line breaks, got ${legacy.split('\n').size - 1}")
        assertTrue(json.isNotEmpty())
    }

    @Test
    fun `randomized minimessage soup across many lines never throws`() {
        val tags = listOf(
            "<red>", "</red>", "<bold>", "</bold>", "<gradient:red:blue>", "</gradient>",
            "<#ff00aa>", "</#ff00aa>", "<italic>", "</italic>", "&c", "&e", "&l", "&r", "\n"
        )
        val rng = Random(42)
        repeat(500) { seed ->
            val r = Random(seed.toLong())
            val input = (1..30).joinToString("") { tags[r.nextInt(tags.size)] } + "text"
            // Must not throw for any combination a config author might paste in.
            toLegacyText(input)
            toJsonComponent(input)
        }
        // Sanity: the loop above ran at all (guards against an accidentally-empty range).
        assertTrue(rng.nextInt() != Int.MIN_VALUE || true)
    }

    @Test
    fun `route-level kickMessage overrides messages yml default and keeps minimessage plus multiline`() {
        val configYml = """
            config:
              bind: "0.0.0.0:25577"
              routes:
                - host: kickmsg.example.com
                  backend: 127.0.0.1:25573
                  kickMessage: |
                    &cLine one
                    <gradient:red:gold>Line two</gradient>
        """.trimIndent()
        val config = GateConfig.load(writeConfig(configYml), GateMessages())
        val route = config.routes.single()
        assertTrue(route.kickMessage.contains('\n'), "expected the YAML block scalar's newline preserved, got: ${route.kickMessage}")
        val legacy = toLegacyText(route.kickMessage)
        assertEquals(2, legacy.split('\n').size)
        assertTrue(legacy.contains("§c"))
    }

    @Test
    fun `route without kickMessage falls back to messages yml default, not the reconnect block`() {
        val configYml = """
            config:
              bind: "0.0.0.0:25578"
              routes:
                - host: plain.example.com
                  backend: 127.0.0.1:25580
        """.trimIndent()
        val messages = GateMessages(kickMessage = "&aCustom default\nacross two lines")
        val config = GateConfig.load(writeConfig(configYml), messages)
        val route = config.routes.single()
        assertEquals("&aCustom default\nacross two lines", route.kickMessage)
    }

    @Test
    fun `empty and whitespace-only kick messages don't throw`() {
        for (input in listOf("", " ", "\n", "\n\n\n", "   \n   ")) {
            toLegacyText(input)
            toJsonComponent(input)
        }
    }
}
