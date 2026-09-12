package me.hippodev.config

import me.hippodev.protocol.toJsonComponent
import me.hippodev.protocol.toLegacyText
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

/** Parsing of messages.yml, including the metrics-limit kick text. */
class GateMessagesTest {

    private fun load(yaml: String): GateMessages {
        val file = Files.createTempDirectory("mcgate-messages").resolve("messages.yml").toFile()
        file.writeText(yaml.trimIndent())
        return GateMessages.load(file.path)
    }

    @Test
    fun `metricsLimitKickMessage is read and falls back to the default`() {
        assertEquals(
            GateMessages().metricsLimitKickMessage,
            load("messages:\n  kickMessage: \"x\"").metricsLimitKickMessage
        )
        assertEquals(
            "&4Out of quota",
            load("messages:\n  metricsLimitKickMessage: \"&4Out of quota\"").metricsLimitKickMessage
        )
    }

    @Test
    fun `default metrics-limit message keeps its lines and gradient through both render paths`() {
        val msg = GateMessages().metricsLimitKickMessage

        // Mid-session kick path: flat legacy string. Newlines survive; the gradient is sampled
        // into the §x repeated-hex format every 1.16+ client understands.
        val legacy = toLegacyText(msg)
        assertTrue(legacy.contains('\n'), "multi-line kick text must keep its newlines")
        assertTrue(legacy.contains("§x"), "gradient must render as §x hex, got: $legacy")

        // Login-refusal path: a structured chat component with real per-segment colors + a \n.
        val json = toJsonComponent(msg)
        assertTrue(json.contains("\\n"), "component JSON must carry the line break")
        assertTrue(json.contains("#") || json.contains("color"), "component JSON must carry colors")
    }

    @Test
    fun `a YAML block scalar produces a genuine multi-line value`() {
        val m = load(
            """
            messages:
              metricsLimitKickMessage: |-
                <red>line one</red>
                <gold>line two</gold>
            """
        ).metricsLimitKickMessage
        assertEquals("<red>line one</red>\n<gold>line two</gold>", m)
    }
}
