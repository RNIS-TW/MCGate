package me.hippodev.protocol

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TextFormatTest {
    @Test
    fun `legacy ampersand codes still work`() {
        val legacy = toLegacyText("&eHello &lworld")
        assertEquals("§eHello §lworld", legacy)
    }

    @Test
    fun `minimessage tags are parsed`() {
        val legacy = toLegacyText("<red>Hello</red>")
        assertTrue(legacy.contains("§c") || legacy.contains("§x"), "expected a color code, got: $legacy")
    }

    @Test
    fun `legacy and minimessage can be mixed`() {
        val legacy = toLegacyText("&e<bold>Offline</bold>")
        assertTrue(legacy.contains("§e"), "expected legacy color to survive, got: $legacy")
        assertTrue(legacy.contains("§l"), "expected bold from the MiniMessage tag, got: $legacy")
    }

    @Test
    fun `json component wraps plain text`() {
        val json = toJsonComponent("Hello")
        assertTrue(json.contains("\"Hello\""), "expected text content in JSON, got: $json")
    }

    @Test
    fun `json component renders minimessage color`() {
        val json = toJsonComponent("<red>Hello</red>")
        assertTrue(json.contains("color"), "expected a color field in JSON, got: $json")
    }
}
