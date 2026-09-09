package me.hippodev

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConsoleColorsTest {

    private val esc = '\u001B'
    private fun strip(s: String) = s.replace(Regex("\\u001B\\[[0-9;]*m"), "")

    @Test
    fun `a standard log line keeps its text intact once ANSI is stripped`() {
        val line = "00:26:29 [INFO] Opening voicechat relay session: /128.118.12.30:40826 (via /162.158.243.4:41879) -> /172.16.27.106:28002"
        val out = colorize(line)

        assertTrue(out.contains(esc), "expected ANSI colour codes")
        assertEquals(
            "[00:26:29] INFO  Opening voicechat relay session: /128.118.12.30:40826 (via /162.158.243.4:41879) -> /172.16.27.106:28002",
            strip(out)
        )
    }

    @Test
    fun `WARN and ERROR render with a fixed-width tag and preserve the message`() {
        val warn = colorize("00:14:13 [WARN] Voicechat datagram from /172.68.87.68:15527 dropped: no header")
        val error = colorize("00:14:13 [ERROR] boom")
        assertEquals("[00:14:13] WARN  Voicechat datagram from /172.68.87.68:15527 dropped: no header", strip(warn))
        assertEquals("[00:14:13] ERROR boom", strip(error))
        assertTrue(warn.contains(esc))
    }

    @Test
    fun `unrecognised lines pass through with text preserved`() {
        val stack = "\tat me.hippodev.Whatever.foo(Whatever.kt:42)"
        assertEquals(stack, strip(colorize(stack)))
    }

    @Test
    fun `highlighters are stable under a second pass`() {
        val line = "00:27:34 [INFO] Disconnected: 'uhc.mcf.routing.tw' (HippoDev, cd614eaf-ab2c-4dd3-ad18-68381f34b745) from /1.2.3.4:5 -> /6.7.8.9:10 (connected 67137 ms)"
        val once = colorize(line)
        assertEquals(strip(once), strip(colorize(strip(once))))
    }

    @Test
    fun `plain text with an IP stays readable`() {
        assertEquals("hello 10.0.0.1 world", strip(colorize("hello 10.0.0.1 world")))
    }
}
