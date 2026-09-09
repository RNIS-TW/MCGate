package me.hippodev

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.LoggingEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class ColorConsoleAppenderTest {

    private val realErr = System.err
    private val captured = ByteArrayOutputStream()

    @BeforeEach
    fun redirect() {
        System.setErr(PrintStream(captured, true))
    }

    @AfterEach
    fun restore() {
        System.setErr(realErr)
    }

    private fun appender(): ColorConsoleAppender {
        val ctx = LoggerContext()
        val enc = PatternLayoutEncoder().apply {
            context = ctx
            pattern = "%d{HH:mm:ss} [%level] %msg%n"
            start()
        }
        return ColorConsoleAppender().apply {
            context = ctx
            encoder = enc
            start()
        }
    }

    private fun event(level: Level, msg: String): LoggingEvent {
        val logger = LoggerContext().getLogger("test")
        return LoggingEvent("fqcn", logger, level, msg, null, null)
    }

    @Test
    fun `each event is written via System-err-println`() {
        appender().doAppend(event(Level.INFO, "hello world"))
        val out = captured.toString()
        assertTrue(out.contains("[INFO]"), out)
        assertTrue(out.contains("hello world"), out)
        assertTrue(out.endsWith("\n"))
    }

    @Test
    fun `a multi-line message is emitted one line at a time`() {
        appender().doAppend(event(Level.WARN, "line one\nline two\nline three"))
        // 3 println calls -> 3 newline-terminated lines.
        assertEquals(3, captured.toString().count { it == '\n' })
    }

    @Test
    fun `an appender with no encoder does not start and writes nothing`() {
        val a = ColorConsoleAppender().apply { context = LoggerContext(); start() }
        assertTrue(!a.isStarted)
        a.doAppend(event(Level.INFO, "nope"))
        assertEquals("", captured.toString())
    }
}
