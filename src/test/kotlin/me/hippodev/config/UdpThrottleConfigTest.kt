package me.hippodev.config

import me.hippodev.udp.UdpThrottle
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Files

/** Parsing of the `udpThrottle:` block and its wiring into the process-wide [UdpThrottle] holder. */
class UdpThrottleConfigTest {

    private fun load(yaml: String): GateConfig {
        val dir = Files.createTempDirectory("mcgate-udp-throttle")
        val file = dir.resolve("config.yml").toFile()
        file.writeText(yaml.trimIndent())
        return GateConfig.load(file.path)
    }

    @AfterEach
    fun tearDown() = UdpThrottle.reset()

    @Test
    fun `absent block uses defaults`() {
        val t = load("config:\n  bind: 0.0.0.0:25565").udpThrottle
        assertEquals(8192, t.maxSessions)
        assertEquals(64, t.maxSessionsPerIp)
        assertEquals(256, t.pendingPacketsPerSession)
        assertEquals(300_000, t.idleTimeoutMillis)
        assertEquals(20_000, t.noReplyTeardownMillis)
    }

    @Test
    fun `full block parses, durations included`() {
        val t = load(
            """
            config:
              udpThrottle:
                maxSessions: 1000
                maxSessionsPerIp: 8
                pendingPacketsPerSession: 32
                idleTimeout: 2m
                noReplyTeardown: 5s
            """
        ).udpThrottle
        assertEquals(1000, t.maxSessions)
        assertEquals(8, t.maxSessionsPerIp)
        assertEquals(32, t.pendingPacketsPerSession)
        assertEquals(120_000, t.idleTimeoutMillis)
        assertEquals(5_000, t.noReplyTeardownMillis)
    }

    @Test
    fun `partial block keeps defaults for the rest`() {
        val t = load(
            """
            config:
              udpThrottle:
                maxSessionsPerIp: 0
            """
        ).udpThrottle
        assertEquals(0, t.maxSessionsPerIp)
        assertEquals(8192, t.maxSessions)
        assertEquals(20_000, t.noReplyTeardownMillis)
    }

    @Test
    fun `apply pushes values into the holder and clamps pending to at least 1`() {
        UdpThrottle.apply(UdpThrottleConfig(maxSessions = 5, maxSessionsPerIp = 3, pendingPacketsPerSession = 0))
        assertEquals(5, UdpThrottle.maxSessions)
        assertEquals(3, UdpThrottle.maxSessionsPerIp)
        assertEquals(1, UdpThrottle.pendingPacketsPerSession)
    }
}
