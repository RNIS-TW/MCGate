package me.hippodev.config

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

/** Parsing of the top-level `udp:` block - in particular its `proxyProtocol` switch, which is
 *  independent of the TCP listener's top-level `proxyProtocol` but falls back to it when unset. */
class UdpConfigTest {

    private fun load(yaml: String): GateConfig {
        val dir = Files.createTempDirectory("mcgate-udp")
        val file = dir.resolve("config.yml").toFile()
        file.writeText(yaml.trimIndent())
        return GateConfig.load(file.path)
    }

    @Test
    fun `absent block inherits the TCP proxyProtocol - false`() {
        assertFalse(load("config:\n  bind: 0.0.0.0:25565").udp.proxyProtocol)
    }

    @Test
    fun `absent block inherits the TCP proxyProtocol - true`() {
        assertTrue(load("config:\n  proxyProtocol: true").udp.proxyProtocol)
    }

    @Test
    fun `udp proxyProtocol overrides the TCP one independently`() {
        assertTrue(load("config:\n  proxyProtocol: false\n  udp:\n    proxyProtocol: true").udp.proxyProtocol)
        assertFalse(load("config:\n  proxyProtocol: true\n  udp:\n    proxyProtocol: false").udp.proxyProtocol)
    }
}
