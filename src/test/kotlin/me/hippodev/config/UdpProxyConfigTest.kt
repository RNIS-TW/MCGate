package me.hippodev.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

/** Parsing of the top-level `udpProxy:` list of static UDP forwards. */
class UdpProxyConfigTest {

    private fun load(yaml: String): GateConfig {
        val dir = Files.createTempDirectory("mcgate-udpproxy")
        val file = dir.resolve("config.yml").toFile()
        file.writeText(yaml.trimIndent())
        return GateConfig.load(file.path)
    }

    @Test
    fun `bind and backend are parsed, logSessions defaults to true`() {
        val proxies = load(
            """
            config:
              bind: 0.0.0.0:25565
              udpProxy:
                - bind: 0.0.0.0:51820
                  backend: 127.0.0.1:51821
            """
        ).udpProxies
        assertEquals(1, proxies.size)
        assertEquals("0.0.0.0:51820", proxies[0].bind)
        assertEquals("127.0.0.1:51821", proxies[0].backend)
        assertTrue(proxies[0].logSessions)
    }

    @Test
    fun `logSessions can be turned off`() {
        val proxies = load(
            """
            config:
              bind: 0.0.0.0:25565
              udpProxy:
                - bind: 0.0.0.0:51820
                  backend: 127.0.0.1:51821
                  logSessions: false
            """
        ).udpProxies
        assertFalse(proxies[0].logSessions)
    }
}
