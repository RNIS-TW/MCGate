package me.hippodev.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

/** Parsing of the per-route `metrics:` block in config.yml. */
class RouteMetricsConfigTest {

    private fun load(yaml: String): GateConfig {
        val dir = Files.createTempDirectory("mcgate-metrics-config")
        val file = dir.resolve("config.yml").toFile()
        file.writeText(yaml.trimIndent())
        return GateConfig.load(file.path)
    }

    @Test
    fun `full metrics block parses`() {
        val cfg = load(
            """
            config:
              routes:
                - host: metered.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    file: data/id.json
                    upload:
                      enabled: true
                      limit: -1
                    download:
                      enabled: true
                      limit: 1048576
            """
        )
        val m = cfg.routes.single().metrics!!
        assertEquals("data/id.json", m.file)
        assertTrue(m.upload.enabled)
        assertEquals(-1, m.upload.limit)
        assertTrue(m.download.enabled)
        assertEquals(1_048_576, m.download.limit)
        assertTrue(m.active)
    }

    @Test
    fun `resetInterval parses as a duration including days`() {
        fun intervalFor(raw: String) = load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    resetInterval: $raw
                    upload:
                      enabled: true
            """
        ).routes.single().metrics!!.resetIntervalMillis

        assertEquals(30L * 86_400_000, intervalFor("30d"))
        assertEquals(168L * 3_600_000, intervalFor("168h"))
        assertEquals(0, load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    upload:
                      enabled: true
            """
        ).routes.single().metrics!!.resetIntervalMillis)
    }

    @Test
    fun `no metrics block leaves route metrics null`() {
        val cfg = load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
            """
        )
        assertNull(cfg.routes.single().metrics)
    }

    @Test
    fun `metrics block with no direction enabled is dropped`() {
        val cfg = load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    file: data/id.json
            """
        )
        assertNull(cfg.routes.single().metrics)
    }

    @Test
    fun `limit accepts k m g t p suffixes multiplied by 1024`() {
        fun limitFor(raw: String) = load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    upload:
                      enabled: true
                      limit: $raw
            """
        ).routes.single().metrics!!.upload.limit

        assertEquals(5_242_880, limitFor("5242880"))
        assertEquals(100L * 1024 * 1024 * 1024, limitFor("100g"))
        assertEquals(100L * 1024 * 1024 * 1024, limitFor("\"100GiB\""))
        assertEquals(512L * 1024 * 1024, limitFor("512m"))
        assertEquals(2L * 1024 * 1024 * 1024 * 1024, limitFor("\"2 t\""))
        assertEquals(-1, limitFor("-1"))
    }

    @Test
    fun `a nonsense limit is rejected at load`() {
        assertThrows(IllegalStateException::class.java) {
            load(
                """
                config:
                  routes:
                    - host: a.example.com
                      backend: 127.0.0.1:25566
                      metrics:
                        upload:
                          enabled: true
                          limit: "100 bananas"
                """
            )
        }
    }

    @Test
    fun `omitted limit defaults to unlimited and file is optional`() {
        val cfg = load(
            """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    upload:
                      enabled: true
            """
        )
        val m = cfg.routes.single().metrics!!
        assertNull(m.file)
        assertEquals(-1, m.upload.limit)
        assertFalse(m.download.enabled)
    }

    @Test
    fun `metrics config participates in Route equality for reload carry-over`() {
        val yaml = """
            config:
              routes:
                - host: a.example.com
                  backend: 127.0.0.1:25566
                  metrics:
                    file: data/id.json
                    upload:
                      enabled: true
        """
        assertEquals(load(yaml).routes.single(), load(yaml).routes.single())
    }
}
