package me.hippodev.config

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DnsCacheTest {

    @Test
    fun `IP literals never need a blocking lookup`() {
        assertTrue(DnsCache.willResolveWithoutBlocking("10.1.2.3", 25565))
        assertTrue(DnsCache.willResolveWithoutBlocking("127.0.0.1", 25565))
    }

    @Test
    fun `a never-seen hostname needs a blocking lookup until it is cached`() {
        val host = "never-seen-${System.nanoTime()}.invalid"
        assertFalse(DnsCache.willResolveWithoutBlocking(host, 25565))
        // resolve() caches even an unresolved result (briefly) so the next call is non-blocking.
        DnsCache.resolve(host, 25565)
        assertTrue(DnsCache.willResolveWithoutBlocking(host, 25565))
    }

    @Test
    fun `a failed lookup returns an unresolved address rather than throwing`() {
        val addr = DnsCache.resolve("definitely-not-a-real-host-${System.nanoTime()}.invalid", 25565)
        assertTrue(addr.isUnresolved)
    }

    @Test
    fun `runOffEventLoop executes the task`() {
        val latch = java.util.concurrent.CountDownLatch(1)
        assertTrue(DnsCache.runOffEventLoop { latch.countDown() })
        assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS))
    }
}
