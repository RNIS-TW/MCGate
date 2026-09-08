package me.hippodev.routing

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Guards the bounded-growth behavior added to stop scanner ping floods on wildcard routes from
 *  growing [PingCache]'s maps without limit (the OOM this was written to fix). */
class PingCacheTest {

    @Test
    fun `expired response entries are not served`() {
        val cache = PingCache()
        cache.put("a:1", "{}", 0)
        Thread.sleep(5)
        assertNull(cache.get("a:1"))
    }

    @Test
    fun `expired down markers clear on read`() {
        val cache = PingCache()
        cache.markDown("a:1")
        assertTrue(cache.isKnownDown("a:1"))
        Thread.sleep(FAILURE_TTL_MILLIS_TEST + 20)
        assertTrue(!cache.isKnownDown("a:1"))
    }

    @Test
    fun `entry map stays bounded under a flood of unique keys`() {
        val cache = PingCache()
        repeat(MAX_ENTRIES_TEST * 2) { i -> cache.put("host$i:$i", "{}", 60_000) }
        assertTrue(
            cache.entryCountForTest() <= MAX_ENTRIES_TEST,
            "expected <= $MAX_ENTRIES_TEST entries, was ${cache.entryCountForTest()}"
        )
    }

    private companion object {
        const val FAILURE_TTL_MILLIS_TEST = 5_000L
        const val MAX_ENTRIES_TEST = 10_000
    }
}
