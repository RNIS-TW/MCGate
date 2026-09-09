package me.hippodev.handler

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FloodControlTest {

    @Test
    fun `global connection cap admits up to the limit then rejects, and frees on release`() {
        repeat(5) { assertTrue(GlobalConnections.tryAcquire(5)) }
        assertFalse(GlobalConnections.tryAcquire(5), "6th concurrent connection over the cap must be rejected")
        GlobalConnections.release()
        assertTrue(GlobalConnections.tryAcquire(5), "a freed slot must be reusable")
        repeat(5) { GlobalConnections.release() }
    }

    @Test
    fun `per-IP rate limiter allows a burst then throttles within the window`() {
        val ip = "198.51.100.42"
        repeat(4) { assertTrue(ConnectionRates.tryAcquire(ip, 4, 10_000)) }
        assertFalse(ConnectionRates.tryAcquire(ip, 4, 10_000), "5th connection in the window must be throttled")
    }

    @Test
    fun `per-IP rate limiter resets after the window elapses`() {
        val ip = "198.51.100.43"
        assertTrue(ConnectionRates.tryAcquire(ip, 1, 50))
        assertFalse(ConnectionRates.tryAcquire(ip, 1, 50))
        Thread.sleep(70)
        assertTrue(ConnectionRates.tryAcquire(ip, 1, 50), "a new window must admit again")
    }

    @Test
    fun `held-reconnect cap admits up to max then rejects, and frees on exit`() {
        val original = HeldReconnectSessions.max
        try {
            HeldReconnectSessions.max = 2
            // Drain any residue from other tests.
            while (HeldReconnectSessions.current > 0) HeldReconnectSessions.exit()
            assertTrue(HeldReconnectSessions.tryEnter())
            assertTrue(HeldReconnectSessions.tryEnter())
            assertFalse(HeldReconnectSessions.tryEnter(), "3rd hold over the cap must be rejected")
            HeldReconnectSessions.exit()
            assertTrue(HeldReconnectSessions.tryEnter(), "a freed hold slot must be reusable")
            HeldReconnectSessions.exit()
            HeldReconnectSessions.exit()
        } finally {
            HeldReconnectSessions.max = original
        }
    }

    @Test
    fun `held-reconnect cap of zero means unlimited`() {
        val original = HeldReconnectSessions.max
        try {
            HeldReconnectSessions.max = 0
            repeat(1000) { assertTrue(HeldReconnectSessions.tryEnter()) }
            repeat(1000) { HeldReconnectSessions.exit() }
        } finally {
            HeldReconnectSessions.max = original
        }
    }
}
