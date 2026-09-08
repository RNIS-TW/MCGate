package me.hippodev.handler

import io.netty.channel.embedded.EmbeddedChannel
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConnectionGuardTest {

    @Test
    fun `per-IP cap admits up to the limit then rejects, and frees on release`() {
        val ip = "203.0.113.7"
        assertTrue(PreLoginConnections.tryAcquire(ip, 3))
        assertTrue(PreLoginConnections.tryAcquire(ip, 3))
        assertTrue(PreLoginConnections.tryAcquire(ip, 3))
        assertFalse(PreLoginConnections.tryAcquire(ip, 3), "4th concurrent pre-login connection must be rejected")

        PreLoginConnections.release(ip)
        assertTrue(PreLoginConnections.tryAcquire(ip, 3), "a freed slot must be reusable")

        repeat(3) { PreLoginConnections.release(ip) }
        assertTrue(PreLoginConnections.countForTest(ip) == 0, "entry must drop to zero (and be pruned) once idle")
    }

    @Test
    fun `login deadline closes a connection that never completes login`() {
        val ch = EmbeddedChannel(ConnectionGuardHandler(loginTimeoutMillis = 40, maxConnectionsPerIp = 0))
        assertTrue(ch.isActive)
        Thread.sleep(70)
        ch.runScheduledPendingTasks()
        assertFalse(ch.isActive, "connection past the login deadline must be closed")
    }

    @Test
    fun `PRELOGIN_DONE cancels the login deadline`() {
        val ch = EmbeddedChannel(ConnectionGuardHandler(loginTimeoutMillis = 40, maxConnectionsPerIp = 0))
        ch.pipeline().fireUserEventTriggered(ConnectionGuardHandler.PRELOGIN_DONE)
        Thread.sleep(70)
        ch.runScheduledPendingTasks()
        assertTrue(ch.isActive, "a connection that completed login must not be closed by the deadline")
        ch.finishAndReleaseAll()
    }
}
