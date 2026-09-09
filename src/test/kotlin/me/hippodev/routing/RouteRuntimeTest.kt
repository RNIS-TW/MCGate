package me.hippodev.routing

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress

class RouteRuntimeTest {

    @Test
    fun `status-dial slots cap concurrent dials per backend and free on end`() {
        val rt = RouteRuntime()
        val addr = InetSocketAddress("127.0.0.1", 25565)
        repeat(MAX_CONCURRENT_STATUS_DIALS) { assertTrue(rt.tryBeginStatusDial(addr)) }
        assertFalse(rt.tryBeginStatusDial(addr), "one past the cap must be refused")

        rt.endStatusDial(addr)
        assertTrue(rt.tryBeginStatusDial(addr), "a freed slot is reusable")
        repeat(MAX_CONCURRENT_STATUS_DIALS) { rt.endStatusDial(addr) }
        // Back to zero -> a fresh dial is allowed again.
        assertTrue(rt.tryBeginStatusDial(addr))
        rt.endStatusDial(addr)
    }

    @Test
    fun `status-dial slots are tracked independently per backend address`() {
        val rt = RouteRuntime()
        val a = InetSocketAddress("127.0.0.1", 1)
        val b = InetSocketAddress("127.0.0.1", 2)
        repeat(MAX_CONCURRENT_STATUS_DIALS) { rt.tryBeginStatusDial(a) }
        assertTrue(rt.tryBeginStatusDial(b), "a different backend has its own budget")
        rt.endStatusDial(b)
        repeat(MAX_CONCURRENT_STATUS_DIALS) { rt.endStatusDial(a) }
    }
}
