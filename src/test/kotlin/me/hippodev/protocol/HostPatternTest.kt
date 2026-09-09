package me.hippodev.protocol

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Test
import java.time.Duration

class HostPatternTest {

    @Test
    fun `single wildcard captures the prefix`() {
        val p = HostPattern("*.mc.example.com")
        assertEquals(listOf("alpha"), p.match("alpha.mc.example.com"))
        assertEquals(listOf("a.b"), p.match("a.b.mc.example.com"))
    }

    @Test
    fun `question mark captures exactly one character`() {
        val p = HostPattern("eu?.example.com")
        assertEquals(listOf("1"), p.match("eu1.example.com"))
        assertNull(p.match("eu12.example.com"))
    }

    @Test
    fun `multi-wildcard captures are greedy, matching the old regex semantics`() {
        val p = HostPattern("*-*.example.com")
        // '$1' takes as much as it can while '$2' still matches.
        assertEquals(listOf("a-b", "c"), p.match("a-b-c.example.com"))
    }

    @Test
    fun `non-matching host returns null`() {
        assertNull(HostPattern("*.mc.example.com").match("evil.example.net"))
        assertNull(HostPattern("play.example.com").match("play.example.org"))
    }

    @Test
    fun `matching is case-insensitive but captures keep the input case`() {
        assertEquals(listOf("AlphA"), HostPattern("*.MC.example.com").match("AlphA.mc.EXAMPLE.com"))
    }

    @Test
    fun `trailing DNS root dot is tolerated on both sides`() {
        assertEquals(listOf("alpha"), HostPattern("*.mc.example.com.").match("alpha.mc.example.com"))
        assertEquals(listOf("alpha"), HostPattern("*.mc.example.com").match("alpha.mc.example.com."))
    }

    @Test
    fun `a pathological multi-wildcard pattern cannot be made to hang (ReDoS)`() {
        val p = HostPattern("*-*-*-*-*.example.com")
        val hostile = "a".repeat(250) // 250 chars, no dashes, no match
        assertTimeoutPreemptively(Duration.ofMillis(500)) {
            repeat(50) { assertNull(p.match(hostile)) }
        }
    }

    @Test
    fun `an over-long host is rejected`() {
        assertNull(HostPattern("*.example.com").match("a".repeat(300) + ".example.com"))
    }
}
