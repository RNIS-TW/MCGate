package me.hippodev.udp

import me.hippodev.config.UdpThrottleConfig

/**
 * Process-wide, hot-reloadable anti-abuse limits for every UDP relay path - the voicechat relay
 * ([me.hippodev.voice.VoiceRelay]) and each static [UdpProxy]. UDP has no handshake to gate on and
 * a trivially spoofable source address, so these caps are the only thing bounding how much a
 * datagram flood can make MCGate allocate (a Session object plus a backend-facing socket/fd per
 * distinct source).
 *
 * Populated from config at startup and on every reload (see Main.kt). The relays read these fields
 * live on each datagram / reaper tick, so a reload takes effect without a restart - only the UDP
 * *bind* addresses are startup-only.
 */
object UdpThrottle {
    private val defaults = UdpThrottleConfig()

    @Volatile var maxSessions: Int = defaults.maxSessions
        private set
    @Volatile var maxSessionsPerIp: Int = defaults.maxSessionsPerIp
        private set
    @Volatile var pendingPacketsPerSession: Int = defaults.pendingPacketsPerSession
        private set
    @Volatile var idleTimeoutMillis: Long = defaults.idleTimeoutMillis
        private set
    @Volatile var noReplyTeardownMillis: Long = defaults.noReplyTeardownMillis
        private set

    fun apply(c: UdpThrottleConfig) {
        maxSessions = c.maxSessions
        maxSessionsPerIp = c.maxSessionsPerIp
        pendingPacketsPerSession = c.pendingPacketsPerSession.coerceAtLeast(1)
        idleTimeoutMillis = c.idleTimeoutMillis
        noReplyTeardownMillis = c.noReplyTeardownMillis
    }

    /** Test hook - restore the compiled-in defaults. */
    fun reset() = apply(defaults)
}
