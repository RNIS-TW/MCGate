package me.hippodev.voice

import io.netty.channel.Channel
import me.hippodev.protocol.effectiveRemoteAddress
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** How long an entry can go un-looked-up before the periodic sweep evicts it - mirrors
 *  [me.hippodev.config.DnsCache]'s idle eviction so a client that disconnects (or never sends any
 *  UDP traffic at all) doesn't sit here forever. */
private const val ENTRY_IDLE_EVICT_MILLIS = 10 * 60_000L

/**
 * Tracks which voicechat backend a connecting client's UDP traffic (Simple Voice Chat and similar
 * mods) should be relayed to, so [VoiceRelay] can route inbound datagrams without parsing any
 * protocol of its own - UDP voice packets carry no hostname the way the Minecraft handshake does.
 * Populated from the TCP side (see `dispatch()` in Main.kt) the moment a route with a `voicechat:`
 * backend is resolved for a connecting player.
 *
 * Keyed by client IP only (not IP:port) since a voice mod's UDP source port has no relation to the
 * game connection's TCP port. Two different players behind the same NAT/router who connect to
 * *different* hostnames routed to different voicechat backends would clobber each other's entry -
 * an accepted limitation, since a UDP voice packet carries nothing else to disambiguate by.
 */
object VoiceRouting {
    private data class Entry(val backend: InetSocketAddress, @Volatile var lastSeen: Long)

    private val entries = ConcurrentHashMap<String, Entry>()
    /** Set by [VoiceRelay.start]/cleared by [VoiceRelay.stop] - lets [unregister] tear down an
     *  active relay session the instant a player logs out, rather than waiting for VoiceRelay's
     *  own idle timeout to notice the client is gone. */
    @Volatile private var relay: VoiceRelay? = null

    init {
        val reaper = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "voice-routing-reaper").apply { isDaemon = true }
        }
        reaper.scheduleAtFixedRate(
            { evictIdle() }, ENTRY_IDLE_EVICT_MILLIS, ENTRY_IDLE_EVICT_MILLIS, TimeUnit.MILLISECONDS
        )
    }

    internal fun attachRelay(relay: VoiceRelay?) {
        this.relay = relay
    }

    fun register(clientIp: String, backend: InetSocketAddress) {
        entries[clientIp] = Entry(backend, System.currentTimeMillis())
    }

    fun resolve(clientIp: String): InetSocketAddress? {
        val entry = entries[clientIp] ?: return null
        entry.lastSeen = System.currentTimeMillis()
        return entry.backend
    }

    /** Drops [clientIp]'s routing entry and immediately closes any live UDP relay session for it -
     *  called when a player's Minecraft connection actually ends (see [unregisterChannel] and its
     *  call sites in LoginRelayHandler/ReconnectHandler), so voice chat "logs out" together with
     *  the game connection instead of lingering for up to VoiceRelay's idle timeout. */
    fun unregister(clientIp: String) {
        entries.remove(clientIp)
        relay?.disconnectClient(clientIp)
    }

    /** Convenience for handler call sites that only have the client [Channel], not its bare IP. */
    fun unregisterChannel(channel: Channel) {
        val addr = channel.effectiveRemoteAddress() as? InetSocketAddress ?: return
        unregister(addr.address.hostAddress)
    }

    private fun evictIdle() {
        val cutoff = System.currentTimeMillis() - ENTRY_IDLE_EVICT_MILLIS
        entries.entries.removeIf { it.value.lastSeen < cutoff }
    }
}
