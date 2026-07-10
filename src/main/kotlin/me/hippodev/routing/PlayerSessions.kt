package me.hippodev.routing

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

data class PlayerSession(
    val name: String,
    val uuid: UUID,
    val host: String,
    val remoteAddress: String,
    /** The backend currently relaying this player, or null while held waiting for reconnect
     *  (see ReconnectHandler). */
    val backend: InetSocketAddress?,
    val connectedAt: Long
)

/** Process-wide registry of currently connected players, for the console `players` command -
 *  keyed by UUID since a player's session moves between LoginRelayHandler (actively relaying)
 *  and ReconnectHandler (waiting to reconnect) without a new connection. */
object PlayerSessions {
    private val sessions = ConcurrentHashMap<UUID, PlayerSession>()

    fun put(session: PlayerSession) {
        sessions[session.uuid] = session
    }

    fun get(uuid: UUID): PlayerSession? = sessions[uuid]

    fun remove(uuid: UUID) {
        sessions.remove(uuid)
    }

    fun all(): List<PlayerSession> = sessions.values.sortedBy { it.name.lowercase() }
}
