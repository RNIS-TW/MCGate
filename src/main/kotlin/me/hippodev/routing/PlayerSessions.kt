package me.hippodev.routing

import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import me.hippodev.protocol.encodePlayDisconnect
import me.hippodev.protocol.reconnectPacketIds
import me.hippodev.protocol.reconnectSupported
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

data class PlayerSession(
    val name: String,
    val uuid: UUID,
    val host: String,
    val remoteAddress: String,
    /** The backend currently relaying this route, or null while held waiting for reconnect
     *  (see ReconnectHandler). */
    val backend: InetSocketAddress?,
    val connectedAt: Long,
    /** The client-facing channel for this session - the same TCP connection whether the player
     *  is actively relaying (LoginRelayHandler) or waiting to reconnect (ReconnectHandler). Lets
     *  the console `kick` command close a specific player's connection. */
    val channel: Channel,
    val protocolVersion: Int,
    /** The compression threshold negotiated on this connection (-1 = none) - needed to frame a
     *  Play Disconnect packet correctly if this session is later kicked with a message. See
     *  [BackendLoginSniffer][me.hippodev.handler.LoginRelayHandler] for where this is captured;
     *  MCGate's relay is otherwise byte-blind past login, so this is the one piece of framing
     *  state tracked for every player, not just ones being held for reconnect. */
    val compressionThreshold: Int,
    /** True once the backend has gone into online-mode encryption on this connection - from that
     *  point on it's a pure ciphertext pipe MCGate cannot inject any packet into without
     *  desyncing the client's stream cipher (see the LOGIN_ENCRYPTION_REQUEST handling in
     *  LoginRelayHandler). A kick message must not be attempted on an encrypted session; only a
     *  bare close is safe. */
    val encrypted: Boolean
)

/** Process-wide registry of currently connected players, for the console `players` command -
 *  keyed by UUID since a player's session moves between LoginRelayHandler (actively relaying)
 *  and ReconnectHandler (waiting to reconnect) without a new connection. */
object PlayerSessions {
    private val log = LoggerFactory.getLogger(PlayerSessions::class.java)
    private val sessions = ConcurrentHashMap<UUID, PlayerSession>()

    fun put(session: PlayerSession) {
        sessions[session.uuid] = session
    }

    fun get(uuid: UUID): PlayerSession? = sessions[uuid]

    fun remove(uuid: UUID) {
        sessions.remove(uuid)
    }

    fun all(): List<PlayerSession> = sessions.values.sortedBy { it.name.lowercase() }

    fun findByName(name: String): PlayerSession? = sessions.values.firstOrNull { it.name.equals(name, ignoreCase = true) }

    /** Drops a player's connection, optionally showing [message] first. MCGate relays raw bytes
     *  past login (see LoginRelayHandler), so a kick message can only be framed correctly for
     *  protocol versions covered by [reconnectSupported]'s packet-ID table (see
     *  ReconnectProtocol.kt) AND on a connection that isn't [PlayerSession.encrypted] - other
     *  cases just get disconnected with no message, same as before this could send one at all.
     *
     *  Returns the channel's close future (null if no such session) rather than a plain boolean -
     *  the write+close here is scheduled on the channel's event loop, not performed synchronously,
     *  so a caller that needs the kick to have actually gone out before proceeding (e.g. shutdown,
     *  which then tears down the very event loop this write runs on) must await it. Awaiting
     *  closeFuture() rather than the write's own future also guarantees the message was flushed
     *  before the connection closes, not raced against it. */
    fun kick(uuid: UUID, message: String? = null): ChannelFuture? {
        val session = sessions[uuid] ?: return null
        if (message != null && !session.encrypted && reconnectSupported(session.protocolVersion)) {
            val ids = reconnectPacketIds(session.protocolVersion)
            session.channel.writeAndFlush(encodePlayDisconnect(ids, message, session.compressionThreshold))
                .addListener(ChannelFutureListener.CLOSE)
        } else {
            if (message != null) {
                val reason = if (session.encrypted) "connection is online-mode encrypted"
                    else "protocol ${session.protocolVersion} has no packet-ID table"
                log.debug("Can't send kick message to '{}' - {}, closing without one", session.name, reason)
            }
            session.channel.close()
        }
        return session.channel.closeFuture()
    }
}
