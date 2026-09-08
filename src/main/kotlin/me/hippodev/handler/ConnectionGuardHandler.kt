package me.hippodev.handler

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.util.concurrent.ScheduledFuture
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * First handler in every incoming pipeline. Bounds the pre-login phase of a connection - the
 * window a connection-flood / slow-loris DDoS lives in:
 *
 *  - **Login deadline.** A connection that opens a socket but never completes its handshake, or
 *    completes the handshake (login) but never sends its Login Start packet, is closed after
 *    [loginTimeoutMillis]. Without this, a flood of half-open connections each pins a channel + an
 *    fd indefinitely (and, before [LoginRelayHandler] started deferring its backend dial, also made
 *    MCGate open a backend socket per bot - which the backend then logged as
 *    "[initial connection] ... read timed out"). Cancelled the moment a connection proves itself
 *    real, signalled by the [PRELOGIN_DONE] user event fired from [LoginRelayHandler]/
 *    [me.hippodev.handler.ReconnectHandler] once a backend is connected or the player is held for
 *    reconnect. Status pings are short-lived and self-closing, so they just ride the deadline out.
 *
 *  - **Per-IP concurrent pre-login cap.** At most [maxConnectionsPerIp] connections from one source
 *    IP may sit in the pre-login phase at once; further ones are dropped immediately. A connection
 *    stops counting as soon as it completes login (the same [PRELOGIN_DONE] signal), so a real
 *    player opening a few connections in a row is unaffected while a single-source socket flood is
 *    capped. Disabled when `proxyProtocol` is on, since every connection would then appear to come
 *    from the upstream load balancer's single IP.
 */
class ConnectionGuardHandler(
    private val loginTimeoutMillis: Long,
    private val maxConnectionsPerIp: Int,
) : ChannelInboundHandlerAdapter() {

    private var deadline: ScheduledFuture<*>? = null
    /** The source IP whose per-IP slot this connection currently holds, or null if it holds none
     *  (cap disabled, cap was already full when it connected, or the slot has been released). */
    private var heldIp: String? = null

    override fun channelActive(ctx: ChannelHandlerContext) {
        val ip = (ctx.channel().remoteAddress() as? InetSocketAddress)?.address?.hostAddress

        if (maxConnectionsPerIp > 0 && ip != null) {
            if (PreLoginConnections.tryAcquire(ip, maxConnectionsPerIp)) {
                heldIp = ip
            } else {
                log.debug("Dropping connection from {} - over per-IP pre-login limit ({})", ip, maxConnectionsPerIp)
                ctx.close()
                return
            }
        }

        if (loginTimeoutMillis > 0) {
            deadline = ctx.channel().eventLoop().schedule({
                log.debug("Closing {} - did not complete login within {} ms", ctx.channel().remoteAddress(), loginTimeoutMillis)
                ctx.close()
            }, loginTimeoutMillis, TimeUnit.MILLISECONDS)
        }

        ctx.fireChannelActive()
    }

    override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
        if (evt === PRELOGIN_DONE) release()
        ctx.fireUserEventTriggered(evt)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        release()
        ctx.fireChannelInactive()
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) {
        // Normally stays for the connection's life; releasing here too means an early pipeline
        // removal can't leak the per-IP slot or leave the deadline task scheduled.
        release()
    }

    /** Idempotent: cancels the login deadline and hands back the per-IP slot (if still held).
     *  Runs on the channel's event loop from every path (user event, inactive, removed). */
    private fun release() {
        deadline?.cancel(false)
        deadline = null
        heldIp?.let { PreLoginConnections.release(it) }
        heldIp = null
    }

    companion object {
        /** Fired down the client pipeline by [LoginRelayHandler] / [me.hippodev.handler.ReconnectHandler]
         *  once a connection has proven itself real (backend connected, or player held for reconnect). */
        val PRELOGIN_DONE = Any()
        private val log = LoggerFactory.getLogger(ConnectionGuardHandler::class.java)
    }
}

/** Process-wide count of connections currently in their pre-login phase, keyed by source IP.
 *  Entries self-remove when their count hits zero, so a transient flood from thousands of distinct
 *  IPs doesn't leave the map permanently bloated (unlike a plain counter map). */
object PreLoginConnections {
    private val counts = ConcurrentHashMap<String, AtomicInteger>()

    /** Reserves a pre-login slot for [ip] if fewer than [max] are already held. */
    fun tryAcquire(ip: String, max: Int): Boolean {
        val counter = counts.computeIfAbsent(ip) { AtomicInteger(0) }
        if (counter.incrementAndGet() > max) {
            // Overshot - give the slot straight back, pruning the entry if we were the last holder.
            if (counter.decrementAndGet() == 0) counts.remove(ip, counter)
            return false
        }
        return true
    }

    fun release(ip: String) {
        counts.computeIfPresent(ip) { _, counter ->
            if (counter.decrementAndGet() <= 0) null else counter
        }
    }

    /** Current pre-login connection count for [ip] - for tests/observability. */
    fun countForTest(ip: String): Int = counts[ip]?.get() ?: 0
}
