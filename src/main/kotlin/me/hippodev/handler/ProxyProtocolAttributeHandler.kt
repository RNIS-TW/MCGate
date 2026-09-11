package me.hippodev.handler

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.haproxy.HAProxyCommand
import io.netty.handler.codec.haproxy.HAProxyMessage
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol
import me.hippodev.protocol.REAL_REMOTE_ADDRESS
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.InetSocketAddress

/**
 * Consumes the single [HAProxyMessage] event `HAProxyMessageDecoder` produces at the start of a
 * connection (only installed ahead of this handler when `proxyProtocol: true` is set globally -
 * see GateConfig and Main.kt's pipeline setup), stores the real client address it carries as a
 * channel attribute, then removes itself. Every later handler in the pipeline reads that address
 * via [me.hippodev.protocol.effectiveRemoteAddress] instead of the channel's own remoteAddress(),
 * which at that point is just the upstream proxy's own address.
 *
 * When the header carries no usable client address - a v2 `LOCAL` command, an `UNSPEC` family, or a
 * degenerate `PROXY TCP4/6` with a loopback/wildcard source or port 0 (Cloudflare Spectrum and
 * TCPShield send such headers on their server-list/MOTD health probes) - the attribute is left
 * unset and [me.hippodev.protocol.effectiveRemoteAddress] falls back to the real TCP peer, as the
 * PROXY protocol spec requires. Storing `InetSocketAddress("", 0)` / `127.0.0.1:0` instead used to
 * propagate downstream: [StatusHandler] would then send an onward PROXY header with port 0 that a
 * strict `proxyProtocol` backend RST-drops, so every MOTD probe through such a proxy failed.
 */
class ProxyProtocolAttributeHandler : SimpleChannelInboundHandler<HAProxyMessage>() {
    private val log = LoggerFactory.getLogger(ProxyProtocolAttributeHandler::class.java)

    override fun channelRead0(ctx: ChannelHandlerContext, msg: HAProxyMessage) {
        val forwarded = forwardedClientAddress(msg)
        if (forwarded != null) {
            ctx.channel().attr(REAL_REMOTE_ADDRESS).set(forwarded)
        } else {
            log.debug("PROXY header {}/{} src={}:{} carries no usable client address, using real peer {}",
                msg.command(), msg.proxiedProtocol(), msg.sourceAddress(), msg.sourcePort(),
                ctx.channel().remoteAddress())
        }
        ctx.pipeline().remove(this)
    }

    /** The real client address a [msg] genuinely forwards, or null for a health-check / degenerate
     *  header that should fall back to the connection's own peer. */
    private fun forwardedClientAddress(msg: HAProxyMessage): InetSocketAddress? {
        if (msg.command() != HAProxyCommand.PROXY) return null
        if (msg.proxiedProtocol() != HAProxyProxiedProtocol.TCP4 &&
            msg.proxiedProtocol() != HAProxyProxiedProtocol.TCP6
        ) return null
        val host = msg.sourceAddress()
        val port = msg.sourcePort()
        if (host.isNullOrEmpty() || port !in 1..65535) return null
        // sourceAddress() from the v2 binary header is already a numeric literal, so this never
        // hits DNS; the try/catch is just for a malformed one.
        val addr = try { InetAddress.getByName(host) } catch (e: Exception) { return null }
        if (addr.isAnyLocalAddress || addr.isLoopbackAddress) return null
        return InetSocketAddress(addr, port)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // A client connecting without a PROXY header (misconfiguration, or something probing the
        // port directly) fails to decode here and should just be dropped quietly, same as any
        // other malformed-handshake connection - not worth a full stack trace per occurrence.
        log.debug("PROXY protocol header error", cause)
        ctx.close()
    }
}
