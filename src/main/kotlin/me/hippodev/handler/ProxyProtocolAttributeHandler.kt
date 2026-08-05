package me.hippodev.handler

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.haproxy.HAProxyMessage
import me.hippodev.protocol.REAL_REMOTE_ADDRESS
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * Consumes the single [HAProxyMessage] event `HAProxyMessageDecoder` produces at the start of a
 * connection (only installed ahead of this handler when `proxyProtocol: true` is set globally -
 * see GateConfig and Main.kt's pipeline setup), stores the real client address it carries as a
 * channel attribute, then removes itself. Every later handler in the pipeline reads that address
 * via [me.hippodev.protocol.effectiveRemoteAddress] instead of the channel's own remoteAddress(),
 * which at that point is just the upstream load balancer's own address.
 */
class ProxyProtocolAttributeHandler : SimpleChannelInboundHandler<HAProxyMessage>() {
    private val log = LoggerFactory.getLogger(ProxyProtocolAttributeHandler::class.java)

    override fun channelRead0(ctx: ChannelHandlerContext, msg: HAProxyMessage) {
        val addr = InetSocketAddress(msg.sourceAddress(), msg.sourcePort())
        ctx.channel().attr(REAL_REMOTE_ADDRESS).set(addr)
        ctx.pipeline().remove(this)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // A client connecting without a PROXY header (misconfiguration, or something probing the
        // port directly) fails to decode here and should just be dropped quietly, same as any
        // other malformed-handshake connection - not worth a full stack trace per occurrence.
        log.debug("PROXY protocol header error", cause)
        ctx.close()
    }
}
