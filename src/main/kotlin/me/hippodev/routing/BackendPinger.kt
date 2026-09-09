package me.hippodev.routing

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import me.hippodev.protocol.encodeHandshake
import me.hippodev.protocol.encodeProxyProtocolHeader
import me.hippodev.protocol.readFrameLength
import me.hippodev.protocol.readString
import me.hippodev.protocol.readVarInt
import me.hippodev.protocol.writeVarInt
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

data class PingResult(val statusJson: String, val latencyMillis: Long)

/**
 * Opens a fresh connection to [addr] and performs a real Server List Ping
 * (handshake + status request) right now, independent of any client traffic
 * or the [PingCache]. Used for on-demand health checks (e.g. the status API)
 * rather than the passive, client-triggered pings in StatusHandler.
 */
fun pingBackendLive(
    eventLoopGroup: EventLoopGroup,
    addr: InetSocketAddress,
    protocolVersion: Int,
    virtualHost: String,
    port: Int,
    timeoutMillis: Long = 5000,
    proxyProtocol: Boolean = false
): CompletableFuture<PingResult> {
    val future = CompletableFuture<PingResult>()
    val startTime = System.currentTimeMillis()
    var openChannel: io.netty.channel.Channel? = null

    val bootstrap = Bootstrap()
        .group(eventLoopGroup)
        .channel(NioSocketChannel::class.java)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis.toInt())
        .handler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                ch.pipeline().addLast(object : ByteToMessageDecoder() {
                    override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
                        val frameStart = buf.readerIndex()
                        val length = try {
                            readFrameLength(buf, frameStart)
                        } catch (e: IllegalStateException) {
                            future.completeExceptionally(e)
                            ctx.close(); return
                        } ?: return
                        if (buf.readableBytes() < length) {
                            buf.readerIndex(frameStart); return
                        }
                        val packetId = readVarInt(buf)
                        if (packetId == 0x00) {
                            val json = readString(buf, 262144)
                            future.complete(PingResult(json, System.currentTimeMillis() - startTime))
                        } else {
                            future.completeExceptionally(IllegalStateException("Unexpected packet id $packetId from backend"))
                        }
                        ctx.close()
                    }
                })
            }
        })

    bootstrap.connect(addr).addListener(ChannelFutureListener { connectFuture ->
        if (!connectFuture.isSuccess) {
            future.completeExceptionally(connectFuture.cause() ?: IllegalStateException("connect failed"))
            return@ChannelFutureListener
        }
        val channel = connectFuture.channel()
        openChannel = channel
        if (proxyProtocol) {
            // No real client behind this probe (it's a standalone health check) - a placeholder
            // source is fine, backends enforcing proxyProtocol just need a syntactically valid
            // header before they'll process anything past it.
            val source = InetSocketAddress(InetAddress.getLoopbackAddress(), 0)
            channel.writeAndFlush(encodeProxyProtocolHeader(source, addr))
        }
        channel.writeAndFlush(encodeHandshake(protocolVersion, virtualHost, port, 1))
        val requestFrame = Unpooled.buffer()
        writeVarInt(requestFrame, 1)
        writeVarInt(requestFrame, 0x00)
        channel.writeAndFlush(requestFrame)
    })

    eventLoopGroup.next().schedule({
        if (future.completeExceptionally(TimeoutException("Ping to $addr timed out after ${timeoutMillis}ms"))) {
            openChannel?.close()
        }
    }, timeoutMillis, TimeUnit.MILLISECONDS)

    return future
}
