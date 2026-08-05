package me.hippodev.protocol

import io.netty.channel.Channel
import io.netty.util.AttributeKey
import java.net.InetSocketAddress
import java.net.SocketAddress

/** Set once per connection by [me.hippodev.handler.ProxyProtocolAttributeHandler] when
 *  `proxyProtocol: true` is enabled globally (see GateConfig) - the real client address an
 *  upstream load balancer reported via a PROXY protocol header, as opposed to [Channel.remoteAddress]
 *  which in that setup is just the load balancer's own address. Absent entirely when the feature
 *  is off, so every other reader must fall back to [Channel.remoteAddress] - see
 *  [effectiveRemoteAddress]. */
val REAL_REMOTE_ADDRESS: AttributeKey<InetSocketAddress> = AttributeKey.valueOf("mcgate.realRemoteAddress")

/** The address MCGate should treat as this connection's real client: the PROXY-protocol-reported
 *  address if [proxyProtocol][me.hippodev.config.GateConfig.proxyProtocol] captured one for this
 *  channel, otherwise the channel's own remote address - the common case, no upstream proxy in
 *  front of MCGate. Every log line/relay/voice-routing lookup that wants the player's real IP
 *  should read this instead of calling `remoteAddress()` directly. */
fun Channel.effectiveRemoteAddress(): SocketAddress = attr(REAL_REMOTE_ADDRESS).get() ?: remoteAddress()
