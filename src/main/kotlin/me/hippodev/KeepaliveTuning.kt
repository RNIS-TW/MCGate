package me.hippodev

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.nio.NioChannelOption
import jdk.net.ExtendedSocketOptions
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("KeepaliveTuning")

/**
 * Extended TCP keepalive tuning (JDK 11+, Linux/macOS) - shortens how quickly the OS starts (and
 * how often it repeats) TCP-level keepalive probes on an otherwise-quiet connection, from the OS
 * default of hours down to seconds. Plain SO_KEEPALIVE alone doesn't help on this timescale: it
 * turns probing on, but leaves the timing at the OS default (Linux: 2 hours before the first
 * probe), which never fires within the lifetime of a connection that's already failing within a
 * couple of minutes - so it can only help a middlebox/NAT-mapping-eviction scenario, not one
 * where the connection is dying from actual packet loss faster than that.
 *
 * Not every platform/JDK exposes these options (only added in JDK 11, and support varies by OS),
 * so failures here are swallowed and logged at DEBUG rather than failing startup - degrading
 * silently to whatever plain SO_KEEPALIVE the caller already configured.
 */
fun ServerBootstrap.applyTunedKeepalive(): ServerBootstrap = try {
    this
        .childOption(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPIDLE), 15)
        .childOption(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPINTERVAL), 5)
        .childOption(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPCOUNT), 3)
} catch (e: Throwable) {
    log.debug("Extended TCP keepalive tuning unavailable on this platform: {}", e.toString())
    this
}

fun Bootstrap.applyTunedKeepalive(): Bootstrap = try {
    this
        .option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPIDLE), 15)
        .option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPINTERVAL), 5)
        .option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPCOUNT), 3)
} catch (e: Throwable) {
    log.debug("Extended TCP keepalive tuning unavailable on this platform: {}", e.toString())
    this
}
