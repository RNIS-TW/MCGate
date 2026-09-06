package me.hippodev.handler

import io.netty.channel.Channel

/**
 * The one piece of flow control every raw byte-pipe in MCGate needs.
 *
 * MCGate relays are a splice: bytes read off one channel are immediately `writeAndFlush`ed to the
 * other. `writeAndFlush` never blocks - if the far side (a player on bad wifi, a backend briefly
 * stalled) can't drain as fast as this side produces, Netty just keeps appending the unsent
 * buffers to that channel's `ChannelOutboundBuffer`. Nothing here ever frees them: they're live,
 * still-referenced, still "waiting to be sent" - so GC can't touch them, and with hundreds of
 * long-lived connections the queued (largely off-heap, pooled-direct) bytes grow until the
 * process is sitting on its whole memory limit for no visible reason (CPU stays low the whole
 * time - it's not busy, it's just holding).
 *
 * Netty already raises a per-channel signal for exactly this - `Channel.isWritable` flips false
 * once the outbound buffer passes the high water mark (see [io.netty.channel.WriteBufferWaterMark],
 * 32 KiB / 64 KiB by default) and back to true once it drains below the low mark, firing
 * `channelWritabilityChanged` each time. Nothing in the relay path was listening to it.
 *
 * [pauseOrResumeReads] is that listener: whenever *this* channel (the one we're writing relayed
 * data *into*) can't take more, stop reading from [source] (the channel those bytes come from) by
 * turning off its autoRead; resume when this channel drains. TCP's own receive-window backpressure
 * then propagates the stall back to whoever is on the far end of [source], instead of MCGate
 * absorbing it all in heap.
 *
 * Both channels of a relayed pair live on the same event loop (every backend [io.netty.bootstrap.Bootstrap]
 * in this codebase is built with `.group(clientChannel.eventLoop())`), so toggling the peer's
 * autoRead from a writability callback is a plain same-thread field write - no synchronization.
 */
fun Channel.pauseOrResumeReads(source: Channel?) {
    source ?: return
    // isAutoRead is also driven briefly during login handoff (LoginRelayHandler flips the client
    // on once a backend is up); by the time either side can be non-writable the splice is fully
    // established and this is the only writer, so there's no fight over the flag.
    if (source.config().isAutoRead != this.isWritable) {
        source.config().isAutoRead = this.isWritable
    }
}
