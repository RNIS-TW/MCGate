package me.hippodev

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.LifeCycle

/**
 * logback console appender that hands each formatted line to `System.err.println(String)` rather
 * than writing raw bytes to a stream. That re-enters the wrapper [installColorConsole] installs on
 * `System.err`, which is where all three of these live:
 *
 *  - level + structure colouring ([colorize]);
 *  - the bounded, drop-on-backpressure async queue that keeps a slow console (laggy SSH, a hosting
 *    panel scraping stdout, a stuck Docker log driver) off the Netty event-loop threads that do
 *    most of the logging - see `MAX_QUEUED_LINES`;
 *  - routing through JLine's `printAbove`, so a line logged while the operator is mid-command
 *    doesn't shred the prompt.
 *
 * logback's stock `ConsoleAppender` writes via `OutputStream.write(byte[])`, which the wrapper
 * cannot intercept (it only overrides `println`), so with it none of the above applied to
 * application log lines - only to stray `System.err.println` / `printStackTrace` output.
 *
 * The FILE appender is untouched and still writes plain, un-coloured text straight to
 * `log/latest.log`.
 */
class ColorConsoleAppender : AppenderBase<ILoggingEvent>() {

    /** Set from `<encoder>` in logback.xml (defaults to a `PatternLayoutEncoder`). */
    var encoder: Encoder<ILoggingEvent>? = null

    override fun start() {
        val enc = encoder
        if (enc == null) {
            addError("No <encoder> configured for ColorConsoleAppender [$name]")
            return
        }
        // Joran normally injects context and starts nested LifeCycle components, but be defensive
        // - encode() on an unstarted PatternLayoutEncoder throws.
        if (enc is LifeCycle && !enc.isStarted) enc.start()
        super.start()
    }

    override fun append(event: ILoggingEvent) {
        val enc = encoder ?: return
        val text = try {
            String(enc.encode(event), Charsets.UTF_8)
        } catch (e: Exception) {
            addError("Failed to encode a log event", e)
            return
        }
        // One encode() can produce several lines (a message plus an exception's stack trace).
        // Emit them one at a time so each passes through colorize / printAbove individually - a
        // bare stack-frame line simply falls to colorize's plain-white branch.
        var from = 0
        while (from <= text.length) {
            val nl = text.indexOf('\n', from)
            val end = if (nl < 0) text.length else nl
            if (end > from) System.err.println(text.substring(from, end))
            if (nl < 0) break
            from = nl + 1
        }
    }
}
