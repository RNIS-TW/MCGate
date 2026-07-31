package me.hippodev

import org.jline.reader.LineReader
import java.io.PrintStream
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

/** Cap on queued-but-not-yet-written console lines. Bounded deliberately: the writer thread
 *  (below) can only drain as fast as the terminal/log collector on the other end accepts bytes,
 *  and an unbounded queue here means a slow/stalled console (laggy SSH session, a hosting panel
 *  scraping stdout slowly, a stuck Docker log driver) grows this queue forever - an actual
 *  unbounded memory leak that scales with how much the server logs, never shrinks, and eventually
 *  OOMs. Every line is still written in full to log/latest.log via the FILE appender regardless
 *  of what happens here, so dropping console lines under backpressure loses nothing durable. */
private const val MAX_QUEUED_LINES = 10_000

/** Set by [me.hippodev.startConsole] once JLine has taken over the terminal for line editing.
 *  Log lines printed while the user is mid-command must go through [LineReader.printAbove]
 *  instead of a raw println - printAbove clears the in-progress prompt/input line, writes the
 *  message, then redraws the prompt below it. A raw println from another thread instead writes
 *  straight into the middle of whatever JLine has on screen, corrupting the prompt (the `>` and
 *  the command being typed end up split across lines in the wrong order). */
@Volatile
var activeLineReader: LineReader? = null

private const val RESET = "[0m"
private const val GRAY = "[90m"
private const val WHITE = "[37m"
private const val RED = "[31m"
private const val YELLOW = "[33m"
private const val GREEN = "[32m"
private const val CYAN = "[36m"

// slf4j-simple (org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss, levelInBrackets=true) formats
// each line as "HH:mm:ss [LEVEL] message" - captured here to reformat as "[HH:mm:ss] [LEVEL]
// message" and color just the level tag.
private val LINE_PATTERN = Regex("""^(\d{2}:\d{2}:\d{2}) (\[\w+]) (.*)$""", RegexOption.DOT_MATCHES_ALL)

/** Colors console log lines by level and, more importantly, moves the actual console write off
 *  whatever thread is logging. slf4j-simple writes synchronously - a log call blocks the calling
 *  thread until the write (and flush) syscall returns. Most `log.info(...)` calls happen directly
 *  on a Netty event-loop thread (connect/login/disconnect logging), and if the terminal/SSH
 *  session/log collector is at all slow to drain, that blocks the event loop - which, for a
 *  player disconnecting, delays the TCP close ACK their client is waiting on and shows up as a
 *  multi-second stall on the "Disconnecting..." screen. Queuing the formatted line and writing it
 *  from a single dedicated thread decouples every log call from actual I/O latency.
 *
 *  Must be called before the first [org.slf4j.Logger] is created: slf4j-simple resolves and
 *  caches its target stream once, the first time it's touched. Coloring itself still respects the
 *  NO_COLOR convention (https://no-color.org) and is skipped when output isn't a real terminal,
 *  so redirecting logs to a file doesn't fill it with escape codes - the async decoupling applies
 *  either way, since a slow file/collector can block just as easily as a slow terminal. */
fun installColorConsole() {
    val original = System.err
    val useColor = System.getenv("NO_COLOR") == null && System.console() != null

    val queue = LinkedBlockingQueue<String>(MAX_QUEUED_LINES)
    val droppedSinceLastNotice = AtomicLong(0)
    val writer = Thread({
        while (true) {
            val line = try {
                queue.take()
            } catch (e: InterruptedException) {
                return@Thread
            }
            val reader = activeLineReader
            if (reader != null) reader.printAbove(line) else original.println(line)

            val dropped = droppedSinceLastNotice.getAndSet(0)
            if (dropped > 0) {
                val notice = "... $dropped console line(s) dropped (console was falling behind)"
                if (reader != null) reader.printAbove(notice) else original.println(notice)
            }
        }
    }, "console-writer")
    writer.isDaemon = true
    writer.start()

    System.setErr(object : PrintStream(original, true) {
        override fun println(x: String?) {
            val text = x ?: ""
            // offer(), not put(): if the writer thread can't keep up (slow terminal/SSH
            // session/log collector), drop the line instead of growing the queue without bound -
            // see MAX_QUEUED_LINES. Every line is still durably written to log/latest.log by the
            // FILE appender independently of this console path.
            if (!queue.offer(if (useColor) colorize(text) else text)) {
                droppedSinceLastNotice.incrementAndGet()
            }
        }
    })
}

private fun colorize(x: String): String {
    val match = LINE_PATTERN.matchEntire(x)
    if (match == null) {
        // Stack trace continuation lines etc. - no level to key off of, just plain white.
        return "$WHITE$x$RESET"
    }
    val (time, level, message) = match.destructured
    val levelColor = when {
        level.contains("ERROR") -> RED
        level.contains("WARN") -> YELLOW
        level.contains("INFO") -> GREEN
        level.contains("DEBUG") || level.contains("TRACE") -> CYAN
        else -> WHITE
    }
    return "$GRAY[$time]$RESET $levelColor$level$RESET $WHITE$message$RESET"
}
