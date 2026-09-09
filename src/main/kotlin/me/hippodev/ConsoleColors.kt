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

// ANSI SGR codes, built from a "" literal so this source file stays free of raw escape
// bytes. Only the basic 8/16-colour set plus bold/dim - universally supported by real terminals
// and by the log viewers hosting panels use; nothing here relies on 256-colour or truecolour.
private const val ESC = ""
private const val RESET = "$ESC[0m"
private const val BOLD = "$ESC[1m"
private const val DIM = "$ESC[2m"
private const val GRAY = "$ESC[90m"
private const val WHITE = "$ESC[37m"
private const val BRIGHT_RED = "$ESC[91m"
private const val BRIGHT_YELLOW = "$ESC[93m"
private const val GREEN = "$ESC[32m"
private const val CYAN = "$ESC[36m"
private const val BRIGHT_CYAN = "$ESC[96m"
private const val MAGENTA = "$ESC[35m"

// slf4j-simple / logback (pattern "HH:mm:ss [LEVEL] msg") formats each line as
// "HH:mm:ss [LEVEL] message" - captured here to reformat as "[HH:mm:ss] [LEVEL] message",
// colour the level tag, and lightly highlight the message.
private val LINE_PATTERN = Regex("""^(\d{2}:\d{2}:\d{2}) \[(\w+)] (.*)$""", RegexOption.DOT_MATCHES_ALL)

/** Per-level colouring of the `[LEVEL]` tag. WARN/ERROR are bold + bright so they jump out of a
 *  wall of INFO; DEBUG/TRACE recede. */
private val LEVEL_COLOR = mapOf(
    "ERROR" to "$BOLD$BRIGHT_RED",
    "WARN" to "$BOLD$BRIGHT_YELLOW",
    "INFO" to GREEN,
    "DEBUG" to CYAN,
    "TRACE" to DIM,
)

/** In-message highlighters, applied in order to the (level-stripped) message. Each wraps its
 *  match in a colour then returns to [WHITE] - the message's base colour - rather than a full
 *  [RESET], so text after the match keeps rendering white. The patterns are anchored to shapes
 *  that cannot occur inside an ANSI escape (no `[`, digits only in specific structures), so
 *  running them one after another never re-matches and corrupts a code an earlier pass inserted. */
private val HIGHLIGHTS: List<Pair<Regex, (String) -> String>> = listOf(
    // Player UUIDs - dim; they're noise unless you're specifically grepping for one.
    Regex("""\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b""")
        to { m -> "$DIM$m$WHITE" },
    // IPv4, optionally with Netty's leading slash and a :port.
    Regex("""/?\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b""")
        to { m -> "$BRIGHT_CYAN$m$WHITE" },
    // 'single-quoted' spans: hostnames, player names, command arguments.
    Regex("""'[^']*'""")
        to { m -> "$BRIGHT_YELLOW$m$WHITE" },
    // Relay / routing arrows.
    Regex(""" -> """)
        to { _ -> " $DIM->$WHITE " },
    // Quantities: "67137 ms", "1460 bytes", "5 route(s)", "12 player(s)".
    Regex("""\b\d[\d,]*\s?(?:ms|bytes?|KB|MB|GB|packets?|route\(s\)|player\(s\)|connection\(s\))\b""")
        to { m -> "$MAGENTA$m$WHITE" },
)

/** Whether to emit ANSI colour. Order of precedence:
 *   1. `NO_COLOR` set (any value)         -> never  (https://no-color.org)
 *   2. `FORCE_COLOR` / `CLICOLOR_FORCE` truthy -> always  (for launchers - IDEs, some panels,
 *      `mvn exec:java` - that give the JVM a real interactive terminal but no `java.io.Console`)
 *   3. a `java.io.Console` is attached    -> yes   (plain `java -jar` in a terminal, `docker -t`)
 *   4. `TERM` set and not "dumb"          -> yes   (interactive but stdio is a pipe)
 *   5. otherwise                          -> no    (systemd/journald, cron, redirected to a file)
 *
 * The async-decoupling and JLine routing in [installColorConsole] apply either way - this only
 * gates the escape codes. */
private fun detectColorSupport(): Boolean {
    if (System.getenv("NO_COLOR") != null) return false
    if (isTruthy(System.getenv("FORCE_COLOR")) || isTruthy(System.getenv("CLICOLOR_FORCE"))) return true
    if (System.console() != null) return true
    val term = System.getenv("TERM")
    return term != null && term != "dumb"
}

private fun isTruthy(v: String?): Boolean = v != null && v.isNotEmpty() && v != "0" && !v.equals("false", ignoreCase = true)

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
    val useColor = detectColorSupport()

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

/** Reformats and colours one already-rendered log line. Pure and allocation-light (one regex
 *  match + a handful of small replacements); never throws - an unrecognised line is passed
 *  through with a single plain-white wrap. */
internal fun colorize(x: String): String {
    val match = LINE_PATTERN.matchEntire(x)
    if (match == null) {
        // Stack-trace continuation lines etc. - no level to key off of, just plain white.
        return "$WHITE$x$RESET"
    }
    val (time, level, message) = match.destructured
    val levelColor = LEVEL_COLOR[level] ?: WHITE
    // Fixed-width level tag so messages line up in a column regardless of "INFO" vs "WARNING".
    val levelTag = level.padEnd(5).take(5)

    var body = message
    for ((pattern, render) in HIGHLIGHTS) {
        body = pattern.replace(body) { render(it.value) }
    }

    return "$GRAY[$time]$RESET $levelColor$levelTag$RESET $WHITE$body$RESET"
}
