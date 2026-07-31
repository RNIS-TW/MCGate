package me.hippodev

import ch.qos.logback.core.rolling.TriggeringPolicyBase
import java.io.File
import java.util.concurrent.atomic.AtomicLong

private const val MAX_LINES_PER_FILE = 10_000L

/** Rolls log/latest.log every [MAX_LINES_PER_FILE] lines rather than logback's usual size/time
 *  triggers - paired with a `FixedWindowRollingPolicy` in logback.xml, this shifts the current
 *  file to log/latest.1.log.gz (bumping any existing latest.1.log.gz to latest.2.log.gz, and so
 *  on) and starts a fresh latest.log, so one long-lived run doesn't grow a single endless log
 *  file. Referenced from logback.xml by fully-qualified class name, so logback instantiates it
 *  directly - not called from Kotlin code. */
class LineCountTriggeringPolicy<E> : TriggeringPolicyBase<E>() {
    private val lineCount = AtomicLong(0)

    override fun isTriggeringEvent(activeFile: File, event: E): Boolean {
        if (lineCount.incrementAndGet() >= MAX_LINES_PER_FILE) {
            lineCount.set(0)
            return true
        }
        return false
    }
}
