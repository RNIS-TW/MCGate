package me.hippodev

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.GZIPOutputStream

/** How many gzipped past-run logs to keep in log/ - archives beyond this many (oldest first) are
 *  deleted on startup so a long-lived install (restarted daily/weekly for months) doesn't
 *  accumulate an ever-growing pile of .log.gz files on disk indefinitely. Only counts the
 *  per-run `<timestamp>.log.gz` files this class creates - the `latest.N.log.gz` within-run rolls
 *  are owned and pruned by logback's own FixedWindowRollingPolicy (see logback.xml), and having
 *  both prune by mtime across the same directory just made them fight. */
private const val MAX_ARCHIVED_LOGS = 7

/** Archives the previous run's log/latest.log (if any) as a timestamped .log.gz before logback
 *  opens a fresh log/latest.log for this run, then prunes old archives beyond [MAX_ARCHIVED_LOGS].
 *  Must run before the first [org.slf4j.Logger] is created, same as [installColorConsole] -
 *  logback's FileAppender opens (and locks) latest.log the moment the logging framework
 *  initializes, so archiving has to happen first or the rename would race an already-open file
 *  handle. */
fun archivePreviousLog(logDir: File = File("log")) {
    val latest = File(logDir, "latest.log")
    if (latest.exists() && latest.length() > 0L) {
        val timestamp = SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Date(latest.lastModified()))
        var archive = File(logDir, "$timestamp.log.gz")
        var suffix = 1
        while (archive.exists()) {
            archive = File(logDir, "$timestamp-$suffix.log.gz")
            suffix++
        }

        GZIPOutputStream(archive.outputStream()).use { gzOut ->
            latest.inputStream().use { it.copyTo(gzOut) }
        }
        latest.delete()
    }

    pruneOldArchives(logDir)
}

private fun pruneOldArchives(logDir: File) {
    val archives = logDir.listFiles { f ->
        // Only the per-run archives this class writes - never logback's own latest.N.log.gz rolls.
        f.isFile && f.name.endsWith(".log.gz") && !f.name.startsWith("latest.")
    } ?: return
    archives.sortedByDescending { it.lastModified() }
        .drop(MAX_ARCHIVED_LOGS)
        .forEach { it.delete() }
}
