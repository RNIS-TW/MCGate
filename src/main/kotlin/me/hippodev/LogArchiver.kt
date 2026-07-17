package me.hippodev

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.GZIPOutputStream

/** Archives the previous run's log/latest.log (if any) as a timestamped .log.gz before logback
 *  opens a fresh log/latest.log for this run. Must run before the first [org.slf4j.Logger] is
 *  created, same as [installColorConsole] - logback's FileAppender opens (and locks) latest.log
 *  the moment the logging framework initializes, so archiving has to happen first or the rename
 *  would race an already-open file handle. */
fun archivePreviousLog(logDir: File = File("log")) {
    val latest = File(logDir, "latest.log")
    if (!latest.exists() || latest.length() == 0L) return

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
