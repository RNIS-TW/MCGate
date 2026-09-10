package me.hippodev.config

import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchKey

object MessagesLoader {
    private val log = LoggerFactory.getLogger(MessagesLoader::class.java)

    /** Loads the messages file at [path], first bootstrapping it from the bundled
     *  default-messages.yml resource if missing. */
    fun loadOrCreateDefault(path: String): GateMessages {
        val file = File(path)
        if (!file.exists()) {
            file.absoluteFile.parentFile?.mkdirs()
            val resource = MessagesLoader::class.java.classLoader.getResourceAsStream("default-messages.yml")
                ?: error("Bundled default-messages.yml resource is missing from the jar")
            resource.use { input -> file.outputStream().use { output -> input.copyTo(output) } }
            log.info("No messages file found at {}, created one from the bundled default", file.absolutePath)
        }
        return GateMessages.load(path)
    }

    /** Watches [path] for changes and calls [onReload] with the freshly parsed messages after each save. */
    fun watch(path: String, onReload: (GateMessages) -> Unit) {
        val file = File(path).absoluteFile
        val dir = file.parentFile.toPath()

        val watchService = FileSystems.getDefault().newWatchService()
        dir.register(
            watchService,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_CREATE
        )

        val thread = Thread({ watchLoop(watchService, file, onReload) }, "messages-watcher")
        thread.isDaemon = true
        thread.start()
    }

    private fun watchLoop(watchService: java.nio.file.WatchService, file: File, onReload: (GateMessages) -> Unit) {
        while (true) {
            val key: WatchKey = try {
                watchService.take()
            } catch (e: InterruptedException) {
                return
            }

            var changed = false
            for (event in key.pollEvents()) {
                val context = event.context() as? Path ?: continue
                if (context.toString() == file.name) changed = true
            }
            key.reset()

            if (changed) {
                // Debounce, then drain the follow-up events a single save emits so they don't
                // trigger a second reload - see drainPendingEvents (ConfigLoader.kt).
                Thread.sleep(200)
                drainPendingEvents(watchService)
                try {
                    onReload(GateMessages.load(file.path))
                    log.info("Reloaded messages from {}", file.path)
                } catch (e: Exception) {
                    log.warn("Failed to reload messages from {}: {}", file.path, e.message)
                }
            }
        }
    }
}
