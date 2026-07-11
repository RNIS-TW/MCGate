package me.hippodev.config

import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchKey

object ConfigLoader {
    private val log = LoggerFactory.getLogger(ConfigLoader::class.java)

    /** Loads the config at [path], first bootstrapping it from the bundled default-config.yml resource if missing.
     *  [messages] supplies the defaults for player-facing text (see [GateMessages]) that routes don't override. */
    fun loadOrCreateDefault(path: String, messages: GateMessages): GateConfig {
        val file = File(path)
        if (!file.exists()) {
            file.absoluteFile.parentFile?.mkdirs()
            val resource = ConfigLoader::class.java.classLoader.getResourceAsStream("default-config.yml")
                ?: error("Bundled default-config.yml resource is missing from the jar")
            resource.use { input -> file.outputStream().use { output -> input.copyTo(output) } }
            log.info("No config file found at {}, created one from the bundled default", file.absolutePath)
        }
        return GateConfig.load(path, messages)
    }

    /** Watches [path] for changes and calls [onReload] with the freshly parsed config after each save.
     *  [messagesSupplier] is re-read on every reload so a messages.yml change picked up in between
     *  still applies the next time config.yml is reloaded. */
    fun watch(path: String, messagesSupplier: () -> GateMessages, onReload: (GateConfig) -> Unit) {
        val file = File(path).absoluteFile
        val dir = file.parentFile.toPath()

        val watchService = FileSystems.getDefault().newWatchService()
        dir.register(
            watchService,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_CREATE
        )

        val thread = Thread({ watchLoop(watchService, file, messagesSupplier, onReload) }, "config-watcher")
        thread.isDaemon = true
        thread.start()
    }

    private fun watchLoop(
        watchService: java.nio.file.WatchService,
        file: File,
        messagesSupplier: () -> GateMessages,
        onReload: (GateConfig) -> Unit
    ) {
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
                // Debounce: editors often emit multiple events for a single save.
                Thread.sleep(200)
                try {
                    onReload(GateConfig.load(file.path, messagesSupplier()))
                    log.info("Reloaded config from {}", file.path)
                } catch (e: Exception) {
                    log.warn("Failed to reload config from {}: {}", file.path, e.message)
                }
            }
        }
    }
}
