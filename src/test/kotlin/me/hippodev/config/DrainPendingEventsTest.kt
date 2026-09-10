package me.hippodev.config

import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds

/** [drainPendingEvents] is what stops a single editor save (which emits a burst of filesystem
 *  events) from triggering more than one config/messages reload: after the debounce sleep it must
 *  consume everything the watch service has queued so the next `take()` blocks again instead of
 *  returning immediately with the leftovers. */
class DrainPendingEventsTest {

    @Test
    fun `drains every queued watch event so a following poll returns nothing`() {
        val dir = Files.createTempDirectory("mcgate-watch-drain")
        val ws = FileSystems.getDefault().newWatchService()
        dir.register(ws, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)

        // Simulate the event burst a save produces.
        repeat(6) { i ->
            val f = dir.resolve("f$i.txt")
            Files.writeString(f, "one")
            Files.writeString(f, "two")
        }

        // Wait until the watch service has actually queued something (inotify is sub-second, the
        // macOS polling fallback can take up to ~10s).
        val deadline = System.currentTimeMillis() + 20_000
        var key = ws.poll()
        while (key == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50)
            key = ws.poll()
        }
        // Put the one we grabbed back into circulation, then drain everything.
        key?.pollEvents()
        key?.reset()

        drainPendingEvents(ws)
        assertNull(ws.poll(), "drain must consume all currently-queued watch events")

        // Give any last straggler event from the writes above time to land, drain once more, and
        // confirm it's still quiet - i.e. the burst really is fully absorbed.
        Thread.sleep(300)
        drainPendingEvents(ws)
        assertNull(ws.poll())

        ws.close()
    }
}
