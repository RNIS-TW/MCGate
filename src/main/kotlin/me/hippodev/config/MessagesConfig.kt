package me.hippodev.config

import org.yaml.snakeyaml.Yaml
import java.io.File

data class ReconnectMessages(
    val title: String = "&eServer is currently offline.",
    val subtitle: String = "&7Waiting to reconnect...",
    val attemptSuffix: String = " (attempt {attempt})",
    val actionBarFrames: List<String> = listOf("&7Reconnecting.", "&7Reconnecting..", "&7Reconnecting...")
)

/** Default text for all player-facing messages, loaded from messages.yml. Routes may still
 *  override any of these per-route via config.yml's `reconnect:` block - see [ReconnectConfig].
 *  `kickMessage` is its own top-level setting (not part of `reconnect:`) since it's sent for any
 *  offline-backend kick, whether or not reconnect-holding is enabled for that route - see
 *  [me.hippodev.config.Route.kickMessage]. */
data class GateMessages(
    val kickMessage: String = "&cServer is offline. Please reconnect shortly.",
    /** Shown when a route's `metrics:` upload/download byte limit has been reached - both to
     *  players kicked off the route and to anyone trying to log in while it's over the cap. Global
     *  (not per-route) since it's about the gate's own accounting, not a backend being down.
     *
     *  Full MiniMessage support incl. gradients (the login-refusal path sends a structured chat
     *  component; the mid-session kick path samples the gradient into per-character `§x` hex, which
     *  every 1.16+ client renders identically), and `\n` / a YAML block scalar for multiple lines. */
    val metricsLimitKickMessage: String =
        "<gradient:#f85032:#e73827><bold>✦ DATA LIMIT REACHED ✦</bold></gradient>\n" +
            "<gray>This server has used up its data-transfer allowance.</gray>\n" +
            "<gradient:#8e9eab:#eef2f3>Please try again later.</gradient>",
    val reconnect: ReconnectMessages = ReconnectMessages()
) {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun load(path: String): GateMessages {
            val file = File(path)
            if (!file.exists()) {
                error("Messages file not found: $path")
            }
            val yaml = Yaml()
            val root = file.inputStream().use { yaml.load<Map<String, Any>>(it) } ?: emptyMap()
            val messagesSection = (root["messages"] as? Map<String, Any>) ?: root

            val defaults = GateMessages()
            return GateMessages(
                kickMessage = messagesSection["kickMessage"] as? String ?: defaults.kickMessage,
                metricsLimitKickMessage = messagesSection["metricsLimitKickMessage"] as? String ?: defaults.metricsLimitKickMessage,
                reconnect = parseReconnect(messagesSection["reconnect"] as? Map<String, Any>)
            )
        }

        private fun parseReconnect(r: Map<String, Any>?): ReconnectMessages {
            val defaults = ReconnectMessages()
            if (r == null) return defaults
            val frames = (r["actionBarFrames"] as? List<*>)?.map { it.toString() }
            return ReconnectMessages(
                title = r["title"] as? String ?: defaults.title,
                subtitle = r["subtitle"] as? String ?: defaults.subtitle,
                attemptSuffix = r["attemptSuffix"] as? String ?: defaults.attemptSuffix,
                actionBarFrames = frames ?: defaults.actionBarFrames
            )
        }
    }
}
