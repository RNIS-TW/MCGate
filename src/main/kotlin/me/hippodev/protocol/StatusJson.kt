package me.hippodev.protocol

import me.hippodev.config.FallbackStatus

fun buildFallbackJson(fallback: FallbackStatus): String {
    val sb = StringBuilder()
    sb.append('{')
    sb.append("\"version\":{")
    sb.append("\"name\":\"").append(escapeJson(fallback.version.name)).append("\",")
    sb.append("\"protocol\":").append(fallback.version.protocol)
    sb.append("},")
    sb.append("\"description\":{\"text\":\"").append(escapeJson(fallback.motd)).append("\"}")
    fallback.players?.let {
        sb.append(",\"players\":{\"online\":").append(it.online)
            .append(",\"max\":").append(it.max).append('}')
    }
    fallback.faviconDataUri?.let {
        sb.append(",\"favicon\":\"").append(escapeJson(it)).append('"')
    }
    sb.append('}')
    return sb.toString()
}

private fun escapeJson(value: String): String {
    val sb = StringBuilder(value.length)
    for (c in value) {
        when (c) {
            '"' -> sb.append("\\\"")
            '\\' -> sb.append("\\\\")
            '\n' -> sb.append("\\n")
            '\r' -> sb.append("\\r")
            '\t' -> sb.append("\\t")
            else -> if (c.code < 0x20) sb.append("\\u%04x".format(c.code)) else sb.append(c)
        }
    }
    return sb.toString()
}
