package me.hippodev.protocol

import java.util.regex.Pattern

/**
 * Compiles a host pattern ('*' = any sequence, '?' = any single char,
 * both captured for $1/$2/... substitution in backend addresses) into a regex.
 */
class HostPattern(val raw: String) {
    val wildcardCount: Int
    private val regex: Pattern

    init {
        val sb = StringBuilder("^")
        var count = 0
        for (c in raw) {
            when (c) {
                '*' -> {
                    sb.append("(.*)")
                    count++
                }
                '?' -> {
                    sb.append("(.)")
                    count++
                }
                else -> sb.append(Pattern.quote(c.toString()))
            }
        }
        sb.append("$")
        wildcardCount = count
        regex = Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE)
    }

    /** Returns the captured wildcard values if [hostname] matches, or null otherwise. */
    fun match(hostname: String): List<String>? {
        val m = regex.matcher(hostname)
        if (!m.matches()) return null
        return (1..m.groupCount()).map { m.group(it) ?: "" }
    }
}

private val paramPattern = Pattern.compile("\\$(\\d+)")

/** Substitutes $1, $2, ... in [template] with values from [captures]. Out-of-range refs are left as-is. */
fun substituteParams(template: String, captures: List<String>): String {
    if (captures.isEmpty()) return template
    val m = paramPattern.matcher(template)
    val sb = StringBuilder()
    while (m.find()) {
        val idx = m.group(1).toInt()
        val replacement = if (idx in 1..captures.size) captures[idx - 1] else m.group()
        m.appendReplacement(sb, Regex.escapeReplacement(replacement))
    }
    m.appendTail(sb)
    return sb.toString()
}
