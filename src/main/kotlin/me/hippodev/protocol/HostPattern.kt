package me.hippodev.protocol

import java.util.regex.Pattern

/**
 * A host pattern: `*` matches any sequence (dots included), `?` matches any single character;
 * both are captured, in order, for `$1`/`$2`/... substitution in backend addresses.
 *
 * Matching runs as a linear-space dynamic-programming pass, deliberately NOT a compiled regex.
 * A regex translation of several `*` wildcards (`*-*-*.example.com`) is a textbook
 * catastrophic-backtracking (ReDoS) vector, and [match] is called on the Netty event-loop
 * thread for every incoming connection - a crafted hostname against such a pattern could
 * otherwise pin that thread at 100% CPU, stalling every player sharing it. The DP pass is
 * O(patternLen * hostLen) with no backtracking, so a hostile input can't blow it up.
 */
class HostPattern(val raw: String) {
    /** Number of `*` / `?` tokens - drives how many `$N` captures the pattern yields. */
    val wildcardCount: Int

    /** [raw] with a trailing DNS root dot trimmed (mirrors HandshakeSniffer's handling of
     *  client-sent hostnames) and lowercased, so literal comparison is case-insensitive. */
    private val pattern: String

    init {
        pattern = raw.trimEnd('.').lowercase()
        wildcardCount = pattern.count { it == '*' || it == '?' }
    }

    /** Captured wildcard values if [hostname] matches, else null. Case-insensitive; the
     *  returned captures preserve the original case of [hostname]. */
    fun match(hostname: String): List<String>? {
        val text = hostname.trimEnd('.')
        // Client hostnames are already capped at 255 by HandshakeSniffer; this is defence in
        // depth for any other caller and keeps the DP table trivially small.
        if (text.length > 255) return null
        val lower = text.lowercase()
        val p = pattern
        val n = p.length
        val m = lower.length

        // dp[i][j] = does the pattern suffix p[i..] match the text suffix lower[j..] ?
        val dp = Array(n + 1) { BooleanArray(m + 1) }
        dp[n][m] = true
        for (i in n - 1 downTo 0) {
            val pc = p[i]
            for (j in m downTo 0) {
                dp[i][j] = when (pc) {
                    '*' -> dp[i + 1][j] || (j < m && dp[i][j + 1])
                    '?' -> j < m && dp[i + 1][j + 1]
                    else -> j < m && lower[j] == pc && dp[i + 1][j + 1]
                }
            }
        }
        if (!dp[0][0]) return null

        // Walk left to right, giving each `*` the longest span the remaining pattern still
        // allows - greedy, matching the old regex `(.*)` semantics for multi-wildcard patterns.
        val captures = ArrayList<String>(wildcardCount)
        var j = 0
        for (i in 0 until n) {
            when (p[i]) {
                '*' -> {
                    var best = j
                    for (k in j..m) if (dp[i + 1][k]) best = k
                    captures.add(text.substring(j, best))
                    j = best
                }
                '?' -> {
                    captures.add(text.substring(j, j + 1))
                    j++
                }
                else -> j++
            }
        }
        return captures
    }

    // Value equality on the normalized pattern, so a structurally-identical Route parsed again
    // on a config reload compares equal - lets RouteRuntime state be carried across reloads
    // (see GateState in Main.kt).
    override fun equals(other: Any?): Boolean = other is HostPattern && other.pattern == pattern
    override fun hashCode(): Int = pattern.hashCode()
    override fun toString(): String = raw
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
