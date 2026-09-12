//! Port of `me.hippodev.protocol.HostPattern`.
//!
//! `*` matches any sequence (dots included), `?` matches any single character; both are
//! captured, in order, for `$1`/`$2`/... substitution in backend addresses.
//!
//! Matching runs as a linear-space dynamic-programming pass, deliberately NOT a compiled
//! regex — a regex translation of several `*` wildcards (`*-*-*.example.com`) is a textbook
//! catastrophic-backtracking (ReDoS) vector, and `match_host` runs on the hot connection-accept
//! path for every incoming client. The DP pass is O(pattern_len * host_len) with no
//! backtracking, so a hostile hostname can't blow it up.

#[derive(Debug, Clone)]
pub struct HostPattern {
    pub raw: String,
    /// Normalized (trailing dot trimmed, lowercased) pattern used for matching/equality.
    pattern: String,
    /// Number of `*` / `?` tokens — drives how many `$N` captures the pattern yields.
    pub wildcard_count: usize,
}

impl HostPattern {
    pub fn new(raw: impl Into<String>) -> Self {
        let raw = raw.into();
        let pattern = raw.trim_end_matches('.').to_lowercase();
        let wildcard_count = pattern.chars().filter(|&c| c == '*' || c == '?').count();
        Self { raw, pattern, wildcard_count }
    }

    /// Captured wildcard values if `hostname` matches, else `None`. Case-insensitive; the
    /// returned captures preserve the original case of `hostname`.
    pub fn match_host(&self, hostname: &str) -> Option<Vec<String>> {
        let text = hostname.trim_end_matches('.');
        // Client hostnames are already capped at 255 by the handshake sniffer; this is
        // defence in depth for any other caller and keeps the DP table trivially small.
        if text.chars().count() > 255 {
            return None;
        }
        let lower: Vec<char> = text.to_lowercase().chars().collect();
        let text_chars: Vec<char> = text.chars().collect();
        let p: Vec<char> = self.pattern.chars().collect();
        let n = p.len();
        let m = lower.len();

        // dp[i][j] = does the pattern suffix p[i..] match the text suffix lower[j..] ?
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[n][m] = true;
        for i in (0..n).rev() {
            let pc = p[i];
            for j in (0..=m).rev() {
                dp[i][j] = match pc {
                    '*' => dp[i + 1][j] || (j < m && dp[i][j + 1]),
                    '?' => j < m && dp[i + 1][j + 1],
                    c => j < m && lower[j] == c && dp[i + 1][j + 1],
                };
            }
        }
        if !dp[0][0] {
            return None;
        }

        // Walk left to right, giving each `*` the longest span the remaining pattern still
        // allows — greedy, matching the old regex `(.*)` semantics for multi-wildcard patterns.
        let mut captures = Vec::with_capacity(self.wildcard_count);
        let mut j = 0usize;
        for i in 0..n {
            match p[i] {
                '*' => {
                    let mut best = j;
                    for k in j..=m {
                        if dp[i + 1][k] {
                            best = k;
                        }
                    }
                    captures.push(text_chars[j..best].iter().collect::<String>());
                    j = best;
                }
                '?' => {
                    captures.push(text_chars[j..j + 1].iter().collect::<String>());
                    j += 1;
                }
                _ => j += 1,
            }
        }
        Some(captures)
    }
}

impl PartialEq for HostPattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}
impl Eq for HostPattern {}

impl std::hash::Hash for HostPattern {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
    }
}

impl std::fmt::Display for HostPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

/// Substitutes $1, $2, ... in `template` with values from `captures`. Out-of-range refs are
/// left as-is.
pub fn substitute_params(template: &str, captures: &[String]) -> String {
    if captures.is_empty() {
        return template.to_string();
    }
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                let digits: String = chars[i + 1..j].iter().collect();
                let idx: usize = digits.parse().unwrap();
                if idx >= 1 && idx <= captures.len() {
                    out.push_str(&captures[idx - 1]);
                } else {
                    out.extend(&chars[i..j]);
                }
                i = j;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_match() {
        let p = HostPattern::new("play.example.com");
        assert_eq!(p.match_host("play.example.com"), Some(vec![]));
        assert_eq!(p.match_host("other.example.com"), None);
    }

    #[test]
    fn case_insensitive_and_trailing_dot() {
        let p = HostPattern::new("Play.Example.com");
        assert_eq!(p.match_host("play.example.com."), Some(vec![]));
    }

    #[test]
    fn single_wildcard_captures() {
        let p = HostPattern::new("*.example.com");
        assert_eq!(p.match_host("foo.example.com"), Some(vec!["foo".to_string()]));
        assert_eq!(p.match_host("foo.bar.example.com"), Some(vec!["foo.bar".to_string()]));
    }

    #[test]
    fn question_mark_captures_one_char() {
        let p = HostPattern::new("s?.example.com");
        assert_eq!(p.match_host("s1.example.com"), Some(vec!["1".to_string()]));
        assert_eq!(p.match_host("s12.example.com"), None);
    }

    #[test]
    fn multi_wildcard_greedy() {
        let p = HostPattern::new("*-*-*.example.com");
        assert_eq!(
            p.match_host("a-b-c-d.example.com"),
            Some(vec!["a-b".into(), "c".into(), "d".into()])
        );
    }

    #[test]
    fn substitute_basic() {
        assert_eq!(substitute_params("srv$1.internal:$2", &["a".into(), "25566".into()]), "srva.internal:25566");
    }

    #[test]
    fn substitute_out_of_range_left_as_is() {
        assert_eq!(substitute_params("srv$5", &["a".into()]), "srv$5");
    }

    #[test]
    fn substitute_no_captures_returns_template() {
        assert_eq!(substitute_params("srv$1", &[]), "srv$1");
    }
}
