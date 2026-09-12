//! Port of `protocol/TextFormat.kt`.
//!
//! The Kotlin version delegates all of this to Adventure (`MiniMessage`, `GsonComponentSerializer`,
//! `LegacyComponentSerializer`) — a full-featured rich-text library with hover/click events,
//! fonts, translatable components, and more. Pulling in an equivalent crate (or reimplementing
//! all of it) isn't worth it: grepping this project's actual config/messages usage shows only
//! plain text, named/hex colors, decorations (bold/italic/etc.), and gradients — e.g.
//! `default-messages.yml`'s `metricsLimitKickMessage` uses `<gradient:..:..>` wrapping `<bold>`.
//! This is a from-scratch parser scoped to exactly that subset, documented as such rather than
//! silently dropping features a config author might reasonably expect from "MiniMessage support."
//! Not supported: hover/click events, fonts, translatable/keybind components, insertion, nested
//! `<lang:>`/`<selector:>`/etc. — anything beyond color/decoration/gradient on plain text.

use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Rgb(u8, u8, u8);

const NAMED_COLORS: &[(&str, char, Rgb)] = &[
    ("black", '0', Rgb(0, 0, 0)),
    ("dark_blue", '1', Rgb(0, 0, 170)),
    ("dark_green", '2', Rgb(0, 170, 0)),
    ("dark_aqua", '3', Rgb(0, 170, 170)),
    ("dark_red", '4', Rgb(170, 0, 0)),
    ("dark_purple", '5', Rgb(170, 0, 170)),
    ("gold", '6', Rgb(255, 170, 0)),
    ("gray", '7', Rgb(170, 170, 170)),
    ("dark_gray", '8', Rgb(85, 85, 85)),
    ("blue", '9', Rgb(85, 85, 255)),
    ("green", 'a', Rgb(85, 255, 85)),
    ("aqua", 'b', Rgb(85, 255, 255)),
    ("red", 'c', Rgb(255, 85, 85)),
    ("light_purple", 'd', Rgb(255, 85, 255)),
    ("yellow", 'e', Rgb(255, 255, 85)),
    ("white", 'f', Rgb(255, 255, 255)),
];

fn named_color(name: &str) -> Option<Rgb> {
    NAMED_COLORS.iter().find(|(n, _, _)| *n == name).map(|(_, _, rgb)| *rgb)
}

fn legacy_char_for_rgb(rgb: Rgb) -> Option<char> {
    NAMED_COLORS.iter().find(|(_, _, v)| *v == rgb).map(|(_, c, _)| *c)
}

fn json_name_for_rgb(rgb: Rgb) -> Option<&'static str> {
    NAMED_COLORS.iter().find(|(_, _, v)| *v == rgb).map(|(n, _, _)| *n)
}

fn parse_hex(s: &str) -> Option<Rgb> {
    let s = s.strip_prefix('#')?;
    if s.len() != 6 {
        return None;
    }
    let r = u8::from_str_radix(&s[0..2], 16).ok()?;
    let g = u8::from_str_radix(&s[2..4], 16).ok()?;
    let b = u8::from_str_radix(&s[4..6], 16).ok()?;
    Some(Rgb(r, g, b))
}

fn color_from_str(s: &str) -> Option<Rgb> {
    if s.starts_with('#') {
        parse_hex(s)
    } else {
        named_color(s)
    }
}

/// Rewrites `&`-prefixed legacy color codes (e.g. `&e`) into their MiniMessage tag equivalent
/// (`<yellow>`), so legacy-coded config values keep working exactly as before while genuine
/// MiniMessage tags elsewhere in the same string are parsed on top.
fn legacy_codes_to_tags(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '&' && i + 1 < chars.len() {
            if let Some(tag) = legacy_tag_for(chars[i + 1].to_ascii_lowercase()) {
                out.push('<');
                out.push_str(tag);
                out.push('>');
                i += 2;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

fn legacy_tag_for(c: char) -> Option<&'static str> {
    match c {
        '0' => Some("black"),
        '1' => Some("dark_blue"),
        '2' => Some("dark_green"),
        '3' => Some("dark_aqua"),
        '4' => Some("dark_red"),
        '5' => Some("dark_purple"),
        '6' => Some("gold"),
        '7' => Some("gray"),
        '8' => Some("dark_gray"),
        '9' => Some("blue"),
        'a' => Some("green"),
        'b' => Some("aqua"),
        'c' => Some("red"),
        'd' => Some("light_purple"),
        'e' => Some("yellow"),
        'f' => Some("white"),
        'k' => Some("obfuscated"),
        'l' => Some("bold"),
        'm' => Some("strikethrough"),
        'n' => Some("underlined"),
        'o' => Some("italic"),
        'r' => Some("reset"),
        _ => None,
    }
}

#[derive(Debug)]
enum Node {
    Text(String),
    Tag { name: String, args: Vec<String>, children: Vec<Node> },
}

/// Parses `chars[*pos..]` into a node list, stopping (and consuming) at the first closing tag if
/// `nested`, or at end-of-input at the top level. Unknown/mismatched closing tags are consumed
/// leniently rather than erroring — this is a best-effort renderer for config text, not a
/// validating parser; malformed markup should degrade gracefully; never panic or reject startup.
fn parse_nodes(chars: &[char], pos: &mut usize, nested: bool) -> Vec<Node> {
    let mut nodes = Vec::new();
    let mut text = String::new();
    while *pos < chars.len() {
        let c = chars[*pos];
        if c == '\\' && *pos + 1 < chars.len() {
            text.push(chars[*pos + 1]);
            *pos += 2;
            continue;
        }
        if c == '<' {
            if let Some(gt_offset) = chars[*pos..].iter().position(|&c| c == '>') {
                let tag_end = *pos + gt_offset;
                let inner: String = chars[*pos + 1..tag_end].iter().collect();
                if let Some(_close_name) = inner.strip_prefix('/') {
                    if nested {
                        if !text.is_empty() {
                            nodes.push(Node::Text(std::mem::take(&mut text)));
                        }
                        *pos = tag_end + 1;
                        return nodes;
                    }
                    // Unmatched closing tag at top level — pass through the "<...>"" text
                    // literally rather than silently eating it.
                    text.push_str(&format!("<{inner}>"));
                    *pos = tag_end + 1;
                    continue;
                }
                let mut parts = inner.split(':');
                let tag_name = parts.next().unwrap_or("").to_string();
                if tag_name.is_empty() {
                    text.push(c);
                    *pos += 1;
                    continue;
                }
                let args: Vec<String> = parts.map(|s| s.to_string()).collect();
                if !text.is_empty() {
                    nodes.push(Node::Text(std::mem::take(&mut text)));
                }
                *pos = tag_end + 1;
                let children = parse_nodes(chars, pos, true);
                nodes.push(Node::Tag { name: tag_name, args, children });
                continue;
            }
        }
        text.push(c);
        *pos += 1;
    }
    if !text.is_empty() {
        nodes.push(Node::Text(text));
    }
    nodes
}

#[derive(Debug, Clone, Default)]
struct Style {
    color: Option<Rgb>,
    bold: bool,
    italic: bool,
    underlined: bool,
    strikethrough: bool,
    obfuscated: bool,
}

fn apply_tag(style: &Style, name: &str, args: &[String]) -> Style {
    match name.to_lowercase().as_str() {
        "reset" => Style::default(),
        "bold" | "b" => Style { bold: true, ..style.clone() },
        "italic" | "i" | "em" => Style { italic: true, ..style.clone() },
        "underlined" | "u" => Style { underlined: true, ..style.clone() },
        "strikethrough" | "st" => Style { strikethrough: true, ..style.clone() },
        "obfuscated" | "obf" => Style { obfuscated: true, ..style.clone() },
        "color" | "colour" => match args.first().and_then(|a| color_from_str(a)) {
            Some(rgb) => Style { color: Some(rgb), ..style.clone() },
            None => style.clone(),
        },
        other if other.starts_with('#') => match parse_hex(other) {
            Some(rgb) => Style { color: Some(rgb), ..style.clone() },
            None => style.clone(),
        },
        other => match named_color(other) {
            Some(rgb) => Style { color: Some(rgb), ..style.clone() },
            // Unknown tag (hover/click/font/etc., or a typo): fall through with the style
            // unchanged rather than erroring — see parse_nodes's doc on graceful degradation.
            None => style.clone(),
        },
    }
}

struct StyledRun {
    text: String,
    style: Style,
}

fn visible_len(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .map(|n| match n {
            Node::Text(s) => s.chars().count(),
            Node::Tag { children, .. } => visible_len(children),
        })
        .sum()
}

fn interpolate(stops: &[Rgb], t: f64) -> Rgb {
    if stops.len() == 1 {
        return stops[0];
    }
    let t = t.clamp(0.0, 1.0);
    let scaled = t * (stops.len() - 1) as f64;
    let idx = (scaled.floor() as usize).min(stops.len() - 2);
    let frac = scaled - idx as f64;
    let a = stops[idx];
    let b = stops[idx + 1];
    Rgb(
        (a.0 as f64 + (b.0 as f64 - a.0 as f64) * frac).round() as u8,
        (a.1 as f64 + (b.1 as f64 - a.1 as f64) * frac).round() as u8,
        (a.2 as f64 + (b.2 as f64 - a.2 as f64) * frac).round() as u8,
    )
}

fn gradient_stops(args: &[String]) -> Vec<Rgb> {
    let stops: Vec<Rgb> = args.iter().filter_map(|a| color_from_str(a)).collect();
    if stops.is_empty() {
        vec![Rgb(255, 255, 255)]
    } else {
        stops
    }
}

fn flatten(nodes: &[Node], style: &Style, out: &mut Vec<StyledRun>) {
    for node in nodes {
        match node {
            Node::Text(s) => {
                if !s.is_empty() {
                    out.push(StyledRun { text: s.clone(), style: style.clone() });
                }
            }
            Node::Tag { name, args, children } => {
                if name.eq_ignore_ascii_case("gradient") {
                    let stops = gradient_stops(args);
                    let total = visible_len(children).max(1);
                    let mut pos = 0usize;
                    flatten_gradient(children, style, &stops, total, &mut pos, out);
                } else {
                    let new_style = apply_tag(style, name, args);
                    flatten(children, &new_style, out);
                }
            }
        }
    }
}

/// Same as [`flatten`] but assigns each visible character a color linearly interpolated across
/// `stops` by its position (`*pos`) out of `total` — matches Adventure's gradient behavior of
/// coloring per-character across all of a `<gradient>` tag's content, independent of any nested
/// decoration tags (e.g. `<gradient:..:..><bold>text</bold></gradient>` keeps the bold but still
/// gradients the color across `text`'s characters).
fn flatten_gradient(nodes: &[Node], style: &Style, stops: &[Rgb], total: usize, pos: &mut usize, out: &mut Vec<StyledRun>) {
    for node in nodes {
        match node {
            Node::Text(s) => {
                for ch in s.chars() {
                    let t = if total <= 1 { 0.0 } else { *pos as f64 / (total - 1) as f64 };
                    let color = interpolate(stops, t);
                    out.push(StyledRun { text: ch.to_string(), style: Style { color: Some(color), ..style.clone() } });
                    *pos += 1;
                }
            }
            Node::Tag { name, args, children } => {
                if name.eq_ignore_ascii_case("gradient") {
                    // Nested gradient: gets its own independent interpolation scope rather than
                    // sharing the outer gradient's position counter.
                    let inner_stops = gradient_stops(args);
                    let inner_total = visible_len(children).max(1);
                    let mut inner_pos = 0usize;
                    flatten_gradient(children, style, &inner_stops, inner_total, &mut inner_pos, out);
                } else {
                    let new_style = apply_tag(style, name, args);
                    flatten_gradient(children, &new_style, stops, total, pos, out);
                }
            }
        }
    }
}

fn parse_runs(input: &str) -> Vec<StyledRun> {
    let converted = legacy_codes_to_tags(input);
    let chars: Vec<char> = converted.chars().collect();
    let mut pos = 0;
    let nodes = parse_nodes(&chars, &mut pos, false);
    let mut runs = Vec::new();
    flatten(&nodes, &Style::default(), &mut runs);
    runs
}

/// Escapes a string for embedding in a JSON string literal. Shared with `status_json.rs`, which
/// needs identical escaping for the same reason (both ultimately produce JSON text components).
pub(crate) fn escape_json(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

fn run_to_json(run: &StyledRun) -> String {
    let mut out = String::new();
    out.push_str("{\"text\":\"");
    out.push_str(&escape_json(&run.text));
    out.push('"');
    if let Some(rgb) = run.style.color {
        match json_name_for_rgb(rgb) {
            Some(name) => {
                let _ = write!(out, ",\"color\":\"{name}\"");
            }
            None => {
                let _ = write!(out, ",\"color\":\"#{:02x}{:02x}{:02x}\"", rgb.0, rgb.1, rgb.2);
            }
        }
    }
    if run.style.bold {
        out.push_str(",\"bold\":true");
    }
    if run.style.italic {
        out.push_str(",\"italic\":true");
    }
    if run.style.underlined {
        out.push_str(",\"underlined\":true");
    }
    if run.style.strikethrough {
        out.push_str(",\"strikethrough\":true");
    }
    if run.style.obfuscated {
        out.push_str(",\"obfuscated\":true");
    }
    out.push('}');
    out
}

/// Serializes to the Minecraft JSON chat component format used by e.g. the status/MOTD response
/// and the Login Disconnect packet.
pub fn to_json_component(input: &str) -> String {
    let runs = parse_runs(input);
    match runs.len() {
        0 => "{\"text\":\"\"}".to_string(),
        1 => run_to_json(&runs[0]),
        _ => {
            let mut out = String::from("{\"text\":\"\",\"extra\":[");
            for (i, run) in runs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&run_to_json(run));
            }
            out.push_str("]}");
            out
        }
    }
}

fn write_legacy_style(out: &mut String, style: &Style) {
    if let Some(rgb) = style.color {
        match legacy_char_for_rgb(rgb) {
            Some(code) => {
                out.push('\u{00a7}');
                out.push(code);
            }
            // Not one of the 16 legacy colors (a genuine hex value or gradient-interpolated
            // color): the extended §x§R§R§G§G§B§B format every client since 1.16 understands.
            None => {
                out.push('\u{00a7}');
                out.push('x');
                for hex_digit in format!("{:02x}{:02x}{:02x}", rgb.0, rgb.1, rgb.2).chars() {
                    out.push('\u{00a7}');
                    out.push(hex_digit);
                }
            }
        }
    }
    if style.bold {
        out.push('\u{00a7}');
        out.push('l');
    }
    if style.italic {
        out.push('\u{00a7}');
        out.push('o');
    }
    if style.underlined {
        out.push('\u{00a7}');
        out.push('n');
    }
    if style.strikethrough {
        out.push('\u{00a7}');
        out.push('m');
    }
    if style.obfuscated {
        out.push('\u{00a7}');
        out.push('k');
    }
}

/// Serializes to a flat `§`-coded string, with hex colors (including gradients sampled into a run
/// of per-character hex codes) written as the `§x§h§h...` extension every client since 1.16
/// understands. For packets that only carry a plain string rather than a full structured
/// component (Title/Subtitle/ActionBar/PlayDisconnect), which can still show colors and basic
/// formatting, just not hover/click/etc.
///
/// Unlike Adventure's `LegacyComponentSerializer`, this always emits the full style prefix for
/// every run rather than diffing against the previous run's style — the output is longer than
/// Adventure's minimal-diff version but renders identically client-side (redundant `§` codes are
/// harmless), which is the correctness bar this port targets, not byte-for-byte output.
pub fn to_legacy_text(input: &str) -> String {
    let runs = parse_runs(input);
    let mut out = String::new();
    for run in &runs {
        if run.text.is_empty() {
            continue;
        }
        write_legacy_style(&mut out, &run.style);
        out.push_str(&run.text);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(to_legacy_text("hello"), "hello");
    }

    #[test]
    fn legacy_code_translates_to_named_color_short_code() {
        // "&e" -> <yellow> -> resolved to the exact NamedColor RGB -> re-shortened to §e, not
        // an extended hex sequence - matches Adventure's hexColors() behavior of only using the
        // extended format for colors that AREN'T an exact legacy-color match.
        assert_eq!(to_legacy_text("&eHello"), "\u{00a7}eHello");
    }

    #[test]
    fn minimessage_named_color_tag() {
        assert_eq!(to_legacy_text("<red>Hi</red>"), "\u{00a7}cHi");
    }

    #[test]
    fn hex_color_not_matching_named_uses_extended_format() {
        let out = to_legacy_text("<#123456>Hi</#123456>");
        assert_eq!(out, "\u{00a7}x\u{00a7}1\u{00a7}2\u{00a7}3\u{00a7}4\u{00a7}5\u{00a7}6Hi");
    }

    #[test]
    fn bold_decoration() {
        assert_eq!(to_legacy_text("<bold>Hi</bold>"), "\u{00a7}lHi");
    }

    #[test]
    fn gradient_produces_per_character_colors() {
        let out = to_legacy_text("<gradient:#000000:#ffffff>AB</gradient>");
        // Two characters -> first at t=0 (black, a named color -> short code §0), second at t=1
        // (white -> §f).
        assert_eq!(out, "\u{00a7}0A\u{00a7}fB");
    }

    #[test]
    fn gradient_wrapping_bold_matches_default_kick_message_shape() {
        // The exact shape used by default-messages.yml's metricsLimitKickMessage.
        let out = to_legacy_text("<gradient:#f85032:#e73827><bold>hi</bold></gradient>");
        assert!(out.contains("\u{00a7}l")); // bold code present
        assert!(out.starts_with("\u{00a7}x")); // first char gets an extended-hex gradient color
    }

    #[test]
    fn unknown_tag_degrades_gracefully_instead_of_erroring() {
        assert_eq!(to_legacy_text("<hover:show_text:'hi'>text</hover>"), "text");
    }

    #[test]
    fn newline_is_preserved_as_literal_character() {
        assert_eq!(to_legacy_text("line1\nline2"), "line1\nline2");
    }

    #[test]
    fn json_component_single_run_has_no_extra_array() {
        let json = to_json_component("<red>Hi</red>");
        assert_eq!(json, r#"{"text":"Hi","color":"red"}"#);
    }

    #[test]
    fn json_component_multi_run_uses_extra_array() {
        let json = to_json_component("<red>A</red><blue>B</blue>");
        assert!(json.starts_with(r#"{"text":"","extra":["#));
        assert!(json.contains(r#""color":"red""#));
    }

    #[test]
    fn json_component_escapes_quotes_and_newlines() {
        let json = to_json_component("say \"hi\"\nline2");
        assert!(json.contains(r#"say \"hi\"\nline2"#));
    }

    #[test]
    fn empty_input_is_empty_text_component() {
        assert_eq!(to_json_component(""), r#"{"text":""}"#);
    }

    /// The real bundled `default-messages.yml`'s `metricsLimitKickMessage` — multi-line, a
    /// gradient wrapping bold text, a plain named color, and a unicode character (✦), all in one
    /// string. Regression coverage against the actual production content this was built for, not
    /// just synthetic test strings.
    fn strip_legacy_codes(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\u{00a7}' {
                chars.next(); // skip the code character
            } else {
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn real_bundled_metrics_limit_kick_message_renders_without_panicking() {
        let messages = crate::messages::GateMessages::default();
        let input = &messages.metrics_limit_kick_message;

        let legacy = to_legacy_text(input);
        // Per-character gradient coloring interleaves a §-code before every letter (see
        // gradient_produces_per_character_colors), so content checks strip codes first rather
        // than looking for the literal substring in the raw, code-interleaved output.
        let plain = strip_legacy_codes(&legacy);
        assert!(plain.contains("DATA LIMIT REACHED"));
        assert!(plain.contains('\u{2726}')); // ✦ preserved
        assert!(legacy.contains("\u{00a7}l")); // bold survives inside the gradient
        assert!(plain.contains("This server has used up"));
        assert!(plain.contains("Please try again later."));
        assert!(legacy.contains('\n')); // multi-line preserved

        let json = to_json_component(input);
        assert!(json.starts_with(r#"{"text":"","extra":["#));
        // "DATA LIMIT REACHED" and "Please try again later." both sit inside a <gradient>, so
        // JSON also splits them into one {"text":"X",...} object per character (same reasoning
        // as the legacy check above) — "This server has used up..." is under a single <gray>
        // tag with no gradient, so it survives as one literal run/substring here.
        assert!(json.contains("This server has used up its data-transfer allowance."));
    }
}
