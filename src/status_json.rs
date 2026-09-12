//! Port of `protocol/StatusJson.kt`.

use crate::config::FallbackStatus;
use crate::text_format::{escape_json, to_json_component};
use std::fmt::Write as _;

pub fn build_fallback_json(fallback: &FallbackStatus) -> String {
    let mut out = String::new();
    out.push('{');
    out.push_str("\"version\":{");
    let _ = write!(out, "\"name\":\"{}\",", escape_json(&fallback.version.name));
    let _ = write!(out, "\"protocol\":{}", fallback.version.protocol);
    out.push_str("},");
    // Goes through the same rich-text pipeline as every other message (see text_format.rs), so
    // &-codes and MiniMessage tags both work in the MOTD too.
    out.push_str("\"description\":");
    out.push_str(&to_json_component(&fallback.motd));
    if let Some(players) = &fallback.players {
        let _ = write!(out, ",\"players\":{{\"online\":{},\"max\":{}}}", players.online, players.max);
    }
    if let Some(favicon) = &fallback.favicon_data_uri {
        let _ = write!(out, ",\"favicon\":\"{}\"", escape_json(favicon));
    }
    out.push('}');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PlayersInfo, VersionInfo};

    #[test]
    fn builds_expected_shape() {
        let fallback = FallbackStatus {
            motd: "&eHello".to_string(),
            version: VersionInfo { name: "1.21".to_string(), protocol: 767 },
            players: Some(PlayersInfo { online: 3, max: 20 }),
            favicon_data_uri: Some("data:image/png;base64,abc".to_string()),
        };
        let json = build_fallback_json(&fallback);
        assert!(json.contains(r#""name":"1.21""#));
        assert!(json.contains(r#""protocol":767"#));
        assert!(json.contains(r#""online":3"#));
        assert!(json.contains(r#""max":20"#));
        assert!(json.contains(r#""favicon":"data:image/png;base64,abc""#));
        assert!(json.contains(r#""color":"yellow""#)); // &e rendered through the rich-text pipeline
    }

    #[test]
    fn omits_optional_fields_when_absent() {
        let fallback = FallbackStatus {
            motd: "hi".to_string(),
            version: VersionInfo { name: "1.21".to_string(), protocol: 767 },
            players: None,
            favicon_data_uri: None,
        };
        let json = build_fallback_json(&fallback);
        assert!(!json.contains("players"));
        assert!(!json.contains("favicon"));
    }
}
