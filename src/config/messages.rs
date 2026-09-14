//! Port of `me.hippodev.config.MessagesConfig` (`GateMessages` / `ReconnectMessages`) and
//! `MessagesLoader` (bootstrap-from-bundled-default + load).

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_yaml::Value;

use crate::config::ReconnectMessages;

/// Bundled default `messages.yml`, embedded at compile time (mirrors the Kotlin build packaging
/// `default-messages.yml` as a jar resource read via the classloader).
const DEFAULT_MESSAGES_YML: &str = include_str!("../../resources/default-messages.yml");

/// Default text for all player-facing messages, loaded from messages.yml. Routes may still
/// override any of these per-route via config.yml's `reconnect:` block. `kick_message` is its
/// own top-level setting (not part of `reconnect:`) since it's sent for any offline-backend
/// kick, whether or not reconnect-holding is enabled for that route.
#[derive(Debug, Clone)]
pub struct GateMessages {
    pub kick_message: String,
    /// Shown when a route's `metrics:` upload/download byte limit has been reached. Full
    /// MiniMessage support (gradients, `\n`) is expected once `protocol::text_format` is
    /// ported — for now this is carried as raw text.
    pub metrics_limit_kick_message: String,
    pub reconnect: ReconnectMessages,
}

impl Default for GateMessages {
    fn default() -> Self {
        Self {
            kick_message: "&cServer is offline. Please reconnect shortly.".into(),
            metrics_limit_kick_message: concat!(
                "<gradient:#f85032:#e73827><bold>\u{2726} DATA LIMIT REACHED \u{2726}</bold></gradient>\n",
                "<gray>This server has used up its data-transfer allowance.</gray>\n",
                "<gradient:#8e9eab:#eef2f3>Please try again later.</gradient>"
            )
            .to_string(),
            reconnect: ReconnectMessages::default(),
        }
    }
}

fn as_map(v: &Value) -> Option<&serde_yaml::Mapping> {
    v.as_mapping()
}

fn get<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a Value> {
    map.get(Value::String(key.to_string()))
}

fn get_str(map: &serde_yaml::Mapping, key: &str) -> Option<String> {
    get(map, key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

impl GateMessages {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).with_context(|| format!("Messages file not found: {}", path.display()))?;
        Self::parse(&text)
    }

    fn parse(text: &str) -> Result<Self> {
        let root: Value = serde_yaml::from_str(text)?;
        let root_map = as_map(&root).cloned().unwrap_or_default();
        let messages_section = get(&root_map, "messages").and_then(as_map).cloned().unwrap_or(root_map);

        let defaults = GateMessages::default();
        Ok(GateMessages {
            kick_message: get_str(&messages_section, "kickMessage").unwrap_or(defaults.kick_message),
            metrics_limit_kick_message: get_str(&messages_section, "metricsLimitKickMessage")
                .unwrap_or(defaults.metrics_limit_kick_message),
            reconnect: parse_reconnect_messages(get(&messages_section, "reconnect").and_then(as_map)),
        })
    }

    /// Loads the messages file at `path`, first bootstrapping it from the bundled
    /// `default-messages.yml` if missing.
    pub fn load_or_create_default(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            fs::write(path, DEFAULT_MESSAGES_YML)
                .with_context(|| format!("failed to bootstrap default messages file at {}", path.display()))?;
            tracing::info!("No messages file found at {}, created one from the bundled default", path.display());
        }
        Self::load(path)
    }
}

fn parse_reconnect_messages(r: Option<&serde_yaml::Mapping>) -> ReconnectMessages {
    let defaults = ReconnectMessages::default();
    let Some(r) = r else { return defaults };
    let frames = get(r, "actionBarFrames")
        .and_then(|v| v.as_sequence())
        .map(|seq| seq.iter().filter_map(|v| v.as_str().map(str::to_string)).collect());
    ReconnectMessages {
        title: get_str(r, "title").unwrap_or(defaults.title),
        subtitle: get_str(r, "subtitle").unwrap_or(defaults.subtitle),
        attempt_suffix: get_str(r, "attemptSuffix").unwrap_or(defaults.attempt_suffix),
        action_bar_frames: frames.unwrap_or(defaults.action_bar_frames),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_when_section_absent() {
        let m = GateMessages::parse("foo: bar").unwrap();
        assert_eq!(m.kick_message, GateMessages::default().kick_message);
    }

    #[test]
    fn reads_top_level_or_nested_messages_section() {
        let nested = GateMessages::parse("messages:\n  kickMessage: \"&cnope\"\n").unwrap();
        assert_eq!(nested.kick_message, "&cnope");

        let top_level = GateMessages::parse("kickMessage: \"&cnope\"\n").unwrap();
        assert_eq!(top_level.kick_message, "&cnope");
    }

    #[test]
    fn reconnect_overrides_merge_with_defaults() {
        let m = GateMessages::parse(
            "messages:\n  reconnect:\n    title: \"custom title\"\n",
        )
        .unwrap();
        assert_eq!(m.reconnect.title, "custom title");
        assert_eq!(m.reconnect.subtitle, ReconnectMessages::default().subtitle);
    }

    #[test]
    fn bundled_default_resource_parses() {
        // Guards against the bundled default-messages.yml drifting out of sync with the schema
        // this loader expects.
        GateMessages::parse(DEFAULT_MESSAGES_YML).unwrap();
    }

    #[test]
    fn load_or_create_default_bootstraps_missing_file() {
        let mut path = std::env::temp_dir();
        path.push(format!("mcgate-messages-test-{}.yml", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert!(!path.exists());
        let m = GateMessages::load_or_create_default(&path).unwrap();
        assert!(path.exists());
        // The bundled default-messages.yml should have been used to bootstrap the file, so
        // reloading it should match parsing that resource directly.
        assert_eq!(m.kick_message, GateMessages::parse(DEFAULT_MESSAGES_YML).unwrap().kick_message);
        std::fs::remove_file(&path).unwrap();
    }
}
