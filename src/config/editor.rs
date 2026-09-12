//! Programmatic read/modify/write access to `config.yml`'s `routes:` list — what the API's route
//! CRUD endpoints (`net::api`) use to add/replace/remove a route and persist it, instead of only
//! being able to read the live in-memory config.
//!
//! Operates on the raw parsed YAML [`Value`] tree, not the typed [`GateConfig`](super::GateConfig)
//! — editing one route this way never touches or reformats anything else in the file (though
//! `serde_yaml` re-serializing the whole document does still lose comments; there's no way around
//! that without a comment-preserving YAML editor, which is a much bigger dependency than this
//! feature warrants). A route is identified by its `host:` field (a string or list) rather than
//! its position in the file or in the sorted `GateConfig.routes` list — routes are re-sorted by
//! priority on load, so "index 3" isn't a stable identity across a mutation, while a route's host
//! set effectively already has to be unique for routing to make sense at all.
//!
//! [`write_validated`] is the safety net: it never touches the real file until the *proposed*
//! content has round-tripped through the exact same [`load_config`](super::load_config) every
//! other config load goes through, written to a throwaway temp file first. A mutation that would
//! produce an invalid config.yml fails loudly and the real file is never touched.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use serde_yaml::Value;

const CONFIG_KEY: &str = "config";
const ROUTES_KEY: &str = "routes";
const HOST_KEY: &str = "host";

fn key(s: &str) -> Value {
    Value::String(s.to_string())
}

/// A parsed `config.yml`, kept as the raw YAML tree so a route mutation doesn't disturb any
/// other section.
pub struct ConfigDocument {
    root: Value,
    /// Whether the file wraps everything in a top-level `config:` key (both shapes are accepted
    /// on load - see `config::load_config` - so this preserves whichever one was actually there).
    has_config_wrapper: bool,
}

impl ConfigDocument {
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).with_context(|| format!("Config file not found: {}", path.display()))?;
        let root: Value = serde_yaml::from_str(&text).with_context(|| format!("{} is not valid YAML", path.display()))?;
        let has_config_wrapper = root.as_mapping().and_then(|m| m.get(key(CONFIG_KEY))).and_then(|v| v.as_mapping()).is_some();
        if root.as_mapping().is_none() {
            anyhow::bail!("{} does not contain a YAML mapping at its root", path.display());
        }
        Ok(Self { root, has_config_wrapper })
    }

    /// The mapping route mutations actually operate on - either the top-level document, or its
    /// `config:` wrapper, matching whichever shape this file was read as.
    fn section_mut(&mut self) -> &mut serde_yaml::Mapping {
        let root_map = self.root.as_mapping_mut().expect("checked to be a mapping in read()");
        if self.has_config_wrapper {
            root_map.get_mut(key(CONFIG_KEY)).and_then(|v| v.as_mapping_mut()).expect("has_config_wrapper implies this key is a mapping")
        } else {
            root_map
        }
    }

    fn routes_seq_mut(&mut self) -> &mut Vec<Value> {
        let section = self.section_mut();
        if !section.contains_key(key(ROUTES_KEY)) {
            section.insert(key(ROUTES_KEY), Value::Sequence(Vec::new()));
        }
        section.get_mut(key(ROUTES_KEY)).and_then(|v| v.as_sequence_mut()).expect("just ensured routes: is a sequence")
    }

    /// Appends a new route entry (any well-formed YAML mapping matching `config.yml`'s route
    /// schema - `host`/`backend`/`strategy`/etc.) to the end of the routes list.
    pub fn create_route(&mut self, route: Value) {
        self.routes_seq_mut().push(route);
    }

    /// Replaces the route whose `host:` field matches `hosts` (as a set, order-independent) with
    /// `new_route`. Returns `false` (no change made) if no route with that host set exists.
    pub fn replace_route(&mut self, hosts: &[String], new_route: Value) -> bool {
        match find_route_index(self.routes_seq_mut(), hosts) {
            Some(i) => {
                self.routes_seq_mut()[i] = new_route;
                true
            }
            None => false,
        }
    }

    /// Removes the route whose `host:` field matches `hosts`. Returns `false` if none did.
    pub fn delete_route(&mut self, hosts: &[String]) -> bool {
        match find_route_index(self.routes_seq_mut(), hosts) {
            Some(i) => {
                self.routes_seq_mut().remove(i);
                true
            }
            None => false,
        }
    }

    pub fn to_yaml_string(&self) -> Result<String> {
        serde_yaml::to_string(&self.root).context("failed to serialize config back to YAML")
    }
}

fn route_hosts(route: &Value) -> Vec<String> {
    let Some(m) = route.as_mapping() else { return Vec::new() };
    match m.get(key(HOST_KEY)) {
        Some(Value::Sequence(seq)) => seq.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

fn find_route_index(routes: &[Value], hosts: &[String]) -> Option<usize> {
    let target: HashSet<&str> = hosts.iter().map(String::as_str).collect();
    routes.iter().position(|r| {
        let actual = route_hosts(r);
        let actual: HashSet<&str> = actual.iter().map(String::as_str).collect();
        actual == target
    })
}

/// Writes `new_yaml` to `path`, but only after confirming it actually loads via
/// [`load_config`](super::load_config) - checked against a throwaway temp file, never `path`
/// itself, so a mutation that would produce an invalid config can never corrupt the real file.
/// Returns the freshly loaded config on success.
pub fn write_validated(path: impl AsRef<Path>, new_yaml: &str, messages: &crate::config::messages::GateMessages) -> Result<crate::config::GateConfig> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("yml.tmp-validate");
    std::fs::write(&tmp_path, new_yaml).with_context(|| format!("failed to write temp validation file {}", tmp_path.display()))?;
    let result = crate::config::load_config(&tmp_path, messages);
    let _ = std::fs::remove_file(&tmp_path);
    let cfg = result?; // propagates the parse/validation error without ever touching `path`
    std::fs::write(path, new_yaml).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tiny inline temp-file helper, matching `config.rs`'s own `tempfile_shim` - so this doesn't
    // need a `tempfile` dev-dependency for a handful of file-editing tests.
    struct TempFile {
        path: std::path::PathBuf,
    }
    impl TempFile {
        fn new(contents: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
            let suffix = nanos.wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed));
            let mut path = std::env::temp_dir();
            path.push(format!("mcgate-editor-test-{}-{suffix}.yml", std::process::id()));
            std::fs::write(&path, contents).unwrap();
            Self { path }
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn write_temp(text: &str) -> TempFile {
        TempFile::new(text)
    }

    const SAMPLE: &str = r#"
config:
  bind: "0.0.0.0:25565"
  routes:
    - host: "a.example.com"
      backend: "127.0.0.1:1"
    - host: ["b.example.com", "b-alt.example.com"]
      backend: "127.0.0.1:2"
"#;

    #[test]
    fn create_route_appends_to_the_sequence() {
        let f = write_temp(SAMPLE);
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        let new_route: Value = serde_yaml::from_str("host: c.example.com\nbackend: 127.0.0.1:3\n").unwrap();
        doc.create_route(new_route);
        let yaml = doc.to_yaml_string().unwrap();
        let reparsed: Value = serde_yaml::from_str(&yaml).unwrap();
        let routes = reparsed.get("config").unwrap().get("routes").unwrap().as_sequence().unwrap();
        assert_eq!(routes.len(), 3);
        assert_eq!(routes[2].get("host").unwrap().as_str().unwrap(), "c.example.com");
    }

    #[test]
    fn replace_route_matches_by_host_set_not_position() {
        let f = write_temp(SAMPLE);
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        let replacement: Value = serde_yaml::from_str("host: [\"b-alt.example.com\", \"b.example.com\"]\nbackend: 127.0.0.1:99\npriority: 5\n").unwrap();
        assert!(doc.replace_route(&["b.example.com".to_string(), "b-alt.example.com".to_string()], replacement));
        let yaml = doc.to_yaml_string().unwrap();
        let reparsed: Value = serde_yaml::from_str(&yaml).unwrap();
        let routes = reparsed.get("config").unwrap().get("routes").unwrap().as_sequence().unwrap();
        assert_eq!(routes.len(), 2, "replace must not add or remove entries");
        assert_eq!(routes[1].get("backend").unwrap().as_str().unwrap(), "127.0.0.1:99");
    }

    #[test]
    fn replace_route_reports_false_when_host_set_not_found() {
        let f = write_temp(SAMPLE);
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        let replacement: Value = serde_yaml::from_str("host: z.example.com\nbackend: 127.0.0.1:9\n").unwrap();
        assert!(!doc.replace_route(&["z.example.com".to_string()], replacement));
    }

    #[test]
    fn delete_route_removes_only_the_matching_entry() {
        let f = write_temp(SAMPLE);
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        assert!(doc.delete_route(&["a.example.com".to_string()]));
        let yaml = doc.to_yaml_string().unwrap();
        let reparsed: Value = serde_yaml::from_str(&yaml).unwrap();
        let routes = reparsed.get("config").unwrap().get("routes").unwrap().as_sequence().unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].get("host").unwrap().as_sequence().unwrap()[0].as_str().unwrap(), "b.example.com");
    }

    #[test]
    fn write_validated_rejects_a_route_missing_backend_without_touching_the_real_file() {
        let f = write_temp(SAMPLE);
        let original = std::fs::read_to_string(&f.path).unwrap();
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        // A route with no `backend:` at all fails `parse_route`'s own validation.
        let broken: Value = serde_yaml::from_str("host: broken.example.com\n").unwrap();
        doc.create_route(broken);
        let yaml = doc.to_yaml_string().unwrap();
        let messages = crate::config::messages::GateMessages::default();
        let result = write_validated(&f.path, &yaml, &messages);
        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(&f.path).unwrap(), original, "the real file must be untouched after a rejected write");
    }

    #[test]
    fn write_validated_accepts_a_well_formed_route_and_applies_it() {
        let f = write_temp(SAMPLE);
        let mut doc = ConfigDocument::read(&f.path).unwrap();
        let new_route: Value = serde_yaml::from_str("host: c.example.com\nbackend: 127.0.0.1:3\n").unwrap();
        doc.create_route(new_route);
        let yaml = doc.to_yaml_string().unwrap();
        let messages = crate::config::messages::GateMessages::default();
        let cfg = write_validated(&f.path, &yaml, &messages).unwrap();
        assert_eq!(cfg.routes.len(), 3);
        assert_eq!(std::fs::read_to_string(&f.path).unwrap(), yaml);
    }
}
