//! Port of `me.hippodev.config.ConfigLoader` + `MessagesLoader`'s file-watching halves (the
//! bootstrap-from-bundled-default halves live as `load_or_create_default` on `config`/`messages`
//! directly). Both Kotlin loaders duplicate the same watch/debounce/reload dance for their own
//! file, so this is factored into one generic watcher used by both — see `watch_config` and
//! `watch_messages` below.
//!
//! Kotlin hand-rolls debounce (`Thread.sleep(200)` + draining any follow-up watch-service events)
//! because a single editor save can emit several raw filesystem events (truncate+write, or
//! write-temp+rename). `notify-debouncer-mini` does the same coalescing for us, so this is
//! considerably shorter than the Kotlin version while keeping the same externally-visible
//! behavior: one reload per save, a short settle delay, reload failures logged and otherwise
//! ignored (previous config/messages kept).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult, Debouncer};

use crate::config::{self, GateConfig};
use crate::messages::GateMessages;

/// Matches Kotlin's 200ms debounce window.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// Watches `path` and calls `on_reload` with the freshly parsed config after each save.
/// `messages_supplier` is re-read on every reload so a `messages.yml` change picked up in
/// between still applies the next time `config.yml` reloads.
///
/// Returns the `Debouncer` handle — it must be kept alive (e.g. held in a `let _watcher = ...`
/// binding for the process lifetime) or the watch is dropped and stops firing.
pub fn watch_config(
    path: impl AsRef<Path>,
    messages_supplier: impl Fn() -> GateMessages + Send + 'static,
    mut on_reload: impl FnMut(GateConfig) + Send + 'static,
) -> Result<Debouncer<notify_debouncer_mini::notify::RecommendedWatcher>> {
    let path: PathBuf = path.as_ref().to_path_buf();
    let watch_path = path.clone();
    watch_file(&watch_path, move || match config::load_config(&path, &messages_supplier()) {
        Ok(cfg) => {
            tracing::info!("Reloaded config from {}", path.display());
            on_reload(cfg);
        }
        Err(e) => tracing::warn!("Failed to reload config from {}: {e}", path.display()),
    })
}

/// Watches `path` and calls `on_reload` with the freshly parsed messages after each save.
pub fn watch_messages(
    path: impl AsRef<Path>,
    mut on_reload: impl FnMut(GateMessages) + Send + 'static,
) -> Result<Debouncer<notify_debouncer_mini::notify::RecommendedWatcher>> {
    let path: PathBuf = path.as_ref().to_path_buf();
    let watch_path = path.clone();
    watch_file(&watch_path, move || match GateMessages::load(&path) {
        Ok(m) => {
            tracing::info!("Reloaded messages from {}", path.display());
            on_reload(m);
        }
        Err(e) => tracing::warn!("Failed to reload messages from {}: {e}", path.display()),
    })
}

/// Generic "call `on_change` whenever `path` is modified" watcher, debounced to one call per
/// burst of filesystem events. Watches the parent directory (not the file directly) so it keeps
/// working across an editor's write-temp-then-rename save pattern, matching the Kotlin watcher's
/// `WatchService` registration on the containing directory.
fn watch_file(
    path: &Path,
    mut on_change: impl FnMut() + Send + 'static,
) -> Result<Debouncer<notify_debouncer_mini::notify::RecommendedWatcher>> {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let file_name = path.file_name().map(|n| n.to_os_string());
    let dir = path.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("."));

    let mut debouncer = new_debouncer(DEBOUNCE, move |res: DebounceEventResult| {
        let Ok(events) = res else { return };
        let changed = events.iter().any(|e| e.path.file_name().map(|n| n.to_os_string()) == file_name);
        if changed {
            on_change();
        }
    })?;
    debouncer.watcher().watch(&dir, RecursiveMode::NonRecursive)?;
    Ok(debouncer)
}
