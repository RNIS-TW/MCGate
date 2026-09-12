//! Port of `Main.kt`'s `version` property.
//!
//! The Kotlin version reads `Package.implementationVersion` from the jar manifest, falling back
//! to `"dev"` when run unpackaged (an IDE run config, `mvn exec`) since there's no manifest to
//! read in that case. Cargo always bakes the crate version into the binary at compile time via
//! `CARGO_PKG_VERSION` (no separate "packaged vs. unpackaged" distinction exists for a compiled
//! Rust binary), so this is simpler than the Kotlin version and the "dev" fallback isn't needed —
//! kept only as a defensive fallback in case that env var is ever empty.
pub fn version() -> &'static str {
    let v = env!("CARGO_PKG_VERSION");
    if v.is_empty() {
        "dev"
    } else {
        v
    }
}

/// Short (8-char) git commit hash this binary was built from, from `build.rs` (a build script is
/// the only thing that can shell out to `git` - a normal compilation unit has no way to read the
/// current commit on its own). `"unknown"` when building from a source tree with no `.git` (e.g.
/// an extracted release tarball) or without `git` on `PATH`.
pub fn git_hash() -> &'static str {
    env!("MCGATE_GIT_HASH")
}

/// Whether the working tree had uncommitted changes at build time - a locally modified build
/// worth knowing apart from a clean release one, same reasoning `git describe --dirty` exists for.
pub fn git_dirty() -> bool {
    env!("MCGATE_GIT_DIRTY") == "true"
}

/// The Rust target triple (OS + architecture + ABI) this binary was compiled for, e.g.
/// `x86_64-unknown-linux-gnu` - only a build script sees Cargo's `TARGET` env var, so `build.rs`
/// captures it the same way it does the git commit.
pub fn target_triple() -> &'static str {
    env!("MCGATE_TARGET_TRIPLE")
}

/// The `rustc --version` output used for this build, captured by `build.rs` the same way.
pub fn rustc_version() -> &'static str {
    env!("MCGATE_RUSTC_VERSION")
}

/// When this binary was compiled, as a human-readable UTC timestamp. `build.rs` can only capture
/// a raw Unix timestamp (no `chrono` in build-script context without adding it as a *build*
/// dependency too, which isn't worth it for one formatted string); this formats it at the call
/// site with `chrono`, which the rest of the binary already depends on anyway.
pub fn build_timestamp_utc() -> String {
    let secs: i64 = env!("MCGATE_BUILD_TIMESTAMP").parse().unwrap_or(0);
    chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()).unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_nonempty() {
        assert!(!version().is_empty());
    }

    #[test]
    fn build_metadata_is_always_present_even_if_placeholder() {
        // Every one of these must resolve to *something* (a real value or "unknown") - never
        // panic or produce an empty string, regardless of whether this build had a `.git`
        // directory, `git`, or `rustc` available to `build.rs`.
        assert!(!git_hash().is_empty());
        assert!(!target_triple().is_empty());
        assert!(!rustc_version().is_empty());
        assert!(!build_timestamp_utc().is_empty());
    }
}
