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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_nonempty() {
        assert!(!version().is_empty());
    }
}
