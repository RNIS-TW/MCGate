//! Captures build-time metadata (git commit, target triple, rustc version, build timestamp) as
//! env vars for `version.rs` to embed via `env!()` - none of this is available to a normal
//! compilation unit on its own (only a build script sees `TARGET`/`RUSTC`, and only a shell-out
//! to `git` can read the current commit), so it has to be gathered here and handed off this way.
//! Every value degrades to a plain "unknown" placeholder rather than failing the build when it
//! can't be determined (e.g. building from a source tarball with no `.git`, or `git`/`rustc` not
//! on `PATH`) - none of this is essential to actually running MCGate, just a "what exactly is
//! this binary" nicety in the startup banner.

use std::process::Command;

fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok().map(|s| s.trim().to_string())
}

fn main() {
    let git_hash = run("git", &["rev-parse", "--short=8", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let git_dirty = run("git", &["status", "--porcelain"]).map(|s| !s.is_empty()).unwrap_or(false);
    println!("cargo:rustc-env=MCGATE_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=MCGATE_GIT_DIRTY={git_dirty}");

    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=MCGATE_TARGET_TRIPLE={target}");

    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc_version = run(&rustc, &["--version"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=MCGATE_RUSTC_VERSION={rustc_version}");

    let build_timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    println!("cargo:rustc-env=MCGATE_BUILD_TIMESTAMP={build_timestamp}");

    // Rebuild these values whenever HEAD moves (a new commit, or switching branches) - otherwise
    // an incremental rebuild that touches no tracked source file would keep stale values forever.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
