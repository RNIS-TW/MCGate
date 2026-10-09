#!/usr/bin/env bash
# Packages the native mcgate binaries into one runnable jar: `java -jar mcgate.jar [CONFIG [MESSAGES]]`.
# For hosts that can only launch a jar (Pterodactyl-style panels). launcher/ holds the tiny Java
# entry point that extracts the right binary for the running OS/arch and execs it - see
# launcher/src/mcgate/launcher/Launcher.java.
#
# One jar runs on every platform it carries a binary for; the launcher picks the right one at
# startup. By default it bundles every target already built under target/<triple>/release/:
#   ./build-jar.sh                              # bundle whatever is already built
#   ./build-jar.sh --all                        # build all of JAR_TARGETS first (needs Docker), then bundle
#   ./build-jar.sh x86_64-unknown-linux-musl    # build just that target, then bundle it
# Output: target/mcgate.jar. Needs a JDK 11+ (javac + jar).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# Everything the launcher knows how to pick (Launcher.targetTriples). Linux uses the static musl
# builds so one binary runs on any distro; the gnu ones are still picked up if built. There is no
# 32-bit macOS target - Apple dropped 32-bit support in macOS 10.15.
JAR_TARGETS=(
  aarch64-apple-darwin x86_64-apple-darwin
  x86_64-unknown-linux-musl aarch64-unknown-linux-musl i686-unknown-linux-musl armv7-unknown-linux-musleabihf
  x86_64-pc-windows-gnu i686-pc-windows-gnu
)
KNOWN=("${JAR_TARGETS[@]}" x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)

if [[ "${1:-}" == "--all" ]]; then
  ./build.sh "${JAR_TARGETS[@]}"
  triples=("${JAR_TARGETS[@]}")
elif [[ $# -gt 0 ]]; then
  ./build.sh "$@"
  triples=("$@")
else
  triples=("${KNOWN[@]}")
fi

work=target/jar
rm -rf "$work"
mkdir -p "$work/classes"

bundled=()
for triple in "${triples[@]}"; do
  bin="target/$triple/release/mcgate"
  [[ "$triple" == *windows* ]] && bin="$bin.exe"
  [[ -f "$bin" ]] || continue
  mkdir -p "$work/classes/natives/$triple"
  cp "$bin" "$work/classes/natives/$triple/"
  bundled+=("$triple")
done
if [[ ${#bundled[@]} -eq 0 ]]; then
  echo "error: no built binaries found under target/<triple>/release/ - run ./build.sh first" >&2
  exit 1
fi

javac --release 11 -d "$work/classes" launcher/src/mcgate/launcher/Launcher.java
version=$(sed -n 's/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n1)
printf 'Main-Class: mcgate.launcher.Launcher\nImplementation-Title: MCGate\nImplementation-Version: %s\n' \
  "$version" >"$work/MANIFEST.MF"
jar --create --file target/mcgate.jar --manifest "$work/MANIFEST.MF" -C "$work/classes" .

echo "built: target/mcgate.jar ($(du -h target/mcgate.jar | cut -f1)), bundles: ${bundled[*]}"
