#!/usr/bin/env bash
# Interactive build picker: arrow keys + space to select one or more target platforms, enter to
# build them. Handles rustup target installation and reaches for `cross` (Docker-based) whenever
# the chosen target needs a different OS/libc than this machine can link natively - see
# README.md's "Cross-compiling for a Linux server" section for why plain `cargo build --target`
# alone isn't enough for that case.
#
# Non-interactive usage (for scripts/CI): pass target triples directly, e.g.
#   ./build.sh x86_64-unknown-linux-gnu aarch64-apple-darwin
#   ./build.sh --list        # print the known targets and exit
#   ./build.sh --all         # build every known target
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# label|target triple
TARGETS=(
  "macOS (Apple Silicon / arm64)|aarch64-apple-darwin"
  "macOS (Intel / x86_64)|x86_64-apple-darwin"
  "Linux x86_64 (glibc)|x86_64-unknown-linux-gnu"
  "Linux ARM64 (glibc)|aarch64-unknown-linux-gnu"
  "Windows x86_64 (gnu)|x86_64-pc-windows-gnu"
)

host_os=$(uname -s)

# Whether $1 (a target triple) needs `cross` (Docker-based toolchain) rather than a plain
# `cargo build --target` on this host - true whenever the target's OS doesn't match the host's,
# since the host linker can't produce another OS's binary format/ABI on its own. Same-OS,
# different-CPU targets (e.g. building x86_64-apple-darwin from an arm64 Mac) link fine natively.
needs_cross() {
  local target="$1"
  case "$host_os" in
    Darwin) [[ "$target" != *"-apple-darwin" ]] ;;
    Linux) [[ "$target" != *"-unknown-linux-"* ]] ;;
    *) return 0 ;;
  esac
}

print_list() {
  echo "Known targets:"
  for entry in "${TARGETS[@]}"; do
    IFS='|' read -r label triple <<<"$entry"
    printf '  %-32s %s\n' "$triple" "$label"
  done
}

build_target() {
  local triple="$1"
  echo
  echo "==> Building $triple"

  rustup target add "$triple" >/dev/null 2>&1 || rustup target add "$triple"

  if needs_cross "$triple"; then
    if ! command -v cross >/dev/null 2>&1; then
      echo "    'cross' not found - installing (cargo install cross --git https://github.com/cross-rs/cross)..."
      cargo install cross --git https://github.com/cross-rs/cross
    fi
    if ! docker info >/dev/null 2>&1; then
      echo "    error: $triple needs Docker (via 'cross') but Docker isn't running. Start Docker and retry." >&2
      return 1
    fi
    cross build --release --locked --target "$triple"
  else
    cargo build --release --locked --target "$triple"
  fi

  local bin="target/$triple/release/mcgate"
  [[ -f "$bin" ]] || bin="target/$triple/release/mcgate.exe"
  if [[ -f "$bin" ]]; then
    echo "    built: $bin ($(du -h "$bin" | cut -f1))"
  else
    echo "    warning: expected binary not found at target/$triple/release/mcgate[.exe]" >&2
  fi
}

# --- Non-interactive modes -------------------------------------------------

if [[ "${1:-}" == "--list" ]]; then
  print_list
  exit 0
fi

if [[ "${1:-}" == "--all" ]]; then
  for entry in "${TARGETS[@]}"; do
    IFS='|' read -r _ triple <<<"$entry"
    build_target "$triple"
  done
  exit 0
fi

if [[ $# -gt 0 ]]; then
  for triple in "$@"; do
    build_target "$triple"
  done
  exit 0
fi

# --- Interactive picker -----------------------------------------------------

if [[ ! -t 0 || ! -t 1 ]]; then
  echo "Not an interactive terminal - pass target triples directly instead, e.g.:" >&2
  echo "  $0 x86_64-unknown-linux-gnu" >&2
  print_list >&2
  exit 1
fi

count=${#TARGETS[@]}
selected=()
for ((i = 0; i < count; i++)); do selected[i]=0; done
cursor=0

hide_cursor() { printf '\e[?25l'; }
show_cursor() { printf '\e[?25h'; }
cleanup() { show_cursor; }
trap cleanup EXIT

draw() {
  # Redraw in place: move up over the previously drawn block first (skipped on first draw).
  if [[ "${drawn:-0}" == "1" ]]; then
    printf '\e[%dA' "$((count + 3))"
  fi
  drawn=1
  echo "Select target(s) to build - Up/Down move, Space toggle, Enter build, A all, Q quit"
  echo "-----------------------------------------------------------------------------"
  for ((i = 0; i < count; i++)); do
    IFS='|' read -r label triple <<<"${TARGETS[i]}"
    local mark=" "
    [[ "${selected[i]}" == "1" ]] && mark="x"
    local line
    line=$(printf '[%s] %-32s %s' "$mark" "$triple" "$label")
    if [[ "$i" == "$cursor" ]]; then
      printf '\e[7m%s\e[0m\e[K\n' "$line"
    else
      printf '%s\e[K\n' "$line"
    fi
  done
  echo "-----------------------------------------------------------------------------"
}

hide_cursor
while true; do
  draw
  IFS= read -rsn1 key
  if [[ "$key" == $'\x1b' ]]; then
    read -rsn2 -t 0.01 rest || rest=""
    key+="$rest"
  fi
  case "$key" in
    $'\x1b[A'|k) ((cursor > 0)) && ((cursor--)) ;;
    $'\x1b[B'|j) ((cursor < count - 1)) && ((cursor++)) ;;
    ' ') [[ "${selected[cursor]}" == "1" ]] && selected[cursor]=0 || selected[cursor]=1 ;;
    a|A) for ((i = 0; i < count; i++)); do selected[i]=1; done ;;
    q|Q) show_cursor; echo "Cancelled."; exit 0 ;;
    "") break ;; # Enter
  esac
done
show_cursor

to_build=()
for ((i = 0; i < count; i++)); do
  [[ "${selected[i]}" == "1" ]] && to_build+=("$i")
done
# Nothing explicitly checked - build whichever entry the cursor was resting on.
if [[ ${#to_build[@]} -eq 0 ]]; then
  to_build=("$cursor")
fi

for i in "${to_build[@]}"; do
  IFS='|' read -r _ triple <<<"${TARGETS[i]}"
  build_target "$triple"
done

echo
echo "Done."
