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

# Color, off for NO_COLOR/non-tty/dumb TERM - same precedence the Rust binary's own
# detect_color_support() uses, so build.sh's output stays consistent with mcgate's own.
use_color=1
if [[ -n "${NO_COLOR:-}" ]] || [[ ! -t 1 ]] || [[ "${TERM:-}" == "dumb" ]]; then
  use_color=0
fi
if [[ "$use_color" == "1" ]]; then
  C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_DIM=$'\e[2m'
  C_CYAN=$'\e[36m'; C_GREEN=$'\e[32m'; C_YELLOW=$'\e[33m'; C_RED=$'\e[31m'
  C_REVERSE=$'\e[7m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_CYAN=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_REVERSE=""
fi

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
    printf "  ${C_YELLOW}%-32s${C_RESET} %s\n" "$triple" "$label"
  done
}

build_target() {
  local triple="$1"
  echo
  echo "${C_CYAN}${C_BOLD}==> Building $triple${C_RESET}"

  rustup target add "$triple" >/dev/null 2>&1 || rustup target add "$triple"

  if needs_cross "$triple"; then
    if ! command -v cross >/dev/null 2>&1; then
      echo "    ${C_DIM}'cross' not found - installing (cargo install cross --git https://github.com/cross-rs/cross)...${C_RESET}"
      cargo install cross --git https://github.com/cross-rs/cross
    fi
    if ! docker info >/dev/null 2>&1; then
      echo "    ${C_RED}error: $triple needs Docker (via 'cross') but Docker isn't running. Start Docker and retry.${C_RESET}" >&2
      return 1
    fi
    cross build --release --locked --target "$triple"
  else
    cargo build --release --locked --target "$triple"
  fi

  local bin="target/$triple/release/mcgate"
  [[ -f "$bin" ]] || bin="target/$triple/release/mcgate.exe"
  if [[ -f "$bin" ]]; then
    # `cross` builds run inside a Docker container and write the binary back into this same
    # (bind-mounted) target/ directory - it doesn't reliably come out execute-bit-set on the host
    # side (and can even land owned by root, e.g. rootful Docker on Linux), which is exactly what
    # makes a freshly built binary need a manual `chmod` before it'll run at all. Do it here so
    # that's never a step anyone has to remember. Best-effort: if this fails (e.g. the file really
    # is root-owned and this isn't running as root), fall through to the warning below instead of
    # aborting the whole script over a chmod.
    if ! chmod +x "$bin" 2>/dev/null; then
      echo "    ${C_YELLOW}warning: couldn't chmod +x $bin (wrong owner?) - you may need: sudo chmod +x $bin${C_RESET}" >&2
    fi
    echo "    ${C_GREEN}built:${C_RESET} $bin (${C_DIM}$(du -h "$bin" | cut -f1)${C_RESET})"
  else
    echo "    ${C_RED}warning: expected binary not found at target/$triple/release/mcgate[.exe]${C_RESET}" >&2
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

# Without this, the terminal stays in canonical (line-buffered) mode: the kernel holds every
# keystroke - arrow keys included - until Enter is pressed, since Enter is what normally flushes
# a line to the reading process. `read -n1` alone does NOT change that; it only tells `read` to
# stop after N characters once they actually arrive. Raw mode (`-icanon -echo`, `min 1 time 0`)
# is what makes each keypress reach `read` immediately, which is what makes the selector
# responsive at all rather than only reacting after an unrelated Enter press.
old_stty=$(stty -g 2>/dev/null || true)
restore_tty() {
  show_cursor
  [[ -n "$old_stty" ]] && stty "$old_stty" 2>/dev/null || true
}
trap restore_tty EXIT
[[ -n "$old_stty" ]] && stty -echo -icanon min 1 time 0 2>/dev/null || true

draw() {
  # Redraw from a fixed home position and clear everything after it, rather than moving the
  # cursor up by a computed line count: the instruction line above is long enough to wrap onto a
  # second terminal row in a narrower window, which silently breaks any fixed-line-count math
  # (undershoots by however many lines wrapped) and leaves a stray copy of it behind every frame
  # - the "spamming" bug. Homing + clear-to-end is exactly as many escape codes and can't drift.
  if [[ "${drawn:-0}" == "1" ]]; then
    printf '\e[H\e[J'
  fi
  drawn=1
  echo "${C_BOLD}Select target(s) to build${C_RESET}"
  echo "${C_DIM}Up/Down move * Space toggle * Enter build * A select all * Q quit${C_RESET}"
  echo "${C_DIM}-----------------------------------------------------------------------------${C_RESET}"
  for ((i = 0; i < count; i++)); do
    IFS='|' read -r label triple <<<"${TARGETS[i]}"
    local mark_plain="[ ]"
    local mark_color="${C_DIM}[ ]${C_RESET}"
    if [[ "${selected[i]}" == "1" ]]; then
      mark_plain="[x]"
      mark_color="${C_GREEN}[x]${C_RESET}"
    fi
    # Pad the plain triple text first, then wrap it in color - padding a string that already
    # contains invisible escape bytes counts them toward the width and misaligns the columns.
    local triple_padded
    triple_padded=$(printf '%-32s' "$triple")
    if [[ "$i" == "$cursor" ]]; then
      # A `\e[0m` reset anywhere in the middle (as the per-field colors below use) would also
      # cancel the reverse-video highlight for the rest of the line, so the highlighted row is
      # rendered as one plain reverse-video span instead of mixing in the other colors.
      printf '%s%s %s %s%s\e[K\n' "$C_REVERSE" "$mark_plain" "$triple_padded" "$label" "$C_RESET"
    else
      printf '%s %s%s%s %s\e[K\n' "$mark_color" "$C_YELLOW" "$triple_padded" "$C_RESET" "$label"
    fi
  done
  echo "${C_DIM}-----------------------------------------------------------------------------${C_RESET}"
}

hide_cursor
while true; do
  draw
  IFS= read -rsn1 key
  if [[ "$key" == $'\x1b' ]]; then
    # Read the continuation ONE byte at a time (not both in a single `read -n2`: a batched read
    # that only catches the first byte before its timeout expires still returns successfully
    # with just that one byte, silently desyncing the stream - the real final byte then arrives
    # on the NEXT loop iteration's fresh `read -n1` and gets misread as a literal keypress).
    #
    # The timeout below MUST be a whole integer, not e.g. "0.2": macOS still ships bash 3.2 as
    # /bin/bash (Apple hasn't updated it since bash went GPLv3), and that version's `read -t`
    # rejects a fractional value outright ("invalid timeout specification") and fails instantly
    # - which silently broke every arrow-key press on macOS specifically, since it made `second`
    # always empty, `key` was left as just a lone Escape, and the real `[`/letter bytes then
    # leaked into later loop iterations as bogus standalone keypresses (a stray `a`/`b` matches
    # the `a|A` "select everything" case below) - that's what made Up/Down look broken/erratic.
    # `-t 1` is a ceiling, not a fixed delay: a real arrow key's continuation bytes are already
    # sitting in the input buffer, so this returns near-instantly in the normal case; only a
    # genuinely lone Escape keypress waits out the full second before the loop continues.
    IFS= read -rsn1 -t 1 second || second=""
    if [[ "$second" == "[" ]]; then
      IFS= read -rsn1 -t 1 third || third=""
      key="${key}${second}${third}"
    else
      key="${key}${second}"
    fi
  fi
  case "$key" in
    # `if`, not `cond && action` - under `set -e`, a false `&&` left-hand side (e.g. pressing Up
    # while already at the first row, which is the very first thing anyone tries) makes the
    # whole compound command's exit status 1 and kills the entire script right there.
    $'\x1b[A'|k) if ((cursor > 0)); then ((cursor--)); fi ;;
    $'\x1b[B'|j) if ((cursor < count - 1)); then ((cursor++)); fi ;;
    ' ')
      if [[ "${selected[cursor]}" == "1" ]]; then
        selected[cursor]=0
      else
        selected[cursor]=1
      fi
      ;;
    a|A) for ((i = 0; i < count; i++)); do selected[i]=1; done ;;
    q|Q) echo "Cancelled."; exit 0 ;; # restore_tty (EXIT trap) undoes raw mode/hides-cursor
    "") break ;; # Enter
  esac
done
# Loop exited via Enter, not a script exit, so restore raw mode/cursor visibility explicitly
# here too - the EXIT trap above only fires once the script itself actually exits.
restore_tty

to_build=()
for ((i = 0; i < count; i++)); do
  # Same `set -e` trap as the arrow-key handlers above: a bare `cond && action` here would kill
  # the script outright on the first unselected entry.
  if [[ "${selected[i]}" == "1" ]]; then
    to_build+=("$i")
  fi
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
