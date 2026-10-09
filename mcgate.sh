#!/bin/sh
# Start/stop MCGate as a background process, with a console you can talk to and live logs.
#
# No tmux, screen or any other dependency: the daemon's stdin is a named pipe this script keeps
# open, and its console output is already in log/latest.log. That works because of two facts
# about the binary:
#   * src/console.rs run_plain() accepts every console command over plain stdin - the rustyline
#     pty path only adds line editing and history, not commands.
#   * every command replies through tracing::info! (src/console.rs handle_command), so the answer
#     lands in log/latest.log rather than on the process's stdout.
# So `console` is just: write typed lines into the pipe, tail the log. The pipe is opened
# read-write by the daemon (exec <>FIFO), which is what keeps it from ever seeing EOF when a
# writer disconnects - otherwise the first `cmd` to finish would silently kill the console.
#
# Usage: ./mcgate.sh {start|stop|restart|status|console|cmd|logs} [args...]
set -eu

APP=mcgate
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUN_DIR=$DIR/.run
PID_FILE=$RUN_DIR/$APP.pid
FIFO=$RUN_DIR/$APP.in
OUT_FILE=$RUN_DIR/$APP.out
LOG_FILE=$DIR/log/latest.log
# Seconds to wait for a graceful shutdown before escalating to SIGINT -> SIGTERM -> SIGKILL.
STOP_TIMEOUT=${MCGATE_STOP_TIMEOUT:-20}

say() { printf '%s\n' "$*"; }
die() { printf '%s: %s\n' "$APP" "$*" >&2; exit 1; }

# Resolves the binary: $MCGATE_BIN wins, then one sitting next to this script (how a release build
# is actually deployed - binary and mcgate.sh in the same directory, no cargo target tree), then a
# local release/debug build, then $PATH.
resolve_bin() {
    if [ -n "${MCGATE_BIN:-}" ]; then
        [ -x "$MCGATE_BIN" ] || die "MCGATE_BIN=$MCGATE_BIN is not executable"
        printf '%s\n' "$MCGATE_BIN"
        return
    fi
    self=$DIR/$(basename -- "$0")
    for candidate in "$DIR/$APP" "$DIR/bin/$APP" "$DIR/target/release/$APP" "$DIR/target/debug/$APP"; do
        # Skip this script itself, in case it was deployed under the binary's own name.
        [ "$candidate" = "$self" ] && continue
        [ -f "$candidate" ] || continue
        [ -x "$candidate" ] || die "found $candidate but it is not executable - run 'chmod +x $candidate'"
        printf '%s\n' "$candidate"
        return
    done
    command -v "$APP" >/dev/null 2>&1 && { command -v "$APP"; return; }
    die "no binary found - put the release binary next to this script as $DIR/$APP, run 'cargo build --release', or set MCGATE_BIN"
}

read_pid() { [ -f "$PID_FILE" ] && cat "$PID_FILE" || printf ''; }
alive() { [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null; }
running_pid() { pid=$(read_pid); alive "$pid" && printf '%s\n' "$pid" || printf ''; }
require_running() {
    pid=$(running_pid)
    [ -n "$pid" ] || die "not running - start it with '$0 start'"
    printf '%s\n' "$pid"
}

# Sends one console command into the daemon's stdin pipe. Never blocks: the daemon holds the read
# end open for its whole lifetime, so there is always a reader.
send() {
    [ -p "$FIFO" ] || die "control pipe $FIFO is missing - restart to recreate it"
    printf '%s\n' "$1" >"$FIFO"
}

start() {
    pid=$(running_pid)
    [ -n "$pid" ] && die "already running (pid $pid) - use 'restart' or 'stop' first"

    bin=$(resolve_bin)
    mkdir -p "$RUN_DIR" "$DIR/log"
    rm -f "$PID_FILE" "$FIFO"
    mkfifo -m 600 "$FIFO"

    # `exec <>FIFO` makes the pipe the daemon's stdin *and* keeps a write end open in the daemon
    # itself, so the console never hits EOF between commands. The pid written is the daemon's own
    # (echo $$ then exec), so `stop` signals mcgate directly rather than a wrapper shell.
    inner="cd '$DIR' && exec <>'$FIFO' && echo \$\$ > '$PID_FILE' && exec '$bin'"
    for arg in "$@"; do inner="$inner '$arg'"; done
    nohup sh -c "$inner" >"$OUT_FILE" 2>&1 &

    # Wait for the pid file, then confirm it survived startup - a bad config.yml or an already-bound
    # port exits early, and without this check 'start' would report success.
    i=0
    while [ ! -s "$PID_FILE" ] && [ "$i" -lt 50 ]; do i=$((i + 1)); sleep 0.1; done
    pid=$(read_pid)
    [ -n "$pid" ] || die "failed to start - see $OUT_FILE"
    sleep 1
    if ! alive "$pid"; then
        rm -f "$PID_FILE" "$FIFO"
        say "$APP: started but exited immediately - last lines:" >&2
        tail -n 15 "$LOG_FILE" 2>/dev/null | sed 's/^/  /' >&2
        exit 1
    fi
    say "$APP: started (pid $pid)"
    say "  console: $0 console     one-off: $0 cmd players     logs: $0 logs"
}

stop() {
    pid=$(running_pid)
    if [ -z "$pid" ]; then
        rm -f "$PID_FILE" "$FIFO"
        say "$APP: not running"
        return 0
    fi

    # The console's own `stop` command is the cleanest exit - it runs the same graceful path as
    # Ctrl+C (kick players, flush the SQLite writers) from inside the process. SIGINT is the
    # fallback if the pipe is gone; SIGTERM/SIGKILL are escalations, not the normal route.
    say "$APP: stopping (pid $pid)..."
    if [ -p "$FIFO" ]; then send stop; else kill -INT "$pid" 2>/dev/null || true; fi
    waited=0
    while alive "$pid" && [ "$waited" -lt "$STOP_TIMEOUT" ]; do
        sleep 1
        waited=$((waited + 1))
        # Half way through, try a signal too, in case the console thread is wedged.
        [ "$waited" -eq $((STOP_TIMEOUT / 2)) ] && kill -INT "$pid" 2>/dev/null || true
    done
    if alive "$pid"; then
        say "$APP: still up after ${STOP_TIMEOUT}s, sending SIGTERM"
        kill -TERM "$pid" 2>/dev/null || true
        sleep 3
    fi
    if alive "$pid"; then
        say "$APP: unresponsive, sending SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$PID_FILE" "$FIFO"
    say "$APP: stopped"
}

status() {
    pid=$(running_pid)
    if [ -z "$pid" ]; then
        [ -f "$PID_FILE" ] && say "$APP: not running (stale pid file $PID_FILE)" || say "$APP: not running"
        return 1
    fi
    say "$APP: running"
    say "  pid     : $pid"
    say "  console : $([ -p "$FIFO" ] && printf 'ready (%s)' "$FIFO" || printf 'unavailable - control pipe missing')"
    say "  log     : $LOG_FILE"
    ps -o pid= -o etime= -o rss= -o command= -p "$pid" 2>/dev/null | sed 's/^/  ps      : /'
    return 0
}

# One-shot: send a command and print whatever it logged in reply.
cmd() {
    [ $# -gt 0 ] || die "usage: $0 cmd <console command>  (e.g. '$0 cmd players')"
    pid=$(require_running)
    before=$(wc -l <"$LOG_FILE" 2>/dev/null || printf 0)
    send "$*"
    # Give the console thread a moment to handle it and the log writer to flush.
    sleep "${MCGATE_CMD_WAIT:-0.6}"
    if alive "$pid" || [ "${1:-}" = stop ]; then
        tail -n "+$((before + 1))" "$LOG_FILE" 2>/dev/null
    fi
}

# Interactive console: typed lines go into the pipe, the log streams back in the same terminal.
console() {
    pid=$(require_running)
    [ -p "$FIFO" ] || die "control pipe $FIFO is missing - restart to recreate it"

    # -F (follow by name, retry) rather than -f: logging.rs rolls latest.log every 10k lines, and
    # plain -f would keep following the renamed file and go silent.
    tail -n "${MCGATE_CONSOLE_BACKLOG:-20}" -F "$LOG_FILE" 2>/dev/null &
    tail_pid=$!
    # shellcheck disable=SC2064
    trap "kill $tail_pid 2>/dev/null || true" EXIT INT TERM

    say "--- $APP console (pid $pid). Type a command; 'help' lists them."
    say "--- Ctrl+D or 'detach' leaves the console; the server keeps running. 'stop' shuts it down."
    while IFS= read -r line; do
        case $line in
            "") continue ;;
            detach|quit) break ;;
        esac
        send "$line"
        sleep "${MCGATE_CMD_WAIT:-0.6}"
        if ! alive "$pid"; then
            say "--- $APP has stopped."
            rm -f "$PID_FILE" "$FIFO"
            break
        fi
    done
    kill "$tail_pid" 2>/dev/null || true
    trap - EXIT INT TERM
}

logs() {
    [ -f "$LOG_FILE" ] || die "no log file at $LOG_FILE"
    case ${1:-} in
        ""|-f) tail -n "${MCGATE_CONSOLE_BACKLOG:-50}" -F "$LOG_FILE" ;;
        *)     tail -n "$1" "$LOG_FILE" ;;
    esac
}

command=${1:-}
[ $# -gt 0 ] && shift
case $command in
    start)   start "$@" ;;
    stop)    stop ;;
    restart) stop; start "$@" ;;
    status)  status ;;
    console|attach) console ;;
    cmd)     cmd "$@" ;;
    logs|tail) logs "$@" ;;
    *)
        cat <<USAGE
Usage: $0 <command> [args]

  start [CONFIG [MESSAGES]]  run mcgate in the background
  stop                       graceful shutdown, escalating if needed
  restart [CONFIG [MESSAGES]]
  status                     pid, console readiness and log location
  console                    interactive console + live log ('detach' to leave)
  cmd <console command>      run one command and print its reply
  logs [-f|N]                follow log/latest.log, or print the last N lines

Env: MCGATE_BIN, MCGATE_STOP_TIMEOUT, MCGATE_CMD_WAIT, MCGATE_CONSOLE_BACKLOG
USAGE
        [ -z "$command" ] && exit 0 || exit 2 ;;
esac
