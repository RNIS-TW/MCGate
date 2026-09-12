# MCGate

A thin, host-based reverse proxy for Minecraft servers, written in Rust/tokio and compiled to a
single native binary — no JVM, no separate runtime.

See [`plan.md`](plan.md) for the port history from the original Java/Kotlin implementation this
was rewritten from, including exactly what was ported, what's deliberately out of scope, and how
each piece was verified. Short version: every section is done — config, hot reload, the TCP
relay (handshake/status/login), backend selection strategies, per-route traffic metrics, SQLite
connection tracking and stats logging, the UDP/voicechat relays, the console REPL, and the HTTP
API — except mid-session auto-reconnect-holding, which was deliberately not ported.

## Project layout

```
src/
  main.rs       entry point: CLI args, startup wiring, the shared graceful-shutdown path
  console.rs    the admin REPL (help/players/kick/reload/stop/...)
  logging.rs    console + rolling-file logging, colour/level handling
  version.rs    build version string

  protocol/     Minecraft wire format - pure parsing/encoding, no app state or I/O
    varint.rs, minecraft_protocol.rs, handshake.rs, nbt.rs, compression.rs,
    text_format.rs, status_json.rs, reconnect_protocol.rs,
    proxy_protocol_tcp.rs, proxy_protocol_datagram.rs

  config/       config.yml/messages.yml schema, parsing, and hot-reload
    config.rs (schema + parsing, at the crate::config root), loader.rs (file
    watching), messages.rs, duration.rs, host_pattern.rs

  net/          the TCP relay: accept loop, backend dial/selection, anti-abuse
                gating, and the HTTP status/metrics API
    server.rs, backend_pinger.rs, backend_selector.rs, buffered_stream.rs,
    connection_guard.rs, dns_cache.rs, flood_control.rs, network_info.rs,
    ping_cache.rs, api.rs

  udp/          UDP: standalone forwards, the voicechat relay + its routing
                table, and the shared anti-abuse session/rate limits
    proxy.rs, voice_relay.rs, voice_routing.rs, throttle.rs

  state/        live process state: connected players, per-route runtime,
                and the SQLite-backed connection-tracking/stats-logging writers
    state.rs (sessions/route runtime, at the crate::state root),
    app_state.rs, connection_tracker.rs, route_metrics_store.rs, stats_logger.rs
```

Each of `config`, `net`, `udp`, and `state` is a directory whose parent file (`config.rs`,
`net.rs`, `udp.rs`, `state.rs`) just declares its submodules — `config.rs` and `state.rs` also
hold that module's core types directly (`GateConfig`/`Route` and `PlayerSession`/`RouteRuntime`
respectively), so e.g. `crate::config::Route` and `crate::config::host_pattern::HostPattern` are
both valid, one from the parent file and one from a submodule.

## Build

Requires the Rust toolchain (stable — install via [rustup](https://rustup.rs) if you don't have
it: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`). The `rusqlite` dependency
compiles SQLite from source (`bundled` feature), so a C compiler needs to be on `PATH` — already
true on macOS (Xcode command line tools) and virtually every Linux distro's build tooling; on a
minimal server image install `build-essential` (Debian/Ubuntu) or `gcc` (RHEL/Alpine) first.

```
cargo build --release
```

Produces a single self-contained binary at `target/release/mcgate` — no separate runtime, no other
files to ship alongside it (`bundled` SQLite is statically linked in). Copy that one file to a
server and run it; that's the whole deployment.

**The binary is only portable across machines with the same OS and CPU architecture as the one it
was built on.** Running a macOS/arm64 (Apple Silicon) build on a Linux/x86_64 server, for
example, fails with `cannot execute binary file: Exec format error`. That's not a corrupted
binary, it's the wrong target. Either build directly on a machine matching the deployment target,
or cross-compile.

### Cross-compiling for a Linux server (e.g. Pterodactyl)

Building on macOS but deploying to a Linux/x86_64 host (the common case: most VPS/hosting-panel
servers, Pterodactyl included, are `x86_64-unknown-linux-gnu`)? Install
[`cross`](https://github.com/cross-rs/cross) once (needs Docker running locally), then:

```
cargo install cross --git https://github.com/cross-rs/cross
cross build --release --target x86_64-unknown-linux-gnu
```

That produces `target/x86_64-unknown-linux-gnu/release/mcgate` — copy that binary to the server.
For an ARM64 Linux host (e.g. AWS Graviton, Oracle's ARM free tier) swap the target for
`aarch64-unknown-linux-gnu` instead.

Plain `cargo build --target ...` isn't enough for cross-OS builds — it still uses the host
linker, which can't produce a Linux glibc binary from macOS. `cross` runs the build inside a
Docker container with the right target toolchain instead.

`cross` writes the binary back out through a bind mount from inside that container, so it doesn't
always come back execute-bit-set (or even owned by you) on the host side — if the server says
`Permission denied` on startup, `chmod +x mcgate` (or `sudo chown` first, if it's owned by root)
before running it. [`build.sh`](build.sh) does this for you automatically (see below).

### `build.sh`: an interactive build picker

```
./build.sh
```

An arrow-key menu over the targets above — `Up`/`Down` to move, `Space` to check one or more,
`A` to select all, `Enter` to build. Installs the needed `rustup` target and `cross` automatically,
picks plain `cargo build` vs. `cross build` per target the same way this section does, and
`chmod +x`'s the result. Non-interactive: `./build.sh <target-triple> [<target-triple> ...]`,
`./build.sh --all`, or `./build.sh --list` to see the known targets.

## Run

```
./target/release/mcgate [config.yml] [messages.yml]
```

Both arguments are optional and default to `config.yml`/`messages.yml` in the current directory;
a missing file is bootstrapped from the built-in defaults on first run.

```
./target/release/mcgate --help       # usage
./target/release/mcgate --version    # prints the built version
```

### Console

Once running, type commands directly into stdin: `help`, `players`, `whois <player>`,
`kick <player> [message]`, `routes`, `metrics [reset <index|host|all>]`, `reload`, `uptime`,
`version`, `stop`/`shutdown`/`exit`. Plain stdin (one command per line) is the default and is
what you want under a hosting panel like Pterodactyl — panel "send command" boxes write a whole
line into the container's stdin at once rather than emulating real keystroke-by-keystroke
terminal input, which line-editing libraries can't follow (it shows up as commands needing to be
sent more than once, or the prompt and typed text landing on separate lines). Set
`MCGATE_INTERACTIVE_CONSOLE=true` to opt into line editing/history instead, when actually running
the binary directly in your own terminal.

`stop`/`shutdown`/`exit` (and Ctrl+C) all go through the same graceful shutdown: every connected
player is disconnected, the SQLite connection-tracking/stats-logging writers are flushed, then
the process exits — rather than hanging until players happen to disconnect on their own.

### Running as a service

A minimal systemd unit (adjust `User`, `WorkingDirectory`, and the binary path):

```ini
[Unit]
Description=MCGate
After=network.target

[Service]
Type=simple
User=mcgate
WorkingDirectory=/opt/mcgate
ExecStart=/opt/mcgate/mcgate config.yml messages.yml
Restart=on-failure
RestartSec=5
# The process handles SIGINT itself (graceful shutdown - see main.rs); SIGTERM's default
# behavior (immediate termination) is fine too since state is either persisted continuously
# (SQLite, route metrics) or doesn't need draining.

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl enable --now mcgate
journalctl -u mcgate -f     # logs (also written to ./log/latest.log regardless)
```

### Memory

Unlike the JVM build, there's no separate heap/arena sizing story here: no `-Xmx`, no
`MaxDirectMemorySize`, no allocator arena tuning (see `plan.md`'s section 3 note on why
`tuneNettyMemoryFootprint`'s equivalent isn't needed — tokio has no comparable fixed per-thread
buffer-cache overhead that scales off a container's misreported CPU count). The binary's resident
set is close to its actual working set: a small fixed amount for the runtime plus a modest
per-connection cost, not a pre-reserved heap. Nothing to configure for typical deployments.

### HTTP API

With `api.enabled: true` (see `config.yml`), MCGate exposes a small JSON admin/status API plus
interactive docs:

- `GET /reference` — a [Scalar](https://github.com/scalar/scalar)-rendered API reference page.
  `GET /` redirects here.
- `GET /openapi.json` — the OpenAPI 3.0 spec backing that page (hand-authored, in
  `resources/openapi.json`).
- `GET /metrics`, `GET /v1/routes[/{index}[/backends|/metrics|/ping]]`, `GET /v1/players` — the
  existing read-only status/metrics endpoints.
- `POST /v1/routes`, `PUT /v1/routes/{index}`, `DELETE /v1/routes/{index}` — add, replace, or
  remove a route at runtime. The body is the same shape as one `config.yml` route entry; a
  mutation is validated through the exact same loader a hand-edited `config.yml` goes through
  (via `config::editor`) before it's written to disk and hot-applied, so an invalid body is
  rejected without ever touching the real file. A route is identified by its `host` set, not its
  raw position — routes are re-sorted by priority on load, so an index alone isn't a stable
  identity across a reload.

`/reference` and `/openapi.json` are unauthenticated (static docs, not live data); every other
endpoint requires `Authorization: Bearer <token>` when `api.token` is set, same as before.

## Test

```
cargo test              # unit tests + real-socket integration tests (TCP/UDP, no mocks)
cargo build --release   # the release binary CI also builds
```

## Continuous integration

`.github/workflows/rust.yml` (GitHub Actions) runs on any push or pull request:

| Job | What it does |
| --- | --- |
| `test` | `cargo test --locked` on Linux and macOS |
| `build` | `cargo build --release --locked`, uploads the resulting binary as a workflow artifact |

Both jobs cache the cargo registry and build output (`Swatinem/rust-cache`) so most runs only
recompile what actually changed.
