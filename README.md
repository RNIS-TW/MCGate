# MCGate (Rust port)

An in-progress Rust/tokio port of the Java/Kotlin MCGate proxy in the repository root. Same
purpose — a thin, host-based reverse proxy for Minecraft servers — same `config.yml`/
`messages.yml` format, but compiled to a single native binary instead of running on a JVM.

See [`plan.md`](plan.md) for exactly what's ported, what's deliberately out of scope, and how
each piece was verified. Short version: every section is done — config, hot reload, the TCP
relay (handshake/status/login), backend selection strategies, per-route traffic metrics, SQLite
connection tracking and stats logging, the UDP/voicechat relays, the console REPL, and the HTTP
API — except mid-session auto-reconnect-holding (`ReconnectHandler.kt`), which is not ported.

## Build

Requires the Rust toolchain (stable — install via [rustup](https://rustup.rs) if you don't have
it: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`). The `rusqlite` dependency
compiles SQLite from source (`bundled` feature), so a C compiler needs to be on `PATH` — already
true on macOS (Xcode command line tools) and virtually every Linux distro's build tooling; on a
minimal server image install `build-essential` (Debian/Ubuntu) or `gcc` (RHEL/Alpine) first.

```
cargo build --release
```

Produces a single self-contained binary at `target/release/mcgate` — no JVM, no separate runtime,
no other files to ship alongside it (`bundled` SQLite is statically linked in). Copy that one
file to a server and run it; that's the whole deployment.

## Run

```
./target/release/mcgate [config.yml] [messages.yml]
```

Both arguments are optional and default to `config.yml`/`messages.yml` in the current directory;
a missing file is bootstrapped from the built-in defaults on first run, same as the Kotlin build.

```
./target/release/mcgate --help       # usage
./target/release/mcgate --version    # prints the built version
```

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
