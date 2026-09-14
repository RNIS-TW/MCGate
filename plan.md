# MCGate Rust Port — Plan

Porting `src/main/kotlin/me/hippodev/**` (~7,900 lines, Kotlin + Netty) to Rust (tokio).
Goal: functional parity, verified module-by-module (unit tests mirroring the Kotlin test
suite + a real `config.yml` smoke run), not a one-shot unverified dump. No commits until
the user asks.

Legend: [x] done+tested, [~] in progress, [ ] not started.

## 0. Foundations

- [x] Cargo project scaffold (`rust-gate/`), tokio + serde_yaml + anyhow + tracing deps
- [x] `host_pattern.rs` — port of `protocol/HostPattern.kt` (DP glob matcher, ReDoS-safe,
      `$N` capture + `substitute_params`)
- [x] `duration.rs` — port of `parseDuration` / `parseByteSize` from `config/Config.kt`
- [x] `config.rs` — port of `config/Config.kt` (`GateConfig`, `Route`, all nested config
      structs, YAML loading). Verified against the real `run/config.yml`.
- [x] `messages.rs` — port of `config/MessagesConfig.kt` + `MessagesLoader.kt` (messages.yml
      schema, `GateMessages`, default-bootstrap-on-first-run from bundled `default-messages.yml`).
      Verified: parses the real bundled resource + round-trips bootstrap-then-load.
- [x] `config_loader.rs` — port of `config/ConfigLoader.kt` + `MessagesLoader.kt`'s watch halves:
      debounced file watching for hot reload via `notify-debouncer-mini` (one generic watcher
      backing both `watch_config`/`watch_messages`, since the Kotlin versions duplicate the same
      debounce/reload logic per file). `config::load_or_create_default` /
      `messages::GateMessages::load_or_create_default` cover the bundled-default-bootstrap half.
      Verified end-to-end against the real `run/config.yml`: a live edit triggers a debounced
      reload; a bad edit logs a warning and keeps the previous config running (matches Kotlin's
      "reload failed, keeping previous config/messages" behavior) rather than crashing.

## 1. Protocol layer (`protocol/`) — DONE (90 tests, verified against real content)

All byte-level protocol parsing/encoding is ported and unit-tested; nothing here is wired to a
live connection yet (that's section 3). `render_reconnect_text` in `config.rs` — previously a
documented passthrough placeholder blocked on this section — now calls `text_format::to_legacy_text`
for real.

- [x] `varint.rs` — VarInt/frame-length/string read+write. Rust-idiomatic API: pure functions over
      `&[u8]` returning `(value, bytes_consumed)` rather than a mutable-reader-index `ByteBuf`
      analogue, since a partial read needs no rollback when nothing was mutated.
- [x] `minecraft_protocol.rs` — port of `MinecraftProtocol.kt`: handshake/status-response/login-
      start/pong encode+parse, the TCP PROXY-v1-header string encoder. `effectiveRemoteAddress`
      is genuinely connection-state plumbing (a Netty channel attribute), not protocol parsing —
      moved to section 3's scope, see the new note there.
- [x] `compression.rs` — port of `Compression.kt`: zlib (RFC 1950, matching Java's
      `Deflater`/`Inflater` default framing — not raw DEFLATE) packet compression via `flate2`,
      threshold handling, round-tripped in tests both below and above the compression threshold.
- [x] `nbt.rs` — port of `Nbt.kt`'s minimal compound/field writer.
- [x] `text_format.rs` — **scoped-down** port of `TextFormat.kt`. The Kotlin version delegates to
      Adventure (full MiniMessage: hover/click events, fonts, translatable components). Grepping
      this project's actual config/messages usage shows only plain text, named/hex colors,
      decorations, and gradients (e.g. the real `metricsLimitKickMessage` — a gradient wrapping
      bold text) — so this is a from-scratch parser covering exactly that subset, explicitly not
      hover/click/fonts/etc., with unknown tags degrading gracefully (pass through, never panic)
      rather than erroring like real MiniMessage would. Verified against the actual bundled
      `default-messages.yml` content, not just synthetic strings — includes confirming named
      colors round-trip to their short legacy code (`§e`) rather than extended hex, matching
      Adventure's `hexColors()` behavior of only extending genuinely non-named RGB values.
- [x] `status_json.rs` — port of `StatusJson.kt`.
- [x] `reconnect_protocol.rs` — port of `ReconnectProtocol.kt`: the full packet-ID bracket table
      and every encoder (login success/disconnect, config/play keepalive, title/subtitle/action
      bar, transfer, join game, empty chunk). Not independently re-verified against a live
      client — same caveat the Kotlin file itself carries.
- [x] `proxy_protocol_datagram.rs` — port of `ProxyProtocolDatagram.kt`: hand-rolled PROXY v1/v2
      UDP header parsing (v1 line-based, v2 binary with LOCAL/UNSPEC/INET/INET6 handling),
      matching the Kotlin version's exact edge-case behavior rather than pulling in a crate.
- **Deferred to section 3** (not a protocol-parsing concern): `ProxyProtocolInput.kt`'s
  `REAL_REMOTE_ADDRESS`/`effectiveRemoteAddress` is Netty channel-attribute plumbing — "where does
  a connection's PROXY-reported address get stored and read back" — not byte parsing. Its Rust
  shape depends on how section 3 represents a connection at all, so porting it now would mean
  guessing at that design twice.

## 2. DNS cache (`config/DnsCache.kt`) — DONE (97 tests total, verified end-to-end)

Confirmed the assumption flagged above: tokio's resolver genuinely doesn't need the Kotlin
version's bounded-executor-pool-with-`AbortPolicy` machinery, since every connection is its own
task and `.await`ing an uncached lookup only suspends that task. Ported instead: a straight
TTL/negative-TTL/size-capped cache with stale-while-revalidate and idle eviction — the parts that
are still worth having independent of the blocking-thread problem.

- [x] `dns_cache.rs` — `resolve()` (async, cache-or-await-real-lookup),
      `will_resolve_without_blocking()`, `warm()`, and a periodic idle-eviction sweep
      (`spawn_evictor`). `resolve`/`warm` deliberately take `&'static self` — satisfiable only by
      the `dns_cache()` singleton — which makes a real bug caught mid-implementation impossible
      by construction: an early version had background refresh/warm tasks close over the *global*
      singleton internally regardless of which `DnsCache` instance they were called on, so a
      locally-constructed instance's writes would silently land in the wrong place. Requiring
      `&'static self` means there is no "wrong instance" to land in.
- [x] Wired into `config.rs`: `Route::resolve_backends`/`resolve_voicechat`/
      `backends_need_blocking_resolution` (previously not ported at all) and a real
      `warm_static_backends` (previously a documented no-op placeholder) all now go through the
      cache.

Second real bug caught while wiring this up, worse than the first: `load_config` runs both from
inside the tokio runtime (startup) **and** from the config-file watcher's callback, which executes
on `notify-debouncer-mini`'s own background thread — not a tokio task. An ambient `tokio::spawn`
inside `warm()` would panic the first time a hot-reload with a hostname backend actually ran,
despite compiling and passing every unit test cleanly. Fixed by threading an explicit
`tokio::runtime::Handle` (captured once at startup, stored in `AppState`, passed to
`warm_static_backends` on every reload) through to `Handle::spawn` instead. Verified for real, not
just in theory: started an instance against the actual `run/config.yml` (which has a hostname
backend, `buserver.ddns.net`), confirmed DNS warming at startup, then touched the config file to
fire a hot reload on the watcher thread and confirmed it completed without panicking.

## 3. Core TCP proxy pipeline (`handler/`, `Main.kt`) — MVP DONE, verified end-to-end with real protocol bytes

**MCGate is now a real, connectable Minecraft proxy** — not just unit-tested in isolation. Verified
with an actual test harness (a fake Minecraft-protocol backend + a raw socket client, real
varint/handshake/status/login bytes, not mocks) driven against the compiled binary: status ping
correctly dialed and relayed with JSON validated, login correctly relayed as a genuine
bidirectional byte splice in both directions, unmatched-host connections correctly rejected. Every
connection/login/disconnect log line matches the Kotlin version's format.

**Deliberately out of scope for this pass** (see the file-by-file notes below for exactly what):
backend selection is sequential-only (no round-robin/least-connections/lowest-latency/failover-
ordering — section 4); no status-ping caching or synthesized fallback beyond `route.fallback`
(section 4's `ping_cache.rs`); a backend dying mid-session just drops the client — no mid-session
reconnect-hold (`ReconnectHandler.kt` is not ported at all); nothing here populates
`PlayerSessions`, `ConnectionTracker`, or `RouteMetricsStore` (sections 4/5), or registers
voice-chat routing (section 6); `route.metrics` login-limit enforcement isn't checked (section 5).
What IS real: handshake parsing, every anti-abuse guard, inbound/outbound PROXY protocol,
`modifyVirtualHost`, DNS-cached backend resolution, and the actual relay.

- [x] TCP accept loop — `server.rs`'s `run()`, binds and spawns one tokio task per connection.
      `so_backlog` (custom listen backlog) isn't wired — tokio's `TcpListener::bind` uses the OS
      default; would need the `socket2` crate to set a custom one. Minor tuning knob, not
      correctness; noted rather than silently dropped.
- [x] `handshake.rs` — port of `HandshakeSniffer.kt`. Adapted from Netty's incremental
      `ByteToMessageDecoder` (re-invoked as more bytes arrive, reusing the channel's own
      cumulation buffer) to a pure `try_parse(&[u8]) -> ParseOutcome` function plus an async loop
      that reads more and retries — same validity checks (512-byte frame cap, packet id 0,
      255-byte host cap, NUL/trailing-dot stripping).
- [x] `buffered_stream.rs` — **not in the original plan, but necessary**: Netty's cumulation
      buffer means a pipelined client (Handshake immediately followed by Login Start in the same
      TCP segment — common in practice) never loses bytes between decoder invocations. A naive
      port reading a handshake into a throwaway `Vec<u8>` off a raw `TcpStream` would silently
      drop whatever came after it in that same read. This wrapper retains unconsumed bytes and
      feeds them back out through its own `AsyncRead` before touching the underlying stream again,
      so framed reads (handshake, Login Start) and the later raw relay (`copy_bidirectional`)
      compose correctly with no special-casing. Caught and fixed before it could cause an
      intermittent, load-dependent bug that unit tests in isolation would never have exercised —
      verified with a dedicated test that pipelines two packets into one read and confirms both
      are recovered, plus a byte-at-a-time reader test for the fragmented-read direction.
- [x] `connection_guard.rs` — port of `ConnectionGuardHandler.kt`'s `PreLoginConnections` +
      guard logic, adapted from Netty lifecycle callbacks to an RAII guard (`PreLoginGuard`):
      acquired at connection start, `.done()` called once Login Start arrives (frees the per-IP
      slot early, matching `PRELOGIN_DONE`), `Drop` releases whatever's still held otherwise. The
      login deadline itself is a `tokio::time::timeout` wrapping the handshake read in `server.rs`
      rather than a separate scheduled task, since there's no separate "deadline handler" to own
      it the way a Netty pipeline stage would.
- [x] `flood_control.rs` — port of `FloodControl.kt`: `GlobalConnections`
      (+ `GlobalConnectionGuard` RAII wrapper), `ConnectionRates` (fixed-window per-IP rate limit,
      periodic stale-window sweep), and `HeldReconnectSessions`'s counter/cap (wired to
      `max_held_reconnect_sessions` from config; not yet consumed by anything since
      reconnect-hold itself isn't ported).
- [x] `proxy_protocol_tcp.rs` — the TCP-stream counterpart to `proxy_protocol_datagram.rs`'s pure
      parser (mirrors what `netty-codec-haproxy`'s `HAProxyMessageDecoder` +
      `ProxyProtocolAttributeHandler.kt` do together): reads exactly as many bytes as the v1/v2
      header declares before handing off to the shared byte-level parser, so it never over-reads
      into the handshake that follows. `ProxyProtocolInput.kt`'s `effectiveRemoteAddress` channel-
      attribute concept doesn't need a separate port — `server.rs` just threads `effective_addr`
      through as a plain local variable, since there's no long-lived "channel" object here to
      attach it to.
- [x] Status ping handling (`server.rs::handle_status` + `fetch_backend_status`) — scoped-down
      port of `StatusHandler.kt`: dials backends sequentially, forwards the connecting client's
      real protocol version, validates the backend's JSON (`version.protocol` present and
      numeric) before ever relaying it to a player, serves `route.fallback` if every backend
      fails, answers the trailing Ping/Pong. **Not ported**: `PingCache` (every request dials
      live — functionally correct, just not load-reducing) and `runtime.tryBeginStatusDial`'s
      concurrent-dial cap (section 4).
- [x] Login relay (`server.rs::handle_login` + `dial_backend` + `relay`) — scoped-down port of
      `LoginRelayHandler.kt`: defers the backend dial until Login Start actually arrives (same
      anti-flood rationale as the Kotlin version), sequential failover across
      `route.backend_templates`, outbound PROXY protocol, `modifyVirtualHost`, then a genuine
      bidirectional splice via `tokio::io::copy_bidirectional`. **Not ported**: backend login
      sniffing for compression-threshold/encryption detection (`BackendLoginSniffer` — only
      useful for reconnect-hold, which isn't ported either), mid-session reconnect-hold,
      `PlayerSessions`/`ConnectionTracker`/`RouteMetricsStore`/`VoiceRouting` integration.
- [x] Flow control — **confirmed a non-issue, nothing to port**: `FlowControl.kt` exists because
      Netty's `writeAndFlush` is fire-and-forget unless something explicitly watches
      `isWritable` and throttles the peer. `tokio::io::copy_bidirectional`'s poll-based design
      only reads more from one side when the other side's writer is actually ready to accept
      more — equivalent backpressure by construction, not something to hand-replicate.
- [x] Graceful shutdown — `main.rs` aborts the accept-loop and API tasks on Ctrl+C. Simpler than
      Kotlin's `stopped: AtomicBoolean` + shutdown-hook dance (no risk of two paths racing to shut
      down the same executor) but also cruder: in-flight relayed connections are dropped rather
      than drained. Revisit if a clean-drain shutdown matters in practice.
- **Not ported at all**: `reconnect_handler.rs` (`ReconnectHandler.kt` — mid-session/all-backends-
  down reconnect-hold: retry/backoff loop, title/subtitle/actionbar animation ticks, transfer once
  a backend recovers). This is a genuinely large, separate feature (NBT world join, keepalive
  loop, animation timer, packet-ID-bracket-gated) rather than core relay plumbing — tracked as its
  own future increment, not bundled into this MVP.

Two real bugs caught and fixed while building this (beyond the buffered-stream one above), neither
of which would have surfaced from unit tests in isolation — both found by writing this section's
plan.md entry honestly and asking "would this actually work," then verifying with the real
end-to-end harness:
1. `dial_backend` forwarded the handshake to the newly-connected backend but never forwarded the
   client's actual Login Start packet — the backend would have hung forever waiting for it.
2. An early draft passed `Route` by value into `dial_backend` via `*route`, which doesn't compile
   (`Route` isn't `Copy`) — caught by the compiler, not silently "fixed" by cloning without
   thinking about why a clone was suddenly needed.

## 4. Routing / backend selection (`routing/`) — DONE except `connection_tracker`/`stats_logger`'s SQLite (section 5)

Verified end-to-end against the compiled binary (not just unit tests): a live login session shows
up in `GET /v1/players` while connected, a console `kick` genuinely severs that live TCP
connection, and — since it shares the same `RouteRuntime` — `active`/`latencyMillis` in
`GET /metrics` and the `routes` console command now reflect real connections, not zeros.

- [x] `backend_selector.rs` — port of `BackendSelector.kt`'s `orderBackends`: all five strategies
      (sequential, random, round-robin, least-connections, lowest-latency). Failover itself (try
      the next address on connect failure) lives in `server.rs`'s dial loop, same split as the
      Kotlin version (`orderBackends` picks the order, the caller loops).
- [x] `route_runtime.rs` — `state::RouteRuntime` extended to the full Kotlin shape: round-robin
      cursor, TTL'd latency readings, in-flight status-dial limiter
      (`MAX_CONCURRENT_STATUS_DIALS`). **Real bug caught and fixed here**: since this now holds
      live active-connection counts, the reload path's old behavior (recreate an empty
      `RouteRuntime` per route index on every reload) would have silently zeroed active-connection
      counts for routes that hadn't even changed. Fixed by carrying runtime state forward via
      structural `Route` equality (`app_state.rs::set_config`), matching Kotlin's `GateState`
      — this was a latent regression waiting for exactly this section to land, not something the
      earlier section 7 code could have caught on its own.
- [x] `backend_pinger.rs` — port of `BackendPinger.kt`: shared by the API's live
      `GET /v1/routes/{i}/ping` (no longer `501`, now a genuine on-demand dial with a 4-slot
      concurrency limiter matching Kotlin's `livePingLimiter`) and reusable independently of
      `server.rs`'s own status-dial path.
- [x] `ping_cache.rs` — port of `PingCache.kt`: TTL response cache, negative "known down" cache,
      periodic sweep, overflow eviction. Wired into `server.rs`'s status handling so a status
      flood no longer dials the backend on every request.
- [x] `player_sessions.rs` — `state::PlayerSessions`/`PlayerSession`, now genuinely populated by
      `server.rs` on every real login (and removed on disconnect), with live packet/byte counters
      updated as bytes actually flow through the relay (see section 5's custom relay loop below).
      `kick` really disconnects a live session now, via `PlayerSession::disconnect` (a
      `tokio::sync::Notify` the relay task selects on alongside the byte splice).
      **Update**: `kick <player> <message>` and `transfer` are both implemented now too —
      `server::sniff_backend_login` (a scoped, from-scratch replacement for `BackendLoginSniffer`,
      not a port of it: it only watches Login-state packet IDs 0x00-0x03, which are stable across
      every version, and deliberately stops the instant Login Success arrives rather than trying
      to track Configuration/Play IDs, which shift release to release) learns the real
      compression threshold and whether the session is encrypted during login, and
      `PlayerSession.pending_action` + `relay`'s `disconnect`-notified branch (`server.rs`) does
      the actual packet write. Both fall back to a plain, message-less disconnect for a session
      that turns out encrypted or on an unverified protocol version, rather than risk corrupting
      the stream.
- [x] `live_metrics.rs` — `state::collect_metrics` now actually resolves non-wildcard routes'
      backends and reads real `RouteRuntime`/`RouteMetricsStore` data, instead of hardcoding
      zero/empty. Made `async` (it now awaits DNS resolution) — updated its one caller
      (`api_server.rs`) and its test accordingly.

## 5. Tracking / metrics (`tracking/`) — DONE, verified against real `.db` files

Crate decision made: `rusqlite` with the `bundled` feature (compiles SQLite from source, no
system-library dependency — confirmed it actually builds clean here). `connection_tracker.rs`'s
writer is a dedicated `std::thread` (not a tokio task) since SQLite access is blocking I/O and
only one thread may own the connection; `stats_logger.rs`'s writer is the same shape but bridges
back into the tokio runtime via a stored `Handle::block_on` to call the now-`async`
`collect_metrics`. Verified end-to-end: ran a real login session through the compiled binary with
both features enabled, then opened the resulting `connections.db`/`stats.db` files with an
independent `sqlite3` connection (not the app's own) and confirmed real rows — a completed
session record and multiple periodic snapshots.

- [x] `route_metrics_store.rs` — port of `RouteMetricsStore.kt`: per-route upload/download byte
      counters, atomic-write-then-rename JSON persistence off the hot path, byte-limit enforcement
      (refuse a new login inline in `server.rs`; kick players already over via a periodic ticker
      that also flushes counters and applies rolling auto-resets), totals surviving a restart.
      Verified end-to-end: configured a 500-byte upload limit, pushed a session past it, confirmed
      a *new* login is refused with a real Login Disconnect packet (not just a dropped
      connection) — the exact mechanism a real deployment would use this feature for.
  - This is also why `server.rs`'s relay is a custom loop rather than
    `tokio::io::copy_bidirectional`: byte counters need to see bytes as they flow (for both
    per-session live stats and timely limit enforcement on long-lived connections), not only a
    final total after the connection ends. Verified the custom loop preserves
    `copy_bidirectional`-equivalent backpressure — each direction only reads more once its own
    `write_all` returns — via the same section-3 end-to-end harness (still a clean bidirectional
    splice) plus the new byte-counting-specific tests above.
- [x] `connection_tracker.rs` — port of `ConnectionTracker.kt`: `crossbeam-channel` bounded
      channel (chosen over `std::sync::mpsc` specifically for its `.len()`, needed for the
      `queuedRecords` metric) with `try_send`/drop-on-full semantics, a dedicated writer thread
      batching commits, retention + row-cap pruning interleaved into the same thread's loop (one
      `Connection`, one writer — SQLite only allows one anyway). **Real bug caught before it could
      ship**: an early draft had the writer thread's queued-count bookkeeping call the *global*
      `connection_tracker()` singleton internally instead of using the `self` it was spawned
      from — the exact same class of bug `dns_cache.rs` was redesigned around earlier in this
      port. Fixed by requiring `&'static self` on `apply_config`/`start_internal` and threading
      that same reference into the writer thread explicitly, so there is no "wrong instance" for
      it to write into.
- [x] `stats_logger.rs` — port of `StatsLogger.kt`: periodic snapshot write + prune on a
      dedicated writer thread, same shape as `connection_tracker.rs` minus the producer/consumer
      queue (only one producer: the timer tick itself). `AppState` needed a self-referential
      `Weak` handle (`self_weak`, set right after construction) so `set_config` — which only ever
      had `&self` — can hand `stats_logger` the `Arc<AppState>` it needs to call the async
      `collect_metrics` via `block_on`; a standard pattern for this, not a workaround.

## 6. UDP relays (`udp/`, `voice/`) — DONE, verified end-to-end with real sockets

Verified against the compiled binary: a real UDP client through the static `udpProxy:` forward,
and a real voicechat UDP session — registered by an actual TCP login, not a test shortcut — relayed
to a fake voice backend, both directions, over real loopback sockets.

- [x] `udp_throttle.rs` — port of `UdpThrottle.kt`: process-wide session caps (total, per-IP),
      pending-packet buffering cap (see `udp_proxy.rs`'s note on why this ended up unused there),
      idle timeout, no-reply-teardown — shared by both UDP paths below, hot-reloadable via
      `app_state.rs`.
- [x] `udp_proxy.rs` — port of `UdpProxy.kt`: standalone static UDP forward, NAT-like
      per-source-session backend socket, session teardown on idle/no-reply. **Simpler than the
      Kotlin version by construction, not by omission**: Netty's `Bootstrap.connect()` on a
      datagram channel is scheduled asynchronously purely for API consistency (UDP "connect"
      itself never touches the network), which is why the Kotlin version needs a `pending` queue
      to hold datagrams arriving mid-connect. `tokio::net::UdpSocket::connect()` completes
      immediately, so awaiting it before returning from the packet handler means a session's
      backend socket always exists before any later datagram for that session is processed —
      there's no pending-queue race to guard against, so `UdpThrottleConfig.pending_packets_per_session`
      is accepted (for config-schema compatibility) but has nothing to bound here.
- [x] `voice_routing.rs` — port of `VoiceRouting.kt`: maps a TCP-login client IP → its voicechat
      backend, idle eviction, and a disconnect hook (a plain callback, not a stored `VoiceRelay`
      reference — avoids a circular module dependency) so a player logging out tears down their
      live voice session immediately rather than waiting on the relay's own idle timeout.
- [x] `voice_relay.rs` — port of `VoiceRelay.kt`: shares the main TCP bind port, full
      PROXY-protocol-aware datagram handling (header on a flow's first packet only, `by_via`
      re-matching for header-less follow-ups, re-pointing `via` if the fronting proxy's source
      rotates), session lifecycle via `udp_throttle`. Wired into `server.rs`'s login path
      (`register_voicechat_route`) and disconnect path (`voice_routing().unregister`).
  - **Two real bugs caught by the compiler, not by review**: both `udp_proxy.rs` and
    `voice_relay.rs` initially matched a `Mutex::lock()`'s result inline inside an `if let`
    (`if let Some(x) = mutex.lock().unwrap().get(...).cloned() { ...await... }`) — Rust's
    temporary-lifetime-extension rules keep that `MutexGuard` alive for the whole `if let` block,
    not just the condition, so the guard was held across an `.await` point, making the containing
    future `!Send` and refusing to compile under `tokio::spawn`. Fixed by binding the lock result
    to a `let` first in each case, which drops the guard at the end of that statement instead.
  - **One real flaky test caught by running the suite repeatedly, not by a single green run**:
    `voice_relay.rs`'s two original tests both used the global `voice_routing()` singleton keyed
    by client IP, and every loopback test client is `"127.0.0.1"` — so cargo's default parallel
    test execution let one test's `register`/`unregister` land mid-flight of the other,
    intermittently failing neither test's own logic was actually wrong about. Fixed by merging
    them into one sequential test (register → verify relay → unregister → verify drop), which
    makes cross-test interleaving impossible rather than papering over it. Confirmed fixed with 6
    consecutive clean full-suite runs, not just one.

## 7. Operational surface — DONE (verified end-to-end; see caveats below)

Everything in this section is implemented and was verified against a real running instance, not
just unit tests: started the binary with a copy of the real `run/config.yml`/`messages.yml`,
issued live `curl` requests against every HTTP endpoint, and fed real commands through the
console over stdin (`MCGATE_PLAIN_CONSOLE=true`). Two structural pieces (`app_state.rs`,
`state.rs`) were added ahead of their planned sections (4/5) purely as scaffolding this section
needed — see the `[~]` entries in section 4 above for exactly what's real vs. still pending there.

- [x] `api_server.rs` — port of `ApiServer.kt` using `axum`. All endpoints implemented with
      identical JSON field names/shapes (camelCase) to the Kotlin version: `GET /metrics`
      (Prometheus text + `?type=json`), `GET /v1/routes`, `GET /v1/routes/{i}`,
      `GET /v1/routes/{i}/backends`, `GET /v1/routes/{i}/metrics`, `GET /v1/players` (paged).
      Bearer-token auth (`api.token`) checked on every route. `GET /v1/routes/{i}/ping` correctly
      returns `501 Not Implemented` — a live backend dial needs section 3's dialer, and returning
      a fake reading would be worse than admitting it's missing. Verified: real `curl` requests
      against a running instance for every endpoint including 404/501/auth paths.
- [x] Console REPL — port of `startConsole` + all commands (`help`, `players`, `whois`, `kick`,
      `transfer`, `routes`, `metrics [reset ...]`, `reload`, `uptime`, `version`,
      `stop`/`shutdown`/`exit`) in `console.rs`, using `rustyline` (JLine's closest analogue —
      line editing/history, an external-print hook wired to `logging::set_active_line_printer` so
      a log line mid-command doesn't corrupt the prompt) with the same `MCGATE_PLAIN_CONSOLE`
      plain-stdin fallback. `kick`/`transfer` correctly report "no such player" for now — see the
      `player_sessions.rs` note in section 4. Verified: real commands piped over stdin against a
      running instance.
- [x] Logging setup — port of `ColorConsoleAppender.kt` + `ConsoleColors.kt` (level/structure
      colorizing, `NO_COLOR`/`FORCE_COLOR` detection, the bounded drop-on-backpressure async
      console queue) and `LogArchiver.kt` + `LineCountTriggeringPolicy.kt` (gzip-archive the
      previous run's log, prune to 7; roll `latest.log` every 10,000 lines to `latest.N.log.gz`,
      capped at 7) in `logging.rs`, wired into `tracing-subscriber` via custom `MakeWriter`s and
      event formatters rather than a custom logback appender class. Verified: colorized/plain
      output shape, gzip archiving, and line-count rolling all covered by tests exercising real
      files/dirs (not mocked), plus real output observed from a running instance.
- [x] Runtime log-level control from `logLevel` config (hot-reloadable) — `LogLevelHandle` in
      `logging.rs`, backed by `tracing_subscriber::reload`, scoped to the `mcgate` crate's own
      targets only (mirrors `applyLogLevel`'s deliberate scoping away from the root logger).
      Wired into `AppState::set_config` so a config reload re-applies it, matching `Main.kt`.
- [x] Startup diagnostics — port of `logNetworkInterfaces()` in `network_info.rs` using the
      `if-addrs` crate. Verified: real interface list observed from a running instance.
- [x] Version reporting — `version.rs`. Simpler than the Kotlin version: Cargo always bakes
      `CARGO_PKG_VERSION` into the binary, so the jar-manifest-vs-unpackaged distinction that
      motivated Kotlin's `"dev"` fallback doesn't exist for a compiled Rust binary.

Bug caught and fixed while wiring this up: `config::parse_host_port` only accepted IP literals
(`SocketAddr`'s own parser), which would have made the *default* API bind address
(`"localhost:8080"`) fail at startup. Fixed to fall back to `ToSocketAddrs` resolution, matching
Kotlin's `InetSocketAddress(host, port)` constructor — caught by a test added specifically for
this (`parse_host_port_resolves_hostnames_like_localhost`), not by inspection.

## 8. Cross-cutting / polish — DONE except the load/soak comparison

- [x] Full hot-reload wiring: `app_state.rs::set_config` is the single funnel every reload path
      goes through (file watcher, messages watcher, console `reload`) and fans out to every
      live-reconfigurable subsystem — log level, DNS pre-warming, `route_metrics_store`,
      `connection_tracker`, `udp_throttle`, and `stats_logger` (via the `self_weak` handle added
      in section 5). `RouteRuntime` carry-forward across reload (structural `Route` equality) was
      added and verified in section 4 once it started holding live data. Nothing left here.
- [x] Memory/allocator tuning note: confirmed, not just assumed — tokio has no per-thread
      buffer-cache/arena concept that scales off a container's misreported CPU count the way
      Netty's pooled allocator does, so `tuneNettyMemoryFootprint()`'s equivalent genuinely isn't
      needed. Documented in `README.md`'s memory section rather than left as a code comment only.
- [x] End-to-end integration test: `tests/e2e.rs` — spawns the actual compiled binary (via
      Cargo's `CARGO_BIN_EXE_mcgate`, not a library call) against a real fake-backend TCP
      listener and drives genuine protocol bytes (status ping, full login relay, unmatched-host
      rejection) over real loopback sockets. This is the automated version of the ad hoc
      verification scripts used while building sections 3-6 — now part of `cargo test`, so CI
      exercises real connectable-proxy behavior on every push, not just isolated unit tests.
      Deliberately black-box (talks only over sockets) so an internal refactor that breaks real
      wire behavior can't hide behind still-passing unit tests.
- [ ] Load/soak test comparison vs. the Kotlin build (throughput, memory, CPU). **Not done** —
      this needs actual measurement on comparable hardware/load, not something to estimate or
      fabricate. This is the real answer to "was Rust worth it" that the rest of this port has
      been deferring; worth doing once someone wants to actually make that call.
- [x] Packaging/deployment story: a single static binary (`cargo build --release`, tuned with
      `lto`/`codegen-units=1`/`strip` — 5.4 MB, no JVM/runtime to install) plus a systemd unit
      example and memory-tuning guidance, documented in `README.md`. Deliberately *not*
      `panic = "abort"` in the release profile despite the smaller binary it'd give: tokio
      isolates a panicking task's failure to that one connection by default (panic = unwind), and
      `abort` would turn one bad packet triggering a bug in a single connection handler into a
      whole-process crash taking down every other player — the opposite of what
      `ConnectionGuardHandler`'s per-connection isolation was for in the Kotlin version.
- [x] GitHub Actions CI: `.github/workflows/rust.yml`, separate from the Kotlin build's
      `Jenkinsfile` at the repo root, scoped to `rust-gate/**` via path filters. `test` job runs
      `cargo test --locked --all-targets` (unit tests + the new `tests/e2e.rs`) on Linux and
      macOS; `build` job runs `cargo build --release --locked`, smoke-tests `--version`/`--help`,
      and uploads the binary as a workflow artifact. Verified by running the exact CI commands
      locally before considering this done, not just eyeballing the YAML.
  - Caught and fixed a real latent CI-breaker in the process: `rust-gate/.gitignore` had
    `Cargo.lock` listed, which is the wrong convention for a binary crate (vs. a library) and
    would have made every `--locked` CI build fail the moment this got committed, with no
    lockfile to check against. Fixed the `.gitignore` rather than dropping `--locked` from CI.

## Design decisions still open (flag before implementing)

1. **Flow control / backpressure model** — Netty's explicit water-mark callback doesn't have a
   direct tokio equivalent; likely candidates are (a) `tokio::io::copy_bidirectional` and trust
   the OS socket buffers + TCP backpressure naturally, or (b) a manual bounded-channel relay loop
   giving explicit control matching the Kotlin 32KiB/64KiB marks. (a) is simpler and may be
   sufficient — worth testing before reimplementing (b).
2. **SQLite crate** — `rusqlite` (sync, needs `spawn_blocking`) vs. `sqlx` (async native). Kotlin
   already runs SQLite writes on a dedicated non-event-loop thread, so `rusqlite` +
   `spawn_blocking`/a dedicated writer task is the closer port; `sqlx` may be nicer long-term.
3. **HTTP framework for the API server** — `axum` is the natural default; confirm no constraint
   against adding it as a dependency.
4. **Console line-editing crate** — `rustyline` is the closest analog to JLine; confirm it can
   coexist with `tracing` log output the way the Kotlin `printAbove`-based integration does.
5. **PROXY protocol parsing** — hand-roll vs. pull in the `ppp` crate; hand-rolling keeps parity
   with the exact Kotlin decoder's error/edge-case behavior (`ProxyProtocolInput.kt`), a crate is
   less code to maintain. Lean hand-rolled given how much this proxy already leans on exact
   parity with the Kotlin edge cases.

## Working agreement

- No `git commit`/`git push` on this port without explicit request (current branch is local
  `dev/go`, not yet pushed).
- Each module above gets ported with tests mirroring its Kotlin counterpart in
  `src/test/kotlin/me/hippodev/**` before moving to the next, and re-verified against a real
  `config.yml`/`messages.yml` where relevant — not a bulk unverified dump.
- Kotlin project stays untouched and buildable throughout; this lives entirely in `rust-gate/`
  until/unless a full cutover is decided.
