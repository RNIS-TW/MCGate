# MCGate Hardening Plan

Resilience / RAM / DDoS review follow-up. Ordered by severity. Each item lists the
problem, the fix, files to touch, and how to verify.

Legend: **P0** = a single packet/request can kill the process or an event-loop thread ·
**P1** = no ceiling on flood damage · **P2** = correctness / efficiency under load · **P3** = polish.

---

## P0-1 — Bound frame lengths in every decoder ✅ DONE

**Problem.** Only `HandshakeSniffer` caps the declared frame length (512 B). Every other
`ByteToMessageDecoder` reads a length varint and lets Netty's cumulation buffer grow to
whatever the peer declared (up to ~2 GiB): `StatusHandler.decode`, `PingPongHandler`,
`BackendStatusFetcher`, `ReconnectHandler.decode`, `BackendLoginSniffer`,
`BackendLoginRelay`, and the `pingBackendLive` inline decoder. A slow-loris that declares
a 100 MB frame and dribbles bytes pins ~100 MB of pooled-direct memory per connection.

**Fix.**
- Add `const val MAX_PACKET_BYTES = (1 shl 21) - 1` (Minecraft's real max) to
  `protocol/MinecraftProtocol.kt`.
- In each decoder, right after reading `length`: `if (length < 0 || length > MAX_PACKET_BYTES) { ctx.close(); return }`.
- Consider a helper `readFrameLength(buf): Int?` that encapsulates read + bound + reset-on-incomplete
  so the six call sites stay identical.

**Files.** `handler/StatusHandler.kt`, `handler/ReconnectHandler.kt`,
`handler/LoginRelayHandler.kt` (BackendLoginSniffer), `routing/BackendPinger.kt`,
`protocol/MinecraftProtocol.kt`.

**Verify.** New test: feed a decoder a varint declaring `Int.MAX_VALUE` and assert the
channel closes without allocating. Extend `BufferLifecycleTest`.

---

## P0-2 — Bound `dataLength` before allocating in `readCompressedFrame` ✅ DONE

> Also fixed: `inflate()` spun forever (`while` loop, `inflate()` returns 0) on a truncated
> zlib stream — now breaks on a zero-progress read.


**Problem.** `readCompressedFrame` reads `dataLength` from the peer and `inflate()` does
`ByteArray(expectedSize)` with no bound. A client held in the reconnect-wait state (or a
compromised backend via `BackendLoginSniffer`) sends `dataLength = Integer.MAX_VALUE` →
`OutOfMemoryError` → with the README-recommended `-XX:+ExitOnOutOfMemoryError` the whole
process exits and every player drops.

**Fix.**
- In `readCompressedFrame` (`protocol/Compression.kt`): after `val dataLength = readVarInt(buf)`,
  `require(dataLength in 0..MAX_PACKET_BYTES)` — throw a checked/typed exception the callers
  already catch (they wrap in `try { ... } catch (e: Exception) { -1 }`).
- Also cap the compressed-input size (`frameEnd - buf.readerIndex()`) the same way.

**Files.** `protocol/Compression.kt` (+ callers already handle the failure path).

**Verify.** Unit test on `readCompressedFrame` with an oversized `dataLength`; assert it
throws rather than allocating. Add to a new `CompressionTest`.

---

## P0-3 — Fix ReDoS in `HostPattern` ✅ DONE (linear DP matcher, greedy captures preserved)

**Problem.** `*` → `(.*)`; `HostPattern.match()` runs on the event-loop thread in
`dispatch()` for every connection. A multi-wildcard pattern (`*-*-*.example.com`) against a
crafted 255-char non-matching host backtracks catastrophically — one handshake pins an
event-loop thread at 100% CPU, stalling every player on that thread.

**Fix (pick one).**
- **Preferred:** replace the regex with a linear glob matcher (two-pointer `*`/`?` match),
  capturing wildcard spans for `$1..$N`. No backtracking possible.
- **Cheaper:** wrap each `*` as an atomic group `(?>(.*))` / possessive where capture allows,
  and hard-cap `wildcardCount` (e.g. reject >4 at config load) plus keep the 255-char host cap.

**Files.** `protocol/HostPattern.kt`; config-load validation in `config/Config.kt`.

**Verify.** Test: pattern `*-*-*-*.x` against `"a".repeat(250)` completes in <10 ms.

---

## P1-4 — Global connection ceiling + per-IP connection-rate throttle

**Problem.** `maxConnectionsPerIp` defaults to `0` (off) and is disabled under
`proxyProtocol`. No process-wide max-connections limit, no "new connections per IP per
second" throttle. Under a distributed flood (or one arriving via the upstream LB): fd
exhaustion (`SO_BACKLOG` hard-coded 128), and one retained 30 s `ScheduledFuture` + closure
per connection piling on the event loops.

**Fix.**
- `GateConfig`: add `maxConnections: Int = 0` (0 = unlimited) and
  `connectionThrottle: { perIp: Int, perSeconds: Int }` (e.g. 6 per 8 s, BungeeCord-style).
- Shared `AtomicInteger` current-connection count; reject in the `ChannelInitializer`
  before adding handlers when over `maxConnections`. Decrement on `channelInactive`.
- Per-IP sliding-window / token-bucket rate limiter (new `handler/ConnectionThrottle.kt`),
  checked in `ConnectionGuardHandler.channelActive` alongside the existing per-IP cap.
- Make `SO_BACKLOG` configurable (`GateConfig.soBacklog: Int = 128`).
- Change defaults: `loginTimeout` 30 s → 10 s; `maxConnectionsPerIp` 0 → 8 (document the
  behavior change in README).

**Files.** `config/Config.kt`, `Main.kt` (bootstrap + initializer),
`handler/ConnectionGuardHandler.kt`, new `handler/ConnectionThrottle.kt`, `README.md`,
`src/main/resources/default-config.yml`.

**Verify.** Extend `ConnectionGuardTest`: open N+1 connections from one IP within the
window, assert the last is dropped immediately; assert global cap rejects at the initializer.

---

## P1-5 — Status path: stop it being a backend-amplification vector

**Problem.** `StatusHandler` dials the backend on every cache miss, and the cache key
includes `protocolVersion` — an attacker cycling the protocol version misses the cache
every time, one backend dial per request, no per-IP or per-backend concurrency limit.

**Fix.**
- Drop `protocolVersion` from the response-cache key; cache per `host:port`. If
  version-multiplexing backends matter, rewrite only the `version` object in the cached
  JSON per client instead of re-dialing.
- Per-backend in-flight-status-dial cap (`Semaphore` / counter in `RouteRuntime`); over the
  cap, serve fallback/close instead of dialing.
- Per-IP status-request rate limit (reuse the P1-4 throttle infra).

**Files.** `handler/StatusHandler.kt`, `routing/PingCache.kt`, `routing/BackendSelector.kt`
(RouteRuntime).

**Verify.** `PingCacheTest`: 100 status requests with distinct protocol versions during a
`cachePingTTL` window trigger exactly one backend dial.

---

## P1-6 — Defer / off-load DNS on the status path; cache negative lookups

**Problem.** First `DnsCache.resolve()` for any hostname blocks the calling thread.
`LoginRelayHandler` defers this until Login Start; `StatusHandler` and `dispatch()`'s
`nextState == 1` branch resolve eagerly on the event loop. `InetSocketAddress(host, port)`
returns an *unresolved* address on failure (never throws) and that gets cached 60 s.

**Fix.**
- In `DnsCache.resolve`, detect `addr.isUnresolved` and either don't cache it or cache with
  a short negative-TTL (5 s) distinct from the 60 s success TTL.
- For the status path: resolve via the `DnsCache` background executor and continue the
  handler on completion, rather than calling `resolveBackends` inline. Or gate eager status
  resolution behind "template has no `$N`" (static routes are already warmed at load).

**Files.** `config/DnsCache.kt`, `handler/StatusHandler.kt`, `Main.kt` (`dispatch`).

**Verify.** Point a route at an unresolvable host; assert the event loop isn't blocked
(status returns fallback promptly) and the failure isn't cached for 60 s.

---

## P1-7 — Gate + cap UDP sessions in `UdpProxy`

**Problem.** `UdpProxy` opens a `Session` **and a backend-facing datagram socket (fd)** per
distinct source `ip:port`, no gating. UDP sources are spoofable → unbounded sessions/fds
far faster than the 5-min idle reaper collects. (`VoiceRelay` is fine — it's gated by a
live TCP login via `VoiceRouting`.)

**Fix.**
- Global session cap (`MAX_SESSIONS`, e.g. 4096) — drop new datagrams when full.
- Per-source-IP session cap.
- New-session creation rate limit.
- Shorten the "no backend reply yet" teardown: if a session's backend never replies within
  N seconds, evict it early instead of waiting `SESSION_IDLE_MILLIS`.

**Files.** `udp/UdpProxy.kt` (mirror constants/pattern into `voice/VoiceRelay.kt` only if
we ever remove its login gate).

**Verify.** New `UdpProxyTest`: fire datagrams from `MAX_SESSIONS + 100` distinct fake
senders, assert session map and open-channel count stay bounded.

---

## P2-8 — Replace global `SecureRandom` in `orderBackends`

**Problem.** `Strategy.RANDOM` calls `secureRandom.nextLong()` per connection;
`SecureRandom` is synchronized → serializes dispatch across all event loops under a flood.

**Fix.** `backends.shuffled(java.util.Random(ThreadLocalRandom.current().nextLong()))`, or
shuffle with `ThreadLocalRandom` directly. Cryptographic randomness isn't needed for load
balancing.

**Files.** `routing/BackendSelector.kt`.

**Verify.** Existing behavior tests still pass; micro-bench optional.

---

## P2-9 — Cap the reconnect-hold state

**Problem.** `reconnect.maxWait` defaults to `0` (unlimited). On an offline-mode server
with `reconnect.enabled`, a login flood during a backend blip promotes every bot to a
permanently-held session with a 500 ms animation timer + 15 s keepalive + retry timer +
`PlayerSession` entry. Nothing checks client `isWritable` during the wait.

**Fix.**
- Default `maxWait` to a finite value (e.g. `5m`); document.
- Global cap on concurrently-held reconnect sessions; past it, kick with `kickMessage`
  instead of holding.
- In `ReconnectHandler`, skip animation/keepalive writes when `!ctx.channel().isWritable`.

**Files.** `config/Config.kt`, `handler/ReconnectHandler.kt`, `handler/LoginRelayHandler.kt`
(the `holding for reconnect` branches), `README.md`.

**Verify.** Extend `KickMessageStressTest` / new test: hold `cap + 10` sessions, assert the
excess are kicked, not held.

---

## P2-10 — Preserve `RouteRuntime` across config reload

**Problem.** Each reload builds a fresh `GateState` with an empty `routeRuntimes` map:
active-connection counts reset to 0 → `least-connections` misroutes and `/metrics`
under-reports until players reconnect.

**Fix.** On reload, carry forward `RouteRuntime` for routes that still exist (match on
structural `Route` equality, which data-class already gives). Move `routeRuntimes` out of
`GateState` into a longer-lived holder keyed by a stable route identity, or copy matching
entries into the new map in `reloadAll` / the watch callbacks.

**Files.** `Main.kt`.

**Verify.** Connect a player, trigger a reload, assert `activeConnections` for their
backend is still 1.

---

## P2-11 — API auth + bounded player listing

**Problem.** No authentication. `/v1/routes/{i}/ping` = unauthenticated on-demand backend
dialing. `/v1/players` and `/metrics?type=json` build a multi-MB string on the event loop
at high player counts.

**Fix.**
- `ApiConfig.token: String?`; when set, require `Authorization: Bearer <token>`, else 401.
- Paginate `/v1/players` (`?limit=&offset=`); cap default response size.
- Rate-limit `/v1/routes/{i}/ping`.

**Files.** `api/ApiServer.kt`, `config/Config.kt`, `README.md`.

**Verify.** New `ApiServerTest`: request without token → 401; with token → 200; players
endpoint respects `limit`.

---

## P3 — Polish

- **IPv6 in `parseHostPort`.** `lastIndexOf(':')` breaks `[::1]:25565`. Parse bracket form.
  `config/Config.kt`.
- **`DnsCache` refresh executor** has an unbounded work queue; give it a bounded queue +
  `CallerRunsPolicy` or `DiscardPolicy`. `config/DnsCache.kt`.
- **Configurable Netty direct-arena count** (`GateConfig.directArenas`), for busy
  deployments that want throughput over the current 2-arena memory floor. `Main.kt`.
- **Encoder allocations.** Reconnect animation path allocates a fresh `Unpooled.buffer()`
  every 500 ms per held player; reuse a pooled allocator or pre-encode static frames.
  `protocol/ReconnectProtocol.kt`.
- **Shut down the stray reapers** (`PingCache`, `DnsCache`, `VoiceRouting`) on shutdown for
  consistency with `ConnectionTracker` / `StatsLogger`. Harmless today (`halt(0)`).

---

## Suggested sequencing

1. **PR 1 — process-kill bugs:** P0-1, P0-2, P0-3. Small, high-value, well-testable.
2. **PR 2 — flood ceiling:** P1-4, P1-5, P1-6. The core DDoS story; ship with README updates.
3. **PR 3 — UDP + reconnect:** P1-7, P2-9.
4. **PR 4 — correctness/perf:** P2-8, P2-10, P2-11.
5. **PR 5 — polish:** P3 batch.

## Testing notes

- `mvn test -DskipTests=false` (the pom skips tests by default).
- Prefer extending the existing stress tests (`ConnectionTrackerStressTest`,
  `KickMessageStressTest`, `ConnectionGuardTest`, `BufferLifecycleTest`) — they already set
  up embedded-channel / loopback harnesses.
- For flood behavior, an `EmbeddedChannel` loop asserting map/counter bounds is enough; no
  need for a real socket load generator in CI.
