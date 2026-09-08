# MCGate

A Java/Kotlin thin, host-based reverse proxy for Minecraft servers. It routes client connections to backend servers by the hostname the client dialed, without acting as a full proxy - no session handling, no server switching, just fast connection forwarding.

## Features

- **Host-based routing** with `*` / `?` wildcards and `$1`, `$2`, ... parameter substitution in backend addresses
- **Per-route `priority`** - higher priority routes are matched before lower ones, regardless of file order (default `0`, ties keep file order)
- **Multiple backends per route** with load balancing strategies: `sequential`, `random`, `round-robin`, `least-connections`, `lowest-latency`, plus automatic failover on connect failure
- **Status ping caching** per backend, with a configurable `cachePingTTL` (or `-1s` to disable)
- **Fallback status response** (motd/version/players/favicon) when all of a route's backends are down
- **`modifyVirtualHost`** - rewrites the handshake hostname before forwarding to the backend
- **Per-route `proxyProtocol`** - sends a PROXY protocol v1 header so the backend sees the real client IP
- **DDoS / flood hardening** - `loginTimeout` closes half-open connections (slow-loris); the backend dial *and* backend DNS resolution are both deferred until the client's Login Start arrives, so a connection flood (e.g. to random subdomains on a wildcard route) never reaches the backend or the resolver; oversized handshake frames are rejected on sight; the ping and DNS caches are size-capped; optional `maxConnectionsPerIp` caps concurrent pre-login connections per source IP
- **Hot config reload** - edits to the config file are picked up live, no restart needed (except for the `bind` address)
- **Bootstraps a default config** on first run if none exists

## Build

Requires JDK 17+ and Maven.

```
mvn package -DskipTests
```

Produces `target/MCGate-1.0-SNAPSHOT.jar`.

## Run

```
java -jar target/MCGate-1.0-SNAPSHOT.jar [path/to/config.yml]
```

If the config path doesn't exist yet, it's created from a bundled example covering every feature. Defaults to `config.yml` in the working directory.

### Memory / JVM flags

MCGate is a byte-relay: its own working set is tiny (a few hundred KiB per connected player). Almost all of the RSS you see is the JVM heap reservation and Netty's pooled buffer arenas, both of which size themselves off the CPU/RAM the JVM *thinks* it has. On a shared hosting node (Pterodactyl and similar) the JVM often sees the whole physical box, not your slice, and over-reserves badly - e.g. ~900 MiB resident with only ~50 players.

The build already shrinks Netty's side of this (small fixed arena count, capped worker threads, a 96 MiB direct-memory ceiling - see `tuneNettyMemoryFootprint` in `Main.kt`). For the heap, pass explicit flags - for a 1 GiB container:

```
java -Xms128m -Xmx512m \
     -XX:+UseSerialGC \
     -XX:MaxDirectMemorySize=128m \
     -XX:+ExitOnOutOfMemoryError \
     -jar MCGate-1.0-SNAPSHOT.jar
```

`-Xmx512m` leaves headroom for direct buffers, thread stacks, and metaspace under the 1 GiB cap. `-XX:+UseSerialGC` has the smallest footprint and actually returns freed memory to the OS, which G1 (the default) largely won't at this heap size. Scale `-Xmx` with the container: roughly `limit - 256m - (players / 500)m`.

## Continuous integration

`Jenkinsfile` defines a declarative pipeline:

| Stage | What it does |
| --- | --- |
| Checkout | `checkout scm` and records the commit SHA; posts a `PENDING` GitHub commit status (`ci/jenkins`) |
| Build | `mvn -B clean compile` |
| Test | `mvn -B test`, publishes JUnit results, zips `surefire-reports` and archives `test-reports.zip` |
| Package | `mvn -B package -DskipTests`, archives `target/MCGate-*.jar` |

On completion it sets the GitHub commit status to `SUCCESS` / `FAILURE` (the check shown on commits and PRs). On pull-request builds (a Multibranch Pipeline job) it also comments on the PR with a pass/fail/skipped table and a link to the archived test report.

**Jenkins setup:**

- Tools named `JDK 21` and `Maven 3` configured under *Manage Jenkins → Tools*.
- Plugins: *GitHub API for Pipeline* (`githubNotify`), *JUnit*; for PR builds/comments also *GitHub Branch Source* and *Pipeline: GitHub*.
- A global **Username with password** credential `github-rnis` (username = a GitHub user with write access to `RNIS-TW/MCGate`, password = a token with *Commit statuses* and *Pull requests* read/write). Update `GITHUB_ACCOUNT` / `GITHUB_REPO` / `GITHUB_CRED` in `Jenkinsfile` if these differ.
- For automatic PR builds, use a *Multibranch Pipeline* job with "Discover pull requests" and a GitHub webhook to `/github-webhook/`.

## Configuration

```yaml
config:
  bind: 0.0.0.0:25565
  routes:
    - host: survival.example.com
      backend: 127.0.0.1:25566

    - host: "*.wildcard.example.com"
      backend: "$1.servers.svc:25568"

    - host: vip.wildcard.example.com
      backend: 127.0.0.1:25573
      priority: 10

    - host: lobby.example.com
      backend: [127.0.0.1:25569, 127.0.0.1:25570]
      strategy: round-robin
      cachePingTTL: 60s

    - host: localhost
      backend: 127.0.0.1:25572
      fallback:
        motd: Server is offline.
        version:
          name: "Try again later!"
          protocol: -1
```

Routes are matched in descending `priority` order (ties keep the order they appear in the file), so a specific host like `vip.wildcard.example.com` can win over an overlapping wildcard such as `*.wildcard.example.com` even though the wildcard is listed first.

See `src/main/resources/default-config.yml` for a complete annotated example.

## API

MCGate can expose a small read-only JSON/HTTP status API. Disabled by default:

```yaml
config:
  api:
    enabled: false
    bind: localhost:8080
```

Endpoints (all `GET`):

| Path | Description |
| --- | --- |
| `/v1/routes` | All routes: hosts, backend templates, strategy, TTL, flags |
| `/v1/routes/{index}` | A single route by its index in the config |
| `/v1/routes/{index}/backends` | Last-known per-backend stats (active connections, latency) for non-wildcard routes |
| `/v1/routes/{index}/ping` | Dials each backend of the route **right now** (bypassing the ping cache) and returns live online status, latency, and the raw status JSON, or an error per backend that's unreachable |

The API only binds when `enabled: true`; bind it to `localhost` or a private interface unless it's behind your own auth/network controls.

## Project layout

```
src/main/kotlin/me/hippodev/
  Main.kt               - bootstrap, config hot-reload wiring, connection dispatch
  ConfigLoader.kt        - default-config bootstrapping + file watcher
  Config.kt               - YAML config model and parsing
  HostPattern.kt          - wildcard host matching and $N substitution
  BackendSelector.kt      - load balancing strategies and per-route runtime state
  HandshakeSniffer.kt     - reads just enough of the handshake packet to route
  LoginRelayHandler.kt    - raw TCP relay for real (login) connections
  StatusHandler.kt        - status ping handling, caching, fallback
  PingCache.kt            - TTL cache for backend status responses
  StatusJson.kt           - fallback status JSON building
  MinecraftProtocol.kt    - varint/string/packet encode-decode helpers
src/main/resources/
  default-config.yml      - bundled example config, copied on first run
```

```
Jenkinsfile               - CI pipeline: checkout, build, test, package, GitHub status + PR comment
```

## License

MIT - see [LICENSE](LICENSE).
