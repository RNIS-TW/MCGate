//! The TCP accept loop and connection dispatch — port of the `dispatch()` function and pipeline
//! wiring in `Main.kt`, plus `StatusHandler.kt`/`LoginRelayHandler.kt`.
//!
//! **Still out of scope** (see plan.md for the full breakdown): a backend that goes down
//! mid-session just drops the client (no mid-session reconnect-hold — `ReconnectHandler.kt` is
//! not ported at all); no `ConnectionTracker`/SQLite persistence or voice-chat routing (sections
//! 5/6); a kicked player is disconnected with no message (needs backend login sniffing for
//! correct compression/encryption framing — see `state.rs`'s `PlayerSession` doc).
//!
//! **What IS real**: handshake parsing, every anti-abuse guard, inbound/outbound PROXY protocol,
//! `modifyVirtualHost`, DNS-cached backend resolution, backend-selection strategies
//! (`backend_selector.rs`), status-ping caching (`ping_cache.rs`), per-route traffic accounting
//! and login-limit enforcement (`route_metrics_store.rs`), live `PlayerSessions` tracking with a
//! working (message-less) kick, and a byte-accurate bidirectional relay.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use crate::state::app_state::AppState;
use crate::net::backend_selector::order_backends;
use crate::net::buffered_stream::BufferedStream;
use crate::config::Route;
use crate::net::flood_control::{connection_rates, GlobalConnectionGuard};
use crate::protocol::handshake::HandshakeError;
use crate::protocol::minecraft_protocol::{encode_handshake, encode_proxy_protocol_header, encode_status_response};
use crate::net::ping_cache::PingCache;
use crate::state::route_metrics_store::route_metrics_store;
use crate::state::{player_sessions, PlayerSession, RouteRuntime};
use crate::protocol::status_json::build_fallback_json;
use crate::protocol::text_format::to_json_component;
use crate::protocol::varint::{read_var_int, write_var_int, MAX_PACKET_BYTES};

fn ping_cache() -> &'static PingCache {
    static INSTANCE: std::sync::OnceLock<PingCache> = std::sync::OnceLock::new();
    INSTANCE.get_or_init(PingCache::default)
}

/// Records one auto-ban violation for `ip` (see `net::ip_ban`), logging once at the moment it
/// actually crosses the threshold and triggers a fresh ban - not on every subsequent rejected
/// connection attempt from the same already-banned IP, which would just be log spam for exactly
/// the flood this exists to mitigate.
fn record_ip_violation(cfg: &crate::config::GateConfig, ip: &str) {
    if !cfg.auto_ban.enabled {
        return;
    }
    let banned = crate::net::ip_ban::ip_ban_list().record_violation(
        ip,
        cfg.auto_ban.max_violations.max(1) as u32,
        Duration::from_millis(cfg.auto_ban.violation_window_millis.max(0) as u64),
        Duration::from_millis(cfg.auto_ban.ban_duration_millis.max(0) as u64),
    );
    if banned {
        tracing::warn!("Auto-banning {ip} for {}s after repeated violations", cfg.auto_ban.ban_duration_millis / 1000);
    }
}

pub async fn run(state: Arc<AppState>) -> anyhow::Result<()> {
    crate::net::ping_cache::spawn_sweeper(ping_cache());
    let bind = state.config().bind_address()?;
    let listener = TcpListener::bind(bind).await?;
    tracing::info!("Listening on {bind}");
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("Accept failed: {e}");
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, peer_addr, state).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream, peer_addr: SocketAddr, state: Arc<AppState>) {
    let cfg = state.config();

    // The cheapest possible rejection: a HashMap lookup, before even the global connection-cap
    // atomic or any socket I/O. Skipped under proxy_protocol for the same reason every other
    // per-IP guard below is - at this point (before the PROXY header is read) `peer_addr` is the
    // upstream load balancer's address, not the real client's, so banning it would ban the load
    // balancer. See `net::ip_ban` for why this exists on top of the other per-IP guards: it's
    // what turns a sustained flood from a small number of repeat-offending source IPs into
    // near-zero-cost drops instead of paying the handshake/guard-acquire cost on every attempt
    // forever.
    if !cfg.proxy_protocol && cfg.auto_ban.enabled && crate::net::ip_ban::ip_ban_list().is_banned(&peer_addr.ip().to_string()) {
        tracing::debug!("Dropping connection from {peer_addr} - source IP is temporarily auto-banned");
        return;
    }

    let Some(_global_guard) = GlobalConnectionGuard::acquire(cfg.connection_throttle.max_connections) else {
        tracing::debug!("Rejecting connection from {peer_addr} - at global connection cap ({})", cfg.connection_throttle.max_connections);
        return;
    };

    // Process-wide per-IP new-connection rate limit - meaningless under proxy_protocol (every
    // connection would appear to come from the upstream load balancer's single IP).
    if !cfg.proxy_protocol && cfg.connection_throttle.max_per_ip_per_window > 0 {
        let ip = peer_addr.ip().to_string();
        let window = Duration::from_millis(cfg.connection_throttle.window_millis.max(0) as u64);
        if !connection_rates().try_acquire(&ip, cfg.connection_throttle.max_per_ip_per_window as u32, window) {
            tracing::debug!("Rejecting connection from {peer_addr} - over connection rate limit");
            record_ip_violation(&cfg, &ip);
            return;
        }
    }

    let _ = stream.set_nodelay(true);

    let mut effective_addr = peer_addr;
    if cfg.proxy_protocol {
        match crate::protocol::proxy_protocol_tcp::read_proxy_protocol_header(&mut stream).await {
            Ok(Some(addr)) => effective_addr = addr,
            Ok(None) => {} // v2 LOCAL / degenerate header - fall back to the real TCP peer
            Err(e) => {
                tracing::debug!("PROXY protocol header error from {peer_addr}: {e}");
                return;
            }
        }
    }

    // Front of the pipeline: bound the pre-login phase so a connection-flood/slow-loris can't
    // tie up tasks/fds. Per-IP capping is skipped under proxy_protocol - every connection would
    // otherwise look like it came from the upstream load balancer.
    let per_ip_limit = if cfg.proxy_protocol { 0 } else { cfg.max_connections_per_ip.max(0) as u32 };
    let ip_string = (!cfg.proxy_protocol).then(|| effective_addr.ip().to_string());
    let Some(mut guard) = crate::net::connection_guard::PreLoginGuard::acquire(ip_string.as_deref(), per_ip_limit) else {
        tracing::debug!("Dropping connection from {effective_addr} - over per-IP pre-login limit ({per_ip_limit})");
        if let Some(ip) = &ip_string {
            record_ip_violation(&cfg, ip);
        }
        return;
    };

    let mut stream = BufferedStream::new(stream);
    let login_timeout = cfg.login_timeout_millis.max(0) as u64;

    let handshake_result = if login_timeout > 0 {
        match tokio::time::timeout(Duration::from_millis(login_timeout), stream.read_handshake()).await {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!("Closing {effective_addr} - did not complete login within {login_timeout} ms");
                if let Some(ip) = &ip_string {
                    record_ip_violation(&cfg, ip);
                }
                return;
            }
        }
    } else {
        stream.read_handshake().await
    };
    let handshake = match handshake_result {
        Ok(h) => h,
        Err(HandshakeError::Eof) => return, // benign - a health-check/scanner connecting and going away
        Err(e) => {
            tracing::debug!("Rejecting connection from {effective_addr}: {e}");
            if let Some(ip) = &ip_string {
                record_ip_violation(&cfg, ip);
            }
            return;
        }
    };

    if cfg.log_connections {
        let kind = if handshake.next_state == 1 { "status" } else { "login" };
        tracing::info!("Connection: host='{}' from {effective_addr} ({kind}, protocol {})", handshake.host, handshake.protocol_version);
    }

    let matched = cfg.routes.iter().enumerate().find_map(|(i, r)| r.match_host(&handshake.host).map(|captures| (i, r.clone(), captures)));
    let Some((route_index, route, captures)) = matched else {
        tracing::info!("No route for host '{}', closing connection from {effective_addr}", handshake.host);
        return;
    };
    let runtime = state.route_runtime(route_index).unwrap_or_default();

    if handshake.next_state == 1 {
        handle_status(&mut stream, &route, &runtime, &captures, &handshake, effective_addr).await;
    } else {
        handle_login(&mut stream, &route, &runtime, &captures, &handshake, effective_addr, &mut guard, &state).await;
    }
}

async fn resolve_ordered_backends(route: &Route, runtime: &RouteRuntime, captures: &[String]) -> Result<Vec<SocketAddr>, crate::net::dns_cache::DnsResolveError> {
    let resolved = route.resolve_backends(captures).await?;
    Ok(order_backends(route, runtime, &resolved))
}

async fn handle_status(
    stream: &mut BufferedStream<TcpStream>,
    route: &Route,
    runtime: &RouteRuntime,
    captures: &[String],
    handshake: &crate::protocol::handshake::Handshake,
    client_addr: SocketAddr,
) {
    // Status Request (packet id 0x00, no fields).
    let request = match stream.read_frame(MAX_PACKET_BYTES).await {
        Ok(f) => f,
        Err(e) => {
            tracing::debug!("Rejecting status connection from {client_addr}: {e}");
            return;
        }
    };
    let (_length, header_len) = read_var_int(&request).unwrap();
    let Ok((packet_id, _)) = read_var_int(&request[header_len..]) else { return };
    if packet_id != 0x00 {
        return;
    }

    let backends = match resolve_ordered_backends(route, runtime, captures).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("Failed to resolve backends for host '{}' from {client_addr}: {e}", handshake.host);
            return;
        }
    };

    for addr in &backends {
        // Cache key is host:port only, NOT host:port:protocolVersion - keying per protocol
        // version would let a status flood trivially bypass the cache.
        let down_key = format!("{}:{}", addr.ip(), addr.port());
        if let Some(cached) = ping_cache().get(&down_key) {
            let _ = tokio::io::AsyncWriteExt::write_all(stream, &encode_status_response(&cached)).await;
            handle_ping(stream, client_addr).await;
            return;
        }
        if ping_cache().is_known_down(&down_key) {
            continue;
        }
        if !runtime.try_begin_status_dial(*addr, crate::state::MAX_CONCURRENT_STATUS_DIALS) {
            tracing::debug!("Too many in-flight status dials to {addr}, failing over");
            continue;
        }
        let start = std::time::Instant::now();
        let result = fetch_backend_status(route, *addr, handshake, client_addr).await;
        runtime.end_status_dial(*addr);
        match result {
            Some(json) => {
                runtime.record_latency(*addr, start.elapsed().as_millis() as i64);
                ping_cache().put(&down_key, json.clone(), route.cache_ping_ttl_millis);
                let _ = tokio::io::AsyncWriteExt::write_all(stream, &encode_status_response(&json)).await;
                handle_ping(stream, client_addr).await;
                return;
            }
            None => {
                ping_cache().mark_down(&down_key);
                continue;
            }
        }
    }

    // Every backend failed - serve a configured fallback status, or just close.
    if let Some(fallback) = &route.fallback {
        let json = build_fallback_json(fallback);
        let _ = tokio::io::AsyncWriteExt::write_all(stream, &encode_status_response(&json)).await;
        handle_ping(stream, client_addr).await;
    }
}

/// Dials `addr` just long enough to fetch and validate its Status Response JSON. `None` on any
/// failure (connect, timeout, malformed response) - the caller fails over to the next backend.
async fn fetch_backend_status(route: &Route, addr: SocketAddr, handshake: &crate::protocol::handshake::Handshake, client_addr: SocketAddr) -> Option<String> {
    let connect = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await;
    let mut backend = match connect {
        Ok(Ok(s)) => s,
        _ => {
            tracing::debug!("Status dial to {addr} failed or timed out");
            return None;
        }
    };
    let _ = backend.set_nodelay(true);

    if route.proxy_protocol && should_forward_proxy_protocol(client_addr) {
        let _ = tokio::io::AsyncWriteExt::write_all(&mut backend, &encode_proxy_protocol_header(client_addr, addr)).await;
    }
    // Forward the connecting client's real protocol version so version-multiplexing backends
    // resolve and report the version the client actually asked for.
    let hs = encode_handshake(handshake.protocol_version, &handshake.host, handshake.port, 1);
    if tokio::io::AsyncWriteExt::write_all(&mut backend, &hs).await.is_err() {
        return None;
    }
    let mut request = Vec::new();
    write_var_int(&mut request, 1);
    write_var_int(&mut request, 0x00);
    if tokio::io::AsyncWriteExt::write_all(&mut backend, &request).await.is_err() {
        return None;
    }

    let mut buffered = BufferedStream::new(backend);
    let frame = match tokio::time::timeout(Duration::from_secs(5), buffered.read_frame(262_144)).await {
        Ok(Ok(f)) => f,
        _ => return None,
    };
    let (_length, header_len) = read_var_int(&frame).ok()?;
    let (packet_id, consumed) = read_var_int(&frame[header_len..]).ok()?;
    if packet_id != 0x00 {
        return None;
    }
    let (json, _) = crate::protocol::varint::read_string(&frame[header_len + consumed..], 262_144).ok()?;
    if !is_valid_status_json(&json) {
        tracing::warn!("Backend {addr} sent a malformed status response, treating as down");
        return None;
    }
    Some(json)
}

/// Whether `json` is a well-formed Server List Ping status response — specifically, that
/// `version.protocol` is present and numeric, since that's the field a malformed/misbehaving
/// backend most often gets wrong. Deliberately permissive about everything else.
fn is_valid_status_json(json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else { return false };
    value.get("version").and_then(|v| v.get("protocol")).map(|p| p.is_number()).unwrap_or(false)
}

/// Reads and answers the trailing optional Ping packet, if the client sends one, then closes.
async fn handle_ping(stream: &mut BufferedStream<TcpStream>, client_addr: SocketAddr) {
    let frame = match tokio::time::timeout(Duration::from_secs(10), stream.read_frame(64)).await {
        Ok(Ok(f)) => f,
        _ => return,
    };
    let (_length, header_len) = match read_var_int(&frame) {
        Ok(v) => v,
        Err(_) => return,
    };
    let Ok((packet_id, consumed)) = read_var_int(&frame[header_len..]) else { return };
    if packet_id != 0x01 {
        return;
    }
    let payload_start = header_len + consumed;
    if frame.len() - payload_start < 8 {
        return;
    }
    let payload = i64::from_be_bytes(frame[payload_start..payload_start + 8].try_into().unwrap());
    if let Err(e) = tokio::io::AsyncWriteExt::write_all(stream, &crate::protocol::minecraft_protocol::encode_pong(payload)).await {
        tracing::debug!("Failed to send pong to {client_addr}: {e}");
    }
}

async fn handle_login(
    stream: &mut BufferedStream<TcpStream>,
    route: &Route,
    runtime: &Arc<RouteRuntime>,
    captures: &[String],
    handshake: &crate::protocol::handshake::Handshake,
    client_addr: SocketAddr,
    guard: &mut crate::net::connection_guard::PreLoginGuard,
    state: &Arc<AppState>,
) {
    // Refuse outright if this route's cumulative usage has already reached its configured limit
    // - just an atomic read, no I/O. Players already on the route are kicked by
    // route_metrics_store's background ticker, not from here.
    if let Some(counter) = route_metrics_store().handle(route) {
        if counter.exceeded() {
            tracing::info!("Route metrics limit reached for host '{}', refusing login from {client_addr}", handshake.host);
            kick(stream, &state.messages().metrics_limit_kick_message).await;
            return;
        }
    }

    // Wait for Login Start before dialing a backend - a connection-flood/slow-loris that
    // completes the handshake but never logs in would otherwise make MCGate open (and hold) a
    // backend socket per bot. ConnectionGuardHandler's deadline (already wrapping this whole
    // function via the caller's timeout) closes the connection if it never comes.
    let login_start_frame = match stream.read_frame(MAX_PACKET_BYTES).await {
        Ok(f) => f,
        Err(e) => {
            tracing::debug!("Rejecting login connection from {client_addr}: {e}");
            return;
        }
    };
    // login_start_frame already includes its own length prefix - parse_login_start expects
    // exactly that shape.
    let parsed_login = crate::protocol::minecraft_protocol::parse_login_start(&login_start_frame);
    let player_name = parsed_login.as_ref().map(|(name, _)| name.clone());

    guard.done(); // proven real - stop the login deadline / free the per-IP pre-login slot

    let backends = match resolve_ordered_backends(route, runtime, captures).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("Failed to resolve backends for host '{}' from {client_addr}: {e}", handshake.host);
            kick(stream, &route.kick_message).await;
            return;
        }
    };

    for addr in &backends {
        let dial_start = std::time::Instant::now();
        match dial_backend(route, *addr, handshake, client_addr).await {
            Ok(mut backend) => {
                // The backend now has the handshake (sent inside dial_backend) but still needs
                // the client's actual Login Start packet forwarded before it'll do anything -
                // without this the backend just sits waiting for login and the connection hangs.
                if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut backend, &login_start_frame).await {
                    tracing::debug!("Failed to forward Login Start to backend {addr}: {e}");
                    continue;
                }
                runtime.record_connect_opened(*addr);
                runtime.record_latency(*addr, dial_start.elapsed().as_millis() as i64);
                let player_tag = match (player_name.as_deref(), parsed_login.as_ref().and_then(|(_, uuid)| *uuid)) {
                    (Some(name), Some(uuid)) => format!(" ({name}, {uuid})"),
                    (Some(name), None) => format!(" ({name})"),
                    (None, _) => String::new(),
                };
                tracing::info!("Connected: '{}'{player_tag} from {client_addr} -> {addr}", handshake.host);

                register_voicechat_route(route, captures, client_addr, &handshake.host, state.config().log_connections).await;

                // Observe the backend's real Login-state response just long enough to learn its
                // actual compression threshold and whether it's encrypted - needed for `kick
                // <player> <message>` and `transfer` to frame an injected packet correctly later
                // (see `sniff_backend_login`'s doc). `unwrap_or((-1, false))` on a sniff failure
                // (backend closed/errored mid-login-response) matches this connection's prior
                // always-hardcoded behavior rather than changing failover semantics here - the
                // now-dead backend fails the same way in `relay`'s own read loop either way.
                let mut backend = BufferedStream::new(backend);
                let (compression_threshold, encrypted) = sniff_backend_login(stream, &mut backend).await.unwrap_or((-1, false));

                let session = parsed_login.as_ref().and_then(|(name, uuid)| {
                    let uuid = (*uuid)?;
                    let session = Arc::new(PlayerSession {
                        name: name.clone(),
                        uuid,
                        host: handshake.host.clone(),
                        remote_address: client_addr.to_string(),
                        backend: Some(*addr),
                        connected_at_millis: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0),
                        protocol_version: handshake.protocol_version,
                        compression_threshold,
                        encrypted,
                        login_attempts: std::sync::atomic::AtomicU32::new(1),
                        packets_sent: std::sync::atomic::AtomicI64::new(0),
                        packets_received: std::sync::atomic::AtomicI64::new(0),
                        bytes_sent: std::sync::atomic::AtomicI64::new(0),
                        bytes_received: std::sync::atomic::AtomicI64::new(0),
                        disconnect: tokio::sync::Notify::new(),
                        pending_action: std::sync::Mutex::new(crate::state::SessionAction::default()),
                    });
                    player_sessions().put(session.clone());
                    Some(session)
                });

                let route_counter = route_metrics_store().handle(route);
                relay(stream, backend, session.as_deref(), route_counter.as_deref()).await;

                runtime.record_connect_closed(*addr);
                if let Some(session) = &session {
                    player_sessions().remove(session.uuid);
                    crate::state::connection_tracker::connection_tracker().record(crate::state::connection_tracker::ConnectionRecord {
                        uuid: session.uuid,
                        name: session.name.clone(),
                        ip: session.remote_address.clone(),
                        host: session.host.clone(),
                        protocol_version: session.protocol_version,
                        login_attempts: session.login_attempts.load(Ordering::Relaxed) as i32,
                        packets_sent: session.packets_sent.load(Ordering::Relaxed),
                        packets_received: session.packets_received.load(Ordering::Relaxed),
                        bytes_sent: session.bytes_sent.load(Ordering::Relaxed),
                        bytes_received: session.bytes_received.load(Ordering::Relaxed),
                        compression_threshold: session.compression_threshold,
                        encrypted: session.encrypted,
                        connected_at: session.connected_at_millis,
                        disconnected_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0),
                    });
                }
                crate::udp::voice_routing::voice_routing().unregister(&client_addr.ip().to_string());
                tracing::info!("Disconnected: '{}' from {client_addr} -> {addr}", handshake.host);
                return;
            }
            Err(e) => {
                tracing::debug!("Failed to connect to backend {addr}: {e}");
                continue;
            }
        }
    }

    tracing::warn!(
        "All backends unreachable for host '{}', kicking{} from {client_addr} (tried {backends:?})",
        handshake.host,
        player_name.as_deref().map(|n| format!(" ({n})")).unwrap_or_default()
    );
    kick(stream, &route.kick_message).await;
}

async fn kick(stream: &mut BufferedStream<TcpStream>, message: &str) {
    let packet = crate::protocol::reconnect_protocol::encode_login_disconnect(&to_json_component(message));
    let _ = tokio::io::AsyncWriteExt::write_all(stream, &packet).await;
}

/// Whether `client_addr` is a real, forwardable source for an outbound PROXY protocol header -
/// used by both `dial_backend` (login) and `fetch_backend_status` (status ping) so the two paths
/// can't drift apart on this again. A degenerate source (loopback, unspecified/wildcard, or port
/// 0 - e.g. a client that connected via `localhost`/`127.0.0.1`) must never be forwarded: a
/// strict backend PROXY protocol implementation RST-drops, or worse, silently withholds any
/// response to, a header claiming such a source - which looks identical to a hung/unreachable
/// backend from the client's side, with no error logged anywhere.
fn should_forward_proxy_protocol(client_addr: SocketAddr) -> bool {
    client_addr.port() != 0 && !client_addr.ip().is_unspecified() && !client_addr.ip().is_loopback()
}

async fn dial_backend(route: &Route, addr: SocketAddr, handshake: &crate::protocol::handshake::Handshake, client_addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut backend = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
    let _ = backend.set_nodelay(true);

    if route.proxy_protocol && should_forward_proxy_protocol(client_addr) {
        tokio::io::AsyncWriteExt::write_all(&mut backend, &encode_proxy_protocol_header(client_addr, addr)).await?;
    }
    if route.modify_virtual_host {
        let hs = encode_handshake(handshake.protocol_version, &addr.ip().to_string(), handshake.port, 2);
        tokio::io::AsyncWriteExt::write_all(&mut backend, &hs).await?;
    } else {
        tokio::io::AsyncWriteExt::write_all(&mut backend, &handshake.raw_frame).await?;
    }
    Ok(backend)
}

/// Observes the backend's Login-state response just long enough to learn the connection's real
/// compression threshold and whether it's encrypted, forwarding every frame it reads to the
/// client unmodified along the way (`relay` takes over as a raw byte splice the moment this
/// returns, using whatever it determined). Also relays client -> backend bytes unmodified for the
/// same window: some backends (e.g. a "modern"/secure proxy-forwarding plugin) send a Login
/// Plugin Request expecting a Login Plugin Response from the client before continuing, and
/// without forwarding that direction too during this window, the response would never reach the
/// backend and the login would hang forever.
///
/// Only Login-state packet IDs are used here (`0x00` Disconnect, `0x01` Encryption Request,
/// `0x02` Login Success, `0x03` Set Compression) — these have been stable across every Minecraft
/// version since compression/encryption were introduced, unlike Configuration/Play IDs (which
/// shift release to release, see `reconnect_protocol.rs`'s file header), so this deliberately
/// stops the instant Login Success arrives rather than trying to track anything past Login state.
///
/// The moment an Encryption Request is observed, every packet the backend sends from then on is
/// ciphertext MCGate never has the key for — trying to parse any of it as a plaintext frame would
/// be reading garbage, not a real protocol violation, so this returns immediately with
/// `encrypted: true` rather than attempting another read.
///
/// Returns `None` if the backend closes/errors before Login Success arrives (nothing real to
/// report — `relay`'s own read loop will surface the same failure immediately once it starts) or
/// a frame turns out unparsable for any other reason (never trust `compression_threshold` off a
/// read that's already suspect).
async fn sniff_backend_login(client: &mut BufferedStream<TcpStream>, backend: &mut BufferedStream<TcpStream>) -> Option<(i32, bool)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut compression_threshold: i32 = -1;
    let mut upload_buf = [0u8; 4096];

    // A single loop selecting fresh per-iteration futures - not two long-lived closures each
    // needing both `client` and `backend` for their own entire lifetime (`client` read in one,
    // written in the other; same for `backend`) - which the borrow checker can't allow, since
    // both would be live at once for as long as the whole sniff runs. Each iteration's two
    // branch futures only ever borrow one of the two variables apiece; `select!` drops
    // whichever branch didn't win before running the winner's body, so there's no overlap.
    loop {
        tokio::select! {
            result = client.read(&mut upload_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => return None,
                    Ok(n) => n,
                };
                if backend.write_all(&upload_buf[..n]).await.is_err() {
                    return None;
                }
            }
            frame_result = tokio::time::timeout(Duration::from_secs(10), backend.read_frame(MAX_PACKET_BYTES)) => {
                let frame = match frame_result {
                    Ok(Ok(f)) => f,
                    _ => return None,
                };
                if client.write_all(&frame).await.is_err() {
                    return None;
                }
                let (_len, header_len) = read_var_int(&frame).ok()?;
                match crate::protocol::compression::read_compressed_frame(&frame[header_len..], compression_threshold) {
                    Ok((0x01, _)) => return Some((compression_threshold, true)), // Encryption Request
                    Ok((0x03, fields)) => {
                        // Set Compression: a single VarInt field, the new threshold.
                        if let Ok((threshold, _)) = read_var_int(&fields) {
                            compression_threshold = threshold;
                        }
                    }
                    Ok((0x02, _)) => return Some((compression_threshold, false)), // Login Success
                    Ok((0x00, _)) => return None,                                 // Disconnect (login)
                    Ok(_) => {}   // Login Plugin Request or anything else - not relevant here
                    Err(_) => return Some((compression_threshold, true)), // unparsable - assume the worst
                }
            }
        }
    }
}

/// The actual byte splice, once a backend is connected and its Login-state response has been
/// sniffed. A custom loop (not `tokio::io::copy_bidirectional`) because live per-session/per-route
/// byte counters (`PlayerSession`, `RouteMetricsStore`) need to see bytes as they flow, not only a
/// final total after the connection ends — and because a console/API `kick`/`transfer` needs to
/// both interrupt the splice AND (when the session isn't encrypted) write one real packet into it
/// first, via `session.disconnect` + `session.pending_action`. Backpressure is still equivalent to
/// `copy_bidirectional`/Netty's water-mark-driven `pauseOrResumeReads`: each direction only reads
/// more once its own `write_all` call has returned, so a slow reader on one side naturally stalls
/// reads from the other via normal `AsyncRead`/`AsyncWrite` polling - nothing extra to port for
/// `FlowControl.kt`.
///
/// A single loop selecting between three branches each iteration (rather than two long-lived
/// upload/download futures plus a `disconnect` watch, as this originally was) so that the
/// disconnect/pending-action branch can also write to `client_w` — a pre-built `download` future
/// would hold an exclusive borrow of it for its own entire lifetime, which the borrow checker
/// won't let a sibling `select!` branch also touch.
async fn relay(client: &mut BufferedStream<TcpStream>, backend: BufferedStream<TcpStream>, session: Option<&PlayerSession>, route_counter: Option<&crate::state::route_metrics_store::RouteTrafficCounter>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut client_r, mut client_w) = tokio::io::split(client);
    let (mut backend_r, mut backend_w) = tokio::io::split(backend);
    let mut upload_buf = [0u8; 8192];
    let mut download_buf = [0u8; 8192];

    loop {
        tokio::select! {
            result = client_r.read(&mut upload_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if let Some(s) = session {
                    s.packets_sent.fetch_add(1, Ordering::Relaxed);
                    s.bytes_sent.fetch_add(n as i64, Ordering::Relaxed);
                }
                if let Some(c) = route_counter {
                    c.add_upload(n as i64);
                }
                if backend_w.write_all(&upload_buf[..n]).await.is_err() {
                    break;
                }
            }
            result = backend_r.read(&mut download_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if let Some(s) = session {
                    s.packets_received.fetch_add(1, Ordering::Relaxed);
                    s.bytes_received.fetch_add(n as i64, Ordering::Relaxed);
                }
                if let Some(c) = route_counter {
                    c.add_download(n as i64);
                }
                if client_w.write_all(&download_buf[..n]).await.is_err() {
                    break;
                }
            }
            _ = wait_for_pending_action(session) => {
                if let Some(s) = session {
                    handle_pending_action(s, &mut client_w).await;
                }
                break;
            }
        }
    }
}

/// `None` never fires - a session-less relay (status/no-UUID logins) has nothing to be kicked or
/// transferred by, so this branch of `relay`'s `select!` should just never win.
async fn wait_for_pending_action(session: Option<&PlayerSession>) {
    match session {
        Some(s) => s.disconnect.notified().await,
        None => std::future::pending().await,
    }
}

/// Runs whatever `session.pending_action` was set to when `session.disconnect` last fired,
/// writing a real packet to the client first for the two variants that need one - unless the
/// session is encrypted or its protocol version isn't in `reconnect_protocol`'s verified bracket,
/// in which case this falls back to a plain disconnect (the original, message-less `kick`
/// behavior) rather than risk corrupting the stream with a packet framed on a guess.
async fn handle_pending_action<W: tokio::io::AsyncWrite + Unpin>(session: &PlayerSession, client_w: &mut W) {
    use tokio::io::AsyncWriteExt;

    let action = std::mem::take(&mut *session.pending_action.lock().unwrap());
    let (verb, packet) = match action {
        crate::state::SessionAction::Disconnect => {
            tracing::debug!("Session for '{}' interrupted by kick", session.name);
            return;
        }
        crate::state::SessionAction::KickWithMessage(msg) if can_inject_packet(session) => {
            let ids = crate::protocol::reconnect_protocol::reconnect_packet_ids(session.protocol_version);
            let rendered = crate::protocol::text_format::to_legacy_text(&msg);
            ("kick", crate::protocol::reconnect_protocol::encode_play_disconnect(&ids, &rendered, session.compression_threshold))
        }
        crate::state::SessionAction::Transfer { host, port } if can_inject_packet(session) => {
            let ids = crate::protocol::reconnect_protocol::reconnect_packet_ids(session.protocol_version);
            ("transfer", crate::protocol::reconnect_protocol::encode_transfer(&ids, &host, port, session.compression_threshold))
        }
        other => {
            tracing::debug!(
                "Can't {} '{}' with a message - session is encrypted or protocol {} is unsupported for packet injection, disconnecting instead",
                if matches!(other, crate::state::SessionAction::Transfer { .. }) { "transfer" } else { "kick" },
                session.name,
                session.protocol_version,
            );
            return;
        }
    };
    if let Err(e) = client_w.write_all(&packet).await {
        tracing::debug!("Failed to send {verb} packet to '{}': {e}", session.name);
    }
}

/// Whether it's safe to write a synthesized packet into `session`'s connection: the session must
/// not be encrypted (MCGate never has the shared secret, so it can't produce a packet the
/// client's cipher would decrypt correctly) and its protocol version must be one
/// `reconnect_protocol` has a verified packet-ID bracket for (packet IDs otherwise aren't known to
/// be correct for that version, and `reconnect_packet_ids` would panic).
fn can_inject_packet(session: &PlayerSession) -> bool {
    !session.encrypted && crate::protocol::reconnect_protocol::reconnect_supported(session.protocol_version)
}

/// Records this connecting client's IP -> voicechat backend mapping (`voice_routing.rs`) if
/// `route` has a `voicechat:` backend configured, so `voice_relay.rs` can relay this player's UDP
/// voice traffic once it starts arriving. UDP carries no hostname to route by, so this is the
/// only point where that association can be made — resolution failures here must only skip voice
/// routing for this connection, never break the player's actual TCP login.
async fn register_voicechat_route(route: &Route, captures: &[String], client_addr: SocketAddr, host: &str, log_connections: bool) {
    if route.voicechat_templates.is_empty() {
        return;
    }
    match route.resolve_voicechat(captures).await {
        Ok(Some(voice_backend)) => {
            crate::udp::voice_routing::voice_routing().register(&client_addr.ip().to_string(), host, voice_backend);
            if log_connections {
                tracing::info!("Voicechat route: host='{host}' from {client_addr} -> {voice_backend}");
            }
        }
        Ok(None) => {}
        Err(e) => tracing::warn!("Failed to resolve voicechat backend for host '{host}': {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn is_valid_status_json_requires_numeric_protocol() {
        assert!(is_valid_status_json(r#"{"version":{"name":"1.21","protocol":767}}"#));
        assert!(!is_valid_status_json(r#"{"version":{"name":"1.21"}}"#));
        assert!(!is_valid_status_json(r#"{"version":{"protocol":"not-a-number"}}"#));
        assert!(!is_valid_status_json("not json at all"));
        assert!(!is_valid_status_json(r#"{"no_version_field":true}"#));
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_var_int(&mut out, payload.len() as i32);
        out.extend_from_slice(payload);
        out
    }

    fn packet(id: i32, fields: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        write_var_int(&mut payload, id);
        payload.extend_from_slice(fields);
        frame(&payload)
    }

    /// Connects a real client<->"backend" TCP pair and runs `sniff_backend_login` against
    /// whatever the given closure writes as the backend's response, returning its result plus
    /// everything the (simulated) client side received - real sockets, not an in-memory mock,
    /// same reasoning as `backend_pinger`'s tests: this is on the connection-accept hot path and
    /// deserves the same real-protocol-bytes-over-a-real-socket confidence as everything else
    /// here.
    async fn run_sniff(backend_writes: Vec<u8>) -> (Option<(i32, bool)>, Vec<u8>) {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_listener.local_addr().unwrap();
        let backend_task = tokio::spawn(async move {
            let (mut sock, _) = backend_listener.accept().await.unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut sock, &backend_writes).await.unwrap();
            // Keep the socket open briefly so a client-side write (Login Plugin Response, etc.)
            // has somewhere to land instead of an immediate reset, then let it drop/close.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client_listener.local_addr().unwrap();
        let client_task = tokio::spawn(async move {
            let (mut sock, _) = client_listener.accept().await.unwrap();
            let mut received = Vec::new();
            let _ = tokio::time::timeout(Duration::from_millis(200), tokio::io::AsyncReadExt::read_to_end(&mut sock, &mut received)).await;
            received
        });

        let client_side = tokio::net::TcpStream::connect(client_addr).await.unwrap();
        let backend_side = tokio::net::TcpStream::connect(backend_addr).await.unwrap();
        let mut client = BufferedStream::new(client_side);
        let mut backend = BufferedStream::new(backend_side);

        let result = sniff_backend_login(&mut client, &mut backend).await;
        drop(client);
        backend_task.await.unwrap();
        let received = client_task.await.unwrap();
        (result, received)
    }

    #[tokio::test]
    async fn login_success_with_no_compression_or_encryption() {
        let (result, forwarded) = run_sniff(packet(0x02, b"whatever fields")).await;
        assert_eq!(result, Some((-1, false)));
        assert_eq!(forwarded, packet(0x02, b"whatever fields"));
    }

    #[tokio::test]
    async fn set_compression_then_login_success_reports_the_threshold() {
        let mut backend_writes = packet(0x03, &{
            let mut v = Vec::new();
            write_var_int(&mut v, 256);
            v
        });
        // Once Set Compression is sent, every packet after it - Login Success included - uses
        // compressed-style framing (a leading `data_length` VarInt, `0` here since the payload
        // is under the threshold) even though `packet()` above always writes the pre-compression
        // shape; a real backend never mixes the two, so build this one with the actual
        // production `compression::frame` instead of `packet()`.
        let mut login_success_payload = Vec::new();
        write_var_int(&mut login_success_payload, 0x02);
        backend_writes.extend_from_slice(&crate::protocol::compression::frame(&login_success_payload, 256));
        let (result, _forwarded) = run_sniff(backend_writes).await;
        assert_eq!(result, Some((256, false)));
    }

    #[tokio::test]
    async fn encryption_request_stops_immediately_and_reports_encrypted() {
        // A real Encryption Request has more fields (server id, public key, verify token) - none
        // of them matter here since sniffing only reacts to the packet id.
        let (result, _forwarded) = run_sniff(packet(0x01, b"pretend-encryption-request-fields")).await;
        assert_eq!(result, Some((-1, true)));
    }

    #[tokio::test]
    async fn disconnect_before_login_success_reports_nothing() {
        let (result, _forwarded) = run_sniff(packet(0x00, b"kicked")).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn backend_closing_mid_response_reports_nothing() {
        // Set Compression arrives, then the backend vanishes before Login Success - never
        // fabricate a result off an incomplete login.
        let backend_writes = packet(0x03, &{
            let mut v = Vec::new();
            write_var_int(&mut v, 64);
            v
        });
        let (result, _forwarded) = run_sniff(backend_writes).await;
        assert_eq!(result, None);
    }

    #[test]
    fn can_inject_packet_refuses_encrypted_or_unsupported_sessions() {
        let base = |protocol_version: i32, encrypted: bool| crate::state::PlayerSession {
            name: "Steve".into(),
            uuid: uuid::Uuid::new_v4(),
            host: "example.com".into(),
            remote_address: "127.0.0.1:0".into(),
            backend: None,
            connected_at_millis: 0,
            protocol_version,
            compression_threshold: -1,
            encrypted,
            login_attempts: std::sync::atomic::AtomicU32::new(1),
            packets_sent: std::sync::atomic::AtomicI64::new(0),
            packets_received: std::sync::atomic::AtomicI64::new(0),
            bytes_sent: std::sync::atomic::AtomicI64::new(0),
            bytes_received: std::sync::atomic::AtomicI64::new(0),
            disconnect: tokio::sync::Notify::new(),
            pending_action: std::sync::Mutex::new(crate::state::SessionAction::default()),
        };
        assert!(can_inject_packet(&base(776, false)));
        assert!(!can_inject_packet(&base(776, true)), "an encrypted session must never be injected into");
        assert!(!can_inject_packet(&base(1, false)), "an unverified/unsupported protocol version must never be injected into");
    }
}
