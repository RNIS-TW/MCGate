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

use crate::app_state::AppState;
use crate::backend_selector::order_backends;
use crate::buffered_stream::BufferedStream;
use crate::config::Route;
use crate::flood_control::{connection_rates, GlobalConnectionGuard};
use crate::handshake::HandshakeError;
use crate::minecraft_protocol::{encode_handshake, encode_proxy_protocol_header, encode_status_response};
use crate::ping_cache::PingCache;
use crate::route_metrics_store::route_metrics_store;
use crate::state::{player_sessions, PlayerSession, RouteRuntime};
use crate::status_json::build_fallback_json;
use crate::text_format::to_json_component;
use crate::varint::{read_var_int, write_var_int, MAX_PACKET_BYTES};

fn ping_cache() -> &'static PingCache {
    static INSTANCE: std::sync::OnceLock<PingCache> = std::sync::OnceLock::new();
    INSTANCE.get_or_init(PingCache::default)
}

pub async fn run(state: Arc<AppState>) -> anyhow::Result<()> {
    crate::ping_cache::spawn_sweeper(ping_cache());
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
            return;
        }
    }

    let _ = stream.set_nodelay(true);

    let mut effective_addr = peer_addr;
    if cfg.proxy_protocol {
        match crate::proxy_protocol_tcp::read_proxy_protocol_header(&mut stream).await {
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
    let Some(mut guard) = crate::connection_guard::PreLoginGuard::acquire(ip_string.as_deref(), per_ip_limit) else {
        tracing::debug!("Dropping connection from {effective_addr} - over per-IP pre-login limit ({per_ip_limit})");
        return;
    };

    let mut stream = BufferedStream::new(stream);
    let login_timeout = cfg.login_timeout_millis.max(0) as u64;

    let handshake_result = if login_timeout > 0 {
        match tokio::time::timeout(Duration::from_millis(login_timeout), stream.read_handshake()).await {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!("Closing {effective_addr} - did not complete login within {login_timeout} ms");
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

async fn resolve_ordered_backends(route: &Route, runtime: &RouteRuntime, captures: &[String]) -> Result<Vec<SocketAddr>, crate::dns_cache::DnsResolveError> {
    let resolved = route.resolve_backends(captures).await?;
    Ok(order_backends(route, runtime, &resolved))
}

async fn handle_status(
    stream: &mut BufferedStream<TcpStream>,
    route: &Route,
    runtime: &RouteRuntime,
    captures: &[String],
    handshake: &crate::handshake::Handshake,
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
async fn fetch_backend_status(route: &Route, addr: SocketAddr, handshake: &crate::handshake::Handshake, client_addr: SocketAddr) -> Option<String> {
    let connect = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await;
    let mut backend = match connect {
        Ok(Ok(s)) => s,
        _ => {
            tracing::debug!("Status dial to {addr} failed or timed out");
            return None;
        }
    };
    let _ = backend.set_nodelay(true);

    if route.proxy_protocol {
        // Never forward a degenerate source (loopback/wildcard/port 0) - a strict backend
        // RST-drops such a PROXY header, which would look exactly like a dead backend.
        if client_addr.port() != 0 && !client_addr.ip().is_unspecified() && !client_addr.ip().is_loopback() {
            let _ = tokio::io::AsyncWriteExt::write_all(&mut backend, &encode_proxy_protocol_header(client_addr, addr)).await;
        }
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
    let (json, _) = crate::varint::read_string(&frame[header_len + consumed..], 262_144).ok()?;
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
    if let Err(e) = tokio::io::AsyncWriteExt::write_all(stream, &crate::minecraft_protocol::encode_pong(payload)).await {
        tracing::debug!("Failed to send pong to {client_addr}: {e}");
    }
}

async fn handle_login(
    stream: &mut BufferedStream<TcpStream>,
    route: &Route,
    runtime: &Arc<RouteRuntime>,
    captures: &[String],
    handshake: &crate::handshake::Handshake,
    client_addr: SocketAddr,
    guard: &mut crate::connection_guard::PreLoginGuard,
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
    let parsed_login = crate::minecraft_protocol::parse_login_start(&login_start_frame);
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
                tracing::info!(
                    "Connected: '{}'{} from {client_addr} -> {addr}",
                    handshake.host,
                    player_name.as_deref().map(|n| format!(" ({n})")).unwrap_or_default()
                );

                register_voicechat_route(route, captures, client_addr, &handshake.host, state.config().log_connections).await;

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
                        compression_threshold: -1,
                        encrypted: false,
                        login_attempts: std::sync::atomic::AtomicU32::new(1),
                        packets_sent: std::sync::atomic::AtomicI64::new(0),
                        packets_received: std::sync::atomic::AtomicI64::new(0),
                        bytes_sent: std::sync::atomic::AtomicI64::new(0),
                        bytes_received: std::sync::atomic::AtomicI64::new(0),
                        disconnect: tokio::sync::Notify::new(),
                    });
                    player_sessions().put(session.clone());
                    Some(session)
                });

                let route_counter = route_metrics_store().handle(route);
                relay(stream, backend, session.as_deref(), route_counter.as_deref()).await;

                runtime.record_connect_closed(*addr);
                if let Some(session) = &session {
                    player_sessions().remove(session.uuid);
                    crate::connection_tracker::connection_tracker().record(crate::connection_tracker::ConnectionRecord {
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
                crate::voice_routing::voice_routing().unregister(&client_addr.ip().to_string());
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
    let packet = crate::reconnect_protocol::encode_login_disconnect(&to_json_component(message));
    let _ = tokio::io::AsyncWriteExt::write_all(stream, &packet).await;
}

async fn dial_backend(route: &Route, addr: SocketAddr, handshake: &crate::handshake::Handshake, client_addr: SocketAddr) -> std::io::Result<TcpStream> {
    let mut backend = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
    let _ = backend.set_nodelay(true);

    if route.proxy_protocol {
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

/// The actual byte splice, once a backend is connected. A custom loop (not
/// `tokio::io::copy_bidirectional`) because live per-session/per-route byte counters
/// (`PlayerSession`, `RouteMetricsStore`) need to see bytes as they flow, not only a final total
/// after the connection ends — and because a console/API `kick` needs a way to interrupt the
/// splice, via `session.disconnect`. Backpressure is still equivalent to
/// `copy_bidirectional`/Netty's water-mark-driven `pauseOrResumeReads`: each direction only reads
/// more once its own `write_all` call has returned, so a slow reader on one side naturally stalls
/// reads from the other via normal `AsyncRead`/`AsyncWrite` polling - nothing extra to port for
/// `FlowControl.kt`.
async fn relay(client: &mut BufferedStream<TcpStream>, backend: TcpStream, session: Option<&PlayerSession>, route_counter: Option<&crate::route_metrics_store::RouteTrafficCounter>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut client_r, mut client_w) = tokio::io::split(client);
    let (mut backend_r, mut backend_w) = tokio::io::split(backend);

    let upload = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = match client_r.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if let Some(s) = session {
                s.packets_sent.fetch_add(1, Ordering::Relaxed);
                s.bytes_sent.fetch_add(n as i64, Ordering::Relaxed);
            }
            if let Some(c) = route_counter {
                c.add_upload(n as i64);
            }
            if backend_w.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };
    let download = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = match backend_r.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if let Some(s) = session {
                s.packets_received.fetch_add(1, Ordering::Relaxed);
                s.bytes_received.fetch_add(n as i64, Ordering::Relaxed);
            }
            if let Some(c) = route_counter {
                c.add_download(n as i64);
            }
            if client_w.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };

    match session {
        Some(s) => {
            tokio::select! {
                _ = upload => {},
                _ = download => {},
                _ = s.disconnect.notified() => {
                    tracing::debug!("Session for '{}' interrupted by kick", s.name);
                },
            }
        }
        None => {
            tokio::select! {
                _ = upload => {},
                _ = download => {},
            }
        }
    }
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
            crate::voice_routing::voice_routing().register(&client_addr.ip().to_string(), host, voice_backend);
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

    #[test]
    fn is_valid_status_json_requires_numeric_protocol() {
        assert!(is_valid_status_json(r#"{"version":{"name":"1.21","protocol":767}}"#));
        assert!(!is_valid_status_json(r#"{"version":{"name":"1.21"}}"#));
        assert!(!is_valid_status_json(r#"{"version":{"protocol":"not-a-number"}}"#));
        assert!(!is_valid_status_json("not json at all"));
        assert!(!is_valid_status_json(r#"{"no_version_field":true}"#));
    }
}
