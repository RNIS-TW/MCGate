//! Port of `voice/VoiceRelay.kt` — relays UDP traffic (Simple Voice Chat and similar mods) on the
//! same port MCGate's Minecraft TCP listener binds, no extra port to open. UDP carries no
//! hostname the way the Minecraft handshake does, so which backend a datagram belongs to is
//! looked up by the sender's IP via `voice_routing.rs`, populated from the TCP side when a route
//! with a `voicechat:` backend is resolved for a client.
//!
//! When `expect_proxy_protocol` is set (the route's UDP `proxyProtocol` config — MCGate sits
//! behind an L4 proxy such as Cloudflare Spectrum that also fronts this UDP port), the fronting
//! proxy prepends a PROXY protocol (v1/v2) header to the **first** datagram of each origin-facing
//! flow (Cloudflare Spectrum, notably, does not repeat it on every packet). That header is
//! stripped and the real client address it reports is used for the `voice_routing` lookup and as
//! the session key — otherwise the datagram would look like it came from the fronting proxy's own
//! IP and never match a routing entry. Subsequent header-less datagrams on the same flow are
//! matched back to that session by their source address (`by_via`). Backend replies go to the
//! datagram's actual sender (the fronting proxy), which de-muxes them to the real client.
//!
//! Same NAT-style approach as `udp_proxy.rs`: each distinct client gets its own ephemeral
//! backend-facing UDP socket, so the backend sees every player as a distinct source address/port.
//! See `udp_proxy.rs`'s doc for why this port doesn't need Kotlin's pending-queue-during-
//! async-connect machinery — the same reasoning applies here.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::Mutex as AsyncMutex;

use crate::protocol::proxy_protocol_datagram::parse_proxy_protocol_header;
use crate::udp::throttle::udp_throttle;
use crate::udp::voice_routing::voice_routing;

const REAPER_INTERVAL: Duration = Duration::from_secs(15);
const MAX_DATAGRAM_SIZE: usize = 65_527;
/// Rate-limits the "datagram arrived but couldn't be relayed" diagnostic log to at most once
/// every 5s, so a mis-set voice backend / proxyProtocol config retrying rapidly can't flood even
/// debug logs.
const DROP_LOG_INTERVAL_MILLIS: i64 = 5_000;

struct Session {
    client_ip: String,
    host: String,
    created_at: Instant,
    last_active: Mutex<Instant>,
    via: Mutex<SocketAddr>,
    backend_replied: AtomicBool,
    backend_socket: Arc<UdpSocket>,
    dead: AtomicBool,
    /// The `spawn_backend_reader` task for this session. It otherwise blocks forever in
    /// `backend_socket.recv().await` — it only notices `dead` after a packet actually arrives —
    /// so once a session is closed (idle timeout, no-reply teardown, or the player logging out)
    /// the task and its `backend_socket`/buffer would leak indefinitely unless aborted here.
    reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Shared {
    public: Arc<UdpSocket>,
    expect_proxy_protocol: bool,
    /// Keyed by the *client* address: the datagram's own source normally, or the address from
    /// the PROXY header under `expect_proxy_protocol`. This is what `disconnect_client` matches
    /// on (by IP).
    sessions: Mutex<HashMap<SocketAddr, Arc<Session>>>,
    /// `expect_proxy_protocol` only: sessions indexed by their current `via` (the fronting
    /// proxy's source address), so a header-less follow-up datagram can still be matched to the
    /// session its flow's first (headered) datagram established.
    by_via: Mutex<HashMap<SocketAddr, Arc<Session>>>,
    sessions_per_ip: Mutex<HashMap<String, i32>>,
    create_lock: AsyncMutex<()>,
    last_drop_log_at: AtomicI64,
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn log_drop(shared: &Shared, reason: &str, from: SocketAddr) {
    let now = now_millis();
    let last = shared.last_drop_log_at.load(Ordering::Relaxed);
    if now - last < DROP_LOG_INTERVAL_MILLIS {
        return;
    }
    shared.last_drop_log_at.store(now, Ordering::Relaxed);
    tracing::debug!("Voicechat datagram from {from} dropped: {reason}");
}

/// Runs the voice relay until the process ends (there's no separate "stop" call in this port —
/// see plan.md). `expect_proxy_protocol` is the route-independent UDP-block config
/// (`GateConfig.udp.proxy_protocol`), matching the Kotlin version's single relay instance shared
/// across every voicechat-enabled route.
pub async fn run(bind_addr: SocketAddr, expect_proxy_protocol: bool) -> anyhow::Result<()> {
    let public = Arc::new(UdpSocket::bind(bind_addr).await?);
    tracing::info!("Relaying UDP (voicechat) on {bind_addr}");

    let shared = Arc::new(Shared {
        public: public.clone(),
        expect_proxy_protocol,
        sessions: Mutex::new(HashMap::new()),
        by_via: Mutex::new(HashMap::new()),
        sessions_per_ip: Mutex::new(HashMap::new()),
        create_lock: AsyncMutex::new(()),
        last_drop_log_at: AtomicI64::new(0),
    });

    voice_routing().attach_relay(Some({
        let shared = shared.clone();
        Box::new(move |client_ip: &str| disconnect_client(&shared, client_ip))
    }));

    {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(REAPER_INTERVAL);
            loop {
                interval.tick().await;
                evict_stale_sessions(&shared);
            }
        });
    }

    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
    loop {
        let (n, via) = match public.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("Voicechat public channel error: {e}");
                continue;
            }
        };
        handle_client_packet(&shared, via, buf[..n].to_vec()).await;
    }
}

/// Closes any live relay session for `client_ip` right away — called via `VoiceRouting::unregister`
/// when a player's Minecraft connection ends.
fn disconnect_client(shared: &Arc<Shared>, client_ip: &str) {
    let to_close: Vec<SocketAddr> = shared.sessions.lock().unwrap().keys().filter(|addr| addr.ip().to_string() == client_ip).copied().collect();
    for addr in to_close {
        let removed = shared.sessions.lock().unwrap().remove(&addr);
        if let Some(session) = removed {
            close_session(shared, &session);
        }
    }
}

async fn handle_client_packet(shared: &Arc<Shared>, via: SocketAddr, content: Vec<u8>) {
    let client_addr: SocketAddr = if shared.expect_proxy_protocol {
        match parse_proxy_protocol_header(&content) {
            Ok((Some(addr), header_len)) => {
                let payload = content[header_len..].to_vec();
                route_headered_datagram(shared, via, addr, payload).await;
                return;
            }
            Ok((None, _)) => return, // v2 LOCAL / UNSPEC - a health check, nothing to route
            Err(_) => {
                // Header-less datagram: every packet after the first, with Cloudflare Spectrum -
                // match it back to an already-open session by its source address.
                let known = shared.by_via.lock().unwrap().get(&via).cloned();
                match known {
                    Some(session) if !session.dead.load(Ordering::Relaxed) => {
                        *session.last_active.lock().unwrap() = Instant::now();
                        forward(&session, &content).await;
                    }
                    _ => log_drop(
                        shared,
                        "header-less datagram from an unknown flow (proxyProtocol is on; the fronting proxy sends the PROXY header only on a flow's first datagram - was it lost, or is the proxy not sending one at all?)",
                        via,
                    ),
                }
                return;
            }
        }
    } else {
        via
    };
    handle_for_client(shared, client_addr, via, content).await;
}

/// A datagram that carried a PROXY header this time - repoint `by_via` if the fronting proxy's
/// source changed, then handle as normal.
async fn route_headered_datagram(shared: &Arc<Shared>, via: SocketAddr, client_addr: SocketAddr, content: Vec<u8>) {
    // Each lookup below is bound to a `let` first (not matched inline in the `if let`) so the
    // `MutexGuard` temporary drops at the end of that statement rather than being
    // lifetime-extended across the whole `if let` block, which would otherwise hold it live
    // across the `.await` calls that follow - see udp_proxy.rs's identical fix for the same
    // pattern.
    let existing = shared.sessions.lock().unwrap().get(&client_addr).cloned();
    if let Some(existing) = existing {
        *existing.last_active.lock().unwrap() = Instant::now();
        repoint_via(shared, &existing, via);
        forward(&existing, &content).await;
        return;
    }
    handle_for_client(shared, client_addr, via, content).await;
}

async fn handle_for_client(shared: &Arc<Shared>, client_addr: SocketAddr, via: SocketAddr, content: Vec<u8>) {
    let existing = shared.sessions.lock().unwrap().get(&client_addr).cloned();
    if let Some(existing) = existing {
        *existing.last_active.lock().unwrap() = Instant::now();
        repoint_via(shared, &existing, via);
        forward(&existing, &content).await;
        return;
    }

    let _guard = shared.create_lock.lock().await;
    let existing = shared.sessions.lock().unwrap().get(&client_addr).cloned();
    if let Some(existing) = existing {
        drop(_guard);
        *existing.last_active.lock().unwrap() = Instant::now();
        repoint_via(shared, &existing, via);
        forward(&existing, &content).await;
        return;
    }

    let client_ip = client_addr.ip().to_string();
    let Some(route) = voice_routing().resolve(&client_ip) else {
        log_drop(
            shared,
            &if shared.expect_proxy_protocol {
                format!("no voicechat route for client {client_ip} (from PROXY header) - is that the player's real IP, and did they log in through a voicechat: route?")
            } else {
                format!("no voicechat route for {client_ip} - did this client log in through a voicechat: route? (if MCGate is behind an L4 proxy, enable proxyProtocol)")
            },
            via,
        );
        return;
    };

    let throttle = udp_throttle();
    let global_cap = throttle.max_sessions();
    if global_cap > 0 && shared.sessions.lock().unwrap().len() as i32 >= global_cap {
        log_drop(shared, &format!("relay is at its session cap ({global_cap})"), via);
        return;
    }
    if !acquire_ip_slot(shared, &client_ip) {
        log_drop(shared, &format!("too many voicechat sessions from {client_ip} (cap {})", throttle.max_sessions_per_ip()), via);
        return;
    }

    tracing::info!("Opening voicechat relay session: '{}' from {client_addr} (via {via}) -> {}", route.host, route.backend);

    let backend_socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => match s.connect(route.backend).await {
            Ok(()) => Arc::new(s),
            Err(e) => {
                tracing::warn!("Failed to open voicechat relay session for '{}' {client_addr} -> {}: {e}", route.host, route.backend);
                release_ip_slot(shared, &client_ip);
                return;
            }
        },
        Err(e) => {
            tracing::warn!("Failed to open voicechat relay session for '{}' {client_addr} -> {}: {e}", route.host, route.backend);
            release_ip_slot(shared, &client_ip);
            return;
        }
    };

    let session = Arc::new(Session {
        client_ip,
        host: route.host,
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        via: Mutex::new(via),
        backend_replied: AtomicBool::new(false),
        backend_socket: backend_socket.clone(),
        dead: AtomicBool::new(false),
        reader_handle: Mutex::new(None),
    });
    shared.sessions.lock().unwrap().insert(client_addr, session.clone());
    if shared.expect_proxy_protocol {
        shared.by_via.lock().unwrap().insert(via, session.clone());
    }
    drop(_guard);

    let handle = spawn_backend_reader(shared.clone(), client_addr, route.backend, session.clone(), backend_socket);
    *session.reader_handle.lock().unwrap() = Some(handle);
    forward(&session, &content).await;
}

fn repoint_via(shared: &Shared, session: &Arc<Session>, new_via: SocketAddr) {
    if !shared.expect_proxy_protocol {
        *session.via.lock().unwrap() = new_via;
        return;
    }
    let mut current = session.via.lock().unwrap();
    if *current == new_via {
        return;
    }
    let mut by_via = shared.by_via.lock().unwrap();
    by_via.remove(&*current);
    *current = new_via;
    by_via.insert(new_via, session.clone());
}

fn acquire_ip_slot(shared: &Shared, ip: &str) -> bool {
    let max = udp_throttle().max_sessions_per_ip();
    if max <= 0 {
        return true;
    }
    let mut counts = shared.sessions_per_ip.lock().unwrap();
    let count = counts.entry(ip.to_string()).or_insert(0);
    *count += 1;
    if *count > max {
        *count -= 1;
        if *count <= 0 {
            counts.remove(ip);
        }
        false
    } else {
        true
    }
}

fn release_ip_slot(shared: &Shared, ip: &str) {
    let mut counts = shared.sessions_per_ip.lock().unwrap();
    if let Some(count) = counts.get_mut(ip) {
        *count -= 1;
        if *count <= 0 {
            counts.remove(ip);
        }
    }
}

async fn forward(session: &Session, content: &[u8]) {
    if session.dead.load(Ordering::Relaxed) {
        return;
    }
    let _ = session.backend_socket.send(content).await;
}

fn spawn_backend_reader(shared: Arc<Shared>, client_addr: SocketAddr, backend_addr: SocketAddr, session: Arc<Session>, backend_socket: Arc<UdpSocket>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
        let mut replied_once = false;
        loop {
            let n = match backend_socket.recv(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    // Most commonly a connection-refused/unreachable error surfacing here for a
                    // *connected* datagram socket. Deliberately does NOT close or evict the
                    // session (see the Kotlin doc this ports from) - a single rejected packet
                    // doesn't mean the whole voice session is dead; only idle timeout or the
                    // player logging out tears a session down.
                    tracing::info!("Voicechat backend {backend_addr} session error for '{}' {client_addr}: {e}", session.host);
                    continue;
                }
            };
            if session.dead.load(Ordering::Relaxed) {
                break;
            }
            *session.last_active.lock().unwrap() = Instant::now();
            if !replied_once {
                replied_once = true;
                session.backend_replied.store(true, Ordering::Relaxed);
                tracing::debug!("Voicechat backend {backend_addr} replied for the first time to '{}' {client_addr}", session.host);
            }
            let via = *session.via.lock().unwrap();
            let _ = shared.public.send_to(&buf[..n], via).await;
        }
    })
}

fn close_session(shared: &Shared, session: &Session) {
    // Remove this session from by_via regardless of its current `via` key (it may have been
    // repointed since creation) rather than looking it up by the possibly-stale key.
    shared.by_via.lock().unwrap().retain(|_, s| !std::ptr::eq(s.as_ref(), session));
    session.dead.store(true, Ordering::Relaxed);
    release_ip_slot(shared, &session.client_ip);
    // The reader task otherwise blocks forever in `backend_socket.recv().await` once the backend
    // stops sending - it only notices `dead` after a packet arrives - so abort it explicitly
    // rather than leaking the task, its buffer, and the backend socket fd.
    if let Some(handle) = session.reader_handle.lock().unwrap().take() {
        handle.abort();
    }
}

fn evict_stale_sessions(shared: &Arc<Shared>) {
    let throttle = udp_throttle();
    let idle_cutoff = Duration::from_millis(throttle.idle_timeout_millis().max(0) as u64);
    let no_reply = throttle.no_reply_teardown_millis();
    let now = Instant::now();

    let stale: Vec<SocketAddr> = {
        let sessions = shared.sessions.lock().unwrap();
        sessions
            .iter()
            .filter(|(_, s)| {
                let idle = now.duration_since(*s.last_active.lock().unwrap()) > idle_cutoff;
                let no_reply_expired = !s.backend_replied.load(Ordering::Relaxed) && no_reply > 0 && now.duration_since(s.created_at).as_millis() as i64 > no_reply;
                idle || no_reply_expired
            })
            .map(|(addr, _)| *addr)
            .collect()
    };
    for addr in stale {
        let removed = shared.sessions.lock().unwrap().remove(&addr);
        if let Some(session) = removed {
            close_session(shared, &session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udp::voice_routing::voice_routing;

    // Both scenarios below share `voice_routing()`'s global singleton, keyed by client IP - and
    // every loopback test client is "127.0.0.1", so running them as two separate `#[tokio::test]`
    // functions raced: cargo runs tests in parallel by default, and one test's `register`/
    // `unregister` could land mid-flight of the other, causing intermittent failures neither
    // test's own logic was actually wrong about. Combined into one sequential test - register,
    // verify relay, unregister, verify drop - so there is no cross-test interleaving possible.
    #[tokio::test]
    async fn relays_when_registered_and_drops_once_unregistered() {
        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                let Ok((n, from)) = backend_socket.recv_from(&mut buf).await else { break };
                let _ = backend_socket.send_to(&buf[..n], from).await;
            }
        });

        let relay_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = UdpSocket::bind(relay_bind).await.unwrap();
        let relay_addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(run(relay_addr, false));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_ip = client.local_addr().unwrap().ip().to_string();

        // Registered: a real datagram relays both ways.
        voice_routing().register(&client_ip, "voice.example.com", backend_addr);
        client.send_to(b"voice-packet", relay_addr).await.unwrap();
        let mut buf = [0u8; 1024];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(&buf[..n], b"voice-packet");

        // Unregistered: the same client IP now gets dropped, not panicking or relayed.
        voice_routing().unregister(&client_ip);
        client.send_to(b"should-be-dropped", relay_addr).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(300), client.recv_from(&mut buf)).await;
        assert!(result.is_err(), "expected no reply for an unregistered client");
    }

    /// Regression test for the leak this fix addresses: without a backend ever sending another
    /// packet, `spawn_backend_reader`'s task used to sit parked in `recv().await` forever once its
    /// session was closed, silently leaking the task, its buffer, and the backend socket fd on
    /// every session that ends without further backend traffic (the common case). Asserts the
    /// reader task actually terminates promptly on `close_session`, not just that routing behaves
    /// correctly afterward.
    #[tokio::test]
    async fn closing_a_session_actually_terminates_its_backend_reader_task() {
        // A backend that never sends anything back - the exact condition that used to hang the
        // reader task forever.
        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();

        let shared = Arc::new(Shared {
            public: Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()),
            expect_proxy_protocol: false,
            sessions: Mutex::new(HashMap::new()),
            by_via: Mutex::new(HashMap::new()),
            sessions_per_ip: Mutex::new(HashMap::new()),
            create_lock: AsyncMutex::new(()),
            last_drop_log_at: AtomicI64::new(0),
        });

        let client_ip = "127.0.0.2";
        voice_routing().register(client_ip, "voice.example.com", backend_addr);
        let client_addr: SocketAddr = format!("{client_ip}:40000").parse().unwrap();
        handle_for_client(&shared, client_addr, client_addr, b"hi".to_vec()).await;

        let session = shared.sessions.lock().unwrap().get(&client_addr).cloned().unwrap();
        let abort_handle = session.reader_handle.lock().unwrap().as_ref().unwrap().abort_handle();
        assert!(!abort_handle.is_finished(), "reader task should still be running while the session is live");

        close_session(&shared, &session);
        // abort() only requests cancellation; give the runtime a tick to actually drop the task.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(abort_handle.is_finished(), "reader task must terminate as soon as its session is closed, even with no further backend traffic");

        voice_routing().unregister(client_ip);
    }
}
