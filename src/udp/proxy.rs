//! Port of `udp/UdpProxy.kt` — a plain static UDP forwarder: every datagram arriving on
//! `bind` is relayed to `backend`, keyed by full source `ip:port` (not just IP, unlike
//! `voice_relay.rs`) since there's no Minecraft login to gate on here.
//!
//! Same NAT-style approach as the voice relay: each distinct client source gets its own
//! ephemeral backend-facing socket, so the backend sees every client as a distinct address, same
//! as if this proxy weren't in the middle.
//!
//! **Simpler than the Kotlin version in one respect**: Netty's `Bootstrap.connect()` on a
//! datagram channel is scheduled asynchronously (even though UDP "connect" never touches the
//! network) purely for API consistency, which is why the Kotlin version needs a `pending` queue
//! to hold datagrams that arrive while that connect is still in flight. `tokio::net::UdpSocket`'s
//! `connect()` completes immediately (it's just a local socket-option call, `connect(2)` on a
//! `SOCK_DGRAM` socket doesn't perform a network round trip) — awaiting it before returning from
//! the packet handler means a session's backend socket always exists before any subsequent
//! datagram for that session is processed, so there's no pending-queue race to guard against.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::Mutex as AsyncMutex;

use crate::config::UdpProxyConfig;
use crate::udp::throttle::udp_throttle;

const REAPER_INTERVAL: Duration = Duration::from_secs(15);
const MAX_DATAGRAM_SIZE: usize = 65_527; // max UDP payload size

struct Session {
    client_ip: String,
    created_at: Instant,
    last_active: Mutex<Instant>,
    backend_replied: AtomicBool,
    backend_socket: Arc<UdpSocket>,
    dead: AtomicBool,
    /// The `spawn_backend_reader` task for this session. It blocks in
    /// `backend_socket.recv().await` and only notices `dead` after a packet actually arrives, so
    /// once a session is evicted the task (and its buffer/socket) would otherwise leak
    /// indefinitely unless aborted here - see `close_session`.
    reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Shared {
    config: UdpProxyConfig,
    public: Arc<UdpSocket>,
    sessions: Mutex<HashMap<SocketAddr, Arc<Session>>>,
    sessions_per_ip: Mutex<HashMap<String, i32>>,
    // Serializes session-creation so two datagrams racing in for the same brand-new sender don't
    // both try to open a backend socket - matches the Kotlin version's putIfAbsent race guard,
    // implemented here as a lock around the whole "check-then-create" sequence instead.
    create_lock: AsyncMutex<()>,
}

pub async fn run(config: UdpProxyConfig) -> anyhow::Result<()> {
    let bind_addr = config.bind_address()?;
    let backend_addr = config.backend_address()?;
    let public = Arc::new(UdpSocket::bind(bind_addr).await?);
    tracing::info!("Relaying UDP {bind_addr} -> {backend_addr}");

    let shared = Arc::new(Shared {
        config,
        public: public.clone(),
        sessions: Mutex::new(HashMap::new()),
        sessions_per_ip: Mutex::new(HashMap::new()),
        create_lock: AsyncMutex::new(()),
    });

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
        let (n, sender) = match public.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("UDP proxy public channel error: {e}");
                continue;
            }
        };
        handle_client_packet(&shared, backend_addr, sender, buf[..n].to_vec()).await;
    }
}

async fn handle_client_packet(shared: &Arc<Shared>, backend_addr: SocketAddr, sender: SocketAddr, content: Vec<u8>) {
    let existing = shared.sessions.lock().unwrap().get(&sender).cloned();
    if let Some(session) = existing {
        *session.last_active.lock().unwrap() = Instant::now();
        forward(&session, &content).await;
        return;
    }

    // Serialize session creation for a given brand-new sender so two datagrams racing in don't
    // both open a backend socket / double-reserve a per-IP slot.
    let _guard = shared.create_lock.lock().await;
    // Re-check under the lock - another task may have created the session while we waited.
    // Bound to a `let` (not matched inline in the `if let`) so the `MutexGuard` temporary drops
    // at the end of this statement rather than being lifetime-extended across the whole `if let`
    // block, which would otherwise still be held live across the `.await` below.
    let existing = shared.sessions.lock().unwrap().get(&sender).cloned();
    if let Some(session) = existing {
        *session.last_active.lock().unwrap() = Instant::now();
        drop(_guard);
        forward(&session, &content).await;
        return;
    }

    let throttle = udp_throttle();
    let global_cap = throttle.max_sessions();
    if global_cap > 0 && shared.sessions.lock().unwrap().len() as i32 >= global_cap {
        return;
    }
    let ip = sender.ip().to_string();
    let per_ip_cap = throttle.max_sessions_per_ip();
    if per_ip_cap > 0 {
        let mut counts = shared.sessions_per_ip.lock().unwrap();
        let count = counts.entry(ip.clone()).or_insert(0);
        *count += 1;
        if *count > per_ip_cap {
            *count -= 1;
            if *count <= 0 {
                counts.remove(&ip);
            }
            return;
        }
    }

    if shared.config.log_sessions {
        tracing::info!("Opening UDP relay session: {sender} -> {backend_addr}");
    } else {
        tracing::debug!("Opening UDP relay session: {sender} -> {backend_addr}");
    }

    // connect() on a UDP socket is a local, non-blocking operation (no network round trip) -
    // awaiting it here still keeps a session's backend socket fully ready before this handler
    // returns, so no subsequent datagram for this session can race ahead of it (see this
    // module's doc for why that removes the need for Kotlin's pending-queue-during-async-connect
    // machinery entirely).
    let backend_socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => match s.connect(backend_addr).await {
            Ok(()) => Arc::new(s),
            Err(e) => {
                tracing::warn!("Failed to open UDP relay session for {sender} -> {backend_addr}: {e}");
                release_ip_slot(shared, &ip);
                return;
            }
        },
        Err(e) => {
            tracing::warn!("Failed to open UDP relay session for {sender} -> {backend_addr}: {e}");
            release_ip_slot(shared, &ip);
            return;
        }
    };

    let session = Arc::new(Session {
        client_ip: ip,
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        backend_replied: AtomicBool::new(false),
        backend_socket: backend_socket.clone(),
        dead: AtomicBool::new(false),
        reader_handle: Mutex::new(None),
    });
    shared.sessions.lock().unwrap().insert(sender, session.clone());
    drop(_guard);

    let handle = spawn_backend_reader(shared.clone(), sender, session.clone(), backend_socket);
    *session.reader_handle.lock().unwrap() = Some(handle);
    forward(&session, &content).await;
}

fn spawn_backend_reader(shared: Arc<Shared>, client_addr: SocketAddr, session: Arc<Session>, backend_socket: Arc<UdpSocket>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
        loop {
            let n = match backend_socket.recv(&mut buf).await {
                Ok(n) => n,
                Err(_) => break, // session torn down (socket dropped/closed) or backend error
            };
            if session.dead.load(Ordering::Relaxed) {
                break;
            }
            *session.last_active.lock().unwrap() = Instant::now();
            session.backend_replied.store(true, Ordering::Relaxed);
            let _ = shared.public.send_to(&buf[..n], client_addr).await;
        }
    })
}

async fn forward(session: &Session, content: &[u8]) {
    if session.dead.load(Ordering::Relaxed) {
        return;
    }
    let _ = session.backend_socket.send(content).await;
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

fn close_session(shared: &Shared, session: &Session) {
    session.dead.store(true, Ordering::Relaxed);
    release_ip_slot(shared, &session.client_ip);
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
    use crate::config::UdpProxyConfig;

    /// Real end-to-end test: a fake UDP "backend" echoes datagrams back; the proxy relays a real
    /// client's datagram to it and the reply back to the client, over actual loopback sockets.
    #[tokio::test]
    async fn relays_datagrams_both_directions_over_real_sockets() {
        // Fake backend: echoes whatever it receives back to the sender.
        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                let Ok((n, from)) = backend_socket.recv_from(&mut buf).await else { break };
                let _ = backend_socket.send_to(&buf[..n], from).await;
            }
        });

        let proxy_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = UdpSocket::bind(proxy_bind).await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        drop(listener); // free the port, then let `run` rebind it - avoids a second config type

        let config = UdpProxyConfig { bind: proxy_addr.to_string(), backend: backend_addr.to_string(), log_sessions: true };
        tokio::spawn(async move {
            let _ = run(config).await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await; // let the proxy finish binding

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"hello", proxy_addr).await.unwrap();

        let mut buf = [0u8; 1024];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    /// Regression test for the leak this fix addresses: without the backend ever sending another
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
            config: UdpProxyConfig { bind: "127.0.0.1:0".into(), backend: backend_addr.to_string(), log_sessions: false },
            public: Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()),
            sessions: Mutex::new(HashMap::new()),
            sessions_per_ip: Mutex::new(HashMap::new()),
            create_lock: AsyncMutex::new(()),
        });

        let sender: SocketAddr = "127.0.0.1:40001".parse().unwrap();
        handle_client_packet(&shared, backend_addr, sender, b"hi".to_vec()).await;

        let session = shared.sessions.lock().unwrap().get(&sender).cloned().unwrap();
        let abort_handle = session.reader_handle.lock().unwrap().as_ref().unwrap().abort_handle();
        assert!(!abort_handle.is_finished(), "reader task should still be running while the session is live");

        close_session(&shared, &session);
        // abort() only requests cancellation; give the runtime a tick to actually drop the task.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(abort_handle.is_finished(), "reader task must terminate as soon as its session is closed, even with no further backend traffic");
    }
}
