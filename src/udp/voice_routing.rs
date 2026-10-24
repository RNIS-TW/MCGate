//! Port of `voice/VoiceRouting.kt` — tracks which voicechat backend a connecting client's UDP
//! traffic (Simple Voice Chat and similar mods) should be relayed to, so `voice_relay.rs` can
//! route inbound datagrams without parsing any protocol of its own — UDP voice packets carry no
//! hostname the way the Minecraft handshake does. Populated from the TCP side (`server.rs`'s
//! login handling) the moment a route with a `voicechat:` backend is resolved for a connecting
//! player.
//!
//! Keyed by client IP only (not IP:port), same accepted limitation as the Kotlin version: two
//! different players behind the same NAT/router connecting to *different* hostnames routed to
//! different voicechat backends would clobber each other's entry, since a UDP voice packet
//! carries nothing else to disambiguate by.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long an entry can go un-looked-up before the periodic sweep evicts it — mirrors
/// `dns_cache.rs`'s idle eviction so a client that disconnects (or never sends any UDP traffic at
/// all) doesn't sit here forever.
const ENTRY_IDLE_EVICT: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct Entry {
    /// The Minecraft hostname the player connected with (the route's `host:` pattern), carried
    /// along purely so `voice_relay.rs` can name it in logs the same way the TCP side does — UDP
    /// itself never carries a hostname.
    pub host: String,
    pub backend: SocketAddr,
    last_seen: Instant,
    /// Live Minecraft connections from this IP currently holding the entry - several players
    /// behind one NAT/CGNAT address share it, and it must outlive all but the last of them.
    refs: u32,
}

#[derive(Default)]
pub struct VoiceRouting {
    entries: Mutex<HashMap<String, Entry>>,
    /// Set by `voice_relay::run` / cleared on stop — lets `unregister` tear down an active relay
    /// session the instant a player logs out, rather than waiting for the relay's own idle
    /// timeout to notice the client is gone. A plain callback (rather than holding a `VoiceRelay`
    /// type directly) avoids a circular module dependency between this and `voice_relay.rs`.
    disconnect_hook: Mutex<Option<Box<dyn Fn(&str) + Send + Sync>>>,
}

pub fn voice_routing() -> &'static VoiceRouting {
    static INSTANCE: OnceLock<VoiceRouting> = OnceLock::new();
    INSTANCE.get_or_init(VoiceRouting::default)
}

impl VoiceRouting {
    pub fn attach_relay(&self, hook: Option<Box<dyn Fn(&str) + Send + Sync>>) {
        *self.disconnect_hook.lock().unwrap() = hook;
    }

    pub fn register(&self, client_ip: &str, host: &str, backend: SocketAddr) {
        let mut entries = self.entries.lock().unwrap();
        let refs = entries.get(client_ip).map_or(0, |e| e.refs) + 1;
        entries.insert(client_ip.to_string(), Entry { host: host.to_string(), backend, last_seen: Instant::now(), refs });
    }

    pub fn resolve(&self, client_ip: &str) -> Option<Entry> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get_mut(client_ip)?;
        entry.last_seen = Instant::now();
        Some(entry.clone())
    }

    /// Releases one connection's hold on `client_ip`'s routing entry; once the last one from
    /// that IP is gone, drops the entry and immediately closes any live UDP relay session for it
    /// — so voice chat "logs out" together with the game connection instead of lingering for up
    /// to the relay's idle timeout, without cutting off other players still on the same IP.
    pub fn unregister(&self, client_ip: &str) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get_mut(client_ip) {
                if entry.refs > 1 {
                    entry.refs -= 1;
                    return;
                }
            }
            entries.remove(client_ip);
        }
        if let Some(hook) = self.disconnect_hook.lock().unwrap().as_ref() {
            hook(client_ip);
        }
    }

    fn evict_idle(&self) {
        let cutoff = Instant::now().checked_sub(ENTRY_IDLE_EVICT);
        let Some(cutoff) = cutoff else { return };
        self.entries.lock().unwrap().retain(|_, e| e.last_seen >= cutoff);
    }
}

/// Spawns the periodic idle-entry sweep. Call once at startup, from within a tokio runtime.
pub fn spawn_evictor() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(ENTRY_IDLE_EVICT);
        interval.tick().await;
        loop {
            interval.tick().await;
            voice_routing().evict_idle();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_resolve_returns_entry() {
        let routing = VoiceRouting::default();
        let backend: SocketAddr = "127.0.0.1:24454".parse().unwrap();
        routing.register("1.2.3.4", "voice.example.com", backend);
        let entry = routing.resolve("1.2.3.4").unwrap();
        assert_eq!(entry.host, "voice.example.com");
        assert_eq!(entry.backend, backend);
    }

    #[test]
    fn resolve_unknown_ip_returns_none() {
        let routing = VoiceRouting::default();
        assert!(routing.resolve("9.9.9.9").is_none());
    }

    #[test]
    fn unregister_removes_entry_and_calls_hook() {
        let routing = VoiceRouting::default();
        let backend: SocketAddr = "127.0.0.1:24454".parse().unwrap();
        routing.register("1.2.3.4", "voice.example.com", backend);

        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();
        routing.attach_relay(Some(Box::new(move |ip: &str| {
            assert_eq!(ip, "1.2.3.4");
            called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        })));

        routing.unregister("1.2.3.4");
        assert!(routing.resolve("1.2.3.4").is_none());
        assert!(called.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn shared_ip_entry_survives_until_last_connection_leaves() {
        let routing = VoiceRouting::default();
        let backend: SocketAddr = "127.0.0.1:24454".parse().unwrap();
        routing.register("1.2.3.4", "voice.example.com", backend);
        routing.register("1.2.3.4", "voice.example.com", backend);

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_clone = calls.clone();
        routing.attach_relay(Some(Box::new(move |_: &str| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })));

        routing.unregister("1.2.3.4");
        assert!(routing.resolve("1.2.3.4").is_some(), "second player on the same IP lost its voice route");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0, "relay session torn down while still in use");
        routing.unregister("1.2.3.4");
        assert!(routing.resolve("1.2.3.4").is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
