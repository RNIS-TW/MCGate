//! The TCP relay path: accepting connections, dialing/selecting backends, anti-abuse gating
//! (per-IP connection limits, DNS caching), and the read-only HTTP status/metrics API.

pub mod api;
pub mod backend_pinger;
pub mod backend_selector;
pub mod buffered_stream;
pub mod connection_guard;
pub mod dns_cache;
pub mod flood_control;
pub mod network_info;
pub mod ping_cache;
pub mod server;
