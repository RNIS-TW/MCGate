//! Port of `routing/BackendSelector.kt`'s `orderBackends` — the `RouteRuntime` state it reads
//! now lives in `state.rs` (extended there rather than duplicated here, since section 7's
//! console/API code already depends on that type).

use std::net::SocketAddr;

use rand::seq::SliceRandom;

use crate::config::{Route, Strategy};
use crate::state::RouteRuntime;

/// Orders `backends` by the route's strategy; caller should try each in order until one connects.
pub fn order_backends(route: &Route, runtime: &RouteRuntime, backends: &[SocketAddr]) -> Vec<SocketAddr> {
    if backends.len() <= 1 {
        return backends.to_vec();
    }
    match route.strategy {
        Strategy::Sequential => backends.to_vec(),
        Strategy::Random => {
            let mut shuffled = backends.to_vec();
            shuffled.shuffle(&mut rand::thread_rng());
            shuffled
        }
        Strategy::RoundRobin => {
            let counter = runtime.round_robin_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let start = counter.rem_euclid(backends.len() as i64) as usize;
            let mut ordered = backends[start..].to_vec();
            ordered.extend_from_slice(&backends[..start]);
            ordered
        }
        Strategy::LeastConnections => {
            let mut ordered = backends.to_vec();
            ordered.sort_by_key(|addr| runtime.active_connections_of(*addr));
            ordered
        }
        Strategy::LowestLatency => {
            let mut ordered = backends.to_vec();
            ordered.sort_by_key(|addr| runtime.latency_of(*addr).unwrap_or(i64::MAX));
            ordered
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addrs(n: usize) -> Vec<SocketAddr> {
        (0..n).map(|i| format!("127.0.0.1:{}", 25000 + i).parse().unwrap()).collect()
    }

    #[test]
    fn single_backend_returned_unordered() {
        let runtime = RouteRuntime::default();
        let backends = addrs(1);
        assert_eq!(order_backends(&sequential_route(), &runtime, &backends), backends);
    }

    fn sequential_route() -> Route {
        crate::config::Route {
            host_patterns: vec![],
            backend_templates: vec![],
            strategy: Strategy::Sequential,
            cache_ping_ttl_millis: 10_000,
            fallback: None,
            modify_virtual_host: false,
            proxy_protocol: false,
            priority: 0,
            reconnect: crate::config::ReconnectConfig::default(),
            kick_message: String::new(),
            voicechat_templates: vec![],
            metrics: None,
        }
    }

    fn route_with_strategy(strategy: Strategy) -> Route {
        Route { strategy, ..sequential_route() }
    }

    #[test]
    fn sequential_preserves_order() {
        let runtime = RouteRuntime::default();
        let backends = addrs(3);
        assert_eq!(order_backends(&route_with_strategy(Strategy::Sequential), &runtime, &backends), backends);
    }

    #[test]
    fn round_robin_rotates_start_each_call() {
        let runtime = RouteRuntime::default();
        let backends = addrs(3);
        let route = route_with_strategy(Strategy::RoundRobin);
        let first = order_backends(&route, &runtime, &backends);
        let second = order_backends(&route, &runtime, &backends);
        let third = order_backends(&route, &runtime, &backends);
        assert_eq!(first, vec![backends[0], backends[1], backends[2]]);
        assert_eq!(second, vec![backends[1], backends[2], backends[0]]);
        assert_eq!(third, vec![backends[2], backends[0], backends[1]]);
    }

    #[test]
    fn least_connections_sorts_ascending() {
        let runtime = RouteRuntime::default();
        let backends = addrs(3);
        runtime.record_connect_opened(backends[0]);
        runtime.record_connect_opened(backends[0]);
        runtime.record_connect_opened(backends[1]);
        let ordered = order_backends(&route_with_strategy(Strategy::LeastConnections), &runtime, &backends);
        assert_eq!(ordered[0], backends[2]); // 0 active
        assert_eq!(ordered[1], backends[1]); // 1 active
        assert_eq!(ordered[2], backends[0]); // 2 active
    }

    #[test]
    fn lowest_latency_sorts_ascending_with_unknown_last() {
        let runtime = RouteRuntime::default();
        let backends = addrs(3);
        runtime.record_latency(backends[0], 100);
        runtime.record_latency(backends[1], 10);
        // backends[2] has no recorded latency - should sort last.
        let ordered = order_backends(&route_with_strategy(Strategy::LowestLatency), &runtime, &backends);
        assert_eq!(ordered, vec![backends[1], backends[0], backends[2]]);
    }

    #[test]
    fn random_contains_same_elements() {
        let runtime = RouteRuntime::default();
        let backends = addrs(5);
        let mut ordered = order_backends(&route_with_strategy(Strategy::Random), &runtime, &backends);
        ordered.sort();
        let mut expected = backends.clone();
        expected.sort();
        assert_eq!(ordered, expected);
    }
}
