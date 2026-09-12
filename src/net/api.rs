//! Port of `api/ApiServer.kt` — a small read-only JSON-over-HTTP admin/status API.
//!
//! Endpoints and JSON field names/shapes are kept identical to the Kotlin version (camelCase
//! keys) so existing dashboards/scripts against it don't need changes. Every endpoint is now
//! fully real, backed by live `RouteRuntime`/`PlayerSessions`/`RouteMetricsStore` data — including
//! `GET /v1/routes/{i}/ping`, a genuine on-demand live backend dial (`backend_pinger.rs`).

use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::state::app_state::AppState;
use crate::config::Route;
use crate::state::{collect_metrics, MetricsSnapshot, PlayerSession};

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    let cfg = state.config();
    if !cfg.api.enabled {
        return Ok(());
    }
    let bind = cfg.api.bind_address()?;
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/v1/routes", get(routes_handler))
        .route("/v1/routes/:index", get(route_handler))
        .route("/v1/routes/:index/backends", get(backends_handler))
        .route("/v1/routes/:index/metrics", get(route_metrics_handler))
        .route("/v1/routes/:index/ping", get(ping_handler))
        .route("/v1/players", get(players_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("API listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}

fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let cfg = state.config();
    let Some(token) = cfg.api.token else { return Ok(()) };
    let expected = format!("Bearer {token}");
    let ok = headers.get(axum::http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()).map(|v| v == expected).unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err((StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response())
    }
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
}

async fn metrics_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, Query(params): Query<std::collections::HashMap<String, String>>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    let routes_for_lookup = cfg.routes.clone();
    let state_for_runtime = state.clone();
    let snapshot = collect_metrics(&cfg.routes, move |r| {
        routes_for_lookup.iter().position(|route| route == r).and_then(|i| state_for_runtime.route_runtime(i))
    })
    .await;
    if params.get("type").map(String::as_str) == Some("json") {
        Json(metrics_json(&snapshot, cfg.api.players_page_limit as usize)).into_response()
    } else {
        (
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            metrics_text(&snapshot),
        )
            .into_response()
    }
}

async fn routes_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    let routes: Vec<Value> = cfg.routes.iter().enumerate().map(|(i, r)| route_json(i, r)).collect();
    Json(json!({ "routes": routes })).into_response()
}

async fn route_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, AxumPath(index): AxumPath<usize>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    match cfg.routes.get(index) {
        Some(route) => Json(route_json(index, route)).into_response(),
        None => not_found(),
    }
}

async fn backends_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, AxumPath(index): AxumPath<usize>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    match cfg.routes.get(index) {
        None => not_found(),
        Some(route) => {
            let runtime = state.route_runtime(index);
            // Wildcard routes have no fixed backend address until a client connects and supplies
            // the captured segments, so live stats are only resolvable for routes whose backend
            // templates have no $N placeholders.
            let resolved = if route.host_patterns.iter().all(|p| p.wildcard_count == 0) {
                route.resolve_backends(&[]).await.ok()
            } else {
                None
            };
            let backends: Vec<Value> = route
                .backend_templates
                .iter()
                .enumerate()
                .map(|(i, template)| {
                    let addr = resolved.as_ref().and_then(|v| v.get(i).copied());
                    let active = addr.and_then(|a| runtime.as_ref().map(|r| r.active_connections_of(a)));
                    let latency = addr.and_then(|a| runtime.as_ref().and_then(|r| r.latency_of(a)));
                    json!({
                        "address": template,
                        "activeConnections": active,
                        "latencyMillis": latency,
                    })
                })
                .collect();
            Json(json!({ "backends": backends })).into_response()
        }
    }
}

async fn route_metrics_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, AxumPath(index): AxumPath<usize>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    let Some(route) = cfg.routes.get(index) else { return not_found() };
    let Some(counter) = crate::state::route_metrics_store::route_metrics_store().handle(route) else { return not_found() };
    use std::sync::atomic::Ordering;
    Json(json!({
        "index": index,
        "hosts": route.host_patterns.iter().map(|p| p.raw.clone()).collect::<Vec<_>>(),
        "file": *counter.file.lock().unwrap(),
        "upload": {
            "enabled": counter.upload_enabled.load(Ordering::Relaxed),
            "bytes": counter.upload_bytes.load(Ordering::Relaxed),
            "limit": counter.upload_limit.load(Ordering::Relaxed),
            "exceeded": counter.upload_exceeded(),
        },
        "download": {
            "enabled": counter.download_enabled.load(Ordering::Relaxed),
            "bytes": counter.download_bytes.load(Ordering::Relaxed),
            "limit": counter.download_limit.load(Ordering::Relaxed),
            "exceeded": counter.download_exceeded(),
        },
        "resetIntervalMillis": counter.reset_interval_millis.load(Ordering::Relaxed),
        "resetAt": counter.reset_at.load(Ordering::Relaxed),
    }))
    .into_response()
}

/// Process-wide cap on concurrent live `/ping` backend dials across all API connections —
/// mirrors `ApiHandler.livePingLimiter` (a `Semaphore(4)`).
fn live_ping_limiter() -> &'static tokio::sync::Semaphore {
    static INSTANCE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    INSTANCE.get_or_init(|| tokio::sync::Semaphore::new(4))
}

async fn ping_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, AxumPath(index): AxumPath<usize>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    let Some(route) = cfg.routes.get(index) else { return not_found() };
    if route.host_patterns.iter().any(|p| p.wildcard_count > 0) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "cannot ping a wildcard route without a concrete host"}))).into_response();
    }
    let Ok(_permit) = live_ping_limiter().try_acquire() else {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error": "too many concurrent live pings"}))).into_response();
    };
    let addrs = match route.resolve_backends(&[]).await {
        Ok(a) => a,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "failed to resolve backends"}))).into_response(),
    };
    let virtual_host = route.host_patterns.first().map(|p| p.raw.clone()).unwrap_or_default();
    let runtime = state.route_runtime(index);

    let mut pings = Vec::new();
    for (i, template) in route.backend_templates.iter().enumerate() {
        let addr = addrs[i];
        let result = crate::net::backend_pinger::ping_backend_live(addr, -1, &virtual_host, addr.port(), std::time::Duration::from_secs(5), route.proxy_protocol).await;
        match result {
            Ok(r) => {
                if let Some(rt) = &runtime {
                    rt.record_latency(addr, r.latency_millis);
                }
                let status: Value = serde_json::from_str(&r.status_json).unwrap_or(Value::Null);
                pings.push(json!({ "address": template, "online": true, "latencyMillis": r.latency_millis, "status": status }));
            }
            Err(e) => pings.push(json!({ "address": template, "online": false, "error": e.to_string() })),
        }
    }
    Json(json!({ "index": index, "pings": pings })).into_response()
}

#[derive(serde::Deserialize)]
struct PlayersQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn players_handler(State(state): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<PlayersQuery>) -> Response {
    if let Err(e) = check_auth(&state, &headers) {
        return e;
    }
    let cfg = state.config();
    let all = crate::state::player_sessions().all();
    let max_limit = cfg.api.players_page_limit.max(0) as usize;
    let limit = q.limit.unwrap_or(max_limit).min(max_limit);
    let offset = q.offset.unwrap_or(0);
    let page: Vec<Value> = if offset >= all.len() {
        Vec::new()
    } else {
        all[offset..(offset + limit).min(all.len())].iter().map(|p| player_json(p)).collect()
    };
    Json(json!({
        "total": all.len(),
        "offset": offset,
        "limit": limit,
        "players": page,
    }))
    .into_response()
}

fn route_json(index: usize, route: &Route) -> Value {
    json!({
        "index": index,
        "hosts": route.host_patterns.iter().map(|p| p.raw.clone()).collect::<Vec<_>>(),
        "backends": route.backend_templates,
        "strategy": strategy_name(route.strategy),
        "priority": route.priority,
        "cachePingTtlMillis": route.cache_ping_ttl_millis,
        "modifyVirtualHost": route.modify_virtual_host,
        "proxyProtocol": route.proxy_protocol,
        "hasFallback": route.fallback.is_some(),
        "hasMetrics": route.metrics.as_ref().map(|m| m.active()).unwrap_or(false),
    })
}

fn strategy_name(s: crate::config::Strategy) -> &'static str {
    use crate::config::Strategy::*;
    match s {
        Sequential => "sequential",
        Random => "random",
        RoundRobin => "round_robin",
        LeastConnections => "least_connections",
        LowestLatency => "lowest_latency",
    }
}

fn player_json(s: &PlayerSession) -> Value {
    json!({
        "uuid": s.uuid.to_string(),
        "name": s.name,
        "ip": s.remote_address,
        "host": s.host,
        "backend": s.backend.map(|b| b.to_string()),
        "protocolVersion": s.protocol_version,
        "connectedAt": s.connected_at_millis,
        "loginAttempts": s.login_attempts.load(std::sync::atomic::Ordering::Relaxed),
        "packetsSent": s.packets_sent.load(std::sync::atomic::Ordering::Relaxed),
        "packetsReceived": s.packets_received.load(std::sync::atomic::Ordering::Relaxed),
        "bytesSent": s.bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
        "bytesReceived": s.bytes_received.load(std::sync::atomic::Ordering::Relaxed),
        "compressionThreshold": s.compression_threshold,
        "encrypted": s.encrypted,
    })
}

fn metrics_json(s: &MetricsSnapshot, player_limit: usize) -> Value {
    json!({
        "playersOnline": s.players_online,
        "onlineByHost": s.online_by_host,
        "uptimeSeconds": s.uptime_seconds,
        "backends": s.backends.iter().map(|b| json!({
            "route": b.route_index,
            "hosts": b.hosts,
            "backend": b.backend,
            "activeConnections": b.active,
            "latencyMillis": b.latency_millis,
        })).collect::<Vec<_>>(),
        "routeMetrics": s.route_metrics.iter().map(|m| json!({
            "route": m.route_index,
            "hosts": m.hosts,
            "file": m.file,
            "upload": {"enabled": m.upload_enabled, "bytes": m.upload_bytes, "limit": m.upload_limit},
            "download": {"enabled": m.download_enabled, "bytes": m.download_bytes, "limit": m.download_limit},
            "resetIntervalMillis": m.reset_interval_millis,
            "resetAt": m.reset_at,
        })).collect::<Vec<_>>(),
        "playersTotal": s.players.len(),
        "players": s.players.iter().take(player_limit).map(|p| player_json(p)).collect::<Vec<_>>(),
        "connectionTracking": { "enabled": s.tracking_enabled },
    })
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn metrics_text(s: &MetricsSnapshot) -> String {
    let mut out = String::new();
    out.push_str("# HELP mcgate_players_online Currently connected players.\n");
    out.push_str("# TYPE mcgate_players_online gauge\n");
    out.push_str(&format!("mcgate_players_online {}\n", s.players_online));

    out.push_str("# HELP mcgate_host_players_online Currently connected players per virtual host.\n");
    out.push_str("# TYPE mcgate_host_players_online gauge\n");
    for (host, count) in &s.online_by_host {
        out.push_str(&format!("mcgate_host_players_online{{host=\"{}\"}} {count}\n", escape_label(host)));
    }

    out.push_str("# HELP mcgate_uptime_seconds Seconds since the process started.\n");
    out.push_str("# TYPE mcgate_uptime_seconds gauge\n");
    out.push_str(&format!("mcgate_uptime_seconds {}\n", s.uptime_seconds));

    out.push_str("# HELP mcgate_route_backend_active_connections Active relayed connections per backend.\n");
    out.push_str("# TYPE mcgate_route_backend_active_connections gauge\n");
    out.push_str("# HELP mcgate_route_backend_latency_ms Last-observed backend connect/ping latency.\n");
    out.push_str("# TYPE mcgate_route_backend_latency_ms gauge\n");
    for b in &s.backends {
        let hosts_label = escape_label(&b.hosts.join(","));
        let labels = format!("route=\"{}\",hosts=\"{hosts_label}\",backend=\"{}\"", b.route_index, escape_label(&b.backend));
        out.push_str(&format!("mcgate_route_backend_active_connections{{{labels}}} {}\n", b.active));
        if let Some(latency) = b.latency_millis {
            out.push_str(&format!("mcgate_route_backend_latency_ms{{{labels}}} {latency}\n"));
        }
    }

    out.push_str("# HELP mcgate_connection_tracking_enabled Whether connection-history tracking is enabled.\n");
    out.push_str("# TYPE mcgate_connection_tracking_enabled gauge\n");
    out.push_str(&format!("mcgate_connection_tracking_enabled {}\n", s.tracking_enabled as u8));

    if !s.route_metrics.is_empty() {
        out.push_str("# HELP mcgate_route_bytes_uploaded_total Cumulative client->backend bytes relayed per route.\n");
        out.push_str("# TYPE mcgate_route_bytes_uploaded_total counter\n");
        out.push_str("# HELP mcgate_route_bytes_downloaded_total Cumulative backend->client bytes relayed per route.\n");
        out.push_str("# TYPE mcgate_route_bytes_downloaded_total counter\n");
        out.push_str("# HELP mcgate_route_bytes_limit Configured byte ceiling per route/direction (absent when unlimited).\n");
        out.push_str("# TYPE mcgate_route_bytes_limit gauge\n");
        for m in &s.route_metrics {
            let labels = format!("route=\"{}\",hosts=\"{}\"", m.route_index, escape_label(&m.hosts.join(",")));
            if m.upload_enabled {
                out.push_str(&format!("mcgate_route_bytes_uploaded_total{{{labels}}} {}\n", m.upload_bytes));
                if m.upload_limit >= 0 {
                    out.push_str(&format!("mcgate_route_bytes_limit{{{labels},direction=\"upload\"}} {}\n", m.upload_limit));
                }
            }
            if m.download_enabled {
                out.push_str(&format!("mcgate_route_bytes_downloaded_total{{{labels}}} {}\n", m.download_bytes));
                if m.download_limit >= 0 {
                    out.push_str(&format!("mcgate_route_bytes_limit{{{labels},direction=\"download\"}} {}\n", m.download_limit));
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{GateConfig, Route};

    #[test]
    fn strategy_names_match_kotlin_lowercase_enum_names() {
        assert_eq!(strategy_name(crate::config::Strategy::RoundRobin), "round_robin");
        assert_eq!(strategy_name(crate::config::Strategy::Sequential), "sequential");
    }

    #[test]
    fn route_json_shape() {
        let route = Route {
            host_patterns: vec![crate::config::host_pattern::HostPattern::new("a.example.com")],
            backend_templates: vec!["127.0.0.1:25566".into()],
            strategy: crate::config::Strategy::Sequential,
            cache_ping_ttl_millis: 10_000,
            fallback: None,
            modify_virtual_host: false,
            proxy_protocol: false,
            priority: 0,
            reconnect: crate::config::ReconnectConfig::default(),
            kick_message: "kick".into(),
            voicechat_templates: vec![],
            metrics: None,
        };
        let json = route_json(0, &route);
        assert_eq!(json["index"], 0);
        assert_eq!(json["hosts"][0], "a.example.com");
        assert_eq!(json["hasMetrics"], false);
    }

    #[tokio::test]
    async fn metrics_json_empty_state_shape() {
        let cfg = GateConfig::default();
        let snapshot = collect_metrics(&cfg.routes, |_| None).await;
        let json = metrics_json(&snapshot, 500);
        assert_eq!(json["playersOnline"], 0);
        assert_eq!(json["playersTotal"], 0);
    }
}
