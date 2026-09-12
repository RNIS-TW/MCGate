//! Port of `me.hippodev.config.Config` (GateConfig / Route / friends) and YAML loading.
//!
//! Deliberately parses the YAML as a loosely-typed `serde_yaml::Value` tree (mirroring the
//! Kotlin `Map<String, Any>` approach) rather than deriving `Deserialize` on the config structs
//! directly, because most fields are optional-with-defaults and a few need custom parsing
//! (durations, byte sizes, `host:`/`backend:` as either a string or a list). This keeps every
//! field's default and validation behavior identical to the original.

pub mod duration;
pub mod host_pattern;
pub mod loader;
pub mod messages;

use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use serde_yaml::Value;

use duration::{parse_byte_size, parse_duration_millis};
use host_pattern::HostPattern;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Sequential,
    Random,
    RoundRobin,
    LeastConnections,
    LowestLatency,
}

impl Strategy {
    fn parse(value: Option<&str>) -> Result<Self> {
        let Some(v) = value else { return Ok(Strategy::Sequential) };
        Ok(match v.to_lowercase().as_str() {
            "sequential" => Strategy::Sequential,
            "random" => Strategy::Random,
            "round-robin" | "round_robin" => Strategy::RoundRobin,
            "least-connections" | "least_connections" => Strategy::LeastConnections,
            "lowest-latency" | "lowest_latency" => Strategy::LowestLatency,
            other => bail!("Unknown load balancing strategy: {other}"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub name: String,
    pub protocol: i32,
}

#[derive(Debug, Clone)]
pub struct PlayersInfo {
    pub online: i32,
    pub max: i32,
}

#[derive(Debug, Clone)]
pub struct FallbackStatus {
    pub motd: String,
    pub version: VersionInfo,
    pub players: Option<PlayersInfo>,
    pub favicon_data_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    pub enabled: bool,
    pub on_mid_session_drop: bool,
    pub retry_interval_millis: i64,
    pub max_retry_interval_millis: i64,
    pub backoff_multiplier: f64,
    pub max_wait_millis: i64,
    pub title: String,
    pub subtitle: String,
    pub attempt_suffix: String,
    pub action_bar_frames: Vec<String>,
    pub animation_interval_millis: i64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            on_mid_session_drop: false,
            retry_interval_millis: 5000,
            max_retry_interval_millis: 30_000,
            backoff_multiplier: 2.0,
            max_wait_millis: 600_000,
            title: "&eServer is currently offline.".into(),
            subtitle: "&7Waiting to reconnect...".into(),
            attempt_suffix: " (attempt {attempt})".into(),
            action_bar_frames: vec![
                "&7Reconnecting.".into(),
                "&7Reconnecting..".into(),
                "&7Reconnecting...".into(),
            ],
            animation_interval_millis: 500,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RouteMetricUsageConfig {
    pub enabled: bool,
    /// -1 = no ceiling.
    pub limit: i64,
}

impl RouteMetricUsageConfig {
    fn defaulted() -> Self {
        Self { enabled: false, limit: -1 }
    }
}

#[derive(Debug, Clone)]
pub struct RouteMetricsConfig {
    pub file: Option<String>,
    pub upload: RouteMetricUsageConfig,
    pub download: RouteMetricUsageConfig,
    pub reset_interval_millis: i64,
}

impl RouteMetricsConfig {
    pub fn active(&self) -> bool {
        self.upload.enabled || self.download.enabled
    }
}

#[derive(Debug, Clone)]
pub struct Route {
    pub host_patterns: Vec<HostPattern>,
    pub backend_templates: Vec<String>,
    pub strategy: Strategy,
    pub cache_ping_ttl_millis: i64,
    pub fallback: Option<FallbackStatus>,
    pub modify_virtual_host: bool,
    pub proxy_protocol: bool,
    pub priority: i32,
    pub reconnect: ReconnectConfig,
    pub kick_message: String,
    pub voicechat_templates: Vec<String>,
    pub metrics: Option<RouteMetricsConfig>,
}

impl Route {
    /// Returns the wildcard captures of the first matching host pattern, or `None`.
    pub fn match_host(&self, hostname: &str) -> Option<Vec<String>> {
        self.host_patterns.iter().find_map(|p| p.match_host(hostname))
    }

    pub async fn resolve_backends(&self, captures: &[String]) -> Result<Vec<SocketAddr>, crate::net::dns_cache::DnsResolveError> {
        let mut out = Vec::with_capacity(self.backend_templates.len());
        for template in &self.backend_templates {
            let substituted = crate::config::host_pattern::substitute_params(template, captures);
            out.push(resolve_backend_address(&substituted).await?);
        }
        Ok(out)
    }

    /// True if resolving this route's backends for `captures` would await at least one real DNS
    /// lookup (an uncached hostname). Callers on the connection-accept path (section 3) use this
    /// to decide whether resolving inline is fine or whether to defer/buffer instead.
    pub fn backends_need_blocking_resolution(&self, captures: &[String]) -> bool {
        self.backend_templates.iter().any(|t| needs_blocking_resolution(&crate::config::host_pattern::substitute_params(t, captures)))
    }

    /// Same resolution as `resolve_backends`, for the optional `voicechat:` backend. `None` when
    /// this route has none configured.
    pub async fn resolve_voicechat(&self, captures: &[String]) -> Result<Option<SocketAddr>, crate::net::dns_cache::DnsResolveError> {
        match self.voicechat_templates.first() {
            None => Ok(None),
            Some(t) => Ok(Some(resolve_backend_address(&crate::config::host_pattern::substitute_params(t, captures)).await?)),
        }
    }
}

fn split_host_port(value: &str, default_port: u16) -> (String, u16) {
    match value.rfind(':') {
        Some(i) => (value[..i].to_string(), value[i + 1..].parse().unwrap_or(default_port)),
        None => (value.to_string(), default_port),
    }
}

/// Like `parse_host_port` but resolves the hostname through the DNS cache instead of directly —
/// see `dns_cache` for why that matters on the connection dispatch hot path.
pub async fn resolve_backend_address(value: &str) -> Result<SocketAddr, crate::net::dns_cache::DnsResolveError> {
    let (host, port) = split_host_port(value, 25565);
    crate::net::dns_cache::dns_cache().resolve(&host, port).await
}

/// Whether `resolve_backend_address` for this `host:port` string would await a real DNS lookup.
pub fn needs_blocking_resolution(value: &str) -> bool {
    let (host, port) = split_host_port(value, 25565);
    !crate::net::dns_cache::dns_cache().will_resolve_without_blocking(&host, port)
}

/// Structural equality used to carry per-backend runtime state (active-connection counts,
/// round-robin cursor, latency readings) across a config reload — mirrors Kotlin's data-class
/// `equals` on `Route` (field-by-field, `HostPattern.equals` ignoring raw casing/trailing dot).
impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.host_patterns == other.host_patterns
            && self.backend_templates == other.backend_templates
            && self.strategy == other.strategy
            && self.priority == other.priority
    }
}
impl Eq for Route {}

#[derive(Debug, Clone, Default)]
pub struct UdpConfig {
    pub proxy_protocol: bool,
}

#[derive(Debug, Clone)]
pub struct UdpProxyConfig {
    pub bind: String,
    pub backend: String,
    pub log_sessions: bool,
}

impl UdpProxyConfig {
    pub fn bind_address(&self) -> Result<SocketAddr> {
        parse_host_port(&self.bind, 25565)
    }
    pub fn backend_address(&self) -> Result<SocketAddr> {
        parse_host_port(&self.backend, 25565)
    }
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub enabled: bool,
    pub bind: String,
    pub token: Option<String>,
    pub players_page_limit: i32,
}

impl ApiConfig {
    pub fn bind_address(&self) -> Result<SocketAddr> {
        parse_host_port(&self.bind, 8080)
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self { enabled: false, bind: "localhost:8080".into(), token: None, players_page_limit: 500 }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionThrottleConfig {
    pub max_connections: i32,
    pub max_per_ip_per_window: i32,
    pub window_millis: i64,
}

impl Default for ConnectionThrottleConfig {
    fn default() -> Self {
        Self { max_connections: 0, max_per_ip_per_window: 8, window_millis: 8_000 }
    }
}

#[derive(Debug, Clone)]
pub struct UdpThrottleConfig {
    pub max_sessions: i32,
    pub max_sessions_per_ip: i32,
    pub pending_packets_per_session: i32,
    pub idle_timeout_millis: i64,
    pub no_reply_teardown_millis: i64,
}

impl Default for UdpThrottleConfig {
    fn default() -> Self {
        Self {
            max_sessions: 8192,
            max_sessions_per_ip: 64,
            pending_packets_per_session: 256,
            idle_timeout_millis: 300_000,
            no_reply_teardown_millis: 20_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTrackingConfig {
    pub enabled: bool,
    pub db_path: String,
    pub retention_days: i32,
    pub max_records: i32,
    pub queue_capacity: i32,
    pub batch_size: i32,
    pub flush_interval_millis: i64,
    pub prune_interval_millis: i64,
}

impl Default for ConnectionTrackingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            db_path: "data/connections.db".into(),
            retention_days: 30,
            max_records: 200_000,
            queue_capacity: 5000,
            batch_size: 200,
            flush_interval_millis: 2000,
            prune_interval_millis: 3_600_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsLoggingConfig {
    pub enabled: bool,
    pub db_path: String,
    pub interval_millis: i64,
    pub retention_days: i32,
    pub max_records: i32,
}

impl Default for StatsLoggingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            db_path: "data/stats.db".into(),
            interval_millis: 60_000,
            retention_days: 14,
            max_records: 50_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GateConfig {
    pub bind: String,
    pub routes: Vec<Route>,
    pub udp: UdpConfig,
    pub udp_proxies: Vec<UdpProxyConfig>,
    pub api: ApiConfig,
    pub connection_tracking: ConnectionTrackingConfig,
    pub stats_logging: StatsLoggingConfig,
    pub worker_threads: i32,
    pub log_connections: bool,
    pub log_level: String,
    pub proxy_protocol: bool,
    pub login_timeout_millis: i64,
    pub max_connections_per_ip: i32,
    pub connection_throttle: ConnectionThrottleConfig,
    pub udp_throttle: UdpThrottleConfig,
    pub so_backlog: i32,
    pub max_held_reconnect_sessions: i32,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:25565".into(),
            routes: Vec::new(),
            udp: UdpConfig::default(),
            udp_proxies: Vec::new(),
            api: ApiConfig::default(),
            connection_tracking: ConnectionTrackingConfig::default(),
            stats_logging: StatsLoggingConfig::default(),
            worker_threads: 0,
            log_connections: false,
            log_level: "INFO".into(),
            proxy_protocol: false,
            login_timeout_millis: 10_000,
            max_connections_per_ip: 8,
            connection_throttle: ConnectionThrottleConfig::default(),
            udp_throttle: UdpThrottleConfig::default(),
            so_backlog: 128,
            max_held_reconnect_sessions: 500,
        }
    }
}

impl GateConfig {
    pub fn bind_address(&self) -> Result<SocketAddr> {
        parse_host_port(&self.bind, 25565)
    }
}

/// Parses `value` ("host:port" or just "host", falling back to `default_port`) without any DNS
/// resolution semantics beyond what `SocketAddr`'s `FromStr`/lookup would need — actual backend
/// hostname resolution goes through the (not-yet-ported) DNS cache; this mirrors Kotlin's
/// `parseHostPort`, used for bind addresses which are always IP literals.
pub fn parse_host_port(value: &str, default_port: u16) -> Result<SocketAddr> {
    let idx = value.rfind(':');
    let (host, port) = match idx {
        Some(i) => (&value[..i], value[i + 1..].parse::<u16>().context("invalid port")?),
        None => (value, default_port),
    };
    let host = if host.is_empty() { "0.0.0.0" } else { host };
    // IP literals parse directly; anything else (e.g. the default API bind's "localhost") needs
    // resolving via `ToSocketAddrs`, matching Kotlin's `InetSocketAddress(host, port)`
    // constructor, which also resolves hostnames rather than requiring an IP literal.
    if let Ok(addr) = format!("{host}:{port}").parse::<SocketAddr>() {
        return Ok(addr);
    }
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .with_context(|| format!("invalid bind address: {value}"))?
        .next()
        .ok_or_else(|| anyhow!("could not resolve bind address: {value}"))
}

fn as_map(v: &Value) -> Option<&serde_yaml::Mapping> {
    v.as_mapping()
}

fn get<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a Value> {
    map.get(Value::String(key.to_string()))
}

fn get_str(map: &serde_yaml::Mapping, key: &str) -> Option<String> {
    get(map, key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn get_bool(map: &serde_yaml::Mapping, key: &str) -> Option<bool> {
    get(map, key).and_then(|v| v.as_bool())
}

fn get_i64(map: &serde_yaml::Mapping, key: &str) -> Option<i64> {
    get(map, key).and_then(|v| v.as_i64())
}

fn get_i32(map: &serde_yaml::Mapping, key: &str) -> Option<i32> {
    get_i64(map, key).map(|v| v as i32)
}

fn get_map<'a>(map: &'a serde_yaml::Mapping, key: &str) -> Option<&'a serde_yaml::Mapping> {
    get(map, key).and_then(as_map)
}

fn string_list(v: &Value) -> Vec<String> {
    match v {
        Value::Sequence(seq) => seq.iter().map(value_to_string).collect(),
        Value::String(s) => vec![s.clone()],
        other => vec![value_to_string(other)],
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => serde_yaml::to_string(other).unwrap_or_default().trim().to_string(),
    }
}

/// Reconnect message text defaults, shared between `messages.yml` (`messages::GateMessages`)
/// and a route's own `reconnect:` override (`ReconnectConfig`). Defined here (rather than in
/// `messages.rs`) since `ReconnectConfig::default()` is the canonical source of these defaults.
#[derive(Debug, Clone)]
pub struct ReconnectMessages {
    pub title: String,
    pub subtitle: String,
    pub attempt_suffix: String,
    pub action_bar_frames: Vec<String>,
}

impl Default for ReconnectMessages {
    fn default() -> Self {
        let d = ReconnectConfig::default();
        Self {
            title: d.title,
            subtitle: d.subtitle,
            attempt_suffix: d.attempt_suffix,
            action_bar_frames: d.action_bar_frames,
        }
    }
}

/// Bundled default `config.yml`, embedded at compile time (mirrors the Kotlin build packaging
/// `default-config.yml` as a jar resource read via the classloader).
const DEFAULT_CONFIG_YML: &str = include_str!("../resources/default-config.yml");

/// Loads the config at `path`, first bootstrapping it from the bundled default-config.yml
/// resource if missing. `messages` supplies the defaults for player-facing text that routes
/// don't override.
pub fn load_or_create_default(path: impl AsRef<Path>, messages: &crate::config::messages::GateMessages) -> Result<GateConfig> {
    let path = path.as_ref();
    if !path.exists() {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(path, DEFAULT_CONFIG_YML)
            .with_context(|| format!("failed to bootstrap default config file at {}", path.display()))?;
        tracing::info!("No config file found at {}, created one from the bundled default", path.display());
    }
    load_config(path, messages)
}

pub fn load_config(path: impl AsRef<Path>, messages: &crate::config::messages::GateMessages) -> Result<GateConfig> {
    let path = path.as_ref();
    let text = fs::read_to_string(path).with_context(|| format!("Config file not found: {}", path.display()))?;
    let root: Value = serde_yaml::from_str(&text)?;
    let root_map = as_map(&root).cloned().unwrap_or_default();
    let config_section = get_map(&root_map, "config").cloned().unwrap_or(root_map);

    let defaults = GateConfig::default();
    let bind = get_str(&config_section, "bind").unwrap_or(defaults.bind.clone());

    let raw_routes: Vec<&Value> = get(&config_section, "routes")
        .and_then(|v| v.as_sequence())
        .map(|s| s.iter().collect())
        .unwrap_or_default();

    let mut routes = Vec::with_capacity(raw_routes.len());
    for (index, r) in raw_routes.iter().enumerate() {
        let map = as_map(r).ok_or_else(|| anyhow!("Route #{index} is not a map"))?;
        routes.push(parse_route(map, index, messages)?);
    }
    // Higher priority routes are matched first; ties keep config file order (stable sort).
    routes.sort_by(|a, b| b.priority.cmp(&a.priority));

    let udp_proxies = get(&config_section, "udpProxy")
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .enumerate()
                .map(|(i, e)| {
                    let m = as_map(e).ok_or_else(|| anyhow!("udpProxy #{i} is not a map"))?;
                    Ok(UdpProxyConfig {
                        bind: get_str(m, "bind").ok_or_else(|| anyhow!("udpProxy #{i} missing 'bind'"))?,
                        backend: get_str(m, "backend").ok_or_else(|| anyhow!("udpProxy #{i} missing 'backend'"))?,
                        log_sessions: get_bool(m, "logSessions").unwrap_or(true),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    let api = parse_api(get_map(&config_section, "api"));
    let connection_tracking = parse_connection_tracking(get_map(&config_section, "connectionTracking"))?;
    let stats_logging = parse_stats_logging(get_map(&config_section, "statsLogging"))?;
    let worker_threads = get_i32(&config_section, "workerThreads").unwrap_or(0);
    let log_connections = get_bool(&config_section, "logConnections").unwrap_or(false);
    let log_level = get_str(&config_section, "logLevel").unwrap_or(defaults.log_level.clone());
    let proxy_protocol = get_bool(&config_section, "proxyProtocol").unwrap_or(false);
    let udp = parse_udp(get_map(&config_section, "udp"), proxy_protocol)?;
    let login_timeout_millis = get_str(&config_section, "loginTimeout")
        .map(|s| parse_duration_millis(&s))
        .transpose()?
        .unwrap_or(defaults.login_timeout_millis);
    let max_connections_per_ip = get_i32(&config_section, "maxConnectionsPerIp").unwrap_or(defaults.max_connections_per_ip);
    let connection_throttle = parse_connection_throttle(get_map(&config_section, "connectionThrottle"))?;
    let udp_throttle = parse_udp_throttle(get_map(&config_section, "udpThrottle"))?;
    let so_backlog = get_i32(&config_section, "soBacklog").unwrap_or(defaults.so_backlog);
    let max_held_reconnect_sessions =
        get_i32(&config_section, "maxHeldReconnectSessions").unwrap_or(defaults.max_held_reconnect_sessions);

    Ok(GateConfig {
        bind,
        routes,
        udp,
        udp_proxies,
        api,
        connection_tracking,
        stats_logging,
        worker_threads,
        log_connections,
        log_level,
        proxy_protocol,
        login_timeout_millis,
        max_connections_per_ip,
        connection_throttle,
        udp_throttle,
        so_backlog,
        max_held_reconnect_sessions,
    })
}

fn parse_udp_throttle(c: Option<&serde_yaml::Mapping>) -> Result<UdpThrottleConfig> {
    let d = UdpThrottleConfig::default();
    let Some(c) = c else { return Ok(d) };
    Ok(UdpThrottleConfig {
        max_sessions: get_i32(c, "maxSessions").unwrap_or(d.max_sessions),
        max_sessions_per_ip: get_i32(c, "maxSessionsPerIp").unwrap_or(d.max_sessions_per_ip),
        pending_packets_per_session: get_i32(c, "pendingPacketsPerSession").unwrap_or(d.pending_packets_per_session),
        idle_timeout_millis: get_str(c, "idleTimeout").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(d.idle_timeout_millis),
        no_reply_teardown_millis: get_str(c, "noReplyTeardown")
            .map(|s| parse_duration_millis(&s))
            .transpose()?
            .unwrap_or(d.no_reply_teardown_millis),
    })
}

fn parse_connection_throttle(c: Option<&serde_yaml::Mapping>) -> Result<ConnectionThrottleConfig> {
    let d = ConnectionThrottleConfig::default();
    let Some(c) = c else { return Ok(d) };
    Ok(ConnectionThrottleConfig {
        max_connections: get_i32(c, "maxConnections").unwrap_or(d.max_connections),
        max_per_ip_per_window: get_i32(c, "maxPerIpPerWindow").unwrap_or(d.max_per_ip_per_window),
        window_millis: get_str(c, "window").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(d.window_millis),
    })
}

fn parse_udp(c: Option<&serde_yaml::Mapping>, default_proxy_protocol: bool) -> Result<UdpConfig> {
    let Some(c) = c else { return Ok(UdpConfig { proxy_protocol: default_proxy_protocol }) };
    Ok(UdpConfig { proxy_protocol: get_bool(c, "proxyProtocol").unwrap_or(default_proxy_protocol) })
}

fn parse_api(a: Option<&serde_yaml::Mapping>) -> ApiConfig {
    let d = ApiConfig::default();
    let Some(a) = a else { return d };
    ApiConfig {
        enabled: get_bool(a, "enabled").unwrap_or(d.enabled),
        bind: get_str(a, "bind").unwrap_or(d.bind),
        token: get_str(a, "token").filter(|s| !s.trim().is_empty()).or(d.token),
        players_page_limit: get_i32(a, "playersPageLimit").unwrap_or(d.players_page_limit),
    }
}

fn parse_connection_tracking(c: Option<&serde_yaml::Mapping>) -> Result<ConnectionTrackingConfig> {
    let d = ConnectionTrackingConfig::default();
    let Some(c) = c else { return Ok(d) };
    Ok(ConnectionTrackingConfig {
        enabled: get_bool(c, "enabled").unwrap_or(d.enabled),
        db_path: get_str(c, "dbPath").unwrap_or(d.db_path),
        retention_days: get_i32(c, "retentionDays").unwrap_or(d.retention_days),
        max_records: get_i32(c, "maxRecords").unwrap_or(d.max_records),
        queue_capacity: get_i32(c, "queueCapacity").unwrap_or(d.queue_capacity),
        batch_size: get_i32(c, "batchSize").unwrap_or(d.batch_size),
        flush_interval_millis: get_str(c, "flushInterval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(d.flush_interval_millis),
        prune_interval_millis: get_str(c, "pruneInterval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(d.prune_interval_millis),
    })
}

fn parse_stats_logging(c: Option<&serde_yaml::Mapping>) -> Result<StatsLoggingConfig> {
    let d = StatsLoggingConfig::default();
    let Some(c) = c else { return Ok(d) };
    Ok(StatsLoggingConfig {
        enabled: get_bool(c, "enabled").unwrap_or(d.enabled),
        db_path: get_str(c, "dbPath").unwrap_or(d.db_path),
        interval_millis: get_str(c, "interval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(d.interval_millis),
        retention_days: get_i32(c, "retentionDays").unwrap_or(d.retention_days),
        max_records: get_i32(c, "maxRecords").unwrap_or(d.max_records),
    })
}

fn parse_route(r: &serde_yaml::Mapping, index: usize, messages: &crate::config::messages::GateMessages) -> Result<Route> {
    let host_raw = get(r, "host").ok_or_else(|| anyhow!("Route #{index} missing 'host'"))?;
    let hosts = string_list(host_raw);
    let host_patterns: Vec<HostPattern> = hosts.iter().map(HostPattern::new).collect();

    let backend_raw = get(r, "backend").ok_or_else(|| anyhow!("Route #{index} missing 'backend'"))?;
    let backends = string_list(backend_raw);

    validate_params(&hosts, &backends, index, "backend");

    let voicechat_templates = get(r, "voicechat").map(string_list).unwrap_or_default();
    if !voicechat_templates.is_empty() {
        validate_params(&hosts, &voicechat_templates, index, "voicechat");
    }

    let strategy = Strategy::parse(get_str(r, "strategy").as_deref())?;
    let cache_ping_ttl_millis = parse_duration_millis(&get_str(r, "cachePingTTL").unwrap_or_else(|| "10s".to_string()))?;
    let fallback = get_map(r, "fallback").map(parse_fallback).transpose()?;
    let modify_virtual_host = get_bool(r, "modifyVirtualHost").unwrap_or(false);
    let proxy_protocol = get_bool(r, "proxyProtocol").unwrap_or(false);
    let priority = get_i32(r, "priority").unwrap_or(0);
    let reconnect = render_reconnect_text(parse_reconnect(get_map(r, "reconnect"), &messages.reconnect)?);
    let kick_message = get_str(r, "kickMessage").unwrap_or_else(|| messages.kick_message.clone());
    let metrics = parse_metrics(get_map(r, "metrics"), index)?;

    Ok(Route {
        host_patterns,
        backend_templates: backends,
        strategy,
        cache_ping_ttl_millis,
        fallback,
        modify_virtual_host,
        proxy_protocol,
        priority,
        reconnect,
        kick_message,
        voicechat_templates,
        metrics,
    })
}

fn parse_metrics(m: Option<&serde_yaml::Mapping>, index: usize) -> Result<Option<RouteMetricsConfig>> {
    let Some(m) = m else { return Ok(None) };
    let usage = |key: &str| -> Result<RouteMetricUsageConfig> {
        let Some(u) = get_map(m, key) else { return Ok(RouteMetricUsageConfig::defaulted()) };
        let limit = match get(u, "limit") {
            None => -1i64,
            Some(Value::Number(n)) => n.as_i64().ok_or_else(|| anyhow!("Route #{index} metrics.{key}.limit is not a byte size"))?,
            Some(Value::String(s)) => parse_byte_size(s).with_context(|| format!("Route #{index} metrics.{key}.limit"))?,
            Some(other) => bail!("Route #{index} metrics.{key}.limit is not a byte size: {other:?}"),
        };
        Ok(RouteMetricUsageConfig { enabled: get_bool(u, "enabled").unwrap_or(false), limit })
    };
    let reset_interval = get_str(m, "resetInterval")
        .map(|s| parse_duration_millis(&s).with_context(|| format!("Route #{index} metrics.resetInterval")))
        .transpose()?
        .unwrap_or(0)
        .max(0);
    let config = RouteMetricsConfig {
        file: get_str(m, "file").filter(|s| !s.trim().is_empty()),
        upload: usage("upload")?,
        download: usage("download")?,
        reset_interval_millis: reset_interval,
    };
    if !config.active() {
        tracing::warn!("Route #{index}: metrics block present but neither upload nor download is enabled - ignoring it");
        return Ok(None);
    }
    Ok(Some(config))
}

fn parse_reconnect(r: Option<&serde_yaml::Mapping>, message_defaults: &ReconnectMessages) -> Result<ReconnectConfig> {
    let base = ReconnectConfig::default();
    let defaults = ReconnectConfig {
        title: message_defaults.title.clone(),
        subtitle: message_defaults.subtitle.clone(),
        attempt_suffix: message_defaults.attempt_suffix.clone(),
        action_bar_frames: message_defaults.action_bar_frames.clone(),
        ..base
    };
    let Some(r) = r else { return Ok(defaults) };
    let enabled = get_bool(r, "enabled").unwrap_or(defaults.enabled);
    let frames = get(r, "actionBarFrames").map(string_list);
    Ok(ReconnectConfig {
        enabled,
        on_mid_session_drop: get_bool(r, "onMidSessionDrop").unwrap_or(enabled),
        retry_interval_millis: get_str(r, "retryInterval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(defaults.retry_interval_millis),
        max_retry_interval_millis: get_str(r, "maxRetryInterval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(defaults.max_retry_interval_millis),
        backoff_multiplier: get(r, "backoffMultiplier").and_then(|v| v.as_f64()).unwrap_or(defaults.backoff_multiplier),
        max_wait_millis: get_str(r, "maxWait").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(defaults.max_wait_millis),
        title: get_str(r, "title").unwrap_or(defaults.title),
        subtitle: get_str(r, "subtitle").unwrap_or(defaults.subtitle),
        attempt_suffix: get_str(r, "attemptSuffix").unwrap_or(defaults.attempt_suffix),
        action_bar_frames: frames.unwrap_or(defaults.action_bar_frames),
        animation_interval_millis: get_str(r, "animationInterval").map(|s| parse_duration_millis(&s)).transpose()?.unwrap_or(defaults.animation_interval_millis),
    })
}

/// Pre-renders `title`/`subtitle`/`attemptSuffix`/`actionBarFrames` once at config load rather
/// than on every packet send (`actionBarFrames` is on the animation timer's hot path — every
/// `animationInterval`, 500ms by default, for every player waiting to reconnect).
fn render_reconnect_text(reconnect: ReconnectConfig) -> ReconnectConfig {
    ReconnectConfig {
        title: crate::protocol::text_format::to_legacy_text(&reconnect.title),
        subtitle: crate::protocol::text_format::to_legacy_text(&reconnect.subtitle),
        attempt_suffix: crate::protocol::text_format::to_legacy_text(&reconnect.attempt_suffix),
        action_bar_frames: reconnect.action_bar_frames.iter().map(|f| crate::protocol::text_format::to_legacy_text(f)).collect(),
        ..reconnect
    }
}

fn validate_params(hosts: &[String], backends: &[String], index: usize, label: &str) {
    let max_wildcards = hosts.iter().map(|h| HostPattern::new(h).wildcard_count).max().unwrap_or(0);
    for backend in backends {
        let mut chars = backend.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c != '$' {
                continue;
            }
            let start = i + 1;
            let mut end = start;
            for (j, d) in backend[start..].char_indices() {
                if d.is_ascii_digit() {
                    end = start + j + d.len_utf8();
                } else {
                    break;
                }
            }
            if end == start {
                continue;
            }
            let n: usize = backend[start..end].parse().unwrap_or(0);
            if max_wildcards == 0 {
                tracing::warn!(
                    "Route #{index}: {label} '{backend}' references ${n}, but host pattern has no wildcards - it won't be substituted"
                );
            } else if n > max_wildcards {
                tracing::warn!(
                    "Route #{index}: {label} '{backend}' references ${n}, but only {max_wildcards} wildcard(s) are captured"
                );
            }
        }
    }
}

fn parse_fallback(f: &serde_yaml::Mapping) -> Result<FallbackStatus> {
    let motd = get_str(f, "motd").unwrap_or_default();
    let version_map = get_map(f, "version");
    let version = VersionInfo {
        name: version_map.and_then(|m| get_str(m, "name")).unwrap_or_default(),
        protocol: version_map.and_then(|m| get_i32(m, "protocol")).unwrap_or(-1),
    };
    let players = get_map(f, "players").map(|m| PlayersInfo {
        online: get_i32(m, "online").unwrap_or(0),
        max: get_i32(m, "max").unwrap_or(0),
    });
    let favicon = get_str(f, "favicon").and_then(|v| resolve_favicon(&v));
    Ok(FallbackStatus { motd, version, players, favicon_data_uri: favicon })
}

fn resolve_favicon(value: &str) -> Option<String> {
    if value.starts_with("data:image") {
        return Some(value.to_string());
    }
    let path = Path::new(value);
    if !path.exists() {
        tracing::warn!("Favicon file not found: {value}");
        return None;
    }
    let bytes = fs::read(path).ok()?;
    Some(format!("data:image/png;base64,{}", base64_encode(&bytes)))
}

/// Minimal base64 encoder (standard alphabet, padded) so `resolve_favicon` doesn't need an extra
/// dependency for one call site.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 { ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[(b2 & 0x3f) as usize] as char } else { '=' });
    }
    out
}

/// Pre-resolves backend hostnames that don't depend on a wildcard capture, so the DNS cache is
/// already warm before the first player connects — see `dns_cache`. Templated backends
/// (containing `$N`) can't be pre-warmed since the real hostname isn't known until a matching
/// connection arrives.
///
/// Deliberately not called from `load_config`/`load_or_create_default` themselves: those run
/// both from inside the tokio runtime (startup) and from the config-file watcher's plain
/// background thread (a hot reload), and warming needs a `tokio::runtime::Handle` that's only
/// available to a caller who knows which context it's in — see `DnsCache::warm`'s doc. Callers
/// (both `main.rs`'s startup and `app_state.rs`'s reload path) call this explicitly right after
/// loading a config, passing their own handle.
pub fn warm_static_backends(routes: &[Route], handle: &tokio::runtime::Handle) {
    for route in routes {
        for template in route.backend_templates.iter().chain(route.voicechat_templates.iter()) {
            if template.contains('$') {
                continue;
            }
            let (host, port) = split_host_port(template, 25565);
            crate::net::dns_cache::dns_cache().warm(handle, &host, port);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(contents: &str) -> tempfile_shim::TempFile {
        tempfile_shim::TempFile::new(contents)
    }

    // Tiny inline temp-file helper so this crate doesn't need a `tempfile` dev-dependency for a
    // handful of config-loading tests.
    mod tempfile_shim {
        use super::*;
        pub struct TempFile {
            pub path: std::path::PathBuf,
        }
        impl TempFile {
            pub fn new(contents: &str) -> Self {
                let mut path = std::env::temp_dir();
                path.push(format!("mcgate-test-{}-{}.yml", std::process::id(), rand_suffix()));
                let mut f = std::fs::File::create(&path).unwrap();
                f.write_all(contents.as_bytes()).unwrap();
                Self { path }
            }
        }
        impl Drop for TempFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }
        fn rand_suffix() -> u64 {
            // A nanosecond timestamp alone isn't collision-proof under true parallel test
            // execution (multiple tests can land on the same tick on a fast multi-core box,
            // then race to create/read/delete the same path) - an atomic counter guarantees
            // every call in this process gets a distinct suffix regardless of timing.
            use std::sync::atomic::{AtomicU64, Ordering};
            use std::time::{SystemTime, UNIX_EPOCH};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
            nanos.wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[test]
    fn loads_minimal_config_with_defaults() {
        let yml = r#"
bind: "0.0.0.0:25565"
routes:
  - host: "play.example.com"
    backend: "127.0.0.1:25566"
"#;
        let f = write_temp(yml);
        let cfg = load_config(&f.path, &crate::config::messages::GateMessages::default()).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0:25565");
        assert_eq!(cfg.routes.len(), 1);
        assert_eq!(cfg.routes[0].backend_templates, vec!["127.0.0.1:25566"]);
        assert_eq!(cfg.routes[0].strategy, Strategy::Sequential);
        assert_eq!(cfg.max_connections_per_ip, 8);
    }

    #[test]
    fn route_priority_sorts_descending_stable() {
        let yml = r#"
routes:
  - host: "a.example.com"
    backend: "127.0.0.1:1"
    priority: 1
  - host: "b.example.com"
    backend: "127.0.0.1:2"
    priority: 5
  - host: "c.example.com"
    backend: "127.0.0.1:3"
    priority: 5
"#;
        let f = write_temp(yml);
        let cfg = load_config(&f.path, &crate::config::messages::GateMessages::default()).unwrap();
        assert_eq!(cfg.routes[0].host_patterns[0].raw, "b.example.com");
        assert_eq!(cfg.routes[1].host_patterns[0].raw, "c.example.com");
        assert_eq!(cfg.routes[2].host_patterns[0].raw, "a.example.com");
    }

    #[test]
    fn missing_backend_errors() {
        let yml = r#"
routes:
  - host: "play.example.com"
"#;
        let f = write_temp(yml);
        assert!(load_config(&f.path, &crate::config::messages::GateMessages::default()).is_err());
    }

    #[test]
    fn metrics_with_no_direction_enabled_is_dropped() {
        let yml = r#"
routes:
  - host: "play.example.com"
    backend: "127.0.0.1:1"
    metrics:
      file: "usage.json"
"#;
        let f = write_temp(yml);
        let cfg = load_config(&f.path, &crate::config::messages::GateMessages::default()).unwrap();
        assert!(cfg.routes[0].metrics.is_none());
    }

    #[test]
    fn metrics_byte_size_limit_parses() {
        let yml = r#"
routes:
  - host: "play.example.com"
    backend: "127.0.0.1:1"
    metrics:
      upload:
        enabled: true
        limit: "100g"
"#;
        let f = write_temp(yml);
        let cfg = load_config(&f.path, &crate::config::messages::GateMessages::default()).unwrap();
        let m = cfg.routes[0].metrics.as_ref().unwrap();
        assert_eq!(m.upload.limit, 100i64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_host_port_defaults_port() {
        let addr = parse_host_port("127.0.0.1", 25565).unwrap();
        assert_eq!(addr.port(), 25565);
    }

    #[test]
    fn parse_host_port_resolves_hostnames_like_localhost() {
        // Regression: the default API bind is "localhost:8080", which SocketAddr's own FromStr
        // rejects outright (it only accepts IP literals) — this must resolve like Kotlin's
        // InetSocketAddress(host, port) constructor does.
        let addr = parse_host_port("localhost:8080", 25565).unwrap();
        assert_eq!(addr.port(), 8080);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn api_config_default_bind_resolves() {
        ApiConfig::default().bind_address().unwrap();
    }

    #[test]
    fn bundled_default_config_parses() {
        // Guards against the bundled default-config.yml drifting out of sync with the schema
        // this loader expects.
        let f = write_temp(DEFAULT_CONFIG_YML);
        load_config(&f.path, &crate::config::messages::GateMessages::default()).unwrap();
    }

    #[test]
    fn load_or_create_default_bootstraps_missing_file() {
        let mut path = std::env::temp_dir();
        path.push(format!("mcgate-config-test-{}.yml", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert!(!path.exists());
        load_or_create_default(&path, &crate::config::messages::GateMessages::default()).unwrap();
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();
    }
}
