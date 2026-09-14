// Most config fields aren't read by the not-yet-ported subsystems (routing/UDP/tracking) — see
// plan.md for what's done vs. pending. Remove this once those land.
#![allow(dead_code)]

mod config;
mod console;
mod logging;
mod net;
mod protocol;
mod state;
mod udp;
mod version;

use anyhow::Result;

use config::loader as config_loader;
use config::messages;
use net::api as api_server;
use net::{dns_cache, flood_control, ip_ban, network_info, server};
use state::{app_state, connection_tracker, route_metrics_store, stats_logger};
use udp::proxy as udp_proxy;
use udp::{voice_relay, voice_routing};

const BANNER: &str = r#"
███╗   ███╗ ██████╗ ██████╗  █████╗ ████████╗███████╗
████╗ ████║██╔════╝██╔════╝ ██╔══██╗╚══██╔══╝██╔════╝
██╔████╔██║██║     ██║  ███╗███████║   ██║   █████╗
██║╚██╔╝██║██║     ██║   ██║██╔══██║   ██║   ██╔══╝
██║ ╚═╝ ██║╚██████╗╚██████╔╝██║  ██║   ██║   ███████╗
╚═╝     ╚═╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝
"#;

const USAGE: &str = "\
Usage: mcgate [CONFIG_FILE] [MESSAGES_FILE]

Runs the MCGate proxy in the current directory. Both arguments are optional
and default to config.yml/messages.yml; a missing file is bootstrapped from
the built-in defaults on first run.

Options:
  -h, --help       Print this help and exit
  -V, --version    Print the version and exit";

/// Parses `mcgate [CONFIG_FILE] [MESSAGES_FILE]`, handling `-h`/`--help` and `-V`/`--version`
/// before anything else touches the filesystem or logging - a single static binary run directly
/// on a server is expected to behave like any other well-mannered CLI tool on those two flags.
fn parse_args() -> Option<(String, String)> {
    let mut positional = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return None;
            }
            "-V" | "--version" => {
                let dirty = if version::git_dirty() { "-dirty" } else { "" };
                println!("mcgate {} ({}{})", version::version(), version::git_hash(), dirty);
                println!("built {} for {}", version::build_timestamp_utc(), version::target_triple());
                println!("{}", version::rustc_version());
                return None;
            }
            _ => positional.push(arg),
        }
    }
    let config_path = positional.first().cloned().unwrap_or_else(|| "config.yml".to_string());
    let messages_path = positional.get(1).cloned().unwrap_or_else(|| "messages.yml".to_string());
    Some((config_path, messages_path))
}

/// Entry point. See plan.md for the full section-by-section port status - every
/// section is done except `ReconnectHandler.kt` (mid-session reconnect-hold), which is
/// deliberately not ported.
#[tokio::main]
async fn main() -> Result<()> {
    let Some((config_path, messages_path)) = parse_args() else {
        return Ok(());
    };

    state::mark_process_start();
    dns_cache::spawn_evictor();
    flood_control::spawn_connection_rate_sweeper();
    ip_ban::spawn_sweeper();

    let messages = messages::GateMessages::load_or_create_default(&messages_path)?;
    let initial_config = config::load_or_create_default(&config_path, &messages)?;

    let (_file_writer, log_level) = logging::init("log", &initial_config.log_level)?;

    println!("{BANNER}");
    tracing::info!("MCGate v{}{}", version::version(), if version::git_dirty() { " (modified build)" } else { "" });
    tracing::info!("Commit {} | Target {} | {}", version::git_hash(), version::target_triple(), version::rustc_version());
    tracing::info!("Built {}", version::build_timestamp_utc());
    tracing::info!("https://github.com/RNIS-TW/MCGate (MIT License)");
    network_info::log_network_interfaces();
    tracing::info!("Loaded messages from {messages_path}");
    tracing::info!("Loaded {} route(s) from {}", initial_config.routes.len(), config_path);

    let state = app_state::AppState::new(config_path.clone(), messages_path.clone(), initial_config, messages, log_level, tokio::runtime::Handle::current());

    // Kept alive for the process lifetime — dropping either debouncer stops that watch.
    let _messages_watch = {
        let state = state.clone();
        config_loader::watch_messages(&state.messages_path.clone(), move |new_messages| {
            if let Err(e) = state.apply_messages_change(new_messages) {
                tracing::warn!("Failed to reload {} after messages change: {e}", state.config_path);
            }
        })?
    };
    let _config_watch = {
        let state_for_messages = state.clone();
        let state = state.clone();
        config_loader::watch_config(&state.config_path.clone(), move || state_for_messages.messages(), move |new_config| {
            state.apply_config_change(new_config);
        })?
    };

    console::start(state.clone());

    let api_task = {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = api_server::serve(state).await {
                tracing::error!("API server failed: {e}");
            }
        })
    };

    flood_control::HeldReconnectSessions::set_max(state.config().max_held_reconnect_sessions);
    route_metrics_store::spawn_ticker(state.clone());
    voice_routing::spawn_evictor();

    // Static UDP forwards (config.yml's top-level `udpProxy:` list) and the voicechat UDP relay
    // are both startup-only, like the main TCP bind address - not reconfigured on a hot config
    // reload (matches Main.kt).
    let startup_config = state.config();
    for udp_proxy_config in &startup_config.udp_proxies {
        let udp_proxy_config = udp_proxy_config.clone();
        tokio::spawn(async move {
            if let Err(e) = udp_proxy::run(udp_proxy_config).await {
                tracing::error!("UDP proxy failed: {e}");
            }
        });
    }

    // Voicechat UDP relay - shares the main TCP listener's port, only bound at all if some route
    // actually configures a `voicechat:` backend.
    if startup_config.routes.iter().any(|r| !r.voicechat_templates.is_empty()) {
        let bind_addr = startup_config.bind_address()?;
        let expect_proxy_protocol = startup_config.udp.proxy_protocol;
        tokio::spawn(async move {
            if let Err(e) = voice_relay::run(bind_addr, expect_proxy_protocol).await {
                tracing::error!("Voicechat UDP relay failed: {e}");
            }
        });
    } else {
        tracing::info!("No routes configure a voicechat backend, skipping voicechat UDP relay");
    }

    let server_task = {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = server::run(state).await {
                tracing::error!("TCP accept loop failed: {e}");
            }
        })
    };

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutdown signal received, stopping...");
    server_task.abort();
    api_task.abort();
    shutdown_gracefully().await
}

/// Ends the process: kicks every connected player, flushes the SQLite writer threads, then forces
/// the process down. Used by both the Ctrl+C handler above and the console `stop`/`shutdown`/
/// `exit` commands (`console.rs`), so both paths actually drain state instead of one of them
/// (previously the console commands) just calling `std::process::exit` directly.
///
/// The forced exit at the end isn't optional cleanup - it's load-bearing. `#[tokio::main]` drops
/// the `Runtime` when `main` returns, and dropping a multi-thread `Runtime` blocks the current
/// thread until every task spawned onto it finishes on its own. Each connected player's
/// `server::relay` splice is one such task, and it only finishes when that player disconnects or
/// its `session.disconnect` `Notify` fires. Without kicking sessions first, "shutdown" would hang
/// until the last player happened to leave - observed as the process seeming to not shut down at
/// all, then quietly dropping players one at a time as they naturally disconnected. Kicking every
/// session first makes each `relay` task end (almost) immediately; the short sleep gives them a
/// moment to actually observe the notification and unwind before `process::exit` guarantees the
/// process goes down regardless.
pub async fn shutdown_gracefully() -> ! {
    let sessions = state::player_sessions().all();
    if !sessions.is_empty() {
        let names: Vec<&str> = sessions.iter().map(|s| s.name.as_str()).collect();
        tracing::info!("Disconnecting {} player(s): {}", sessions.len(), names.join(", "));
        for s in &sessions {
            s.disconnect.notify_waiters();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    // Join the SQLite writer threads so a final flush actually lands on disk rather than racing
    // process exit - matches the Kotlin version's bounded-wait shutdown for the same reason.
    connection_tracker::connection_tracker().shutdown();
    stats_logger::stats_logger().shutdown();
    tracing::info!("Stopped.");
    std::process::exit(0);
}
