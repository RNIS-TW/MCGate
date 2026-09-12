//! Port of `Main.kt`'s `startConsole` + its command handlers (`printHelp`, `printPlayers`,
//! `printRoutes`, `metricsCommand`/`printMetrics`, `printWhois`, `formatDuration`,
//! `formatBytes`).
//!
//! Uses `rustyline` as the closest analogue to JLine (line editing/history, an external-print
//! hook so a log line arriving mid-command doesn't corrupt the prompt) with a plain-stdin
//! fallback when `MCGATE_PLAIN_CONSOLE=true` is set or a real terminal isn't available — same
//! shape as the Kotlin version's JLine-or-plain-`BufferedReader` fallback.
//!
//! `kick` really disconnects a live session now (`PlayerSession::disconnect`, awaited alongside
//! the relay splice in `server.rs`) but can't carry a message — see `state.rs`'s `PlayerSession`
//! doc for why. `transfer` still just reports "not yet supported": a real Play-state `transfer`
//! packet needs the same compression-threshold framing knowledge kick messages do.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rustyline::{DefaultEditor, ExternalPrinter};

use crate::app_state::AppState;
use crate::route_metrics_store::route_metrics_store;
use crate::state::player_sessions;
use crate::version::version;

pub fn start(state: Arc<AppState>) {
    let plain = std::env::var("MCGATE_PLAIN_CONSOLE").map(|v| v == "true").unwrap_or(false);
    std::thread::Builder::new()
        .name("console".into())
        .spawn(move || run(state, plain))
        .expect("failed to spawn console thread");
}

fn run(state: Arc<AppState>, force_plain: bool) {
    if !force_plain {
        if let Ok(mut editor) = DefaultEditor::new() {
            if let Ok(printer) = editor.create_external_printer() {
                install_external_printer(printer);
                run_rustyline(state, editor);
                return;
            }
        }
        tracing::debug!("rustyline unavailable, falling back to plain console input");
    }
    run_plain(state);
}

fn install_external_printer(mut printer: impl ExternalPrinter + Send + 'static) {
    crate::logging::set_active_line_printer(Some(Box::new(move |line: &str| {
        let _ = printer.print(format!("{line}\n"));
    })));
}

fn run_rustyline(state: Arc<AppState>, mut editor: DefaultEditor) {
    loop {
        match editor.readline("> ") {
            Ok(line) => {
                let _ = editor.add_history_entry(line.as_str());
                if handle_command(&state, &line) {
                    return;
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                tracing::info!("Ctrl+C received, shutting down...");
                std::process::exit(0);
            }
            Err(rustyline::error::ReadlineError::Eof) => return,
            Err(_) => return,
        }
    }
}

fn run_plain(state: Arc<AppState>) {
    use std::io::BufRead;
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { return };
        if handle_command(&state, &line) {
            return;
        }
    }
}

/// Returns `true` when the process should stop reading further commands (a stop/shutdown/exit
/// command was issued).
fn handle_command(state: &Arc<AppState>, line: &str) -> bool {
    let trimmed = line.trim();
    let command = trimmed.split(' ').next().unwrap_or("").to_lowercase();
    let rest = trimmed[command.len()..].trim();

    match command.as_str() {
        "stop" | "shutdown" | "exit" => {
            tracing::info!("Stop command received, shutting down...");
            std::process::exit(0);
        }
        "help" | "?" => print_help(),
        "players" | "list" | "playerlist" => print_players(),
        "routes" => print_routes(state),
        "metrics" => metrics_command(state, rest),
        "version" => tracing::info!("MCGate v{}", version()),
        "reload" => state.reload_all("Manual reload"),
        "uptime" => tracing::info!("Uptime: {}", format_duration(state.started_at.elapsed())),
        "kick" => kick_command(rest),
        "transfer" => transfer_command(rest),
        "whois" => {
            if rest.is_empty() {
                tracing::info!("Usage: whois <player>");
            } else {
                print_whois(rest);
            }
        }
        "" => {}
        _ => tracing::info!("Unknown command: '{trimmed}' (try 'help')"),
    }
    false
}

fn print_help() {
    tracing::info!("Commands:");
    tracing::info!("  help                     - show this list");
    tracing::info!("  players, list, playerlist - list connected players");
    tracing::info!("  whois <player>           - show full session detail for one player");
    tracing::info!("  kick <player> [message]  - disconnect a player, optionally with a message");
    tracing::info!("  transfer <player> <host:port> - send a player directly to another Minecraft server");
    tracing::info!("  routes                   - list configured routes and backend status");
    tracing::info!("  metrics [reset <index|host|all>] - show per-route byte usage, or reset a counter to zero");
    tracing::info!("  reload                   - re-read config.yml and messages.yml now");
    tracing::info!("  uptime                   - show how long MCGate has been running");
    tracing::info!("  version                  - show the running MCGate version");
    tracing::info!("  stop, shutdown, exit     - stop the server");
}

fn print_players() {
    let sessions = player_sessions().all();
    if sessions.is_empty() {
        tracing::info!("No players connected.");
        return;
    }
    tracing::info!("{} player(s) connected:", sessions.len());
    for s in sessions {
        let status = s.backend.map(|b| b.to_string()).unwrap_or_else(|| "waiting to reconnect".to_string());
        tracing::info!("  {} ({}) from {} on '{}' -> {} ping=n/a", s.name, s.uuid, s.remote_address, s.host, status);
    }
}

fn print_whois(name: &str) {
    let Some(s) = player_sessions().find_by_name(name) else {
        tracing::info!("No player named '{name}' is connected.");
        return;
    };
    let status = s.backend.map(|b| b.to_string()).unwrap_or_else(|| "waiting to reconnect".to_string());
    tracing::info!("Player: {} ({})", s.name, s.uuid);
    tracing::info!("  From:      {}", s.remote_address);
    tracing::info!("  Host:      {}", s.host);
    tracing::info!("  Status:    {status}");
    tracing::info!("  Ping:      n/a");
    tracing::info!("  Sent:      {} packet(s), {} byte(s)", s.packets_sent.load(Ordering::Relaxed), s.bytes_sent.load(Ordering::Relaxed));
    tracing::info!("  Received:  {} packet(s), {} byte(s)", s.packets_received.load(Ordering::Relaxed), s.bytes_received.load(Ordering::Relaxed));
}

fn kick_command(rest: &str) {
    let mut parts = rest.splitn(2, ' ');
    let name = parts.next().unwrap_or("").trim();
    let message = parts.next().map(str::trim).filter(|m| !m.is_empty());
    if name.is_empty() {
        tracing::info!("Usage: kick <player> [message]");
        return;
    }
    match player_sessions().find_by_name(name) {
        None => tracing::info!("No player named '{name}' is connected."),
        Some(s) => {
            if message.is_some() {
                // See state.rs's PlayerSession doc: a message-carrying kick needs the
                // connection's negotiated compression threshold, which needs backend login
                // sniffing (not ported) to know safely - silently dropping the message here
                // rather than risking a corrupted packet on a connection MCGate can't safely
                // write an ad-hoc frame into.
                tracing::debug!("kick message ignored for '{}' - message-carrying kick isn't supported yet", s.name);
            }
            s.disconnect.notify_waiters();
            tracing::info!("Kicked '{}'.", s.name);
        }
    }
}

fn transfer_command(rest: &str) {
    let mut parts = rest.splitn(2, ' ');
    let name = parts.next().unwrap_or("").trim();
    let target = parts.next().unwrap_or("").trim();
    if name.is_empty() || target.is_empty() {
        tracing::info!("Usage: transfer <player> <host:port>");
        return;
    }
    match player_sessions().find_by_name(name) {
        None => tracing::info!("No player named '{name}' is connected."),
        // A real transfer needs a Play-state `transfer` packet framed with the connection's
        // negotiated compression threshold - same limitation as a message-carrying kick (see
        // state.rs's PlayerSession doc). Not supported yet; report that plainly rather than
        // silently doing nothing.
        Some(s) => tracing::info!("Can't transfer '{}': not yet supported (needs backend login sniffing for correct packet framing)", s.name),
    }
}

fn metrics_command(state: &Arc<AppState>, args: &str) {
    if args.is_empty() {
        print_metrics(state);
        return;
    }
    let parts: Vec<&str> = args.split_whitespace().collect();
    if parts.len() != 2 || !parts[0].eq_ignore_ascii_case("reset") {
        tracing::info!("Usage: metrics [reset <index|host|all>]");
        return;
    }
    let target = parts[1];
    let store = route_metrics_store();
    if target.eq_ignore_ascii_case("all") {
        let n = store.reset_all();
        tracing::info!("{}", if n == 0 { "No metrics counters to reset.".to_string() } else { format!("Reset {n} metrics counter(s).") });
        return;
    }

    let cfg = state.config();
    let matched: Vec<&crate::config::Route> = target
        .parse::<usize>()
        .ok()
        .and_then(|idx| cfg.routes.get(idx))
        .map(|r| vec![r])
        .unwrap_or_else(|| cfg.routes.iter().filter(|r| r.host_patterns.iter().any(|p| p.raw == target) || r.match_host(target).is_some()).collect());

    if matched.is_empty() {
        tracing::info!("No route matches '{target}' (give a route index, a host, or 'all').");
        return;
    }
    let mut reset = 0;
    for route in matched {
        if store.reset(route).is_some() {
            reset += 1;
            tracing::info!("Reset metrics for '{}': upload/download now 0 bytes.", route.host_patterns.iter().map(|p| p.raw.as_str()).collect::<Vec<_>>().join(", "));
        }
    }
    if reset == 0 {
        tracing::info!("Matched route(s) but none have metrics accounting enabled.");
    }
}

fn format_bytes(bytes: i64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", units[unit])
}

fn print_metrics(state: &Arc<AppState>) {
    let cfg = state.config();
    let store = route_metrics_store();
    let rows: Vec<(usize, Arc<crate::route_metrics_store::RouteTrafficCounter>)> =
        cfg.routes.iter().enumerate().filter_map(|(i, r)| store.handle(r).map(|c| (i, c))).collect();
    if rows.is_empty() {
        tracing::info!("No routes have metrics accounting enabled.");
        return;
    }
    tracing::info!("Per-route traffic usage:");
    for (index, c) in rows {
        let hosts = cfg.routes[index].host_patterns.iter().map(|p| p.raw.as_str()).collect::<Vec<_>>().join(", ");
        let file = c.file.lock().unwrap().clone();
        tracing::info!("[{index}] {hosts}{}", file.map(|f| format!(" (-> {f})")).unwrap_or_default());
        if c.upload_enabled.load(Ordering::Relaxed) {
            let limit = c.upload_limit.load(Ordering::Relaxed);
            let cap = if limit >= 0 { format!(" of {}{}", format_bytes(limit), if c.upload_exceeded() { " [LIMIT REACHED]" } else { "" }) } else { " (no limit)".to_string() };
            tracing::info!("      upload  : {}{cap}", format_bytes(c.upload_bytes.load(Ordering::Relaxed)));
        }
        if c.download_enabled.load(Ordering::Relaxed) {
            let limit = c.download_limit.load(Ordering::Relaxed);
            let cap = if limit >= 0 { format!(" of {}{}", format_bytes(limit), if c.download_exceeded() { " [LIMIT REACHED]" } else { "" }) } else { " (no limit)".to_string() };
            tracing::info!("      download: {}{cap}", format_bytes(c.download_bytes.load(Ordering::Relaxed)));
        }
    }
}

fn print_routes(state: &Arc<AppState>) {
    let cfg = state.config();
    if cfg.routes.is_empty() {
        tracing::info!("No routes configured.");
        return;
    }
    for (index, route) in cfg.routes.iter().enumerate() {
        let hosts: Vec<&str> = route.host_patterns.iter().map(|p| p.raw.as_str()).collect();
        tracing::info!("[{index}] {} (priority {})", hosts.join(", "), route.priority);
        let runtime = state.route_runtime(index);
        // Live stats need a resolved address; only shown for routes with no wildcard captures,
        // matching the Kotlin `routes` command's own caveat.
        let resolved = if route.host_patterns.iter().all(|p| p.wildcard_count == 0) {
            resolve_blocking(state, route)
        } else {
            None
        };
        for (i, template) in route.backend_templates.iter().enumerate() {
            let addr = resolved.as_ref().and_then(|v: &Vec<std::net::SocketAddr>| v.get(i).copied());
            let active = addr.and_then(|a| runtime.as_ref().map(|r| r.active_connections_of(a))).unwrap_or(0);
            let latency = addr.and_then(|a| runtime.as_ref().and_then(|r| r.latency_of(a))).map(|ms| format!("{ms}ms")).unwrap_or_else(|| "n/a".to_string());
            tracing::info!("      -> {template} (active={active}, latency={latency})");
        }
    }
}

/// `print_routes` runs on the plain console `std::thread` (see `console::start`), not a tokio
/// task, so it can't `.await` the real DNS-cached resolution `Route::resolve_backends` needs.
/// `AppState::runtime_handle` (captured at startup, always from inside the runtime) lets a
/// foreign thread bridge into it via a plain `block_on` — NOT `tokio::task::block_in_place`, which
/// is only valid when the calling thread is itself a tokio runtime worker (this one isn't, so
/// using it would panic). Blocking this thread doesn't block any tokio worker. Only ever hits
/// already-cached/IP-literal backends in practice (a route just printed by `routes` has almost
/// certainly been dialed or warmed already), so this essentially never blocks on real I/O.
fn resolve_blocking(state: &Arc<AppState>, route: &crate::config::Route) -> Option<Vec<std::net::SocketAddr>> {
    let route = route.clone();
    state.runtime_handle().block_on(async move { route.resolve_backends(&[]).await.ok() })
}

/// Matches `formatDuration` in `Main.kt`.
fn format_duration(d: Duration) -> String {
    let total_seconds = d.as_secs();
    let days = total_seconds / 86_400;
    let hours = (total_seconds % 86_400) / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m {seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_matches_kotlin_thresholds() {
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h 1m 5s");
        assert_eq!(format_duration(Duration::from_secs(90_065)), "1d 1h 1m");
    }
}
