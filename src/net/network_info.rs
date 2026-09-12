//! Port of `Main.kt`'s `logNetworkInterfaces()`.

/// Logs every up network interface and its addresses at startup — mainly useful on multi-homed
/// hosts (multiple NICs/public IPs, e.g. separate TCP and UDP egress paths) to see at a glance
/// which local address an outbound connection would actually use, since that's otherwise picked
/// silently by the OS's routing table.
pub fn log_network_interfaces() {
    match if_addrs::get_if_addrs() {
        Ok(addrs) => {
            for (name, group) in group_by_interface(&addrs) {
                if group.is_empty() {
                    continue;
                }
                let mut flags = Vec::new();
                if group.iter().any(|a| a.is_loopback()) {
                    flags.push("loopback");
                }
                let flag_str = if flags.is_empty() { String::new() } else { format!(" ({})", flags.join(", ")) };
                let ips: Vec<String> = group.iter().map(|a| a.ip().to_string()).collect();
                tracing::info!("Network interface '{name}'{flag_str}: {}", ips.join(", "));
            }
        }
        Err(e) => tracing::warn!("Failed to enumerate network interfaces: {e}"),
    }
}

fn group_by_interface(addrs: &[if_addrs::Interface]) -> Vec<(&str, Vec<&if_addrs::Interface>)> {
    let mut names: Vec<&str> = Vec::new();
    let mut groups: std::collections::HashMap<&str, Vec<&if_addrs::Interface>> = std::collections::HashMap::new();
    for a in addrs {
        if !names.contains(&a.name.as_str()) {
            names.push(&a.name);
        }
        groups.entry(&a.name).or_default().push(a);
    }
    names.into_iter().map(|n| (n, groups.remove(n).unwrap_or_default())).collect()
}
