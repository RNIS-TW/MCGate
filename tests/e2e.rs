//! Real end-to-end test: spawns the actual compiled `mcgate` binary (via Cargo's
//! `CARGO_BIN_EXE_mcgate`, not a library call) against a real fake-backend TCP listener, and
//! drives genuine Minecraft-protocol bytes over real loopback sockets — a status ping and a full
//! login relay. This is the automated, `cargo test`-integrated version of the ad hoc verification
//! scripts used while building sections 3-6 (see plan.md) — running it here means CI
//! (`.github/workflows/rust.yml`) exercises the actual connectable-proxy behavior on every push,
//! not just the unit tests of individual pieces.
//!
//! Deliberately black-box: this only talks to the binary over sockets, the same way a real
//! Minecraft client and a real backend would, so it can't be fooled by an internal refactor that
//! breaks the real wire behavior while individual unit tests still pass.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct McGateProcess {
    child: Child,
}

impl Drop for McGateProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_var_int(out: &mut Vec<u8>, value: i32) {
    let mut v = value as u32;
    loop {
        if v & !0x7Fu32 == 0 {
            out.push(v as u8);
            return;
        }
        out.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
}

fn read_var_int(stream: &mut TcpStream) -> i32 {
    let mut result = 0i32;
    let mut shift = 0;
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).expect("eof reading varint");
        result |= ((b[0] & 0x7F) as i32) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_var_int(out, bytes.len() as i32);
    out.extend_from_slice(bytes);
}

fn encode_handshake(protocol: i32, host: &str, port: u16, next_state: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_var_int(&mut payload, protocol);
    write_string(&mut payload, host);
    payload.extend_from_slice(&port.to_be_bytes());
    write_var_int(&mut payload, next_state);
    let mut frame = Vec::new();
    write_var_int(&mut frame, payload.len() as i32);
    frame.extend_from_slice(&payload);
    frame
}

fn encode_login_start(name: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    write_string(&mut payload, name);
    let mut frame = Vec::new();
    write_var_int(&mut frame, payload.len() as i32);
    frame.extend_from_slice(&payload);
    frame
}

fn encode_status_request() -> Vec<u8> {
    let mut payload = Vec::new();
    write_var_int(&mut payload, 0x00);
    let mut frame = Vec::new();
    write_var_int(&mut frame, payload.len() as i32);
    frame.extend_from_slice(&payload);
    frame
}

/// A minimal fake Minecraft backend: reads the handshake, then either answers a Status Request
/// with a valid status JSON, or (login) sends a greeting and echoes whatever it receives after -
/// enough to prove a real bidirectional relay, not just a one-shot response.
fn spawn_fake_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            std::thread::spawn(move || {
                let _length = read_var_int(&mut stream);
                // Skip the handshake payload's fields by reading raw bytes: packet id, protocol
                // varint, host string, port (2 bytes), next state varint. Simplify by just
                // draining fixed/variable fields in order.
                let _packet_id = read_var_int(&mut stream);
                let _protocol = read_var_int(&mut stream);
                let host_len = read_var_int(&mut stream) as usize;
                let mut host_buf = vec![0u8; host_len];
                stream.read_exact(&mut host_buf).unwrap();
                let mut port_buf = [0u8; 2];
                stream.read_exact(&mut port_buf).unwrap();
                let next_state = read_var_int(&mut stream);

                if next_state == 1 {
                    let _req_len = read_var_int(&mut stream);
                    let _req_id = read_var_int(&mut stream);
                    let json = r#"{"version":{"name":"1.21","protocol":767},"description":{"text":"fake backend"}}"#;
                    let mut payload = Vec::new();
                    write_var_int(&mut payload, 0x00);
                    write_string(&mut payload, json);
                    let mut frame = Vec::new();
                    write_var_int(&mut frame, payload.len() as i32);
                    frame.extend_from_slice(&payload);
                    let _ = stream.write_all(&frame);
                } else {
                    let _login_len = read_var_int(&mut stream);
                    let mut rest = vec![0u8; _login_len as usize];
                    stream.read_exact(&mut rest).unwrap();
                    // A real, framed Login Success (0x02) - mcgate's `sniff_backend_login`
                    // watches for this before falling back to a raw byte splice, and without a
                    // real one here it would sit waiting (up to its own timeout) for a login
                    // response that never properly arrives. Field contents don't matter, only
                    // the packet id.
                    let mut payload = Vec::new();
                    write_var_int(&mut payload, 0x02);
                    payload.extend_from_slice(&[0u8; 16]); // uuid (dummy)
                    write_string(&mut payload, "Steve");
                    write_var_int(&mut payload, 0); // properties count
                    payload.extend_from_slice(&[0u8; 16]); // session id (dummy)
                    let mut frame = Vec::new();
                    write_var_int(&mut frame, payload.len() as i32);
                    frame.extend_from_slice(&payload);
                    let _ = stream.write_all(&frame);
                    let _ = stream.write_all(b"HELLO-FROM-BACKEND");
                    let mut buf = [0u8; 256];
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                let mut echo = b"ECHO:".to_vec();
                                echo.extend_from_slice(&buf[..n]);
                                if stream.write_all(&echo).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start_mcgate(dir: &std::path::Path, mcgate_port: u16, backend_addr: SocketAddr) -> McGateProcess {
    let config_path = dir.join("config.yml");
    let messages_path = dir.join("messages.yml");
    std::fs::write(
        &config_path,
        format!(
            "config:\n  bind: \"127.0.0.1:{mcgate_port}\"\n  logConnections: true\n  routes:\n    - host: \"test.example.com\"\n      backend: \"127.0.0.1:{}\"\n",
            backend_addr.port()
        ),
    )
    .unwrap();
    std::fs::write(&messages_path, "messages:\n  kickMessage: \"no backend\"\n").unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_mcgate"))
        .arg(&config_path)
        .arg(&messages_path)
        .current_dir(dir)
        .env("MCGATE_PLAIN_CONSOLE", "true")
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start mcgate binary - did `cargo build` run first?");
    McGateProcess { child }
}

/// Same as `start_mcgate` but with `proxyProtocol: true` and a small `maxConnectionsPerIp`, for
/// exercising the pre-login per-IP guard under PROXY protocol.
fn start_mcgate_proxy_protocol(dir: &std::path::Path, mcgate_port: u16, backend_addr: SocketAddr, max_connections_per_ip: u32) -> McGateProcess {
    let config_path = dir.join("config.yml");
    let messages_path = dir.join("messages.yml");
    std::fs::write(
        &config_path,
        format!(
            "config:\n  bind: \"127.0.0.1:{mcgate_port}\"\n  proxyProtocol: true\n  maxConnectionsPerIp: {max_connections_per_ip}\n  logConnections: true\n  routes:\n    - host: \"test.example.com\"\n      backend: \"127.0.0.1:{}\"\n",
            backend_addr.port()
        ),
    )
    .unwrap();
    std::fs::write(&messages_path, "messages:\n  kickMessage: \"no backend\"\n").unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_mcgate"))
        .arg(&config_path)
        .arg(&messages_path)
        .current_dir(dir)
        .env("MCGATE_PLAIN_CONSOLE", "true")
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start mcgate binary - did `cargo build` run first?");
    McGateProcess { child }
}

fn write_proxy_v1_header(stream: &mut TcpStream, src_ip: &str, src_port: u16, dst_port: u16) {
    let header = format!("PROXY TCP4 {src_ip} 127.0.0.1 {src_port} {dst_port}\r\n");
    stream.write_all(header.as_bytes()).unwrap();
}

fn wait_for_port(addr: SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("mcgate never started listening on {addr}");
}

#[test]
fn status_ping_and_login_relay_over_real_sockets() {
    let dir = std::env::temp_dir().join(format!("mcgate-e2e-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let backend_addr = spawn_fake_backend();
    let mcgate_port = free_tcp_port();
    let mcgate_addr: SocketAddr = format!("127.0.0.1:{mcgate_port}").parse().unwrap();
    let _process = start_mcgate(&dir, mcgate_port, backend_addr);
    wait_for_port(mcgate_addr);

    // Status ping: a real handshake + status request, expecting a validated JSON response.
    {
        let mut stream = TcpStream::connect(mcgate_addr).unwrap();
        stream.write_all(&encode_handshake(767, "test.example.com", mcgate_port, 1)).unwrap();
        stream.write_all(&encode_status_request()).unwrap();
        let _length = read_var_int(&mut stream);
        let _packet_id = read_var_int(&mut stream);
        let json_len = read_var_int(&mut stream) as usize;
        let mut json_buf = vec![0u8; json_len];
        stream.read_exact(&mut json_buf).unwrap();
        let json = String::from_utf8(json_buf).unwrap();
        assert!(json.contains("\"protocol\":767"), "unexpected status JSON: {json}");
    }

    // Login relay: a real handshake + Login Start, expecting a genuine bidirectional byte splice.
    {
        let mut stream = TcpStream::connect(mcgate_addr).unwrap();
        stream.write_all(&encode_handshake(767, "test.example.com", mcgate_port, 2)).unwrap();
        stream.write_all(&encode_login_start("Steve")).unwrap();

        // mcgate's login sniffer forwards the backend's real Login Success frame to the client
        // unmodified before the raw splice takes over - skip past it (by its own length prefix)
        // rather than assuming a fixed size, same as a real client would.
        let login_success_len = read_var_int(&mut stream) as usize;
        let mut login_success_buf = vec![0u8; login_success_len];
        stream.read_exact(&mut login_success_buf).unwrap();

        let mut greeting = [0u8; "HELLO-FROM-BACKEND".len()];
        stream.read_exact(&mut greeting).unwrap();
        assert_eq!(&greeting, b"HELLO-FROM-BACKEND");

        stream.write_all(b"ping-through-relay").unwrap();
        let mut echoed = [0u8; "ECHO:ping-through-relay".len()];
        stream.read_exact(&mut echoed).unwrap();
        assert_eq!(&echoed, b"ECHO:ping-through-relay");
    }

    // Unmatched host: connection must be closed, not silently hung or relayed anywhere.
    {
        let mut stream = TcpStream::connect(mcgate_addr).unwrap();
        stream.write_all(&encode_handshake(767, "no-such-route.example.com", mcgate_port, 2)).unwrap();
        stream.write_all(&encode_login_start("Steve")).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut buf = [0u8; 1];
        let n = stream.read(&mut buf).unwrap_or(0);
        assert_eq!(n, 0, "expected the connection to be closed for an unmatched host");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression test for a real bug: `maxConnectionsPerIp` was unconditionally disabled whenever
/// `proxyProtocol: true`, even though the real client address (from the PROXY header) is already
/// known by the point that guard runs - so under proxy_protocol (as in the reported production
/// config) there was no cap at all on concurrent pre-login connections per real client, which is
/// exactly what lets a reconnect storm during a backend outage grow memory without bound. This
/// drives real PROXY v1 headers (all reporting the same real client IP) over real loopback
/// sockets against the actual compiled binary, and asserts the cap is now enforced.
#[test]
fn per_ip_pre_login_cap_applies_under_proxy_protocol() {
    let dir = std::env::temp_dir().join(format!("mcgate-e2e-proxyproto-captest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let backend_addr = spawn_fake_backend();
    let mcgate_port = free_tcp_port();
    let mcgate_addr: SocketAddr = format!("127.0.0.1:{mcgate_port}").parse().unwrap();
    let _process = start_mcgate_proxy_protocol(&dir, mcgate_port, backend_addr, 2);
    wait_for_port(mcgate_addr);

    let real_client_ip = "203.0.113.42"; // same reported "real" client IP on every connection below

    // Two connections at the cap: send only the PROXY header (no handshake yet) so each holds its
    // pre-login slot open rather than completing and releasing it.
    let mut held = Vec::new();
    for i in 0..2u16 {
        let mut stream = TcpStream::connect(mcgate_addr).unwrap();
        write_proxy_v1_header(&mut stream, real_client_ip, 40000 + i, mcgate_port);
        held.push(stream);
    }
    // Give mcgate a moment to actually read each header and acquire its guard before the next
    // connection below tests the cap.
    std::thread::sleep(Duration::from_millis(200));

    // A third connection from the same real client IP (different TCP peer port, same PROXY
    // header address) must be rejected outright - the fixed bug would have let this through
    // unconditionally.
    let mut third = TcpStream::connect(mcgate_addr).unwrap();
    write_proxy_v1_header(&mut third, real_client_ip, 40010, mcgate_port);
    third.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = [0u8; 1];
    // Distinguish an actual close (rejected - what we want) from a read timeout (the guard did
    // NOT reject it, mcgate is just still waiting on a handshake that never arrives) - collapsing
    // both to "0 bytes" via `unwrap_or(0)` would make this assertion pass either way and defeat
    // the whole point of the test.
    match third.read(&mut buf) {
        Ok(0) => {} // closed immediately - rejected, as expected
        Ok(n) => panic!("expected the connection to be closed, got {n} unexpected byte(s)"),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
            panic!("connection is still open after 2s - maxConnectionsPerIp did not reject the 3rd connection from the same real client IP under proxyProtocol")
        }
        Err(e) => panic!("unexpected read error: {e}"),
    }

    // Freeing one held slot must admit a new connection from that same IP again.
    drop(held.remove(0));
    std::thread::sleep(Duration::from_millis(200));
    let mut fourth = TcpStream::connect(mcgate_addr).unwrap();
    write_proxy_v1_header(&mut fourth, real_client_ip, 40020, mcgate_port);
    fourth.write_all(&encode_handshake(767, "test.example.com", mcgate_port, 1)).unwrap();
    fourth.write_all(&encode_status_request()).unwrap();
    fourth.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut first_byte = [0u8; 1];
    fourth.read_exact(&mut first_byte).expect("a freed per-IP slot should admit a new connection from the same real client IP");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A fake version-multiplexing backend (like Velocity/ViaVersion): its status response echoes the
/// pinging client's protocol version and the virtual host it asked for.
fn spawn_echoing_status_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            std::thread::spawn(move || {
                let _length = read_var_int(&mut stream);
                let _packet_id = read_var_int(&mut stream);
                let protocol = read_var_int(&mut stream);
                let host_len = read_var_int(&mut stream) as usize;
                let mut host_buf = vec![0u8; host_len];
                stream.read_exact(&mut host_buf).unwrap();
                let host = String::from_utf8(host_buf).unwrap();
                let mut port_buf = [0u8; 2];
                stream.read_exact(&mut port_buf).unwrap();
                let _next_state = read_var_int(&mut stream);
                let _req_len = read_var_int(&mut stream);
                let _req_id = read_var_int(&mut stream);
                let json = format!(r#"{{"version":{{"name":"proto-{protocol}","protocol":{protocol}}},"description":{{"text":"motd-for-{host}"}}}}"#);
                let mut payload = Vec::new();
                write_var_int(&mut payload, 0x00);
                write_string(&mut payload, &json);
                let mut frame = Vec::new();
                write_var_int(&mut frame, payload.len() as i32);
                frame.extend_from_slice(&payload);
                let _ = stream.write_all(&frame);
            });
        }
    });
    addr
}

fn status_ping(mcgate_addr: SocketAddr, protocol: i32, host: &str) -> String {
    let mut stream = TcpStream::connect(mcgate_addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.write_all(&encode_handshake(protocol, host, mcgate_addr.port(), 1)).unwrap();
    stream.write_all(&encode_status_request()).unwrap();
    let _length = read_var_int(&mut stream);
    let _packet_id = read_var_int(&mut stream);
    let json_len = read_var_int(&mut stream) as usize;
    let mut json_buf = vec![0u8; json_len];
    stream.read_exact(&mut json_buf).unwrap();
    String::from_utf8(json_buf).unwrap()
}

/// Regression test for a real bug: status responses were cached per backend address only, so
/// within the cache TTL every client got whichever client pinged first's response - a
/// version-multiplexing backend's echoed protocol showed up as an "incompatible" red version
/// for clients on any other version, and hostnames sharing a backend shared one MOTD.
#[test]
fn status_cache_is_per_protocol_version_and_host() {
    let dir = std::env::temp_dir().join(format!("mcgate-e2e-statuscache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let backend_addr = spawn_echoing_status_backend();
    let mcgate_port = free_tcp_port();
    let mcgate_addr: SocketAddr = format!("127.0.0.1:{mcgate_port}").parse().unwrap();
    std::fs::write(
        dir.join("config.yml"),
        format!(
            "config:\n  bind: \"127.0.0.1:{mcgate_port}\"\n  routes:\n    - host:\n        - \"a.example.com\"\n        - \"b.example.com\"\n      backend: \"127.0.0.1:{}\"\n      cachePingTTL: 60s\n",
            backend_addr.port()
        ),
    )
    .unwrap();
    std::fs::write(dir.join("messages.yml"), "messages:\n  kickMessage: \"no backend\"\n").unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_mcgate"))
        .arg(dir.join("config.yml"))
        .arg(dir.join("messages.yml"))
        .current_dir(&dir)
        .env("MCGATE_PLAIN_CONSOLE", "true")
        .env("NO_COLOR", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _process = McGateProcess { child };
    wait_for_port(mcgate_addr);

    // Different client versions, same host, well within the cache TTL.
    let v767 = status_ping(mcgate_addr, 767, "a.example.com");
    assert!(v767.contains(r#""protocol":767"#), "{v767}");
    let v47 = status_ping(mcgate_addr, 47, "a.example.com");
    assert!(v47.contains(r#""protocol":47"#), "1.8 client got another version's cached status: {v47}");
    let v767_again = status_ping(mcgate_addr, 767, "a.example.com");
    assert!(v767_again.contains(r#""protocol":767"#), "{v767_again}");

    // Different host sharing the same backend must get its own MOTD, not a's cached one.
    let b = status_ping(mcgate_addr, 767, "b.example.com");
    assert!(b.contains("motd-for-b.example.com"), "b.example.com got another host's cached MOTD: {b}");

    let _ = std::fs::remove_dir_all(&dir);
}
