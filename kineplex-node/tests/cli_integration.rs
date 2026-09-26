use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::{Command, Output};

use kineplex_node::cli::parse_from;

fn run_node(arguments: &[&str]) -> Output {
    run_node_with_rust_log(arguments, None)
}

fn run_node_with_rust_log(arguments: &[&str], rust_log: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kineplex-node"));
    command.args(arguments).env_remove("RUST_LOG");

    if let Some(filter) = rust_log {
        command.env("RUST_LOG", filter);
    }

    command
        .output()
        .expect("the compiled kineplex-node binary must start")
}

#[test]
fn public_parser_returns_native_configuration() {
    let config = parse_from([
        "kineplex-node",
        "--bind-ip",
        "0.0.0.0",
        "--advertise-ip",
        "127.0.0.1",
        "--port",
        "8001",
        "--seed",
        "192.168.1.10:8000,192.168.1.11:8000",
    ])
    .expect("the documented CLI example must parse");

    assert_eq!(config.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(config.advertise_ip, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    assert_eq!(config.port, 8001);
    assert_eq!(
        config.seeds,
        [
            SocketAddr::from(([192, 168, 1, 10], 8000)),
            SocketAddr::from(([192, 168, 1, 11], 8000)),
        ]
    );
}

#[test]
fn rust_log_can_suppress_boot_events() {
    let output = run_node_with_rust_log(&[], Some("off"));

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn invalid_rust_log_fails_boot_gracefully() {
    let output = run_node_with_rust_log(&[], Some("[invalid"));
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(stderr.contains("failed to initialize tracing"));
    assert!(stderr.contains("invalid RUST_LOG filter"));
}

#[test]
fn invalid_port_exits_gracefully_with_help_hint() {
    let output = run_node(&["--port", "999999"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(stderr.contains("invalid value '999999'"));
    assert!(stderr.contains("number too large to fit in target type"));
    assert!(stderr.contains("--help"));
}

#[test]
fn port_without_room_for_gossip_is_rejected() {
    let output = run_node(&["--port", "65535"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("cannot reserve Gossip port +1"));
    assert!(stderr.contains("--help"));
}

#[test]
fn malformed_or_overflowing_seed_exits_gracefully() {
    for seed in ["invalid-address", "127.0.0.1:65535"] {
        let output = run_node(&["--seed", seed]);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(stderr.contains(seed));
        assert!(stderr.contains("--seed"));
        assert!(stderr.contains("--help"));
    }
}

#[test]
fn help_describes_all_boot_options() {
    let output = run_node(&["--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(stdout.contains("--port <PORT>"));
    assert!(stdout.contains("--bind-ip <IP>"));
    assert!(stdout.contains("--advertise-ip <IP>"));
    assert!(stdout.contains("--seed <IP:PORT>"));
    assert!(stdout.contains("IPv6"));
}
