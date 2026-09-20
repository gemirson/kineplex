use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::{Command, Output};

use kineplex_node::cli::parse_from;

fn run_node(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kineplex-node"))
        .args(arguments)
        .output()
        .expect("the compiled kineplex-node binary must start")
}

#[test]
fn public_parser_returns_native_configuration() {
    let config = parse_from([
        "kineplex-node",
        "--bind-ip",
        "127.0.0.1",
        "--port",
        "8001",
        "--seed",
        "192.168.1.10:8000,192.168.1.11:8000",
    ])
    .expect("the documented CLI example must parse");

    assert_eq!(config.bind_ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
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
fn binary_prints_startup_configuration() {
    let output = run_node(&[
        "--port",
        "8001",
        "--seed",
        "192.168.1.10:8000,192.168.1.11:8000",
    ]);

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Starting KinePlex Node on 0.0.0.0:8001. Seeds: [192.168.1.10:8000, 192.168.1.11:8000]\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn invalid_port_exits_gracefully_with_help_hint() {
    let output = run_node(&["--port", "999999"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("invalid value '999999'"));
    assert!(stderr.contains("number too large to fit in target type"));
    assert!(stderr.contains("--help"));
}

#[test]
fn malformed_seed_exits_gracefully_with_help_hint() {
    let output = run_node(&["--seed", "invalid-address"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("invalid-address"));
    assert!(stderr.contains("--seed"));
    assert!(stderr.contains("--help"));
}

#[test]
fn help_describes_all_boot_options() {
    let output = run_node(&["--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("--port <PORT>"));
    assert!(stdout.contains("--bind-ip <IP>"));
    assert!(stdout.contains("--seed <IP:PORT>"));
    assert!(stdout.contains("IPv6"));
}
