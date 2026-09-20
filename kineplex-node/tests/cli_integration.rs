use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::{Command, Output};

use kineplex_node::cli::parse_from;
use serde_json::Value;

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

fn one_json_event(output: &Output) -> Value {
    let stdout = String::from_utf8(output.stdout.clone())
        .expect("structured logs on stdout must contain valid UTF-8");
    let mut lines = stdout.lines();
    let event = lines.next().expect("one JSON log event must be emitted");
    assert!(lines.next().is_none(), "only one info event was expected");
    serde_json::from_str(event).expect("the tracing output must be valid JSON")
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
fn binary_emits_structured_startup_json() {
    let output = run_node(&[
        "--port",
        "8001",
        "--seed",
        "192.168.1.10:8000,192.168.1.11:8000",
    ]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let event = one_json_event(&output);
    assert_eq!(event["level"], "INFO");
    assert_eq!(event["target"], "kineplex_node");
    assert_eq!(event["fields"]["message"], "Starting KinePlex Node");
    assert_eq!(event["fields"]["bind_addr"], "0.0.0.0:8001");
    assert_eq!(event["fields"]["seed_count"], 2);
    assert_eq!(
        event["fields"]["seeds"],
        "[192.168.1.10:8000, 192.168.1.11:8000]"
    );
    assert!(event["timestamp"].is_string());
}

#[test]
fn rust_log_filters_info_events() {
    let output = run_node_with_rust_log(&[], Some("error"));

    assert!(output.status.success());
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
fn malformed_seed_exits_gracefully_with_help_hint() {
    let output = run_node(&["--seed", "invalid-address"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(stderr.contains("invalid-address"));
    assert!(stderr.contains("--seed"));
    assert!(stderr.contains("--help"));
}

#[test]
fn help_describes_all_boot_options() {
    let output = run_node(&["--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(stdout.contains("--port <PORT>"));
    assert!(stdout.contains("--bind-ip <IP>"));
    assert!(stdout.contains("--seed <IP:PORT>"));
    assert!(stdout.contains("IPv6"));
}
