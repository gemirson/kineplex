use std::net::SocketAddr;
use std::time::Duration;

use kineplex_node::control::ControlServer;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use uuid::Uuid;

async fn send_http(address: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("Control Plane TCP listener must accept connections");
    stream
        .write_all(request)
        .await
        .expect("test request must be written");
    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("Control Plane must answer before the test deadline")
        .expect("test response must be readable");
    response
}

fn request(body: &str) -> Vec<u8> {
    format!(
        "POST /submit_graph HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

fn status(response: &[u8]) -> u16 {
    let text = String::from_utf8_lossy(response);
    let line = text
        .lines()
        .next()
        .expect("HTTP response must have a status line");
    line.split_whitespace()
        .nth(1)
        .expect("HTTP status line must contain a code")
        .parse()
        .expect("HTTP status code must be numeric")
}

fn response_json(response: &[u8]) -> Value {
    let text = String::from_utf8_lossy(response);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("test response must contain a body");
    serde_json::from_str(body).expect("Control Plane response body must be JSON")
}

#[tokio::test]
async fn valid_graph_returns_202_and_uuid_v4() {
    let server = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("Control Plane must bind an ephemeral TCP port");
    let response = send_http(
        server.local_addr(),
        &request(r#"{"client_id":"app_bi_01","nodes":[{"id":"source"}],"edges":[]}"#),
    )
    .await;

    assert_eq!(status(&response), 202);
    let body = response_json(&response);
    let graph_id = body["graph_id"]
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("accepted response must contain a UUID");
    assert_eq!(graph_id.get_version(), Some(uuid::Version::Random));
    assert_eq!(body["status"], "ACCEPTED_FOR_VALIDATION");

    server
        .shutdown()
        .await
        .expect("server must shut down cleanly");
}

#[tokio::test]
async fn malformed_graph_returns_descriptive_400() {
    let server = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("Control Plane must bind an ephemeral TCP port");
    let response = send_http(
        server.local_addr(),
        &request(r#"{"client_id":"app_bi_01","nodes":[}"#),
    )
    .await;

    assert_eq!(status(&response), 400);
    let body = response_json(&response);
    assert!(body["error"]
        .as_str()
        .is_some_and(|error| error.contains("invalid graph JSON")));

    server
        .shutdown()
        .await
        .expect("server must shut down cleanly");
}

#[tokio::test]
async fn slow_body_is_rejected_after_two_seconds() {
    let server = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("Control Plane must bind an ephemeral TCP port");
    let mut stream = TcpStream::connect(server.local_addr())
        .await
        .expect("Control Plane must accept a slow client");
    stream
        .write_all(
            b"POST /submit_graph HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 32\r\nConnection: close\r\n\r\n{",
        )
        .await
        .expect("slow client headers must be written");

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("timeout response must arrive")
        .expect("timeout response must be readable");
    assert_eq!(status(&response), 408);
    let body = response_json(&response);
    assert_eq!(body["error"], "request body read timed out");

    server
        .shutdown()
        .await
        .expect("server must shut down cleanly");
}
