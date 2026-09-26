use std::net::SocketAddr;
use std::time::Duration;

use kineplex_net::routing::RoutingTable;
use kineplex_net::telemetry::NodeTelemetry;
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

#[tokio::test]
async fn dag_cycle_and_orphan_are_rejected_before_acceptance() {
    let server = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("Control Plane must bind an ephemeral TCP port");
    let cycle = request(
        r#"{"client_id":"client","nodes":[{"id":"A"},{"id":"B"},{"id":"C"}],"edges":[{"source":"A","target":"B"},{"source":"B","target":"C"},{"source":"C","target":"A"}]}"#,
    );
    let orphan = request(
        r#"{"client_id":"client","nodes":[{"id":"A"},{"id":"B"},{"id":"C"}],"edges":[{"source":"A","target":"B"}]}"#,
    );

    let cycle_response = send_http(server.local_addr(), &cycle).await;
    let orphan_response = send_http(server.local_addr(), &orphan).await;
    assert_eq!(status(&cycle_response), 400);
    assert!(response_json(&cycle_response)["error"]
        .as_str()
        .is_some_and(|error| error.contains("cycle detected")));
    assert_eq!(status(&orphan_response), 400);
    assert!(response_json(&orphan_response)["error"]
        .as_str()
        .is_some_and(|error| error.contains("orphan node detected")));

    server
        .shutdown()
        .await
        .expect("server must shut down cleanly");
}

#[tokio::test]
async fn allocator_retries_next_idle_node_when_first_candidate_is_unavailable() {
    let receiver = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("receiver Control Plane must bind");
    let receiver_gossip =
        SocketAddr::new(receiver.local_addr().ip(), receiver.local_addr().port() + 1);
    let unavailable_gossip = SocketAddr::from(([127, 0, 0, 1], 10));
    let routing = RoutingTable::new();
    routing.upsert(unavailable_gossip, NodeTelemetry::new(1, 100));
    routing.upsert(receiver_gossip, NodeTelemetry::new(2, 100));

    let coordinator =
        ControlServer::bind_with_routing(SocketAddr::from(([127, 0, 0, 1], 0)), routing)
            .await
            .expect("coordinator Control Plane must bind");
    let response = send_http(
        coordinator.local_addr(),
        &request(
            r#"{"client_id":"client","nodes":[{"id":"step-1","wasm":"filter.wasm"}],"edges":[]}"#,
        ),
    )
    .await;

    assert_eq!(status(&response), 202);
    assert_eq!(
        response_json(&response)["status"],
        "ACCEPTED_FOR_VALIDATION"
    );

    coordinator
        .shutdown()
        .await
        .expect("coordinator must shut down");
    receiver.shutdown().await.expect("receiver must shut down");
}

#[tokio::test]
async fn allocator_returns_failure_after_partial_allocation_and_rolls_back() {
    let receiver = ControlServer::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("receiver Control Plane must bind");
    let receiver_gossip =
        SocketAddr::new(receiver.local_addr().ip(), receiver.local_addr().port() + 1);
    let unavailable_gossip = SocketAddr::from(([127, 0, 0, 1], 10));
    let routing = RoutingTable::new();
    routing.upsert(receiver_gossip, NodeTelemetry::new(1, 100));
    routing.upsert(unavailable_gossip, NodeTelemetry::new(2, 100));

    let coordinator =
        ControlServer::bind_with_routing(SocketAddr::from(([127, 0, 0, 1], 0)), routing)
            .await
            .expect("coordinator Control Plane must bind");
    let response = send_http(
        coordinator.local_addr(),
        &request(r#"{"client_id":"client","nodes":[{"id":"step-1"},{"id":"step-2"}],"edges":[{"source":"step-1","target":"step-2"}]}"#),
    )
    .await;

    assert_eq!(status(&response), 503);
    assert!(response_json(&response)["error"]
        .as_str()
        .is_some_and(|error| error.contains("no candidate accepted")));

    coordinator
        .shutdown()
        .await
        .expect("coordinator must shut down");
    receiver.shutdown().await.expect("receiver must shut down");
}
