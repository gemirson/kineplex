//! TCP Control Plane endpoint for graph submissions and allocation commands.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Json, Path, Query, Request};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{serve, Router};
use dashmap::DashMap;
use futures_util::stream;
use kineplex_core::graph::KineGraph;
use kineplex_core::spike_tap::{SpikeTap, TappedSpike};
use kineplex_net::routing::RoutingTable;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

const GRAPH_BODY_LIMIT_BYTES: usize = 1_048_576;
const GRAPH_READ_TIMEOUT: Duration = Duration::from_secs(2);
const ALLOCATION_ACCEPT_TIMEOUT: Duration = Duration::from_millis(500);

/// Raw graph payload accepted by the Control Plane.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitGraphRequest {
    /// Client or application submitting the graph.
    pub client_id: String,
    /// Raw graph node definitions.
    pub nodes: Vec<serde_json::Value>,
    /// Raw directed graph edge definitions.
    pub edges: Vec<serde_json::Value>,
}

impl SubmitGraphRequest {
    fn validate(&self) -> Result<(), &'static str> {
        if self.client_id.trim().is_empty() {
            return Err("client_id must not be empty");
        }
        if self.nodes.is_empty() {
            return Err("nodes must contain at least one node");
        }
        Ok(())
    }
}

/// Accepted response returned after a graph passes validation and allocation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubmitGraphResponse {
    /// Server-generated graph identifier.
    pub graph_id: Uuid,
    /// Lifecycle state of the accepted submission.
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct TappingQuery {
    graph_id: u64,
    synapse_id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AllocationRequest {
    allocation_id: Uuid,
    step_id: String,
    wasm: String,
    listen_to: Option<SocketAddr>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CancelAllocationRequest {
    allocation_id: Uuid,
    step_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct AllocationResponse {
    status: &'static str,
}

#[derive(Clone)]
struct ControlState {
    routing_table: RoutingTable,
    graph_status: Arc<DashMap<Uuid, String>>,
}

#[derive(Clone, Debug, Serialize)]
struct GraphStatusResponse {
    graph_id: Uuid,
    status: String,
}

/// Error returned while binding or stopping the Control Plane server.
#[derive(Debug)]
pub enum ControlServerError {
    /// The TCP listener or HTTP server returned an error.
    Server(String),
    /// The isolated server task could not be joined.
    Join(tokio::task::JoinError),
}

impl Display for ControlServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server(error) => write!(formatter, "Control Plane server failed: {error}"),
            Self::Join(error) => write!(formatter, "Control Plane task failed: {error}"),
        }
    }
}

impl Error for ControlServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Server(_) => None,
            Self::Join(error) => Some(error),
        }
    }
}

/// Isolated TCP Control Plane server.
pub struct ControlServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), ControlServerError>>,
}

impl ControlServer {
    /// Binds a standalone Control Plane with no allocation candidates.
    pub async fn bind(address: SocketAddr) -> Result<Self, ControlServerError> {
        Self::bind_with_routing(address, RoutingTable::new()).await
    }

    /// Binds the Control Plane with the live Gossip routing view.
    pub async fn bind_with_routing(
        address: SocketAddr,
        routing_table: RoutingTable,
    ) -> Result<Self, ControlServerError> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| ControlServerError::Server(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| ControlServerError::Server(error.to_string()))?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let state = Arc::new(ControlState {
            routing_table,
            graph_status: Arc::new(DashMap::new()),
        });
        let task = tokio::spawn(async move {
            serve(listener, router(state))
                .with_graceful_shutdown(async {
                    let _shutdown_result = shutdown_rx.await;
                })
                .await
                .map_err(|error| ControlServerError::Server(error.to_string()))
        });

        tracing::debug!(%local_addr, "Control Plane TCP server bound");
        Ok(Self {
            local_addr,
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    /// Returns the actual TCP address, including an OS-assigned port when bound to 0.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops the HTTP task and waits for its resources to be released.
    pub async fn shutdown(mut self) -> Result<(), ControlServerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        self.task.await.map_err(ControlServerError::Join)?
    }
}

fn router(state: Arc<ControlState>) -> Router {
    Router::new()
        .route("/submit_graph", post(submit_graph))
        .route("/tap", get(subscribe_tapping))
        .route("/graph_status/:graph_id", get(graph_status))
        .route("/allocate_step", post(allocate_step))
        .route("/cancel_allocation", post(cancel_allocation))
        .with_state(state)
        .layer(DefaultBodyLimit::max(GRAPH_BODY_LIMIT_BYTES))
        .layer(middleware::from_fn(request_timeout))
}

async fn subscribe_tapping(Query(query): Query<TappingQuery>) -> Response {
    let subscription = SpikeTap::global().subscribe();
    let events = stream::unfold(
        (subscription, query.graph_id, query.synapse_id),
        |(mut subscription, graph_id, synapse_id)| async move {
            loop {
                let event = subscription.recv().await?;
                if event.graph_id == graph_id && event.synapse_id == synapse_id {
                    let line = tapping_json_line(&event);
                    return Some((
                        Ok::<Vec<u8>, std::convert::Infallible>(line),
                        (subscription, graph_id, synapse_id),
                    ));
                }
            }
        },
    );
    let mut response = Response::new(Body::from_stream(events));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/x-ndjson"
            .parse()
            .expect("static content type is valid"),
    );
    response
}

async fn graph_status(
    axum::extract::State(state): axum::extract::State<Arc<ControlState>>,
    Path(graph_id): Path<Uuid>,
) -> Response {
    match state.graph_status.get(&graph_id) {
        Some(status) => Json(GraphStatusResponse {
            graph_id,
            status: status.clone(),
        })
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("graph {graph_id} was not found"),
            }),
        )
            .into_response(),
    }
}

fn tapping_json_line(event: &TappedSpike) -> Vec<u8> {
    let mut line = serde_json::to_vec(event).unwrap_or_default();
    line.push(b'\n');
    line
}

async fn request_timeout(request: Request, next: Next) -> Response {
    match timeout(GRAPH_READ_TIMEOUT, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(ErrorResponse {
                error: "request body read timed out".to_owned(),
            }),
        )
            .into_response(),
    }
}

async fn submit_graph(
    axum::extract::State(state): axum::extract::State<Arc<ControlState>>,
    payload: Result<Json<SubmitGraphRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(error) => return bad_request(format!("invalid graph JSON: {error}")),
    };
    if let Err(error) = payload.validate() {
        return bad_request(error.to_owned());
    }

    let graph = match KineGraph::from_json(&payload.nodes, &payload.edges) {
        Ok(graph) => graph,
        Err(error) => return bad_request(format!("invalid graph: {error}")),
    };
    let graph_id = Uuid::new_v4();
    state.graph_status.insert(graph_id, "ALLOCATING".to_owned());

    if !state.routing_table.is_empty() {
        if let Err(error) = allocate_graph(graph_id, &graph, &state.routing_table).await {
            state.graph_status.insert(graph_id, "FAILED".to_owned());
            tracing::error!(%graph_id, %error, "graph allocation failed; rollback completed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error }),
            )
                .into_response();
        }
    }

    state.graph_status.insert(graph_id, "RUNNING".to_owned());
    tracing::info!(
        %graph_id,
        client_id = %payload.client_id,
        stages = graph.node_count(),
        "graph accepted for validation"
    );
    (
        StatusCode::ACCEPTED,
        Json(SubmitGraphResponse {
            graph_id,
            status: "ACCEPTED_FOR_VALIDATION",
        }),
    )
        .into_response()
}

async fn allocate_step(Json(request): Json<AllocationRequest>) -> Response {
    if request.step_id.trim().is_empty() || request.wasm.trim().is_empty() {
        return bad_request("allocation requires step_id and wasm".to_owned());
    }
    tracing::info!(
        allocation_id = %request.allocation_id,
        step = %request.step_id,
        listen_to = ?request.listen_to,
        "allocation accepted"
    );
    (
        StatusCode::ACCEPTED,
        Json(AllocationResponse {
            status: "ALLOCATION_ACCEPTED",
        }),
    )
        .into_response()
}

async fn cancel_allocation(Json(request): Json<CancelAllocationRequest>) -> Response {
    tracing::warn!(
        allocation_id = %request.allocation_id,
        step = %request.step_id,
        "allocation cancelled during rollback"
    );
    (
        StatusCode::ACCEPTED,
        Json(AllocationResponse {
            status: "ALLOCATION_CANCELLED",
        }),
    )
        .into_response()
}

async fn allocate_graph(
    allocation_id: Uuid,
    graph: &KineGraph,
    routing_table: &RoutingTable,
) -> Result<(), String> {
    let stages = graph.stage_ids();
    let candidates = routing_table.get_best_nodes(routing_table.len());
    let mut assigned = Vec::new();

    for (stage_index, stage_id) in stages.iter().enumerate() {
        let wasm = graph
            .node_payload(stage_id)
            .and_then(|payload| payload.get("wasm"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("pending")
            .to_owned();
        let listen_to = assigned.last().copied();
        let mut accepted = false;

        for candidate in candidates.iter().skip(stage_index) {
            if assigned
                .iter()
                .any(|assigned_node| assigned_node == candidate)
            {
                continue;
            }
            let target = control_addr(*candidate)?;
            let request = AllocationRequest {
                allocation_id,
                step_id: stage_id.clone(),
                wasm: wasm.clone(),
                listen_to,
            };
            match send_control_request(target, "/allocate_step", &request).await {
                Ok(()) => {
                    tracing::info!(step = %stage_id, node = %candidate, "step assigned");
                    assigned.push(*candidate);
                    accepted = true;
                    break;
                }
                Err(error) => {
                    tracing::warn!(step = %stage_id, node = %candidate, %error, "allocation candidate refused");
                }
            }
        }

        if !accepted {
            rollback_allocations(allocation_id, &assigned).await;
            return Err(format!("no candidate accepted step '{stage_id}'"));
        }
    }

    tracing::info!(%allocation_id, assigned = assigned.len(), "graph allocation topology established");
    Ok(())
}

async fn rollback_allocations(allocation_id: Uuid, assigned: &[SocketAddr]) {
    for candidate in assigned {
        if let Ok(target) = control_addr(*candidate) {
            let request = CancelAllocationRequest {
                allocation_id,
                step_id: "rollback".to_owned(),
            };
            let _cancel_result = send_control_request(target, "/cancel_allocation", &request).await;
        }
    }
}

fn control_addr(gossip_addr: SocketAddr) -> Result<SocketAddr, String> {
    let port = gossip_addr
        .port()
        .checked_sub(1)
        .ok_or_else(|| format!("Gossip endpoint {gossip_addr} has no control-plane port"))?;
    Ok(SocketAddr::new(gossip_addr.ip(), port))
}

async fn send_control_request<T: Serialize>(
    target: SocketAddr,
    path: &str,
    payload: &T,
) -> Result<(), String> {
    let result = timeout(ALLOCATION_ACCEPT_TIMEOUT, async {
        let mut stream = TcpStream::connect(target)
            .await
            .map_err(|error| error.to_string())?;
        let body = serde_json::to_vec(payload).map_err(|error| error.to_string())?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {target}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        stream
            .write_all(&body)
            .await
            .map_err(|error| error.to_string())?;
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|error| error.to_string())?;
        let status = String::from_utf8_lossy(&response)
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| "invalid allocation response".to_owned())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!("target returned HTTP {status}"))
        }
    })
    .await;
    result.map_err(|_| "allocation acceptance timed out after 500ms".to_owned())?
}

fn bad_request(error: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response()
}

#[cfg(test)]
mod tests {
    use super::SubmitGraphRequest;
    use serde_json::json;

    #[test]
    fn accepts_structurally_valid_graph() {
        let request = SubmitGraphRequest {
            client_id: "app_bi_01".to_owned(),
            nodes: vec![json!({"id": "source"})],
            edges: Vec::new(),
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn rejects_empty_client_or_nodes() {
        let empty_client = SubmitGraphRequest {
            client_id: " ".to_owned(),
            nodes: vec![json!({})],
            edges: Vec::new(),
        };
        assert_eq!(empty_client.validate(), Err("client_id must not be empty"));

        let empty_nodes = SubmitGraphRequest {
            client_id: "client".to_owned(),
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        assert_eq!(
            empty_nodes.validate(),
            Err("nodes must contain at least one node")
        );
    }
}
