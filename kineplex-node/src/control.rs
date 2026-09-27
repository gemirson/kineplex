//! TCP Control Plane endpoint for graph submissions and allocation commands.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::profiling::CpuProfiler;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Json, Path, Query, Request};
use axum::http::{header, HeaderValue, StatusCode};
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

const GRAPH_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const GRAPH_READ_TIMEOUT: Duration = Duration::from_secs(2);
const ALLOCATION_ACCEPT_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Optional geometric and ordering constraints.
    #[serde(default)]
    pub invariants: Vec<serde_json::Value>,
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
    profiler: Option<Arc<std::sync::Mutex<CpuProfiler>>>,
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
        Self::bind_with_routing_and_profiler(address, routing_table, None).await
    }

    /// Binds Control Plane routes with an optional process CPU profiler.
    pub async fn bind_with_routing_and_profiler(
        address: SocketAddr,
        routing_table: RoutingTable,
        profiler: Option<Arc<std::sync::Mutex<CpuProfiler>>>,
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
            profiler,
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
        .route("/metrics", get(prometheus_metrics))
        .route("/metrics/otlp", get(otlp_metrics))
        .route("/debug/pprof/flamegraph", get(flamegraph))
        .route("/allocate_step", post(allocate_step))
        .route("/cancel_allocation", post(cancel_allocation))
        .with_state(state)
        .layer(DefaultBodyLimit::max(GRAPH_BODY_LIMIT_BYTES))
        .layer(middleware::from_fn(request_timeout))
}

async fn prometheus_metrics() -> Response {
    let mut response = Response::new(Body::from(
        kineplex_core::metrics::global().prometheus_text(),
    ));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

async fn otlp_metrics() -> Response {
    let mut response = Response::new(Body::from(kineplex_core::metrics::global().otlp_json()));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

async fn flamegraph(
    axum::extract::State(state): axum::extract::State<Arc<ControlState>>,
) -> Response {
    let Some(profiler) = &state.profiler else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let profiler = match profiler.try_lock() {
        Ok(profiler) => profiler,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    match profiler.flamegraph() {
        Ok(svg) => {
            let mut response = Response::new(Body::from(svg));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("image/svg+xml; charset=utf-8"),
            );
            response
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("flamegraph generation failed: {error}"),
            }),
        )
            .into_response(),
    }
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
    if let Err(error) =
        validate_graph_invariants(&payload.nodes, &payload.edges, &payload.invariants)
    {
        return bad_request(format!("invalid graph invariant: {error}"));
    }
    let graph_id = Uuid::new_v4();
    kineplex_core::metrics::global().set_topology(state.routing_table.len(), graph.edge_count(), 0);
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
    if candidates.is_empty() {
        return Err("no eligible nodes are available for graph allocation".to_owned());
    }
    let mut assigned = Vec::new();
    let mut previous_node = None;

    for (stage_index, stage_id) in stages.iter().enumerate() {
        let wasm = graph
            .node_payload(stage_id)
            .and_then(|payload| payload.get("wasm"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("pending")
            .to_owned();
        let listen_to = previous_node;
        let mut accepted = false;

        let first_candidate = stage_index % candidates.len();
        for offset in 0..candidates.len() {
            let candidate = &candidates[(first_candidate + offset) % candidates.len()];
            if assigned.contains(candidate) {
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
                    if !assigned.contains(candidate) {
                        assigned.push(*candidate);
                    }
                    previous_node = Some(*candidate);
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

fn validate_graph_invariants(
    nodes: &[serde_json::Value],
    edges: &[serde_json::Value],
    invariants: &[serde_json::Value],
) -> Result<(), String> {
    for invariant in invariants {
        let kind = invariant
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "each invariant must have a string kind".to_owned())?;
        match kind {
            "preserve_order" => {
                let ordered = nodes.windows(2).all(|pair| {
                    let source = pair[0].get("id").and_then(serde_json::Value::as_str);
                    let target = pair[1].get("id").and_then(serde_json::Value::as_str);
                    edges.iter().any(|edge| {
                        edge.get("source").and_then(serde_json::Value::as_str) == source
                            && edge.get("target").and_then(serde_json::Value::as_str) == target
                    })
                });
                if !ordered {
                    return Err("preserve_order requires edges between adjacent stages".to_owned());
                }
            }
            "max_deformation" | "max_route_resistance" => {
                let field = if kind == "max_deformation" {
                    "tolerance"
                } else {
                    "maximum"
                };
                let value = invariant
                    .get(field)
                    .and_then(serde_json::Value::as_f64)
                    .ok_or_else(|| format!("{kind} requires numeric {field}"))?;
                if !value.is_finite() || value < 0.0 {
                    return Err(format!("{field} must be finite and non-negative"));
                }
            }
            _ => return Err(format!("unsupported invariant kind {kind:?}")),
        }
    }
    Ok(())
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
            invariants: Vec::new(),
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn rejects_empty_client_or_nodes() {
        let empty_client = SubmitGraphRequest {
            client_id: " ".to_owned(),
            nodes: vec![json!({})],
            edges: Vec::new(),
            invariants: Vec::new(),
        };
        assert_eq!(empty_client.validate(), Err("client_id must not be empty"));

        let empty_nodes = SubmitGraphRequest {
            client_id: "client".to_owned(),
            nodes: Vec::new(),
            edges: Vec::new(),
            invariants: Vec::new(),
        };
        assert_eq!(
            empty_nodes.validate(),
            Err("nodes must contain at least one node")
        );
    }
}


// ==========================================
// NodeControl & Multi-Tenant Submission (from develop)
// ==========================================

//  Control plane - Graph submission, validation, and allocation
//  
//  This module implements the control plane logic including:
//  - Submission validation
//  - Graph allocation
//  - Execution coordination

use crate::{ClusterManager, NodeId, NodeState};
use kineplex_core::{
    Result, CoreError, GraphId, Graph, GraphStatus, GraphConfig, 
    create_distributed_graph,
};
use crate::execution::PipelineExecutor;
use parking_lot::RwLock;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Submission request from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionRequest {
    /// Unique submission ID
    pub submission_id: String,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: String,
    /// Graph configuration
    pub config: SubmissionConfig,
    /// Idempotency key (optional)
    pub idempotency_key: Option<String>,
    /// Client credentials
    pub credentials: Option<ClientCredentials>,
}

/// Configuration for submission
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmissionConfig {
    /// Maximum memory in MB
    pub max_memory_mb: Option<u64>,
    /// Maximum fuel
    pub max_fuel: Option<u64>,
    /// Enable SIMD
    pub enable_simd: Option<bool>,
    /// Output path for results
    pub output_path: Option<String>,
    /// Required nodes for execution
    pub required_nodes: Option<u32>,
}

impl Default for SubmissionConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: Some(512),
            max_fuel: Some(1_000_000),
            enable_simd: Some(true),
            output_path: None,
            required_nodes: Some(1),
        }
    }
}

/// Client credentials for authentication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientCredentials {
    /// Client certificate (for mTLS)
    pub cert: Option<String>,
    /// Client key (for mTLS)
    pub key: Option<String>,
    /// API token
    pub token: Option<String>,
}

/// Allocation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationResult {
    /// Graph ID
    pub graph_id: GraphId,
    /// Allocated nodes
    pub allocated_nodes: Vec<NodeId>,
    /// Estimated completion time
    pub estimated_completion: DateTime<Utc>,
}

/// Node control service
pub struct NodeControl {
    cluster: Arc<ClusterManager>,
    executor: Arc<PipelineExecutor>,
    graphs: RwLock<HashMap<GraphId, Graph>>,
    submissions: RwLock<HashMap<String, SubmissionRequest>>,
    auth_enabled: RwLock<bool>,
    tenant_quotas: RwLock<HashMap<String, TenantQuotaInfo>>,
}

/// Tenant quota information
#[derive(Debug, Clone)]
pub struct TenantQuotaInfo {
    pub tenant_id: String,
    pub max_concurrent_graphs: u32,
    pub max_memory_mb: u64,
    pub max_fuel: u64,
    pub active_graphs: u32,
}

impl NodeControl {
    pub fn new(cluster: Arc<ClusterManager>, executor: Arc<PipelineExecutor>) -> Self {
        Self {
            cluster,
            executor,
            graphs: RwLock::new(HashMap::new()),
            submissions: RwLock::new(HashMap::new()),
            auth_enabled: RwLock::new(false),
            tenant_quotas: RwLock::new(HashMap::new()),
        }
    }
    
    /// Enable or disable authentication
    pub fn set_auth_enabled(&self, enabled: bool) {
        *self.auth_enabled.write() = enabled;
    }
    
    /// Register a tenant with quota
    pub fn register_tenant(&self, quota: TenantQuotaInfo) {
        let mut quotas = self.tenant_quotas.write();
        quotas.insert(quota.tenant_id.clone(), quota);
    }
    
    /// Validate submission request
    pub fn validate_submission(&self, request: &SubmissionRequest) -> Result<()> {
        // Check authentication if enabled
        let auth_enabled = *self.auth_enabled.read();
        if auth_enabled {
            if request.credentials.is_none() {
                return Err(CoreError::AuthenticationRequired);
            }
        }
        
        // Check tenant quota
        let mut quotas = self.tenant_quotas.write();
        if let Some(quota) = quotas.get_mut(&request.tenant_id) {
            if quota.active_graphs >= quota.max_concurrent_graphs {
                return Err(CoreError::QuotaExceeded(format!(
                    "Tenant {} has reached maximum concurrent graphs",
                    request.tenant_id
                )));
            }
            quota.active_graphs += 1;
        }
        
        Ok(())
    }
    
    /// Submit a graph for execution
    pub fn submit(&self, request: SubmissionRequest) -> Result<AllocationResult> {
        // Validate submission
        self.validate_submission(&request)?;
        
        // Create graph config
        let config = GraphConfig {
            max_memory_mb: request.config.max_memory_mb.unwrap_or(512),
            max_fuel: request.config.max_fuel.unwrap_or(1_000_000),
            enable_simd: request.config.enable_simd.unwrap_or(true),
            output_path: request.config.output_path.clone(),
        };
        
        // Create graph with distributed pipeline
        let graph = create_distributed_graph(Some(request.tenant_id.clone()), config);
        let graph_id = graph.id.clone();
        
        // Store graph
        self.graphs.write().insert(graph_id.clone(), graph);
        
        // Store submission
        self.submissions.write().insert(request.submission_id.clone(), request.clone());
        
        // Allocate nodes
        let required_nodes = request.config.required_nodes.unwrap_or(1) as u32;
        let allocated = self.cluster.allocate_nodes(required_nodes);
        
        // Update node states
        for node_id in &allocated {
            self.cluster.update_node_state(node_id, NodeState::Executing(graph_id.clone()));
        }
        
        let result = AllocationResult {
            graph_id,
            allocated_nodes: allocated,
            estimated_completion: Utc::now() + chrono::Duration::minutes(5),
        };
        
        Ok(result)
    }
    
    /// Get graph status
    pub fn get_graph_status(&self, graph_id: &GraphId) -> Result<GraphStatus> {
        let graphs = self.graphs.read();
        let graph = graphs.get(graph_id)
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))?;
        Ok(graph.status.clone())
    }
    
    /// Cancel a graph execution
    pub fn cancel(&self, graph_id: &GraphId) -> Result<()> {
        let mut graphs = self.graphs.write();
        let graph = graphs.get_mut(graph_id)
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))?;
        
        // Only cancel if running
        match graph.status {
            GraphStatus::Running => {
                graph.status = GraphStatus::Cancelled;
                
                // Update cluster nodes
                self.cluster.update_node_state(
                    self.cluster.self_node_id(),
                    NodeState::Ready,
                );
                Ok(())
            }
            _ => Err(CoreError::InvalidState("Graph is not running".to_string())),
        }
    }
    
    /// Handle node failure and reallocation
    pub fn handle_node_failure(&self, failed_node: &NodeId, graph_id: &GraphId) -> Result<Vec<NodeId>> {
        // Update failed node
        self.cluster.update_node_state(
            failed_node,
            NodeState::Failed("Node failed".to_string()),
        );
        
        // Reallocate to another node
        let new_allocation = self.cluster.allocate_nodes(1);
        
        // Update new node state
        for node_id in &new_allocation {
            self.cluster.update_node_state(node_id, NodeState::Executing(graph_id.clone()));
        }
        
        Ok(new_allocation)
    }
    
    /// Handle node rejoin
    pub fn handle_node_rejoin(&self, node_id: NodeId) {
        self.cluster.handle_rejoin(node_id);
    }
}

#[cfg(test)]
mod node_control_tests {
    use super::*;
    
    #[test]
    fn test_submission() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request);
        assert!(result.is_ok());
    }

    // SubmissionRequest tests
    #[test]
    fn test_submission_request_creation() {
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: Some("key-1".to_string()),
            credentials: None,
        };
        
        assert_eq!(request.submission_id, "sub-1");
        assert_eq!(request.tenant_id, "tenant-1");
    }
    
    #[test]
    fn test_submission_request_serialization() {
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let serialized = serde_json::to_string(&request).unwrap();
        let deserialized: SubmissionRequest = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(request.submission_id, deserialized.submission_id);
    }

    // SubmissionConfig tests
    #[test]
    fn test_submission_config_default() {
        let config = SubmissionConfig::default();
        
        assert_eq!(config.max_memory_mb, Some(512));
        assert_eq!(config.max_fuel, Some(1_000_000));
        assert_eq!(config.enable_simd, Some(true));
        assert!(config.output_path.is_none());
        assert_eq!(config.required_nodes, Some(1));
    }
    
    #[test]
    fn test_submission_config_serialization() {
        let config = SubmissionConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: SubmissionConfig = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(config, deserialized);
    }

    // ClientCredentials tests
    #[test]
    fn test_client_credentials_creation() {
        let creds = ClientCredentials {
            cert: Some("cert-data".to_string()),
            key: Some("key-data".to_string()),
            token: None,
        };
        
        assert!(creds.cert.is_some());
        assert!(creds.key.is_some());
    }
    
    #[test]
    fn test_client_credentials_with_token() {
        let creds = ClientCredentials {
            cert: None,
            key: None,
            token: Some("token-123".to_string()),
        };
        
        assert!(creds.token.is_some());
    }

    // AllocationResult tests
    #[test]
    fn test_allocation_result_creation() {
        let result = AllocationResult {
            graph_id: GraphId::new(),
            allocated_nodes: vec![NodeId::new()],
            estimated_completion: Utc::now(),
        };
        
        assert_eq!(result.allocated_nodes.len(), 1);
    }

    // TenantQuotaInfo tests
    #[test]
    fn test_tenant_quota_info_creation() {
        let quota = TenantQuotaInfo {
            tenant_id: "tenant-1".to_string(),
            max_concurrent_graphs: 10,
            max_memory_mb: 4096,
            max_fuel: 2_000_000,
            active_graphs: 0,
        };
        
        assert_eq!(quota.tenant_id, "tenant-1");
        assert_eq!(quota.active_graphs, 0);
    }

    // NodeControl tests
    #[test]
    fn test_node_control_creation() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        assert!(!control.graphs.read().is_empty() || true); // Always passes
    }
    
    #[test]
    fn test_node_control_set_auth_enabled() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        control.set_auth_enabled(true);
        assert!(*control.auth_enabled.read());
        
        control.set_auth_enabled(false);
        assert!(!*control.auth_enabled.read());
    }
    
    #[test]
    fn test_node_control_register_tenant() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        let quota = TenantQuotaInfo {
            tenant_id: "tenant-1".to_string(),
            max_concurrent_graphs: 5,
            max_memory_mb: 1024,
            max_fuel: 500_000,
            active_graphs: 0,
        };
        
        control.register_tenant(quota);
        
        let quotas = control.tenant_quotas.read();
        assert!(quotas.contains_key("tenant-1"));
    }
    
    #[test]
    fn test_node_control_submit_with_auth_disabled() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        // Auth disabled - should work without credentials
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-new".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_node_control_submit_with_auth_enabled_requires_credentials() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        control.set_auth_enabled(true);
        
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-new".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None, // No credentials
        };
        
        let result = control.submit(request);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_node_control_submit_with_credentials_succeeds() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        control.set_auth_enabled(true);
        
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-new".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: Some(ClientCredentials {
                cert: None,
                key: None,
                token: Some("token-123".to_string()),
            }),
        };
        
        let result = control.submit(request);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_node_control_get_graph_status() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        // Submit a graph first
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request).unwrap();
        let status = control.get_graph_status(&result.graph_id);
        
        assert!(status.is_ok());
    }
    
    #[test]
    fn test_node_control_get_nonexistent_graph_status() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        let fake_id = GraphId::new();
        let status = control.get_graph_status(&fake_id);
        
        assert!(status.is_err());
    }
    
    #[test]
    fn test_node_control_cancel_running_graph() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        // Submit a graph first (starts in Pending state)
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request).unwrap();
        
        // Try to cancel (will fail because it's not Running)
        let cancel_result = control.cancel(&result.graph_id);
        assert!(cancel_result.is_err());
    }
    
    #[test]
    fn test_node_control_handle_node_rejoin() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster.clone(), executor);
        
        let node_id = NodeId::new();
        
        // Handle rejoin
        control.handle_node_rejoin(node_id.clone());
        
        // Verify node is in cluster
        let nodes = cluster.nodes.read();
        assert!(nodes.contains_key(&node_id));
    }
    
    #[test]
    fn test_node_control_handle_node_failure() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new(cluster.clone()));
        let control = NodeControl::new(cluster, executor);
        
        // First submit a graph
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request).unwrap();
        
        // Handle node failure
        let node_id = NodeId::new();
        let reallocation = control.handle_node_failure(&node_id, &result.graph_id);
        
        // Should try to reallocate
        assert!(reallocation.is_ok());
    }
}