//! Core library for the KinePlex distributed analytical engine.

pub mod accumulator;
pub mod arrow_concat;
pub mod arrow_ffi;
pub mod buffer_recycle;
pub mod data;
pub mod egress;
pub mod error;
pub mod flatbuffers;
pub mod geometry;
pub mod graph;
pub mod iouaring;
pub mod iouring_net;
pub mod metric_formula;
pub mod metrics;
pub mod observability;
pub mod packet;
pub mod physical_plan;
pub mod plane_isolation;
pub mod quic;
pub mod receptor;
pub mod reliability;
pub mod spike_tap;
pub mod synapse;
pub mod terminal;
pub mod topology;
pub mod wasm;

pub use error::{CoreError, Result};
pub use data::ArrowData;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a graph execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphId(pub Uuid);

impl GraphId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for GraphId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for GraphId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stage in the execution pipeline
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Stage {
    /// Reception stage - receives input data
    Receptor,
    /// Wasm execution stage
    Wasm,
    /// Aggregation stage
    Aggregation,
    /// Terminal stage - produces final output
    Terminal,
}

impl Stage {
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Receptor => "receptor",
            Stage::Wasm => "wasm",
            Stage::Aggregation => "aggregation",
            Stage::Terminal => "terminal",
        }
    }
    
    pub fn all() -> Vec<Stage> {
        vec![Stage::Receptor, Stage::Wasm, Stage::Aggregation, Stage::Terminal]
    }
}

/// Status of a graph execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GraphStatus {
    /// Graph is pending execution
    Pending,
    /// Graph is currently executing
    Running,
    /// Graph execution completed successfully
    Completed,
    /// Graph execution failed
    Failed(String),
    /// Graph was cancelled
    Cancelled,
}

impl GraphStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, GraphStatus::Completed | GraphStatus::Failed(_) | GraphStatus::Cancelled)
    }
    
    pub fn is_active(&self) -> bool {
        matches!(self, GraphStatus::Pending | GraphStatus::Running)
    }
    
    pub fn as_str(&self) -> &str {
        match self {
            GraphStatus::Pending => "pending",
            GraphStatus::Running => "running",
            GraphStatus::Completed => "completed",
            GraphStatus::Failed(_) => "failed",
            GraphStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for GraphStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphStatus::Pending => write!(f, "pending"),
            GraphStatus::Running => write!(f, "running"),
            GraphStatus::Completed => write!(f, "completed"),
            GraphStatus::Failed(msg) => write!(f, "failed: {}", msg),
            GraphStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Graph definition for execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    /// Unique identifier
    pub id: GraphId,
    /// Current status
    pub status: GraphStatus,
    /// Stages in the pipeline
    pub stages: Vec<Stage>,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: Option<String>,
    /// Configuration
    pub config: GraphConfig,
}

impl Graph {
    pub fn new(id: GraphId, config: GraphConfig) -> Self {
        Self {
            id,
            status: GraphStatus::Pending,
            stages: Stage::all(),
            tenant_id: None,
            config,
        }
    }
    
    pub fn with_tenant(mut self, tenant_id: String) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }
    
    pub fn with_status(mut self, status: GraphStatus) -> Self {
        self.status = status;
        self
    }
    
    pub fn stages_count(&self) -> usize {
        self.stages.len()
    }
}

/// Configuration for graph execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// Maximum memory in MB
    pub max_memory_mb: u64,
    /// Maximum fuel (instructions)
    pub max_fuel: u64,
    /// Enable SIMD optimizations
    pub enable_simd: bool,
    /// Output path for Parquet files
    pub output_path: Option<String>,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_fuel: 1_000_000,
            enable_simd: true,
            output_path: None,
        }
    }
}

impl GraphConfig {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_max_memory(mut self, mb: u64) -> Self {
        self.max_memory_mb = mb;
        self
    }
    
    pub fn with_max_fuel(mut self, fuel: u64) -> Self {
        self.max_fuel = fuel;
        self
    }
    
    pub fn with_simd(mut self, enable: bool) -> Self {
        self.enable_simd = enable;
        self
    }
    
    pub fn with_output_path(mut self, path: String) -> Self {
        self.output_path = Some(path);
        self
    }
}

/// Graph executor trait - implements the distributed execution pipeline
pub trait GraphExecutor: Send + Sync {
    /// Execute a graph across the distributed pipeline
    fn execute(&self, graph: &Graph) -> impl std::future::Future<Output = Result<ExecutionResult>> + Send;
    
    /// Get current execution status
    fn status(&self, graph_id: &GraphId) -> Result<GraphStatus>;
    
    /// Cancel a running execution
    fn cancel(&self, graph_id: &GraphId) -> Result<()>;
}

/// Result of graph execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Graph ID
    pub graph_id: GraphId,
    /// Final status
    pub status: GraphStatus,
    /// Output data (Arrow IPC format)
    pub output: Option<Vec<u8>>,
    /// Output Parquet path (if materialized)
    pub output_path: Option<String>,
    /// Execution metrics
    pub metrics: ExecutionMetrics,
}

impl ExecutionResult {
    pub fn new(graph_id: GraphId, status: GraphStatus, metrics: ExecutionMetrics) -> Self {
        Self {
            graph_id,
            status,
            output: None,
            output_path: None,
            metrics,
        }
    }
    
    pub fn with_output(mut self, data: Vec<u8>) -> Self {
        self.output = Some(data);
        self
    }
    
    pub fn with_output_path(mut self, path: String) -> Self {
        self.output_path = Some(path);
        self
    }
    
    pub fn is_success(&self) -> bool {
        self.status == GraphStatus::Completed
    }
}

/// Metrics collected during execution
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionMetrics {
    /// Total execution time in milliseconds
    pub total_time_ms: u64,
    /// Time per stage in milliseconds
    pub stage_times_ms: std::collections::HashMap<String, u64>,
    /// Memory peak in bytes
    pub memory_peak_bytes: u64,
    /// CPU time in milliseconds
    pub cpu_time_ms: u64,
    /// Number of rows processed
    pub rows_processed: u64,
    /// Number of nodes involved
    pub nodes_involved: u32,
}

impl ExecutionMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_stage_time(mut self, stage: &str, time_ms: u64) -> Self {
        self.stage_times_ms.insert(stage.to_string(), time_ms);
        self
    }
    
    pub fn with_memory(mut self, bytes: u64) -> Self {
        self.memory_peak_bytes = bytes;
        self
    }
    
    pub fn with_cpu_time(mut self, time_ms: u64) -> Self {
        self.cpu_time_ms = time_ms;
        self
    }
    
    pub fn with_rows(mut self, count: u64) -> Self {
        self.rows_processed = count;
        self
    }
    
    pub fn with_nodes(mut self, count: u32) -> Self {
        self.nodes_involved = count;
        self
    }
    
    pub fn total_stage_time(&self) -> u64 {
        self.stage_times_ms.values().sum()
    }
}

/// Create a new graph with the standard distributed pipeline
pub fn create_distributed_graph(tenant_id: Option<String>, config: GraphConfig) -> Graph {
    Graph {
        id: GraphId::new(),
        status: GraphStatus::Pending,
        stages: vec![
            Stage::Receptor,
            Stage::Wasm,
            Stage::Aggregation,
            Stage::Terminal,
        ],
        tenant_id,
        config,
    }
}

/// Create a simple graph with a single stage for testing
pub fn create_simple_graph(tenant_id: Option<String>) -> Graph {
    create_distributed_graph(tenant_id, GraphConfig::default())
}


// ==========================================
// NodeConfig & Node Initializer (from HEAD)
// ==========================================

use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

/// Default UDP port used by a KinePlex node.
pub const DEFAULT_NODE_PORT: u16 = 8000;

/// Immutable runtime configuration shared by all node components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub advertise_ip: Option<IpAddr>,
    pub seeds: Vec<SocketAddr>,
    /// Whether to enable the native CPU sampling endpoint.
    pub enable_profiling: bool,
}

impl NodeConfig {
    #[must_use]
    pub const fn new(bind_ip: IpAddr, port: u16, seeds: Vec<SocketAddr>) -> Self {
        Self {
            bind_ip,
            port,
            advertise_ip: None,
            seeds,
            enable_profiling: false,
        }
    }

    #[must_use]
    pub const fn with_advertise_ip(mut self, advertise_ip: Option<IpAddr>) -> Self {
        self.advertise_ip = advertise_ip;
        self
    }

    /// Enables or disables native CPU profiling for this node process.
    #[must_use]
    pub const fn with_profiling(mut self, enable_profiling: bool) -> Self {
        self.enable_profiling = enable_profiling;
        self
    }

    #[must_use]
    pub fn advertise_addr(&self) -> Option<SocketAddr> {
        self.advertise_ip
            .or_else(|| (!self.bind_ip.is_unspecified()).then_some(self.bind_ip))
            .map(|ip| SocketAddr::new(ip, self.port))
    }

    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_ip, self.port)
    }
}

static NODE_CONFIG: OnceLock<NodeConfig> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeConfigInitError {
    AlreadyInitialized,
    UnavailableAfterInitialization,
}

impl std::fmt::Display for NodeConfigInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInitialized => {
                formatter.write_str("node configuration is already initialized")
            }
            Self::UnavailableAfterInitialization => {
                formatter.write_str("node configuration is unavailable after initialization")
            }
        }
    }
}

impl std::error::Error for NodeConfigInitError {}

pub fn initialize_node_config(
    config: NodeConfig,
) -> std::result::Result<&'static NodeConfig, NodeConfigInitError> {
    NODE_CONFIG.set(config).map_err(|_| {
        tracing::warn!("node configuration initialization was attempted more than once");
        NodeConfigInitError::AlreadyInitialized
    })?;

    let config = NODE_CONFIG
        .get()
        .ok_or(NodeConfigInitError::UnavailableAfterInitialization)?;
    tracing::debug!(bind_addr = %config.bind_addr(), seed_count = config.seeds.len(), "node configuration initialized");
    Ok(config)
}

#[must_use]
pub fn node_config() -> Option<&'static NodeConfig> {
    NODE_CONFIG.get()
}
